# Incident — ETH reversal trade: unreachable take-profit target, timeout never fired

**Status: both issues fixed, tested, deployed.**

## The trade

```
[PAPER] 🎯 ETH ENTRY (taker) filled → EXIT quote resting | 15:43:58 | T-61s | DOWN ↓ | reversal
5.71sh @ 0.8750 → exit target 1.0250

[PAPER] ✅ ETH TRADE WIN | 15:45:00 | DOWN ↓ | reversal
entry=0.8750 → exit=1.0000 | dur=62s | cycle open→exit: $1936.63→$1934.20 | delta=-0.125% | pnl=+$0.6700 | 1W/0L/0SL/0UW/0TO | ind: p_up=0.5000 (edge-0.4850) vol=1.05e-3
```

ETH's `unwind_time_rev` is 28s. The position entered at T-61s remaining and didn't close until
natural cycle resolution at T-0 (`dur=62s`), 34s past the timeout deadline — as a plain `WIN`,
not `Outcome::Timeout`.

## Issue 1 — take-profit target above 1.0 is structurally unreachable

`tp_price = cost + unwind_pnl_rev` (`worker.rs::finalize_entry_fill`) has no ceiling.
`unwind_pnl_rev = 0.15` for ETH; entry filled at `0.8750` → `tp_price = 1.0250`. A Polymarket
token's price can never exceed ~0.99 (never a real 1.00 while still tradeable — 1.00 is the
*resolution* value, not a quotable price) — a resting sell at `1.0250` can **never** trade
through. This isn't specific to this trade: any reversal fill priced above `0.99 − unwind_pnl_rev`
produces an unreachable target — for ETH (`unwind_pnl_rev = 0.15`) that's any fill above `0.84`,
which is routine for a reversal entry (the whole strategy fires on prices recovering toward
`price_high_rev = 0.9`).

**Confirmed structurally, not just for this trade** — `finalize_entry_fill` computes `tp_price`
with a single unconditional addition, no `.min(...)`:

```rust
let tp_price = cost + self.unwind_pnl;
```

### Fix implemented

`MAX_SELL_PRICE: f64 = 0.99` (`execution.rs`, mirroring the existing `MIN_GTC_SHARES`/
`PAPER_TRADE_THROUGH` engineering-constant pattern). The formula turned out to have **four** call
sites, not one — `finalize_entry_fill`, `on_limit_sell_placed`'s `Matched` and `Failed|DryRun`
branches, and `on_unwind_failed` all independently recomputed `X + unwind_pnl` — so instead of
patching each inline, extracted a single free function:

```rust
fn tp_price_for(basis: f64, unwind_pnl: f64) -> f64 {
    (basis + unwind_pnl).min(MAX_SELL_PRICE)
}
```

(A free function, not a `Worker` method — several call sites already hold a `&mut self.state`
borrow via a `WorkerState::Holding(h)` match when they need this, and a `&self` method would
conflict with that.) All four sites now go through it, so a future call site literally cannot
reintroduce the uncapped bug without also skipping this function. Doesn't change behavior for the
common case (most fills sit well under `0.84`); only clamps the minority of fills where `cost` is
already close to the ceiling — those positions now get a *reachable* (if compressed) take-profit
target instead of one that can only ever be closed by stop-loss/timeout/cycle-close.

## Issue 2 — the max-holding-time (timeout) safety net didn't fire

### Why it isn't a state-machine logic bug

`on_poly`'s timeout check is unconditional on `exit_arm` (cancels a resting GTC sell first if one
exists, then force-closes) — read the current code and it looks structurally correct. Wrote a
new test, `timeout_force_closes_with_a_confirmed_gtc_resting_exit_arm` (`worker.rs`), reproducing
the *exact* production shape this trade was in — a >=`MIN_GTC_SHARES` fill with a **confirmed**
`GtcResting` exit arm (the existing `timeout_force_closes_after_unwind_time_elapsed_with_no_other_exit`
test never sent `LimitSellPlaced`, so it only ever exercised the provisional `PriceMonitor` arm —
a real coverage gap, now closed). The new test passes: given a tick at `entry_ts + unwind_time`,
the timeout fires correctly regardless of exit-arm type. **The decision logic itself is not the
bug.**

### The actual root cause: the timeout check only runs on a `PolyTick`, and none arrived

`on_poly` (and therefore the timeout branch) only executes when the driver calls
`worker.step(Event::PolyTick(tick))` — which only happens when a new poly price tick arrives over
NATS. `price_feed` only publishes `price.poly.<ASSET>` when Polymarket's own best-bid-ask/
price-change WebSocket emits an update (`price_feed/src/collect.rs`, confirmed by reading the
publish call site) — there is no periodic keepalive tick.

Evidence this is exactly what happened, pulled from Oracle's `live.log`:
- Console lines: an entry BUY, a resting `LIMIT SELL` ack, then **nothing** until the final
  `TradeRecord` at cycle close. Specifically **zero** `[ORDER] ... CANCEL ...` or
  `[ORDER] ... CLOSE ...` lines in between. `[ORDER] ... CANCEL ...` is an *unconditional* print
  inside `Action::CancelLimitSell`'s handler (`bin/live.rs`) — if the timeout branch had fired at
  all, this line would exist regardless of whether the subsequent close itself succeeded or
  failed. Its total absence proves the branch never fired, not that it fired-and-failed.
- `exit_attempts: 0` on the final `TradeRecord` — corroborates zero close attempts of any kind.
- The heartbeat log (30s-interval, independent of the bug) shows ETH's DOWN price frozen at
  exactly `0.9850` across *both* the T-38s and T-8s snapshots — a 30-second stretch spanning the
  T-33s timeout deadline (`entry at T-61s + 28s unwind_time = T-33s`) with **no visible price
  movement** — consistent with the order book going quiet once the outcome became near-certain
  (thin remaining interest at the extremes is exactly when a book stops emitting best-bid-ask
  updates).
- No process restart in this window (`journalctl -u trader-live`, `13:55–16:00 HKT`: nothing
  between the 13:55:52 deploy and now) — rules out a reconcile-on-restart explanation.

So: **the max-holding-time force-close is entirely tick-driven, not wall-clock-driven.** When
the order book goes quiet — plausibly *because* the position has moved decisively one way,
exactly when a time-based safety exit matters most — there's no tick to evaluate the timeout on,
and the position silently rides to natural cycle-close instead. This is a structural gap, not a
one-off: it will recur any time a held position's book goes quiet for longer than
`unwind_time_rev` before the cycle actually ends.

### Fix implemented

Decoupled the timeout (and, for free, the stop-loss) check from tick arrival: every currently-
`Holding` position is now re-evaluated on a wall-clock cadence, using the last known price, not
only when a fresh tick happens to arrive. The driver's `main()` already had a `ticker.tick()` arm
firing every 1s (used for cycle-boundary detection); it now also feeds a synthetic, same-price
`PolyTick` (`ts: now_secs_f64()`, `up`/`dn`: `slot.last_poly_up`/`dn`, `up_bid`/`up_ask` left at
the `0.0` "unobserved" sentinel) into any slot whose worker is currently `Holding`, so `on_poly`'s
stop-loss/take-profit/timeout checks re-run every second regardless of real tick cadence:

```rust
_ = ticker.tick() => {
    for slot in assets.iter_mut().filter(|s| {
        should_reevaluate_holding(s.worker.is_holding(), s.last_poly_up, s.last_poly_dn)
    }) {
        let synthetic = PolyTick { ts: now_secs_f64(), up: slot.last_poly_up, dn: slot.last_poly_dn, up_bid: 0.0, up_ask: 0.0 };
        let actions = slot.worker.step(Event::PolyTick(synthetic));
        driver.process_actions(slot, actions, Feed::Clob, &indicator_store, PUP_GATE_MAX_AGE_SECS).await;
    }
    // existing cycle-boundary logic unchanged, right after ...
}
```

Two small additions support this:
- `Worker::is_holding()` (mirrors the existing `is_confirming()`) — lets the driver filter without
  exposing `WorkerState` itself. Deliberately scoped to `Holding` only: **not** `Watching` (would
  re-run `try_enter` on stale data) and **not** `Unwinding`/`StopExiting`/`TimingOut` (those already
  have their own close attempt outstanding — re-checking them would be redundant, not harmful, but
  there's no reason to).
- `should_reevaluate_holding(is_holding, last_poly_up, last_poly_dn) -> bool` — the filter as a
  pure, directly-testable function (mirrors this file's existing `should_suppress_startup_cycle`
  pattern): `is_holding` AND both prices have actually been observed (`> 0.0`) — guards against
  synthesizing a tick from the `0.0` "never ticked" sentinel, which can't happen for a genuinely
  `Holding` slot (it needed a real price to enter) but costs nothing to guard regardless.

### Cross-reference

The same "tick-driven, not wall-clock-driven" shape is what let the take-profit itself never
resolve in issue 1 — a live/reachable target would eventually get *touched* by a real tick when
the book resumes activity, but a genuinely quiet book means neither the take-profit nor the
timeout has any event to fire on. Both proposed fixes together close the gap completely: issue 1
guarantees the target is reachable *if* a tick arrives; issue 2 guarantees a check happens even if
one doesn't.

## Tests

`worker.rs`:
- `timeout_force_closes_with_a_confirmed_gtc_resting_exit_arm` — proves the timeout's
  state-machine decision logic is correct in isolation (ruling out issue 2 being a `worker.rs`
  bug), closing a real pre-existing coverage gap regardless of this incident.
- `tp_price_for_uncapped_case_is_plain_addition` / `tp_price_for_caps_at_max_sell_price` /
  `tp_price_for_exactly_at_cap_is_unchanged` — direct coverage of the capping formula, including
  the exact regression value (`0.875 + 0.15 → 0.99`, not `1.025`).
- `high_cost_fill_produces_a_capped_reachable_take_profit_target` — end-to-end reproduction of
  the real trade through the actual entry-fill path (`try_enter` → `OrderFilled` →
  `finalize_entry_fill`), confirming the resulting `Action::PlaceLimitSell` and `HoldingData`
  both carry the capped `0.99`, not `1.025`.
- `capped_take_profit_target_is_reachable_and_fires` — the capped target isn't just a number: once
  price actually reaches it, take-profit genuinely fires.
- `is_holding_true_only_while_holding` — `Holding` only, not before entry and not once an exit is
  already in flight.

`bin/live.rs`:
- `should_reevaluate_holding_tests::{holding_with_real_prices_is_reevaluated,
  not_holding_is_never_reevaluated, never_ticked_slot_is_never_reevaluated_even_if_marked_holding}`
  — the wall-clock filter's three cases directly.

Full suite: 312 lib + 5 `backtest` + 61 `live` (11 new across both files) — all green.
`cargo fmt --all --check` / `cargo clippy --all-targets --all-features -- -D warnings` clean.
Local `live --paper` smoke run (new per-second re-check loop active, zero held positions): clean
start/stop, no panics — confirms the loop is inert and harmless in the common (nothing held) case;
the decision logic itself is proven by the unit tests above, following this repo's existing
precedent that `bin/live.rs`'s driver loop isn't directly integration-tested (no test constructs a
full running `main()`).

## Deploy

`./scripts/deploy_trader.sh` (trader-only — no config change needed for either fix).
