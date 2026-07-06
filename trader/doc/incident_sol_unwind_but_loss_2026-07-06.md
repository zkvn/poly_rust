# Incident — SOL UNWIND lost money despite a correct directional call, 2026-07-06

Telegram alert:

```
✅ SOL TRADE UNWIND | 05:44:33 | UP ↑ | reversal
entry=0.9000 → exit=0.8200 | cycle: $81.61→$81.67 | pnl=-$0.1073 | 0W/0L
```

## 1. This is not a bad directional call — the underlying went the *right* way

`live_trades_sol_reversal.csv` row for this trade:

```
logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error
1783287873.39,sol-updown-5m-1783287600,reversal,UP,1783287870,0.8999999999999999,0.82,UNWIND,-0.1073,0,
```

Binance SOL moved `$81.61 → $81.67` across the cycle — up, the same direction as the `Up` position taken. This is the **only** UNWIND row across all four in the SOL CSV where `exit_price < token_price`; the other three all show a normal profitable take-profit (`exit_price > token_price`). So the loss isn't the reversal thesis failing — it's the exit fill itself landing far below both the entry price and the intended take-profit target.

## 2. Sequence, from `live.log` (lines 43823–43836, cycle `sol-updown-5m-1783287600`)

```
[live] heartbeat SOL (reversal) ... T-43s binance=81.5600 up=0.1150 dn=0.8850
[ORDER] SOL BUY Up @ 0.8950 size=$1.00 -> placed=true shares=1.1111 cost=0.9000 err=None
[close] retry 1/5: ... "not enough balance / allowance: the balance is not enough -> balance: 0, order amount: 1110000"
[close] retry 2/5: ... "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."
[close] retry 3/5: ... "no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."
[ORDER] SOL CLOSE 1.1111 (TakeProfit) -> status=Matched sold=1.1100 usdc=0.9102 err=None
[TRADE] TradeRecord { ..., side: Up, token_price: 0.9, exit_price: 0.82, outcome: Unwind, pnl: -0.1073, exit_attempts: 0, ... }
[live] heartbeat SOL (reversal) ... T-13s binance=81.6300 up=0.8050 dn=0.1950
```

Entry filled at an average price of **0.90** (signal was 0.895; the entry itself swept a thin book up to 0.90 — see §5). The take-profit target (`tp_price`) armed at entry was `0.90 + unwind_pnl_rev(0.03) = 0.93` (`worker.rs:619`, `strategy_20260705.toml` `[unwind_pnl_rev] default = 0.03`, no SOL override). Instead of selling at 0.93, the position was closed **3.4 seconds later** for an average of **0.82** — 11 cents below the intended target and 8 cents below entry.

`exit_attempts=0` in the TradeRecord only means the *worker-level* retry (re-invoking `close_position()` on a later tick) never happened — it filled on the same call. But **within that single call**, `close_position()` has its own internal retry loop (`execution.rs:429`, `self.cfg.close_max_retries = 5`) that resubmits the FAK sell on certain errors. The log shows 3 failed internal attempts (unsettled balance, then no book match twice) before the 4th succeeded — meaning the sell order was live and re-submitting into the book for ~3.4 seconds while price reverted from the entry-time spike back down to 0.82.

Fee math checks out against the logged pnl: `1.1111 × (0.82 − 0.90) = -0.0889` on price alone, plus ≈$0.018 combined entry+exit fees ≈ **-0.1073**, matching exactly.

## 3. Root cause: entry has a max-price guard, exit has no min-price guard

**Entry side — capped, by two independent layers:**
- `gates.rs:76-78` — `MaxBuyPrice` gate rejects entry if `token_price > params.max_buy_price` (0.95).
- `gates.rs:79-84` — `PriceHighRev` gate (reversal-only) rejects entry if `token_price > price_high_rev` (0.90).
- `execution.rs:280-322` — the BUY is placed as a **limit** FAK (`.price(price_dec)`, `execution.rs:319`), with the limit price bounded by `aggressive_entry_price()` (`execution.rs:133-138`), so it structurally cannot fill above `max_buy_price`.

**Exit side — no equivalent floor:**
- Both take-profit ("unwind") and stop-loss exits funnel into `LiveExecutionEngine::close_position()` (`execution.rs:411-473`), called from `worker.rs:592` (StopLoss) and `worker.rs:602` (TakeProfit).
- Its order builder (`execution.rs:429-437`) is a bare `market_order()` / `OrderType::FAK` sized only in shares — **no `.price(...)` call anywhere in this function**. Compare directly to the entry builder at `execution.rs:313-322`, which does call `.price(price_dec)`.
- The only per-exit check is `h.shares >= MIN_SELLABLE_SHARES` (`worker.rs:586`, `:600`) — a dust-avoidance check, not a price guard.
- Net effect: once an unwind/stop-loss is triggered, the sell will fill at **whatever price the book gives it**, arbitrarily far below the trigger price, with nothing to reject or bound a bad fill.

Confirming this gap was never wired up, not merely undocumented: `strategy_20260705.toml:13` has

```
# FAK order slippage — covers normal 1-tick bid-ask spread without sweeping wide asks.
order_slippage = 0.05
```

`grep -rn "order_slippage" trader/src/` returns **zero matches** — this config key is parsed nowhere. A slippage/price-floor guard was evidently planned (there's even a comment describing its intended purpose) but never implemented in `close_position()` or anywhere else.

This also matches a prior, still-open observation in `trader/doc/latency_2026-07-04.md:113-115`, which flagged that "a stale price at exactly the wrong moment matters more for unwind than for entry, since unwind is racing a specific target price" — anticipating this exact failure mode as a latency-instrumentation gap, before it produced a concrete realized loss.

## 4. Why "reversal" in the alert is misleading here

The `reversal` in `✅ SOL TRADE UNWIND | ... | reversal` is `slot.worker.strategy_name` — the label for the *entry strategy* (`ReversalStrategy`, dip-and-recover entry logic), not a description of what happened on exit. "UNWIND" is this codebase's name for a take-profit close (`CloseReason::TakeProfit`, armed via `ExitArm::PriceMonitor { tp_price }`, fired at `worker.rs:599-602` once `exit_price >= tp_price`). Nothing in either label describes price direction on the way out — the alert format doesn't distinguish "hit target cleanly" from "hit target then the fill slipped," which is part of why this looked surprising at a glance.

## 5. Secondary, smaller effect: entry itself already paid up

Signal price was 0.895 (under both the 0.90 `price_high_rev` gate and the 0.95 `max_buy_price` gate), but the BUY's actual average fill was 0.90 — the entry-side FAK swept a thin book and `aggressive_entry_price()` (bounded by `max_buy_price = 0.95`, not `price_high_rev`) let it walk up to 0.90 across retries. This cost half a cent of entry-side slippage before the exit-side loss even began; not the main driver of the loss, but consistent with SOL's book being thin at this moment on both sides of the trade.

## 6. Why the 3 failed internal attempts happened, what they mean, and whether the fix addresses it

This was an explicit question during the fix (see §9 Q&A): *"explain what is that '3 failed internal attempts, unsettled balance etc', why would they happen here, what the impacts are, proposal for fix if there is any changes."*

**Why they happen:**
- **Retry 1 — `"not enough balance ... balance: 0"`**: the entry BUY's fill is confirmed by the CLOB API immediately, but the actual token balance only becomes spendable once the corresponding Polygon transaction is mined on-chain. A close attempt fired milliseconds after the entry fill (exactly this incident's shape — entry and the take-profit crossing landed within the same tick window) races that on-chain settlement and loses the race almost every time. This is normal, expected latency (typically ~1-2s on Polygon), not a bug.
- **Retries 2-3 — `"no orders found to match with FAK order"`**: a FAK (Fill-And-Kill) order only matches against liquidity that exists on the book *right now*; if nothing is resting at an acceptable price at that exact instant, it's killed rather than queued. Thin order books on lower-volume assets (SOL, at 1-2 shares of size) routinely have brief moments with no matching counterparty, especially for the "sell into a spike" case here, where the spike itself was the thing about to disappear.
- **Impact**: none of this is inherently harmful *if the eventual fill price is still bounded*. The actual harm in this incident was that `close_position()`'s retry loop had no price floor at all — it kept trying at market with no `.price()` limit, so by the time attempt 4 matched, ~3.4 seconds had passed and the price had moved 8-11 cents against the position. The retries themselves aren't the defect; the *unbounded price* they were eventually allowed to fill at is.

**Does the fix (§7 below) address this?** Yes, directly, without needing a separate change:
- The settlement-lag retry ("balance: 0") and the thin-book retry ("no orders found") both still *can* happen under the new bounded design — but now the bound (`min_price = tp_price`) means a fill that does eventually land can never be worse than the take-profit target itself. There is no longer a price-safety reason to retry internally at all.
- Per direction (§9), the new design also removes the *internal* retry loop for take-profit closes entirely (single attempt, bounded price) and instead waits for the next real `PolyTick` to retry — turning "retry immediately, unbounded, up to 5x in a tight loop" into "retry once per real market tick, always bounded," which is strictly safer on both the original DOGE hammering incident's axis (rate of attempts) and this incident's axis (price of the eventual fill).
- Stop-loss closes intentionally keep the old unbounded `close_position()` and its retry loop unchanged — a stop-loss must close regardless of price, so bounding it or slowing its retries down would reintroduce the risk `close_position()` was originally built to eliminate (trader/doc's `incident_halt_reset...`/stop-loss-never-filled history).

No further change beyond §7 was needed to fully resolve the "why do these retries happen" question — they're expected exchange behavior, not a defect, once the price floor makes them safe to have at all.

## 7. Fix — implemented 2026-07-06

Per direction (§9 Q&A), all four points below were implemented (not just proposed):

1. **Minimum sell price for a take-profit unwind is automatically the take-profit target itself — `tp_price` — no new config field.** `execution.rs::close_position_at_price(token_id, shares, min_price)` is a new `ExecutionEngine` method: a FAK sell **with** `.price(min_price)` (mirroring the entry builder's existing `.price(price_dec)` at `execution.rs:319`). `worker.rs`'s `Action::ClosePosition` now carries `limit_price: Option<f64>` — `Some(tp_price)` for `CloseReason::TakeProfit` (set in `on_poly`, `worker.rs:~617`), `None` for `CloseReason::StopLoss`. `bin/live.rs::execute()` branches on this field to call `close_position_at_price` vs. the original unbounded `close_position`.
2. **FAK confirmed as the right order type** — kept for both the bounded take-profit close and the unbounded stop-loss close; no switch to GTC for either (see §8's follow-up on when GTC actually becomes legal/relevant).
3. **On a no-fill, a take-profit close now waits for the next real tick instead of retrying internally; stop-loss keeps its unbounded retry loop and no price floor.** `close_position_at_price` makes exactly **one** attempt (no internal retry loop) — if it can't fill at `min_price` right now, it returns `Failed` immediately. `worker.rs::on_unwind_failed` now re-arms `ExitArm::PriceMonitor { tp_price }` (previously: a one-shot `TakeProfitAbandoned` latch that gave up for the rest of the cycle) so the very next qualifying `PolyTick` retries the close — safe because each attempt is price-bounded (can never fill worse than `tp_price`) and naturally rate-limited by real market ticks rather than an internal loop, which is what avoided both this incident's unbounded-slippage failure mode *and* the original DOGE incident's 284-attempts-in-9-seconds hammering failure mode. `close_position()` (stop-loss) is untouched.
   - **Caveat worth being explicit about:** this is a change to the retry *mechanism*, not just an added price. The old `close_position()` specifically retried `"balance: 0"` (entry BUY confirmed by the API but not yet spendable — the Polygon tx hasn't settled on-chain, typically ~1-2s) with an internal 1s sleep-and-retry. `close_position_at_price` has no such internal wait — if a take-profit fires within ~1-2s of entry (this incident's exact shape) and hits `"balance: 0"` on its one attempt, it now fails immediately and recovery depends on the *next real `PolyTick`* arriving with the price still qualifying, not a guaranteed 1-second internal wait. Usually equal or faster in an active market, but a genuine behavioral difference, not merely "same retries, now bounded."
4. **`UnwindWatcher` is wired up** (`trader/src/unwind.rs`, previously implemented/tested but never invoked). `bin/live.rs::main()` now spawns it at startup, authenticated with the live engine's own derived credentials (`LiveExecutionEngine::credentials()`, new getter) and the funder address, subscribed to the USER channel for all markets (empty `markets` list). Every incoming fill message is now logged with our own wall-clock receipt timestamp *before* dispatch, regardless of whether anything is `watch()`-ing that order — a passive, always-on real-time record of exchange-reported fills, independent of (and cross-checkable against) the request/response timestamps captured in point 5 below.
5. **Telegram + CSV latency instrumentation** (raised alongside the above, not originally part of the price-guard fix but implemented in the same pass): both the "Order placed" and "... order executed" Telegram messages now report **signal latency** (triggering tick's own timestamp → driver received it) and **process latency** (driver received it → order confirmed), in ms. The same four numbers (`entry_signal_latency_ms`, `entry_process_latency_ms`, `exit_signal_latency_ms`, `exit_process_latency_ms`) are new `TradeRecord` fields, logged as four new trailing CSV columns (`live_trades_*.csv`) and surfaced as two combined columns ("Entry Latency (ms)", "Exit Latency (ms)") in `trade_reconcile.py`'s Trade History table — full signal/process breakdown stays available in the raw CSV. `exit_*` is `0` when a position resolved by natural market close rather than an early exit order.

**Verification:** `cargo test` — 136 lib tests + 3 `live.rs` bin tests pass, including a rewritten `worker.rs` test (`failed_unwind_retries_close_on_next_qualifying_poly_tick`, replacing the old `failed_unwind_does_not_retrigger_close_on_next_poly_tick`) asserting the new retry-on-next-tick behavior, and new `execution.rs` tests for `close_position_at_price` and the order-kind chooser (§8). `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt --all --check` both clean.

## 8. Follow-up: order sizing (limit vs FAK) by trade size

Mid-fix, direction came in (verbatim in §9 Q&A) recalling that trade size is "a big factor" — below $5 for entry it must be a market order, uncertainty about whether a limit order is even allowed for a small exit — and asking for an automatic, tested choice between limit and FAK, since entry and exit might differ, with a request to research this externally.

**Research finding** (web search + the vendored SDK's own types, full citations in `trader/README.md`'s new "Order sizing: limit vs FAK" section): Polymarket enforces two independent, differently-denominated minimum sizes, not one $5 rule:
- A resting **GTC/GTD limit order must be for at least 5 shares** (share-denominated) — confirmed both via the CLOB orderbook response's own `min_order_size` field (vendored SDK, `clob::types::response`) and via `../btc_5mins/README.md` (the reference Python bot this Rust trader ports), which independently hit and documented the identical "hard 5-token minimum for all resting (GTC) SELL orders."
- A marketable **FAK/FOK order has only a $1 USDC notional floor** (docs.polymarket.com's `INVALID_ORDER_MIN_SIZE`; already hit and fixed in this repo, `incident_order_fail_2026-07-04.md`), no share-count floor at all.

These reconcile with the "$5" recollection: at this bot's typical 0.80–0.95 entry prices, 5 shares ≈ $4.50–5.00 notional — so "$5 minimum" is an accurate rule of thumb at these prices, even though the underlying exchange rule is share-denominated, not dollar-denominated. It isn't a separate third rule.

**What this means for entry vs. exit, concretely:**
- **Exit** already had exactly this GTC-vs-FAK branch (`worker.rs::on_order_filled`, `filled_shares >= 5.0` → attempt a resting GTC), just as an inline magic number rather than a named, tested rule — and it was already correct: at the current $1 stake (1.05–1.5 shares), it always takes the FAK/`PriceMonitor` path, which is exactly why this incident's exit had no GTC leg to fall back on. Centralized into `execution::choose_exit_order_kind(shares)` / `MIN_GTC_SHARES` / `MIN_MARKETABLE_USDC`, with unit tests pinning both sides of the 5-share boundary. No behavior change at the current $1 stake; this exact branch becomes reachable once shares cross 5 — i.e., once the stake is raised to $5+ as planned.
- **Entry** deliberately keeps its existing behavior — always a marketable FAK (`execution.rs::place`), regardless of stake size — documented as a strategy design choice, not a size limitation: reversal/high_prob entries react to a live price crossing a trigger band and need the current price immediately, and a resting GTC buy risks missing the entry window if price moves away before it fills. `../btc_5mins` makes the identical choice for entries. This wasn't a gap to fix; §7's price-guard fix and this centralization together are what's needed at both the current $1 stake and the planned $5+ stake — no entry-side order-type change was made or is proposed.

## 9. Plan (as executed)

1. `execution.rs`: add `close_position_at_price` (bounded, single-attempt FAK) alongside the unchanged `close_position`; add `LiveExecutionEngine::credentials()`.
2. `worker.rs`: `Action::ClosePosition` carries `limit_price`/`signal_ts`; `Action::PlaceBuy` carries `signal_ts`. Remove `ExitArm::TakeProfitAbandoned`; `on_unwind_failed` re-arms `PriceMonitor` instead. Thread entry/exit signal+process latency through `Event`s, `HoldingData`, and `TradeRecord`.
3. `types.rs`: four new `TradeRecord` latency fields.
4. `bin/live.rs`: capture wall-clock timestamps around `engine.place()`/`close_position(_at_price)()`; compute + report latencies on the two Telegram messages; branch on `limit_price` to choose the execution method; extend `CSV_HEADER`/`log_trade`/`append_csv_header_if_new` (generalized to pad any legacy row up to the current field count, not just one hardcoded prior generation); spawn `UnwindWatcher` at startup.
5. `unwind.rs`: log every USER-channel message with a receipt timestamp before dispatch.
6. `execution.rs`: `choose_exit_order_kind`/`MIN_GTC_SHARES`/`MIN_MARKETABLE_USDC`, tested; wired into `worker.rs::on_order_filled` in place of the inline `>= 5.0`.
7. `trade_reconcile.py`: `CSV_COLUMNS` extended; Trade History table gets two combined latency columns.
8. `README.md` (top-level): updated the latency-instrumentation gap bullet to reflect it's closed; new incident entry; new "Order sizing: limit vs FAK" section quoting `../btc_5mins/README.md` and citing the SDK/docs research.
9. This doc: §6-§8 above, this plan, and the Q&A log below.
10. `cargo build`/`test`/`clippy -D warnings`/`fmt --check` all clean before considering the pass done.

## 10. Q&A log

**Q (mid-fix, verbatim):** "1. minimum sell price for unwind is automatically the target unwind price , e.g. if take profit target is 0.03 and current price 0.9, then minimum is automatically 0.93, no new config is needed / 2. sounds good, use FAK / 3. when order cannot be filled af first, take-profit unwind wait for next tick, stop-loss no floor / 4. yes wire up unwind watcher for real time book record / also update telegram: 1. for order placed message, send out latency info: signal latency: received dt - signal ts, process latency: order confirmed dt - received dt, all in Ms / for order executed message, do the same / for daily recon script also add latency info into maybe columns of trade table to keep it simple yet complete / also explain what is that '3 failed internal attempts, unsettled balance etc', why would they happen here, what the impacts are, proposal for fix if there is any changes. if the answer to this question would be addressed by above fixes, just go head with changes, otherwise you can ask me for my review. / put everything in the same doc, also write a plan section for the above fixes and also doc my question and your answer"

**A:** Implemented exactly as directed — see §7 for points 1-4 and the Telegram/CSV latency additions, §6 for the "3 failed internal attempts" explanation (concluded it's fully addressed by the price-bound + wait-for-next-tick redesign, so proceeded without a separate review gate), §9 for the plan as executed, and this section for the log itself.

**Q (follow-up, verbatim):** "I remember now trade size is a big factor, below 5 dollar for entry it must be market order, I'm not try whether for exit limit is allowed, have a test and make the code automatically choose between limit and fak, maybe entry and exit is different, search internet too. your work earlier is not wasted as I will use 5 dollar above bet later. / also doc this in readme, maybe you want to move some info from readme in ../btc_5mins over"

**A:** Researched via web search + the vendored SDK's own types (see §8) — confirmed two separate size floors (5 shares for GTC, $1 notional for FAK), not one $5 rule; the "$5" recollection is that rule expressed in dollars at typical ~$0.90 prices. Centralized the existing (already-correct) exit-side GTC/FAK choice into a tested `execution::choose_exit_order_kind` function rather than changing its behavior; confirmed entry intentionally never uses GTC, as a strategy design choice rather than a gap, and left it unchanged. Documented all of this, plus the relevant `../btc_5mins/README.md` quote, in `trader/README.md`'s new "Order sizing: limit vs FAK" section (§8 has the same content in this doc for completeness).
