# Plan — port `unwind_time` (max-holding-time force-exit) from `btc_5mins/studies/unwind_safely` to the live trader

## 1. What `unwind_time` is

Source: `btc_5mins/bot/backtest_numba.py` (see `studies/unwind_safely/PLAN.md` for the sweep design).
A per-strategy, per-asset seconds value (`unwind_time_rev` for reversal, `unwind_time_hp` for
high_prob). While a position is open, checked **last** in the exit chain, after PnL-based
stop-loss, absolute stop-loss, and take-profit unwind all fail to fire on that tick:

```python
# max holding time (checked last — after unwind_pnl)
if unwind_time > 0.0 and pos_side != 0 and (ts - pos_entry_ts) >= unwind_time:
    ex = up if pos_side == 1 else dn
    shares = trade_size / pos_token
    out_pnl[ci, cyc] = shares * ex - trade_size
    out_outcome[ci, cyc] = 5  # TIMEOUT
    out_side[ci, cyc] = entry_side
    pos_side = 0
    # NOTE: cum_losses NOT incremented — TIMEOUT is neither a
    # win nor a stop-loss for halt purposes, same as _replay_cycle
```

Mechanically: force-close the position **at whatever the current market price is**, regardless of
whether that's a profit or a loss — `0.0` disables it (the same "0 = off" sentinel convention this
codebase already uses for `sl_reversal`/`sl_high_prob`). It does not count toward the halt
loss-streak either way. This is a pure **max-exposure-time cap**, not a PnL-based decision — it
answers "how long are we willing to sit in a position that hasn't resolved on its own," not "is
this a good price to exit at."

This is genuinely new behavior for the live trader — confirmed via `grep -rn unwind_time
trader/src/` returning nothing. Today a live position can stay open for the full remainder of the
cycle if neither stop-loss nor take-profit ever crosses.

## 2. Study findings (ETH — the only live `trade_assets` entry)

From `studies/unwind_safely/results/`:

- **`full_history_ETH_20260707_200431.json`** (best-by-PnL, unbounded history): `unwind_time_rev
  = 14.0s`, `unwind_time_hp = 28.0s` — but best-by-PnL here also picked `sl_pnl_rev = 0.50` at
  **17.3% win rate**, i.e. it's optimizing for a small number of huge wins on the reversal side,
  not a usable live profile.
- **`walk_forward_ETH_20260707_232318.json`** (4-step walk-forward, final calibration over the
  full 2026-05-26→2026-07-02 window): `unwind_time_rev = 30.0s`, `unwind_time_hp = 30.0s` — both
  at the **top of the tested range** (10–30s, step 2s).

The walk-forward report's own summary is explicit about why that second number should not be
taken at face value:

> **Same `unwind_time`-overfit pattern as BTC.** Every Cal-selected top-5 row across all 4 steps,
> both strategies, uses `unwind_time` at 22–30s (mostly the top of the range or very close to
> it)... Final calibration confirms: `unwind_time_rev=30s`, `unwind_time_hp=30s`, both
> near-disabled.

This is the **same grid-boundary-artifact pattern** already documented for `sl_pnl` in
`btc_5mins/studies/bt2/followup_sl_pnl_boundary_2026-07-07.md` (and which directly caused the
SOL/DOGE near-total-loss trades audited in `trader/doc/audit_sl_no_trigger_2026-07-07.md`): an
unconstrained-at-the-edge sweep always walks to whichever boundary of the tested range it's
allowed to reach, and a value sitting exactly on that boundary tells you "the range should have
been wider," not "this is the optimum." **The study never tested anything past 30s** — we have no
evidence about 40s, 60s, or 120s, only that within [10, 30], longer beat shorter every time.

**Important correction to the walk-forward doc's own "near-disabled" framing:** 30s is *not* close
to a no-op. Reversal's `reversal_start_time = 120` means most entries happen with 150–180s left in
a 300s cycle; a position still open 30s after entry (no stop, no take-profit yet) is common, not
rare. A live `unwind_time = 30` would force-close a non-trivial share of trades at market,
mid-cycle, that would otherwise have resolved via take-profit, stop-loss, or a natural cycle-close
WIN/LOSS. It's a real, fairly aggressive behavioral change — just one whose *magnitude* the current
sweep range can't actually validate as optimal.

## 3. Is this a good idea to implement?

**The mechanism: yes.** Unlike `sl_pnl`/`unwind_pnl`, this is not a price-based bet on where the
market goes — it's a hold-time circuit breaker, structurally independent of whether a PnL
threshold is even reachable. It's a direct, complementary answer to the SOL/DOGE audit finding:
those trades lost most of their value because `sl_pnl_rev = 0.80` was unreachable given the entry
price, so nothing forced an exit until the position had already bled out. A max-holding-time cap
would have force-closed those positions early regardless of whether the PnL stop could ever
mathematically trigger. This is real, additive risk control.

**The specific value: not yet validated, needs a decision.** Given the exact class of mistake
already made once this session (trusting a grid-boundary value for `sl_pnl` — see
`btc_5mins/studies/bt2/followup_sl_pnl_boundary_2026-07-07.md` — led to real losses before being
tightened in `trader/doc/audit_sl_no_trigger_2026-07-07.md`), I'm not comfortable silently picking
`30.0` and shipping it as if the sweep validated it. Three honest options:

| Option | What it means | Trade-off |
|---|---|---|
| **A. Ship disabled (`0.0`)** | Implement the mechanism end-to-end, land it in config with the field wired through, but default off | Zero behavioral change today; the feature exists and can be turned on later with a real value once a wider sweep (e.g. 10–120s) is run. Safest. |
| **B. Ship at `30.0`** | Use the walk-forward "final calibration" value as-is | Matches what the data *does* show within its tested range (longer > shorter, monotonically, every step) — even a boundary value is still directionally informative, unlike `sl_pnl` where the boundary masked catastrophic unreachability. Reasonable if you're comfortable treating it as "best of what we tested," not "validated optimum." |
| **C. Re-sweep with a wider range first, implement after** | Extend `UNWIND_TIMES` to e.g. 10–120s in `full_history_sweep.py`/`weekly_walk_forward.py`, rerun, then come back to this plan with a real (non-boundary) number | Most rigorous; delays getting the risk-control mechanism live at all. |

My recommendation is **B**: ship the mechanism now with `unwind_time_rev = unwind_time_hp = 30.0`
for ETH. Reasoning: (1) the boundary-artifact risk here is asymmetric-safe compared to `sl_pnl` —
a too-short `unwind_time` just exits positions earlier/more often, which is the same direction as
"more cautious," not the SOL/DOGE failure mode of "the stop can't fire at all, so the position rides
all the way down"; (2) 30s is well above `no_enter_when_time_left = 10`, so it doesn't fight
existing timing gates; (3) waiting on a full re-sweep (option C) leaves the SOL/DOGE-style tail
risk this is meant to backstop live in the meantime. But this is a real judgment call on a
real-money parameter, not a mechanical port — **flagging for explicit confirmation before writing
`30.0` into the live TOML** (options A/C are one-line-config-change-away alternatives once the
mechanism below is in place either way).

## 4. Implementation plan (mechanism — needed regardless of which option above is chosen)

### Config (`trader/src/config.rs`)
- `StrategyToml`: add `pub unwind_time_rev: HashMap<String, f64>` and `pub unwind_time_hp:
  HashMap<String, f64>` (same shape as `unwind_pnl_rev`/`unwind_pnl_hp`).
- `AssetParams`: add resolved `pub unwind_time_rev: f64` / `pub unwind_time_hp: f64`.
- `StrategyToml::resolve()`: add `unwind_time_rev: req(&self.unwind_time_rev, asset,
  "unwind_time_rev")?` and the `_hp` counterpart, alongside the existing `unwind_pnl_*` lines.
- Existing `config.rs` unit tests that build an inline `StrategyToml`/TOML string will need
  `[unwind_time_rev]`/`[unwind_time_hp]` sections added or they'll fail `req()`'s missing-key error.

### Types (`trader/src/types.rs`)
- `Outcome`: add `Timeout` variant. `as_str()` → `"TIMEOUT"`.
- `is_loss_for_halt()`: no change needed — `matches!(self, Outcome::Loss | Outcome::StopLoss)`
  already excludes any variant not listed, so `Timeout` is excluded by construction, matching the
  backtest's "cum_losses NOT incremented" comment exactly. Worth a one-line comment noting this is
  deliberate, not an oversight.

### Worker state machine (`trader/src/worker.rs`)
- `CloseReason`: add `Timeout` variant.
- New `WorkerState::TimingOut(HoldingData)`, parallel to the existing `StopExiting(HoldingData)` —
  same `HoldingData` payload, same "FAK SELL in flight, unbounded" mechanics, distinct so the
  eventual `Outcome` and Telegram copy can differ. Touches the existing exhaustive matches:
  `PersistedWorkerState` (add `TimingOut(HoldingData)` + its two serialize/deserialize arms),
  `is_open()`, and the "current open position" accessor (`Holding(h) | Unwinding(h) |
  StopExiting(h) => Some(h.clone())`) — extend each to include `TimingOut(h)`. The compiler's
  exhaustiveness check will catch any match arm I miss; that's the point of using a new variant
  instead of overloading `StopExiting`.
- `Worker::common()`: add an `unwind_time: f64` parameter, store as `self.unwind_time` (mirrors
  `sl`/`sl_pnl`/`unwind_pnl` already there). `new_reversal`/`new_high_prob` pass
  `p.unwind_time_rev`/`p.unwind_time_hp`.
- `on_poly`: after the existing take-profit check, add the timeout check — only reached if neither
  stop-loss nor take-profit fired this tick, matching the backtest's "checked last" ordering:
  ```rust
  if self.unwind_time > 0.0 && (tick.ts - h.entry_ts) >= self.unwind_time && h.shares >= MIN_SELLABLE_SHARES {
      self.state = WorkerState::TimingOut(h.clone());
      return vec![Action::ClosePosition { shares: h.shares, reason: CloseReason::Timeout, limit_price: None, signal_ts: tick.ts }, Action::Persist];
  }
  ```
  `limit_price: None` — unbounded FAK, same as stop-loss, because "close at whatever the market
  price is" is exactly the backtest semantics (`ex = up if pos_side == 1 else dn`, no floor).
  Confirmed `tick.ts` (`PolyTick`, from `now_secs_f64()` at receipt in `marketdata.rs:192`) and
  `h.entry_ts` (from `self.last_binance_ts()`, itself set from `BinanceTick.ts` —
  `marketdata.rs:126`, also `now_secs_f64()` at receipt) are the same wall-clock domain, so the
  delta is valid without any unit conversion.
- New `Event::TimeoutSellFilled { sold_shares, exit_price, signal_latency_ms, process_latency_ms }`
  / `Event::TimeoutSellFailed { error }`, wired into `step()`'s dispatch alongside the existing
  `StopSellFilled`/`StopSellFailed` arms.
- New `on_timeout_sell_filled`/`on_timeout_sell_failed`, copied from
  `on_stop_sell_filled`/`on_stop_sell_failed` with `WorkerState::TimingOut` in place of
  `StopExiting` and `Outcome::Timeout` in place of `Outcome::StopLoss` passed to
  `finalize_or_hold_residual`. Failure case falls back to `WorkerState::Holding(h)` exactly like
  `on_stop_sell_failed` does — `on_poly`'s timeout condition (`tick.ts - h.entry_ts >=
  unwind_time`) stays true (more true, as time passes), so the very next `PolyTick` naturally
  re-fires the close attempt with no separate retry-counter logic needed, mirroring how
  stop-loss's own retry already works.
- `StopLossVerdict`'s `EnrichOnly` handling (currently gated on `record.outcome ==
  Outcome::StopLoss`) is left StopLoss-only, not extended to `Timeout` — a timeout exit's "was
  this good or costly" verdict is a separate, lower-priority nice-to-have (the position could have
  timed out into either a would-have-won or would-have-lost continuation) and isn't needed for the
  core risk-control behavior. Can be added later as its own small change if wanted.

### Driver (`trader/src/bin/live.rs`)
- `AssetSlot`: add `timeouts: u32` counter (parallel to `wins`/`losses`/`stoplosses`/`unwinds`) and
  `timeout_notified: bool` (parallel to `sl_notified`, reset at the same cycle-open site
  `sl_notified` is reset at, `live.rs:1062`).
- `Action::ClosePosition` dispatch: extend the `label` match (`"STOP LOSS"` / `"TAKE PROFIT"`) with
  a third arm, `CloseReason::Timeout => "TIME LIMIT"`; extend the `(matched, reason)` → `Event`
  match with `(true, CloseReason::Timeout) => Event::TimeoutSellFilled { .. }` / `(false,
  CloseReason::Timeout) => Event::TimeoutSellFailed { .. }`. Add a first-trigger notify block for
  `CloseReason::Timeout` mirroring the existing `sl_notified`-gated stop-loss alert (same
  spam-guard reasoning: `on_poly` can refire `ClosePosition{Timeout}` every tick until the position
  clears, same as stop-loss).
- `Action::LogTrade` handler: extend `slot.cycle_trades` eligibility list, the outcome→counter
  match (`Outcome::Timeout => slot.timeouts += 1`), and the icon match — since a timeout can close
  at a profit or a loss (it's not directionally biased like stop-loss/take-profit), branch the icon
  on `rec.pnl` sign for the `Timeout` case specifically rather than a fixed icon:
  ```rust
  let icon = match rec.outcome {
      Outcome::Win | Outcome::Unwind => "✅",
      Outcome::Loss | Outcome::StopLoss => "❌",
      Outcome::Timeout if rec.pnl >= 0.0 => "⏱️✅",
      Outcome::Timeout => "⏱️❌",
  };
  ```
- `Action::LogTradeCorrection` handler: extend the two `match previous_outcome` / `match
  record.outcome` catch-alls (`StopLoss | Unwind => {}`) to `StopLoss | Unwind | Timeout => {}` —
  `Confirming` only ever holds `Win`/`Loss`, so this stays a no-op branch, just needs the arm added
  for exhaustiveness.

### Telegram `/status` (`trader/src/telegram/commands.rs`)
- Add `"unwind_time_rev"` / `"unwind_time_hp"` to the params list already printing
  `unwind_pnl_rev`/`sl_pnl_rev`/`unwind_pnl_hp`/`sl_pnl_hp` (line ~20-23) — this is the exact
  visibility gap that let the `sl_pnl` stale-config incident go unnoticed for a full deploy cycle;
  a new exit parameter should be visible from day one, not added to `/status` as an afterthought.

### Config file (`trader/config/strategy_20260705.toml`)
- Add `[unwind_time_rev]` / `[unwind_time_hp]` sections, `default = 0.0` (disabled) or `30.0`
  (once decided per §3) — in-place edit with an updated `ts`/`source` meta comment, matching how
  every other exit-parameter change landed this session (no new dated file).

## 5. Tests

**`trader/src/config.rs`** — resolve test asserting `unwind_time_rev`/`unwind_time_hp` round-trip
through `default` and an asset-specific override, same shape as the existing `sl_pnl_rev` test.

**`trader/src/worker.rs`**:
- `timeout_force_closes_after_unwind_time_elapsed_with_no_other_exit` — position open, no
  stop/take-profit condition met, `PolyTick` at `entry_ts + unwind_time` → `TimingOut` state,
  `ClosePosition{reason: Timeout, limit_price: None}` action.
- `timeout_does_not_fire_before_threshold_elapsed` — same setup, tick at `entry_ts + unwind_time -
  1` → no action, still `Holding`.
- `timeout_disabled_when_unwind_time_zero` — `unwind_time = 0.0` (the sentinel/default) → never
  fires regardless of elapsed time, matching the backtest's `unwind_time > 0.0` gate.
- `stoploss_takes_priority_over_timeout_on_same_tick` — construct a tick where both the stop-loss
  price condition and the elapsed-time condition are simultaneously true → asserts `StopExiting`,
  not `TimingOut` (matches the backtest's fixed check order: stop-loss, then take-profit, then
  timeout last).
- `timeout_exit_outcome_excluded_from_halt_loss_streak` — feed an `Outcome::Timeout` `TradeRecord`
  (constructed via the existing `halt_test_record`-style helper) into `HaltTracker::record_trade`
  and assert the loss-streak counter does not advance, regardless of the record's `pnl` sign.
- `timeout_sell_failure_falls_back_to_holding_and_refires_next_tick` — mirrors the existing
  stop-loss failure test: `on_timeout_sell_failed` → `Holding`, then a subsequent `PolyTick` past
  the threshold re-triggers `ClosePosition{Timeout}`.
- Extend the existing `PersistedState` round-trip test (if one covers `StopExiting`) to also cover
  `TimingOut`, confirming crash-recovery serialization doesn't drop the new variant.

## 6. Local testing before deploy

- `cargo build` (compiles clean, exhaustiveness-checks every new match arm).
- `cargo test` — full existing suite plus the new tests above.
- `cargo clippy --all-targets --all-features -- -D warnings` — this repo's CI gate, cleaned to zero
  as of `trader/doc/`'s clippy incident entry; must stay clean.
- `cargo fmt --all --check`.

## 7. Deploy

Once implemented and tested locally: `./scripts/deploy_trader.sh` (full build + binary rsync +
config sync + restart — this is a code change, not a config-only change, so the new
`--update-config` fast path from this session doesn't apply here; the binary itself needs
rebuilding). Verify post-deploy via Telegram `/status` that `unwind_time_rev`/`unwind_time_hp` show
the intended value (§4's `/status` change makes this directly checkable, unlike the earlier
`sl_pnl` incident where the value wasn't surfaced anywhere).

## 8. Documentation

`README.md` — new entry under the exit-parameters/config section (near where `sl_pnl`/`unwind_pnl`
are already documented) describing `unwind_time_{rev,hp}`, the `0.0` = disabled convention, the
grid-boundary caveat from §2-3, and a link back to this plan doc and
`studies/unwind_safely/results/walk_forward_ETH_20260707_232318.md`.
