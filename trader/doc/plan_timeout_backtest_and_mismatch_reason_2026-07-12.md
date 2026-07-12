# Plan — implement `unwind_time`/TIMEOUT in `machine.rs`, and a mismatch-reason classifier in `trade_reconcile.py`

Follow-up to `trader/doc/incident_bt_vs_live_discrepancy_2026-07-12.md`'s two proposed fixes.
Both close gaps identified there: (1) the backtest replay engine structurally cannot reproduce a
live `TIMEOUT` exit, guaranteeing an `OUTCOME DIFF`/`BT DID NOT FIRE` for every such trade; (2) the
recon report states *that* BT and live disagree but never *why*, even when the why (a real halt) is
fully reconstructable from `live.log`.

## Part 1 — `unwind_time`/`Outcome::Timeout` in `machine.rs`

### Why this is real, not hypothetical
`config/strategy_20260709.toml` (the live config right now) has `unwind_time_rev` = 26.0 default
(ETH=28.0, DOGE=30.0) and `unwind_time_hp` = 30.0 — **enabled for every asset trading live today.**
`AssetParams.unwind_time_rev`/`.unwind_time_hp` are already resolved by `config.rs` (confirmed:
`resolve()` lines 179/186, with existing round-trip tests). `Outcome::Timeout` already exists in
`types.rs`. The only thing missing is `machine.rs` (the backtest replay engine) actually reading
and acting on those two already-resolved fields — right now it silently ignores them
(`Machine` has no `unwind_time` field at all).

### Design
`machine.rs`'s `on_poly` is a synchronous exit-chain check (1. PnL-SL, 2. absolute SL, 3.
take-profit unwind, in that order, each an early `return self.emit(...)`) — unlike `worker.rs`,
backtest fills are instantaneous (sim venue), so there's no `TimingOut`/in-flight state, no
`Action::ClosePosition`, no fill/fail event pair to add. This makes the port much smaller than the
original `worker.rs` port (`plan_unwind_time_2026-07-08.md` §4) needed — no new `TradeState`
variant, no new `PersistedState` arm, no driver/Telegram changes (those are `live.rs`-only
concerns machine.rs doesn't have).

**Machine struct**: add `unwind_time: f64`, populated in `new_reversal`/`new_high_prob` from
`p.unwind_time_rev`/`p.unwind_time_hp` (same pattern as the existing `sl`/`sl_pnl`/`unwind_pnl`
fields three lines above where they're already set).

**New private helper**, mirroring `emit`'s ownership shape:
```rust
/// Force-close a held position once it's been open >= `unwind_time`, at
/// whatever the current market price is (win or lose) — checked last, after
/// every other exit condition (matches worker.rs's live ordering and the
/// original Python `_replay_cycle` order; see
/// trader/doc/plan_unwind_time_2026-07-08.md). `now`/`exit_price` are the
/// caller's own tick data — the position can time out on either a poly tick
/// (current tick's own price) or a binance-only tick (cached latest poly
/// price for the held side), since unwind_time is a pure elapsed-time cap,
/// not conditioned on a poly crossing.
fn check_timeout(&mut self, h: &HoldingData, now: f64, exit_price: f64) -> Option<TradeRecord> {
    if self.unwind_time <= 0.0 || (now - h.entry_ts) < self.unwind_time {
        return None;
    }
    let shares = self.trade_size / h.token_price;
    let pnl = round4(shares * exit_price - self.trade_size);
    self.emit(h.clone(), Outcome::Timeout, exit_price, pnl)
}
```
`pnl`/`exit_price` use the plain WIN/LOSS-style formula (`shares * exit_price - trade_size`, no
floor/cap) — matches the Python source exactly (`ex = up if pos_side==1 else dn; ... out_pnl =
shares*ex - trade_size`) and `worker.rs`'s `CloseReason::Timeout` semantics ("close at whatever the
market price is, win or lose").

**`on_poly`**: after the existing 3 checks, add a 4th using the tick's own already-computed
`exit_price` local:
```rust
// 4. Max holding time (checked last)
if let Some(rec) = self.check_timeout(&h, tick.ts, exit_price) {
    return Some(rec);
}
None
```

**`on_binance`**: currently `pub fn on_binance(&mut self, tick: BinanceTick)` (no return value) —
change to `-> Option<TradeRecord>`. A position can time out between poly ticks (unwind_time is
pure elapsed-time, no poly crossing needed), so this must also check it, using
`self.latest_poly.up()`/`.dn()` (the cached last-known poly price for the held side — always
populated by the time a position exists, since entry itself depends on poly-derived signals):
```rust
pub fn on_binance(&mut self, tick: BinanceTick) -> Option<TradeRecord> {
    self.delta_pct.on_binance(tick);
    self.latest_binance.on_binance(tick);
    self.last_binance = tick.price;

    if let TradeState::Holding(h) = &self.state {
        let h = h.clone();
        let exit_price = if h.side == Side::Up { self.latest_poly.up() } else { self.latest_poly.dn() };
        return self.check_timeout(&h, tick.ts, exit_price);
    }
    self.try_enter(tick.ts);
    None
}
```
(`try_enter` is a no-op when not `Watching` anyway, so branching on `Holding` first instead of
calling `try_enter` unconditionally is behavior-preserving, not just an optimization.)

**Call site** (`backtest.rs`'s `run_cycle`, the `MergedTick::Binance` arm): currently discards
`on_binance`'s (void) return; update to collect the new `Option<TradeRecord>` the same way the
`MergedTick::Poly` arm already does:
```rust
MergedTick::Binance(bt) => {
    for m in machines.iter_mut() {
        if let Some(rec) = m.on_binance(bt) {
            completed.push(rec);
        }
    }
}
```

**`HaltTracker::record_trade`**: no change needed — already gated on `outcome.is_loss_for_halt()`,
which (per `types.rs`) only matches `Loss | StopLoss`; `Timeout` is excluded by construction,
matching the Python "`cum_losses NOT incremented`" comment and `worker.rs`'s existing behavior.
Worth a regression test asserting this explicitly for the backtest's own `HaltTracker` (mirrors
`plan_unwind_time_2026-07-08.md §5`'s `timeout_exit_outcome_excluded_from_halt_loss_streak`, which
only covered `worker.rs`, not `backtest.rs`).

### Tests (`machine.rs`)
- `timeout_force_closes_after_unwind_time_elapsed_on_poly_tick` — Holding, no SL/unwind condition
  met, `on_poly` at `entry_ts + unwind_time` → `Some(TradeRecord)` with `Outcome::Timeout`, state
  back to `Watching`.
- `timeout_force_closes_on_binance_only_tick` — same setup, but the threshold-crossing tick is a
  `BinanceTick`, not a `PolyTick` — confirms the `on_binance` path fires too, using the cached
  `latest_poly` price as `exit_price`.
- `timeout_does_not_fire_before_threshold_elapsed` — tick at `entry_ts + unwind_time - 1` → `None`,
  still `Holding`.
- `timeout_disabled_when_unwind_time_zero` — `unwind_time = 0.0` (sentinel) → never fires
  regardless of elapsed time.
- `stoploss_takes_priority_over_timeout_on_same_tick` — construct a tick where both the SL price
  condition and the elapsed-time condition are simultaneously true → asserts `Outcome::StopLoss`,
  confirming the existing 1-2-3-then-4 check order is preserved, not just documented in a comment.
- `timeout_pnl_can_be_positive_or_negative` — two variants (price above/below entry) both use the
  plain `shares*exit_price - trade_size` formula, no floor/cap — distinguishes this from
  SL/unwind's bounded-by-construction pnl.
- `timeout_excluded_from_halt_loss_streak` — feed an `Outcome::Timeout` `TradeRecord` into
  `HaltTracker::record_trade` (the `backtest.rs`-local tracker, not `worker.rs`'s) and assert the
  loss-streak counter does not advance.
- `format_csv`/`format_table` in `backtest.rs`: no change needed (both already handle
  `Outcome::Timeout` — `format_csv` via `t.outcome.as_str()`, `format_table` via its existing
  `timeouts` counter arm, which the doc comment there already flagged as "never fires today").
  Existing `csv_row_matches_trade_fields`-style tests stay valid; no new test needed there since
  the format code path itself isn't changing, only what feeds into it.

### Non-goals
No config, `worker.rs`, `live.rs`, or Telegram changes — those are already live and correct; this
is purely making the replay engine consistent with logic that already ships live. No behavioral
change to live trading at all.

## Part 2 — `classify_mismatch_reason()` in `trade_reconcile.py`

### Scope adjustment now that Part 1 lands
`incident_bt_vs_live_discrepancy_2026-07-12.md`'s proposal had a `TIMEOUT ⇒ "structural"` rule as
priority 1. Once Part 1 ships, that rule would be **actively misleading** — the backtest *can* now
model `TIMEOUT`, so hardcoding "this is always a known engine gap" would misreport genuine
mismatches as expected/benign. Dropped from this plan. If a `TIMEOUT` row still shows a mismatch
after Part 1 (tick-timing edge case, price-series difference, etc.), it should fall through to
the same classification as any other unexplained row — that's a real signal, not noise to
suppress.

### Design
New function, called once per report run (not per-row) to build shared context, then applied per
mismatching row:

```python
def build_halt_windows(live_log_path: Path, window_start: datetime, window_end: datetime) -> list:
    """[(start_ts, end_ts, reason_label), ...] — reconstructed from live.log the same way
    this investigation's one-off script did: track the most recent heartbeat's
    slug+T-Ns (real time = slug_cycle_ts(slug) + 300 - T), attach that timestamp
    to the next halt/resume/gamma-halt/balance-drawdown line encountered.
    A halt with no matching resume before window_end stays open through
    window_end (e.g. a halt still active at report-generation time)."""

def classify_mismatch_reason(
    row: dict, halt_windows: list, config_change_ts: Optional[float], tick_count: Optional[int],
) -> str:
    """Priority order — first match wins:
    1. cycle_ts falls inside a halt_windows entry -> "live halted: {reason_label} {HH:MM}-{HH:MM}"
    2. config_change_ts is not None and inside [cycle_ts, window_end] (config changed *after*
       this cycle but *within* the report window) -> "config changed {HH:MM} same-window (verify
       params before trusting this row)"
    3. tick_count is not None and < SPARSE_TICK_THRESHOLD -> f"sparse tick data ({tick_count}
       ticks this cycle)"
    4. else -> "unexplained"
    """
```

`build_halt_windows` reuses the exact regexes and heartbeat-interpolation technique from this
investigation's `/tmp/.../reconstruct_halt_timeline.py` scratch script, promoted into the shipped
module (previously a one-off, not committed). Halt *sources* distinguished by which pattern
matched (`manual /halt`, `balance drawdown >25%`, `balance-decrease (asset+strategy)`,
`Gamma-unresolved halt`) so the reason label says which, not just "halted."

`config_change_ts`: `git log -1 --format=%ct -- <resolved config path>` (subprocess, best-effort —
wrapped in the same try/except-degrade-to-None pattern as every other optional enrichment in this
file; a missing `git` binary or non-repo checkout must not break the report).

`tick_count`: `len(underlying_prices.get(slug, []))` — already available from
`load_underlying_price_series` (added for Entry Δ%/Cycle Δ%), no new data source needed.
`SPARSE_TICK_THRESHOLD = 60` (a full 5-min cycle at the ~4Hz binance sample rate this project
otherwise sees is ~1200 ticks; 60 is a conservative floor well below any normal cycle, chosen to
only flag genuinely gappy cycles, not just quiet ones).

**Wiring**: add a `reason` field to the row dicts built in `build_live_vs_bt` (for `SIDE DIFF`,
`OUTCOME DIFF`, `BT DID NOT FIRE` statuses only — `MATCH` and `NO PRICE DATA` rows get `"—"`,
since a `MATCH` needs no explanation and `NO PRICE DATA` already is one) and `build_bt_vs_live`
(every row, since every row there is by definition a mismatch). New **Reason** column appended to
both rendered tables (last column, after `Status`/`BT PnL`).

### Tests (`test_trade_reconcile.py`)
- `BuildHaltWindowsTests`: parses a small synthetic `live.log` fixture with interleaved heartbeat
  + halt + resume lines, asserts the reconstructed window boundaries and reason labels; a halt
  line with no matching heartbeat nearby (malformed/edge-of-file) doesn't crash, just contributes
  no window; an unresumed halt at file end stays open through `window_end`.
- `ClassifyMismatchReasonTests`: one test per priority-order branch (halt-window match, config-
  drift match, sparse-tick match, unexplained fallback), plus a priority test confirming halt beats
  config-drift beats sparse-data when more than one condition is simultaneously true for the same
  row (deterministic precedence, not whichever happens to be checked last).
- `BuildLiveVsBtTests`/`BuildBtVsLiveTests`: extend existing tests to assert `reason` is present
  and `"—"` for `MATCH` rows specifically (regression guard against accidentally reason-tagging
  matches).

### Non-goals
Not attempting full historical config-diff attribution (which specific TOML keys changed) in this
pass — the "config changed, verify params" label is intentionally a caveat/pointer, not a claim
that the change *caused* the mismatch (per `incident_bt_vs_live_discrepancy_2026-07-12.md`'s own
finding that most config edits don't touch entry-decision params). Not modeling balance-drawdown
halts inside `backtest.rs` itself — the halt-window approach classifies after the fact from
`live.log`, it doesn't make the backtest engine aware of balance state, which stays the
recommended lighter-weight approach per that doc's "Longer-term, architectural" section.

## Plan review — is this a good plan?

**Part 1: yes, low-risk, ship it.** Small, additive, mirrors an already-live, already-tested
mechanism (`worker.rs`'s), touches no config/live-trading code, and every new code path is
exercised by the new tests before it ships. The only judgment call (`SPARSE_TICK_THRESHOLD`,
`60`) is in Part 2, not Part 1.

**Part 2: yes, with the scope adjustment above already folded in.** It's investigation-grade
(labels, not hard verdicts) by design — matches the honesty goal from
`incident_bt_vs_live_discrepancy_2026-07-12.md`'s "unexplained — needs manual review" category,
which stays the fallback here too rather than being engineered away. Main risk is the
heartbeat-interpolation timestamp reconstruction being off by more than the ~30s heartbeat
interval in some edge case (e.g. a long gap in heartbeat lines around a halt event) — acceptable
for a ±few-minute classification against 5-minute cycles, and the existing investigation already
validated it against 20+ real events at ±1-3 log lines of precision.

Proceeding to implement both parts as planned.
