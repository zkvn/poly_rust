# Incident — ETH TIMEOUT losses ran overnight without ever tripping the loss-streak halt, 2026-07-14

## 1. What happened

`live_trades_eth_reversal.csv` / `live_state_eth_reversal.json` show a run of ETH `reversal`
`TIMEOUT` exits (the `unwind_time_rev` max-holding-time force-close) overnight, several of them
landing at a real loss, with `halt_losses: 0` the whole time — `halt_rev` (currently `1`, tightened
from `2` on 2026-07-13, see `strategy_20260713.toml`) never engaged for any of them:

```
1783942419.0693376,...,STOPLOSS,-0.5967,...   (19:33:39 HKT — this one DID halt, see below)
...HALT RESET | 02:00:00 | reversal...
1783970617.4480956,...,DOWN,...,TIMEOUT,-0.2967,...  (05:50 HKT)
1783976024.8413901,...,UP,...,TIMEOUT,-0.0877,...    (06:20 HKT)
1783985635.3212087,...,DOWN,...,TIMEOUT,-0.2127,...  (07:13 HKT)
1783985972.1325634,...,DOWN,...,TIMEOUT,-0.1855,...  (07:19 HKT)
1783987783.1363783,...,UP,...,TIMEOUT,-0.3923,...    (08:09 HKT)
```

Five losing TIMEOUT exits between the 02:00 HKT daily halt reset and 08:09 HKT, totaling
**-$1.075** in realized losses, none of which incremented the per-strategy loss-streak counter —
`halt_losses` sat at `0` the entire time and no `ETH HALTED | reversal` Telegram alert fired for
any of them (the one alert in the window, 19:33:39, was a genuine `STOPLOSS`, which *does* count
and correctly halted/reset on schedule).

Note the prior `STOPLOSS` at 19:33:39 **did** correctly trip the halt (confirmed via
`live.log`'s `🟡 ETH HALTED | 19:33:39 | reversal` line) — this incident is specifically about
`TIMEOUT`, not the halt mechanism being broken outright.

## 2. Root cause

`Outcome::is_loss_for_halt()` (`trader/src/types.rs`) only matched `Loss | StopLoss`:

```rust
pub fn is_loss_for_halt(self) -> bool {
    matches!(self, Outcome::Loss | Outcome::StopLoss)
}
```

`Timeout` (`unwind_time_rev`/`unwind_time_hp`'s max-holding-time force-close) was blanket-excluded
regardless of its `pnl` sign. The reasoning at the time (`plan_unwind_time_2026-07-08.md`) was
that `TIMEOUT`, unlike `StopLoss`, isn't a "signal quality failure" — it's a pure elapsed-time cap
that can land on either side of zero, so it was treated the same as `Unwind` (never counts). That
reasoning conflated two different things: `Unwind` is directionally *fixed* to a gain by
construction (a take-profit exit, bounded by `tp_price` since the 2026-07-06 SOL incident's fix —
see `incident_sol_unwind_but_loss_2026-07-06.md`), so excluding it unconditionally is correct.
`TIMEOUT` has no such directional guarantee — its own doc comment says exactly this
("may land at a profit or a loss") — so excluding it *unconditionally*, rather than gating it on
`pnl < 0.0`, discarded real losses from the halt calculation. This was in fact already covered by
an existing test (`halt_tracker_record_trade_ignores_non_loss_and_other_strategy`, now split/
renamed — see §4) that explicitly asserted a losing-pnl `TIMEOUT` must *not* halt, confirming this
was deliberate, tested behavior — just a wrong call that hadn't yet produced a big enough loss
streak to notice.

`HaltTracker::record_trade`/`correct_trade` (`trader/src/backtest.rs`) both call
`outcome.is_loss_for_halt()` with no `pnl` argument, so the gap applied identically to the live
trader (`worker.rs`'s `Worker::halt`) and the Rust backtest (`backtest.rs::run_backtest`) — they
share the same `HaltTracker`/`Outcome` code, so whatever the live process actually experienced
overnight, a backtest run over the same data would have reproduced the identical gap (see §5 for
the check performed).

## 3. Why it wasn't a bug over the four TIMEOUT tests written to date

The four prior `TIMEOUT` tests in `machine.rs`/`worker.rs` (`timeout_force_closes_after_unwind_time_elapsed_*`,
etc.) all exercise the TIMEOUT *emission* path (`Machine::check_timeout`, `Worker::on_timeout_sell_filled`)
in isolation — none of them drive a `HaltTracker` afterward, so none of them could have caught this. The
one test that *did* touch `HaltTracker` + `TIMEOUT` together
(`halt_tracker_record_trade_ignores_non_loss_and_other_strategy`) explicitly asserted the
gap's exact behavior as correct, rather than exposing it as a bug — a deliberate design choice, that,
per §2, didn't distinguish "excluded because directionally fixed" (right, for `Unwind`) from
"excluded because pnl-ambiguous, so gate on pnl" (wrong, for `Timeout`).

## 4. Fix

`Outcome::is_loss_for_halt` now takes the trade's `pnl` and gates `Timeout` on its sign:

```rust
pub fn is_loss_for_halt(self, pnl: f64) -> bool {
    match self {
        Outcome::Loss | Outcome::StopLoss => true,
        Outcome::Timeout => pnl < 0.0,
        Outcome::Unwind | Outcome::Win => false,
    }
}
```

- `HaltTracker::record_trade` (`backtest.rs`) now calls `rec.outcome.is_loss_for_halt(rec.pnl)` —
  used identically by both the live trader (`worker.rs`'s `finalize_or_hold_residual`, the shared
  tail for `StopLoss`/`Unwind`/`Timeout` exits) and the Rust backtest (`run_backtest`), so both are
  fixed by the same change — no separate backtest-only fix was needed (see §6 for the "does the
  backtest independently need a fix" question this directly answers: no, it shares `HaltTracker`
  with the live path, so it was reproducing the identical gap, and is now covered by the identical fix).
- `HaltTracker::correct_trade` (used by `worker.rs::on_api_result` to undo/apply a loss-count delta
  when a provisional `Confirming` outcome is later Gamma-corrected) now takes both the previous and
  corrected `pnl` alongside their outcomes, for the same reason — even though in practice
  `correct_trade`'s only live call site only ever sees `Win`/`Loss` (the `Confirming` state is
  reached solely by held-to-resolution trades, never `StopLoss`/`Unwind`/`Timeout`), the function
  itself is generic over `Outcome` and `pnl`-gating had to be threaded through consistently rather
  than left silently wrong for a `Timeout` correction that could theoretically reach it later.
- `machine.rs::check_timeout`'s doc comment (referencing the old unconditional exclusion) updated
  to match.

## 5. Backtest check (did it already count halt from unwind loss properly?)

No — see §2/§4. `trader/src/backtest.rs::run_backtest` drives the *same* `HaltTracker` and
`Outcome::is_loss_for_halt` as the live trader, so it had the identical gap: a losing `TIMEOUT` in
a backtest replay never incremented `losses_rev`/`losses_hp` either, meaning a backtest run over
last night's exact ETH data would **not** have reproduced last night's real (correct) 19:33:39
`STOPLOSS` halt engaging as expected but would have similarly failed to halt on the subsequent
`TIMEOUT` loss streak — i.e. the backtest and live behavior were consistent with each other, just
consistently wrong in the same way. Fixed by the same `is_loss_for_halt`/`record_trade` change,
since both paths share this code; no backtest-specific patch was required.

## 6. Sibling project (`../btc_5mins`) — checked only, not fixed (out of scope for `poly_rust`)

Per explicit direction, only checked, not modified. `../btc_5mins/bot/backtest.py::_replay_all`
(driven by `../btc_5mins/scripts/bt2.py`) has the **identical** gap, at line ~1490:

```python
for t in trades:
    if t["outcome"] in ("LOSS", "STOPLOSS"):
        if t["strategy"] == "high_prob":
            losses_hp += 1
        else:
            losses_rev += 1
```

`UNWIND` and `TIMEOUT` are both excluded unconditionally, same as this repo's now-fixed Rust code
— a losing `TIMEOUT` in a `bt2.py` sweep never counts toward `losses_rev`/`losses_hp` either. This
is presumably the origin of the Rust port's identical logic (this Rust trader ports `../btc_5mins`,
per `trader/README.md`'s existing cross-references). Not fixed here — flagged for the user's own
attention if `btc_5mins`'s bt2 sweeps should also be corrected.

## 7. Verification

- New/updated tests in `trader/src/backtest.rs`:
  - `halt_tracker_record_trade_ignores_non_loss_and_other_strategy` — narrowed to the parts still
    true (`Win`/`Unwind`/winning-`Timeout`/other-strategy `Loss` never halt).
  - `halt_tracker_record_trade_counts_losing_timeout_only` (new) — a losing-pnl `Timeout` now
    counts toward the streak and can trip the halt, same as `Loss`/`StopLoss`; a winning-pnl one
    still doesn't.
  - `halt_tracker_correct_trade_gates_timeout_by_pnl` (new) — `correct_trade` re-pricing a
    `Timeout` from a loss to a win clears a halt it tripped, mirroring `record_trade`'s gating.
  - `halt_tracker_correct_trade_adjusts_losses_by_the_right_delta` — updated call sites for the new
    `correct_trade(outcome, pnl, outcome, pnl)` signature (behavior unchanged, `Win`/`Loss` don't
    depend on `pnl`).
- `cargo test` (trader crate): 186 lib tests pass, including all of the above. (4 pre-existing,
  unrelated failures in `config`/`config_log` tests — config-drift against `strategy_20260713.toml`,
  not caused by or related to this change; flagged in README's `## TODO`, not fixed here.)
- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `cargo fmt --all --check` clean.
