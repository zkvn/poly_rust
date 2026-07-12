# Incident: ETH high_prob halted on a phantom second loss (2026-07-10)

## Summary

At 22:24:55 HKT on 2026-07-09, ETH `high_prob` hit its stop-loss and was
immediately halted (`halt_prob = 2`, "2 consecutive losses"). Only one real
loss had occurred in the session — the halt counter had been silently
overcounted by 1 more than two hours earlier and never corrected.

## Timeline (all times HKT, 2026-07-09; session boundary is `halt_reset_hour_hp = 8`)

| ts (epoch) | time | event |
|---|---|---|
| 1783584300 | 16:05:00 | Cycle `eth-updown-5m-1783584000` closes; natural-resolution guess is **LOSS** (`pnl -1.0042`). `HaltTracker::record_trade` runs → `losses: 0 → 1`. Worker enters `Confirming(Loss)`. |
| 1783584413 | 16:06:53 | Gamma `ApiResult` arrives and disagrees: the position actually **WON**. `on_api_result` flips the record and emits `LogTradeCorrection` (Loss → Win, pnl `-1.0042 → +0.0596`) — logged as a second CSV row for the same cycle. **`self.halt.losses` is never touched by this path and stays at 1.** |
| 1783586700 – 1783594800 | 16:45 – 18:53 | Three more high_prob cycles resolve as clean WINs. Nothing resets or corrects the stale counter — `HaltTracker` only ever increments on a loss-shaped outcome; there is no separate "reset on win" and, more importantly, no "undo" for a corrected loss. |
| 1783607094 | 22:24:54 | Stop-loss fires for real on `eth-updown-5m-1783606800` (`pnl -0.6176`). `record_trade` runs once → `losses: 1 → 2`. |
| 1783607095 | 22:24:55 | `losses (2) >= halt_prob (2)` → `HaltEngaged` → Telegram "🟡 ETH HALTED" → new entries suppressed for the rest of the session. |

Evidence: `trader/live_logs/live_trades_eth_high_prob.csv` (the LOSS→WIN pair
for `eth-updown-5m-1783584000`, then the lone STOPLOSS row for
`eth-updown-5m-1783606800`) and `trader/live_logs/live.log` lines
~105806–105824.

Note: the 22:24 stop-loss itself was *not* double-logged — it involved two
partial FAK fills (0.28 then 0.86 shares) that each re-triggered
`on_poly`'s stop-loss check, producing two "STOP LOSS order executed"
Telegram messages, but `finalize_or_hold_residual` only calls
`HaltTracker::record_trade` on the final fill that closes the position out,
so that part of the pipeline counted this cycle correctly as 1 loss. The
double-count came from the *earlier*, unrelated cycle's uncorrected
provisional loss.

## Root cause

`Worker::on_api_result`'s `Confirming` branch (the code path that applies a
Gamma correction to a provisional Win/Loss guessed at cycle-close) updates
the trade log (`Action::LogTradeCorrection`) and, in `bin/live.rs`, the
Telegram-facing `slot.wins`/`slot.losses` display tally — but it never
touches `Worker.halt` (the `HaltTracker` backing `halt_rev`/`halt_prob`).

`HaltTracker::record_trade` had already counted the *original* (wrong)
outcome at cycle-close time, on the reasonable assumption that a `Confirming`
record's outcome was final. When Gamma later flips Loss→Win, that earlier
increment is now wrong but nothing walks it back — the phantom loss lives in
`losses` for the rest of the session (or until enough real losses/wins
happen to trip the halt on the phantom count, as happened here). The
symmetric case (a provisional Win flipped to a real Loss) is also wrong in
the other direction: a genuine loss silently never counts toward the halt at
all.

## Fix

Added `HaltTracker::correct_trade(previous_outcome, corrected_outcome)`
(`trader/src/backtest.rs`), called from `on_api_result`'s `Confirming` flip
branch (`trader/src/worker.rs`). It compares whether the previous and
corrected outcomes are loss-shaped (`Outcome::is_loss_for_halt`) and:

- Loss → Win: decrements `losses` by 1 (undoes the phantom count).
- Win → Loss: increments `losses` by 1 (counts the loss that was missed).
- No change in loss-ness: no-op.

If this correction causes the halt to newly engage, the existing
`Action::HaltEngaged` fires (same Telegram message as a live-caught halt). If
it causes an *already-engaged* halt to no longer meet the threshold, a new
`Action::HaltClearedByCorrection` fires, notifying that entries have been
re-armed by the correction. Both are exercised by new tests in
`trader/src/worker.rs`.

## Verification

- `cargo test` — new regression tests reproduce this exact timeline
  (provisional loss corrected to a win, followed by one real loss, must
  *not* halt at `halt_prob = 2`) and the reverse/engage/clear transitions.
- `cargo clippy --all-targets --all-features -- -D warnings` clean.

## Design note (confirmed 2026-07-10, not a bug)

While diagnosing this, we noticed `HaltTracker` never resets its loss count
on an intervening WIN — despite being documented as a "consecutive-loss"/
"loss-streak" halt, `halt_rev`/`halt_prob` actually tracks total losses
within the HKT session, in any order, not a true consecutive streak (a
`Loss, Win, Win, Win, Loss` sequence still trips `halt_max=2` even though
the losses are never back-to-back). Raised with the user and confirmed as
the intended behavior, not a bug — session-total loss counting is what
should ship. No code change from this; noted here only so the "consecutive"
naming in comments/config keys isn't misread as a mismatch with the actual
semantics.
