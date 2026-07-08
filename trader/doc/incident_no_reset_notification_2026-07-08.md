# Incident — halt cleared with no Telegram notification, 2026-07-08

**Severity: medium.** The balance-drawdown halt engaged on 2026-07-07 (all assets, real
money) was silently cleared by a routine service restart — not by `/resume`, not by the
daily loss-streak reset — and no Telegram message was ever sent to say so. The user
noticed trading had resumed but never got the "halt reset" notification they expected.

## Timeline (all HKT)

- **2026-07-07 11:25** — Balance dropped >25% from session baseline. `BalanceEvent::DrawdownHalt`
  fired, setting `entry_suppressed = true` for every asset then running (BTC, ETH, SOL,
  BNB, XRP, DOGE). Telegram sent correctly:
  `🛑 balance drawdown >25% from session baseline — halted new entries on all assets
  (BTC, ETH, SOL, BNB, XRP, DOGE). Send /resume to re-arm.`
- No `/resume` command appears anywhere in `live_logs/live.log` after this point.
- **2026-07-07 23:37** — `trader-live.service` restarted (deploy that scoped production
  down to ETH-only: `🔴 live driver shutting down (SIGTERM): BTC, ETH, SOL, BNB, XRP,
  DOGE` → `🟢 live driver started: ETH:high_prob, ETH:reversal`). This restart
  constructs a brand-new `Worker` per asset/strategy, which default-initializes
  `entry_suppressed = false` (`worker.rs:326`) — the drawdown halt was gone from that
  moment, with no log line and no Telegram message referencing the halt at all.
- **2026-07-08 03:19** — First ETH entry since the halt (`📋 ETH Order placed`), 4 hours
  after the halt was actually cleared, confirming trading had silently re-armed.
  Several more trades follow through the morning.
- **2026-07-08 08:29–08:30** — Second restart (deploying `a1546b2`, the `unwind_time` /
  `--update-config` change). Same silent-reinit mechanism, but by this point there was
  nothing left to clear — the halt had already vanished 9 hours earlier.

The user's "did I miss the reset notification?" question was actually "the halt cleared
and nobody said so" — confirmed: it did clear, at 23:37 the previous night, via a deploy
restart, and no code path exists that notifies for that.

## Root cause

`entry_suppressed` (the flag `/halt`, `/resume`, and `BalanceEvent::DrawdownHalt` all
operate on) and `HaltTracker`'s loss-streak state (`losses`, `last_session`) are both
purely in-memory fields on `Worker` — neither is part of `PersistedState`
(`worker.rs:204-212`, `to_persisted`/`reconcile`). This was already known and called out
explicitly in `trader/doc/incident_halt_reset_2026-07-03.md`'s root-cause section, but
that incident's fix only addressed the *cycle-open* code path silently clearing halts
every ~5 minutes — it didn't address process **restarts**, which reconstruct every
`Worker` from scratch via `Worker::common` (`worker.rs:314-328`) and get
`entry_suppressed: false` / a fresh zero-state `HaltTracker` for free, regardless of what
was true immediately before the restart.

Every notification path that exists today is tied to a specific *event*, not to a
before/after state comparison at startup:
- `/halt`, `/resume` → immediate reply to the Telegram command that caused them
  (`telegram/mod.rs:65-70`).
- `BalanceEvent::DrawdownHalt` → notified inline at its call site (`bin/live.rs:1235`
  area).
- Loss-streak halt engage/clear → `Action::HaltEngaged`/`Action::HaltReset`, emitted only
  from `record_trade`/`reset_if_new_session` (`worker.rs:522-524`, `backtest.rs:306-315`).

None of these fire on process startup, and startup is exactly when `entry_suppressed`
silently flips back to `false`. The generic `🟢 live driver started` message
(`bin/live.rs`, restart announcement) doesn't know or say whether a halt was in effect
a moment ago — it's asset/strategy list only, not state.

This is effectively the mirror image of the 2026-07-03 incident: that one made `/halt`
too weak (cleared every 5 minutes); this one makes it too weak in a different way
(cleared on any restart) while giving no visibility that it happened either way.

## Proposed fix

1. **Persist halt state.** Add `entry_suppressed: bool` and the `HaltTracker`'s
   `losses`/`last_session` to `PersistedState` (or a new sibling struct) so a restart
   reconstructs the exact halt state that existed before shutdown, the same way
   in-flight `Holding`/`Unwinding` positions already survive restarts today.
2. **Notify on a startup halt-state mismatch.** On process start, after loading
   persisted state per asset/strategy, if any slot comes back halted (either
   `entry_suppressed` or the loss-streak), fold that into the existing `🟢 live driver
   started` message (e.g. append `⚠️ 2/6 slots still halted from before restart:
   ETH:high_prob (drawdown), BTC:reversal (loss-streak)`) instead of staying silent.
   Symmetrically, if restoring the halt state resolves that it should legitimately be
   auto-cleared (e.g. a loss-streak session boundary was crossed while the process was
   down), fire the normal `Action::HaltReset` notification at startup rather than
   swallowing it.
3. Add a worker test mirroring `halt_survives_multiple_cycle_boundaries` from the
   2026-07-03 fix, but for the restart path specifically: serialize a halted worker via
   `to_persisted`, reconstruct via `reconcile`, and assert `is_halted()` is still `true`
   and that startup emits the appropriate notification action.

Not yet implemented — this document is the diagnosis; changes should go through the
usual build/deploy review before shipping to Oracle given this is real-money halt logic.
