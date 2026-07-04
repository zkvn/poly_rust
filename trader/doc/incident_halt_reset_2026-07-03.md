# Incident — `/halt` silently cleared within one cycle, 2026-07-03

**Severity: critical.** The user sent `/halt` via Telegram at 17:36 HKT to stop the live
(real-money) bot from opening new positions. The bot placed a new ETH trade at 18:09 —
33 minutes and 6-7 cycle boundaries later — as if the halt had never been sent.

## Root cause

`bin/live.rs`'s single call site for `Event::CycleOpen` (fired every ~5 minutes, once
per asset/strategy, whenever a new market cycle opens) hardcoded:

```rust
let actions = slot.worker.step(Event::CycleOpen { ctx, slug: slug.clone(), entry_suppressed: false });
```

`worker.rs::on_cycle_open` then did an unconditional overwrite:

```rust
self.entry_suppressed = entry_suppressed;
```

`entry_suppressed` is exactly the flag `/halt` sets to `true` and `/resume` sets back to
`false` (`on_control`, `ControlEvent::Halt`/`Resume`). Since the live binary's only
`CycleOpen` call always passed the literal `false`, **every single cycle boundary
silently reset any active halt back to `false`** — with no log line, no Telegram
notification, nothing. `/halt` therefore only suppressed entries until the *next* cycle
open, i.e. at most ~5 minutes, then trading silently resumed. This has been broken
since the halt feature was built — the 5-minute cycle cadence just meant it usually
looked like it worked if checked immediately after sending `/halt`, and nobody had
previously tested it across a cycle boundary.

`entry_suppressed` was never part of `PersistedState` either — it only ever lived in
this one in-memory field, set by `ControlEvent::Halt`/`BalanceEvent::DrawdownHalt` and
cleared by `ControlEvent::Resume` — so the `CycleOpen` parameter had no legitimate
purpose in the live binary; passing anything other than "whatever it already is" is
always wrong there.

(The backtest engine's separate `machine.rs::Machine::cycle_open` also takes an
`entry_suppressed` parameter, but `backtest.rs` computes it correctly each cycle from
its own loss-streak tracker (`halt_rev.is_halted()`/`halt_hp.is_halted()`) rather than
hardcoding it — that's a different, correctly-implemented mechanism and is unaffected
by this bug.)

## Fix

Removed `entry_suppressed` from `Event::CycleOpen` entirely, rather than just fixing the
call site to pass the correct value — this closes off the bug class structurally (no
parameter to get wrong) instead of relying on every future call site remembering to
thread the right value through:

- `worker.rs::Event::CycleOpen` no longer carries the field.
- `on_cycle_open` no longer touches `self.entry_suppressed` at all — halt state can now
  only change via `Event::Control(Halt/Resume)` or `Event::Balance(DrawdownHalt)`, full
  stop.
- `bin/live.rs`'s call site updated to match.
- Updated worker.rs's existing `halt_suppresses_only_new_entries` test to set the halt
  via `Event::Control(ControlEvent::Halt)` before `CycleOpen` (matching how it actually
  happens in production) instead of injecting `entry_suppressed: true` directly into the
  event.
- Added `halt_survives_multiple_cycle_boundaries`: sends `/halt`, drives 5 consecutive
  `CycleOpen` events (simulating 25 minutes of cycles), asserts `is_halted()` stays true
  through every one, then confirms `/resume` still correctly clears it.

Full test suite: 122 passed (lib), 0 failed.

## Why this wasn't caught sooner

`/status`'s halted/active indicator (`bin/live.rs`, reads `slot.worker.is_halted()`)
would also have shown "🟢 active" immediately after the silent reset, so checking status
right after `/halt` wouldn't have revealed the problem either — the bug only surfaces
once a cycle boundary passes while still expecting to be halted, which is exactly what
happened here. Recommend the user re-check `/status` a few minutes after any future
`/halt` as a habit until this fix has been live for a while, though the structural fix
here should make that unnecessary going forward.
