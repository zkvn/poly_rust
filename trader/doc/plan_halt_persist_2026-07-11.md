# Plan — persist a Control/Balance halt to disk immediately, not on the next unrelated event

## Bottom line up front

Add two tiny synchronous helpers in `trader/src/bin/live.rs` —
`apply_control(slot, event)` and `apply_balance_halt(slot)` — that call `slot.worker.step(...)`
and then `persist(slot)` in one place, and route the **6** call sites where a halt/resume is
unambiguously meant to take effect immediately (global `/halt`, global `/resume`, scoped
`/halt <asset> [strategy]`, scoped `/resume`, the 25%-drawdown backstop, and the new scoped
balance-decrease halt) through them. That closes the gap flagged in README's `## TODO` after the
last change.

**Deliberately excluded from this fix, flagged for your review rather than silently bundled in:**
the 2 SIGINT/SIGTERM shutdown handlers also call `Event::Control(ControlEvent::Halt)` and discard
it — but that call has been a no-op since the day it was written (commit `5162d9a`, 2026-07-03),
and fixing it uniformly would, for the first time, make it *actually* persist. Since
`deploy_trader.sh` sends SIGTERM on every deploy, that would mean **every deploy halts every
asset/strategy**, requiring a manual `/resume` before trading resumes — a real operational change
disguised as a bugfix. Recommend leaving those 2 call sites untouched (§3 below) unless you
actually want that behavior.

This is a plan for review; nothing has been implemented yet.

## 1. Root cause (recap + one new finding)

`Worker::on_control` (`worker.rs:1406-1417`) and `Worker::on_balance` (`worker.rs:1419-1424`) both
flip `entry_suppressed` and return `vec![Action::Persist]` — an explicit signal, per the
`Action::Persist => persist(slot)` handling every other event type already gets inside
`process_actions` (`live.rs`, `Action::Persist => persist(slot)` arm). But **every** call site that
fires `Event::Control(..)` or `Event::Balance(..)` discards `.step()`'s return value instead of
acting on it:

| # | Call site | `live.rs` line | Trigger |
|---|---|---|---|
| 1 | SIGINT handler | `1601` | Ctrl-C |
| 2 | SIGTERM handler | `1617` | `deploy_trader.sh` restart |
| 3 | `/halt` (global) | `1630` | Telegram |
| 4 | `/resume` (global) | `1636` | Telegram |
| 5 | `/halt <asset> [strategy]` | `1659` | Telegram |
| 6 | `/resume <asset> [strategy]` | `1682` | Telegram |
| 7 | Scoped balance-decrease halt (2026-07-11) | `1736` | balance timer |
| 8 | Global 25%-drawdown halt | `1750` | balance timer |

In-memory `entry_suppressed` flips immediately in all 8 cases (so the *running* process behaves
correctly right away — `/halt` really does stop new entries this session). What's missing is the
disk write: `live_state_<asset>_<strategy>.json` only catches up whenever some *other* event later
persists that slot (e.g. its next trade, or a `/set` command — anything that returns
`Action::Persist` through the normal `process_actions` path). Between the halt and that next event,
a crash or restart reverts to the last-written (un-halted) state.

**New finding while scoping this fix:** rows 1-2 (the shutdown handlers) have discarded this same
return value since the handler was first written (`git log -S"shutting down (SIGINT/SIGTERM)"` →
`5162d9a`, 2026-07-03) — so whatever "mark halted before shutting down" was meant to accomplish has
never once taken effect; the process sets the flag in memory and exits a line later, and nothing
ever reads or persists it. This isn't a regression from the 2026-07-11 change, but it means fixing
*all 8* sites uniformly is a bigger behavior change than it looks (§3).

## 2. Proposed solutions

### Option 1 (recommended): two small synchronous helpers

```rust
fn apply_control(slot: &mut AssetSlot, event: ControlEvent) {
    slot.worker.step(Event::Control(event));
    persist(slot);
}

fn apply_balance_halt(slot: &mut AssetSlot) {
    slot.worker.step(Event::Balance(BalanceEvent::DrawdownHalt));
    persist(slot);
}
```

Each of the 6 in-scope call sites (§3) changes from `slot.worker.step(Event::Control(...));` to
`apply_control(slot, ControlEvent::Halt);` (or `apply_balance_halt(slot);`) — a one-line swap per
loop body, no `.await` needed since `persist()` is already synchronous (`std::fs::write`,
`live.rs:354`). Matches this file's existing habit of small free-function helpers (`persist`
itself, `confirming_asset_labels`, `arrow_side`). Deterministic and minimal: both `on_control` and
`on_balance` only ever return `[Action::Persist]` today, so hand-writing that one guaranteed
follow-up call is simpler than a generic action dispatcher for a return shape that never varies.

### Option 2: route through the existing async `process_actions` pipeline instead

Same as how `Event::ApiResult`/`ApiResultTimeout` already do it (`live.rs`, `driver.process_actions(slot, actions, Feed::Clob).await`)
— reuses the fully generic action executor, so it would automatically pick up any *new* action
`on_control`/`on_balance` might emit in the future without this fix needing to be revisited. Rejected
as the primary option: it turns 6 previously-synchronous call sites into `.await` points and pulls
in the boxed-recursive `process_actions` machinery (designed for CLOB order sequences) for a return
value that is contractually always just `Persist` — speculative generality the project's own
guidelines call out to avoid. Worth it only if you expect `on_control`/`on_balance` to grow more
actions soon; not proposed as the default.

### Option 3 (rejected): persist inside `Worker::on_control`/`on_balance` directly

Would make `step()` do its own I/O, breaking the module's own stated invariant
(`worker.rs`'s header comment: `step(event) -> Vec<Action>` is "a pure, synchronous" function) that
every other event handler in this file already respects — `Worker` decides *what* happened,
`live.rs` executes it. Not proposed.

## 3. Scope decision: exclude the 2 shutdown call sites (rows 1-2)

Recommend fixing only rows 3-8 — the 4 Telegram halt/resume commands and the 2 balance-driven
halts — where "persist immediately" is unambiguously correct: a human or the balance guard decided
this halt should stick, and the whole point of a halt is that it survives a restart.

Leave the SIGINT/SIGTERM handlers' `slot.worker.step(Event::Control(ControlEvent::Halt));` exactly
as-is (still discarded, still a no-op) — not because it's fine, but because *fixing* it changes
behavior most deploys don't want: `deploy_trader.sh` restarts `trader-live.service` via SIGTERM on
every routine deploy (including the one just shipped for the Gamma/balance-halt change), and every
deploy in the incident history so far has assumed trading resumes automatically afterward (see the
"no open position at restart time" pre-deploy checks in README's incident log — never "remember to
`/resume` after this deploy"). If persisting a shutdown-halt is actually wanted (e.g. "a restart
should always come back paused until a human confirms it's safe"), that's a deliberate,
separate product decision with its own README/TODO writeup and a deploy-script change to
auto-`/resume` afterward — not something to fold into a persistence bugfix. Flagging here for your
call; happy to do either.

## 4. Files touched

- `trader/src/bin/live.rs` — add `apply_control`/`apply_balance_halt` (near `persist`, `live.rs:354`);
  swap rows 3-8 in §1's table to call through them. Rows 1-2 unchanged.

## 5. Tests

- Existing `persisted_slot_tests` module already covers `persist`/`load_persisted_slot` round-tripping
  in isolation — no change needed there.
- New: a test that builds a minimal real `AssetSlot` (not just a bare `Worker`, since
  `apply_control`/`apply_balance_halt` take `&mut AssetSlot`) pointed at a tempfile `state_file`,
  calls `apply_control(&mut slot, ControlEvent::Halt)`, and asserts the file on disk shows
  `entry_suppressed: true` **before** any trade event — the actual scenario this fix closes. This is
  the one place this task needs slightly heavier test scaffolding than the 2026-07-11 change added
  (`balance_halt_scope_tests` there only constructs bare `Worker`s, not full `AssetSlot`s with their
  `U256`/`AssetParams`/log-path fields) — worth a look at `AssetSlot`'s constructor (`live.rs:442+`)
  to see how cheaply a test instance can be built before committing to this test's shape.
- `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all --check`.

## 6. Deploy

Same as prior changes: `scripts/deploy_trader.sh`. Because rows 1-2 are explicitly left unchanged
(§3), this fix doesn't alter shutdown/restart behavior at all — no new pre-deploy caution beyond
the existing "no open Holding-family position" check.
