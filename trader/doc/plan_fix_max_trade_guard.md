# Plan — fix `--max-trades` to be a per-cycle guard, not a lifetime cap

**Status: implemented** (worker.rs, bin/live.rs, backtest.rs — 2026-07-03). Superseded
detail: the "shut down once every slot hits its cap" behavior isn't just deleted per §3
below — it's replaced by wiring up the config-driven consecutive-loss halt
(`halt_rev`/`halt_prob` + `halt_reset_hour_rev`/`halt_reset_hour_hp`) that was already
parsed from the strategy TOML but never actually consumed anywhere in the live path (only
`backtest.rs` implemented it, as `HaltTracker`). Per direction: no new
`--max-lifetime-trades` flag — the existing config's own halt-by-loss-count + daily
reset-hour mechanism is the actual "stop trading" control this repo already has and
wants, once it's wired up correctly. See §6 for what changed and how it's tested.

Follow-up to `trader/doc/incident_missed_eth_2026-07-03.md`. Correction to that doc's
proposed fix, per direction: **`--max-trades` should gate "how many trades this
(asset, strategy) slot may take within the current 5-minute cycle" — a counter that
resets to zero every time a new cycle opens — not "how many trades this slot may ever
take for the life of the process."** My earlier suggestion (raise/remove the cap
globally) throws away the guard instead of fixing its scope. This plan fixes the scope.

## 1. Current (wrong) behavior

`bin/live.rs`:

- `AssetSlot.trades_completed: u32` (`:216`) increments once per finished trade
  (`:428`, inside `Action::LogTrade` handling) and **never resets**.
- The per-tick cycle-open gate (`:798`):
  ```rust
  if slot.last_binance <= 0.0 || slot.trades_completed >= args.max_trades {
      slot.current_slug = None;
      continue;
  }
  ```
  checks that same never-reset counter, so once a slot's lifetime total reaches
  `max_trades` (deployed as `1`), it never opens another cycle again for as long as the
  process runs — which is what left ETH `high_prob` dark for 40+ minutes on 2026-07-03,
  missing the 16:59:42 cycle.
- `main()`'s outer loop (`:750`) also shuts the whole process down once *every* slot's
  lifetime counter reaches `max_trades`.

Worth noting `worker.rs::on_cycle_open` (`:377-400`) already unconditionally resets
`self.state = WorkerState::Watching` on every `CycleOpen`, and entry logic
(`on_binance`, `:449`) only fires `if matches!(self.state, WorkerState::Watching)` — so
the `Worker` state machine already structurally guarantees at most one entry attempt per
cycle on its own. The `AssetSlot`-level counter in `live.rs` is a second, independent
guard on top of that (defense-in-depth / bookkeeping), and it's *that* second guard
whose scope is wrong — it was written as a lifetime cap instead of a per-cycle one.

## 2. Target behavior

- Each `AssetSlot` tracks trades taken **in the currently-open cycle only**.
- That counter resets to `0` every time a new cycle opens for that slot (mirroring
  `on_cycle_open`'s own state reset).
- The counter gates whatever it's checked against using only the current cycle's count
  — never a running lifetime total. A slot that took its one trade this cycle is exactly
  as eligible to trade next cycle as a slot that took none.
- Opening a new cycle is never blocked by trade history. The only legitimate reason to
  skip opening a cycle is missing price data (`slot.last_binance <= 0.0`), which is
  unrelated to this bug and stays as-is.
- The process does not self-shut-down based on trade counts — a per-cycle-resetting
  counter has no meaningful notion of "done forever," and an always-on
  `Restart=always` systemd service (`plan_rust_module.md:1163`) shouldn't be exiting
  itself this way regardless.

## 3. Code changes

**`AssetSlot` (`bin/live.rs:207-`)**
- Rename `trades_completed` → `cycle_trades` and document it as "trades logged in the
  currently-open cycle; reset on every `CycleOpen`." Keep `wins`/`losses`/`stoplosses`/
  `unwinds`/`total_pnl` exactly as they are today (lifetime stats for `/status` and
  Telegram summaries) — those are correct as lifetime counters and this bug never
  touched them.

**Reset point — new-cycle branch (`:826-832`)**
- Immediately after `slot.current_slug = Some(slug)`, add `slot.cycle_trades = 0`.
  This is the per-cycle reset; every asset/strategy gets a clean slate exactly when its
  own new cycle opens (assets don't share cycle boundaries in lockstep with each other
  in this codebase's model, so this must live per-slot, not globally).

**Cycle-open gate (`:798`)**
- Change:
  ```rust
  if slot.last_binance <= 0.0 || slot.trades_completed >= args.max_trades {
  ```
  to:
  ```rust
  if slot.last_binance <= 0.0 {
  ```
  Trade history no longer plays any role in whether a new cycle opens. (This is the line
  that actually caused the incident — removing the trade-count half of this condition is
  the fix.)

**Trade-logged increment (`:428`)**
- `slot.trades_completed += 1` → `slot.cycle_trades += 1`. Same call site, same
  condition (`matches!(rec.outcome, Outcome::Win | Outcome::Loss | Outcome::StopLoss |
  Outcome::Unwind)`), just renamed to match its new, correctly-scoped meaning.

**Defense-in-depth entry gate**
- Even though `Worker` already can't produce a second entry inside one cycle, add an
  explicit check where `BinanceTick`/`PolyTick` events get dispatched to
  `slot.worker.step(...)` (`:757-776`): skip dispatch (or skip just the tick types that
  can trigger entry) if `slot.cycle_trades >= args.max_trades`. This makes the
  per-cycle cap real and independently enforced at the driver level, matching the stated
  model exactly ("ETH `high_prob` can only trade 1 during a 5-min slot") rather than
  relying solely on `Worker`'s internal state for that guarantee. Since `max_trades`
  defaults to `1` and `Worker` never produces more than one entry/cycle anyway, this
  should be a no-op in practice today — it only matters if that invariant is ever
  weakened later (e.g. a future re-entry-after-stop-loss feature within the same cycle).

**Remove the trade-count shutdown path (`:750-754`)** — done, deleted outright, no
replacement tied to trade counts. With a per-cycle-resetting counter this condition would
be true only in the brief window right after every slot happens to complete a trade in
the same cycle, which isn't a meaningful "done" signal and isn't what anyone wants from a
`Restart=always` production service. Per direction, no bounded-manual-test-run flag was
added either (no `--max-lifetime-trades`) — that's not a control this repo needs; the
"stop trading" control that actually matters is the consecutive-loss halt below.

**Docs/comments**
- `Args::max_trades` doc comment (`:76-77`, and the file-header comment `:11-17`
  ("Each asset is hard-capped at `--max-trades` completed trades... a real-money run is
  bounded regardless of how long it takes")) currently describes the old lifetime-cap
  semantics — rewrite to describe the per-cycle guard.
- README.md:290's description of the BNB test run's `max-trades 1` should get a note
  that this flag's meaning changed (per-cycle, not lifetime) as of this fix, so that
  historical description isn't misread as still describing current behavior.

## 4. Testing (as implemented)

`bin/live.rs`'s driver loop still isn't unit-tested (async + real I/O, unchanged from
before) — no pure-function extraction was done there in the end; the per-cycle reset
(`slot.cycle_trades = 0` on new-cycle) and gate changes were verified by `cargo build
--bins` (clean) plus manual read-through, since the actual "does this reset and re-arm
correctly" behavior is exercised at the `Worker` level instead (see §6 below — the
consecutive-loss halt tests there cover cycle-boundary reset semantics directly). All 124
lib tests + 3 `live.rs` bin tests pass (`cargo test`).

## 5. Deployment

- No `PersistedState`/crash-recovery format changes for `cycle_trades` — it's transient
  `AssetSlot` bookkeeping in the driver, never persisted.
- `HoldingData.realized_pnl` (added in §6/pnl fix) *is* part of `PersistedState` (nested
  inside `Holding`/`Unwinding`/`StopExiting`), but is `#[serde(default)]`, so old
  persisted-state JSON without the field deserializes fine as `0.0` — no migration step
  needed. (Moot in practice today: `bin/live.rs` doesn't currently reload persisted state
  on startup at all — a pre-existing, separate gap, not touched here.)
- Ship as a normal `trader-live.service` restart. No change needed to the deploy
  command's `--max-trades 1` value — it remains the right default for the (now
  correctly-scoped) per-cycle cap.

## 6. Also implemented this pass: the consecutive-loss halt, and a pnl bug it surfaced

Two closely-related fixes landed alongside the above, per direction ("I just need
current halt by trade loss to work properly" / "fix telegram pnl calculation"):

**Halt-by-loss now actually works.** `AssetParams.halt_rev`/`halt_prob` (consecutive-loss
count) and `halt_reset_hour_rev`/`halt_reset_hour_hp` (daily HKT reset hour) were parsed
from the strategy TOML and displayed in `/status`, but nothing in the live path ever
consumed them — `entry_suppressed` was only ever set by `/halt` or the balance-drawdown
guard. `backtest.rs` already had a correct, tested implementation (`HaltTracker`) that
the live binary never used. Fix: made `HaltTracker`/`hkt_session` `pub(crate)`, gave
`Worker` its own `halt: HaltTracker` (constructed per-strategy from the right
config pair in `new_reversal`/`new_high_prob`), call `reset_if_new_session(ctx.start_ts)`
in `on_cycle_open`, call `record_trade(...)` at every `Action::LogTrade` site, and OR
`self.halt.is_halted()` into both the entry gate (`on_binance`) and the public
`is_halted()` getter (so `/status` shows "🟡 halted" for this too). New test:
`halt_by_loss_streak_suppresses_entry_and_resets_next_session` (worker.rs) — drives 2
losses to trip `halt_rev=2`, confirms entries are actually suppressed (not just
`is_halted()` reporting true) across a same-session cycle boundary, then confirms a cycle
opening 100,000s later (a different HKT day) clears it and entries fire again. Not
persisted across process restarts — `bin/live.rs` doesn't reload any persisted state on
startup today (see §5), so this is a pre-existing gap this fix doesn't newly introduce or
claim to solve.

**Pnl formula bug**, found from a live Telegram report: `✅ ETH TRADE WIN | entry=0.8900
→ exit=1.0000 | pnl=-$0.9964` — a WIN showing a pnl near *negative* the whole stake.
Root cause: every terminal pnl calculation (`on_cycle_close`, `on_unwind_filled`'s and
`on_stop_sell_filled`'s full-close branches) computed `shares * exit_price -
self.trade_size` — subtracting the *nominal* configured trade size, not the actual cost
basis of the shares being settled. That's only equivalent to the correct answer when
`shares == trade_size / token_price` exactly (no partial fill ever happened); the moment
an earlier *partial* take-profit/stop-loss fill reduced `h.shares` to a small residual
(`on_unwind_filled`/`on_stop_sell_filled`'s `sold_shares < h.shares` branch discarded the
partial sale's proceeds entirely), the formula settled the leftover sliver of shares
against the *full* original stake — producing exactly this kind of wildly-wrong negative
pnl on a win. Cross-checked against `../btc_5mins/bot/worker.py`'s reference formula
(`pnl_win = shares * (1.0 - cost)`, `pnl_loss = -shares * cost`) — Python computes pnl
from the actual shares/cost being settled, not a fixed nominal size. Fix: added
`HoldingData.realized_pnl` (dollars already locked in from an earlier partial fill,
accumulated in both partial-fill branches), unified every terminal site onto one
`settle_pnl(h, exit_price) = h.realized_pnl + h.shares * (exit_price - h.token_price)`
helper. New test: `partial_unwind_then_cycle_close_totals_both_legs_pnl` (worker.rs) —
6-of-10 shares sold at a profit, residual 4 shares resolve at cycle close, asserts the
total pnl equals the by-hand arithmetic (+$1.38), not the old formula's wildly-wrong
value. `on_api_result`'s API-flip branch was left as is (it already always recomputes
`shares = trade_size / token_price` from scratch, which is self-consistent for its own
formula and doesn't have real `shares`/`realized_pnl` available to it since `TradeRecord`
doesn't carry a `shares` field — expanding `TradeRecord`'s schema would ripple into the
CSV format, out of scope for this pass).
