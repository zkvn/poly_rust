# Fix — mid-cycle restart corrupts `cycle_open_binance`, 2026-07-15

**Source:** `../btc_5mins/doc/audit_bt2_stoploss_2026-07-15.md` §3b (cross-project
audit; the bug itself lives entirely in this repo). Also tracked in this repo's
own `README.md` TODO before this fix.

## Root cause

`trader/src/bin/live.rs:1510`:

```rust
let mut current_slot_val: u64 = 0;
```

`current_slot_val` starts at `0` on every process start. The first `ticker`
tick (fires every 1s) always takes the `slot_now != current_slot_val` branch
(`live.rs:1557-58`) unconditionally, because no real Unix-epoch 5-minute slot
can ever equal `0`. That branch treats the current slot as a brand-new cycle
and stamps `open_binance = slot.last_binance` — whatever Binance is trading at
*this instant* — regardless of how far into that slot the wall clock actually
is.

`Worker::on_cycle_open` (`worker.rs:751`) takes `ctx.open_binance` at face
value as `cycle_open_binance`, which then drives every signal computed for the
rest of that cycle: `delta_pct`, the reversal strategy's reset, and the final
`price_moved_up = last_binance > cycle_open_binance` resolution check.

**Any process restart that lands inside an already-open cycle** — a
`--update-config`/`--config-only` deploy, a full binary redeploy, or a
crash+respawn under systemd's `Restart=always` — silently corrupts that one
cycle's signal math. Confirmed real-world incident: the 2026-07-15
`--update-config` deploy restarted `trader-live.service` 100s into an open BTC
cycle, producing two `[live] new cycle ... slug=btc-updown-5m-1784076900`
log lines 100s apart with two different `open_binance` values, directly
implicated in one costly stop-loss that fired in that cycle (see audit §1,
the `2026-07-15 08:55 BTC` row).

Position/cycle state is deliberately *not* restored across a restart
(README "Restart behavior") — only halt state (`entry_suppressed`,
`halt_losses`, `halt_last_session`) survives, via `PersistedState`. So a fix
that tries to resume the true in-flight position is out of scope; the bug is
narrower than that: it's specifically the fabricated *reference price* used to
open a "new" cycle that was actually already running.

## Proposed fix

Of the two options the audit flagged, going with the simpler one — it doesn't
require reconstructing historical price data (no dependency on Binance klines
or `price_feed`'s own recorded-open lookup) and reuses the existing
"no-entry" pattern the codebase already has for halts:

**On startup, if the real current slot is already more than a few seconds
old, don't fire a (fabricated) `CycleOpen` for it at all — leave
`current_slug: None` for every asset and let the existing `current_slug.is_some()`
guards on `BinanceTick`/`PolyTick` naturally suppress entries for the
remainder of that cycle. Resume normal cycle handling at the next genuine
5-minute boundary, with a correctly-timed `open_binance`.**

Concretely, in `trader/src/bin/live.rs`'s `ticker.tick()` arm:

- Record whether this is the very first firing since process start
  (`current_slot_val == 0`, before it gets overwritten — real epoch slots are
  never `0`).
- Compute `elapsed_into_slot = now_secs_f64() - slot_now as f64`.
- If both are true — first-ever tick *and* `elapsed_into_slot` exceeds a small
  threshold (proposed: 5s; the ticker fires every 1s, so a genuine boundary
  crossing during normal steady-state operation is always caught within ~1-2s,
  never anywhere close to 5s) — skip the per-asset "fresh cycle" block
  entirely for this one tick, and log why. `current_slot_val` is still updated
  to `slot_now`, so the branch won't re-fire until the real next boundary.
- Otherwise (clean start at/near a boundary, or any subsequent normal
  boundary crossing during the run) — unchanged behavior.

No change to `worker.rs`: skipping `Event::CycleOpen` entirely (rather than
firing it with some "suppressed" flag) means `entry_suppressed` — which is
persisted and already carries real halt semantics (`/halt`, loss-streak,
drawdown) — is never touched by this, so a restart can never accidentally
clear or extend an unrelated halt.

**Trade-off accepted:** an asset that restarts mid-cycle trades zero cycles
that "restart cycle" — same cost as any other legitimate halt, and strictly
better than trading it on a fabricated reference price. Restarts are
infrequent (config pushes, occasional crash-respawn) so this is a small,
bounded cost.

## Test plan

### Unit tests (`trader/src/bin/live.rs`, `#[cfg(test)] mod tests`)

Can't easily unit-test the `ticker.tick()` arm directly (it's inline in
`main`'s `tokio::select!` loop) — the fix logic is expressed as a small
extracted decision, e.g. a pure function
`fn should_suppress_startup_cycle(is_first_tick: bool, elapsed_into_slot: f64) -> bool`,
unit-tested directly:

1. First tick, `elapsed_into_slot = 0.3` (clean start right at boundary) → `false`.
2. First tick, `elapsed_into_slot = 100.0` (the real 2026-07-15 incident shape) → `true`.
3. First tick, `elapsed_into_slot` exactly at the threshold boundary (5.0) →
   not suppressed (`>`, not `>=`) — document the choice.
4. Not-first tick (steady-state boundary crossing), `elapsed_into_slot` large
   (simulating a slow/delayed tick under load) → `false` — the guard only
   ever applies to the first tick after process start.

### Integration-shaped test (existing `AssetSlot`/`Worker` test harness in `live.rs`)

Reuse the existing persisted-state test helpers (`scratch_state_path`,
`load_persisted_slot`) to assert the *outcome*, not just the decision
function:

5. Simulate a restart 100s into a cycle (mirroring `current_slot_val = 0`,
   `slot_now` = a slot ~100s old): assert no `CycleOpen` event reaches the
   worker, `current_slug` stays `None`, and a subsequent `BinanceTick`/
   `PolyTick` for that slot is a no-op (no `Action`s produced) — confirming
   entries stay suppressed for the rest of that cycle.
6. Simulate the *next* boundary after that (advance `slot_now` by one full
   period): assert `CycleOpen` fires normally with `open_binance` equal to
   the price at that genuine boundary — confirming recovery is automatic and
   the fix doesn't leave the asset permanently stuck.
7. Confirm existing halt semantics are untouched: a worker with
   `entry_suppressed = true` persisted from before the restart (real `/halt`,
   unrelated to this bug) stays halted after the first clean boundary too —
   this fix must never interact with that flag.

### Local docker verification (`docker-compose.yml`)

The compose file's `trader` service currently mounts the **real** production
`.env`/config (`/home/kev/apps/btc_5mins/.env`, real API keys) and there is no
`--dry-run`/paper-trade flag in the Rust binary (unlike the Python bot in
`btc_5mins`, which defaults to `paper_trade=True`) — the Rust trader always
places real orders. So this step must **not** run `docker compose up trader`
against the real `.env` as a way of exercising the fix.

Plan instead:
1. `docker compose build trader` — confirms the Dockerfile still builds clean
   with the fix (compile-level check only).
2. Run the built binary locally (not via `docker compose up`, and not with
   real credentials) against `nats` + `price-feed` only, using a scratch/dummy
   `.env` (garbage API key/secret) so any CLOB call fails auth rather than
   succeeding — start it mid-cycle on purpose (launch a few minutes after a
   5-minute boundary) and confirm the log shows the new
   `"startup landed ...s into an already-open cycle"` line instead of a
   fabricated `"new cycle"` line, then confirm a normal `"new cycle"` line
   appears at the next real boundary with the correct price.
3. Kill/restart that same local process mid-cycle a second time to confirm
   the guard re-triggers identically on a second restart (not just
   first-ever binary launch — `current_slot_val` really does reset to 0 every
   process start, so this should reproduce every time).

## Deploy plan

Once unit/integration tests pass and `cargo fmt --all --check` /
`cargo clippy --all-targets --all-features -- -D warnings` are clean:

1. `python scripts/deploy_oracle.py --trader-only` (binary changed, config
   didn't) — cross-compiles aarch64, rsyncs `live` to Oracle, restarts
   `trader-live.service` via systemd (never a direct kill — see that script's
   own module docstring on why, referencing the 2026-07-03 double-process
   incident).
2. Confirm `systemctl is-active trader-live.service` reports `active` (the
   script already checks this and fails non-zero otherwise).
3. Tail `trader/live_logs/live.log` on Oracle for the next few minutes:
   - If this deploy itself happens to land mid-cycle, confirm the new
     suppression log line appears instead of a fabricated `"new cycle"` line
     — a live, real-world confirmation of the fix on the exact class of event
     that caused the original incident.
   - Confirm the *next* clean boundary opens normally with a correct
     `open_binance`.
4. Update `README.md`: remove the now-fixed TODO entry, add a short "known
   incidents" entry pointing at this doc.
