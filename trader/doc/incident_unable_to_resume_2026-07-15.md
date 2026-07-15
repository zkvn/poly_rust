# Incident — BTC stuck halted despite repeated `/resume`, 2026-07-15

**Severity: high — live, real-money BTC trading, ongoing at time of writing.** BTC
(`reversal`) tripped an automatic halt at 08:59:40 HKT this morning. The operator sent
`/resume` three separate times over the following ~5 hours (two unscoped, one
`/resume btc`), each of which replied with an unqualified "▶️ Resumed" success message,
and a full process restart happened in between — yet `/status` kept showing BTC halted
the entire time, and BTC placed zero trades all day up to 14:16 HKT when this was
diagnosed.

## Timeline (all HKT, from `trader/live_logs/live.log`)

- **2026-07-15 08:58:46** — `trader-live.service` restarted (config/binary deploy).
  Roster: `BTC:reversal, BNB:reversal` (per today's `strategy_20260715.toml`, trade
  assets narrowed to BTC+BNB — see that config's own `meta.source`).
- **08:58:xx** — `▶️ Resumed all assets (BTC, BNB).` sent (routine post-restart resume;
  nothing was actually halted yet at this point).
- **08:59:28** — BTC order placed (DOWN, `reversal`).
- **08:59:39** — Stop-loss triggered; order executed, **pnl -0.5273**.
- **08:59:40** — `❌ BTC TRADE STOPLOSS` followed immediately by `🟡 BTC HALTED | reversal`.
  This is the per-strategy consecutive-loss halt (`halt_rev`), **not** a manual `/halt` —
  `halt_rev` was tightened `2 → 1` on 2026-07-13 and carried forward unchanged into
  today's `strategy_20260715.toml` (`# halt_rev=1 unchanged`), so a *single* stop-loss
  now trips it immediately, on the very first loss of the HKT session
  (`halt_reset_hour_rev = 2`, i.e. the 2am HKT daily rollover — nowhere close to
  08:59).
- **09:03:00** — `🔴 BTC STOP COSTLY` — an unrelated advisory notification (the
  counterfactual "would this stop-loss have been worth it" verdict, `bin/live.rs`'s
  `Action::StopLossVerdict`). Purely informational; does not touch halt state.
- **~10:00–10:05** — `▶️ Resumed all assets (BTC, BNB).` sent (first real resume attempt
  after the halt). BTC did not resume trading.
- **10:50** — `trader-live.service` restarted again (another deploy). Persisted halt
  state (`entry_suppressed`, `halt_losses`, `halt_last_session`) survives restarts by
  design (`Worker::restore_halt`), so BTC came back up still halted.
- **14:05:20–14:05:54** — Operator checks `/status`, sends `/resume` (unscoped —
  `▶️ Resumed all assets (BTC, BNB).`), checks `/status` again, sends `/resume btc`
  (`▶️ Resumed BTC.`), checks `/status` a third time. BTC still shows halted.
- **14:16** (diagnosis time) — `live_state_btc_reversal.json` on disk:
  ```json
  {
    "entry_suppressed": false,
    "halt_losses": 1,
    "halt_last_session": "2026-07-15",
    "stats": { "last_trade": "08:59:40 DOWN STOPLOSS pnl=-0.5273" }
  }
  ```
  `entry_suppressed: false` confirms every `/resume` *did* apply and persist correctly.
  `halt_losses: 1` (with `halt_rev = 1`) is why `is_halted()` is still `true` —
  BTC has not traded since the 08:59:40 stop-loss, over 5 hours and 3 resume attempts
  later.

## Root cause

`Worker::is_halted()` (`worker.rs`) is an OR of two independent gates:

```rust
pub fn is_halted(&self) -> bool {
    self.entry_suppressed || self.halt.is_halted()
}
```

- `entry_suppressed` — the manual/drawdown/gamma-unresolved gate. Set by `/halt`, a
  balance-drawdown, or an unresolved Gamma confirmation; cleared by `/resume`.
- `self.halt` (`HaltTracker`, `backtest.rs`) — the per-strategy consecutive-loss
  counter (`halt_rev`/`halt_prob`). Set by `record_trade` on a qualifying loss; cleared
  **only** by the daily `halt_reset_hour_rev`/`_hp` session rollover.

This separation is deliberate and correctly enforced: `ControlEvent::Resume` in
`Worker::on_control` only ever does `self.entry_suppressed = false` — it has never
touched `self.halt`, and `control.rs`'s own doc comment says as much ("halt/resume here
must only set/clear that flag"). `/resume` working exactly as designed is not the bug.

The bug is that **the one command meant to clear the other gate was never wired up in
the live binary.** `HELP_TEXT` (`telegram/render.rs`) has advertised
`/reset_losses [asset] — zero the halt loss counter` since it was written, `commands.rs`
parses it into `Command::ResetLosses`, and `control.rs`'s `ControlTarget`/`apply_control`
even has a fully-tested `reset_losses` path — but that whole `ControlTarget`/`ControlMsg`
module is dead code in production (`telegram/mod.rs`'s own note: `TelegramBot::run_loop`
"is not invoked anywhere in this crate's tests or binaries yet"). The actual live
dispatcher, `bin/live.rs`'s `telegram_rx` `match cmd { ... }`, hand-rolls `Halt` and
`Resume` per-slot routing directly against `ControlEvent`, but had no arm for
`Command::ResetLosses` at all — it fell through to the catch-all:

```rust
_ => Some("not supported by this Rust live driver yet.".to_string()),
```

So with `halt_rev` tightened to `1` (2026-07-13, still `1` in today's
`strategy_20260715.toml`), a single stop-loss trips the loss-streak halt, and the
*only* commands that exist to clear it in production are (a) wait for 2am HKT, or (b) a
command that has silently never worked. `/resume`, correctly clearing the unrelated
manual gate every time it's sent, produced a genuine "✅ Resumed" reply on all three
attempts — which is why the operator had no reason to suspect it wasn't the right
command. Compounding this, neither the `/resume` reply nor the `/status` halted light
distinguished *which* gate was still up, so there was no way to discover the real
cause short of reading the source.

## Fix

1. **`ControlEvent::ResetLosses`** (new variant, `worker.rs`) → `Worker::on_control`
   calls `self.halt.reset_losses()` (new method, `backtest.rs`, zeroes the counter) and
   persists immediately, mirroring `Halt`/`Resume`'s existing behavior
   (`trader/doc/plan_halt_persist_2026-07-11.md`).
2. **Wired `Command::ResetLosses` into `bin/live.rs`'s telegram dispatcher** — an
   asset-scoped and an all-assets arm, symmetric to the existing `Halt`/`Resume` arms,
   replying `🔄 Reset loss-streak halt counter for {label}.`
3. **Honest `/resume` reply.** If a slot is still halted after `/resume` because the
   loss-streak gate is up, the reply now appends
   `⚠️ still halted (loss-streak): BTC/reversal (1/1) — /reset_losses to clear now, or
   wait for the daily reset.` instead of silently claiming full success.
4. **`/status` now shows the reason.** The halted light changed from a flat
   `🟡 halted` to `🟡 halted (suppressed)` / `🟡 halted (loss-streak N/M)` / both
   joined with `+` when applicable, so this is diagnosable from Telegram alone next
   time, without reading source.

`entry_suppressed` and the loss-streak counter remain intentionally independent —
`/reset_losses` never touches `entry_suppressed`, and `/resume` never touches the
loss-streak counter, so a worker halted for both reasons needs both commands, same as
today, just with the missing one now actually present.

## Verification

- New/updated tests:
  - `trader/src/worker.rs`: `resume_does_not_clear_a_loss_streak_halt` (regression —
    3x `/resume` on a loss-streak-only halt, never clears),
    `reset_losses_clears_loss_streak_but_not_manual_halt`,
    `reset_losses_is_a_no_op_when_not_halted`, and `ResetLosses` added to
    `control_and_balance_events_persist_immediately`'s persist-action assertion.
  - `trader/src/bin/live.rs` (`halt_persist_tests`): `apply_control_reset_losses_persists_immediately`
    (mirrors the existing `apply_control_halt_persists_immediately`/`_resume_` tests),
    `resume_reply_note_flags_a_still_active_loss_streak_halt` (drives the actual
    incident shape end-to-end at the slot level: manual halt + loss-streak halt,
    3x `/resume`, halt survives, the reply-note helper flags it, `/reset_losses`
    clears it).
- `cargo test` (trader crate): 193/193 passing tests still pass, plus 6 new ones (199
  total). 4 pre-existing, unrelated failures in `config`/`config_log` tests
  (config-drift against today's `strategy_20260715.toml`, already tracked in
  README's `## TODO` since 2026-07-09/07-14 — confirmed identical on `main` before
  this change, not touched by this fix).
- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `cargo fmt --all --check` clean.
- `docker compose build trader` — Dockerfile still builds clean with the change
  (compile-level check only; per `trader/doc/fix_live_deploy_2026-07-15.md`'s
  precedent, `docker compose up trader` is never run locally against the real
  production `.env`/API keys).
- Deployed to Oracle via `scripts/deploy_trader.sh` (`deploy_oracle.py --trader-only`,
  goes through `systemctl restart trader-live.service`); on-call verification: service
  reports `active`, and `/reset_losses btc` actually clears BTC's `/status` halted
  light on the live production process. See the deploy log / commit for exact
  timestamps.

## Follow-up (not part of this fix)

- `scripts/deploy_trader.sh`'s own header comment still describes a tmux-based restart
  ("kills its tmux session... starts the new binary in a fresh tmux session"); the
  actual mechanism (`deploy_oracle.py`) has used `systemctl restart trader-live.service`
  since at least the 2026-07-03 double-process incident. Stale comment, not a behavior
  bug — flagged in README `## TODO`, not fixed here (out of scope for a halt-routing
  fix).
