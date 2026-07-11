# Plan — extend Gamma resolution window, scope the balance-decrease halt to asset+strategy

## Bottom line up front

Two changes, both config-driven, no change to when/how a trade *enters*:

1. **Gamma poll window**: keep the first poll at 60s after cycle close (unchanged), but stop
   giving up at ~120s (currently borrowed from `reversal_start_time`) — extend the deadline to a
   new, independent **10 minutes (600s)**, retrying every **20s** instead of every 3s. Gamma is
   decoupled from `reversal_start_time` entirely (new dedicated config field).
2. **Balance-decrease halt**: today, a balance drop >25% from session-start baseline halts *every*
   asset+strategy process-wide. That stays as a coarse backstop. New, in addition: a per-cycle
   check at 120s into the next cycle (offset now configurable, not hardcoded) compares balance to
   the *previous* cycle's checkpoint — if it dropped, halt **only** the asset+strategy(ies) that
   currently have an unresolved (`Confirming`) trade pending Gamma confirmation; every other
   asset/strategy keeps trading. If balance rose, no halt — the pending trade keeps being polled
   exactly as today.

Point 3 the request raised — "only report Gamma status on timeout or on a mismatch, stay quiet on
a clean confirmation" — **is already how the code behaves** (`Action::ApiResultNote` for a clean
match is `println!`-only, never Telegram; `Action::LogTradeCorrection` on a mismatch and
`Action::GammaHaltEngaged`/`GammaUnresolvedContinued` on timeout already `notify()`). No change
needed there beyond updating the timeout message's displayed window from `reversal_start_time` to
the new deadline constant.

This is a plan for review, nothing has been implemented yet.

## 1. Current behavior (for context)

**Gamma poll window** — `trader/src/bin/live.rs:150-196` (`spawn_resolution_watcher`), fed by
`GammaPollCadence` at the `Action::LogTrade` call site (`live.rs:978-990`):

- `poll_delay_secs` = `gamma_poll_delay_secs` config, default `60` — wait before first poll.
- `poll_interval_secs` = `gamma_poll_interval_secs` config, default `3` — retry cadence.
- `deadline_secs` = **`reversal_start_time`** (`live.rs:985`), default `120` — give-up point. This
  reuse was deliberate (comment at `live.rs:139-143`): `reversal_start_time` is "the earliest
  either strategy could possibly want to open a new position this cycle anyway," so blocking
  re-entry until then was framed as free. On timeout (`None` past deadline), the watcher reports
  `(asset, strategy, None)` → `Event::ApiResultTimeout` → `Worker::on_api_result_timeout`
  (`worker.rs:1373-1404`): halts (`entry_suppressed = true`, `Action::GammaHaltEngaged`) unless
  `GammaBalanceTracker::increased()` says balance rose since the *previous* cycle's checkpoint, in
  which case it continues unsuppressed (`Action::GammaUnresolvedContinued`).

**Balance checks** — `trader/src/balance.rs`, driven by one global timer in `live.rs:1452-1453` /
`1684-1699`:

- `BalanceGuard` — session-start baseline, fires once if drawdown from that fixed baseline exceeds
  `DRAWDOWN_LIMIT = 0.25` (const). On fire, `live.rs:1689-1691` loops **every** `AssetSlot` and
  calls `.step(Event::Balance(BalanceEvent::DrawdownHalt))` on all of them — global halt.
- `GammaBalanceTracker` — rolling cycle-over-cycle (`record()`/`increased()`), fed from the *same*
  balance sample, currently consumed only by the Gamma-timeout path above, not used to halt
  anything on its own.
- Both are sampled once per cycle at `window_start + CHECK_OFFSET_SECS`, `CHECK_OFFSET_SECS = 120`
  and `WINDOW_SECS = 300` — both **hardcoded consts** in `balance.rs:12-13`, not config.

**Cycle** = one `period_secs` (default 300s) window (`marketdata.rs:23`, `live.rs:1505-1516`).

## 2. Proposed changes

### 2a. Gamma poll window

- `trader/src/config.rs`: add a new per-asset field `gamma_poll_deadline_secs` (TOML
  `HashMap<String,f64>` with `"default"` fallback, same shape as the two existing gamma fields),
  default `600.0`. Resolved into `AssetParams` alongside `gamma_poll_delay_secs`/
  `gamma_poll_interval_secs`.
- `trader/config/strategy_20260709.toml` (and any other active strategy TOML): add
  `[gamma_poll_deadline_secs] default = 600`, and change `[gamma_poll_interval_secs] default = 3`
  → `20`. `gamma_poll_delay_secs` stays `60` (unchanged).
- `live.rs:985`: `GammaPollCadence.deadline_secs` reads `slot.params.gamma_poll_deadline_secs`
  instead of `slot.params.reversal_start_time`.
- `live.rs:1085-1088` and `1110-1113` (the `GammaHaltEngaged`/`GammaUnresolvedContinued` Telegram
  text): swap the displayed `{:.0}s` window from `slot.params.reversal_start_time` to
  `slot.params.gamma_poll_deadline_secs`, since that's what actually gated the wait now.
- Update the doc comments at `live.rs:120-149` (`GammaPollCadence`/`spawn_resolution_watcher`)
  that currently justify reusing `reversal_start_time` — that reasoning no longer applies once
  the deadline is 600s, independent of entry timing.

### 2b. Balance-check cadence becomes configurable

- `balance.rs`: drop the hardcoded `CHECK_OFFSET_SECS`/`WINDOW_SECS` consts; `seconds_until_next_check`
  takes them as parameters instead (defaults unchanged: offset `120`, window `300`).
- This is **not** a per-asset value (there's one wallet balance for the whole process, sampled
  once per checkpoint and fanned out to every slot) — it becomes a new top-level CLI arg /
  top-level config field, e.g. `--balance-check-offset-secs` (default `120`), not a per-asset TOML
  map like the gamma fields. `WINDOW_SECS` can just reuse `args.period_secs` (already available,
  already equals 300 by default) rather than a second hardcoded constant that could drift from it.
- `live.rs:1452-1453` / `1697-1698`: pass the new offset through instead of the removed consts.

### 2c. Scoped balance-decrease halt (new, additive to the existing global 25% guard)

Per the review answers: **keep** the existing global `BalanceGuard` (25% off session-start
baseline, halts everything) as a coarse backstop for a broad/systemic drawdown. Add a second,
finer-grained check alongside it, fed from the same per-checkpoint balance sample (no extra API
calls):

- New comparison basis: **previous cycle's checkpoint** (cycle-over-cycle) — i.e. reuse
  `GammaBalanceTracker`'s existing `record()`/`increased()` pattern (already tested, already fed
  from this same sample) rather than inventing a second tracker. `increased() == Some(false)`
  (and only when we have two real samples to compare — `None` fails safe by *not* triggering this
  new halt, same "unknown ⇒ don't act" convention `GammaBalanceTracker`'s own doc comment already
  documents) is the trigger condition.
- **Scope**: on trigger, halt only the `AssetSlot`s whose `Worker` currently has an unresolved
  trade pending Gamma confirmation — i.e. `WorkerState::Confirming(_)`. Everything `Watching` (no
  exposure this cycle) is left alone. This directly matches the "eth high_prob only" example: if
  that's the only slot in `Confirming` at checkpoint time, it's the only one halted.
- `Worker::state` is a private field (`worker.rs:397`) — add a small public accessor,
  `Worker::is_confirming(&self) -> bool`, so `live.rs` can filter on it without reaching into
  worker internals.
- Mechanically this reuses the existing halt effect (`Event::Balance(BalanceEvent::DrawdownHalt)`
  → `entry_suppressed = true`, `worker.rs:1421`) — no new `BalanceEvent` variant needed. The only
  change at the `live.rs` call site is the iterator: `assets.iter_mut()` (all, existing global
  path) vs. `assets.iter_mut().filter(|s| s.worker.is_confirming())` (new scoped path) — and each
  path gets its own Telegram text so the two mechanisms remain distinguishable in the log/chat
  (e.g. "🛑 balance drawdown >25% from session baseline — halted new entries on all assets" stays
  as-is; new: "🟡 ETH/high_prob balance decreased vs last cycle's checkpoint while its Gamma result
  was still pending — halted new entries on ETH/high_prob only.").
- If balance *increases* at the checkpoint, no halt — the pending trade(s) keep being polled
  exactly as today, all the way to `gamma_poll_deadline_secs`.
- **Multiple concurrent `Confirming` slots**: the balance signal is account-wide (one USDC
  balance), so if two+ assets/strategies both have a trade pending at the same checkpoint, a
  single decrease can't be attributed to just one of them. Proposed behavior: halt every
  currently-`Confirming` slot in that case (still not "the whole thing" — idle assets/strategies
  are still spared), and say so explicitly in the Telegram text (list all halted slots). Flagging
  this as the one place the "eth-only" example doesn't fully generalize — worth a look during
  review.

### 2d. Gamma status reporting (no code change expected)

Re-confirmed against current code — already matches the requested "only report on timeout or
mismatch" rule:

- Clean confirmation (Gamma agrees with the provisional Binance-based result) →
  `Action::ApiResultNote`, `println!`-only (`worker.rs:1295-1300`, `live.rs:1118`). No Telegram
  message today, none proposed.
- Mismatch (Gamma disagrees, pnl/outcome corrected) → `Action::LogTradeCorrection`, `notify()`s
  today (`live.rs:1012-1024`). Unchanged.
- Timeout (`gamma_poll_deadline_secs` elapses with no result) → `Action::GammaHaltEngaged` /
  `Action::GammaUnresolvedContinued`, both `notify()` today (`live.rs:1079-1114`). Unchanged
  except the displayed window value (§2a).

## 3. Open question carried into implementation

The "multiple concurrent `Confirming` slots at one balance checkpoint" case (§2c) doesn't have a
clean single-asset attribution — proposed default is "halt all currently-pending slots," flag for
review rather than silently deciding.

## 4. Files touched (implementation, not yet done)

- `trader/src/config.rs` — new `gamma_poll_deadline_secs` field + resolution into `AssetParams`.
- `trader/config/strategy_20260709.toml` — add `gamma_poll_deadline_secs = 600`, change
  `gamma_poll_interval_secs` to `20`.
- `trader/src/balance.rs` — de-hardcode `CHECK_OFFSET_SECS`/`WINDOW_SECS`; no change to
  `BalanceGuard`'s 25%-drawdown logic itself.
- `trader/src/worker.rs` — new `pub fn is_confirming(&self) -> bool`.
- `trader/src/bin/live.rs` — `GammaPollCadence` deadline source, timeout message text, new CLI arg
  for the balance-check offset, new scoped-halt branch alongside the existing global one at the
  `balance_deadline` timer arm (~`live.rs:1684-1699`).

## 5. Tests to add

- `balance.rs`: `seconds_until_next_check` with non-default offset/window args (replacing the
  const-based tests).
- `worker.rs`: `is_confirming()` true only in `WorkerState::Confirming`.
- `live.rs` bin tests (or a new integration test if the existing harness supports multi-slot
  balance-timer scenarios): scoped halt fires only on the `Confirming` slot(s), leaves `Watching`
  slots' `entry_suppressed` untouched; balance-increase suppresses the scoped halt.
- `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo fmt --all --check`.

## 6. Deploy

Same as prior changes to this binary: `scripts/deploy_trader.sh` cross-compiles `live` for
aarch64, rsyncs binary + config to Oracle, `systemctl restart trader-live.service`.
