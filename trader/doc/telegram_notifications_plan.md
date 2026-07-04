# Richer Telegram notifications for the Rust trader

> Status: planned, not yet implemented. Written 2026-07-03, deferred to investigate a
> live incident (wrong WIN message vs. actual balance loss) first.

## Context

Right now the Rust trader's Telegram bot sends exactly one kind of push message —
`💰 <b>{asset}</b> {time} {SIDE} {OUTCOME} pnl={pnl}` — fired only when a trade closes
(win/loss/stop-loss/unwind, `trader/src/bin/live.rs`'s `Action::LogTrade` handler in
`process_actions`). There is no message when an order is actually placed, which is why
the user saw a win message with no corresponding entry notice.

Studying `bot/telegram_bot.py`'s `_event_loop()` (the Python bot's actual production
notification path — confirmed via its `WorkerEvent` queue architecture, not the unused
`_tail_loop()` log-tailer) shows the intended shape: a `trade_placed` message at entry, an
immediate minimal `stop_loss` ping when a stop triggers, a rich `cycle_result` message at
close, and an `api_result_update` alert when Polymarket's actual resolution disagrees with
the bot's own Binance-based estimate. Rust has no equivalent of the first two at all, and
while `TradeRecord`/`Confirming`/`EnrichOnly`/`Event::ApiResult` already model the
estimate-vs-actual comparison in `worker.rs`, **nothing in `live.rs` ever produces an
`ApiResult` event in production** — that whole mismatch path only exists in unit tests
today. Making "alert on mismatch" real requires adding the missing piece: a background
poller against Polymarket's Gamma API (`bot/worker.py:_fetch_api_went_up`, no Rust
equivalent), confirmed in scope by the user.

`har`/P(up)/SNR (`bot/signals.py`'s `VolHarSignal`/`PUpSignal`/`SnrSignal`) are computed
in Python at entry and shown in its `cycle_result` message, but **have no Rust port at
all** (`trader/src/strategies.rs` has no HAR concept; `config.rs` only parses
`har_beta`/`har_nu`/`har_pup_enabled` from the TOML for schema compatibility). Per the
user's own instruction, these fields are simply omitted from every Rust message — not
faked, not stubbed, just left out — porting HAR itself is a separate, much larger task.

## Plan

### 1. `trader/src/worker.rs` — small new accessors + Action variants

- Add two read-only accessors alongside the existing `is_halted()`/`has_open_position()`/
  `delta_pct()` pattern:
  - `pub fn cycle_end_ts(&self) -> f64` — for "time left in cycle" at entry/SL-trigger.
  - `pub fn cycle_open_binance(&self) -> f64` — for the enriched close message's cycle
    price move.
- Add two new `Action` variants (`Action::LogTrade`'s shape is untouched — several tests
  pattern-match `Action::LogTrade(_)` directly, so this avoids churn there):
  ```rust
  LogTradeCorrection { previous_outcome: Outcome, previous_pnl: f64, record: TradeRecord },
  StopLossVerdict { record: TradeRecord, would_have_won: bool },
  ```
- Restructure `on_api_result` (currently ~worker.rs:589-611):
  - `Confirming` branch: capture `previous_outcome`/`previous_pnl` from the *original*
    record before mutating; on `flip_needed`, emit `Action::LogTradeCorrection { .. }`
    instead of `Action::LogTrade` (non-flip case unchanged — just `Action::Persist`).
  - `EnrichOnly` branch: if `record.outcome == Outcome::StopLoss`, compute
    `would_have_won` by relativizing `won` to `record.side` and emit
    `Action::StopLossVerdict { .. }`. If `record.outcome == Outcome::Unwind`, emit nothing
    new (matches Python's explicit `if is_unwind: continue` skip — a take-profit exit
    doesn't need a counterfactual verdict). Still never touches pnl/result/halt, per the
    existing comment.
- Update the two existing `on_api_result` tests (`api_result_flips_confirming_outcome_and_recomputes_pnl`
  looks for `Action::LogTrade`, needs to look for `Action::LogTradeCorrection` instead;
  `api_result_on_enrich_only_never_touches_pnl` uses an `UnwindFilled` record, so add an
  assertion that no `StopLossVerdict` fires for it) and add one new test constructing a
  `StopLoss` `EnrichOnly` record to confirm `StopLossVerdict` fires with the right
  `would_have_won`.

### 2. `trader/src/marketdata.rs` — Gamma resolution fetch

Add `pub async fn fetch_gamma_resolution(http: &reqwest::Client, slug: &str) -> Option<bool>`,
a direct port of `bot/worker.py::_fetch_api_went_up` (lines 941-976): `GET
https://gamma-api.polymarket.com/events?slug={slug}`, parse `events[0].markets[0]`'s
`outcomes`/`outcomePrices` (JSON-encoded strings, need an inner `serde_json::from_str`),
return `Some(true)` if the "Up" outcome's price ≥ 0.99, `Some(false)` if "Down" ≥ 0.99,
else `None` (not yet resolved, or any fetch/parse error — mirrors Python's broad
`except Exception: return None`). No proxy needed — Gamma is a read-only public endpoint,
same as the existing unproxied `fetch_meta` calls; only CLOB *order writes* need the EC2
proxy.

### 3. `trader/src/bin/live.rs` — the resolution watcher + wiring

- Add `http: reqwest::Client` to `Driver` (cloned from the same `http_client()` already
  built in `main()` — cheap, `reqwest::Client` is `Arc`-backed internally).
- New unbounded channel `(String /* asset */, &'static str /* strategy */, bool /* won */)`
  for watcher → main-loop handoff, plus a new `tokio::select!` branch that finds the
  matching slot (`assets.iter_mut().find(|s| s.worker.asset == asset && s.worker.strategy_name == strategy)`)
  and feeds `Event::ApiResult { won }` through `process_actions`, exactly like the
  existing binance/poly tick branches.
- New helper `spawn_resolution_watcher(http, slug, side, asset, strategy, tx)`: a
  `tokio::spawn`ed loop matching Python's cadence — up to 20 attempts, sleep 30s before
  each check (~10 min ceiling) — calling `fetch_gamma_resolution`, relativizing the result
  to `side`, sending `(asset, strategy, won)` and returning on the first resolved
  attempt; gives up silently (with a log line) after 20 tries.
- In `process_actions`'s existing `Action::LogTrade(rec)` arm: after the existing
  bookkeeping, spawn this watcher unconditionally (works for all four outcomes — the
  state machine itself decides Confirming vs EnrichOnly vs Unwind-skip, so the call site
  doesn't need to branch).
- New match arms in `process_actions` for `Action::LogTradeCorrection` (reverse the
  `previous_outcome`'s tally on `slot.wins`/`slot.losses`, apply the new outcome's tally,
  adjust `slot.total_pnl` by `record.pnl - previous_pnl`, append the corrected row via the
  existing `log_trade()` — CSV stays append-only, so a corrected trade produces two rows;
  no CSV schema change) and `Action::StopLossVerdict` (message-only, no counter/CSV
  changes, matching the "never rewrites pnl/result/halt" contract).
- In `execute()`'s `Action::PlaceBuy` arm: after a successful fill
  (`result.placed && result.filled_shares > 0.0`), send the entry notification. On the
  else branch (rejected / zero fill), send the order-rejection notification instead.
- In `execute()`'s `Action::ClosePosition` arm, `reason == CloseReason::StopLoss` case:
  send the stop-loss-triggered notification *before* calling `self.engine.close_position(..)`
  (so it fires immediately on trigger, independent of whether the close itself
  succeeds) — side is derived from `token_id == slot.up_id` (no new Worker accessor
  needed), trigger price from `slot.last_poly_up`/`last_poly_dn`.
- Enrich the existing `Action::LogTrade` message (replacing the current bare
  `💰 <b>{asset}</b> {summary}`).

### 4. Message templates

Small local `fn arrow_side(side: Side) -> &'static str` (`"UP ↑"` / `"DOWN ↓"`) for
display only — doesn't touch `Side::as_str()` (used by CSV logging, left alone).

```
📋 <b>{asset}</b> Order placed | {HH:MM:SS HKT} | T-{secs}s | {side} | {strategy}
price={fill_price:.4f} | delta={delta_pct:+.3f}%
```
```
🛑 <b>{asset}</b> STOP LOSS triggered | {HH:MM:SS HKT} | T-{secs}s | {side} | {strategy}
price={trigger_price:.4f} | delta={delta_pct:+.3f}%
```
```
{✅|❌} <b>{asset} TRADE {OUTCOME}</b> | {HH:MM:SS HKT} | {side} | {strategy}
entry={token_price:.4f} → exit={exit_price:.4f} | cycle: ${open:.2f}→${close:.2f} | pnl={±}${pnl:.4f} | {wins}W/{losses}L
```
```
⚠️ <b>{asset} RESULT CORRECTED</b> | {HH:MM:SS HKT} | {strategy}
estimated={OLD_OUTCOME} → API={NEW_OUTCOME} | pnl {±old:.4f} → {±new:.4f}
```
```
🟢 <b>{asset} STOP GOOD</b> | {HH:MM:SS HKT} | {strategy}
market moved against the position — stop saved money
```
```
🔴 <b>{asset} STOP COSTLY</b> | {HH:MM:SS HKT} | {strategy}
market would have favored the position — stop cost money
```
```
❗ <b>{asset}</b> Order REJECTED | {HH:MM:SS HKT} | T-{secs}s | {side} | {strategy}
signal price={signal_price:.4f} | delta={delta_pct:+.3f}% | error={error}
```
(`Action::PlaceBuy` arm in `execute()`, the `else` branch of the fill-success check —
`result.placed == false || result.filled_shares <= 0.0`. Uses the signal-time `price` from
the action itself, since there's no realized fill price to report; `error` from
`TradeResult.error`, e.g. `"ORDER_FAILED"` / the underlying CLOB error string.)

### 5. Explicitly out of scope

- **HAR/P(up)/SNR/edge** — not implemented anywhere in Rust; omitted from every message
  (per the user's own instruction), not a placeholder. Porting `bot/signals.py`'s HAR
  signals is a separate, larger task if ever wanted.
- **Retry-count suffix** on the entry/rejection message (Python's `" after N retries"`) —
  `execution.rs::place()` already retries internally; it just doesn't return the attempt
  count out to the caller today. Adding that count to `TradeResult` is a small separate
  follow-up, not done here.
- **CSV schema changes** — no new columns (e.g. Python's `api_result` column); the
  verdict/correction messages are Telegram-only. A corrected trade produces a second
  append-only CSV row rather than an in-place rewrite.

## Verification

1. `cargo test --lib` in `trader/` — all existing tests plus the updated/new
   `on_api_result` tests must pass.
2. Cross-compile locally (`cross build --release --bin=live --target=aarch64-unknown-linux-gnu`),
   never on Oracle.
3. Redeploy via `scripts/deploy_oracle.py` (unchanged by this task).
4. Monitor Oracle: since real trades are infrequent/small ($1, max-trades 1 per asset),
   verify structurally via `tmux capture-pane` logs and, if a live entry occurs during the
   monitoring window, confirm the new `📋 Order placed` Telegram message arrives, and (if a
   cycle resolves during monitoring) confirm the enriched close message and — if timing
   allows — that the Gamma watcher logs a resolution check ~30s after close.
