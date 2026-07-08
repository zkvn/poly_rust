# Incident — DOGE trade logged/alerted as WIN despite Polymarket resolving it a LOSS, 2026-07-09

Telegram alert (04:50:00 HKT):

```
✅ DOGE TRADE WIN | 04:50:00 | DOWN ↓ | high_prob
entry=0.9300 → exit=1.0000 | cycle: $0.07→$0.07 | pnl=+$0.0704 | 1W/0L
```

Two separate problems, confirmed independently:

1. The pnl shown is **positive** (+$0.0704) but the real Polymarket resolution for this
   market was the opposite outcome — the position actually lost.
2. No Telegram message ever flagged the mismatch, even though (as shown in §4) the
   machinery to do exactly that exists and, in one of its two independent paths, *did*
   correctly detect it — just never told anyone.

## 1. Ground truth: the CSV row and the daily recon cross-check agree it's wrong

`trader/live_logs/live_trades_doge_high_prob.csv`:

```
logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error,...
1783543800.43,doge-updown-5m-1783543500,high_prob,DOWN,1783543787.03,0.9300,1.0,WIN,0.0704,0,,...
```

`trader/results/daily_recon/trade_recon_2026-07-08_to_2026-07-09.md` (generated the same
morning at 06:20 HKT by `trade_reconcile.py`, which independently re-queries the Gamma
API for every trade's actual resolution):

```
### Mismatches

| Time | Slug | Side | Logged | Gamma Actual |
|---|---|---|---|---|
| 2026-07-09 04:50:00 | doge-updown-5m-1783543500 | DOWN | WIN | LOSS |

**Any row here means worker.rs's own ApiResult-correction path (`Action::LogTradeCorrection`)
let a wrong WIN/LOSS through — treat as a bug, not noise.**
```

So the recon script itself already says, in its own generated output, that this is a bug
in the live correction path, not a data question. That comment (from a previous pass)
turned out to be exactly right — see §3.

## 2. Why the pnl was ever wrong in the first place (Issue 1)

`worker.rs::on_cycle_close` (lines 709–733) determines WIN/LOSS **provisionally**, from
the trader's own Binance feed, not from Polymarket's actual settlement:

```rust
let price_moved_up = self.last_binance > self.cycle_open_binance;
let won = match h.side {
    Side::Up => price_moved_up,
    Side::Down => !price_moved_up,
};
let exit_price = if won { 1.0 } else { 0.0 };
let pnl = settle_pnl(&h, exit_price);
let outcome = if won { Outcome::Win } else { Outcome::Loss };
```

This is a snapshot comparison of `last_binance` (the most recent Binance trade tick this
process happened to have received) against `cycle_open_binance` (the same, captured at
cycle start). It is **by design an estimate** — the very next state, `Confirming`, exists
specifically because this call can be wrong: Polymarket's actual resolution can come from
a different price source/timing than this process's own Binance WS tick stream, and a
tick landing a few hundred ms on the wrong side of the cycle boundary is enough to flip
the call. The codebase already anticipated this (see `on_api_result` / `Action::LogTradeCorrection`,
§3) — it just isn't itself a bug that the provisional call is sometimes wrong. What's a bug
is that the safety net built to catch it (§3) silently failed here.

Concretely, for this trade: Binance's `last_binance` was not above `cycle_open_binance` at
the moment of cycle close, so `Side::Down` was scored `won=true`. Gamma's actual resolution
(per the recon cross-check, §1) was the opposite. The Telegram message at line 917–924 of
`bin/live.rs` reports exactly this provisional `outcome`/`pnl` — there is no hedge or
"estimated" qualifier in the message copy, so it reads as final when it isn't yet.

## 3. Why nothing corrected it (Issue 2) — two independent failures

### 3a. The live in-process correction path silently dropped the result

There's a designed-for-this mechanism: after any Win/Loss `LogTrade`, `bin/live.rs::execute`
(lines 926–933) spawns `spawn_resolution_watcher` (`bin/live.rs:124–149`), which polls Gamma
every 30s (up to 20 attempts, ~10 min ceiling) and sends `(asset, strategy, won)` back into
the event loop as `Event::ApiResult`. `worker.rs::on_api_result` (lines 1226–1279) is supposed
to flip the record and fire a "⚠️ RESULT CORRECTED" Telegram message
(`bin/live.rs:955–961`) if the real result disagrees with the provisional one.

`live.log` shows the watcher running normally at first:

```
[TRADE] TradeRecord { ..., outcome: Win, pnl: 0.0704, ... }
[telegram] sent: ✅ <b>DOGE TRADE WIN</b> | 04:50:00 | DOWN ↓ | high_prob
[live] API pending (attempt 1/20) for doge-updown-5m-1783543500
[live] API pending (attempt 2/20) for doge-updown-5m-1783543500
...
[live] API pending (attempt 7/20) for doge-updown-5m-1783543500
```

— then **nothing**. No attempt 8, no `gave up waiting... after 20 attempts`, no
`API-corrected`, no `RESULT CORRECTED` Telegram message, and the CSV was never rewritten
with a corrected row. The watcher task didn't crash (the process kept running normally for
other assets) and didn't time out (no "gave up" line) — the only way to stop logging
`API pending` without either of those is for `fetch_gamma_resolution` to have finally
returned `Some(...)` and the loop to `return`ed after sending on `tx`. So the correct
answer *was* fetched — it just never took effect.

**Root cause: `on_cycle_close`'s fallback unconditionally clobbers `Confirming`/`EnrichOnly`
state on every cycle boundary, not just when a new position needs recording.**

```rust
// worker.rs:709-724
fn on_cycle_close(&mut self) -> Vec<Action> {
    let holding = match &self.state {
        WorkerState::Holding(h)
        | WorkerState::Unwinding(h)
        | WorkerState::StopExiting(h)
        | WorkerState::TimingOut(h) => Some(h.clone()),
        _ => None,
    };
    let Some(h) = holding else {
        self.state = WorkerState::Watching;   // <-- fires even when state == Confirming(_)
        return vec![];
    };
    ...
```

`CycleClose` fires for *every* asset/strategy worker on *every* 5-minute cycle boundary,
regardless of whether that worker did anything this cycle. When a trade closes and enters
`Confirming`, the worker correctly can't open a new position that cycle (`try_enter` requires
`WorkerState::Watching`) — but the very next `CycleClose` event (one cycle later, i.e. up
to 5 minutes after the trade, which is *inside* the resolution watcher's up-to-10-minute
window) hits the `else` branch above with `holding = None` (there's nothing to close,
correctly) and, as an unconditional side effect, stomps `Confirming(record)` back to
`Watching`. Once that happens, the eventual `Event::ApiResult` lands in `on_api_result`
(`worker.rs:1226`) with `self.state` no longer matching `WorkerState::Confirming(_)`, so it
falls through to the silent `_ => vec![]` arm (line 1279) — no log line, no Telegram
message, no CSV write, nothing observable at all. This matches the evidence exactly: the
watcher's polling (real, 30s-spaced, independent of the cycle FSM) kept going and got an
answer; the FSM had already discarded the place to put it.

This is a race, not a guarantee-to-fail: if Gamma resolves within one cycle length (~5 min)
of the trade closing, the correction still goes through fine — most of this bot's Win/Loss
trades likely do get corrected successfully, which is presumably why this hadn't been
caught by casual observation before the recon script's independent cross-check (§1) started
running. But any resolution that takes longer than one cycle — anything at or beyond the
~5-minute mark, well inside the watcher's own advertised 10-minute ceiling — is silently
dropped by construction, every time.

### 3b. The daily Python reconciliation caught it correctly, but has no alerting

`trade_reconcile.py`'s Gamma cross-check (§1) is a *second*, independent path that also
queries Gamma per-trade, and it worked exactly as designed here — it found the mismatch
and wrote it into `trader/results/daily_recon/trade_recon_2026-07-08_to_2026-07-09.md` with
an explicit "treat as a bug, not noise" comment. But that script's only output actions are
writing the markdown file and `git commit` + `git push` it
(`trade_reconcile.py:713–721`); `trader/scripts/bash/run_daily_recon.bash` (the cron
wrapper) has no Telegram step at all. So the one system that *did* work correctly produced
a result that was only ever visible to someone who went looking at the git history or the
file directly — which is exactly how this was found (on request), not proactively.

## 4. Proposed fixes

1. **Stop `on_cycle_close` from clobbering an in-flight confirmation.** Only reset to
   `Watching` when the current state isn't already `Confirming`/`EnrichOnly`:
   ```rust
   let Some(h) = holding else {
       if !matches!(self.state, WorkerState::Confirming(_) | WorkerState::EnrichOnly(_)) {
           self.state = WorkerState::Watching;
       }
       return vec![];
   };
   ```
   **Caveat, needs to ship together with this:** today, a worker "stuck" in `Confirming`
   can't enter new trades (`try_enter` requires `Watching`). If this fix ships alone, a
   Gamma resolution that never arrives (the watcher's own 20-attempt/10-min give-up path,
   `bin/live.rs:147`, which today just logs and does nothing else) leaves the worker
   permanently unable to trade that asset/strategy again — trading a silent-wrong-pnl bug
   for a silent-frozen-worker bug. The give-up path needs to also emit an event that forces
   `self.state` back to `Watching` (leaving the original provisional record as the final
   one) *and* sends a Telegram alert ("⚠️ never got Gamma confirmation for X, kept
   provisional result") so a timeout is at least visible, unlike today.

2. **Wire `trade_reconcile.py`'s Gamma mismatches into Telegram**, not just git. Cheapest
   version: after `trade_reconcile.py --today` runs in `run_daily_recon.bash`, grep the
   generated report's "Mismatches" table and, if non-empty, POST a summary to the same
   Telegram bot/chat the live trader already notifies (credentials likely already available
   to whatever process configured the live trader's bot token). This path is a same-day,
   not same-cycle, backstop — but it's a backstop that currently exists and does nothing
   with its own findings.

3. **Log the drop, at minimum, even before the structural fix lands.** Both silent-discard
   arms in `on_api_result` (`worker.rs:1230–1233`'s "no flip needed" early return and the
   catch-all `_ => vec![]` at line 1279) currently produce zero output. Even a `println!`
   noting "ApiResult for {asset}/{strategy} arrived while state was {:?}, ignoring" would
   have made this incident discoverable directly from `live.log` instead of requiring
   correlating watcher attempt counts against cycle-boundary log lines after the fact.

Not fixed in this pass — flagged in top-level `README.md`'s `## TODO` per project
convention.
