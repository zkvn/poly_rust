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

**Root cause: two independent unconditional state resets clobber `Confirming`/`EnrichOnly`
on a cycle boundary — and the more severe of the two fires within about a second of the
trade closing, not "one cycle later" as first thought (see the correction below).**

```rust
// worker.rs:677-698 — on_cycle_open, fires for every asset/strategy on every 5-min boundary
fn on_cycle_open(&mut self, ctx: CycleContext, slug: String) -> Vec<Action> {
    ...
    // A fresh cycle never inherits an in-flight position from the last one
    // (each cycle's trade is fully resolved before the next opens).
    self.state = WorkerState::Watching;   // <-- fires even when state == Confirming(_)
    ...
```

```rust
// worker.rs:709-724 — on_cycle_close's fallback, same bug, defense-in-depth case
fn on_cycle_close(&mut self) -> Vec<Action> {
    let holding = match &self.state { /* Holding-family */ _ => None };
    let Some(h) = holding else {
        self.state = WorkerState::Watching;   // <-- fires even when state == Confirming(_)
        return vec![];
    };
    ...
```

**Correction to the initial read of this bug:** `bin/live.rs`'s ticker (lines 1390–1448)
fires `Event::CycleClose` for the ending cycle and then, in the same loop iteration for that
same worker, `Event::CycleOpen` for the next one — separated only by an async `fetch_meta`
Gamma call, typically well under a second. So `on_cycle_open`'s unconditional reset (not
`on_cycle_close`'s) is the one that actually fires here, and it does so almost immediately
after `Confirming` is set — long before the resolution watcher's first poll (which, at the
old 30s-per-attempt cadence, couldn't even *ask* Gamma until 30s had passed). Read literally,
this means the correction path should fail **every time**, not just when Gamma is slow — yet
`live.log` also has two earlier examples of it succeeding
(`grep 'API-corrected' trader/live_logs/live.log`, both 2026-07-03). The likely explanation:
`bin/live.rs:1402-1405` skips `CycleOpen` entirely for a slot whose `last_binance` reads `<=
0` that tick (a stale/gapped Binance feed) — leaving `current_slug = None`, so that worker's
next `CycleOpen` doesn't fire until the *following* 5-minute boundary. Both surviving
corrections coincide with retry-storm incidents already documented elsewhere in this repo
(`exit_attempts: 284` and `exit_attempts: 3`), which independently suggests a rough patch for
that process around then, plausibly including a feed gap. In other words, the correction path
appears to only have ever worked *by accident*, when something else was already going wrong —
under normal, healthy operation it silently drops every time, well within its own advertised
10-minute ceiling, because the state it needs is gone in under a second.

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

## 4. Decision (direction given 2026-07-09, verbatim in §6) and fix design

Direction: prefer **halt over guess**. If Gamma hasn't resolved by the time it would matter,
stop trading that asset/strategy and wait for a human, rather than silently keep going on an
unverified result. Explicitly **not** doing the `trade_reconcile.py` → Telegram wiring (§3b,
fix 2 in the original draft) — kept out of scope, Python stays un-mixed with the live Rust
path.

1. **Stop both `on_cycle_open` and `on_cycle_close` from clobbering an in-flight
   confirmation.** Neither may reset `self.state` to `Watching` when it's already
   `Confirming`/`EnrichOnly`. This alone would leave a worker permanently stuck if Gamma
   never resolves, so it ships together with fix 2, never alone.

2. **Replace the resolution watcher's cadence and add a hard deadline + halt.** Gamma
   "usually won't give you anything until 20 to 60 seconds after cycle end" (direction,
   verbatim) — until then, polling is free: nothing in either strategy can fire an entry
   near the start of a fresh cycle anyway (`reversal` is explicitly gated by
   `reversal_start_time`, currently 120s; `high_prob` only evaluates inside
   `enter_when_time_left` of cycle *end*, i.e. the last ~10–20s). So: retry Gamma every
   **1 second** (cheap, and there's no missed-trade cost to waiting) starting right after
   the position closes, until either (a) it resolves — apply the correction if needed,
   clear back to `Watching`, trade normally for whatever's left of the window — or (b) the
   asset's own `reversal_start_time` deadline elapses unresolved — in which case **send a
   Telegram alert and halt new entries for that asset/strategy** (reusing the existing
   `entry_suppressed` / `/resume` mechanism that manual `/halt` and the balance-drawdown
   halt already use), leaving the original provisional record as-is for manual review. No
   further auto-retry after the deadline — a human decides when it's safe to `/resume`.

3. **Log the previously-silent `on_api_result` branches.** Both the "no flip needed"
   confirmation and the new deadline/halt path get an explicit log line — this incident
   was only diagnosable by cross-referencing watcher attempt counts against cycle-boundary
   log lines after the fact; a direct line removes that step next time.

**Implementation:** `trader/src/worker.rs` and `trader/src/bin/live.rs` — see the commit
that follows this doc for the exact diff. New unit tests cover: `Confirming` surviving both
a `CycleOpen` and a `CycleClose` with nothing to hold; the timeout path halting `Confirming`
(entry suppressed, original record untouched) but *not* halting `EnrichOnly` (advisory-only,
per its existing "never touches pnl/result/halt" contract); and the diagnostic log actions
firing on both the no-flip-needed and stale-state branches.

## 5. Not in scope

- `trade_reconcile.py` → Telegram wiring (§3b) — explicitly deferred, Python stays
  git-only; flagged in top-level `README.md`'s `## TODO`.

## 6. Q&A log

**Q (verbatim):** "silent-wrong-pnl bug for a silent-frozen-worker bug is great trade off
for me, I need it to be accurate and under control, I'd rather stop than going forward
blindly, so yes, if gamma doesn't resolve, halt the market from future cycles until it's
clear. this part may needs further consideration, gamma API usually won't give you anything
until 20 to 60 seconds after cycle end, you can add a retry for gamma with 1 second sleep,
it won't affect anything as there is no signal or any chance to place a bet around beginning
of the cycle, so it's safe to retry until that reveral_start_time config value, currently at
120 seconds into a new cycle, if gamma returns before that, it's ok to trade the new cycle,
otherwise send an error message to telegram and halt the market, leave it for user manual
investigation . make sure it is super clean" / "I don't want python recon mixed, so skip
this one" / "log is good idea, definitely we need such info"

**A:** Implemented exactly as directed — §4 above. `on_cycle_open`/`on_cycle_close` no
longer clobber `Confirming`/`EnrichOnly`; the watcher polls every 1s and gives up exactly
at `reversal_start_time` seconds after the position closed, halting (not guessing) on
timeout via the existing halt/`entry_suppressed`/`/resume` plumbing; `trade_reconcile.py`
untouched, per direction; diagnostic log lines added on every previously-silent branch.

## 7. Follow-on refinements (2026-07-09, same day; extended 2026-07-11)

Two more changes to the same watcher, same day as the fix above:

1. **Polling cadence became delayed and config-driven**, not an immediate hardcoded 1s
   loop — Gamma "usually won't give you anything until 20-60s after cycle end," so the
   watcher now waits `gamma_poll_delay_secs` (new per-asset config, default 60s, clamped
   to the deadline) before its first attempt, then retries every
   `gamma_poll_interval_secs` (new per-asset config, default 3s).
2. **A balance-based override on the halt itself.** If Gamma still hasn't resolved at the
   deadline, `bin/live.rs` also checks a new `GammaBalanceTracker` (`balance.rs`) — a
   rolling comparison of the account's balance at this cycle's periodic checkpoint against
   the previous cycle's checkpoint. If balance is up, the slot does **not** halt — the
   provisional record still stands, unverified, but new entries continue
   (`Action::GammaUnresolvedContinued` instead of `Action::GammaHaltEngaged`, still
   Telegram-alerted). An unknown/failed balance sample fails safe to *not* skipping the
   halt, matching this doc's own "halt over guess" rule (§6). Never clears a halt from
   another source (manual `/halt`, loss-streak, drawdown) — only ever *adds* suppression.

   **Risk tradeoff accepted here, worth being explicit about:** this is a deliberate
   loosening of the exact halt this incident introduced. A wrong provisional WIN/LOSS can
   now go un-halted — and so un-flagged for manual review — if the rest of the account
   happens to be net up that cycle. The mitigating factor is that the provisional record
   itself is unchanged and still marked unverified in the CSV/Telegram either way, so
   `trade_reconcile.py`'s independent daily Gamma cross-check still catches it the next
   day even if the live halt doesn't fire same-day.

**Extended 2026-07-11** — Gamma's deadline decoupled from `reversal_start_time` (now its own
`gamma_poll_deadline_secs`, default 600s) and the balance-decrease halt scoped to the specific
asset+strategy involved instead of process-wide. Full plan: `plan_gammapi_2026-07-11.md`
(same directory).
