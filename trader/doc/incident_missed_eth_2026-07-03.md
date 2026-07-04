# Incident — Rust bot missed ETH `high_prob` trade, 16:59:42 HKT 2026-07-03

Investigating why the Python bot (`btc_5mins`) took an ETH `high_prob` trade at 16:59:42
that the Rust bot (`poly_rust/trader`) never saw at all — no entry attempt, no skip log,
nothing.

## 1. The trade Python took, Rust didn't

`btc_5mins/log/trades_2026_07_03_12_35.log`:

```
2026-07-03 16:59:42,ETH,UP ↑,high_prob,WIN,WIN,18,1719.19,1719.89,1719.90,+0.7100,...
```

Cycle `1719.19 → 1719.89`, entered with 18s left in the cycle, WIN. This is cycle
`eth-updown-5m-1783068900` (16:55:00–17:00:00 HKT) — confirmed by matching
`open_binance=1719.19` in the Rust log for that exact slug.

Rust's `live_trades_eth_high_prob.csv` has no row for this cycle — nor for
`eth-updown-5m-1783069200` (17:00–17:05) either. It jumps straight from a trade at
`eth-updown-5m-1783067400` (16:30–16:35) to the next one at `eth-updown-5m-1783073100`
(18:05–18:10), skipping 8 cycles (16:35 through 18:00) entirely.

## 2. What Rust's ETH (high_prob) slot was actually doing: nothing

`trader/live_logs/live.log` shows ETH `reversal` heartbeats running continuously straight
through 16:35–17:10 (lines ~358–490: `new cycle`/`heartbeat ETH (reversal)` every 5
minutes, no gaps). But **`ETH (high_prob)` heartbeats stop completely after cycle
`1783067400` and don't resume until `1783069800`** (17:10) — an exact, clean 40-minute
gap with zero log lines for that strategy, while its sibling `ETH (reversal)` slot (same
asset, same process, same event loop) kept ticking normally the whole time. That rules
out a network/feed/exchange outage — only one of ETH's two strategy slots went dark.

The last thing that slot did before going dark:

```
[TRADE] TradeRecord { slug: "eth-updown-5m-1783067400", cycle_start: 1783067400.0,
  strategy: "high_prob", side: Up, entry_ts: 1783067680.0, token_price: 0.88...,
  exit_price: 1.0, outcome: Win, pnl: 0.1364, ... }
```

— a completed (Win) trade, at 16:30–16:35.

## 3. Root cause: `--max-trades 1` permanently retires a slot after its first trade, for the life of the process

`bin/live.rs`'s per-tick cycle-open gate (`:798`):

```rust
if slot.last_binance <= 0.0 || slot.trades_completed >= args.max_trades {
    slot.current_slug = None;
    continue;
}
```

`trades_completed` is incremented once per finished trade and never decremented
(`:428`). The live process is launched with `--max-trades 1` (confirmed by the log
banner: `[live] assets=BTC,ETH,DOGE size_usdc=$1.00 max_trades=1 ...`, and by the
deploy template in `plan_rust_module.md:1209`). So the instant a slot's
`trades_completed` reaches 1, every subsequent tick takes this branch: it clears
`current_slug` and `continue`s past the `fetch_meta`/`CycleOpen` code that would open
the next cycle. **There is no code path that ever re-arms a slot once it hits its cap —
only a full process restart, which zeroes `trades_completed` back to 0 for every slot,
gets it trading again.**

This isn't a shutdown-and-relaunch-immediately situation either: the process only
self-terminates once *every* asset/strategy slot has hit the cap (`:750`,
`"all assets reached max_trades — shutting down cleanly"`), which requires BTC
`reversal`, DOGE `reversal`, and ETH `reversal` to also each complete a trade — something
that can take arbitrarily long. In this incident, ETH `high_prob` capped out at 16:35 and
the other three slots hadn't all capped yet, so the process kept running for 40 more
minutes with that one slot silently dark, until:

```
[live] shutting down (SIGTERM).
[live] assets=BTC,ETH,DOGE size_usdc=$1.00 max_trades=1 log_dir=...
...
[live] new cycle ETH (high_prob) slug=eth-updown-5m-1783069800 open_binance=1727.95
```

— an external SIGTERM (manual restart/redeploy, not the cap-reached path — that prints a
different message and wasn't logged here) happened to land at 17:11:53, which is what
coincidentally reset the counters and let ETH `high_prob` resume. Had that restart not
happened when it did, the slot would have stayed dark for however much longer it took the
other three slots to each land their own trade — this specific recovery time was luck,
not a bound anything guarantees.

**This is not a one-off.** As of the last line in `live.log` (18:20, cycle
`1783074000`), ETH `high_prob` is dark *again* — it capped out on the
`eth-updown-5m-1783073100` (18:05–18:10) trade and hasn't opened a new cycle since. It
will stay dark until the next restart, whenever that happens to occur.

The Python bot has no equivalent per-run trade cap — it re-arms and evaluates every
5-minute cycle indefinitely, which is why it caught the 16:59:42 entry that Rust
structurally could not have taken regardless of any signal-timing or execution-quality
issue.

## 4. Why this exists / how it got here

`--max-trades` (default 1) is a deliberate bounded-risk guard for **manual test runs**
— see README.md:290 ("A live BNB test..., max-trades 1) bought 1.0752 shares...") and
`plan_rust_module.md:1133` ("max_trades=1, new account, ... confirmed routing"). It was
designed to cap exposure while validating a fresh strategy/account, not to gate an
always-on production strategy. The systemd deployment (`Restart=always`,
`plan_rust_module.md:1163`) combined with that same `--max-trades 1` flag means the
"bounded test" semantics leaked into the production launch config: any strategy that
finishes its one allotted trade before its siblings do goes dark for an unbounded,
unpredictable stretch — bounded only by how long until the next process restart, which
today has mostly been happening as a side effect of active deploys (this is the same day
as the DOGE oversell fix, the halt-reset fix, and the retry-storm fix — see
`incident_doge_2026-07-03.md`, `incident_halt_reset_2026-07-03.md`).

Worth noting restarts aren't free either: they drop any in-flight
`spawn_resolution_watcher` task (harmless here — it's best-effort reconciliation, see
`incident_doge_2026-07-03.md`), and — until today's fix — silently cleared any active
`/halt` (`incident_halt_reset_2026-07-03.md`). Relying on restarts to re-arm capped slots
therefore isn't a safe substitute for fixing the cap logic itself.

## 5. Proposed fix

**Recommended: stop using a hard per-process trade-count cap for strategies meant to run
continuously.** Raise `--max-trades` to a value no strategy could plausibly reach in a
trading day (or remove the cap path for the live binary entirely), and rely on the
mechanisms already built for actual risk control — `BalanceGuard`/drawdown halt and
manual `/halt` — instead of an incidental trade-count stop. This restores "always ready
for the next cycle" behavior matching the Python bot and removes the dark-slot failure
mode structurally, rather than depending on restart timing to bound it.

Alternative, if a genuine "N trades per day" cap is wanted: reset `trades_completed` on
a real calendar-day boundary (HKT midnight) inside the driver loop, rather than tying it
to process lifetime — so a slot that caps out mid-day re-arms on its own the next day
without needing an external restart at all. This is more work and isn't obviously needed
given `BalanceGuard` already exists for risk bounding; recommend the simpler fix above
unless there's a specific reason a hard per-day trade cap is wanted independent of P&L.

Not changed / out of scope: the entry-timing/signal logic itself (both bots agreed this
was a real signal, Rust just structurally couldn't act on it); the DOGE oversell and
halt-reset bugs (separate incidents, already fixed elsewhere).
