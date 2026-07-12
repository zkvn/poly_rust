# Trade Audit — 2026-07-12

Two questions raised: (1) BTC `reversal` has fired zero live trades in the past few days — is
that a bug? (2) A DOGE order/stop-loss Telegram alert seen today (09:33:40–09:34:27) doesn't
appear in poly_rust's records — is it missing, and is it related to today's `price_feed`
collector-crash-loop fix (`price_feed/doc/incident_collector_data_loss_2026-07-12.md`)?

**Both resolved, no bug found in poly_rust, no code changes made.**

## 1. BTC `reversal` — zero trades, confirmed genuine via backtest replay

`trader/live_logs/live_trades_btc_reversal.csv` has exactly 2 rows total, both from
**2026-07-06/07** (`btc-updown-5m-1783348200` UNWIND +0.0779, `btc-updown-5m-1783357200`
UNWIND +0.1031) — nothing since. `strategy_20260709.toml`'s own meta comment confirms BTC's
`[strategies]` assignment (`default = ["reversal"]`, BTC not overridden) has been unchanged
since at least 2026-07-08, so the strategy has been continuously live for the whole dry spell,
not just "the past few days."

**Backtest replay independently reproduces zero trades**, run against the real recorded price
data for every day with available `backtest_prices/BTC_*` parquet in the window (`./target/release/backtest --asset BTC --date <date> --prices-dir backtest_prices --config-dir config`, both with and without `--no-halt`):

| Date | With halt | `--no-halt` |
|---|---|---|
| 2026-07-09 | No trades. | No trades. |
| 2026-07-10 | No trades. | No trades. |
| 2026-07-11 | No trades. | No trades. |
| 2026-07-12 | No trades. | No trades. |

8/8 runs agree with live. The `--no-halt` runs matching the halted runs rules out halt
suppression as the cause — confirmed independently by `live_state_btc_reversal.json` currently
reading `halt_losses: 0, entry_suppressed: false` (BTC itself isn't halted right now).

**Root cause: not a bug — the entry condition is a large, infrequent intra-cycle swing.**
`ReversalStrategy::evaluate` (`trader/src/strategies.rs:42-85`) only fires on a genuine
dip-and-recover: the side must first dip below `reversal_low_threshold` (default `0.20`, BTC
uses the default) *and then* recover above `reversal` (`0.55` for BTC) within the same 5-minute
cycle, after `reversal_start_time` (120s in) and with `delta_pct` confirming direction. That's a
0.20→0.55+ swing inside one cycle — for context, the config's own full-history study
(`studies/unwind_safely/results/full_history_*_20260708_153401.md`, cited in
`strategy_20260709.toml`'s comments) found only **98 BTC reversal trades** across its entire
history window, versus 429 for DOGE and 186 for ETH — BTC reversal was already the rarest-firing
of the three assets before this dry spell, consistent with 0 trades being a quiet-market
continuation rather than a break from baseline.

**Caveat, doesn't change the conclusion:** `price_feed`'s collector crash-loop
(`price_feed/doc/incident_collector_data_loss_2026-07-12.md`) collapsed tick coverage to ~14-15%
for 2026-07-11 22:30 onward through 2026-07-12 15:08, which could in principle make a
tick-by-tick backtest miss a real trigger for the 07-11 (partial) and 07-12 runs specifically.
This doesn't undermine the finding: 07-09 and the first ~22.5 hours of 07-10 had normal ~93%
coverage (crash-loop started 07-10 22:30) and *also* independently produced zero trades, so the
zero-trade result holds on unaffected data too.

**Conclusion: working as designed.** No code change proposed.

## 2. The 09:33:40–09:34:27 DOGE order/stop-loss — not poly_rust's trade, a different bot's

The order/stop-loss pair the user saw is from the **Python bot (`btc_5mins`)**, a separate
trading system on the same Oracle box — not poly_rust/`trader-live.service`. It isn't "missing"
from poly_rust's records because it was never poly_rust's trade to record, and it has no
relationship to today's `price_feed` collector-crash-loop fix (a different pipeline entirely —
see below).

**Message format match, not poly_rust's own format.** poly_rust's Telegram templates
(`trader/src/bin/live.rs:742`, `:822`) read `"... | price={:.4} | delta={delta_pct:+.3}% |
{clob_latency_str} | ... | n_attempts={}"` and `"STOP LOSS triggered | ..."`. The pasted alert
(`token price=0.8250`, bare `"DOGE STOP LOSS"` with no "triggered") instead matches
`btc_5mins/bot/telegram_bot.py:1411` and `:1456` byte-for-byte:
```python
f"📋 <b>{asset}</b> Order placed{retry_suffix} | {ts} | T-{int(time_left)}s | {side} | {entry_type}\n"
...
f"🛑 <b>{asset} STOP LOSS</b> | {ts} | {side}"
```

**Ground truth confirms it's `btc_5mins`'s trade.** Its own trade log on Oracle,
`/home/ubuntu/apps/btc_5mins/log/trades_2026_07_10_10_45.log`, has the exact row:
```
2026-07-12 09:34:27,DOGE,UP ↑,stop_loss_reversal,STOPLOSS,pending,33,0.07,0.07,0.07,+0.0000,
+0.000273,+0.0000,+0.000273,0.07,0.07,1,0,0.8250,-0.5342,,0.8508,1.0400,+0.0758,0.00150619,
dry_run,0.3850,0.3850
```
`bet_entry_price=0.8250` matches the alert's `token price=0.8250` and the timestamp matches
exactly.

**poly_rust's own log for the same cycle shows no such trade — confirmed from the source
machine, not a sync artifact.** `trader-live.service` on Oracle has been running continuously
since `2026-07-11 13:24:26` with zero restarts (`systemctl status`), so its `live.log` is one
unbroken record. The matching 09:30:00–09:35:00 HKT cycle (`doge-updown-5m-1783819800`) has a
full heartbeat trace but **no "Order placed" or "STOP LOSS" line for DOGE anywhere in it** —
poly_rust's own DOGE-UP probability read `0.355 → 0.650 → 0.375` across the T-93s/T-63s/T-33s
heartbeats, never printing anything near the `btc_5mins` alert's `0.8250`. This also rules out
the "local copy is just stale" explanation: `live_trades_doge_reversal.csv` is byte-identical
between the local repo and Oracle (`stat` confirms same mtime, `2026-07-11 09:38:48`, on both) —
there's nothing unsynced; poly_rust genuinely produced zero DOGE reversal activity in this
cycle.

**Not related to the `price_feed` collector-crash-loop fix.** That incident is specific to
`poly-collector.service`'s parquet-writing pipeline (`price_feed/src/collect.rs`,
`reconcile.rs`) and its downstream NATS bridge to `trader-live.service`
(`nats://127.0.0.1:4222`). `btc_5mins`'s live worker (`bot/worker.py`) has no NATS references at
all — grepped `bot/*.py` for `nats`/`NATS`, zero hits — it runs its own independent price
ingestion, separate from poly_rust's collector. Even if `poly-collector` had been mid-crash-loop
at 09:33 today (it wasn't confirmed either way for this exact minute, but the incident doc's
window is 2026-07-10 22:30 → 2026-07-12 15:08, which does span this time), that pipeline only
feeds poly_rust's own trader and its historical parquet — it structurally cannot explain a gap
in a different bot's independent log.

**Observation, not investigated further (out of poly_rust's scope):** `btc_5mins`'s own row
shows `api_result=pending` and `exit_fill=dry_run` for this stop-loss — worth the user checking
in that project directly if they want to confirm the stop-loss was a real fill vs. a
dry-run/simulated exit, since that determines whether real money was actually at risk here.

## Conclusion

No poly_rust bug in either case; no code changes made. BTC reversal's dry spell is a real,
backtest-confirmed quiet period for a rare-firing strategy variant, not a fault. The DOGE
alert belongs to `btc_5mins`, an entirely separate bot/pipeline on the same box.
