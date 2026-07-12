# Trade Audit — 2026-07-12

Two questions raised: (1) BTC `reversal` has fired zero live trades in the past few days — is
that a bug? (2) A DOGE order/stop-loss Telegram alert seen today (09:33:40–09:34:27) doesn't
appear in poly_rust's records — assuming the signal was genuine and the trade really happened,
why did the Rust bot miss it? Could the Rust backtest catch it? If not, what do the CLOB/book
data say in Rust's defense?

**(1) is resolved clean — not a bug.** **(2) is *not* a clean exoneration** — revised below after
an initial pass wrongly focused on "whose trade is this" instead of "why didn't Rust act on a
signal its own architecture should have been able to see." The corrected finding: the entry
signal was very likely genuine, Rust's reversal engine is architecturally capable of catching it,
but whether Rust's own precondition was actually satisfied **cannot be verified**, because the
exact raw tick data needed to check is gone — destroyed by the same-day `price_feed`
collector-crash-loop bug (`price_feed/doc/incident_collector_data_loss_2026-07-12.md`). That's a
real, if indirect, connection to that incident, not "unrelated" as the first pass concluded.

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

## 2. The 09:33:40–09:34:27 DOGE trade — genuine signal, Rust's non-participation is unverifiable, not exonerated

The order/stop-loss pair belongs to the **Python bot (`btc_5mins`)**, confirmed via its own trade
log (`/home/ubuntu/apps/btc_5mins/log/trades_2026_07_10_10_45.log`, row timestamped
`2026-07-12 09:34:27`, entry price `0.8250`, matching the alert exactly) and its Telegram
template (`bot/telegram_bot.py:1411`/`:1456` match the pasted message byte-for-byte; poly_rust's
own templates in `live.rs:742`/`:822` don't). That part of the first pass was right. What was
wrong was stopping there — "it's a different bot's trade" doesn't answer whether *poly_rust*
should have caught the same move and didn't.

### The signal was very likely genuine

`btc_5mins/log/live_2026-07-12.log` has a 5-second-resolution price trace for this exact cycle
(`doge-updown-5m-1783819800`, 09:30:00–09:35:00 HKT). The relevant stretch:

```
09:33:00  T-119s  UP=0.6100
09:33:05  T-114s  UP=0.3200
09:33:10  T-109s  UP=0.3200
09:33:15  T-104s  UP=0.3150   <- closest approach to the 0.30 dip threshold
09:33:20  T- 99s  UP=0.3200
09:33:25  T- 94s  UP=0.3700
09:33:30  T- 89s  UP=0.4650
09:33:35  T- 84s  UP=0.7350   Entry skipped — delta_pct=+0.0273% does not meet ±0.0800% filter
09:33:40  T- 79s  UP=0.7750   🔄 reversal entry triggered (UP @ 0.7750), delta_pct now +0.0857%
```

Two things make this look like a real, fast market move rather than a data glitch: the price
climb from 0.32→0.775 happens over ~35 real seconds with a coherent path, not a single
discontinuous jump; and the bot's own `delta_pct` (Binance-side confirmation) crossed its 0.08%
minimum-conviction gate in the same 5-second step (`+0.0273%` → `+0.0857%`) that the entry fired
— i.e. Python's *own* filter correctly withheld the trade 5 seconds earlier for insufficient
confirmation, then fired once both the probability and the underlying-price conditions lined up.
That's a system behaving as designed on real input, not misfiring on noise.

### Rust's reversal engine is architecturally identical — this isn't a design gap

`btc_5mins/bot/strategies.py`'s `ReversalStrategy`/`SawLowSignal` and poly_rust's
`trader/src/strategies.rs`/`trader/src/signal/saw_low.rs` are the same design, ported 1:1: both
require a `SawLowSignal` latch (side's price dips below `reversal_low_threshold`, **0.30 for
DOGE** in the shared config) inside a `time_left ∈ [no_enter_when_time_left, reversal_start_time]
= [10s, 120s]` window, *then* a recovery above `reversal` (**0.60 for DOGE**) with `delta_pct`
confirming direction — and both evaluate on **every raw tick**, not on a sampled/heartbeat
cadence (`worker.rs:908-912`'s `on_poly` calls `saw_low_up.on_poly(tick)` per tick, same as
`bot/worker.py`'s `self.bus.subscribe_poly(self._sig_saw_low_up.on_tick)`). So Rust's live
process had the same *architectural* opportunity to catch this pattern that Python did — this
isn't "Python's strategy can do something Rust's can't."

**`poly-collector` was not crash-looping during the trade window itself.** `journalctl` on Oracle
shows restarts at `09:15:07` and `09:50:07`/`:12` but none between — the collector ran
continuously for the entire 09:15:07→09:50:07 stretch, which fully covers 09:33:40–09:34:27. So
NATS delivery to `trader-live.service` was not interrupted at the moment that mattered; Rust's
live process was live and receiving ticks throughout.

### But we cannot verify what Rust's own SawLow signal actually saw

The closest sampled value to the 0.30 dip threshold in Python's 5-second log is **0.3150** — only
0.015 above it, sampled every 5 seconds. `SawLowSignal`'s own doc comments (both languages) are
explicit that it exists to catch **sub-second** dip-and-recover swings a periodic sampler would
miss — meaning a real dip below 0.30 in the gaps between Python's own 5-second snapshots is
entirely plausible, and Rust's live process (evaluating every tick, same as Python) could
independently have seen the same or a different sub-threshold instant. There is no way to answer
this from Rust's own live log: **poly_rust logs heartbeats only every 30 seconds** (6× coarser
than Python's already-too-coarse-for-this 5s snapshots) — `doge-updown-5m-1783819800`'s heartbeat
trace (T-93s=0.355, T-63s=0.650, T-33s=0.375) brackets the entire entry-and-stop-loss episode in
just three points and cannot resolve a sub-threshold instant either way.

**The one source that could answer this — raw recorded tick data — is gone.** Both
`price_feed/raw/DOGE_poly_2026-07-12_09.parquet` and `DOGE_binance_2026-07-12_09.parquet` have
**zero rows** for the entire `09:00:00`–`09:50:11` window (first row in each: `09:50:12` /
`09:50:15.250`), exactly matching the destruction pattern
`price_feed/doc/incident_collector_data_loss_2026-07-12.md` documents for other assets/hours: the
collector ran uninterrupted from `09:15:07` accumulating ticks in its `.tmp` writer, then crashed
at `09:50:07` and its restart's `open_for_hour()` truncated the unsealed `.tmp` before any of that
hour's data could be carried forward — destroying the 09:15:07→09:50:07 stretch in full,
**including our exact trade window**. This is the same bug as §1's collector incident, just
manifesting here as a forensics gap instead of a live-trading gap.

### The Rust backtest cannot catch this trade either — confirmed empirically, not assumed

```
$ ./target/release/backtest --asset DOGE --date 2026-07-12 --prices-dir backtest_prices --config-dir config --format csv
slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts
doge-updown-5m-1783785600,reversal,DOWN,0.755000,0.905000,UNWIND,0.198700,1783785810.000
doge-updown-5m-1783821300,reversal,UP,0.830000,0.980000,UNWIND,0.180700,1783821568.500
```

Neither row is `doge-updown-5m-1783819800`. Directly confirmed why: `backtest_prices/DOGE_poly_
2026-07-12.parquet` (the aggregate the backtest replays against, built from the same destroyed
`raw/` files) has **zero rows** anywhere in `09:25`–`09:40`. The backtest isn't wrong about this
cycle — it has no data to be right or wrong *with*. This is a structural, not a logic, gap.

### Verdict

Not the clean "different bot, not our problem" story the first pass gave. Corrected: the signal
was probably real; Rust's engine is capable of the same detection Python's made; there's no
evidence Rust's feed was itself broken (no `RECONCILE-STALE` fired anywhere near this window,
meaning WS-cached and REST-polled prices agreed within tolerance throughout — see
`reconcile.rs`); but whether Rust's own `SawLowSignal` actually latched on its own tick stream in
the ~40s before entry is **unknown and, with current data, unknowable** — both the tick-level
ground truth and the backtest's ability to replay it were destroyed by the same `price_feed`
crash-loop bug fixed later today. This is a genuine observability gap, not a resolved question —
flagged in README's TODO below rather than claimed as either a miss or a correct decline.

**Observation, not investigated further (out of poly_rust's scope):** `btc_5mins`'s own row shows
`api_result=pending` and `exit_fill=dry_run` for this stop-loss — worth the user checking in that
project directly if they want to confirm the stop-loss was a real fill vs. a dry-run/simulated
exit.

## Conclusion

(1) BTC reversal's dry spell is a real, backtest-confirmed quiet period for a rare-firing
strategy variant — not a fault, no code change. (2) The DOGE trade is `btc_5mins`'s, the
underlying signal looks genuine, and Rust's reversal engine is architecturally capable of the
same catch — but whether Rust actually saw (and correctly declined) or missed a fireable pattern
cannot be determined, because the exact tick-level evidence needed to check was destroyed by the
`price_feed` collector-crash-loop bug before this audit could examine it. Added to README's TODO:
poly_rust's live heartbeat cadence (30s) is too coarse to ever forensically resolve a
sub-threshold `SawLowSignal` latch even on intact data — logging tick-level saw-low
latch/no-latch events (not just heartbeats) would close this gap for future incidents,
independent of the parquet-destruction bug already fixed.
