# Incident: `reversal` and `v_shape` logging identical entry/exit timestamps on BNB-5m (2026-07-16)

## Symptom

Reported from `doc/report/2026-07-16/trades_2026-07-16_02.md` (BNB-5m table, lines 486-503):
12 `reversal_{low}_{high}` variants **and** 8 `v_shape` variants (the `(0.7,0.3,0.7)` triple) all
logged `entry (HKT) = 2026-07-16 02:09:40.022`. Six of the `reversal` variants and six of the
`v_shape` variants further share the exact same `exit (HKT) = 2026-07-16 02:09:50.172` and the
exact same `pnl = 0.0778`. `reversal` and `v_shape` are separate engines — `reversal` runs
through `trader::machine::Machine` (Binance-gated momentum), `v_shape` is `siglab`'s own
self-contained `VShapeEngine` (pure CLOB price action, no Binance) — so two different strategy
families agreeing to the millisecond on both entry and exit looked, on its face, "impossible."

Asked directly: is this the same bug fixed yesterday
(`doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`)?

**No — different mechanism, and that fix is still intact.** The 2026-07-14 bug was `entry_ts`
getting stamped from a shared *Binance* tick while a market's own poly price was stale, causing
**different market durations of the same asset** to falsely share `entry_ts`. Today's cluster is
a **single market** (`bnb-updown-5m-1784138700`), and the raw JSONL (see below) shows
`entry_price_ts == entry_ts` for every trade in the cluster — i.e. every one of these entries
genuinely fired off the market's own live poly tick, not a stale cached one. The fix from
yesterday is doing exactly what it's supposed to; it just isn't the mechanism at play here.

## Investigation

Pulled `siglab_trades.jsonl` straight from the running container (`docker cp
siglab-siglab-1:/app/logs/siglab_trades.jsonl`), the same discipline the 07-14 investigation
used, rather than trusting the rendered Markdown (which can obscure float-level detail). The
raw records for this cluster (excerpted, `reversal_0.3_0.55` and `v_0.7_0.3_0.7_0.3_0.1` shown):

```json
{"strategy":"reversal","variant_id":"reversal_0.3_0.55","entry_ts":1784138980.0224602,
 "entry_price_ts":1784138980.0224602,"token_price":0.8999999999999999,
 "exit_price":0.97,"outcome":"UNWIND","pnl":0.0778}
{"strategy":"v_shape","variant_id":"v_0.7_0.3_0.7_0.3_0.1","entry_ts":1784138980.0224602,
 "entry_price_ts":1784138980.0224602,"token_price":0.8999999999999999,
 "exit_price":0.97,"outcome":"UNWIND","pnl":0.0778}
```

`entry_ts`, `entry_price_ts`, and `token_price` are bit-for-bit identical (not just
same-second-rounded) between the two strategies. That level of exactness has one explanation:
both records came from processing the literal same `PolyTick` object.

**Entry correlation — `siglab/src/market.rs`'s tick loop.** The `Some(tick) = poly_rx.recv()`
branch (lines 170-200) runs every incoming poly tick through *every* `Machine` (`m.on_poly(tick)`,
line 182) and then every `VShapeEngine` (`e.on_tick(tick.up, tick.ts)`, line 190) — same tick
object, same loop iteration. `trader::machine::Machine::try_enter` stamps
`entry_ts: now` where `now` is that tick's own `ts` (`machine.rs:337,386`); `VShapeEngine::on_tick`
stamps `entry_ts: ts` from the same argument (`v_shape.rs:202,208`). So identical `entry_ts`
across strategies isn't a code defect — it's guaranteed *whenever both conditions happen to be
satisfied by the same tick*.

Whether that's plausible turns out to hinge on the two conditions being philosophically the same
shape. `reversal`'s entry gate (`trader/src/strategies.rs:66`):

```rust
if saw_low_up.saw_low() && up > self.reversal && dp > 0.0 { ... }
```

— a prior dip latch (`SawLowSignal`, latches once price ever drops below
`reversal_low_threshold` — 0.2/0.3/0.4 across these variants, `siglab/config/markets.toml:213`)
plus a later recovery above a threshold (0.55-0.8) plus positive momentum. `v_shape`'s entry
(`siglab/src/v_shape.rs:103-114`) is a `high1(0.7) → low(0.3) → high2(0.7)` latch chain — same
"dipped, then sharply recovered" shape, just implemented independently with its own thresholds.
Since 2026-07-13's `reversal_start_time = 999999` widening (`markets.toml:170-173`), `saw_low`'s
window spans the *entire* cycle, same as `v_shape`'s latches. So a single real sharp recovery
tick anywhere in the cycle can legitimately complete both. This is the same *class* of "expected,
same-market clustering" the 07-14 writeup already documented for reversal-vs-reversal — it's now
visible across a second strategy family because `v_shape.rs` (added 2026-07-15, the day *after*
that investigation) was never checked against it.

**Exit correlation — a second, independent shared mechanism.** `trader::machine::Machine` and
`siglab`'s `VShapeEngine` each force-close any still-open position within
`FORCE_UNWIND_BEFORE_CYCLE_END_SECS = 10.0` seconds of cycle end, at whatever price the market
carries on the tick that crosses that boundary (`trader/src/machine.rs:90,302-315`;
`siglab/src/v_shape.rs:39,228-231` — the latter's own doc comment: "same value and rationale as
`trader/src/machine.rs`'s constant of the same name"). This cycle ends at
`1784138700 + 300 = 1784139000`, so the boundary is `1784138990.0`. Every trade in this cluster
was still holding at that instant, so all of them — 12 `reversal` variants and 6 of the 8
`v_shape` variants (the two with `unwind_pnl=0.05` had already hit their own tighter take-profit
target earlier, at `1784138984.66`, exiting independently — expected per-variant divergence) —
exit on the very next real tick (`1784138990.1728`) at the identical price, hence identical pnl.
Two unrelated engines, one shared constant, one shared market tick stream: guaranteed
correlation, not a bug.

## Was the underlying BNB move itself real, or a data artifact?

Cross-checked against `price_feed`'s **independently recorded** BNB-5m parquet
(`price_feed/raw/BNB_poly_2026-07-16_02.parquet`, `BNB_book_2026-07-16_02.parquet`) — a separate
recorder, separate WS connection, from the same real Polymarket CLOB feed. It shows a genuine,
dramatic late-cycle move in this window: `up` chopping between 0.27 and 0.82 through 02:09:34,
jumping to 0.945 by 02:09:39.4, and grinding up to 0.97-0.995 by 02:09:52 — with the order book
extremely thin moments earlier (`best_bid`/`best_ask` as wide as 0.10/0.90 around 02:09:32,
essentially no resting liquidity on one side). This is a classic late-cycle, thin-liquidity
convergence as a 5-minute market approaches resolution, corroborated independently — not a
`siglab`-only artifact.

**One loose end, not fully resolved:** the archived `price_feed` recorder samples at a fixed
~200ms cadence (confirmed: 3,583 of 3,587 seconds in the file's minute have exactly 5 rows —
i.e. it's a poller, not a raw per-message tick log), and its snapshots bracketing the entry
instant (`02:09:40.000` and `.200`) both read `up = 0.945`, bid/ask `0.94/0.95` — a 1-cent
spread. The live-logged entry price was `0.8999999999999999` (~0.90), about 4.5¢ off that
bracket. By contrast, the force-unwind *exit* price (`0.97`) matches the archived data closely
(`up = 0.970` from `02:09:44.6` on). So the anomaly is specifically at the entry tick, not a
systematic offset. Two explanations, not distinguishable from 200ms-sampled archive data alone:
a genuine sub-200ms real dip the poller simply missed between samples, or `trader::marketdata::
spawn_poly_task` (`marketdata.rs:157-207`) — which merges two independently-arriving WS streams,
`best_bid_ask` and `price_changes`, into one `(bid, ask)` pair with no proven atomicity guarantee
— pairing a fresh update from one stream with a momentarily stale value from the other. Either
way, it's a single real merged tick both engines legitimately (and independently) reacted to —
not a fabricated or duplicated record, so it doesn't change the "not a regression" conclusion,
but it's worth closing out (see Follow-ups).

## Root cause

Not a bug in the sense the 2026-07-14 issue was (a spurious `entry_ts` from a stale/wrong feed).
Every field logged here reflects a tick the engine actually observed. Two ordinary, independent
mechanisms compound to produce the correlation:

1. `market.rs` feeds the identical poly tick to every `reversal`/`high_prob` `Machine` **and**
   every `v_shape` `VShapeEngine` in the same loop iteration, and both strategy families
   implement structurally similar "dipped, then recovered" entry conditions — so one real sharp
   move can complete both at once.
2. `trader::machine` and `v_shape.rs` each independently force-close any open position within
   10s of cycle end at the market's current price — so positions from either strategy still open
   at that boundary exit together, at an identical price.

## Why yesterday's fix didn't cover this

It wasn't supposed to — `v_shape.rs` didn't exist yet on 2026-07-14. The fix added
`entry_price_ts` specifically to catch entries firing off a *stale* cached poly price via a
shared Binance tick; that regression test
(`machine::tests::entry_price_ts_reflects_stale_cached_poly_tick_not_triggering_binance_tick`)
targets `Machine::on_binance`, a code path `VShapeEngine` doesn't have (no Binance feed at all).
This isn't a skipped test case from that work — it's a new correlation surface that opened up the
next day when `v_shape` was added and started sharing `market.rs`'s tick loop and the
`FORCE_UNWIND_BEFORE_CYCLE_END_SECS` constant with `reversal`.

## Verification

- Cross-checked against `price_feed`'s independently-recorded BNB-5m parquet for
  2026-07-16 02:09:00–02:10:05 HKT (`price_feed/raw/BNB_{poly,book}_2026-07-16_02.parquet`).
- Pulled and inspected the raw `siglab_trades.jsonl` directly from the running container to
  confirm float-level equality of `entry_ts`/`entry_price_ts`/`token_price` across strategies,
  ruling out a rendering artifact.
- Reconciled the pnl arithmetic by hand: `shares = 1.0 / 0.9 = 1.1111`,
  `pnl = 1.1111 * 0.97 - 1.0 = 0.0778` — matches every affected row exactly.
- No code change made — this is a "confirmed, not a bug" writeup, same disposition as the
  2026-07-14 `incident_same_entry_ts_2026-07-14.md` finding.

## Follow-ups / TODO

- **(data quality, open)** `trader::marketdata::spawn_poly_task` merges two independently-arriving
  WS streams (`best_bid_ask` + `price_changes`) into one `(bid, ask)` pair with no verified
  atomicity guarantee. The one ~4.5¢ unexplained entry-price gap found here couldn't be
  conclusively attributed given the archived recorder's 200ms sampling. Worth either auditing
  `polymarket_client_sdk_v2`'s `PriceChangeBatchEntry` guarantees directly, or adding raw
  per-message (not resampled) logging next time this question comes up.
- **(simulation fidelity, noticed but out of scope)** force-unwind-near-cycle-end fills at the
  raw mid-price with no spread/liquidity check; this same cycle had spreads as wide as 0.80
  minutes earlier, so paper PnL near cycle-end in thin books may be more optimistic than a real
  fill could achieve. Not investigated further here.
- **(process)** `v_shape.rs` landed 2026-07-15, one day after the correlated-timestamp
  investigation, without a note connecting it to that investigation's findings. This writeup
  closes that gap after the fact. No new regression test is being added for the clustering
  itself, since it isn't a bug to guard against — matching the precedent set by 07-14's
  reversal-internal clustering, which also got no dedicated test for the same reason.
