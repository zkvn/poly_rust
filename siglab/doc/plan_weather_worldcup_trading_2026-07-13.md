# Plan: simulate reversal trades for weather and World Cup markets

Status: **approved (simplified), implemented same day.** Originally written as a
pending-review plan; revised after explicit feedback to drop the parts that made the first
draft complicated, before any of that version was built. What's below is the design that
was actually implemented — see `siglab/src/bucket_reversal.rs`.

## What changed from the first draft, and why

The first draft proposed three new pieces: a synthetic self-momentum reference feed to
unblock `delta_pct` (since these markets have no Binance-equivalent), real Yes/No-aware
Gamma resolution polling, and one new additive method on `trader::machine::Machine` to
close a position against a real ground-truth outcome. All three are **gone**, per explicit
feedback:

- **No `delta_pct` / reference feed at all for these markets.** Not approximated, not
  synthesized — just not part of the entry condition. Crypto keeps using it unchanged
  (`trader::machine::Machine`/`ReversalStrategy` untouched, still delta-gated).
- **No Gamma resolution polling, no "real outcome" concept, no touching
  `trader::machine::Machine` at all.** Every position gets force-closed by **stop-loss,
  take-profit, or a fixed 30-second max hold** — resolved entirely from observed CLOB price
  action, never from knowing whether the market's real-world outcome was Yes or No. This
  fully sidesteps the earlier design's hardest problem (Yes/No-aware resolution, mutual
  exclusivity across a negRisk group's buckets) rather than solving it — with a 30-second
  hold cap, the position is always long gone before real resolution would even matter.
- **`high_prob` removed entirely, everywhere** (including the existing crypto
  `high_prob_btc`/`eth`/`doge` variants, deleted from `config/markets.toml`) — reversal-only
  going forward, on all market types.
- **Zero changes to `trader/` or `price_feed/`.** The earlier `resolve_with_outcome`
  proposal, even though additive and low-risk, is dropped along with the rest of the
  resolution machinery it existed to support. This module is a fresh, self-contained
  decision core in `siglab` — it reuses `trader`'s types nowhere except read-only constants
  it already had no dependency on changing.

## What crypto's reversal keeps doing (unchanged, verified)

Asked to double-check `delta_pct`/cycle-open tracking specifically for the 15m and
hourly-ET (60-minute) durations, since those are newer additions than the original 5m
config. Checked both the code and real trade data:

- **Code**: `market.rs`'s `run_market` is duration-agnostic by construction — each
  `(asset, duration)` task keeps its own local `last_binance`, continuously updated from the
  shared per-asset Binance broadcast regardless of `period_secs`, and captures a fresh
  `cycle_open_binance` at its own rotation boundary (`slot != current_slot_val`) using
  whatever `last_binance` currently holds. Nothing in this path treats 5m specially or
  assumes a particular period length.
- **Real data**: pulled actual 15m and hourly-ET trade records from the running container's
  log — token prices, exit prices, and PnL all internally consistent (e.g. a `DOWN` trade
  entering at 0.635 and stopping out at 0.495 showing pnl -0.2205, matching
  `shares * (exit - entry)` exactly).
- **Conclusion: no bug found.** `delta_pct` is tracked correctly for every duration, not
  just 5m. Left entirely as-is.

## Design: `bucket_reversal.rs` — a minimal, self-contained engine

One instance per `(bucket, variant)`, fed the bucket's own mid-price ticks (`up`, the same
feed `event_monitor.rs` already batches per event/city). No `CycleContext`, no rotation, no
concept of a market "closing" — it just watches continuously and can fire again immediately
after each position closes.

```rust
enum State {
    Watching { saw_low_up: bool, saw_low_dn: bool },
    Holding { side_up: bool, entry_price: f64, entry_ts: f64 },
}
```

**Entry** (`Watching`): latch `saw_low_up = true` the first time `up < low_threshold`
(independently, `saw_low_dn` the first time `dn < low_threshold`); once latched, it **never
un-latches on its own** — no time window, matches the crypto grid's already-established "any
time during monitoring" behavior. Fire long-Yes the moment `saw_low_up && up > high_threshold`
(symmetric for the No side). No delta/direction confirmation of any kind.

**Exit** (`Holding`), checked in this order, first match wins — same PnL formulas as
`trader::machine::Machine`'s (`shares = trade_size / entry_price`, `pnl = shares * exit -
trade_size`), just computed locally instead of calling into `trader`:

1. Stop-loss: current price ≤ `entry_price - 0.3` → outcome `STOPLOSS`.
2. Take-profit: current price ≥ `entry_price + 0.15` → outcome `UNWIND`.
3. Timeout: 30 seconds elapsed since entry → outcome `TIMEOUT` at current price.

Every exit returns to `Watching { saw_low_up: false, saw_low_dn: false }`, ready to fire
again. `sl_pnl`/`unwind_pnl`/max-hold values are fixed constants (0.3 / 0.15 / 30s), not
per-variant config — matching the crypto grid's now-uniform values.

**The 18-variant grid** (same `(low, high)` combinations as `config/markets.toml`'s crypto
reversal variants: low ∈ {0.2, 0.3, 0.4} × high ∈ {0.55, 0.6, 0.65, 0.7, 0.75, 0.8}) is
reused as-is, generated in code rather than duplicated into another TOML file, since the
values are identical and fixed.

## Where it plugs in

`event_monitor.rs`'s `run_event_feed` already demultiplexes each incoming tick by `asset_id`
to update one snapshot per bucket. It now also holds `HashMap<U256, Vec<BucketReversalEngine>>`
(18 engines per bucket, built alongside the existing `labels` map at discovery time) and
feeds each tick to every engine for that bucket, forwarding any resulting closed trade to the
same `trade_tx` channel crypto trades already flow through — so the existing hourly report
(`render_trade_summary`/`render_trade_table`, both already generic over
`SiglabTradeRecord`) picks these up with no further report changes needed. `market` is set
to the same `{snapshot_prefix}:{label}` key already used for snapshots/staleness (e.g.
`"weather:hong-kong: 33°C"`), so weather/World Cup trades are visibly distinguishable from
crypto ones in the report without needing a new `market_kind` variant *(one is added anyway,
`MarketKind::Weather`/`MarketKind::Worldcup`, since `SiglabTradeRecord::market_kind` already
existed as a field and leaving it always `Crypto` for non-crypto trades would be actively
misleading)*.

## What this does and doesn't claim

This produces real, mechanically well-defined PnL — "if this pattern had fired and the
position had been scalped for a max of 30 seconds using stop-loss/take-profit/timeout, here
is what would have happened" — using only observed price action, nothing fabricated. It does
**not** claim weather/World Cup markets have any real predictive edge for this pattern.
`studies/weather/weather_poly_2026-07-12.md`'s own research found the one documented real
edge in weather markets is forecast-latency arbitrage, not price reversals — this is
explicitly an experiment to see what a reversal-scalping heuristic's PnL looks like on these
markets' actual (thin, slow-moving) order books, starting from no expectation that it works.

## Scale note

Same conclusion as the first draft's §5, unchanged: ~51 weather cities × ~11 buckets ×
18 variants, plus ~62 World Cup events × (1-60 buckets) × 18 — tens of thousands of engine
instances. Each is a few floats and a branch per tick, no allocation on the hot path beyond
what already exists for snapshot updates, so this is expected to be cheap for the same
reason Machine-instance count was never the cost driver for crypto (see
`doc/incident_ws_2026-07-13.md`) — verified empirically after implementation, not assumed.
