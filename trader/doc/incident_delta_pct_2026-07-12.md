# Incident — Entry Δ%/Cycle Δ% used the wrong price entirely, 2026-07-12

## Background

2026-07-12 added four columns to `trade_reconcile.py`'s "Live vs BT"/"BT vs Live" tables: Entry
Time (T-Ns), Entry/Exit CLOB price, and two delta-percent columns. A same-day follow-up fixed a
"+15000%" outlier in one of them (`load_cycle_open_prices` picking a stale one-tick echo of the
previous cycle's price as "cycle open" — see the git history for that fix). The user then flagged
that the *formula* itself was still wrong, independent of the stale-echo bug: **delta should use
the underlying asset price, not the CLOB price.**

## Root cause

Both delta columns were computed entirely from Polymarket CLOB (order-book) prices — i.e. an
**implied probability in `[0, 1]`**, not a price in the normal sense:

```python
# before (wrong)
"cycle_delta_pct": _pct_change(entry_price, exit_price),   # entry_price/exit_price = CLOB token_price
"entry_delta_pct": _pct_change(open_price, entry_price),   # open_price = CLOB price at cycle open
```

A CLOB probability swinging from 0.44 to 0.95 is a real, large *probability* move, but it says
nothing about how far the actual asset (ETH/BTC/DOGE/...) moved — and it mechanically trends
toward the `[0, 1]` boundary near cycle close **regardless of the underlying's magnitude**, because
that's what a probability does as an outcome becomes near-certain. That's exactly what produced
the absurd magnitudes seen in the reports:

| Row | CLOB-based delta (before) | What actually happened |
|---|---|---|
| ETH high_prob UP, 2026-07-10 21:45:00 | Entry Δ% = **+113.5%** | ETH moved **+0.08%** in 5 min (1799.95 → 1801.44) |
| DOGE reversal DOWN, 2026-07-12 00:00:00 (after the stale-echo fix) | Entry Δ% = **+86.4%** | DOGE moved **−0.09%** in that same span (0.07535 → 0.07528) |

No 5-minute crypto move is +113%. The CLOB-based formula was measuring the token's own payout
curve, not the market. It also happens to be near-circular with the outcome column right next to
it: a WIN's CLOB exit price is ~1.0000 almost by definition, so "Cycle Δ%" on CLOB price mostly
just re-derived "was this a WIN" in percent form, rather than telling you anything new.

The underlying (Binance) price is also **literally what the Polymarket "updown" market resolves
against** — price at cycle close vs. price at cycle open — so it's the correct "delta" to report
even by the market's own resolution rule, not just a better proxy.

## Fix

Replaced the CLOB-based lookup (`load_cycle_open_prices`, reading `{asset}_poly_{date}.parquet`)
with `load_underlying_price_series`, reading the same local `{asset}_binance_{date}.parquet` files
`run_backtest_reconciliation` already builds/syncs for the BT replay (no extra network calls):

```python
# after
"cycle_delta_pct": _pct_change(open_p, close_p),   # open_p/close_p = Binance price, cycle open -> close
"entry_delta_pct": _pct_change(open_p, entry_p),   # entry_p = Binance price nearest entry_ts
```

- **Cycle Δ%** = underlying move over the *whole cycle*, open→close (`(close − open) / open`).
  Deliberately not "entry→exit" — there's no `exit_ts` in `TradeRecord` (only `entry_ts`), and
  open→close is both computable without one and matches the market's own resolution rule.
- **Entry Δ%** = how far the underlying had already moved from cycle open to the moment of entry
  (`(entry − cycle_open) / cycle_open`) — how much of that move had already happened before the
  trade was placed. `entry_ts` is `worker.rs`'s `last_binance_ts()` (`src/worker.rs:1038,1434`),
  i.e. it's literally the timestamp of a recorded Binance tick, so the lookup (nearest tick by
  `|Δt|`) should land on an exact or near-exact match, not an interpolation.
- **Entry Px**/**Exit Px** columns are unchanged — those still show the actual CLOB price traded,
  which is legitimate, useful information in its own right (execution price). Only the *delta*
  columns moved to the underlying price. The table's explanatory note in the rendered report was
  updated to say so explicitly, to avoid the reader assuming all four price columns share a source.

Verified against real data (2026-07-10/07-11 reports, regenerated): DOGE's outlier is now **+86.4%
→ correctly recomputed to a value in line with the table below** once the underlying-price formula
replaced the CLOB one; typical deltas across both regenerated reports now land in the
**±0.0%–0.2%** range, consistent with real 5-minute crypto price action.

## A second, unrelated finding surfaced while verifying this fix

Regenerating the reports after the fix, most rows for the 2026-07-11→07-12 window show `—` for
both delta columns — not a regression, but exposure of a **separate, currently-active** data-gap
in `price_feed`: local (and Oracle-side, checksum-confirmed identical — this is not a sync issue)
Binance/poly tick coverage collapsed from ~93% of minutes covered on 2026-07-10 to **~14-15%** on
2026-07-11 and 2026-07-12, for every asset checked (ETH/DOGE/BTC), starting sharply at the
2026-07-11 00:00 boundary and **still ongoing as of this write-up** (2026-07-12 ~12:00 HKT — the
most recent hourly raw file, `ETH_binance_2026-07-12_10.parquet`, has only 96 rows covering
10:59:36–10:59:59, i.e. the collector is only capturing the last ~10-30s of most hours right now).
This is a live `price_feed` collector reliability issue, separate from `trader`/`trade_reconcile.py`
— not investigated further here (would need Oracle-side collector logs); flagged in README's TODO
for follow-up. The `—` fallback for missing underlying-price data is `load_underlying_price_series`/
`_underlying_price_at`/`_cycle_open_close` behaving exactly as designed (never guess, never crash),
so no report-side bug here — just a data-availability ceiling on how many rows can show a delta
until the collector issue is fixed.

## Tests

`test_trade_reconcile.py`: `LoadUnderlyingPriceSeriesTests`, `SafeLoadUnderlyingPriceSeriesTests`,
`UnderlyingPriceAtTests`, `CycleOpenCloseTests`, plus updated `BuildLiveVsBtTests`/
`BuildBtVsLiveTests` delta-column tests asserting against a synthetic Binance tick series rather
than CLOB prices. Full suite green (46 tests) before this doc was written; see the commit this
ships with for the final count including the two reports regenerated with the fix.
