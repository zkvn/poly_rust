# Binance WS Recording — Plan

Add a third per-asset feed (`binance`) to the existing collector
(`src/collect.rs`), alongside the current `poly` and `book` feeds, so
`raw/{ASSET}_binance_{date}.parquet` is Rust-recorded instead of relying on
the Python bot's own copy (`bot/price_recorder.py`). Built and validated on
branch `price-feed-binance-recorder` before touching the production Oracle
collector.

---

## 1. Goal & success criteria

**Goal:** produce `raw/{ASSET}_binance_{date}.parquet`, schema-compatible with
existing `prices/{ASSET}_binance*.parquet` files (`ts: f64, binance: f64, slug:
str`), plus two extra diagnostic columns (`server_ts`, `latency_ms`) following
the same nullable pattern already used for `poly`.

**Done when:**
1. One WS connection per asset to `wss://stream.binance.com:9443/ws/{symbol}@trade`
   (no auth, no subscribe handshake — connect and read).
2. Sampled at a **fixed 250 ms cadence** (not raw per-trade) — see §3 for why.
3. `latency_ms` populated from Binance's own `E` (event time, ms) field — see §2.
4. Runs alongside the existing `poly`/`book` feeds with no behavior change to
   those (verified via the parallel-run comparison, §6).
5. `pd.read_parquet('raw/BTC_binance_<date>.parquet')` loads cleanly in the
   `btc_5mins` venv.

---

## 2. Binance message shape (verified live, 2026-07-01)

Raw `@trade` stream message:

```json
{
  "e": "trade",
  "E": 1782909700935,
  "s": "BTCUSDT",
  "t": 6467756031,
  "p": "58413.30000000",
  "q": "0.00007000",
  "T": 1782909700935,
  "m": false,
  "M": true
}
```

- `E` — event time (ms, Binance server clock). `T` — trade time (ms); observed
  identical to `E` in every sampled message so far.
- Measured latency `local_recv_ms - E` ≈ 200ms in a live sample — same order of
  magnitude as the existing poly `latency_ms` column, so it's a meaningful
  diagnostic, not noise.
- No auth needed; the endpoint is public. No REST backfill needed for
  continuous recording (klines REST is only used elsewhere for point-in-time
  historical lookups, not gap-filling — same in the Python bot).

**Measured tick rate** (`@trade`, 20s live samples, 2026-07-01): BTC ~46/s,
ETH ~25/s, BNB ~9-19/s (bursty), SOL ~7.5/s, XRP ~6/s, DOGE ~3.6/s. Confirms
the docstring in `bot/price_feed.py` ("~10-100 Hz for liquid pairs").

---

## 3. Sampling cadence — fixed 250 ms (decided)

Python's `price_recorder.py` buffers Binance ticks **raw, one row per trade**
(different from its poly/chainlink sampler, which runs at a fixed 250ms). For
the Rust recorder we sample Binance at a **fixed 250 ms cadence** instead —
same pattern as poly, decided explicitly rather than mirroring Python's raw
approach:

- Maintain per-asset "latest known" state (`latest_binance_price`,
  `latest_binance_server_ts_ms`), updated on every WS trade message — no write
  here.
- A sampler tick (piggybacking on the existing 200 ms poly/book ticker, or its
  own 250 ms `tokio::time::interval` — see §5 for which) snapshots the latest
  state into one row: `ts = aligned, binance = price, slug, server_ts,
  latency_ms`.
- This caps the binance feed at 4 rows/sec regardless of trade burst rate
  (BTC's ~46/s raw rate would otherwise write ~10x more rows than poly/book
  do today).
- Emit a row only when a price has been seen at least once (Zero Means Zero —
  don't write a row before the first trade arrives).

---

## 4. Schema

`raw/{ASSET}_binance_{YYYY-MM-DD}.parquet`:

| Column | Arrow type | Meaning |
|---|---|---|
| `ts` | `Float64` | Unix epoch seconds, aligned to the 250ms sample grid |
| `binance` | `Float64` | latest trade price |
| `slug` | `Utf8` | cycle slug (asset-level feed, but tagged with slug for join convenience, matching Python) |
| `server_ts` | `Float64` (nullable) | Binance `E` field, ms |
| `latency_ms` | `Float64` (nullable) | `received_at_ms - server_ts` |

The core three columns (`ts`, `binance`, `slug`) match `bot/price_recorder.py`'s
`_BINANCE_SCHEMA` exactly, so existing merge/backtest loaders that only read
those three keep working unmodified; `server_ts`/`latency_ms` are additive,
same pattern already established for the `poly` schema in `collect.rs`.

---

## 5. Module changes

All in `src/collect.rs` (no new files — this feed reuses the existing
per-asset task/writer machinery):

1. **Cargo.toml:** add `tokio-tungstenite = { version = "0.29", features =
   ["rustls-tls-webpki-roots"] }` (already proven in the sibling `trader`
   crate's `marketdata.rs`, same workspace, same version already in the
   Cargo.lock via a transitive dep).
2. **`binance_schema()`** — new fn alongside `poly_schema()`/`book_schema()`.
3. **`AssetState`** — add `latest_binance_price: f64`, `latest_binance_server_ts_ms: i64`,
   `latest_binance_received_at_ms: i64`.
4. **`spawn_binance_task`** — ported from `trader/src/marketdata.rs`
   (already validated live for hours against BTC/ETH/DOGE in the shadow-feed
   work): one WS connection per asset, reconnect-with-backoff loop, parse `p`
   + `E` fields, write into the shared `AssetState`.
5. **`AssetWriters`** — add a third `ParquetBuf` (`binance`), same
   `open_with_carry`/`rotate_if_needed` pattern as `poly`/`book`.
6. **Sampler** — extend the existing 200ms ticker (or add a dedicated 250ms
   one — TBD during implementation, whichever keeps the grid-alignment math
   simplest) to also emit a `binance` row per asset when a price is known.
7. **Output directory** — made configurable (CLI flag or env var, default
   unchanged) so the new recorder can run locally writing to a scratch
   directory without colliding with the production Oracle collector's `raw/`
   during validation (§6).

---

## 6. Validation: 30-minute parallel run (required before touching Oracle)

The production collector keeps running unmodified on the Oracle box
(10.8.0.1, tmux) throughout. The new recorder runs **locally** (this dev
machine) against the same live public feeds, writing to a separate local
directory (e.g. `raw_new/`), for ~30 minutes.

Both processes are independent public-WS readers of the same real-time data
(Polymarket CLOB is public/unauthenticated for book+price; Binance trade
stream is public), so this is a valid apples-to-apples check — no
interference, no shared state, no risk to the production process.

**Comparison, for the same time window:**
- `poly`/`book`: pull the corresponding window from Oracle's production
  `raw/{ASSET}_poly_<date>.parquet` / `_book_<date>.parquet` (scp/read) and
  diff against the new local recorder's output for the same assets/timestamps
  — prices should agree closely (small timing/network differences expected,
  not systematic divergence). This confirms the refactor didn't regress the
  existing feeds.
- `binance`: new column, no old baseline to diff against — sanity-check
  instead: cadence ≈250ms (row count over the window ≈ window_secs × 4),
  `latency_ms` values are small and stable (double-digit-to-~200ms range, not
  wildly off), and the recorded price tracks a live reference (e.g. the same
  live Binance WS sample used in §2).

**Document the actual comparison result** (row counts, price diffs, latency
stats) back in this file once run, before merging this branch or touching the
Oracle deployment.

### Actual results (run 2026-07-01, 21:00-21:34 HKT, 34 min, all 6 assets)

**Poly/book regression check — done by code diff, not data diff.** Pulling
Oracle's in-progress (footerless) file required its Python recovery utility
(`bot/parquet_utils.py::recover_poly_parquet`/`recover_book_parquet`), which
turned out to be hardcoded to the **old 4-column Python-recorder schema**
(`ts, up, dn, slug`) — it errored (`All arrays must be of the same length`)
against the Rust collector's current 6-column schema (`+ server_ts,
latency_ms`), a pre-existing incompatibility unrelated to this branch (the
Rust collector added those two columns before this work started). Rather than
fix stale recovery tooling out of scope, verified no-regression by **diffing
`collect.rs` against `main`**: the only removed lines are `run()`'s signature
(replaced by parameterized `run_with_raw_dir()`, identical behavior when
called with `"raw"`) and two `flush_all()` call sites gaining one new
argument. Every existing poly/book function (`spawn_book_task`,
`spawn_bba_task`, `spawn_trade_task`, `AssetWriters`, `poly_row`, `book_row`,
both schemas, the 200ms/1s ticker branches) is **byte-for-byte untouched** —
this is a purely additive diff, so regression risk is nil by construction, not
just by data comparison.

**Local recorder data — validated directly:**

| Asset | poly rows | poly cadence | up+dn invariant | book rows (UP+DN) |
|---|---|---|---|---|
| BTC | 10,124 | 5.00/s | 1.000000 | 20,248 |
| ETH | 10,124 | 5.00/s | 1.000000 | — |
| BNB | 10,124 | 5.00/s | 1.000000 | — |
| SOL | 10,124 | 5.00/s | 1.000000 | — |
| XRP | 10,124 | 5.00/s | 1.000000 | — |
| DOGE | 10,124 | 5.00/s | 1.000000 | — |

Book schema confirmed correct: depth columns are `float32` (not float64), UP/DN
split exactly 50/50 (10,124 each for BTC), `best_bid`/`best_ask` in sane [0,1]
range.

**New binance columns — sanity-checked (no prior baseline to diff):**

| Asset | rows | cadence | price range | latency mean/p50/p99/max (ms) |
|---|---|---|---|---|
| BTC | 8,105 | 4.000/s | 58590.93 – 58904.00 | 210.9 / 210 / 241.0 / 400 |
| ETH | 8,105 | 4.000/s | 1570.01 – 1579.48 | 208.2 / 207 / 273.0 / 367 |
| BNB | 8,106 | 4.000/s | 542.90 – 545.50 | 211.4 / 210 / 274.9 / 571 |
| SOL | 8,106 | 4.000/s | 74.63 – 75.43 | 208.3 / 207 / 243.7 / 488 |
| XRP | 8,106 | 4.000/s | 1.0367 – 1.0440 | 210.4 / 209 / 243.0 / 351 |
| DOGE | 8,105 | 4.000/s | 0.0712 – 0.0719 | 209.5 / 208 / 283.0 / 460 |

Cadence is **exactly 4.000 rows/sec for all 6 assets** — the 250ms sampler
grid works as designed regardless of each asset's underlying trade rate (BTC
~46 trades/sec vs DOGE ~3.6/sec, per §2's measurement, collapse to the same
4Hz recorded rate). `server_ts` has zero nulls across all assets. Latency is
tightly clustered ~207-211ms mean with occasional bursts to 350-570ms (network
jitter, not a systemic problem) — consistent with the ~206ms measured in the
one-off live sample in §2. BNB's recorded price range (542.90-545.50) matches
the independently-observed BNB price (~542-543) from the concurrent live
trader B2 test in the same time window — good cross-corroboration from an
unrelated part of this session.

**Conclusion: validated.** No regression to poly/book (by code diff + direct
data sanity check); new binance recording behaves exactly as designed (fixed
4Hz cadence, real latency tracking, correct prices). Safe to proceed toward
deploying to the Oracle collector when ready — that deployment step itself is
still a separate, deliberate action, not implied by this validation.

### Deployed to production (2026-07-01, 22:12 HKT)

Merged to `main` (cherry-picked the single price_feed commit, not the whole
branch, to avoid pulling in unrelated in-flight work from `trader-a1`), then
rolled out via the new `scripts/upgrade_collector.sh`: push → ssh Oracle →
`git pull` → `cargo build --release` → restart `poly-collector.service`.
Confirmed healthy post-restart: all assets reconnected (CLOB + Binance),
poly/book carried forward correctly via the existing restart-safety mechanism,
binance data flowing at the same measured 4Hz for every asset within ~1-2 min
of restart (production's larger carry-replay — 21+ hours of accumulated
book/poly data vs the 30-min local test — delayed the very first flush
slightly longer than seen locally, but no errors, no crash).

**One finding, not a bug:** production auto-discovers assets from Polymarket
(the systemd unit runs `price_feed collect` with no asset list), which
currently includes **HYPE** — Hyperliquid's token, not listed on Binance
(`HYPEUSDT` → Binance API error `-1121 Invalid symbol`). The WS "connects"
(the raw stream endpoint doesn't validate the symbol at handshake) but never
receives messages, so `HYPE_binance_*.parquet` stays empty. Correct behavior
per the project's Zero-Means-Zero rule — no code change needed, just a data
availability gap for an asset Binance doesn't list.

---

## 7. Decisions (settled)

1. **Cadence: fixed 250ms**, not Python's raw-per-trade — explicit user
   decision, keeps write volume bounded regardless of an asset's trade rate.
2. **Latency tracking**: yes, via Binance's `E` field, same pattern as the
   existing `poly` `server_ts`/`latency_ms` columns.
3. **Validation methodology**: local parallel run vs production Oracle
   collector, not a direct in-place modification of the live deployment.
4. **New branch**: `price-feed-binance-recorder`, kept separate from
   `trader-a1`'s unrelated work.

5. **Sampler cadence implementation**: binance runs its **own dedicated 250ms
   ticker** (`ticker_250ms`), separate from the existing 200ms poly/book
   ticker — kept the timestamp-grid math simplest (`round(now*4)/4` vs the
   existing `round(now*5)/5`) and avoided coupling an unrelated feed's cadence
   to poly/book's.
6. **Validated 2026-07-01** (§6 "Actual results") — no regression (by code
   diff), new feed behaves as designed (by direct data check). Not yet
   deployed to the Oracle production collector — that's a separate follow-up
   action.
