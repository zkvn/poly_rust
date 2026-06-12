# Rust Coin Data Collector — Plan

A new, clean, **headless** data collector for Polymarket 5-min Up/Down markets,
built on [`rs-clob-client-v2`](https://github.com/Polymarket/rs-clob-client-v2)
(`polymarket_client_sdk_v2`). Its only job: stream CLOB price + full order-book
data for a set of coins and persist it to `raw/` in **byte-for-byte the same
parquet schema** as the Python `btc_5mins` project, so the files drop straight
into that project's existing merge/backtest pipeline.

This is deliberately separate from the existing `markets.rs` TUI. No ratatui, no
derived signals (delta, p_up, sigma), no Binance/Chainlink. Just clean CLOB +
book capture.

---

## 1. Goal & success criteria

**Goal:** produce `raw/{ASSET}_poly_{date}.parquet` and
`raw/{ASSET}_book_{date}.parquet` files that are schema-identical to those the
Python bot writes, capped at **≤5 ticks/sec per (asset, feed)**.

**Done when:**
1. For each requested asset, the collector discovers the live 5-min slug + UP/DN
   token IDs from Gamma, rotates them at each slot boundary.
2. A `poly` parquet is written with schema `{ts: f64, up: f64, dn: f64, slug: str}`
   identical to `bot/price_recorder.py::_POLY_SCHEMA`.
3. A `book` parquet is written with schema identical to
   `bot/book_recorder.py::_SCHEMA` (11 columns incl. four `list<float32>`
   depth columns), one row per side per snapshot.
4. `python -c "import pandas as pd; pd.read_parquet('raw/BTC_poly_<date>.parquet')"`
   loads cleanly in the `btc_5mins` venv, and
   `scripts/data_cleanup.py::merge_raw_to_prices` picks the files up (requires the
   **3-part daily filename** — see §5).
5. Write rate, observed over 60 s, is ≤5 rows/sec per asset per feed.
6. Ctrl-C / SIGTERM flushes buffers and closes writers (valid PAR1 footer).

---

## 2. Non-goals (keep it simple)

- No TUI. Log progress to stderr only.
- No Binance / Chainlink feeds, no `delta`/`delta_pct`/`p_up`/`bn_sigma`.
- No trading, no auth (book + price subscriptions are public/unauthenticated).
- No REST `/midprice` or `/book` polling — the Rust SDK gives both over WS
  (`subscribe_prices`, `subscribe_orderbook`), which is lower-latency and the
  whole point of moving to Rust. (REST is only the Python implementation detail
  we are replacing, not part of the target format.)

---

## 3. Exact output schemas (the crux)

These MUST match the Python writers field-for-field, including types, so the
existing `btc_5mins` loaders and backtests read them with zero changes.

### 3a. `poly` feed — `raw/{ASSET}_poly_{YYYY-MM-DD}.parquet`

Source: `bot/price_recorder.py::_POLY_SCHEMA`

| Column | Arrow type | Meaning |
|---|---|---|
| `ts`   | `Float64` | **Unix epoch seconds** (not ms), aligned to the sample grid |
| `up`   | `Float64` | UP-token mid price (0..1) |
| `dn`   | `Float64` | DOWN-token mid price = `1 - up` |
| `slug` | `Utf8`    | e.g. `btc-updown-5m-1778737500` |

Note: Python stores `ts` as **float64 seconds** (`round(now*4)/4`), NOT the
`Int64 timestamp_ms` that the existing `markets.rs` uses. Use `f64` seconds.

### 3b. `book` feed — `raw/{ASSET}_book_{YYYY-MM-DD}.parquet`

Source: `bot/book_recorder.py::_SCHEMA`. **Two rows per snapshot** (UP and DN).

| Column | Arrow type | Meaning |
|---|---|---|
| `ts`         | `Float64`          | Unix epoch seconds of the snapshot |
| `asset`      | `Utf8`             | `"BTC"`, `"ETH"`, … |
| `slug`       | `Utf8`             | cycle slug |
| `side`       | `Utf8`             | `"UP"` or `"DN"` |
| `best_bid`   | `Float64`          | best bid price (see ordering note §6) |
| `best_ask`   | `Float64`          | best ask price |
| `last_trade` | `Float64`          | last trade price, `0.0` if unknown |
| `bid_prices` | `List<Float32>`    | full bid ladder |
| `bid_sizes`  | `List<Float32>`    | full bid sizes |
| `ask_prices` | `List<Float32>`    | full ask ladder |
| `ask_sizes`  | `List<Float32>`    | full ask sizes |

Critical: the four depth columns are `list<float32>` (NOT float64). Python's
`merge_raw_to_prices` / recovery normalizes to float32 and breaks on float64
drift. Build these with Arrow `ListBuilder<Float32Builder>`.

---

## 4. SDK surface we use (verified in 0.6.0-canary.1)

From `polymarket_client_sdk_v2::clob::ws::Client` (feature `ws`):

| Method | Returns | Use |
|---|---|---|
| `subscribe_orderbook(Vec<U256>)` | `Stream<BookUpdate>` | full depth → book feed + mid |
| `subscribe_last_trade_price(Vec<U256>)` | `Stream<LastTradePrice>` | `last_trade` column |

`BookUpdate { asset_id: U256, market: B256, timestamp: i64, bids: Vec<OrderBookLevel>, asks: Vec<OrderBookLevel>, hash }`
where `OrderBookLevel { price: Decimal, size: Decimal }`. **`bids.first()` / `asks.first()` is the best level** (confirmed by the SDK's own `subscribe_midpoints`, which computes `(bids.first + asks.first)/2`).

We derive the `poly` mid from the same `BookUpdate` (`(best_bid+best_ask)/2`)
rather than subscribing to `subscribe_prices` separately — one subscription
feeds both feeds, fewer moving parts. (`subscribe_midpoints` exists but throws
away the depth we need for the book feed, so we use `subscribe_orderbook`
directly.)

Gamma slug/token discovery: reuse the REST pattern already in
`markets.rs::fetch_meta` (`reqwest` → `gamma-api.polymarket.com/events?slug=…`),
but capture **both** UP and DN token IDs (the existing code grabs UP only; the
book feed needs both sides).

---

## 5. File naming, rotation, restart safety

**Filename:** `raw/{ASSET}_{feed}_{YYYY-MM-DD}.parquet` — exactly **3
underscore-separated stem parts**, HKT calendar date. This is mandatory:
`scripts/data_cleanup.py::merge_raw_to_prices` skips any file whose stem is not
exactly 3 parts (per-cycle/legacy names are ignored). So we cannot use the
`…_HHMMSS.parquet` unique-per-run trick that `chainlink.rs` uses — one daily
file per asset+feed.

**Rotation:** at HKT midnight, `finish()` the current writer and open a new one
for the new date (same pattern as `markets.rs::ParquetBuffer::flush`).

**Restart safety (carry):** one daily file means an intra-day restart would
overwrite the morning's data. Match Python's carry mechanism
(`price_recorder.py` / `book_recorder.py`):
- On startup, for today's HKT file: if it exists **and has a valid footer**,
  read it back into an Arrow `RecordBatch` and write it as the first batch of
  the freshly opened writer, then continue appending.
- If the file has no footer (previous run was SIGKILLed mid-write), it is
  unreadable → discard, start fresh (same lossy behavior Python documents).
- Reading back: `parquet::arrow::ParquetRecordBatchReaderBuilder` over the
  existing file, collect batches, re-emit into the new writer. Cast/validate the
  schema matches before replay; on mismatch, discard.

**Carry is required (decided).** The carry read-back adds ~40 lines, but
without it any intra-day restart overwrites the morning's data. Implement it
from the start; it mirrors the Python invariant the format already assumes.

**Flush cadence:** flush every `N` rows or every ~10 s, whichever first (as
`markets.rs` does). `writer.finish()` only on rotation + shutdown (the PAR1
footer). Buffer in memory between flushes via `ArrowWriter::write(batch)`.

**Shutdown:** `tokio::signal::ctrl_c()` (and ideally SIGTERM via
`tokio::signal::unix`) → final flush → `writer.finish()` for every open writer.

---

## 6. The 5 Hz throttle & timestamp grid

The CLOB WS can emit book updates far faster than 5/sec. Requirement: **≤5
rows/sec per (asset, feed)**. Use a **sampler**, mirroring Python's design
(Python samples poly at 250 ms = 4 Hz; we go 200 ms = 5 Hz):

- Maintain per-asset "latest known" state: `latest_book: Option<BookUpdate>`
  (and last-trade price), updated on every WS message — no write here.
- A `tokio::time::interval(200ms)` sampler tick, per asset (or one global tick
  iterating all assets), snapshots the latest state into:
  - one `poly` row: `ts = aligned, up = mid, dn = 1-mid, slug`
  - two `book` rows (UP, DN) with full depth + best_bid/ask/last_trade
- **Timestamp grid:** align `ts` to the 200 ms grid: `ts = round(now_secs*5)/5`
  (Python uses `round(now*4)/4` for 250 ms). This makes timestamps land on clean
  boundaries and dedupes naturally.
- Only emit a row when `latest_book.is_some()` and the cycle is active (slug
  known), so we never write `0.0`-as-"no data" — honor the project's
  **Zero Means Zero** rule: skip rows with no book rather than writing zeros.

This caps each feed at exactly 5 writes/sec regardless of WS burst rate.

**Book level ordering (decided): reverse to match Python.** Python's REST
recorder stored bids **ascending** (`best_bid = bids[-1]`). The Rust WS gives
**best-first** (`bids.first()` is best). **Reverse the WS ladders** so
`bid_prices`/`ask_prices` are stored worst→best (best last), matching
`book_recorder.py`, keeping the list columns byte-compatible with existing
Python book data. The scalar `best_bid`/`best_ask` columns are unambiguous
either way.

---

## 7. Module / architecture

New subcommand `collect`, new file `src/collect.rs`. Add to `main.rs`:

```rust
enum Cmd {
    Markets { assets: Vec<String> },       // existing TUI
    Collect { assets: Vec<String> },       // new headless collector
}
```

Data flow (per asset, all on the tokio runtime):

```
 meta task (Gamma, 10s)  ──► watch<SlotTokens{ up: U256, dn: U256, slug }>
                                   │
            ┌──────────────────────┼───────────────────────┐
   orderbook WS task         last-trade WS task        sampler (200ms)
 (subscribe_orderbook)    (subscribe_last_trade_price)       │
            │                      │                         │
       latest_book[idx]      latest_trade[idx] ◄─────────────┘ reads latest_*
            └──────────────► shared per-asset state ─────────► writes poly+book rows
                                                                     │
                                                            ParquetWriters (per asset, per feed)
```

Reuse from `markets.rs` (copy, don't share — studies/CLAUDE.md favors
self-contained): `hkt()`, `current_slot()`, `make_slug()`, `fetch_meta` (extended
to return DN token), the `ParquetBuffer` rotation/flush skeleton (but with the
new schemas + carry).

Shared state: simplest is a `tokio::sync::watch` or `Arc<Mutex<Vec<AssetState>>>`
holding `latest_book` / `latest_trade`; the sampler reads, the WS tasks write.
Given low contention, `Arc<Mutex<…>>` is fine and simplest.

**Token rotation:** at each 5-min boundary the meta task pushes new UP/DN IDs;
the orderbook + last-trade WS tasks `unsubscribe_orderbook(old)` /
resubscribe with the new IDs — same coordination `markets.rs::ws_manager` already
does via `watch<Vec<Option<U256>>>`. Extend the watch payload to carry both
tokens + slug per asset.

---

## 8. Dependencies

Already in `Cargo.toml` — no new crates needed:
`polymarket_client_sdk_v2` (`ws`, `clob`, `rtds` — we only need `ws`+`clob`),
`tokio`, `futures`, `anyhow`, `chrono`, `arrow`, `parquet`, `reqwest`,
`serde_json`. `rust_decimal` is re-exported by the SDK for `Decimal`→`f64`
(`.try_into()` / `.to_f64()`).

`Decimal → f64`: use `decimal.to_string().parse::<f64>()` (as `markets.rs` does)
or `rust_decimal::prelude::ToPrimitive::to_f64`. For the `list<float32>` depth,
cast to `f32`.

---

## 9. Milestones (each independently verifiable)

1. **Scaffold** — add `Collect` subcommand; `collect::run(assets)` prints
   discovered slug + UP/DN tokens per asset, rotating each slot.
   *Verify:* run 6 min, see one slot rotation logged for each asset.

2. **Book WS → memory** — subscribe_orderbook for all tokens; log best_bid/ask
   per asset on each update; handle token rotation (unsub/resub).
   *Verify:* live best_bid/ask printed, survives a slot boundary.

3. **poly writer** — sampler at 200 ms writes `{ASSET}_poly_{date}.parquet`.
   *Verify:* `pd.read_parquet(...)` in the btc_5mins venv; schema == `_POLY_SCHEMA`;
   `df.groupby(df.ts.astype(int)).size().max() <= 5`.

4. **book writer** — same sampler emits 2 rows/snapshot with full depth as
   `list<float32>`.
   *Verify:* schema == `book_recorder._SCHEMA` (check `df.dtypes`, list element
   type is float32); UP/DN rows present; ≤5 snapshots/sec.

5. **Rotation + carry + shutdown** — HKT-midnight rotation; read-back carry on
   restart; Ctrl-C/SIGTERM flush+finish.
   *Verify:* start, kill, restart within the same day → reload merged file, row
   count == before + after (no morning data lost); file has valid footer after
   clean kill.

6. **End-to-end merge** — drop a day of `raw/*.parquet` into the btc_5mins
   `raw/` and run `scripts/data_cleanup.py::merge_raw_to_prices`.
   *Verify:* merges into `prices/{ASSET}_{poly,book}.parquet` with no errors.

---

## 10. Build & run

```bash
cd "/Users/kz/My Drive/apps/polymarket/poly_rust/price_feed"
cargo run --release -- collect btc eth sol bnb xrp doge
# headless / long-running:
nohup cargo run --release -- collect btc eth sol bnb xrp doge > log/collect.log 2>&1 &
```

Output lands in `raw/` (created on startup). Sync/merge with the Python project
exactly as its existing `raw/ → prices/` pipeline expects.

---

## 11. Decisions (settled)

1. **`raw/` location** — write to **this project's own**
   `poly_rust/price_feed/raw/`. Merging into the `btc_5mins` tree is a separate
   copy step, not the collector's job.
2. **Carry on restart** — **implement now** (§5).
3. **Book ladder ordering** — **reverse to match Python** (best-last) (§6).

### Still to confirm

- **Asset set** — default to the six BTC/ETH/SOL/BNB/XRP/DOGE? (SDK book/price
  subs are slug-driven, so any 5-min asset works; only Gamma slug naming
  matters.)
