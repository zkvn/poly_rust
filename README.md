# poly_rust

Rust price recorder for Polymarket CLOB markets. Streams live order-book and price data via the
Polymarket CLOB WebSocket API and writes daily Parquet files.

## Data Files

The recorder writes three file types per asset per day into `price_feed/raw/` (and aggregated
variants in `raw_15_mins/`, `raw_1hr/`, `raw_4hr/`):

| File pattern | Contents | Source |
|---|---|---|
| `{asset}_book_{date}.parquet` | Full order-book snapshots (bid/ask ladder, sizes) | `subscribe_orderbook` |
| `{asset}_poly_{date}.parquet` | CLOB price feed: best-bid/ask + last trade price | `subscribe_best_bid_ask` + `subscribe_prices` |
| `{asset}_hl_{date}.parquet`  | High/low aggregates for the interval | derived |

**Assets recorded:** BNB, BTC, DOGE, ETH, HYPE, SOL, XRP

### Parquet file integrity

The collector uses `ArrowWriter` from the Rust `parquet` crate. The parquet footer (`PAR1` magic +
file metadata) is only written when the writer is explicitly closed. Mid-write files captured by
rsync or a crash will be missing the footer and will be unreadable by standard readers.

**Hourly seal:** the collector closes and reopens each writer every hour, flushing a valid footer to
disk. This means files are always at most ~1 hour stale and readable at any sync time. On restart
the writer carries forward all rows from the last sealed file.

**Recovery:** `btc_5mins/bot/parquet_utils.py` contains `recover_poly_parquet()` and
`recover_book_parquet()` which recover footerless files by scanning raw page bytes. The Rust
collector writes a 6-column poly schema (`ts, up, dn, slug, server_ts, latency_ms`) and a
13-column book schema (the original 11 + `server_ts, latency_ms`). The Python recovery functions
use strides of 6 (poly) and 13 (book) dict pages per row group.

### Sync to local

A cron on the local Linux machine pulls all `raw*/` folders from the Oracle box daily at 18:00 HKT:

```
0 18 * * *  bash /home/kev/apps/poly_rust/price_feed/scripts/sync_oracle.sh >> .../sync_oracle.log 2>&1
```

Script: `price_feed/scripts/sync_oracle.sh` — uses `rsync` over SSH from `ubuntu@10.8.0.1`.

### Approximate data sizes (as of 2026-06-30)

| Location | `*_poly_*` (CLOB) | `*_book_*` |
|---|---|---|
| Oracle (`10.8.0.1`) | 1.7 GB | 7.7 GB |
| Local Linux | 1.7 GB | 7.6 GB |

---

## Build and deploy

### Oracle box is aarch64 — cross-compile locally

Oracle (`10.8.0.1`) is ARM64. The dev machine is x86-64. Use `cross` (Docker-based) to build:

```bash
# one-time setup
cargo install cross
# then for any binary
cross build --release --bin price_feed --target aarch64-unknown-linux-gnu
rsync -avz target/aarch64-unknown-linux-gnu/release/price_feed ubuntu@10.8.0.1:/home/ubuntu/apps/poly_rust/price_feed/target/release/
```

`cross` uses the `ghcr.io/cross-rs/aarch64-unknown-linux-gnu` Docker image — no system linker
install required. Build takes ~45 s when dependencies are cached (first run ~5 min).

**Do not build on Oracle with `cargo build`** — it saturates the box's CPU for several minutes and
blocks the live collector.

### Restart collector after deploy

The collector handles `SIGTERM` cleanly (flushes + closes all parquet writers before exit):

```bash
# on Oracle
pkill -TERM -f 'price_feed collect'
sleep 2
cd /home/ubuntu/apps/poly_rust/price_feed
nohup ./target/release/price_feed collect >> collector.log 2>&1 &
```

---

## Trading engine — known incidents

### Stop-loss close never filled (2026-07-02, fixed)

A live BNB test (`trader/src/bin/live.rs`, size $1, max-trades 1) bought 1.0752 shares of "Up"
for $0.9999, the stop-loss triggered, and **every single close retry failed** for the rest of the
run (hundreds of retries, `status=Failed sold=0.0000`). The position was never exited and rode to
market resolution; "Up" lost, so the position settled to $0. **Total loss: $0.9999** (confirmed via
Polymarket's public `data-api.polymarket.com/positions` endpoint — `currentValue: 0` on
`bnb-updown-5m-1782971400`).

**Root cause:** `execution.rs::close_position()` built the market SELL order as
`.amount(Amount::usdc(size_dec))`, where `size_dec` was the **held share count** (1.0753), not a
USDC amount. The SDK has two distinct constructors, `Amount::usdc()` and `Amount::shares()`.
Wrapping a share count in `Amount::usdc` tells the exchange "I want ~$1.0753 in proceeds", which at
a <$1 price requires selling *more* shares than are actually held — so the order could never
match. Every retry hit `"no orders found to match with FAK order"` / `"not enough balance"`, which
the retry loop treated as transient and retried forever instead of surfacing as a real error. The
retry logic explicitly listing `"not enough balance"` as retryable is a strong sign this exact
failure had been seen before and papered over with retries rather than fixed.

**Fix:** use `Amount::shares(size_dec)` instead, matching `place_limit_sell`'s existing correct
pattern (`round2(shares)` → 2-decimal `Decimal`, since `Amount::shares` enforces `LOT_SIZE_SCALE=2`
— unlike `Amount::usdc` which allows more decimal places, so the old 4-decimal formatting would
have failed validation immediately if this had been caught locally instead of live). Verified with
`cargo test --lib execution` (all 7 tests pass) after the change.

**Lesson:** any future live/shadow test should watch for repeated `[SL close] retry` log lines as a
red flag — that pattern means the close is structurally broken, not just hitting temporary
liquidity, and the position will ride uncontrolled to market resolution.
