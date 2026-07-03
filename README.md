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

**Recovery:** `price_feed/scripts/recover_live_tmp.py` recovers footerless/unsealed `*.tmp` files by
scanning raw parquet page bytes, and merges them with the day's other era files (old pre-seal daily
file + hourly-sealed files) into one up-to-date, deduped `{asset}_{type}_{date}.parquet` per asset:

```bash
# after a fresh sync_oracle.sh, so the .tmp isn't a stale/mid-flush snapshot
python3 price_feed/scripts/recover_live_tmp.py --type poly --date 2026-07-02
python3 price_feed/scripts/recover_live_tmp.py --type book --date 2026-07-02
```

It builds on the low-level page/thrift decoding primitives in the sibling `btc_5mins` project
(`btc_5mins/bot/parquet_utils.py`), but does **not** call that project's own
`recover_poly_parquet()` / `recover_book_parquet()` — those don't work on this repo's Rust-written
files:

- The Rust collector writes poly (`ts, up, dn, slug, server_ts, latency_ms`, 6 cols) and book
  (the original 11 + `server_ts, latency_ms`, 13 cols) columns as `NOT NULL` except `server_ts`/
  `latency_ms`. Required columns have no definition-levels section in their data pages, but
  `parquet_utils._data_indices()` always assumes a 4-byte definition-levels length prefix is
  present — for a required column that reads into real data, decodes a garbage bit-width, and
  raises (silently swallowed, so recovery returns 0 rows).
- The Rust arrow writer never emits definition level 3 for the book schema's `not null`
  `list<float32>` columns (only levels 0 = empty list, 2 = element present); the shared
  `recover_book_parquet()` hardcodes `max_def=3` as its "present" sentinel, so it always decodes
  every list as empty.
- The shared `recover_book_parquet()` also hardcodes an 11-column row-group stride; this repo's
  book schema has 13 columns, which silently misaligns every row group after the first.

`recover_live_tmp.py` re-implements the poly/book recovery with fixes for all three, documented in
its module docstring.

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

### Deploy to Oracle (one command)

```bash
# from repo root, using btc_5mins venv which has paramiko
source ../btc_5mins/venv/bin/activate
python scripts/deploy_oracle.py
```

Builds aarch64 binaries via `cross` (Docker-based), rsyncs them to Oracle, restarts
`poly-collector` (systemd), stops the old trader cleanly (SIGTERM → 10 s → SIGKILL),
starts the new trader in a tmux session named `trader`.

```bash
# useful flags
python scripts/deploy_oracle.py --dry-run          # preview, no changes
python scripts/deploy_oracle.py --skip-build       # rsync + restart only (binaries already built)
python scripts/deploy_oracle.py --price-feed-only  # skip trader
python scripts/deploy_oracle.py --trader-only      # skip price_feed
```

### Trader env file

The trader has its own env file at `/home/ubuntu/apps/poly_rust/trader/.env` — separate from
the Python bot's `/home/ubuntu/apps/btc_5mins/.env`. They share the same `TELEGRAM_CHAT_ID`
but use **different bot tokens**, so Telegram notifications stay in the same chat but come
from distinct bots without `getUpdates` conflicts.

`scripts/deploy_oracle.py` is configured to use the trader's own env file (`TRADER_ENV_FILE`
constant). Do not change it to point at `btc_5mins/.env` — that causes both bots to poll
the same token, producing 409 Conflict errors on `getUpdates` and cross-contaminated
startup notifications.

### Monitor after deploy

```bash
# price_feed (systemd)
ssh ubuntu@10.8.0.1 "journalctl -u poly-collector -f -o cat"

# live trader (tmux)
ssh ubuntu@10.8.0.1 "tmux attach -t trader"
# detach: Ctrl-B D

# one-shot status
ssh ubuntu@10.8.0.1 "
  systemctl is-active poly-collector
  pgrep -u ubuntu -a -f 'live '
  top -bn1 | grep -E 'price_f|live'
"
```

### Push local changes

```bash
cd /home/kev/apps/poly_rust
git add -p            # review hunks
git commit -m "..."
git push
```

### Cross-compilation details

Oracle is aarch64. `cross` (Docker-based) cross-compiles locally — never build on Oracle.

```bash
cargo install cross  # one-time
```

**OpenSSL gotcha:** `price_feed` uses `tokio-tungstenite` with `rustls-tls-webpki-roots`.
Rustls ≥0.22 requires an explicit crypto provider call at startup:

```rust
let _ = rustls::crypto::ring::default_provider().install_default();
```

This is already in `main()` for both `price_feed` and `trader`. Without it, `cross` builds
succeed but the process panics at runtime when the first TLS connection opens.

`price_feed/Cross.toml` configures the cross Docker image to pre-install `libssl-dev:arm64`
(needed only for any future native-tls dependency; currently unused but kept as a safeguard):

```toml
[target.aarch64-unknown-linux-gnu]
pre-build = ["dpkg --add-architecture arm64",
             "apt-get update && apt-get install -y --no-install-recommends libssl-dev:arm64 pkg-config"]
```

**Never `cargo build` on Oracle** — it saturates the box's CPU for several minutes and
blocks the live collector and trader.

### Local Docker test — full NATS pipeline

Runs price-feed + NATS + trader locally against live Polymarket/Binance APIs (x86_64 images):

```bash
docker compose up --build

# check NATS throughput
curl http://localhost:8222/varz | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['in_msgs'], 'published,', d['out_msgs'], 'delivered')"

# trader logs (look for "[NATS] first binance/poly tick")
docker compose logs -f trader
```

`price-feed` publishes to `price.binance.BTC` and `price.poly.BTC`; `trader` subscribes and
trades. Requires `/home/kev/apps/btc_5mins/.env` mounted read-only into the trader container.

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
