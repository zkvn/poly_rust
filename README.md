# poly_rust

Rust price recorder for Polymarket CLOB markets. Streams live order-book, price, and Binance
spot-price data and writes hourly Parquet files.

## Git branch convention

**Each feature gets its own branch. Do not mix unrelated features in one branch** (e.g. price
recorder work and trading-engine work must not land in the same branch) unless the user explicitly
confirms otherwise. Branch off `main`, not off another feature branch, unless that feature branch
has already been merged. Before deploying any binary built from a branch, confirm which branch is
actually checked out / running on the target machine — deploying a branch that's missing another
branch's already-shipped feature will silently regress production (this happened once: a
price-recorder fix branch that predated the Binance-recording feature was deployed over it on
Oracle, killing Binance recording for about an hour before being caught).

## Data Files

The recorder writes files per asset per hour into `price_feed/raw/` (and aggregated variants in
`raw_15_mins/`, `raw_4hr/`):

| File pattern | Contents | Source |
|---|---|---|
| `{asset}_book_{date}_{HH}.parquet` | Full order-book snapshots (bid/ask ladder, sizes) | `subscribe_orderbook` |
| `{asset}_poly_{date}_{HH}.parquet` | CLOB price feed: best-bid/ask + last trade price | `subscribe_best_bid_ask` + `subscribe_prices` |
| `{asset}_binance_{date}_{HH}.parquet` | Binance spot trade price + latency (`raw/` only, period-independent) | `wss://stream.binance.com:9443/ws/{symbol}@trade` |

`{HH}` is the 2-digit HKT hour (00–23). Every `poly`/`book` row also carries `server_ts` (source
exchange timestamp, ms) and `latency_ms` (local receive time − `server_ts`) for both the
Polymarket CLOB feed and the Binance feed — this is the latency figure to read for either source.

**Assets recorded:** BNB, BTC, DOGE, ETH, HYPE, SOL, XRP (HYPE has no Binance market — its
`_binance_` files are legitimately absent, not a bug).

### Parquet file integrity — hourly seal

The collector uses `ArrowWriter` from the Rust `parquet` crate. The parquet footer (`PAR1` magic +
file metadata) is only written when the writer is explicitly closed — a file copied mid-write (by
rsync, or left behind by a crash) will be missing the footer and unreadable by standard readers.

The collector writes to a `{asset}_{type}_{date}_{HH}.parquet.tmp` file for the current hour. When
the wall-clock hour advances, it closes that writer (footer written) and **atomically renames** it
to the final `{asset}_{type}_{date}_{HH}.parquet` name, then opens a fresh `.tmp` for the new hour.
This is O(1) — no re-read or re-encode of prior hours' data, unlike an earlier (buggy) design that
reopened and rewrote the entire day's file every hour, causing a multi-minute CPU spike on startup.
Graceful shutdown (`SIGTERM`) seals the in-progress hour the same way. On restart, a leftover
`.tmp` from a now-stale hour (i.e. the process crashed) is recovered at startup: its rows are read
and rewritten into a properly-closed file at the final name, bounded by at most one hour of data.

`rsync` should exclude `*.tmp` — sealed hourly files are always safe to sync; the active hour's
`.tmp` is not.

**Recovery of already-corrupted files:** `price_feed/scripts/recover_rust_parquet.py` recovers
footerless/truncated files by scanning raw page bytes directly (bypassing the need for a footer).
Usage: `python price_feed/scripts/recover_rust_parquet.py "raw_4hr/*.parquet"` (dry run, reports
row counts) or add `--write` to overwrite the source files with recovered data.

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

Sizes are roughly equal; a small delta in book is expected since today's file is still being written
on the remote before the nightly sync runs.

---

## TODO

- **Backfill hour-14 gap on Oracle (2026-07-02, price_feed) — still open.** While iterating the
  hourly-seal fix live, an intermediate (partially-fixed) binary was stopped mid-hour and
  overwrote the original `{asset}_{type}_2026-07-02_14.parquet` files, losing the 14:00–14:09 HKT
  window (~9 min, all assets, `raw/` + `raw_15_mins/` + `raw_4hr/`). The 14:00–14:09 rows were
  backed up to `/home/ubuntu/apps/poly_rust/price_feed/_14_backup/` on Oracle **before** the
  overwrite happened. The 15:00 HKT seal has since completed (confirmed — Oracle's `_14.parquet`
  is now a stable, no-writer-holding-it-open file covering 14:10–15:00), so the merge can be done
  any time: for every file in `_14_backup/<dir>/`, `pd.concat` it with the current
  `<dir>/<file>`, sort by `ts`, drop exact-duplicate rows, write back — then delete `_14_backup/`.
  Not urgent — low-stakes recorder data, not trading capital — but should be cleaned up so the
  historical record for that hour is complete. **Not yet done as of 2026-07-02 15:xx HKT** — the
  local dev-machine merge done the same day (combining old-daily + hourly + live `.tmp` into one
  file per asset/type for testing) pulled from Oracle's `_14.parquet` as-is and therefore does
  **not** include this backfilled window either; re-run the merge after backfilling on Oracle if
  the 14:00–14:09 window matters for whatever you're testing.

- **Binance data gap 2026-07-02 00:00–13:50 HKT — permanent, not fixable.** Binance recording was
  down for this window (see the git-branch-convention incident above: a branch predating the
  Binance feature was deployed over the box, and it took until ~13:50 to get Binance recording
  running again under the new hourly-seal code). The old daily-rotation `{asset}_binance_2026-07-
  02.parquet` files are 0 bytes for this reason — nothing to recover, no page bytes were ever
  written. Binance data for today starts cleanly at 13:50 HKT onward.

## Build and deploy

### Deploy to Oracle (one command)

Deploys both the price recorder and the live trader together — the recommended path for routine
deploys (use the feature-branch workflow below only when iterating on `price_feed` alone):

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

**Since this redeploys `price_feed` too, the git branch convention above applies here as well** —
confirm which branch is checked out locally before running it, or you'll silently ship whatever
that branch's `price_feed` looks like (this is exactly how the Binance-recording regression in the
TODO above happened).

### Trader env file

The trader has its own env file at `/home/ubuntu/apps/poly_rust/trader/.env` — separate from
the Python bot's `/home/ubuntu/apps/btc_5mins/.env`. They share the same `TELEGRAM_CHAT_ID`
but use **different bot tokens**, so Telegram notifications stay in the same chat but come
from distinct bots without `getUpdates` conflicts.

`scripts/deploy_oracle.py` is configured to use the trader's own env file (`TRADER_ENV_FILE`
constant). Do not change it to point at `btc_5mins/.env` — that causes both bots to poll
the same token, producing 409 Conflict errors on `getUpdates` and cross-contaminated
startup notifications.

### Oracle infra: NATS price bridge

Oracle runs a local `nats-server` (systemd unit `nats-server.service`, bound to
`127.0.0.1:4222` only — no external exposure needed). `poly-collector`'s `ExecStart`
publishes live Binance/Poly ticks there (`--nats-url nats://127.0.0.1:4222`), and the
trader subscribes instead of opening its own direct Binance/Poly WebSockets
(`deploy_oracle.py`'s `TRADER_NATS_URL`). This is required, not just an optimization: an
asset with more than one configured strategy (e.g. `ETH: [high_prob, reversal]`) spawns
multiple `AssetSlot`s in one trader process, and they all subscribe to the *same*
`price.binance.<ASSET>` / `price.poly.<ASSET>` subjects rather than each opening a
redundant connection.

`price_feed::collect::run()` treats a failed NATS connect as fatal — under
`Restart=always` that would crash-loop `poly-collector` (taking the whole
price-recording pipeline down with it) if NATS ever goes down. If you ever touch either
unit, bring `nats-server` up and confirm it's reachable (`ss -tln | grep 4222`) *before*
restarting `poly-collector`.

```bash
# NATS server status
ssh ubuntu@10.8.0.1 "systemctl is-active nats-server; ss -tln | grep 4222"
```

Assets and strategies are never hand-listed in the deploy script — `deploy_oracle.py`'s
`TRADER_ASSETS` reads `trade_assets` from the newest `btc_5mins/config/strategy_*.toml`
at deploy time (mirroring `bot/config.py`'s own glob+sort-latest rule), and the trader
binary resolves each asset's strategy list from `AssetParams.strategies` in that same
TOML (`trader/src/config.rs`) — so an asset like ETH with `[high_prob, reversal]` gets
two independent workers, and `/status` shows both.

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
blocks the live collector and trader.

`price_feed/Cross.toml` configures the cross Docker image to pre-install `libssl-dev:arm64`
(needed only for any future native-tls dependency; currently unused but kept as a safeguard):

```toml
[target.aarch64-unknown-linux-gnu]
pre-build = ["dpkg --add-architecture arm64",
             "apt-get update && apt-get install -y --no-install-recommends libssl-dev:arm64 pkg-config"]
```

**Rustls provider gotcha:** `price_feed` uses `tokio-tungstenite` with `rustls-tls-webpki-roots`,
and (since the NATS bridge) `async-nats` with its own `rustls` usage. Rustls ≥0.22 requires an
explicit crypto provider call at startup once multiple crates share rustls:

```rust
let _ = rustls::crypto::ring::default_provider().install_default();
```

This is already in `main()` for both `price_feed` and `trader`. Without it, `cross` builds
succeed but the process panics at runtime when the first TLS connection opens.

### Restart collector after deploy

The collector handles `SIGTERM` cleanly (seals + closes all parquet writers before exit):

```bash
# on Oracle
pkill -TERM -f 'price_feed collect'
sleep 2
cd /home/ubuntu/apps/poly_rust/price_feed
nohup ./target/release/price_feed collect >> collector.log 2>&1 &
```

### Monitor after deploy

```bash
# price_feed (systemd)
ssh ubuntu@10.8.0.1 "journalctl -u poly-collector -f -o cat"

# live trader (tmux)
ssh ubuntu@10.8.0.1 "tmux attach -t trader"
# detach: Ctrl-B D

# one-shot status
ssh ubuntu@10.8.0.1 "
  systemctl is-active poly-collector nats-server
  pgrep -u ubuntu -a -f 'live '
  top -bn1 | grep -E 'price_f|live'
"
```

### Feature-branch deploy workflow

Standard sequence for landing a price-recorder-only change (see the git branch convention above —
one feature per branch). For a combined price_feed+trader deploy, use `deploy_oracle.py` above
instead once both sides are ready.

1. Develop and test on the feature branch, based off `main`.
2. Commit, push the branch.
3. Build the release binary locally (native, same arch as dev machine) and run it against a
   scratch `--raw-dir` for a real multi-asset soak test — not just `cargo build`/`cargo check`.
4. If the local run is healthy, cross-compile for aarch64 (`cross build ... --target
   aarch64-unknown-linux-gnu`) and deploy to Oracle. **Before deploying, confirm which branch is
   actually checked out on Oracle** (`git status` there) so you don't silently drop a
   already-shipped feature from a different branch (see the incident noted above).
5. Watch the Oracle collector log and CPU/memory (`top`, `ps`) for a few minutes to confirm no
   regression (e.g. a startup CPU spike, a missing feed).
6. If healthy: merge the feature branch into `main`, push `main`. The README documentation for
   the feature is part of that merge — **README.md is maintained as the up-to-date doc on `main`**,
   not duplicated per-branch.
7. If unhealthy: return to the feature branch, fix, and repeat from step 3 (use `cross`'s Docker
   build locally to iterate without needing Oracle access) until the Oracle run is clean.

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
