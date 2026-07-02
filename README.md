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

The collector handles `SIGTERM` cleanly (seals + closes all parquet writers before exit):

```bash
# on Oracle
pkill -TERM -f 'price_feed collect'
sleep 2
cd /home/ubuntu/apps/poly_rust/price_feed
nohup ./target/release/price_feed collect >> collector.log 2>&1 &
```

### Feature-branch deploy workflow

Standard sequence for landing a price-recorder change (see the git branch convention above —
one feature per branch):

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
