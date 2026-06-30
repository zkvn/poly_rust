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

### Sync to local

A cron on the local Linux machine pulls all `raw*/` folders from the Oracle box daily at 18:00:

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
