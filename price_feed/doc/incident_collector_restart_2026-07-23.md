# Incident — `poly-collector` restarting mostly because of an untraded asset, 2026-07-23

**FIXED (code) same day, not yet deployed.** Root cause investigated, HYPE dropped from
`price_feed`'s collected asset set (`price_feed/src/collect.rs`), verified locally (`cargo build`/
`cargo fmt --all --check`/`cargo clippy --all-targets --all-features -- -D warnings`/`cargo test`
all clean, plus the `trader/scripts/trade_reconcile.py` test suite — 126 tests — still green after
removing its now-redundant HYPE entry). **Deploy to Oracle is a separate, pending step** — this
doc was written while checking in on the BTC/SOL `delta_pct_rev` relax 24h paper-trade window
(`trader/doc/plan_delta_relax_btc_sol_2026-07-22.md`), which surfaced `poly-collector.service` at
15 restarts (`NRestarts=15`) against `trader-live.service`/`poly-indicator.service` both at 0.

## Problem statement

Checking Oracle service health mid-window (`systemctl show -p ActiveState,SubState,NRestarts`):

```
trader-live.service:    NRestarts=0
poly-indicator.service: NRestarts=0
poly-collector.service: NRestarts=15
```

15 restarts is not itself alarming in isolation — `poly-collector`'s `RECONCILE-STALE` mechanism
(`price_feed/src/reconcile.rs`, see `price_feed/doc/incident_collector_data_loss_2026-07-12.md`
for its full history) is designed to self-restart on confirmed feed staleness, and every restart
since the 2026-07-12 fix is graceful (parquet footers written, no data loss). But every restart
briefly interrupts *every* asset's feed — including the 6 the trader actually trades (BTC, ETH,
SOL, BNB, XRP, DOGE) — via the shared NATS publish path `trader-live.service` subscribes to
instead of opening its own WebSockets. A restart rate worth checking is a restart rate worth
explaining.

## Investigation

Pulled every `RECONCILE-STALE`/restart line from `journalctl -u poly-collector.service` since the
BTC/SOL relax deploy (2026-07-22 13:46 HKT) through now (2026-07-23 12:37 HKT, ~23h):

```
15:49:48  SOL  rest_mid=0.0050 cached_mid=0.0550 diff=0.0500  restart counter -> 8
23:08:50  BTC  rest_mid=0.5250 cached_mid=0.4050 diff=0.1200  restart counter -> 9
00:03:01  HYPE rest_mid=0.0050 cached_mid=0.0500 diff=0.0450  restart counter -> 10
00:24:31  HYPE rest_mid=0.0050 cached_mid=0.0500 diff=0.0450  restart counter -> 11
01:04:47  HYPE rest_mid=0.5750 cached_mid=0.9250 diff=0.3500  restart counter -> 12
01:46:53  HYPE rest_mid=0.6550 cached_mid=0.7700 diff=0.1150  restart counter -> 13
03:13:45  HYPE rest_mid=0.0050 cached_mid=0.0700 diff=0.0650  restart counter -> 14
04:19:31  SOL  rest_mid=0.7150 cached_mid=0.6650 diff=0.0500  restart counter -> 15
```

**5 of 8 restarts in this window (62.5%) are HYPE.** Two, at 00:03 and 00:24, are only 21 minutes
apart. This lines up with something already documented in three separate places in this codebase
before today:

- `price_feed/src/collect.rs` (two comments, both now updated): HYPE has no Binance market at all
  (`HYPEUSDT` isn't a valid Binance symbol), so its `_binance_*.parquet` files are always empty by
  design — already required a `sample.price <= 0.0` skip and an ordering fix to avoid stalling its
  hourly seal forever.
- `price_feed/scripts/data_quality.py`'s `discover_recorded_asset_kinds` docstring: HYPE/binance
  is called out by name as the one pair that would otherwise "drown out real gaps."
- The 2026-07-12 incident doc's own root-cause section lists HYPE first among the assets showing
  the near-zero `rest_mid` mismatch pattern that mechanism was originally miscalibrated for.

HYPE was never one of the 6 assets `trader` actually trades (`hourly_et_coin_name` in
`trader/src/marketdata.rs` explicitly asserts it has no hourly-ET market either) — it's tracked by
`price_feed` only because `discover_assets()` pulls every active market under Polymarket's up/down
tag, with no filtering.

## Root cause

`spawn_reconcile_task` (`collect.rs`) compares two independently-sourced mid prices every 5s:
`cached_mid` (from the WS best-bid-ask subscription, `(best_bid + best_ask) / 2`) against
`rest_mid` (a fresh REST `/midpoint` poll). Three consecutive polls disagreeing by more than 0.04
triggers a graceful restart.

HYPE's Polymarket market is thin enough that its WS best-bid-ask feed goes quiet for extended
stretches — no new quotes on one or both sides — so `cached_mid` sticks at a stale value while the
book's true price (reflected by the REST poll, computed fresh each time) keeps moving. By the time
three consecutive 5s polls have elapsed (~15s), the gap is large enough to confirm as "staleness"
even though nothing is actually broken — it's a genuinely illiquid market being read by a
mechanism tuned around genuinely-traded ones. This is the same shape as the 2026-07-12 incident's
near-zero-mismatch pattern, just concentrated on the one asset structurally most prone to it
(no Binance cross-check, no real trading activity to keep its book fresh) rather than spread thin
enough across all assets to be hard to see.

This is a case where an over-broad discovery step (every Polymarket up/down market, no allowlist)
picked up an asset that the trading system has no use for, and that asset turned out to be
structurally the worst fit for a reconciliation mechanism tuned against liquid, actively-traded
books.

## Fix

Filter HYPE out of `price_feed::collect::run()`'s final asset list — applies whether the assets
come from `discover_assets()` (the production path; no `--assets` flag is passed by
`poly-collector.service`) or an explicit `--assets` override:

```rust
let assets: Vec<String> = assets.into_iter().filter(|a| a != "HYPE").collect();
```

Also updated, since they referenced HYPE as a still-live example:
- Two comments in `collect.rs` (Binance-staleness-observation section) — reworded to describe the
  "no Binance market" case generically, with a pointer to this doc for why HYPE specifically no
  longer applies.
- README's "Assets recorded" line — HYPE removed from the list, with a dated note plus a pointer
  to this doc. Historical `HYPE_*.parquet` files already on disk are unaffected and still readable
  — `data_quality.py`'s HYPE/binance skip-list entry stays, since it's about historical files, not
  future collection.
- `trader/scripts/trade_reconcile.py`'s `SLUG_ASSET_PREFIX` dict — dropped the explicit
  `"hype": "HYPE"` entry. Functionally a no-op: `asset_from_slug`'s fallback
  (`.get(prefix, prefix.upper())`) already produces the identical `"HYPE"` result for an
  unrecognized `"hype"` prefix, so this only removes now-misleading dead documentation, not
  behavior.

Deliberately **not** touched: `gamma_recorder/src/gamma.rs`'s HYPE-named test case (a different
subsystem — event-resolution detection, unrelated to price collection) and
`trader/src/marketdata.rs`'s `hourly_et_coin_name("HYPE")` assertion (tests generic
"asset outside the six-asset hourly-ET set" behavior, not price-feed collection — still accurate
and useful regardless of this change).

## Verification

- `cargo build`, `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D
  warnings` — all clean in `price_feed`.
- `cargo test` in `price_feed` — 44 passed, 0 failed (no test referenced HYPE directly; nothing to
  update).
- `/home/kev/apps/btc_5mins/venv/bin/python trader/scripts/test_trade_reconcile.py` (this
  project's actual test interpreter, matching the cron job) — 126 passed, 0 failed.
- Not yet run against live traffic — `poly-collector.service` on Oracle hasn't been redeployed
  with this change as of this doc.

## Residual / not fixed here

The other 3 restarts in the same window (2 SOL, 1 BTC) are on assets the trader actively trades
and are out of scope for this fix — they may be genuine feed hiccups worth their own look if the
rate stays elevated after HYPE stops contributing the majority share, but a 3-in-23h rate on
2 traded assets isn't itself alarming yet. Worth revisiting once there's a clean post-HYPE-removal
baseline to compare against.

## Next step

Deploy: `./scripts/deploy_oracle.py --price-feed-only` (or equivalent), then confirm via
`journalctl -u poly-collector.service` that startup logs "discovered N assets" without HYPE in the
list, and that `RECONCILE-STALE HYPE` lines stop appearing entirely. Held pending explicit
go-ahead since it restarts a live service other running processes (`trader-live`) depend on for
its NATS feed.
