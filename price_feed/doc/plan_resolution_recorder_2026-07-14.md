# Plan — official market-resolution recorder (Gamma outcomes)

## Bottom line up front

Add a **new `price_feed` subcommand** (not a new crate): `price_feed resolve`, living in a new
`price_feed/src/resolutions.rs` module. It has two modes sharing one Gamma-fetch/upsert code path:

1. `--backfill --from <date> --to <date>` — one-shot: bulk-paginate Gamma's `/events` list
   endpoint (closed=true, crypto tag, date range) rather than one HTTP call per market, and
   upsert every `{asset}-updown-{5m,15m,4h}-{slot}` event found into a local store.
2. No flags (long-running daemon) — as each tracked (asset, duration) slot closes, wait 5 min,
   poll Gamma for that one slug, retry on a backoff until resolved or a deadline, upsert the
   result. A periodic housekeeping sweep retries any row still `UNRESOLVED` after its deadline.

**Storage: SQLite** (`price_feed/resolutions.db`), not Parquet/CSV/JSON/TOML — see §3 for why.
One table, primary key `(asset, duration, slot)`, upserted in place.

This is a plan for review — nothing implemented yet.

---

## 1. Motivation

Two consumers want real Polymarket resolution outcomes, not a proxy:

- **Continuous backtest validation.** `trader/src/backtest.rs` currently derives a cycle's
  outcome purely from recorded price data — `Machine::cycle_close()` compares `last_binance`
  against `cycle_open_binance`. That's a reasonable proxy but it is **not** what Polymarket
  actually settles against (Chainlink BTC/USD stream via UMA, per the resolution text on the
  market itself — confirmed live, see §2). Any divergence (feed gaps, an oracle edge case, a
  tie exactly at the boundary) currently has no way to surface — the backtest just silently
  trusts its own proxy. A local table of ground-truth outcomes lets the backtest (or a
  dedicated recon check) continuously diff its price-based outcome against the real one across
  the whole history, and flag drift automatically as new data lands, rather than a one-off audit.
- **Daily recon** (`trader/scripts/trade_reconcile.py`'s Gamma Cross-Check section) currently
  calls `fetch_gamma_outcome(slug)` live, once per trade, against `gamma-api.polymarket.com`
  during every recon run — network-dependent, slow at scale, and re-fetches the same slug if
  recon is re-run. Once this table exists, recon can look outcomes up locally first and only
  fall back to a live Gamma call for slugs too recent to be in the table yet (see §7).

## 2. Confirmed Gamma response shape (live-checked 2026-07-14)

`GET https://gamma-api.polymarket.com/events?slug=btc-updown-5m-<slot>` →
`events[0].markets[0]` has (fields actually seen, not assumed):

```json
{
  "conditionId": "0x4d12d4b1...",
  "slug": "btc-updown-5m-1784042400",
  "outcomes": "[\"Up\", \"Down\"]",
  "outcomePrices": "[\"0\", \"1\"]",
  "umaResolutionStatus": "resolved",
  "closedTime": "2026-07-14 15:25:20+00",
  "clobTokenIds": "[\"905220...\", \"676718...\"]"
}
```

- `umaResolutionStatus == "resolved"` is a clean, explicit finality signal — better than
  `trade_reconcile.py::fetch_gamma_outcome`'s current heuristic (an `outcomePrices >= 0.99`
  threshold), which we'll keep as a cross-check but not the primary gate.
- `closedTime` is the real resolution timestamp — use it for `resolved_at_ts` instead of "when we
  happened to poll it."
- The bulk list form (`?tag_id=102127&closed=true&limit=100&offset=N`) returns paginated events
  across all crypto assets/durations in one call — confirmed working live. This is what backfill
  should use instead of one request per historical slot (see §5 for why that matters).

## 3. Storage format: SQLite, not Parquet/CSV/JSON/TOML

What this table actually needs to do, sized realistically: ~7 assets × (288 five-min + 96
fifteen-min + 6 four-hour) ≈ **2,730 new rows/day**, all history since price_feed started
recording (~2026-06-12) is on the order of 100–150k rows total. Small, forever. The access
pattern is point lookups/upserts by `(asset, duration, slot)` from **two different languages**
(Rust backtest binary, Python recon script), with a row's life cycle
`UNRESOLVED → resolved` (i.e. **update-in-place**, not pure append).

| Format | Fit | Why / why not |
|---|---|---|
| **Parquet** | Poor | Immutable columnar — no row-level upsert. Marking a pending row resolved would mean the same tmp-file/hourly-reseal dance `collect.rs` already does for high-volume tick data, except here it's keyed point-updates on a tiny table, not a natural time-ordered append stream. Real complexity for zero benefit at this row count. |
| **CSV** | Poor | Append is fine; upsert isn't — flipping one row from pending to resolved means a full-file rewrite (or duplicate rows + "last one wins" logic downstream), and a reader mid-read during that rewrite can see a truncated file. No index for point lookups. |
| **JSON** | Poor | Same append/upsert/rewrite-race problem as CSV, plus no index — every lookup means loading and scanning the whole file. |
| **TOML** | Wrong shape | Not designed for many repeated tabular records at all. |
| **SQLite** | **Recommended** | Purpose-built for exactly this: indexed point lookups, real `UPSERT`, and — with WAL mode — safe concurrent reads from other processes while one writer appends/updates. Both languages read it natively: Python's `sqlite3` is stdlib (zero new dependency, lighter than the `pandas`/`pyarrow` recon already carries for parquet); Rust needs `rusqlite` (new dependency — **flagging**: no crate in this repo currently uses SQLite anywhere, this would be the first). |

**Consumption model matches the existing `raw/*.parquet` convention**, not a new one: `trader`
already reads `price_feed/raw/*.parquet` directly with its own parquet loader — it does **not**
depend on the `price_feed` crate to do so (confirmed: no root-level Cargo workspace; `siglab`
path-depends on `trader`, but `trader` and `price_feed` are fully independent crates). Treat
`resolutions.db` the same way: a plain data artifact on disk, opened independently by whichever
consumer needs it (`trader`'s backtest binary via `rusqlite`, `trade_reconcile.py` via stdlib
`sqlite3`) — no new crate-to-crate dependency introduced.

## 4. Schema

```sql
CREATE TABLE market_resolutions (
    asset            TEXT    NOT NULL,   -- 'BTC','ETH','SOL','DOGE','XRP','BNB','HYPE'
    duration         TEXT    NOT NULL,   -- '5m' | '15m' | '4h' (matches make_slug's suffix)
    slot             INTEGER NOT NULL,   -- unix seconds, == current_slot_for(interval)
    slug             TEXT    NOT NULL,
    condition_id     TEXT,
    open_ts          INTEGER NOT NULL,   -- == slot
    close_ts         INTEGER NOT NULL,   -- == slot + interval
    outcome          TEXT    NOT NULL,   -- 'UP' | 'DOWN' | 'UNRESOLVED'
    up_token_id      TEXT,
    down_token_id    TEXT,
    resolved_at_ts   INTEGER,            -- from Gamma's closedTime, NULL until resolved
    check_attempts   INTEGER NOT NULL DEFAULT 0,
    last_checked_ts  INTEGER,
    PRIMARY KEY (asset, duration, slot)
) WITHOUT ROWID;

CREATE INDEX idx_market_resolutions_unresolved ON market_resolutions (outcome)
    WHERE outcome = 'UNRESOLVED';
```

`WITHOUT ROWID` since the natural key is already unique and compact — avoids a redundant
autoincrement rowid for a table that's purely keyed lookups. The partial index keeps the
housekeeping sweep's "find rows still pending" query cheap even once the table has 100k+ rows.

## 5. New module vs. new crate

**Recommend: new module inside the existing `price_feed` crate**, new `Cmd::Resolve` subcommand
alongside `Markets`/`Collect` in `main.rs`, not a standalone crate/binary.

Why:
- `make_slug`/`current_slot_for` (`collect.rs:55,95`) **must** stay byte-identical to whatever
  the resolution recorder uses to compute a slot's slug — they're the same naming scheme by
  construction. Reusing the functions directly (make them `pub(crate)`) guarantees that; a
  separate crate would have to duplicate them and could drift.
- Already has every dependency needed: `reqwest`, `serde_json`, `chrono`, `tokio`. A new crate
  would re-declare all four with independent version pins — a maintenance/drift risk for no
  reason (`Cargo.toml` already shown to have exactly these).
- Same deploy story: cross-compiled aarch64 via `cross`, rsynced to Oracle, run as another
  `Restart=always` systemd unit — identical to how `collect` is already deployed (see root
  `README.md` → "Build and deploy"). Adding a subcommand is zero new deploy-pipeline surface;
  a new crate would be a second binary artifact to build/ship/monitor.
- Downside, for completeness: couples the resolution recorder's release cadence to
  `price_feed`'s build. Acceptable — `Markets` and `Collect` already share this without issue.

## 6. Backfill mode

**Don't** do one Gamma call per historical `(asset, duration, slot)` — at 7 assets × ~390
slots/day since 2026-06-12 (~33 days as of today), that's on the order of **90k+ individual
HTTP calls**, most of which would be pointless (e.g. an asset not yet tracked on a given day, or
a slot that was skipped). Instead:

- Paginate `GET /events?tag_id=102127&closed=true&start_date_min=<from>&start_date_max=<to>&limit=100&offset=N`
  (confirmed working live, §2) — walk pages until an empty page, parsing every event's `slug`
  against `^(asset)-updown-(5m|15m|4h)-(\d+)$` for the 7 known assets, upserting matches.
- Pin down each per-asset earliest sealed date by scanning `raw*/` filenames rather than
  hardcoding one repo-wide start date — `HYPE_hl_2026-06-13.parquet` etc. show all 7 assets
  already present from day one in this repo's data (~2026-06-12/13), but that's a fact to
  re-derive at implementation time, not bake in as a constant.
- `--from`/`--to` flags let this be re-run for a narrower window (e.g. re-backfill one bad day)
  without re-walking the whole history.
- Idempotent by construction — `INSERT ... ON CONFLICT (asset, duration, slot) DO UPDATE`, so
  re-running backfill over an already-populated range is safe and just confirms/refreshes rows.

## 7. Continuous-update mode

For each tracked `(asset, duration)`:
- Compute slot boundaries the same way `collect.rs` already does (`current_slot_for(interval)`),
  and when a slot rolls over, schedule a resolution check at `close_ts + 5 min`.
- Poll Gamma for that one slug (single-market `fetch_meta`-style call — cheap here since it's
  exactly one call per just-closed market, not backfill-scale), retry every ~30s if
  `umaResolutionStatus != "resolved"`, up to a deadline (proposed: 15 min past close — generous
  vs. the live trader's own Gamma-confirmation deadline in
  `trader/doc/plan_gammapi_2026-07-11.md`, since nothing here is blocking a trading decision).
- On timeout, upsert `outcome='UNRESOLVED'`, bump `check_attempts`, record `last_checked_ts`. A
  periodic housekeeping pass (e.g. every 30 min) re-queries the partial index (§4) for any
  `UNRESOLVED` row and retries it — covers the rare case Gamma is down or slow past the
  per-market deadline, without a live in-memory timer surviving a process restart.
- A crash/restart loses only in-memory pending-check timers, never data — on restart, anything
  that should already be resolved but isn't in the DB yet gets picked up by the same
  housekeeping sweep (treat "closed_ts + 5min already passed but no row exists" the same as
  `UNRESOLVED`).

## 8. Where it runs / deploy

Gamma reads are plain HTTPS GETs — confirmed **not** geoblocked (the geoblock only affects CLOB
*order-placement* POSTs; GETs for balance/market-data are unaffected everywhere, see
`[[infra_network]]`). Recommend running it on **Oracle**, alongside `collect`, as its own
systemd unit (`price_feed resolve`, no flags) — keeps one writer for `resolutions.db`, consistent
with `collect` already owning `raw/`. `sync_oracle.sh` gets one addition: rsync
`resolutions.db*` (and `resolutions.db-wal`/`-shm` if present under WAL mode, or just take a
consistent snapshot via `sqlite3 ... ".backup"` before syncing) down to local alongside the
existing `raw*/` folders, same cron.

The one-shot `--backfill` can be run manually from either box (or the dev machine) since it's
read-mostly-GETs too — run it once against Oracle's `resolutions.db` after deploy, before
switching on the continuous mode.

## 9. Consumers

- **`trader/src/backtest.rs`**: add a small `rusqlite`-based reader (new dependency in
  `trader/Cargo.toml`) that looks up `(asset, duration, slot)` and compares against the existing
  price-based `Machine::cycle_close()` outcome — surface a mismatch count/list rather than
  changing what the backtest itself simulates against (the price-based proxy is still what a
  live trader actually saw in real time; the resolutions table is the audit, not a replacement).
- **`trade_reconcile.py`**'s Gamma Cross-Check section: try `resolutions.db` first
  (`sqlite3` stdlib, no new dependency), fall back to the existing live
  `fetch_gamma_outcome(slug)` only for slugs not yet in the table (too recent — still within the
  5-minute-plus-retry window) or rows still `UNRESOLVED`. Net effect: recon gets faster and
  Gamma-outage-resilient for the common case (older, already-resolved trades), while keeping
  the live-fetch fallback for genuinely fresh ones.

## 10. Open questions for user

1. **Retention** — keep `UNRESOLVED` rows forever (rare, small volume) or expire/drop after some
   period if Gamma truly never resolves (e.g. a market that got archived/voided)? Proposed:
   keep forever, they're cheap and useful as a "known gap" audit trail.
2. **Backfill start date** — confirm ~2026-06-12 (earliest sealed `raw/` data) is the right
   floor, or if there's value going back further (Gamma likely has resolution history predating
   this repo's own price recording, but there'd be no price data to reconcile against, so no
   backtest use for it — only relevant if recon ever wants pre-price_feed history, which seems
   unlikely).
3. **`trader/Cargo.toml` picking up `rusqlite`** — fine as a new dependency, or prefer trader
   shell out to a tiny Python/CLI helper instead to avoid adding a new Rust dependency there?
   Leaning toward `rusqlite` directly (keeps the mismatch-check in the same binary as the
   backtest it's auditing), but flagging since it's a new pattern for this codebase.

---

# DeepSeek Comments

*(pending — sending this plan to DeepSeek for review next)*

# Claude Thoughts on DeepSeek Review

*(pending)*
