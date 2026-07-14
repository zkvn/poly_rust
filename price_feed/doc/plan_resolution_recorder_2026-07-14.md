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

- **Resolution check uses either signal, not `umaResolutionStatus` alone** (revised after live
  spot-checks — see "Claude Thoughts on DeepSeek Review" below): `umaResolutionStatus ==
  "resolved"` **or** `outcomePrices` containing a value ≥0.99. Live-checked the most-recently-closed
  5m slot across all 7 assets ~4 minutes after close: BTC/ETH/SOL/DOGE/XRP/BNB already show both
  signals resolved, but **HYPE's `umaResolutionStatus` stayed `None` even though `outcomePrices`
  was already decisive** (`["0.0005","0.9995"]`) — gating on `umaResolutionStatus` alone would
  never resolve HYPE rows. `trade_reconcile.py::fetch_gamma_outcome` already uses the
  `outcomePrices` threshold today (not `umaResolutionStatus`); keeping both as an OR preserves
  that proven path and adds the stronger signal for assets that populate it.
- `closedTime`, when present, is used for `resolved_at_ts` — for these `automaticallyResolved: true`
  Chainlink-settled markets it's consistently close+20s (verified live, not "whenever we happened
  to poll"). Falls back to "timestamp of the first poll that saw a decisive/resolved signal" for
  rows where `closedTime` is absent (the HYPE case above).
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
    resolved_at_ts   INTEGER,            -- Gamma's closedTime, else first-observed-resolved poll time; NULL until resolved
    check_attempts   INTEGER NOT NULL DEFAULT 0,
    last_checked_ts  INTEGER,
    PRIMARY KEY (asset, duration, slot)
) WITHOUT ROWID;

CREATE INDEX idx_market_resolutions_unresolved ON market_resolutions (outcome)
    WHERE outcome = 'UNRESOLVED';

CREATE INDEX idx_market_resolutions_history ON market_resolutions (asset, duration, close_ts);
```

`WITHOUT ROWID` since the natural key is already unique and compact — avoids a redundant
autoincrement rowid for a table that's purely keyed lookups. The partial index keeps the
housekeeping sweep's "find rows still pending" query cheap even once the table has 100k+ rows.
The second index supports the range queries backtest/recon actually want ("all BTC-5m
resolutions between date X and Y"), which the primary key alone (point lookups by exact slot)
doesn't serve well.

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
- **New dependency: `rusqlite`**, in `price_feed/Cargo.toml` (the writer) — not just `trader`'s
  (the reader), which the first draft of this plan missed. Use the `bundled` feature so the
  aarch64 cross-compile (`cross build --target aarch64-unknown-linux-gnu`) links a vendored
  SQLite instead of depending on a `libsqlite3-dev`-equivalent being present in the `cross`
  Docker image — otherwise the Oracle cross-compile step breaks.
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
  (confirmed live to return full market objects per page, §2) — walk pages until an empty page,
  parsing every event's `slug` against `^(asset)-updown-(5m|15m|4h)-(\d+)$`, upserting matches.
  Exact ordering/date-range params to land reliably on updown-market pages (rather than mixed-in
  unrelated event types, seen live during verification) are an implementation-time detail to pin
  down, not a blocker — the core assumption (bulk call ⇒ full fields, no per-market follow-up)
  is confirmed.
- Add a small delay between pages (e.g. 200–500ms) and exponential backoff on a 429, since a full
  historical walk is hundreds of pages back-to-back and Gamma's rate limits for this aren't known.
- Derive the tracked asset list **dynamically** (e.g. from `raw*/` directory names at startup)
  rather than a hardcoded 7-asset list, so a newly-added asset isn't silently dropped from
  backfill.
- Pin down each per-asset earliest sealed date by scanning `raw*/` filenames rather than
  hardcoding one repo-wide start date — `HYPE_hl_2026-06-13.parquet` etc. show all 7 assets
  already present from day one in this repo's data (~2026-06-12/13), but that's a fact to
  re-derive at implementation time, not bake in as a constant.
- `--from`/`--to` flags let this be re-run for a narrower window (e.g. re-backfill one bad day)
  without re-walking the whole history.
- Idempotent by construction — `INSERT ... ON CONFLICT (asset, duration, slot) DO UPDATE`, so
  re-running backfill over an already-populated range is safe and just confirms/refreshes rows.
- If the table is empty on startup of the continuous mode, trigger a full backfill for the
  configured date range automatically first, rather than falling back to one-call-per-slot catch
  up (see §7).

## 7. Continuous-update mode

**One retry mechanism, not two** (revised — the first draft had a dedicated per-market retry
loop *and* an independent periodic sweep, which could both poll the same still-pending slug in
the same window). Just the sweep:

- When a slot rolls over (`current_slot_for(interval)`, same as `collect.rs`), insert a
  `pending` placeholder row for it (`outcome='UNRESOLVED'`, `last_checked_ts=NULL`) at
  `close_ts + 5 min` — no per-market timer/task, just a row that now exists to be picked up.
- A single periodic sweep (e.g. every 30s) queries the partial index (§4) for
  `outcome='UNRESOLVED' AND (last_checked_ts IS NULL OR last_checked_ts < now - retry_interval)`,
  polls Gamma for each, and upserts on a decisive signal (§2's "either signal" rule) or just
  bumps `check_attempts`/`last_checked_ts` if still pending. Past a deadline (proposed: 15 min
  past close — comfortably past the ~20s-to-4min settlement times observed live for 6/7 assets,
  see "Claude Thoughts" below; HYPE resolves by price threshold well within this too), the row
  simply stays `UNRESOLVED` and keeps getting swept at the retry interval rather than a separate
  timeout state — cheap at this row volume, and avoids a second code path for "gave up."
- **Startup/periodic gap reconciliation** (new — the sweep above only revisits rows that already
  exist; a full process outage spanning a slot close means **no row is ever created** for it, so
  the sweep alone would never discover the gap). On startup, and folded into the same periodic
  pass, for each tracked `(asset, duration)` diff "every expected slot from the last slot seen in
  the DB up to now" against rows actually present, and insert missing ones as fresh `UNRESOLVED`
  placeholders before the normal sweep logic picks them up. This is what makes a crash/restart
  lose nothing — only an in-memory timer would have been lost, and there isn't one anymore.

## 8. Where it runs / deploy

Gamma reads are plain HTTPS GETs — confirmed **not** geoblocked (the geoblock only affects CLOB
*order-placement* POSTs; GETs for balance/market-data are unaffected everywhere, see
`[[infra_network]]`). Recommend running it on **Oracle**, alongside `collect`, as its own
systemd unit (`price_feed resolve`, no flags) — keeps one writer for `resolutions.db`, consistent
with `collect` already owning `raw/`. Writer opens the DB with `PRAGMA journal_mode=WAL`.
`sync_oracle.sh` gets one addition: rather than rsyncing the live file directly (a WAL-mode DB can
have committed-but-not-yet-checkpointed writes; a reader on the other end could see a stale or
inconsistent copy), run `sqlite3 resolutions.db "VACUUM INTO '/tmp/resolutions_snapshot.db'"` on
Oracle first for a consistent, fully-checkpointed copy, then rsync that snapshot down alongside
the existing `raw*/` folders, same cron. Every consumer — Rust or Python, local or on Oracle —
opens its copy **read-only** (`SQLITE_OPEN_READ_ONLY` / `sqlite3.connect('file:...?mode=ro',
uri=True)`) so nothing but the one intended writer process can ever touch the file.

The one-shot `--backfill` can be run manually from either box (or the dev machine) since it's
read-mostly-GETs too — run it once against Oracle's `resolutions.db` after deploy, before
switching on the continuous mode.

## 9. Consumers

- **`trader/src/backtest.rs`**: add a small `rusqlite`-based reader (new dependency in
  `trader/Cargo.toml`, `bundled` feature for the same cross-compile reason as §5) that looks up
  `(asset, duration, slot)` and compares against the existing price-based `Machine::cycle_close()`
  outcome — surface a mismatch count/list rather than changing what the backtest itself simulates
  against (the price-based proxy is still what a live trader actually saw in real time; the
  resolutions table is the audit, not a replacement). The comparison must distinguish "proxy
  produced no outcome at all" (e.g. a price-feed gap — not a real disagreement) from "proxy said
  Up, real resolution said Down" — only the latter counts as a mismatch, otherwise every
  data-quality gap in `raw/` would inflate the mismatch count and bury genuine divergences.
- **`trade_reconcile.py`**'s Gamma Cross-Check section: try `resolutions.db` first
  (`sqlite3` stdlib, no new dependency, opened read-only per §8), fall back to the existing live
  `fetch_gamma_outcome(slug)` only for slugs not yet in the table (too recent, or a row still
  `UNRESOLVED`). Net effect: recon gets faster and Gamma-outage-resilient for the common case
  (older, already-resolved trades), while keeping the live-fetch fallback for genuinely fresh
  ones. Needs a new config value for the DB path, and must fall back to the existing live-fetch
  behavior (with a logged warning, not a crash) if the synced DB file isn't present yet — e.g. a
  fresh checkout, or local dev before the first sync has run.

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

Sent the plan as-is to `deepseek-v4-pro` for a critical pre-implementation review. Full response
condensed to the substantive points (some overlapping sub-points merged):

1. **`price_feed` itself needs `rusqlite` too** (it's the writer) — the plan only mentioned adding
   it to `trader/Cargo.toml`. Flagged that `rusqlite` compiles/links native SQLite, so the
   aarch64 cross-compile for Oracle needs the `bundled` feature (or a `libsqlite3-dev` toolchain
   dependency), or the build pipeline breaks.
2. **Bulk `/events` backfill endpoint might return slim/summarized market objects**, not the full
   field set (`outcomePrices`, `umaResolutionStatus`, `closedTime`, `clobTokenIds`) — said this
   was a "must-verify before coding," since if true the backfill strategy collapses back into the
   ~90k-call-per-slot problem it's meant to avoid.
3. **No rate-limiting/backoff mentioned for backfill pagination** — hundreds of pages hitting
   Gamma back-to-back risks 429s; wants a polite delay + exponential backoff on throttling.
4. **Startup catch-up gap in the continuous mode**: the housekeeping sweep only revisits existing
   `UNRESOLVED` rows — if the resolver process is down when a slot closes, **no row is ever
   created** for that slot, so the sweep has nothing to find and it's lost forever, not just
   delayed. Wants an explicit "diff expected slots vs. rows present" reconciliation on startup
   (and periodically), not just the DB-row sweep.
5. **Retry-loop / housekeeping-sweep overlap**: the per-market 30s retry loop and the 30-min
   sweep could both poll the same still-pending slug in the same window, wasting calls. Wants
   one retry path, not two.
6. **`closedTime` may not be the real resolution time** — argued it's plausibly just the market's
   betting-close time (traditional UMA questions can take much longer than 20s to actually
   settle/resolve after close), and that blindly storing it as `resolved_at_ts` could be
   measuring the wrong thing entirely.
7. **5-minute-after-close poll delay might be too early** — reasoned that UMA resolution can lag
   close by "seconds to several minutes," and that the plan's justification (borrowed from the
   live trader's deadline doc) didn't actually establish real resolution latency.
8. Smaller points, all reasonable and adopted without needing verification: add
   `(asset, duration, close_ts)` range index for "give me a window" queries; open the local
   synced DB copy read-only in every consumer; snapshot via `VACUUM INTO` before rsync rather than
   copying the live file; derive the tracked asset list dynamically (e.g. from `raw*/` directory
   names) instead of hardcoding 7 assets so a newly-added asset isn't silently dropped from
   backfill; distinguish "proxy outcome is `None`" from "proxy disagrees with real outcome" in the
   backtest mismatch check, so price-feed gaps aren't miscounted as real divergences; add a DB-path
   config + graceful (non-crashing) fallback in `trade_reconcile.py` if the synced DB isn't there
   yet; basic upsert/error logging for future debugging.

# Claude Thoughts on DeepSeek Review

Went back to the live Gamma API to check the two claims that would actually change the design
(#2 and #6/#7 above) rather than taking them on faith — both are falsifiable and this is a plan
doc, not a place to guess.

**#2 (bulk endpoint returns slim objects) — checked live, DeepSeek was wrong.**
`GET /events?tag_id=102127&closed=true&limit=1` returns the *full* market object — same ~70
fields as the single-slug endpoint, including `outcomePrices`, `umaResolutionStatus`,
`closedTime`, and `clobTokenIds` all present and populated. Confirmed the events on that endpoint
aren't limited to updown markets either (first page under default ordering surfaced an unrelated
quarterly BTC market from 2025), so §6's backfill still needs the right `closed`/date-range/`order`
query params to land on the right pages efficiently — noting that as an implementation-time detail
to pin down, not a blocker. The core assumption (bulk call ⇒ full fields, no per-market follow-up
needed) holds.

**#6/#7 (`closedTime` timing, 5-min delay) — checked live, more nuanced than either of us assumed.**
Fetched the most-recently-closed 5m slot for all 7 assets **~4 minutes after close**:

| Asset | `umaResolutionStatus` | `closedTime` vs `endDate` | `outcomePrices` |
|---|---|---|---|
| BTC/ETH/SOL/DOGE/XRP/BNB | `resolved` | close + 20s, identical across all 6 | `["0","1"]` or `["1","0"]` — fully decisive |
| HYPE | `None` (missing) | `None` | `["0.0005","0.9995"]` — decisive by threshold, but the status field never populated |

So DeepSeek's instinct to distrust a single "looks resolved" signal was right, but for a different
reason than it gave: **`closedTime` really is a close-to-real settlement timestamp for these
specific markets** — they're `automaticallyResolved: true` against a Chainlink stream, not a
disputed UMA question, so 6 of 7 assets are already fully resolved (both `umaResolutionStatus` and
a decisive `outcomePrices`) within ~20 seconds of close, and 5 minutes is not too early for those.
**HYPE is the actual problem**, and not the one either of us flagged: its `umaResolutionStatus`
field stays `None` even once the price has clearly settled, so a design that gates purely on
`umaResolutionStatus == "resolved"` (as §2 originally proposed, "better than the outcomePrices
threshold") would **never resolve HYPE rows** — they'd sit `UNRESOLVED` and get endlessly retried
by the sweep until the deadline, forever, every cycle. Fix: treat resolution as **either** signal
(`umaResolutionStatus == "resolved"` **or** `outcomePrices` containing a value ≥0.99), matching
what `trade_reconcile.py::fetch_gamma_outcome` already does today (threshold-only) — §2's framing
of `umaResolutionStatus` as strictly superior was wrong and is corrected here. Keep `closedTime`
as `resolved_at_ts` when present; fall back to "first poll that saw a decisive/resolved signal"
for rows where it's absent (matches DeepSeek's fallback suggestion in #6, kept for that case).

**Adopted as-is (no further verification needed, reasoning holds on inspection):**
- #1 (`rusqlite` + `bundled` feature in `price_feed/Cargo.toml` too, not just `trader`'s) — correct,
  §5 only mentioned the read side.
- #4 (startup/periodic gap reconciliation, not just an `UNRESOLVED`-row sweep) — correct and a real
  bug in the original design: a full outage across a slot boundary leaves **no row at all**, which
  the sweep (keyed on existing rows) can never discover. Folding into §7: on startup and on each
  housekeeping pass, additionally diff "expected slots for each tracked (asset, duration) from
  last-seen slot to now" against rows present, inserting `UNRESOLVED` placeholders for gaps before
  the normal retry logic picks them up.
- #5 (single retry path) — correct simplification; §7 collapses to one mechanism: the sweep alone,
  gated by `last_checked_ts` (skip a row if checked more recently than the retry interval), no
  separate concurrent per-market loop.
- #3, and all of §8's smaller points — reasonable, low-cost, adopted into §4/§6/§9/§10 without
  needing live verification.

**Net changes to the plan (§2, §4, §6, §7, §9 above already reflect these):** resolution check is
now "either signal," not `umaResolutionStatus`-primary; `resolved_at_ts` prefers `closedTime` but
falls back to observed-poll-time when absent; continuous mode has one retry mechanism (the sweep)
instead of two; added startup/periodic gap reconciliation against expected slots, not just
existing rows; added the `rusqlite`/`bundled` cross-compile note for `price_feed`'s own
`Cargo.toml`; added the `(asset, duration, close_ts)` range index, `VACUUM INTO` snapshot recipe,
read-only consumer opens, dynamic asset-list derivation, backtest `None`-vs-mismatch distinction,
and `trade_reconcile.py`'s graceful missing-DB fallback.
