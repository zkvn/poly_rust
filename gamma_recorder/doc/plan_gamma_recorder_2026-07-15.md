# Plan — `gamma_recorder`: independent Polymarket Gamma-API results recorder

## Bottom line up front

**New standalone crate**, `gamma_recorder/`, sibling to `price_feed/`, `trader/`, `siglab/` at
repo root — **not** a subcommand of `price_feed`, and **zero code changes to `price_feed/` or
`trader/`** (revised from the 2026-07-14 draft of this plan, which proposed folding it into
`price_feed` as a new subcommand — the user wants full independence instead, both because
`price_feed`/`trader` must stay untouched and because this module is expected to grow beyond
just up-down market resolutions into other Gamma data over time).

First concrete feature — official up-down market resolutions — has two modes sharing one
Gamma-fetch/upsert code path:

1. `--backfill --from 2026-06-12 --to <today>` — one-shot: bulk-paginate Gamma's `/events` list
   endpoint, upsert every matching `{asset}-updown-{5m,15m,4h}-{slot}` event found.
2. No flags (long-running daemon) — records each tracked market's resolution ~5 min after its
   slot closes, with retry/backoff and gap reconciliation (§7).

**Storage: SQLite** (`gamma_recorder/data/gamma.db`), not Parquet/CSV/JSON/TOML — see §3.

**Must be validated locally in Docker with CPU/memory monitoring before it ever touches
Oracle** — see §11. This is a plan for review — nothing implemented yet.

---

## 1. Motivation

- **Continuous backtest validation** (potential future consumer, out of scope for this module —
  see §10). `trader/src/backtest.rs` currently derives a cycle's outcome purely from recorded
  price data (`Machine::cycle_close()` compares `last_binance` against `cycle_open_binance`) —
  a reasonable proxy, but not what Polymarket actually settles against. A local table of
  ground-truth outcomes would let a backtest continuously diff its price-based outcome against
  the real one, if and when someone wires that up later.
- **Daily recon** (potential future consumer, also out of scope here). `trade_reconcile.py`'s
  Gamma Cross-Check section currently calls Gamma live, once per trade, every recon run.
- **Room to grow beyond up-down resolutions.** The user may want this module to record other
  Gamma API results later (e.g. weather/World Cup event data, volume/liquidity snapshots, other
  market metadata) — hence a fully independent crate with its own `Cargo.toml`/binary/deploy
  story, rather than coupling this to `price_feed`'s release cycle. Concretely: each new Gamma
  data type gets its own subcommand + table when it's actually needed, not a speculative
  abstraction layer built now for data types that don't exist yet (per this repo's own
  "don't design for hypothetical future requirements" convention) — the independence of the
  *crate* is what buys the room to grow; the *code* stays as simple as the one concrete feature
  being built.

## 2. Confirmed Gamma response shape + official docs (checked 2026-07-14/15)

`GET https://gamma-api.polymarket.com/events?slug=btc-updown-5m-<slot>` →
`events[0].markets[0]` has (fields actually observed, not assumed):

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

**Official rate limit** (docs.polymarket.com/quickstart/introduction/rate-limits, fetched
2026-07-15): Gamma's `/events` endpoint is limited to **500 requests / 10 seconds**, and the
documented behavior on exceeding it is *throttling* (requests delayed/queued), not an outright
rejection — no 429 is documented, but treat one as possible and back off on it anyway (cheap
insurance, §6). This is the number the backfill's inter-page sleep is sized against (§6).

**Resolution check uses either signal, not `umaResolutionStatus` alone**: `umaResolutionStatus
== "resolved"` **or** `outcomePrices` containing a value ≥0.99. Live-checked the
most-recently-closed 5m slot across all 7 assets ~4 minutes after close: BTC/ETH/SOL/DOGE/XRP/BNB
already show both signals resolved, but **HYPE's `umaResolutionStatus` stayed `None` even though
`outcomePrices` was already decisive** (`["0.0005","0.9995"]`) — gating on `umaResolutionStatus`
alone would never resolve HYPE rows. `trade_reconcile.py::fetch_gamma_outcome` (unrelated code,
not touched by this plan — see §10) already uses the `outcomePrices` threshold today; matching
that signal here too, on top of the stronger one where it's available, is what makes HYPE work.

`closedTime`, when present, is used for `resolved_at_ts` — for these `automaticallyResolved: true`
Chainlink-settled markets it's consistently close+20s (verified live). Falls back to "timestamp
of the first poll that saw a decisive/resolved signal" for rows where `closedTime` is absent (the
HYPE case).

**Bulk pagination** (`?tag_id=102127&closed=true&limit=100&offset=N`, offset-based, confirmed
live) returns the *full* market object per event, same field set as the single-slug endpoint —
not a slimmed-down summary. Confirmed the endpoint isn't limited to updown markets (a default-order
page surfaced an unrelated 2025 quarterly market), so the exact `order`/date-range params to land
reliably on updown-market pages are an implementation-time detail, not a blocker — the load-bearing
assumption (bulk call ⇒ full fields, no per-market follow-up needed) holds.

## 3. Storage format: SQLite, not Parquet/CSV/JSON/TOML

~7 assets × (288 five-min + 96 fifteen-min + 6 four-hour) ≈ **2,730 new rows/day**; full history
back to 2026-06-12 is on the order of 100–150k rows. Small, forever. Access pattern is point
lookups/upserts by `(asset, duration, slot)`, with a row's life cycle `UNRESOLVED → resolved`
(update-in-place, not pure append) — and, per §1, likely more tables of a similar shape for other
Gamma data types later.

| Format | Fit | Why / why not |
|---|---|---|
| **Parquet** | Poor | Immutable columnar — no row-level upsert; would need the same tmp-file/reseal dance `price_feed/src/collect.rs` uses for high-volume tick data, for no benefit at this row count. |
| **CSV** | Poor | No upsert/index; flipping a row from pending to resolved means a full-file rewrite, with a rewrite-race risk for anything reading concurrently. |
| **JSON** | Poor | Same append/upsert/rewrite-race problem as CSV, no index. |
| **TOML** | Wrong shape | Not designed for many repeated tabular records at all. |
| **SQLite** | **Recommended** | Indexed point lookups, real `UPSERT`, and — with WAL mode — safe concurrent reads from other processes while one writer appends/updates. `rusqlite` (Rust) with the `bundled` feature needs no system SQLite; Python's `sqlite3` is stdlib. Both are read-only consumers of a file this crate owns exclusively — no dependency either direction. |

One database file, one table per Gamma data type (`market_resolutions` is the only one specced
here); a later data type gets its own table in the same file rather than a new database, since
they'd share the same writer process and deploy story anyway.

## 4. Schema

```sql
CREATE TABLE market_resolutions (
    asset            TEXT    NOT NULL,   -- 'BTC','ETH','SOL','DOGE','XRP','BNB','HYPE'
    duration         TEXT    NOT NULL,   -- '5m' | '15m' | '4h'
    slot             INTEGER NOT NULL,   -- unix seconds, floor(ts / interval) * interval
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

`WITHOUT ROWID` since the natural key is already unique and compact. The partial index keeps the
housekeeping sweep's "find rows still pending" query cheap past 100k+ rows. The second index
supports range queries ("all BTC-5m resolutions between date X and Y") that a future consumer
would want and the primary key alone doesn't serve.

## 5. Crate layout — fully independent, no shared code with `price_feed`/`trader`

```
gamma_recorder/
  Cargo.toml           # own deps: reqwest, serde_json, chrono, tokio, clap, rusqlite (bundled)
  Cross.toml           # aarch64 cross-compile config, mirrors price_feed/Cross.toml
  Dockerfile           # local test image, mirrors trader/Dockerfile's pattern
  src/
    main.rs            # clap CLI: Cmd::Resolve { backfill, from, to }
    gamma.rs            # Gamma HTTP client: fetch-by-slug, fetch-page (bulk), signal-resolved logic
    slots.rs            # slot/slug math — asset+interval -> slug, e.g. "{asset}-updown-{suffix}-{slot}"
    db.rs               # schema/migrations, upsert, sweep queries
  systemd/
    poly-gamma-recorder.service
  doc/
    plan_gamma_recorder_2026-07-15.md   # this file
```

`slots.rs` **duplicates** the tiny slot/slug formula already in `price_feed/src/collect.rs`
(`make_slug`/`current_slot_for`) rather than importing it — the two crates share zero code by
design. This is a deliberate, small, low-risk duplication: the formula is Polymarket's own public
market-naming convention (`{asset}-updown-{suffix}-{slot}`, `slot = floor(unix_ts / interval) *
interval`), not an internal implementation detail that could silently drift between the two
crates — if Polymarket ever changed it, both copies would need updating in lockstep regardless of
which crate owns the "canonical" version. Worth a one-line comment in both files cross-referencing
each other so a future reader isn't surprised to find the same three lines twice.

No root Cargo workspace exists today (`price_feed`, `trader`, `siglab` are already three
independent crates — `siglab` path-depends on `trader`, but nothing depends on `price_feed`), so
adding a fourth independent crate is the established pattern, not a new one.

## 6. Backfill mode

- Paginate `GET /events?tag_id=102127&closed=true&start_date_min=<from>&start_date_max=<to>&limit=100&offset=N`
  (confirmed live to return full market objects, §2) — walk pages until an empty page, parsing
  every event's `slug` against `^(asset)-updown-(5m|15m|4h)-(\d+)$`, upserting matches.
- **Sleep between pages** — official limit is 500 req/10s (§2), so correctness doesn't strictly
  require throttling at all at this call volume, but the user asked for generous spacing to avoid
  any HTTP issues in practice, so default to **500ms between page requests** (≈2 req/s, ~40x
  under the documented budget) rather than cutting it close. Add exponential backoff (start 2s,
  double, cap at ~60s) on any non-2xx response before retrying the same page. At ~500ms/page,
  backfilling June 12 → today (order of a few hundred to ~1,000 pages once the right filter params
  are pinned down at implementation time, §2) takes on the order of minutes, not hours — acceptable
  for a one-shot.
- Derive the tracked asset list **dynamically** from `price_feed/raw*/` directory names at
  startup (read-only filesystem scan — not a code dependency on `price_feed`) rather than a
  hardcoded 7-asset list, so a newly-added asset isn't silently dropped from backfill.
- Pin down each per-asset earliest sealed date the same way (scanning `price_feed/raw*/`
  filenames) rather than hardcoding a start date. Confirmed live: all 7 assets already have data
  from day one (~2026-06-12/13) — good enough as the default `--from`, per the user's
  confirmation that "backfill from June is fine" — but re-derive per-asset rather than assume
  every future asset also started on that date.
- `--from`/`--to` flags allow re-running over a narrower window (e.g. re-backfill one bad day)
  without re-walking the whole history. Idempotent by construction —
  `INSERT ... ON CONFLICT (asset, duration, slot) DO UPDATE` — so re-running over an
  already-populated range is safe.
- If the table is empty on startup of the continuous mode, trigger a full backfill for the
  configured date range automatically first, rather than falling back to one-call-per-slot catch-up.

## 7. Continuous-update mode

One retry mechanism, not two competing ones (a dedicated per-market retry loop *and* an
independent sweep would risk both polling the same still-pending slug in the same window):

- When a slot rolls over (own `slots.rs` math, §5), insert a `pending` placeholder row
  (`outcome='UNRESOLVED'`, `last_checked_ts=NULL`) — no per-market timer/task, just a row that now
  exists to be picked up.
- A single periodic sweep (e.g. every 30s) queries the partial index (§4) for
  `outcome='UNRESOLVED' AND (last_checked_ts IS NULL OR last_checked_ts < now - retry_interval)`,
  polls Gamma for each, and upserts on a decisive signal (§2's "either signal" rule) or bumps
  `check_attempts`/`last_checked_ts` if still pending. No hard timeout state — a row simply stays
  `UNRESOLVED` and keeps getting swept at the retry interval, which is cheap at this row volume and
  avoids a second code path for "gave up." (6/7 assets resolve within ~20s–4min of close, live
  confirmed; HYPE resolves by the price-threshold signal well within the same window, so in
  practice nothing lingers long.)
- **Startup/periodic gap reconciliation**: the sweep above only revisits rows that already exist —
  a full process outage spanning a slot close means **no row is ever created** for it, so the
  sweep alone would never discover the gap. On startup, and folded into the same periodic pass,
  for each tracked `(asset, duration)` diff "every expected slot from the last slot seen in the DB
  up to now" against rows actually present, and insert missing ones as fresh `UNRESOLVED`
  placeholders before the normal sweep picks them up. This is what makes a crash/restart lose
  nothing — there's no in-memory timer to lose in the first place.

## 8. Retention

**Keep every row forever** — per the user's confirmation, `UNRESOLVED` rows (or any row) are
cheap at this volume and useful as a "known gap" audit trail; no expiry/pruning logic.

## 9. Where it runs / deploy

Gamma reads are plain HTTPS GETs — not geoblocked (the geoblock only affects CLOB
*order-placement* POSTs; GETs are unaffected everywhere, see `[[infra_network]]`). Runs on
**Oracle** as its own systemd unit (`poly-gamma-recorder.service`, running `gamma_recorder
resolve`), independent of `poly-collector` — separate binary, separate working directory
(`~/apps/poly_rust/gamma_recorder/`), separate log. Writer opens `gamma.db` with
`PRAGMA journal_mode=WAL`.

Syncing a copy to local: rather than rsyncing the live file directly (a WAL-mode DB can have
committed-but-not-yet-checkpointed writes; a reader could see a stale/inconsistent copy), run
`sqlite3 gamma.db "VACUUM INTO '/tmp/gamma_snapshot.db'"` on Oracle first for a consistent,
checkpointed copy, then rsync that snapshot down — its own small script
(`gamma_recorder/scripts/sync_oracle.sh`), independent of `price_feed/scripts/sync_oracle.sh`.
Any future consumer opens its copy **read-only**.

The one-shot `--backfill` can be run from either box or the dev machine (read-mostly GETs) — run
it once against Oracle's `gamma.db` after deploy, before switching on the continuous mode.

## 10. Non-goals / explicitly out of scope for this plan

- **No changes to `price_feed/` or `trader/` code, ever, in this plan.** Reading the resulting
  `gamma.db` from a backtest or from `trade_reconcile.py` is a plausible future use (§1) but a
  separate decision for later — this plan builds the recorder and stops there.
- No integration with `trader/src/backtest.rs`'s outcome logic, no new dependency in
  `trader/Cargo.toml`, no changes to `trade_reconcile.py`'s Gamma Cross-Check section.
- No other Gamma data types built now (weather/World Cup/volume snapshots etc.) — noted in §1 as
  future room to grow, not designed or built here.

## 11. Local validation plan — Docker + CPU/memory monitoring, before Oracle ever sees this

**Required gate before any Oracle deploy**, per the user's instruction. Mirrors the existing
`trader/Dockerfile` "local test image" pattern (same x86-64 arch as the dev host, fast build, used
to validate before touching Oracle — see root `README.md` → "Build and deploy" and
`[[infra_network]]`).

1. **Build**: `gamma_recorder/Dockerfile`, multi-stage (`rust:1-bookworm` builder →
   `debian:bookworm-slim` runtime + `ca-certificates`), same shape as `trader/Dockerfile`.
   `docker build -t gamma_recorder:local gamma_recorder/`.
2. **Backfill soak test**, resource-constrained on purpose to catch runaway usage rather than
   hide it behind a generous limit:
   ```bash
   docker run --rm --memory=512m --cpus=1 \
     -v "$(pwd)/gamma_recorder/data:/data" \
     gamma_recorder:local resolve --backfill --from 2026-06-12 --to <today> --db /data/gamma.db
   ```
   Run `docker stats` (or `docker stats --no-stream` polled every few seconds into a log) in
   parallel for the full run; capture peak/steady-state CPU% and memory. Expect flat, low memory
   (streaming page-by-page upserts, no in-memory accumulation of the full result set) and CPU
   dominated by network wait, not compute.
3. **Correctness spot-check**: after backfill, pick ~10–20 random `(asset, duration, slot)` rows
   from `gamma.db` spanning different days/assets/durations, re-fetch each slug live from Gamma,
   and confirm the stored `outcome`/`resolved_at_ts` match. Also confirm row counts per
   `(asset, duration)` are in the right ballpark for the date range (e.g. ~288/day for 5m).
4. **Continuous-mode soak test**: run `gamma_recorder:local resolve` (no flags) against a fresh or
   the backfilled DB for a sustained period (propose ≥60 min, covering multiple 5m/15m rollovers
   and at least one 4h one if the window allows) under the same `--memory`/`--cpus` constraint,
   with `docker stats` logging throughout. Watch specifically for:
   - Memory climbing over time (would indicate the sweep or gap-reconciliation query is
     accumulating something instead of being properly bounded/paged).
   - CPU spiking in a pattern that doesn't match the 30s sweep cadence (would indicate a busy-loop
     bug).
   - The gap-reconciliation logic firing spuriously on every pass instead of only when a genuine
     gap exists (cheap to check: log line count for "inserted N missing rows" should be ~0 during
     a healthy continuous run with no artificial outage).
5. **Only after (2)–(4) pass** does cross-compiling (`cross build --release --target
   aarch64-unknown-linux-gnu`, mirroring `price_feed/Cross.toml`) and rsyncing to Oracle happen.

---

# DeepSeek Comments

*(pending — sending the revised plan to DeepSeek next)*

# Claude Thoughts on DeepSeek Review

*(pending)*

---

# Appendix — DeepSeek review of the 2026-07-14 draft (superseded design)

The prior draft of this plan proposed folding this into `price_feed` as a new subcommand,
reusing `price_feed`'s `make_slug`/`current_slot_for` directly and adding `rusqlite` to both
`price_feed/Cargo.toml` and `trader/Cargo.toml` for a backtest-integration consumer. That
architecture is superseded by the independent-crate design above (§5, §10) per the user's
explicit direction — `price_feed`/`trader` are not to be touched at all. The review findings
below are preserved because most of them are architecture-independent and already folded into
this draft (§2's either-signal fix, §6's rate-limiting, §7's single-retry-path and gap
reconciliation, §4's range index, §9's `VACUUM INTO` snapshot recipe); they're kept here for a
paper trail rather than repeated in the main body.

Sent the prior plan as-is to `deepseek-v4-pro` for a critical pre-implementation review. Condensed
to the substantive points:

1. The writer crate needs `rusqlite` too, not just the reader — flagged the `bundled` feature
   requirement for aarch64 cross-compilation. *(Still applies — now just to `gamma_recorder`'s
   own `Cargo.toml`, §5.)*
2. Bulk `/events` backfill might return slim/summarized market objects, not full fields — flagged
   as a must-verify-before-coding risk. *(Checked live: false alarm, full fields present, §2.)*
3. No rate-limiting/backoff mentioned for backfill pagination. *(Adopted, §6 — now grounded in
   the official documented 500 req/10s limit rather than guessed.)*
4. Startup catch-up gap in the continuous mode: the housekeeping sweep only revisits existing
   `UNRESOLVED` rows; a full outage across a slot boundary leaves no row at all, undiscoverable by
   the sweep. *(Adopted, §7 — real bug, confirmed and fixed.)*
5. Retry-loop / housekeeping-sweep overlap could double-poll the same slug. *(Adopted, §7 —
   collapsed to one mechanism.)*
6. `closedTime` may not be the real resolution time, could be just close-of-betting.
   *(Checked live: more nuanced than either DeepSeek or the original plan assumed — see the
   HYPE finding in §2. `closedTime` is accurate for 6/7 assets; the real gap was HYPE's missing
   `umaResolutionStatus`, which neither DeepSeek nor the original plan caught.)*
7. 5-minute-after-close poll delay might be too early. *(Checked live: not too early for 6/7
   assets — resolution lands within ~20s–4min of close. HYPE's issue was the missing status
   field, not timing, so this specific critique didn't hold up, but the underlying instinct to
   distrust a single signal was right for a different reason.)*
8. Smaller adopted points: `(asset, duration, close_ts)` range index; open synced DB copies
   read-only; snapshot via `VACUUM INTO` before rsync; derive tracked asset list dynamically
   instead of hardcoding; distinguish "proxy produced no outcome" from "proxy disagreed with real
   outcome" in any future backtest consumer (relevant again if that integration ever happens,
   §10); graceful fallback if a consumer's local DB copy is missing.
