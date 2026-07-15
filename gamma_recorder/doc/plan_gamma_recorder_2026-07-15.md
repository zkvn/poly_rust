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
not a slimmed-down summary. **Selectivity, verified live 2026-07-15**: `tag_id=102127` alone
(default ordering) is *not* selective — a default-order page surfaced an unrelated 2025 quarterly
market, and a 3-page/300-event pull with no `order` param matched the updown slug pattern 0/300
times. Adding `&order=startDate&ascending=false` fixes this completely — the same 3-page/300-event
pull with that param matched **300/300**. So the backfill query (§6) must use
`order=startDate&ascending=false`, not just `tag_id`+`closed`; with it, the "few hundred pages,
not thousands" page-count estimate holds.

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
    resolved_at_is_estimated INTEGER NOT NULL DEFAULT 0,  -- 1 when resolved_at_ts is a poll-time fallback (no closedTime, e.g. HYPE), not Gamma's own timestamp
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
would want and the primary key alone doesn't serve. `resolved_at_is_estimated` (added after the
second DeepSeek review, §"DeepSeek Comments — round 2" below) keeps the approximate HYPE-style
fallback timestamp distinguishable from a real `closedTime` rather than silently blending the two
under one column name.

## 5. Crate layout — fully independent, no shared code with `price_feed`/`trader`

```
gamma_recorder/
  Cargo.toml           # own deps: reqwest, serde_json, chrono, tokio, clap, rusqlite (bundled)
  Cross.toml           # aarch64 cross-compile config, mirrors price_feed/Cross.toml
  Dockerfile           # local test image, mirrors trader/Dockerfile's pattern
  src/
    main.rs            # clap CLI: Cmd::Resolve { backfill, from, to }
    gamma.rs            # Gamma HTTP client: fetch-by-slug, fetch-page (bulk), signal-resolved logic
    updown_slots.rs      # slot/slug math for updown markets specifically — asset+interval -> slug,
                          # e.g. "{asset}-updown-{suffix}-{slot}". Named for this one data type
                          # deliberately, not "slots.rs" — a future Gamma data type (e.g. weather)
                          # would very likely key its markets completely differently and get its
                          # own module, not a forced-generic one (DeepSeek round 2 feedback).
    db.rs               # schema/migrations (idempotent `CREATE TABLE IF NOT EXISTS` per table,
                         # no migration framework needed at this size), upsert, sweep queries
  systemd/
    poly-gamma-recorder.service
  doc/
    plan_gamma_recorder_2026-07-15.md   # this file
```

`updown_slots.rs` **duplicates** the tiny slot/slug formula already in `price_feed/src/collect.rs`
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

- Paginate `GET /events?tag_id=102127&closed=true&order=startDate&ascending=false&start_date_min=<from>&start_date_max=<to>&limit=100&offset=N`
  — the `order=startDate&ascending=false` is required, not optional: verified live that without
  it, `tag_id=102127` alone is not selective for updown markets (0/300 matched in a 3-page pull);
  with it, 300/300 matched (§2). Walk pages until an empty page, parsing every event's `slug`
  against `^(asset)-updown-(5m|15m|4h)-(\d+)$`, upserting matches.
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
  hardcoded 7-asset list, so a newly-added asset isn't silently dropped from backfill. This is a
  soft coupling to `price_feed`'s directory layout, not its code (flagged in DeepSeek round 2) —
  mitigate with a `--assets BTC,ETH,...` override flag, and log loudly (don't silently no-op) if
  the scan finds zero assets or the directory is missing, falling back to the override or erroring
  out rather than running an empty backfill.
- Pin down each per-asset earliest sealed date the same way (scanning `price_feed/raw*/`
  filenames) rather than hardcoding a start date. Confirmed live: all 7 assets already have data
  from day one (~2026-06-12/13) — good enough as the default `--from`, per the user's
  confirmation that "backfill from June is fine" — but re-derive per-asset rather than assume
  every future asset also started on that date. Log the derived per-asset start dates so a
  suspicious one (e.g. a newly-added asset whose earliest file is *after* the requested `--from`)
  is visible, not silent.
- `--from`/`--to` flags allow re-running over a narrower window (e.g. re-backfill one bad day)
  without re-walking the whole history; `--to` defaults to today (UTC) if omitted rather than
  requiring a literal date every time. Idempotent by construction —
  `INSERT ... ON CONFLICT (asset, duration, slot) DO UPDATE` — so re-running over an
  already-populated range is safe (verify in §11: a row that already has a decisive outcome
  shouldn't be perturbed by a second backfill pass over the same range).
- If the table is empty on startup of the continuous mode, trigger a full backfill for the
  configured date range automatically first, rather than falling back to one-call-per-slot catch-up.

## 7. Continuous-update mode

One retry mechanism, not two competing ones (a dedicated per-market retry loop *and* an
independent sweep would risk both polling the same still-pending slug in the same window):

- When a slot rolls over (own `updown_slots.rs` math, §5), insert a `pending` placeholder row
  (`outcome='UNRESOLVED'`, `last_checked_ts=NULL`) — no per-market timer/task, just a row that now
  exists to be picked up.
- A single periodic sweep (e.g. every 30s) queries the partial index (§4) for
  `outcome='UNRESOLVED' AND close_ts <= now AND (last_checked_ts IS NULL OR last_checked_ts < now
  - retry_interval)` — **the `close_ts <= now` guard matters** (added after DeepSeek round 2):
  without it, gap reconciliation below would insert placeholder rows for slots that haven't closed
  yet, and the sweep would waste polls on markets that can't possibly be resolved. Polls Gamma for
  each due row, with a small fixed delay between individual polls (e.g. 100ms) so a burst of many
  simultaneously-due rows — e.g. right after recovering from a long outage — can't fire a
  request storm; upserts on a decisive signal (§2's "either signal" rule) or bumps
  `check_attempts`/`last_checked_ts` if still pending. No hard timeout state — a row simply stays
  `UNRESOLVED` and keeps getting swept at the retry interval, which is cheap at this row volume and
  avoids a second code path for "gave up." (6/7 assets resolve within ~20s–4min of close, live
  confirmed; HYPE resolves by the price-threshold signal well within the same window, so in
  practice nothing lingers long.)
- **Startup/periodic gap reconciliation**: the sweep above only revisits rows that already exist —
  a full process outage spanning a slot close means **no row is ever created** for it, so the
  sweep alone would never discover the gap. On startup, and folded into the same periodic pass,
  for each tracked `(asset, duration)` diff "every expected **already-closed** slot from the last
  slot seen in the DB up to the most-recently-closed one" (not up to "now" — a slot that has
  opened but not yet closed is not a gap) against rows actually present, and insert missing ones
  as fresh `UNRESOLVED` placeholders before the normal sweep picks them up. This is what makes a
  crash/restart lose nothing — there's no in-memory timer to lose in the first place.
- **Optional, low-priority**: an internal-gap sanity check (a bug that skips one slot but keeps
  inserting later ones would slip past the "last-seen slot" heuristic above, since it only looks
  at the frontier, not holes behind it). A periodic check that `slot(n+1) - slot(n) == interval`
  for consecutive rows per `(asset, duration)` would catch this; low probability, worth a log-line
  alert if found rather than building automatic backfill-on-detect for it.

## 8. Retention

**Keep every row forever** — per the user's confirmation, `UNRESOLVED` rows (or any row) are
cheap at this volume and useful as a "known gap" audit trail; no expiry/pruning logic.

## 9. Where it runs / deploy

Gamma reads are plain HTTPS GETs — not geoblocked (the geoblock only affects CLOB
*order-placement* POSTs; GETs are unaffected everywhere, see `[[infra_network]]`). Runs on
**Oracle** as its own systemd unit (`poly-gamma-recorder.service`, running `gamma_recorder
resolve`, `Restart=always`/`RestartSec=5` so a crash doesn't leave the recorder permanently down —
matches the existing `poly-collector`/`trader-live.service` convention), independent of
`poly-collector` — separate binary, separate working directory
(`~/apps/poly_rust/gamma_recorder/`), separate log. Writer opens `gamma.db` with
`PRAGMA journal_mode=WAL` and `PRAGMA busy_timeout=5000` (so an ad hoc `sqlite3` debugging session
opened concurrently waits briefly instead of hitting `SQLITE_BUSY` immediately).

Syncing a copy to local: rather than rsyncing the live file directly (a WAL-mode DB can have
committed-but-not-yet-checkpointed writes; a reader could see a stale/inconsistent copy), run
`sqlite3 gamma.db "VACUUM INTO '/tmp/gamma_snapshot.db'"` on Oracle first for a consistent,
checkpointed copy, then rsync that snapshot down — its own small script
(`gamma_recorder/scripts/sync_oracle.sh`), independent of `price_feed/scripts/sync_oracle.sh`.
Any future consumer opens its copy **read-only**.

The one-shot `--backfill` can be run from either box or the dev machine (read-mostly GETs) — run
it once against Oracle's `gamma.db` after deploy, before switching on the continuous mode.

**Operational nice-to-have, optional, not a build requirement:** a "set and forget" daemon with no
consumer watching it can silently fall behind (e.g. the sweep task panics/hangs) with nobody
noticing until someone goes looking for a specific row that isn't there. Not building this now —
the module works without it — but worth a one-line log heartbeat per sweep pass (row counts
checked/resolved/still-pending) so the signal already exists if anyone ever points a log-watcher
at it later.

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
   and confirm the stored `outcome`/`resolved_at_ts` match. Also compute the **exact** expected row
   count for the backfilled date range (e.g. `days × 288` for 5m, `× 96` for 15m, `× 6` for 4h,
   per asset) and assert the DB count matches, not just "in the right ballpark" — a systematic
   off-by-a-day-or-page bug wouldn't necessarily show up as an obviously wrong ballpark.
4. **Idempotency check**: run the same backfill range twice; assert the second run leaves every
   already-decisive row unchanged (no flapping between UP/DOWN, no `resolved_at_ts` drift) —
   confirms the `ON CONFLICT ... DO UPDATE` upsert is actually safe to re-run, not just assumed to
   be.
5. **Continuous-mode soak test**: run `gamma_recorder:local resolve` (no flags) against a fresh or
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
6. **Concurrent-reader check**, run alongside the soak above: a second process
   (`sqlite3 gamma.db "SELECT ..."` on a timer) polling the live DB while the writer is active,
   confirming no `database is locked` errors surface — validates the WAL + `busy_timeout` setup
   under real concurrent access, not just in theory.
7. **Simulated-outage test**: during the soak, stop the container (`docker stop`), wait past at
   least one 5m slot boundary, then restart it. Confirm the "inserted N missing rows" gap-fill log
   line fires with `N > 0` on restart, and that the DB ends up with a row for the slot that closed
   during the downtime — this is the one test that actually exercises the gap-reconciliation logic
   §7 depends on; the steady-state soak alone never triggers it.
8. **Fault-injection test** (lower priority, do if time allows): point the container at a
   deliberately flaky endpoint (a local proxy that returns `503`/times out for a fraction of
   requests, or simply the real Gamma URL with a temporarily wrong port to force connection
   failures) for a few minutes, and confirm the backoff logic engages (visible in logs) rather than
   spinning in a tight retry loop that spikes CPU.
9. **Only after (2)–(7) pass** (8 optional) does cross-compiling (`cross build --release --target
   aarch64-unknown-linux-gnu`, mirroring `price_feed/Cross.toml`) and rsyncing to Oracle happen.
   Optionally, for extra confidence in the `rusqlite`/`bundled` cross-compiled SQLite specifically:
   run the cross-compiled aarch64 binary locally under `qemu-aarch64-static` against a small
   backfill slice and diff its `gamma.db` output against the x86-64 Docker run's — catches any
   subtle arch-specific SQLite behavior before it ever reaches Oracle. Not a hard gate given the
   effort involved; worth doing if the earlier steps raise any doubt.

---

# DeepSeek Comments

Sent this revised (independent-crate) plan to `deepseek-v4-pro`, including the round-1 appendix
below for context on what changed and why, and asked it to specifically assess the new
independent-crate approach, the Docker/CPU-memory test plan's actual coverage, the single-sweep
design under the new architecture, and the "room to grow" generality goal. Condensed to the
substantive points:

1. **Overall verdict: "solid, well-reasoned, and safe to implement."** The independent-crate
   approach, duplicated slot/slug formula, single-sweep design, and SQLite choice were all judged
   sound as-is — no fundamental redesign flagged.
2. **Dynamic asset-list discovery from `price_feed/raw*/` is a soft coupling worth guarding.**
   Scanning another crate's data directory isn't a code dependency, but a directory rename/re-org
   would silently break it (empty asset list → backfill does nothing). Wants a `--assets` override
   flag and loud logging (not a silent no-op) if the scan comes back empty.
3. **The Docker/CPU-memory test plan (§11) doesn't exercise several failure modes it implicitly
   claims to cover**: no induced Gamma flakiness (backoff logic never actually triggered), no
   simulated outage + restart (the gap-reconciliation logic — the one piece of this design that
   only matters during a real failure — is never actually exercised by a steady 60-minute soak),
   no exact-count backfill assertion (only "ballpark"), no concurrent-reader-while-writing check
   (WAL is claimed safe but never tested with a second connection open), no idempotent-rerun check.
4. **Single-sweep + gap reconciliation is "correct and the cleanest possible"** under the new
   architecture, with two tightening points: (a) the gap-reconciliation frontier should stop at
   the most-recently-*closed* slot, not "now" — otherwise it inserts (and the sweep then polls)
   placeholder rows for slots that haven't closed yet, wasting calls; (b) an internal gap (a slot
   silently skipped while later ones keep getting inserted) would slip past the
   "last-seen-slot" frontier heuristic entirely, since it only looks forward, never checks for
   holes behind it.
5. **The sweep itself has no rate limiter for the non-error case** — only backfill's page-to-page
   delay was specified. If many rows go `UNRESOLVED` simultaneously (e.g. right after recovering
   from a long outage, when gap-reconciliation just inserted a batch of placeholders), the sweep
   could fire a burst of individual Gamma polls with no spacing between them.
6. **Generality for future Gamma data types**: judged as "correctly avoids both overbuilding and
   underbuilding" — independent crate + one-table-per-data-type + no premature generic abstraction
   layer. One naming nit: `slots.rs` is named generically but is entirely up-down-market-specific;
   a second data type with a different keying scheme would need its own module anyway, so a
   more specific name avoids future confusion.
7. **Backfill selectivity of `tag_id=102127` wasn't actually confirmed** — only checked that it
   returns updown markets, not that it returns *only* updown markets; if the tag also covers
   unrelated market types, the "few hundred pages" estimate could be off by an order of magnitude.
8. **Smaller points, all adopted without needing further verification**: `PRAGMA busy_timeout`;
   `resolved_at_ts`'s HYPE-style fallback deserves its own flag/column rather than silently
   blending "real Gamma timestamp" and "our poll time" under one name (raised in both rounds now);
   idempotent `CREATE TABLE IF NOT EXISTS` migration approach for future tables; `systemd`
   `Restart=always`; optional heartbeat logging for future observability; `--to` defaulting to
   today instead of requiring a literal date.

# Claude Thoughts on DeepSeek Review

Checked the one claim from this round that's actually falsifiable and load-bearing for the
backfill design — point 7, `tag_id=102127` selectivity — live, rather than accepting either my
own prior "implementation-time detail" hand-wave or DeepSeek's "not actually confirmed" concern at
face value.

**Result: confirmed selective, but only with the right query params — DeepSeek's underlying
worry was legitimate, and the live check found the actual fix, not just reassurance.** Pulled 3
pages / 300 events at `tag_id=102127&closed=true` with **no `order` param**: 0/300 matched the
updown slug pattern — i.e. tag `102127` alone genuinely is *not* selective, exactly as DeepSeek
suspected. Re-ran the identical pull with `order=startDate&ascending=false` added: **300/300**
matched. So the fix isn't "don't worry, it's selective" — it's "the query must include
`order=startDate&ascending=false`, which the original plan treated as an optional/TBD detail
rather than a hard requirement." Folded into §2 and §6 directly: the backfill query in §6 now
specifies the full param set including ordering, not just `tag_id`+`closed`, and the "few hundred
pages" estimate is now grounded in a verified selective query rather than an optimistic guess.

**Adopted as-is, no further verification needed (all architecture-independent, testing-methodology,
or straightforward-correctness points):**
- #2 (`--assets` override + loud-not-silent failure on empty asset-list scan) — §6.
- #3 (expanded Docker test plan: fault injection, simulated-outage/restart, exact row-count
  assertion, concurrent-reader check, idempotent-rerun check) — all folded into §11 as new
  numbered steps; the simulated-outage/restart test in particular is the one test that actually
  exercises gap-reconciliation, which the original plan's soak test never would have triggered on
  its own.
- #4a (gap-reconciliation frontier stops at the most-recently-*closed* slot, sweep query gets a
  `close_ts <= now` guard) — real bug in the original wording ("up to now" silently included the
  currently-open, not-yet-closed slot) — fixed in §7.
- #4b (internal-gap sanity check) — added as an explicit optional/low-priority note in §7, not a
  build requirement.
- #5 (rate-limit the sweep itself, not just backfill pagination) — real gap; a batch of
  gap-reconciliation placeholders recovering from an outage would otherwise burst-poll Gamma with
  no spacing — fixed in §7 with a small fixed delay between individual sweep polls.
- #6 (`updown_slots.rs` instead of `slots.rs`) — adopted directly in §5, cheap and clear.
- #8's smaller points — `busy_timeout` and `Restart=always` in §9, `resolved_at_is_estimated`
  column in §4, idempotent-migration note in §5's crate layout, heartbeat-logging note added as an
  optional item near §9/§10, `--to` default in §6.

**Net changes this round (already reflected in §2/§4/§5/§6/§7/§9/§11 above)**: backfill query now
requires `order=startDate&ascending=false` (verified live, not optional); `--assets` override +
loud failure on empty asset scan; gap reconciliation stops at the last *closed* slot, not "now";
sweep gets its own inter-poll delay; new schema column distinguishing an estimated
`resolved_at_ts` from a real one; `busy_timeout`/`Restart=always` deploy details; renamed
`slots.rs` → `updown_slots.rs`; §11 gained fault-injection, simulated-outage/restart,
exact-count, concurrent-reader, and idempotent-rerun tests, plus an optional QEMU aarch64
cross-check.

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
