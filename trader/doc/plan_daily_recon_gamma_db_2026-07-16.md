# Plan: daily recon — drop the dead CLOB-history table, resolve outcomes from gamma_recorder's local db first (2026-07-16)

## Status: implementing (part 1 already shipped, this doc covers part 2)

## Problem

Two follow-ups from today's daily-recon audit (README's "Trading engine — known incidents" /
2026-07-16):

1. **"CLOB Price History (token held)" table** — already removed
   (`trader/scripts/trade_reconcile.py`, commit `a5958f9`). Checked all 69 STOPLOSS/UNWIND trades
   ever logged: hold durations run 2.6s–75s, always far under the CLOB API's 10-min-bar fidelity
   floor, so the table never once showed a sample from inside an actual hold — pure noise,
   redundant with the `quality` verdict. Not revisited further in this doc; noted here only so
   this plan reads as the complete story of today's recon changes.

2. **Gamma outcome resolution still hits the live API on every run** — `fetch_gamma_outcome()`
   calls `GAMMA_API/events?slug=...` once per distinct slug in the window, every single recon run
   (cron every 2h + any ad-hoc run). `gamma_recorder` (native local process, see `## Cron /
   long-running process` in the main README) has been running since 2026-07-15, continuously
   polling the same Gamma API and caching every resolution it sees into a local SQLite db
   (`gamma_recorder/data/gamma.db`) — this was explicitly flagged as a "future consumer, not
   built yet" in `gamma_recorder/doc/plan_gamma_recorder_2026-07-15.md` (§1, §9's non-goals). This
   plan is that follow-through: make `trade_reconcile.py` use the local db as the primary
   resolution source, cutting live-API calls (rate-limit exposure, latency, and a hard runtime
   dependency on Gamma's API being reachable) down to only the slugs the local db hasn't caught
   yet.

## Current behavior (`fetch_gamma_outcome`, trader/scripts/trade_reconcile.py:126-152)

```python
def fetch_gamma_outcome(slug: str) -> Optional[str]:
    """Return 'UP' or 'DOWN' once the market is resolved on Gamma, else None."""
    # GET {GAMMA_API}/events?slug={slug}, parse outcomePrices, return the
    # outcome whose price >= 0.99, else None (unresolved or request failed).
```

Called once per distinct slug from `annotate_rows()` (line ~304), which builds the WIN/LOSS
correction + STOPLOSS/UNWIND quality verdicts the whole report depends on. `None` is
indistinguishable between "genuinely not resolved yet" and "request failed" — both already fall
back to `status = "PENDING"` in the existing code, which is the behavior this plan preserves.

## gamma_recorder's schema (confirmed by inspecting the live db)

```sql
CREATE TABLE market_resolutions (
    asset            TEXT    NOT NULL,
    duration         TEXT    NOT NULL,
    slot             INTEGER NOT NULL,
    slug             TEXT    NOT NULL,
    condition_id     TEXT,
    open_ts          INTEGER NOT NULL,
    close_ts         INTEGER NOT NULL,
    outcome          TEXT    NOT NULL,     -- 'UP' | 'DOWN' | 'UNRESOLVED'
    up_token_id      TEXT,
    down_token_id    TEXT,
    resolved_at_ts   INTEGER,
    resolved_at_is_estimated INTEGER NOT NULL DEFAULT 0,
    check_attempts   INTEGER NOT NULL DEFAULT 0,
    last_checked_ts  INTEGER,
    PRIMARY KEY (asset, duration, slot)
) WITHOUT ROWID
-- idx_market_resolutions_unresolved ON (outcome) WHERE outcome = 'UNRESOLVED'
-- idx_market_resolutions_history ON (asset, duration, close_ts)
```

`slug` isn't part of the primary key but is a plain column with real values in every row
(spot-checked against a known trade slug, `sol-updown-5m-1784141100` → row present, `outcome =
'DOWN'`, matches what the live-API path returned for the same slug in today's report) — a `WHERE
slug = ?` scan is fine at current row counts (~91k rows total; no index on `slug` today, added
below since this becomes a hot lookup path).

## Design

`fetch_gamma_outcome` becomes a thin dispatcher over two paths, keeping its existing signature and
`None`-means-pending contract so `annotate_rows` and every existing test (`test_gamma_reconcile.py`
mocks `fetch_gamma_outcome` itself, not its internals) need zero changes:

```python
GAMMA_DB_PATH = REPO_ROOT.parent / "gamma_recorder" / "data" / "gamma.db"

def _fetch_gamma_outcome_from_db(slug: str) -> Optional[str]:
    """Local gamma_recorder SQLite cache — fast, no network, no rate limit.
    Returns None (not just 'not found') for a missing row *or* an
    UNRESOLVED one; either way the caller falls back to the live API,
    since the db is a best-effort cache, not guaranteed complete."""
    if not GAMMA_DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{GAMMA_DB_PATH}?mode=ro", uri=True, timeout=5)
        try:
            row = conn.execute(
                "SELECT outcome FROM market_resolutions WHERE slug = ?", (slug,)
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    if row and row[0] in ("UP", "DOWN"):
        return row[0]
    return None


def fetch_gamma_outcome(slug: str) -> Optional[str]:
    """Return 'UP' or 'DOWN' once the market is resolved, else None.
    Tries gamma_recorder's local SQLite db first; falls back to the live
    Gamma API if the slug isn't there (or is still UNRESOLVED there) —
    the db is a cache, not a guarantee (recorder downtime, coverage
    gaps, an asset it doesn't track)."""
    from_db = _fetch_gamma_outcome_from_db(slug)
    if from_db is not None:
        return from_db
    return _fetch_gamma_outcome_from_api(slug)  # existing body, renamed
```

Key decisions:

- **Db is consulted first, API is the fallback — not the other way round, and not exclusive.**
  "Final resolution" (the user's framing) means the db wins whenever it has an answer; it does not
  mean *only* the db. `trade_reconcile.py` must not develop a hard dependency on `gamma_recorder`
  never having gaps — that process itself has no supervisor/restart-on-crash yet (README, `##
  Cron / long-running process`). A missing row and an `UNRESOLVED` row both fall through to the
  API identically — recon should never show *fewer* resolved trades than the pre-change behavior.
- **Read-only, WAL-safe, non-blocking.** `mode=ro` on the main db file; the `-wal`/`-shm`
  companions still need normal (non-ro) filesystem access for SQLite's WAL read protocol, which
  is fine — same user (`kev`), same host, already the case for `gamma_recorder`'s own reads.
  Empirically verified (ad-hoc query while `gamma_recorder` was actively writing, no lock errors).
  `timeout=5` bounds any pathological wait if the process is mid-checkpoint.
- **Fully swallow `sqlite3.Error`**, same posture as the existing API path's blanket
  `except Exception`. A locked/corrupt/missing db degrades to "always fall back to API," i.e.
  today's behavior — never a hard failure of the recon run.
- **Add `idx_market_resolutions_slug`** (new index on `market_resolutions(slug)`) — this makes
  every recon lookup an index seek instead of a full-table scan over ~91k rows.  This is the one
  change that touches `gamma_recorder` itself (a migration, additive, `IF NOT EXISTS`) rather than
  purely `trade_reconcile.py`.
- **No new CLI flag.** Db-first-then-API is an internal implementation detail; no need to expose a
  way to force one path or the other for a script that already treats resolution as "try the cheap
  thing, fall back to the definitive thing."

## Explicitly out of scope

- Any change to `gamma_recorder` itself beyond the one additive index (no schema redesign, no new
  columns).
- Backfilling/reconciling the two sources against each other (e.g. flagging if db and API
  disagree) — out of scope until there's evidence they ever do.
- Removing the live-API path entirely — it stays as the fallback indefinitely, not a
  soon-to-be-deleted shim.
- The `_fetch_token_ids_for_slug`/CLOB-history code — already deleted in part 1, not touched here.

## Risk / edge cases

- **gamma_recorder not running / db absent** (e.g. a fresh checkout, or the process crashed and
  hasn't been restarted): `GAMMA_DB_PATH.exists()` is `False` → every lookup falls straight to the
  API, identical to current behavior. No regression.
- **Db has a stale/wrong outcome**: extremely unlikely — `outcome` is only ever written from a
  Gamma resolution the recorder itself fetched (same source of truth as the live-API path); the
  `resolved_at_is_estimated` flag only marks the *timestamp* as guessed during gap-recovery
  seeding, never the outcome. Not treated as a risk that needs mitigation here.
- **Concurrent write during read**: WAL mode is specifically designed for this (readers never
  block writers, writers never block readers); already exercised implicitly since
  `gamma_recorder`'s own periodic sweep both reads and writes concurrently.
- **A slug resolves on Gamma but `gamma_recorder` hasn't polled it yet**: falls to
  `UNRESOLVED`/missing → API fallback fires → same latency-to-resolution as today, not worse.

## Testing plan

- New unit tests (`test_trade_reconcile.py`, temp-sqlite pattern matching the file's existing
  `tempfile.TemporaryDirectory()` style):
  1. Db has a resolved (`UP`/`DOWN`) row for the slug → returned directly, API not called
     (assert via `patch.object(mod, "_fetch_gamma_outcome_from_api")` and `assert_not_called`).
  2. Db has the slug but `outcome = 'UNRESOLVED'` → falls back to API.
  3. Db doesn't have the slug at all → falls back to API.
  4. `GAMMA_DB_PATH` doesn't exist on disk → falls back to API (no exception).
  5. Db file exists but isn't a valid SQLite file (corrupt) → falls back to API, no crash.
- Full existing suite (`test_trade_reconcile.py` + `test_gamma_reconcile.py`, 131 tests as of the
  CLOB-history removal) must stay green unmodified — `annotate_rows`'s tests mock
  `fetch_gamma_outcome` at the top level, so they're insulated from this refactor by construction.
- End-to-end parity check: regenerate today's report (`trade_reconcile.py --today`) against the
  live `gamma_recorder/data/gamma.db` and confirm identical outcomes/verdicts to the last
  (API-only) run — same 12 trades, same resolved/pending counts, same match rate. This is the real
  proof the swap is behavior-preserving, not just unit-test-clean.
