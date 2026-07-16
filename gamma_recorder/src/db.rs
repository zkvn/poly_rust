//! `gamma.db` schema, upsert, sweep, and gap-reconciliation queries.
//!
//! Idempotent `CREATE TABLE IF NOT EXISTS` migration (no migration framework needed at
//! this size, per the plan doc §5) — a later Gamma data type gets its own table in this
//! same file/database.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Up,
    Down,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Up => "UP",
            Outcome::Down => "DOWN",
        }
    }
}

pub const UNRESOLVED: &str = "UNRESOLVED";

/// Opens (creating if needed) `gamma.db` at `path`, sets WAL + busy_timeout, and
/// ensures the schema exists.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating db parent dir {}", parent.display()))?;
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    init_schema(&conn)?;
    Ok(conn)
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS market_resolutions (
            asset            TEXT    NOT NULL,
            duration         TEXT    NOT NULL,
            slot             INTEGER NOT NULL,
            slug             TEXT    NOT NULL,
            condition_id     TEXT,
            open_ts          INTEGER NOT NULL,
            close_ts         INTEGER NOT NULL,
            outcome          TEXT    NOT NULL,
            up_token_id      TEXT,
            down_token_id    TEXT,
            resolved_at_ts   INTEGER,
            resolved_at_is_estimated INTEGER NOT NULL DEFAULT 0,
            check_attempts   INTEGER NOT NULL DEFAULT 0,
            last_checked_ts  INTEGER,
            PRIMARY KEY (asset, duration, slot)
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_market_resolutions_unresolved
            ON market_resolutions (outcome) WHERE outcome = 'UNRESOLVED';

        CREATE INDEX IF NOT EXISTS idx_market_resolutions_history
            ON market_resolutions (asset, duration, close_ts);

        CREATE INDEX IF NOT EXISTS idx_market_resolutions_slug
            ON market_resolutions (slug);
        "#,
    )?;
    Ok(())
}

/// One fetched-from-Gamma observation to merge into the table.
pub struct ResolutionUpsert {
    pub asset: String,
    pub duration: String,
    pub slot: i64,
    pub slug: String,
    pub condition_id: Option<String>,
    pub open_ts: i64,
    pub close_ts: i64,
    /// `None` if Gamma doesn't show a decisive signal yet.
    pub outcome: Option<Outcome>,
    pub up_token_id: Option<String>,
    pub down_token_id: Option<String>,
    pub resolved_at_ts: Option<i64>,
    pub resolved_at_is_estimated: bool,
    pub now_ts: i64,
}

/// Sticky-outcome upsert: once a row is resolved (`outcome != 'UNRESOLVED'`), a later
/// call with the same key never changes its outcome/resolved_at_ts/is_estimated — only
/// `check_attempts`/`last_checked_ts` still bump. This is what makes re-running backfill
/// or re-polling a already-resolved row in the sweep idempotent (plan doc §11 item 4).
pub fn upsert_resolution(conn: &Connection, row: &ResolutionUpsert) -> Result<()> {
    let outcome_str = row.outcome.map(Outcome::as_str).unwrap_or(UNRESOLVED);
    conn.execute(
        r#"
        INSERT INTO market_resolutions
            (asset, duration, slot, slug, condition_id, open_ts, close_ts, outcome,
             up_token_id, down_token_id, resolved_at_ts, resolved_at_is_estimated,
             check_attempts, last_checked_ts)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13)
        ON CONFLICT (asset, duration, slot) DO UPDATE SET
            slug = excluded.slug,
            condition_id = COALESCE(excluded.condition_id, market_resolutions.condition_id),
            up_token_id = COALESCE(excluded.up_token_id, market_resolutions.up_token_id),
            down_token_id = COALESCE(excluded.down_token_id, market_resolutions.down_token_id),
            outcome = CASE WHEN market_resolutions.outcome = 'UNRESOLVED'
                           THEN excluded.outcome ELSE market_resolutions.outcome END,
            resolved_at_ts = CASE WHEN market_resolutions.outcome = 'UNRESOLVED'
                                  THEN excluded.resolved_at_ts ELSE market_resolutions.resolved_at_ts END,
            resolved_at_is_estimated = CASE WHEN market_resolutions.outcome = 'UNRESOLVED'
                                            THEN excluded.resolved_at_is_estimated
                                            ELSE market_resolutions.resolved_at_is_estimated END,
            check_attempts = market_resolutions.check_attempts + 1,
            last_checked_ts = excluded.last_checked_ts
        "#,
        params![
            row.asset,
            row.duration,
            row.slot,
            row.slug,
            row.condition_id,
            row.open_ts,
            row.close_ts,
            outcome_str,
            row.up_token_id,
            row.down_token_id,
            row.resolved_at_ts,
            row.resolved_at_is_estimated as i64,
            row.now_ts,
        ],
    )?;
    Ok(())
}

/// Inserts a bare `UNRESOLVED` placeholder row for a slot that just closed, if one
/// doesn't already exist. Used by gap reconciliation — never overwrites an existing row.
pub fn insert_placeholder(
    conn: &Connection,
    asset: &str,
    duration: &str,
    slot: i64,
    open_ts: i64,
    close_ts: i64,
) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO market_resolutions
            (asset, duration, slot, slug, open_ts, close_ts, outcome)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT (asset, duration, slot) DO NOTHING
        "#,
        params![
            asset,
            duration,
            slot,
            crate::updown_slots::make_slug(asset, duration, slot),
            open_ts,
            close_ts,
            UNRESOLVED,
        ],
    )?;
    Ok(())
}

/// Highest `slot` recorded for this `(asset, duration)`, or `None` if there are no
/// rows yet (table effectively empty for this market).
pub fn max_slot(conn: &Connection, asset: &str, duration: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT MAX(slot) FROM market_resolutions WHERE asset = ?1 AND duration = ?2",
        params![asset, duration],
        |r| r.get(0),
    )
    .optional()
    .map(|opt| opt.flatten())
    .context("querying max slot")
}

/// True if the table has no rows at all (drives the "empty table triggers a full
/// backfill on continuous-mode startup" rule, plan doc §6).
pub fn is_empty(conn: &Connection) -> Result<bool> {
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM market_resolutions", [], |r| r.get(0))?;
    Ok(count == 0)
}

pub fn total_rows(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM market_resolutions", [], |r| r.get(0))?)
}

/// One row due for a sweep poll: still `UNRESOLVED`, already closed, and either never
/// checked or not checked within `retry_interval` seconds.
pub struct DueRow {
    pub asset: String,
    pub duration: String,
    pub slot: i64,
    pub slug: String,
}

pub fn due_unresolved(conn: &Connection, now_ts: i64, retry_interval: i64) -> Result<Vec<DueRow>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT asset, duration, slot, slug FROM market_resolutions
        WHERE outcome = 'UNRESOLVED'
          AND close_ts <= ?1
          AND (last_checked_ts IS NULL OR last_checked_ts < ?1 - ?2)
        "#,
    )?;
    let rows = stmt
        .query_map(params![now_ts, retry_interval], |r| {
            Ok(DueRow {
                asset: r.get(0)?,
                duration: r.get(1)?,
                slot: r.get(2)?,
                slug: r.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("gamma.db")).unwrap();
        (dir, conn)
    }

    fn sample(now_ts: i64, outcome: Option<Outcome>) -> ResolutionUpsert {
        ResolutionUpsert {
            asset: "BTC".to_string(),
            duration: "5m".to_string(),
            slot: 1_700_000_000,
            slug: "btc-updown-5m-1700000000".to_string(),
            condition_id: Some("0xabc".to_string()),
            open_ts: 1_700_000_000,
            close_ts: 1_700_000_300,
            outcome,
            up_token_id: Some("up-token".to_string()),
            down_token_id: Some("down-token".to_string()),
            resolved_at_ts: outcome.map(|_| 1_700_000_320),
            resolved_at_is_estimated: false,
            now_ts,
        }
    }

    #[test]
    fn insert_then_resolve_transitions_once() {
        let (_dir, conn) = open_tmp();
        upsert_resolution(&conn, &sample(1_700_000_301, None)).unwrap();
        assert_eq!(total_rows(&conn).unwrap(), 1);

        upsert_resolution(&conn, &sample(1_700_000_330, Some(Outcome::Up))).unwrap();
        let outcome: String = conn
            .query_row(
                "SELECT outcome FROM market_resolutions WHERE asset='BTC' AND duration='5m' AND slot=1700000000",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome, "UP");
    }

    #[test]
    fn resolved_row_is_sticky_against_later_upserts() {
        let (_dir, conn) = open_tmp();
        upsert_resolution(&conn, &sample(1_700_000_330, Some(Outcome::Up))).unwrap();

        // A second pass (e.g. re-run backfill) with a *different* resolved_at_ts must
        // not perturb the already-decided row.
        let mut second = sample(1_700_000_999, Some(Outcome::Down));
        second.resolved_at_ts = Some(9_999_999);
        upsert_resolution(&conn, &second).unwrap();

        let (outcome, resolved_at): (String, i64) = conn
            .query_row(
                "SELECT outcome, resolved_at_ts FROM market_resolutions WHERE asset='BTC' AND duration='5m' AND slot=1700000000",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(outcome, "UP");
        assert_eq!(resolved_at, 1_700_000_320);
    }

    #[test]
    fn placeholder_does_not_clobber_existing_row() {
        let (_dir, conn) = open_tmp();
        upsert_resolution(&conn, &sample(1_700_000_330, Some(Outcome::Up))).unwrap();
        insert_placeholder(
            &conn,
            "BTC",
            "5m",
            1_700_000_000,
            1_700_000_000,
            1_700_000_300,
        )
        .unwrap();

        let outcome: String = conn
            .query_row(
                "SELECT outcome FROM market_resolutions WHERE asset='BTC' AND duration='5m' AND slot=1700000000",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome, "UP");
        assert_eq!(total_rows(&conn).unwrap(), 1);
    }

    #[test]
    fn max_slot_and_is_empty() {
        let (_dir, conn) = open_tmp();
        assert!(is_empty(&conn).unwrap());
        assert_eq!(max_slot(&conn, "BTC", "5m").unwrap(), None);

        upsert_resolution(&conn, &sample(1_700_000_301, None)).unwrap();
        assert!(!is_empty(&conn).unwrap());
        assert_eq!(max_slot(&conn, "BTC", "5m").unwrap(), Some(1_700_000_000));
        assert_eq!(max_slot(&conn, "ETH", "5m").unwrap(), None);
    }

    #[test]
    fn due_unresolved_respects_close_ts_and_retry_interval() {
        let (_dir, conn) = open_tmp();
        // Not yet closed -> never due, regardless of last_checked_ts (NULL here).
        insert_placeholder(
            &conn,
            "BTC",
            "5m",
            2_000_000_000,
            2_000_000_000,
            2_000_000_300,
        )
        .unwrap();

        // Closed, never checked (last_checked_ts NULL from a fresh placeholder) -> due.
        insert_placeholder(
            &conn,
            "BTC",
            "5m",
            1_700_000_000,
            1_700_000_000,
            1_700_000_300,
        )
        .unwrap();

        let due = due_unresolved(&conn, 1_700_000_305, 30).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].slot, 1_700_000_000);

        // A sweep poll at 1_700_000_305 finds it still unresolved and stamps last_checked_ts.
        upsert_resolution(&conn, &sample(1_700_000_305, None)).unwrap();

        // Just checked -> not due again within retry_interval.
        let due_again = due_unresolved(&conn, 1_700_000_310, 30).unwrap();
        assert_eq!(due_again.len(), 0);

        // Past retry_interval -> due again.
        let due_later = due_unresolved(&conn, 1_700_000_340, 30).unwrap();
        assert_eq!(due_later.len(), 1);
    }
}
