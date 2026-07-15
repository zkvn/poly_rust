mod db;
mod gamma;
mod updown_slots;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{Days, NaiveDate, Utc};
use clap::Parser;
use rusqlite::Connection;

use db::ResolutionUpsert;

/// Gamma tag id for up-down markets (5m/15m/4h), confirmed selective only when combined
/// with `order=startDate&ascending=false` — see plan doc §2/§6.
const UPDOWN_TAG_ID: &str = "102127";
const BACKFILL_PAGE_LIMIT: u32 = 100;
const BACKFILL_PAGE_SLEEP: Duration = Duration::from_millis(500);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const SWEEP_RETRY_INTERVAL_SECS: i64 = 60;
const SWEEP_POLL_SLEEP: Duration = Duration::from_millis(100);

#[derive(Parser)]
#[command(name = "gamma_recorder")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Records official Polymarket up-down market resolutions into gamma.db.
    Resolve {
        /// One-shot bulk backfill instead of the long-running continuous mode.
        #[arg(long)]
        backfill: bool,
        /// Backfill start date (YYYY-MM-DD). Defaults to the earliest sealed date found
        /// under `--price-feed-dir`'s raw*/ directories.
        #[arg(long)]
        from: Option<String>,
        /// Backfill end date (YYYY-MM-DD), inclusive. Defaults to today (UTC).
        #[arg(long)]
        to: Option<String>,
        /// Override the tracked asset list instead of scanning `--price-feed-dir`.
        #[arg(long, value_delimiter = ',')]
        assets: Option<Vec<String>>,
        #[arg(long, default_value = "data/gamma.db")]
        db: PathBuf,
        /// Read-only scan target for dynamic asset discovery + earliest-date inference.
        #[arg(long, default_value = "../price_feed")]
        price_feed_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Resolve {
            backfill,
            from,
            to,
            assets,
            db,
            price_feed_dir,
        } => run_resolve(backfill, from, to, assets, db, price_feed_dir).await,
    }
}

async fn run_resolve(
    backfill: bool,
    from: Option<String>,
    to: Option<String>,
    assets_override: Option<Vec<String>>,
    db_path: PathBuf,
    price_feed_dir: PathBuf,
) -> Result<()> {
    let scan = discover_assets(&price_feed_dir, assets_override)?;
    println!(
        "[gamma_recorder] tracking {} asset(s): {}",
        scan.assets.len(),
        scan.assets.join(",")
    );
    for (asset, date) in &scan.earliest_dates {
        println!("[gamma_recorder]   earliest sealed date for {asset}: {date}");
    }

    let conn = db::open(&db_path).with_context(|| format!("opening {}", db_path.display()))?;
    let client = reqwest::Client::builder()
        .user_agent("gamma_recorder/0.1")
        .build()
        .context("building reqwest client")?;

    let today = Utc::now().date_naive();
    let default_from = scan.earliest_dates.values().min().copied().unwrap_or(today);

    if backfill {
        let from = parse_date_arg(from.as_deref(), default_from)?;
        let to = parse_date_arg(to.as_deref(), today)?;
        let stats = run_backfill(&conn, &client, &scan.assets, from, to).await?;
        println!(
            "[gamma_recorder] backfill done: {} pages, {} rows upserted (range {from} .. {to}), {} total rows in db",
            stats.pages,
            stats.upserted,
            db::total_rows(&conn)?
        );
        return Ok(());
    }

    run_continuous(&conn, &client, &scan.assets, default_from, today).await
}

fn parse_date_arg(raw: Option<&str>, default: NaiveDate) -> Result<NaiveDate> {
    match raw {
        None => Ok(default),
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .with_context(|| format!("parsing date '{s}' (expected YYYY-MM-DD)")),
    }
}

struct AssetScan {
    assets: Vec<String>,
    earliest_dates: BTreeMap<String, NaiveDate>,
}

/// Dynamically derives the tracked asset list (and each asset's earliest sealed date)
/// from `price_feed`'s `raw*/` directory filenames — a read-only filesystem scan, not a
/// code dependency on `price_feed`. `--assets` overrides the scan entirely. Fails loudly
/// (never silently no-ops) if the scan finds no raw*/ dirs or zero assets and no override
/// was given, per plan doc §6.
fn discover_assets(
    price_feed_dir: &Path,
    assets_override: Option<Vec<String>>,
) -> Result<AssetScan> {
    if let Some(assets) = assets_override {
        if assets.is_empty() {
            bail!("--assets was passed but empty");
        }
        let assets: Vec<String> = assets.iter().map(|a| a.to_uppercase()).collect();
        return Ok(AssetScan {
            assets,
            earliest_dates: BTreeMap::new(),
        });
    }

    let mut assets = BTreeSet::new();
    let mut earliest_dates: BTreeMap<String, NaiveDate> = BTreeMap::new();
    let mut scanned_any_dir = false;

    let top_entries = std::fs::read_dir(price_feed_dir)
        .with_context(|| format!("scanning {} for raw*/ dirs", price_feed_dir.display()))?;
    for top_entry in top_entries {
        let top_entry = top_entry?;
        let path = top_entry.path();
        if !path.is_dir() {
            continue;
        }
        let dir_name = top_entry.file_name().to_string_lossy().to_string();
        if !dir_name.starts_with("raw") {
            continue;
        }
        scanned_any_dir = true;
        for file_entry in std::fs::read_dir(&path)? {
            let file_entry = file_entry?;
            if !file_entry.path().is_file() {
                continue;
            }
            let fname = file_entry.file_name().to_string_lossy().to_string();
            let Some((asset, rest)) = fname.split_once('_') else {
                continue;
            };
            if asset.is_empty() || !asset.chars().all(|c| c.is_ascii_uppercase()) {
                continue;
            }
            assets.insert(asset.to_string());
            if let Some(date) = extract_date(rest) {
                earliest_dates
                    .entry(asset.to_string())
                    .and_modify(|d| *d = (*d).min(date))
                    .or_insert(date);
            }
        }
    }

    if !scanned_any_dir {
        bail!(
            "no raw*/ directories found under {} — pass --assets to override",
            price_feed_dir.display()
        );
    }
    if assets.is_empty() {
        bail!(
            "scanned raw*/ dirs under {} but found zero assets — pass --assets to override",
            price_feed_dir.display()
        );
    }

    Ok(AssetScan {
        assets: assets.into_iter().collect(),
        earliest_dates,
    })
}

/// Pulls a `YYYY-MM-DD` date out of a filename remainder like `binance_2026-07-02_13.parquet`.
fn extract_date(rest: &str) -> Option<NaiveDate> {
    rest.split('_').find_map(|part| {
        let candidate = part.get(0..10)?;
        NaiveDate::parse_from_str(candidate, "%Y-%m-%d").ok()
    })
}

struct BackfillStats {
    pages: u64,
    upserted: u64,
}

/// One-shot bulk backfill: paginate `/events/keyset?tag_id=...&order=startDate&ascending=false`
/// over `[from, to]` via cursor (`after_cursor`/`next_cursor`), upserting every event whose
/// slug matches an updown market for a tracked asset. See plan doc §6 for the
/// rate-limiting/backoff rationale; see `gamma::fetch_events_page`'s doc comment for why
/// this uses the keyset endpoint rather than the plain offset-paginated one (the latter
/// caps out around offset+limit <= 2100, verified live 2026-07-15, well under a single
/// day's worth of events across all tracked assets).
async fn run_backfill(
    conn: &Connection,
    client: &reqwest::Client,
    assets: &[String],
    from: NaiveDate,
    to: NaiveDate,
) -> Result<BackfillStats> {
    let from_str = from.format("%Y-%m-%d").to_string();
    // Exclusive upper bound (to + 1 day) so a single-day range (from == to) doesn't hit
    // Gamma's "invalid time range" validation on min == max, and so the whole `to` day
    // is actually included.
    let to_exclusive = to
        .checked_add_days(Days::new(1))
        .context("computing backfill end date")?
        .format("%Y-%m-%d")
        .to_string();

    let mut cursor: Option<String> = None;
    let mut pages: u64 = 0;
    let mut upserted: u64 = 0;

    loop {
        let query = vec![
            ("tag_id", UPDOWN_TAG_ID.to_string()),
            ("closed", "true".to_string()),
            ("order", "startDate".to_string()),
            ("ascending", "false".to_string()),
            ("start_date_min", from_str.clone()),
            ("start_date_max", to_exclusive.clone()),
            ("limit", BACKFILL_PAGE_LIMIT.to_string()),
        ];
        let (events, next_cursor) =
            gamma::fetch_events_page(client, &query, cursor.as_deref()).await?;
        pages += 1;
        if events.is_empty() {
            break;
        }

        let now_ts = updown_slots::now_secs();
        for event in &events {
            for market in &event.markets {
                if upsert_if_tracked(conn, market, assets, now_ts)? {
                    upserted += 1;
                }
            }
        }

        match next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
        tokio::time::sleep(BACKFILL_PAGE_SLEEP).await;
    }

    Ok(BackfillStats { pages, upserted })
}

/// Parses+upserts one Gamma market if its slug is a tracked-asset updown market.
/// Returns `true` if it matched and was upserted.
fn upsert_if_tracked(
    conn: &Connection,
    market: &gamma::GammaMarket,
    assets: &[String],
    now_ts: i64,
) -> Result<bool> {
    let Some(parts) = updown_slots::parse_slug(&market.slug) else {
        return Ok(false);
    };
    if !assets.iter().any(|a| a == &parts.asset) {
        return Ok(false);
    }
    let interval = updown_slots::interval_secs(&parts.duration)
        .expect("parse_slug already validated the duration suffix") as i64;
    let signal = gamma::resolution_signal(market, now_ts)?;

    let row = ResolutionUpsert {
        asset: parts.asset,
        duration: parts.duration,
        slot: parts.slot,
        slug: market.slug.clone(),
        condition_id: market.condition_id.clone(),
        open_ts: parts.slot,
        close_ts: parts.slot + interval,
        outcome: signal.as_ref().map(|s| s.outcome),
        up_token_id: signal.as_ref().and_then(|s| s.up_token_id.clone()),
        down_token_id: signal.as_ref().and_then(|s| s.down_token_id.clone()),
        resolved_at_ts: signal.as_ref().map(|s| s.resolved_at_ts),
        resolved_at_is_estimated: signal
            .as_ref()
            .map(|s| s.resolved_at_is_estimated)
            .unwrap_or(false),
        now_ts,
    };
    db::upsert_resolution(conn, &row)?;
    Ok(true)
}

/// Long-running daemon: one periodic sweep folding gap reconciliation (insert
/// placeholders for any already-closed slot missing from the DB, frontier capped at the
/// most-recently-*closed* slot) and due-row polling — a single retry mechanism, not two
/// competing ones. See plan doc §7.
async fn run_continuous(
    conn: &Connection,
    client: &reqwest::Client,
    assets: &[String],
    default_from: NaiveDate,
    today: NaiveDate,
) -> Result<()> {
    if db::is_empty(conn)? {
        println!("[gamma_recorder] gamma.db empty on startup — running full backfill first");
        let stats = run_backfill(conn, client, assets, default_from, today).await?;
        println!(
            "[gamma_recorder] startup backfill done: {} pages, {} rows upserted",
            stats.pages, stats.upserted
        );
    }

    println!("[gamma_recorder] entering continuous mode, sweep every {SWEEP_INTERVAL:?}");
    loop {
        let now_ts = updown_slots::now_secs();
        let gap_inserted = reconcile_gaps(conn, assets, now_ts)?;

        let due = db::due_unresolved(conn, now_ts, SWEEP_RETRY_INTERVAL_SECS)?;
        let mut resolved = 0u64;
        let mut checked = 0u64;
        for row in &due {
            checked += 1;
            match sweep_one(conn, client, row).await {
                Ok(true) => resolved += 1,
                Ok(false) => {}
                Err(e) => eprintln!("[gamma_recorder] sweep poll failed for {}: {e:#}", row.slug),
            }
            tokio::time::sleep(SWEEP_POLL_SLEEP).await;
        }

        println!(
            "[gamma_recorder] heartbeat: checked={checked} resolved={resolved} gap_inserted={gap_inserted}"
        );
        tokio::time::sleep(SWEEP_INTERVAL).await;
    }
}

/// Inserts missing `UNRESOLVED` placeholders for every already-closed slot between the
/// last slot seen in the DB (exclusive) and the most-recently-closed slot (inclusive),
/// per tracked `(asset, duration)`. The `close_ts <= now` frontier cap matters: without
/// it this would insert rows for slots that haven't closed yet, and the sweep would then
/// waste polls on markets that can't possibly be resolved.
fn reconcile_gaps(conn: &Connection, assets: &[String], now_ts: i64) -> Result<u64> {
    let mut inserted = 0u64;
    for asset in assets {
        for (duration, interval_u64) in updown_slots::DURATIONS {
            let interval = interval_u64 as i64;
            let last_closed_slot = updown_slots::current_slot_for(interval_u64, now_ts) - interval;
            let start_slot = match db::max_slot(conn, asset, duration)? {
                Some(max) => max + interval,
                // No history at all for this (asset, duration) yet — seed just the
                // current frontier rather than walking all history here; a genuinely
                // empty table is handled by the startup backfill instead.
                None => last_closed_slot,
            };
            let mut slot = start_slot;
            while slot <= last_closed_slot {
                db::insert_placeholder(conn, asset, duration, slot, slot, slot + interval)?;
                inserted += 1;
                slot += interval;
            }
        }
    }
    Ok(inserted)
}

/// Polls Gamma for one due row and upserts the result. Returns `true` if it resolved.
async fn sweep_one(conn: &Connection, client: &reqwest::Client, row: &db::DueRow) -> Result<bool> {
    let interval = updown_slots::interval_secs(&row.duration)
        .context("due row had an unrecognized duration")? as i64;
    let market = gamma::fetch_by_slug(client, &row.slug).await?;
    let now_ts = updown_slots::now_secs();

    let (signal, condition_id) = match &market {
        Some(m) => (gamma::resolution_signal(m, now_ts)?, m.condition_id.clone()),
        None => (None, None),
    };
    let resolved = signal.is_some();

    let upsert = ResolutionUpsert {
        asset: row.asset.clone(),
        duration: row.duration.clone(),
        slot: row.slot,
        slug: row.slug.clone(),
        condition_id,
        open_ts: row.slot,
        close_ts: row.slot + interval,
        outcome: signal.as_ref().map(|s| s.outcome),
        up_token_id: signal.as_ref().and_then(|s| s.up_token_id.clone()),
        down_token_id: signal.as_ref().and_then(|s| s.down_token_id.clone()),
        resolved_at_ts: signal.as_ref().map(|s| s.resolved_at_ts),
        resolved_at_is_estimated: signal
            .as_ref()
            .map(|s| s.resolved_at_is_estimated)
            .unwrap_or(false),
        now_ts,
    };
    db::upsert_resolution(conn, &upsert)?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_date_from_daily_filename_remainder() {
        assert_eq!(
            extract_date("binance_2026-07-01.parquet"),
            NaiveDate::from_ymd_opt(2026, 7, 1)
        );
    }

    #[test]
    fn extract_date_from_hourly_filename_remainder() {
        assert_eq!(
            extract_date("binance_2026-07-02_13.parquet"),
            NaiveDate::from_ymd_opt(2026, 7, 2)
        );
    }

    #[test]
    fn extract_date_none_for_junk() {
        assert_eq!(extract_date("not_a_date.parquet"), None);
    }

    #[test]
    fn discover_assets_scans_raw_dirs_and_earliest_dates() {
        let dir = tempfile::tempdir().unwrap();
        let raw = dir.path().join("raw");
        let raw15 = dir.path().join("raw_15_mins");
        std::fs::create_dir_all(&raw).unwrap();
        std::fs::create_dir_all(&raw15).unwrap();
        std::fs::write(raw.join("BTC_binance_2026-06-15.parquet"), b"").unwrap();
        std::fs::write(raw.join("BTC_binance_2026-06-12_08.parquet"), b"").unwrap();
        std::fs::write(raw15.join("ETH_binance_2026-06-20.parquet"), b"").unwrap();
        // Junk subdir with an underscore prefix should be ignored (not recursed into).
        let junk = raw.join("_stale_pre_hourly_seal_2026-07-02");
        std::fs::create_dir_all(&junk).unwrap();
        std::fs::write(junk.join("XRP_binance_2020-01-01.parquet"), b"").unwrap();

        let scan = discover_assets(dir.path(), None).unwrap();
        assert_eq!(scan.assets, vec!["BTC".to_string(), "ETH".to_string()]);
        assert_eq!(
            scan.earliest_dates.get("BTC").copied(),
            NaiveDate::from_ymd_opt(2026, 6, 12)
        );
        assert_eq!(
            scan.earliest_dates.get("ETH").copied(),
            NaiveDate::from_ymd_opt(2026, 6, 20)
        );
    }

    #[test]
    fn discover_assets_errors_loudly_on_missing_raw_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(discover_assets(dir.path(), None).is_err());
    }

    #[test]
    fn discover_assets_errors_loudly_on_empty_scan() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("raw")).unwrap();
        assert!(discover_assets(dir.path(), None).is_err());
    }

    #[test]
    fn assets_override_skips_scan_entirely() {
        let dir = tempfile::tempdir().unwrap(); // no raw*/ dirs at all
        let scan =
            discover_assets(dir.path(), Some(vec!["btc".to_string(), "eth".to_string()])).unwrap();
        assert_eq!(scan.assets, vec!["BTC".to_string(), "ETH".to_string()]);
    }
}
