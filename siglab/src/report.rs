//! Signal-summary report, written as Markdown with collapsible sections nested two levels
//! deep: one `<details>` per real HKT hour (newest first, headed by a real `### {hour}`
//! Markdown heading), each containing one `<details>` per report-writer run that landed
//! within that hour (there can be several now that runs fire every
//! `--report-interval-secs`, e.g. every 15 min instead of hourly). Trades are *not* split
//! per run — one "Trades this hour" section, merged across every trade logged since the
//! start of the real HKT hour, sits at the top of the hour's body and is regenerated fresh
//! on every write (2026-07-14, explicit request); each nested run section covers only
//! market-state/staleness/CPU snapshots for its own `window_secs` window. File name is
//! `signal_report_{YYYY-MM-DD}_{AM|PM}.md` (HKT date, split at the 12:00 HKT boundary —
//! 2026-07-14, after the single-file-per-day report grew to 2.2MB and became unwieldy to
//! open) — a new file starts each half-day, and every run within a half-day updates the
//! *same* file, either by nesting a new run inside the still-open current-hour section or
//! by starting a fresh hour section (which also collapses the previous hour's section,
//! since only the current hour stays expanded by default).
//!
//! The hour/run boundary is tracked with plain HTML comments (`<!-- siglab-hour:... -->`,
//! `<!-- siglab-hour-body-start/end -->`, `<!-- siglab-run -->`,
//! `<!-- siglab-hour-trades-start/end -->`, `<!-- siglab-config-start/end -->`) rather than
//! by parsing the `<details>` tree — much simpler than generic bracket-matching, and safe
//! since these exact strings never otherwise appear in rendered content.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{FixedOffset, NaiveDate, TimeZone as _, Timelike, Utc};

use crate::cgroup;
use crate::config::SiglabConfig;
use crate::record::SiglabTradeRecord;
use crate::snapshot::SharedSnapshots;

/// Marks the start of one report-writer run's section within a `trades_{date}_{HH}.md`
/// file — runs stack newest-first, one file per real HKT hour (so unlike the pre-2026-07-15
/// AM/PM-split format, there's no longer any need for a separate hour-boundary marker: the
/// filename itself identifies the hour).
const RUN_MARKER: &str = "<!-- siglab-run -->\n";
// Wraps the config-table section so it can be replaced wholesale on every write (reflects
// whatever `markets.toml` currently says, not whatever it said the first time the file was
// created) without disturbing the header-based file-identity check in
// `write_hourly_report` — that check only looks at the fixed `header` string, and this
// section always lives right after it.
const CONFIG_MARKER_START: &str = "<!-- siglab-config-start -->\n";
const CONFIG_MARKER_END: &str = "<!-- siglab-config-end -->\n";
// Same replace-wholesale pattern as CONFIG_MARKER_*, but scoped inside one hour's body:
// wraps the hour-merged trades section so every write regenerates it fresh from every trade
// logged so far this hour, instead of stacking one separate trades table per 15-min run
// (2026-07-14, explicit request — the file is still written every
// `--report-interval-secs`, just no longer split that way).
const HOUR_TRADES_MARKER_START: &str = "<!-- siglab-hour-trades-start -->\n";
const HOUR_TRADES_MARKER_END: &str = "<!-- siglab-hour-trades-end -->\n";

/// One staleness event, timestamped so the report can filter to "this run's window".
#[derive(Debug, Clone)]
pub struct StaleLogEntry {
    pub at_ms: i64,
    pub market: String,
    pub silent_ms: i64,
    pub bucket_ms: i64,
}

pub type SharedStaleLog = Arc<Mutex<Vec<StaleLogEntry>>>;

pub fn new_stale_log() -> SharedStaleLog {
    Arc::new(Mutex::new(Vec::new()))
}

pub fn log_stale_event(
    log: &SharedStaleLog,
    at_ms: i64,
    market: String,
    silent_ms: i64,
    bucket_ms: i64,
) {
    if let Ok(mut v) = log.lock() {
        v.push(StaleLogEntry {
            at_ms,
            market,
            silent_ms,
            bucket_ms,
        });
        // Cap unbounded growth if report-writing ever falls behind — keep the most recent
        // 5000 events, plenty for a 15-minute report cadence.
        if v.len() > 5000 {
            let excess = v.len() - 5000;
            v.drain(0..excess);
        }
    }
}

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

fn now_hkt() -> chrono::DateTime<FixedOffset> {
    Utc::now().with_timezone(&hkt())
}

/// `report_dir/{date}/` — one folder per real HKT day (2026-07-15, replacing the
/// AM/PM-split flat files: even split in two, a single day's file kept growing unwieldy).
pub fn day_dir(report_dir: &Path, date: &str) -> PathBuf {
    report_dir.join(date)
}

/// `report_dir/{date}/summary_{date}.md` — day-level config + PnL rollup + hour index,
/// always fully rewritten from the trade log's ground truth on every write (not appended).
pub fn summary_path(report_dir: &Path, date: &str) -> PathBuf {
    day_dir(report_dir, date).join(format!("summary_{date}.md"))
}

/// `report_dir/{date}/trades_{date}_{HH}.md` — one real HKT hour's trade tables + each
/// report-writer run's market-state/staleness/CPU snapshot, newest run first.
pub fn trades_path(report_dir: &Path, date: &str, hour: &str) -> PathBuf {
    day_dir(report_dir, date).join(format!("trades_{date}_{hour}.md"))
}

/// Reads trade records from `trade_log_path` (JSONL), filtering by `logged_at` (unix
/// seconds) >= `since_unix`. Best-effort — a missing/unreadable file (no trades yet) is
/// treated as zero trades, not an error.
fn recent_trades(trade_log_path: &Path, since_unix: f64) -> Vec<SiglabTradeRecord> {
    let Ok(content) = std::fs::read_to_string(trade_log_path) else {
        return Vec::new();
    };
    content
        .lines()
        .filter_map(|l| serde_json::from_str::<SiglabTradeRecord>(l).ok())
        .filter(|r| r.logged_at >= since_unix)
        .collect()
}

/// Renders a unix timestamp as an HKT datetime string, to millisecond precision. Truncating
/// to whole seconds (the previous behavior) made genuinely-distinct trades a fraction of a
/// second apart look identical in the rendered report — part of what made the correlated-
/// variant-firing incident harder to read from the report alone; see
/// `doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`.
fn datetime_hkt(ts: f64) -> String {
    let secs = ts.trunc() as i64;
    let nanos = (ts.fract().max(0.0) * 1_000_000_000.0).round() as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| {
            dt.with_timezone(&hkt())
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|| "?".to_string())
}

#[derive(Default)]
struct MarketStrategyAgg {
    trades: u32,
    pnl: f64,
    sl: u32,
    timeout: u32,
    unwind: u32,
}

#[derive(Default)]
struct VariantPerf {
    trades: u32,
    wins: u32,
    pnl: f64,
}

impl VariantPerf {
    fn win_rate(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64
        }
    }

    fn pnl_per_trade(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.pnl / self.trades as f64
        }
    }
}

/// "Top performing strategies" — three top-5 leaderboards (by total pnl, by win rate, by
/// pnl per trade) over the same variant-level aggregation as `render_variant_totals_summary`
/// (summed across every market a variant trades). "Win" here means `pnl > 0`, not the `WIN`
/// outcome label — post-2026-07-14 `FORCE_UNWIND_BEFORE_CYCLE_END_SECS`, almost every exit is
/// STOPLOSS/TIMEOUT/UNWIND, so a WIN-outcome-only win rate would be close to undefined. Shown
/// right after `### Strategy config` in `render_summary_body`, computed from the same
/// `day_trades` window as `## Day summary` below it. Added 2026-07-16 per explicit request.
fn render_top_strategies_section(trades: &[SiglabTradeRecord]) -> String {
    let mut out = String::from("### Top performing strategies\n\n");

    let mut agg: HashMap<String, VariantPerf> = HashMap::new();
    for t in trades {
        let entry = agg.entry(t.variant_id.clone()).or_default();
        entry.trades += 1;
        entry.pnl += t.pnl;
        if t.pnl > 0.0 {
            entry.wins += 1;
        }
    }
    if agg.is_empty() {
        out.push_str("_No trades yet today._\n\n");
        return out;
    }
    let rows: Vec<(String, VariantPerf)> = agg.into_iter().collect();

    out.push_str(&render_leaderboard("Top 5 by total pnl", &rows, |p| p.pnl));
    out.push_str(&render_leaderboard("Top 5 by win rate", &rows, |p| {
        p.win_rate()
    }));
    out.push_str(&render_leaderboard("Top 5 by pnl per trade", &rows, |p| {
        p.pnl_per_trade()
    }));
    out
}

/// One leaderboard table within `render_top_strategies_section`, sorted descending by `key`
/// and truncated to the top 5 rows.
fn render_leaderboard(
    title: &str,
    rows: &[(String, VariantPerf)],
    key: impl Fn(&VariantPerf) -> f64,
) -> String {
    let mut sorted: Vec<&(String, VariantPerf)> = rows.iter().collect();
    sorted.sort_by(|a, b| {
        key(&b.1)
            .partial_cmp(&key(&a.1))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut out = format!("**{title}**\n\n");
    out.push_str(
        "| variant | trades | win rate | pnl/trade | total pnl |\n\
         |---|---|---|---|---|\n",
    );
    for (variant, p) in sorted.into_iter().take(5) {
        out.push_str(&format!(
            "| {variant} | {} | {:.1}% | {:.4} | {:.4} |\n",
            p.trades,
            p.win_rate() * 100.0,
            p.pnl_per_trade(),
            p.pnl,
        ));
    }
    out.push('\n');
    out
}

/// Aggregated PnL by (market, strategy), shown *above* the per-market trade tables — this
/// answers "how did each market/strategy combo do," the per-market tables below answer "what
/// happened." Note this aggregates by `strategy` (`"reversal"`/`"high_prob"`), not by the
/// finer-grained `variant_id` — with 18 reversal variants often firing together on the same
/// dip (they share the same underlying price move, just different thresholds), a
/// per-variant summary here would mostly repeat near-identical rows. See
/// `render_variant_summary` immediately below for the per-variant breakdown (added
/// 2026-07-14 per explicit request) — that's exactly what makes the repetition visible and
/// quantifiable rather than just asserted.
///
/// `sl`/`timeout`/`unwind` are the only exits reachable since the 2026-07-14
/// `FORCE_UNWIND_BEFORE_CYCLE_END_SECS` change (a still-open position can no longer ride to
/// a natural WIN/LOSS cycle-close) — a `WIN`/`LOSS` row would still count toward `trades`
/// and `total pnl` but not toward any of these 3 columns, which is deliberate: they should
/// be rare-to-nonexistent going forward, not a 4th column that's usually zero.
fn render_trade_summary(trades: &[SiglabTradeRecord]) -> String {
    let mut out = String::from("#### Summary: PnL by market and strategy\n\n");
    let mut agg: HashMap<(String, String), MarketStrategyAgg> = HashMap::new();
    for t in trades {
        let entry = agg
            .entry((t.market.clone(), t.strategy.clone()))
            .or_default();
        entry.trades += 1;
        entry.pnl += t.pnl;
        match t.outcome.as_str() {
            "STOPLOSS" => entry.sl += 1,
            "TIMEOUT" => entry.timeout += 1,
            "UNWIND" => entry.unwind += 1,
            _ => {}
        }
    }
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    out.push_str(
        "| market | strategy | trades | sl | timeout | unwind | total pnl |\n\
         |---|---|---|---|---|---|---|\n",
    );
    for ((market, strategy), a) in &rows {
        out.push_str(&format!(
            "| {market} | {strategy} | {} | {} | {} | {} | {:.4} |\n",
            a.trades, a.sl, a.timeout, a.unwind, a.pnl
        ));
    }
    let grand_total: f64 = trades.iter().map(|t| t.pnl).sum();
    out.push_str(&format!("\n**Total pnl: {grand_total:.4}**\n\n"));
    out
}

/// Same shape as `render_trade_summary`, but broken down by (market, `variant_id`) instead
/// of (market, `strategy`) — e.g. `reversal_0.2_0.7` rather than just `reversal`. Shown
/// directly below the market/strategy summary; added 2026-07-14 per explicit request.
fn render_variant_summary(trades: &[SiglabTradeRecord]) -> String {
    let mut out = String::from("#### Summary: PnL by market and variant\n\n");
    let mut agg: HashMap<(String, String), MarketStrategyAgg> = HashMap::new();
    for t in trades {
        let entry = agg
            .entry((t.market.clone(), t.variant_id.clone()))
            .or_default();
        entry.trades += 1;
        entry.pnl += t.pnl;
        match t.outcome.as_str() {
            "STOPLOSS" => entry.sl += 1,
            "TIMEOUT" => entry.timeout += 1,
            "UNWIND" => entry.unwind += 1,
            _ => {}
        }
    }
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    out.push_str(
        "| market | variant | trades | sl | timeout | unwind | total pnl |\n\
         |---|---|---|---|---|---|---|\n",
    );
    for ((market, variant), a) in &rows {
        out.push_str(&format!(
            "| {market} | {variant} | {} | {} | {} | {} | {:.4} |\n",
            a.trades, a.sl, a.timeout, a.unwind, a.pnl
        ));
    }
    let grand_total: f64 = trades.iter().map(|t| t.pnl).sum();
    out.push_str(&format!("\n**Total pnl: {grand_total:.4}**\n\n"));
    out
}

/// Same shape as `render_trade_summary`/`render_variant_summary`, but aggregated by
/// `variant_id` alone, summed across every market — answers "which variant performs best
/// overall" (e.g. `reversal_0.3_0.55`'s total pnl across BTC/ETH/XRP/weather/etc combined),
/// which the per-market `render_variant_summary` table can't show directly since it never
/// collapses a variant's rows across markets. Shown last of the three summary tables, as
/// the most zoomed-out view. Added 2026-07-15 per explicit request.
fn render_variant_totals_summary(trades: &[SiglabTradeRecord]) -> String {
    let mut out = String::from("#### Summary: PnL by variant (all markets)\n\n");
    let mut agg: HashMap<String, MarketStrategyAgg> = HashMap::new();
    for t in trades {
        let entry = agg.entry(t.variant_id.clone()).or_default();
        entry.trades += 1;
        entry.pnl += t.pnl;
        match t.outcome.as_str() {
            "STOPLOSS" => entry.sl += 1,
            "TIMEOUT" => entry.timeout += 1,
            "UNWIND" => entry.unwind += 1,
            _ => {}
        }
    }
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    out.push_str(
        "| variant | trades | sl | timeout | unwind | total pnl |\n\
         |---|---|---|---|---|---|\n",
    );
    for (variant, a) in &rows {
        out.push_str(&format!(
            "| {variant} | {} | {} | {} | {} | {:.4} |\n",
            a.trades, a.sl, a.timeout, a.unwind, a.pnl
        ));
    }
    let grand_total: f64 = trades.iter().map(|t| t.pnl).sum();
    out.push_str(&format!("\n**Total pnl: {grand_total:.4}**\n\n"));
    out
}

/// One market's trade table (no `market` column — it's already in the enclosing
/// `<details>`'s summary), sorted by entry time.
fn render_market_trade_table(rows: &[&SiglabTradeRecord]) -> String {
    let mut sorted: Vec<_> = rows.to_vec();
    sorted.sort_by(|a, b| {
        a.entry_ts
            .partial_cmp(&b.entry_ts)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut out = String::from(
        "| entry (HKT) | exit (HKT) | holding (s) | variant | side | outcome | pnl |\n\
         |---|---|---|---|---|---|---|\n",
    );
    for t in &sorted {
        // `logged_at` is stamped immediately after the trade record is produced (same
        // synchronous handler as the exit decision) — an accurate proxy for exit time
        // without needing a further trader/src change to carry a real exit_ts through.
        let holding_secs = t.logged_at - t.entry_ts;
        out.push_str(&format!(
            "| {} | {} | {holding_secs:.1} | {} | {} | {} | {:.4} |\n",
            datetime_hkt(t.entry_ts),
            datetime_hkt(t.logged_at),
            t.variant_id,
            t.side,
            t.outcome,
            t.pnl
        ));
    }
    out.push('\n');
    out
}

/// The whole hour-level "Trades" block: merged across every run so far this real HKT hour
/// (not split into one sub-section per 15-min run) — an outer collapsible section containing
/// the market/strategy PnL summary, then one collapsible table per market (sorted
/// alphabetically), each individually collapsible so an hour with many quiet weather/World
/// Cup buckets doesn't force-render dozens of tables at once. Regenerated fresh on every
/// write (see `HOUR_TRADES_MARKER_START/END` in `write_hourly_report`) rather than
/// accumulated per-run — the file is still written every `--report-interval-secs` (15 min in
/// production), it just no longer stacks one trades table per run within the hour.
fn render_hour_trades_section(trades: &[SiglabTradeRecord]) -> String {
    let mut out = format!(
        "<details>\n<summary>Trades this hour: {} trade(s)</summary>\n\n",
        trades.len()
    );
    if trades.is_empty() {
        out.push_str("_No trades fired._\n\n</details>\n\n");
        return out;
    }

    out.push_str(&render_trade_summary(trades));
    out.push_str(&render_variant_summary(trades));
    out.push_str(&render_variant_totals_summary(trades));

    let mut by_market: HashMap<&str, Vec<&SiglabTradeRecord>> = HashMap::new();
    for t in trades {
        by_market.entry(t.market.as_str()).or_default().push(t);
    }
    let mut markets: Vec<&str> = by_market.keys().copied().collect();
    markets.sort();

    for market in markets {
        let rows = &by_market[market];
        let count = rows.len();
        let pnl: f64 = rows.iter().map(|t| t.pnl).sum();
        let label = if market.is_empty() {
            "(unknown market)"
        } else {
            market
        };
        out.push_str(&format!(
            "<details>\n<summary>{label} — {count} trade(s), total pnl {pnl:.4}</summary>\n\n"
        ));
        out.push_str(&render_market_trade_table(rows));
        out.push_str("</details>\n\n");
    }

    out.push_str("</details>\n\n");
    out
}

/// Renders a collapsible section grouping `snaps` of the given `kind` by the prefix before
/// `": "` in their label (e.g. `"hong-kong: 33°C"` groups under `"hong-kong"`), showing one
/// row per group with its current highest-probability outcome — the most informative single
/// number per group without dumping every bucket of every group into the report every run.
/// Currently only called for `kind="weather"` — a second event class (formerly World Cup,
/// removed 2026-07-24) would reuse this unchanged.
fn render_grouped_snapshot_section(
    snaps: &[crate::snapshot::MarketSnapshot],
    kind: &str,
    title: &str,
    group_col_name: &str,
    now_ms: i64,
) -> String {
    let mut by_group: HashMap<String, Vec<&crate::snapshot::MarketSnapshot>> = HashMap::new();
    for s in snaps.iter().filter(|s| s.kind == kind) {
        if let Some((group, _)) = s.label.split_once(": ") {
            by_group.entry(group.to_string()).or_default().push(s);
        }
    }
    let mut groups: Vec<_> = by_group.keys().cloned().collect();
    groups.sort();

    let mut out = format!(
        "<details>\n<summary>{title} ({} reporting)</summary>\n\n",
        groups.len(),
    );
    out.push_str(&format!(
        "| {group_col_name} | top outcome | probability | age (s) |\n|---|---|---|---|\n"
    ));
    for group in &groups {
        let group_snaps = &by_group[group];
        if let Some(top) = group_snaps.iter().max_by(|a, b| {
            a.up_price
                .partial_cmp(&b.up_price)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            let age = ((now_ms - top.last_tick_ms).max(0) as f64) / 1000.0;
            let outcome_label = top
                .label
                .split_once(": ")
                .map(|(_, b)| b)
                .unwrap_or(&top.label);
            out.push_str(&format!(
                "| {group} | {outcome_label} | {:.3} | {age:.1} |\n",
                top.up_price
            ));
        }
    }
    out.push_str("\n</details>\n\n");
    out
}

/// One report-writer run's section (nested inside its hour's `<details>`): market state
/// snapshots, staleness health, CPU/memory for this run's own `window_secs` window — trades
/// are *not* rendered here (moved to the hour level, merged across the whole hour rather
/// than split per run — see `render_hour_trades_section`/`HOUR_TRADES_MARKER_START` in
/// `write_hourly_report`); this run's own new-trade count is still shown in the summary line
/// as a quick "what just happened" signal.
fn render_run_section(
    run_label: &str,
    snapshots: &SharedSnapshots,
    trade_log_path: &Path,
    stale_log: &SharedStaleLog,
    cgroup_prev: Option<cgroup::Sample>,
    cgroup_now: Option<cgroup::Sample>,
    window_secs: f64,
) -> String {
    let now_unix = Utc::now().timestamp() as f64;
    let since_unix = now_unix - window_secs;
    let now_ms = (now_unix * 1000.0) as i64;

    let new_trade_count = recent_trades(trade_log_path, since_unix).len();
    let snaps: Vec<_> = snapshots
        .lock()
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();

    let stale_events: Vec<StaleLogEntry> = {
        let mut log = stale_log.lock().unwrap_or_else(|p| p.into_inner());
        let since_ms = (since_unix * 1000.0) as i64;
        let recent: Vec<_> = log
            .iter()
            .filter(|e| e.at_ms >= since_ms)
            .cloned()
            .collect();
        // Trim anything older than the window so the log doesn't grow forever.
        log.retain(|e| e.at_ms >= since_ms);
        recent
    };

    let mut out = String::new();
    let summary = format!(
        "{run_label} HKT — {} crypto market(s), {} weather bucket(s), {new_trade_count} new trade(s), {} stale event(s)",
        snaps.iter().filter(|s| s.kind == "crypto").count(),
        snaps.iter().filter(|s| s.kind == "weather").count(),
        stale_events.len(),
    );
    out.push_str(&format!("<details>\n<summary>{summary}</summary>\n\n"));

    // ── Crypto market state ──
    let mut crypto: Vec<_> = snaps.iter().filter(|s| s.kind == "crypto").collect();
    crypto.sort_by(|a, b| a.label.cmp(&b.label));
    out.push_str("<details>\n<summary>Crypto market state snapshot</summary>\n\n");
    out.push_str("| market | up | down | age (s) |\n|---|---|---|---|\n");
    for s in &crypto {
        let age = ((now_ms - s.last_tick_ms).max(0) as f64) / 1000.0;
        out.push_str(&format!(
            "| {} | {:.4} | {:.4} | {:.1} |\n",
            s.label, s.up_price, s.dn_price, age
        ));
    }
    out.push_str("\n</details>\n\n");

    // ── Weather market state — one row per city, showing its current highest-probability
    //    bucket (the most informative single number per city without dumping every bucket
    //    of every city into the report every run). ──
    out.push_str(&render_grouped_snapshot_section(
        &snaps,
        "weather",
        "Weather market state snapshot",
        "city",
        now_ms,
    ));

    // ── Staleness health ──
    out.push_str(
        "<details>\n<summary>Staleness events (observe-only — no auto action taken)</summary>\n\n",
    );
    if stale_events.is_empty() {
        out.push_str("_No staleness escalations this run._\n\n");
    } else {
        out.push_str("| market | silent (ms) | bucket crossed (ms) |\n|---|---|---|\n");
        for e in stale_events.iter().take(200) {
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                e.market, e.silent_ms, e.bucket_ms
            ));
        }
        if stale_events.len() > 200 {
            out.push_str(&format!("\n_... and {} more._\n", stale_events.len() - 200));
        }
    }
    out.push_str("\n</details>\n\n");

    // ── CPU / memory ──
    out.push_str("<details>\n<summary>CPU / memory</summary>\n\n");
    match (cgroup_prev, cgroup_now) {
        (Some(prev), Some(now_s)) => {
            let cpu_pct = cgroup::cpu_percent(&prev, &now_s);
            let mem_mib = now_s.mem_bytes as f64 / (1024.0 * 1024.0);
            out.push_str(&format!(
                "- CPU (avg over this run's window, one-core=100%): **{cpu_pct:.2}%**\n- Memory (current): **{mem_mib:.1} MiB**\n\n"
            ));
        }
        _ => {
            out.push_str("_cgroup stats unavailable (not running under cgroup v2, e.g. outside Docker)._\n\n");
        }
    }
    out.push_str("</details>\n\n</details>\n\n");

    out
}

fn fmt_opt(x: Option<f64>) -> String {
    x.map(|v| format!("{v}")).unwrap_or_else(|| "-".to_string())
}

/// Config-table section, wrapped in `CONFIG_MARKER_START/END` so `write_hourly_report` can
/// replace it wholesale on every write rather than let it go stale — reflects whatever the
/// three siglab config files currently say. Added after the correlated-variant-firing
/// incident made clear how easy it is to lose track of which of the 18 reversal variants
/// maps to which (low, high) pair purely by reading trade rows; see
/// `doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`. Deliberately no
/// markets/durations table — `config/markets.toml`'s `[[market]]`/`[[hourly_market]]` list
/// changes rarely and is already documented in `siglab/README.md`; this section is about the
/// *strategy* parameters that actually explain trade behavior.
fn render_config_section(cfg: &SiglabConfig, weather_cities: &[String]) -> String {
    let mut out = format!("{CONFIG_MARKER_START}### Strategy config\n\n");

    out.push_str("<details>\n<summary>Crypto reversal variants</summary>\n\n");
    out.push_str(
        "| variant | strategy | assets | reversal_low_threshold | reversal | sl_pnl_rev | \
         unwind_pnl_rev | unwind_time_rev (s) | price_high_rev | delta_pct_rev | \
         max_buy_price | trade_size ($) |\n\
         |---|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for v in &cfg.variants {
        let assets = if v.assets.is_empty() {
            "all".to_string()
        } else {
            v.assets.join(", ")
        };
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {:.2} | {:.2} |\n",
            v.id,
            v.strategy,
            assets,
            fmt_opt(v.reversal_low_threshold),
            fmt_opt(v.reversal),
            fmt_opt(v.sl_pnl_rev),
            fmt_opt(v.unwind_pnl_rev),
            fmt_opt(v.unwind_time_rev),
            v.price_high_rev,
            v.delta_pct_rev,
            v.max_buy_price,
            v.trade_size_usdc,
        ));
    }
    out.push_str("\n</details>\n\n");

    out.push_str("<details>\n<summary>V-shape variants (crypto + weather)</summary>\n\n");
    out.push_str(
        "| variant | high1 | low | high2 | sl_pnl | unwind_pnl | unwind_time (s) | \
         trade_size ($) |\n\
         |---|---|---|---|---|---|---|---|\n",
    );
    for (id, p) in crate::v_shape::v_shape_grid() {
        out.push_str(&format!(
            "| {id} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.0} | {:.2} |\n",
            p.high1,
            p.low,
            p.high2,
            p.sl_pnl,
            p.unwind_pnl,
            crate::v_shape::UNWIND_TIME_SECS,
            crate::v_shape::TRADE_SIZE_USDC,
        ));
    }
    out.push_str("\n</details>\n\n");

    out.push_str(&format!(
        "<details>\n<summary>Weather cities ({})</summary>\n\n| city |\n|---|\n",
        weather_cities.len()
    ));
    for city in weather_cities {
        out.push_str(&format!("| {city} |\n"));
    }
    out.push_str("\n</details>\n\n");

    out.push_str(
        "Weather buckets trade the same `reversal_{low}_{high}` 18-combo grid via \
         `bucket_reversal.rs::reversal_grid()` (fixed `sl_pnl=0.3`/`unwind_pnl=0.15`/\
         `max_hold=25s`), not `config/markets.toml` — see that module's doc comment. Crypto \
         reversal variants additionally force-close (labeled `UNWIND`) within 10s of the \
         market's own cycle-end regardless of holding time — weather buckets have no \
         cycle-end concept, so that rule doesn't apply to them (see `bucket_reversal.rs`'s \
         doc comment). All markets — crypto *and*, since 2026-07-17, weather buckets too — \
         additionally run the 16-variant V-shape grid above via `v_shape.rs::VShapeEngine` — \
         a self-contained engine like `bucket_reversal.rs` (no `gates.rs`, no delta_pct/\
         Binance-direction requirement). On crypto it tracks real cycle boundaries and \
         reuses the same force-unwind-within-10s-of-cycle-end rule the reversal grid gets \
         from `trader::machine::Machine`; on weather buckets it is simply never given a \
         cycle, which permanently disables that branch, leaving stop-loss/take-profit/\
         timeout as its only exits — the same exit model `bucket_reversal.rs` uses (see \
         `v_shape.rs`'s doc comment and `doc/feature_v_2026-07-17.md`). World Cup support \
         (and its own reversal/V-shape trades on those buckets) was removed 2026-07-24 — see \
         `doc/plan_better_signal_2026-07-24.md`.\n\n",
    );

    out.push_str(CONFIG_MARKER_END);
    out
}

/// Renders one hour's whole `<details>` block (marker comment + a `###` section header —
/// same style as `### Strategy config` — + summary + `inner`, which is the concatenation of
/// that hour's `RUN_MARKER`-prefixed run blocks, newest first). The `###` heading lives
/// outside/before the `<details>` so it's a real jump-to-able Markdown heading, not just
/// collapsible-summary text.
/// `trades_{date}_{hour}.md`'s fixed leading text — shared by `write_hourly_report` (live)
/// and `regenerate_from_trade_log` (one-off backfill) so both produce byte-identical
/// headers; `write_hourly_report`'s `existing.strip_prefix(&header)` continuation check
/// depends on that.
fn trades_header(date: &str, hour: &str) -> String {
    format!(
        "# siglab signal report — {date} {hour}:00 HKT\n\n\
         Auto-generated by siglab. Covers exactly this one real HKT hour: a merged trade-\
         tables section (regenerated fresh from the trade log on every write, not split per\n\
         report-writer run) followed by each run's own market-state/staleness/CPU snapshot,\n\
         newest run first. See `summary_{date}.md` in this same folder for the day's\n\
         strategy config, whole-day PnL rollup, and an index of every hour.\n\n"
    )
}

/// `summary_{date}.md`'s fixed leading text.
fn summary_header(date: &str) -> String {
    format!(
        "# siglab signal report — {date}\n\n\
         Auto-generated by siglab, always fully rewritten from the trade log's ground truth\n\
         on every write (not appended). Strategy config, whole-day PnL rollup, and an index\n\
         of every hour's own `trades_{date}_{{HH}}.md` file (trade tables + market-state/\n\
         staleness/CPU snapshots) live below. See\n\
         `siglab/doc/local_resource_test_2026-07-13.md` for the Docker resource baseline and\n\
         `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md` for what this harness does\n\
         and does not claim. Weather markets trade via self-contained `bucket_reversal.rs`\n\
         reversal (18 variants/bucket) and `v_shape.rs` V-shape (16 variants/bucket) engines,\n\
         no delta/Gamma/resolve (see those files' doc comments and\n\
         `doc/feature_v_2026-07-17.md`) — separate from crypto's `trader::machine::Machine`\n\
         (reversal grid). World Cup support was removed 2026-07-24 (tournament over) — see\n\
         `doc/plan_better_signal_2026-07-24.md`.\n\n"
    )
}

/// Strips a previously-written `HOUR_TRADES_MARKER_START/END`-wrapped block out of `body` —
/// it's always regenerated fresh on every write rather than accumulated, so any old copy
/// must be removed before splicing in the new one. Returns `body` unchanged if no markers
/// are found (e.g. a brand new file).
fn strip_old_hour_trades(body: &str) -> String {
    match (
        body.find(HOUR_TRADES_MARKER_START),
        body.find(HOUR_TRADES_MARKER_END),
    ) {
        (Some(start), Some(end)) if end >= start => {
            let mut s = body[..start].to_string();
            s.push_str(&body[end + HOUR_TRADES_MARKER_END.len()..]);
            s
        }
        _ => body.to_string(),
    }
}

/// Builds `summary_{date}.md`'s full contents (config + whole-day PnL rollup + hour index)
/// from `day_trades` (every trade logged on `date` so far) — shared by the live writer and
/// the one-off backfill so both produce identical output. `dir` is this date's folder,
/// scanned for which `trades_{date}_{HH}.md` files actually exist (see `render_hour_index`).
fn render_summary_body(
    date: &str,
    dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    day_trades: &[SiglabTradeRecord],
) -> String {
    let mut s = summary_header(date);
    s.push_str(&render_config_section(cfg, weather_cities));
    s.push_str(&render_top_strategies_section(day_trades));
    s.push_str("## Day summary\n\n");
    s.push_str(&collapsible_day_summary(&render_trade_summary(day_trades)));
    s.push_str(&collapsible_day_summary(&render_variant_summary(
        day_trades,
    )));
    s.push_str(&collapsible_day_summary(&render_variant_totals_summary(
        day_trades,
    )));
    s.push_str(&render_hour_index(dir, date, day_trades));
    s
}

/// Wraps a `#### Title\n\n<body>` block (as produced by `render_trade_summary` and its two
/// siblings) in a collapsible `<details>`, keeping the original title text as the visible
/// `<summary>`. Used only for the day-level `## Day summary` section — a full day's variant
/// breakdown can run to well over a thousand rows, and with three of these stacked back to
/// back the file became unwieldy to scroll through (2026-07-16). The same three functions
/// are also used per-hour in `render_hour_trades_section`, which stays small enough to leave
/// expanded as-is.
fn collapsible_day_summary(section: &str) -> String {
    let (title_line, rest) = section.split_once('\n').unwrap_or((section, ""));
    let title = title_line.trim_start_matches('#').trim();
    let body = rest.trim_start_matches('\n');
    format!("<details>\n<summary>{title}</summary>\n\n{body}</details>\n\n")
}

/// An "Hours" index table: one row per `trades_{date}_{HH}.md` file that actually exists on
/// disk in `dir` (not just per hour that has trades — a quiet hour still gets a file, with
/// its run/snapshot data), newest hour first, each linking to that file.
fn render_hour_index(dir: &Path, date: &str, day_trades: &[SiglabTradeRecord]) -> String {
    let mut by_hour: HashMap<String, (u32, f64)> = HashMap::new();
    for t in day_trades {
        let Some(dt) = Utc
            .timestamp_opt(t.logged_at.trunc() as i64, 0)
            .single()
            .map(|d| d.with_timezone(&hkt()))
        else {
            continue;
        };
        let entry = by_hour.entry(dt.format("%H").to_string()).or_default();
        entry.0 += 1;
        entry.1 += t.pnl;
    }

    let prefix = format!("trades_{date}_");
    let mut hours: Vec<String> = std::fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter_map(|name| {
            name.strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix(".md"))
                .map(str::to_string)
        })
        .collect();
    hours.sort();
    hours.reverse();

    let mut out = String::from("## Hours\n\n| hour | trades | pnl | file |\n|---|---|---|---|\n");
    for hour in &hours {
        let (count, pnl) = by_hour.get(hour).copied().unwrap_or((0, 0.0));
        let pnl = pnl + 0.0; // normalize -0.0, same reasoning as elsewhere in this file
        out.push_str(&format!(
            "| {hour}:00 | {count} | {pnl:.4} | [trades_{date}_{hour}.md](trades_{date}_{hour}.md) |\n"
        ));
    }
    out.push('\n');
    out
}

/// One-off: rebuild every `report_dir/{date}/summary_{date}.md` +
/// `report_dir/{date}/trades_{date}_{HH}.md` entirely from the trade log's ground truth,
/// grouped by real HKT date/hour, using the exact same rendering functions as the live
/// writer. Historical market-state/staleness/CPU snapshots aren't recoverable from the trade
/// log alone (they were never persisted anywhere else), so each regenerated hour file
/// carries a note instead of fabricating them; the live process's next natural write still
/// extends whichever hour is current normally.
///
/// `since_date`, if set, skips any date before it — lets a backfill run be scoped to recent
/// days instead of reprocessing the trade log's entire history.
pub fn regenerate_from_trade_log(
    trade_log_path: &Path,
    report_dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    since_date: Option<NaiveDate>,
) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(report_dir).context("create report dir")?;
    let all_trades = recent_trades(trade_log_path, 0.0);

    let mut by_date_hour: std::collections::BTreeMap<(String, String), Vec<SiglabTradeRecord>> =
        std::collections::BTreeMap::new();
    for t in all_trades {
        let Some(dt) = Utc
            .timestamp_opt(t.logged_at.trunc() as i64, 0)
            .single()
            .map(|d| d.with_timezone(&hkt()))
        else {
            continue;
        };
        let date_naive = dt.date_naive();
        if since_date.is_some_and(|since| date_naive < since) {
            continue;
        }
        let date = dt.format("%Y-%m-%d").to_string();
        let hour = dt.format("%H").to_string();
        by_date_hour.entry((date, hour)).or_default().push(t);
    }

    let mut hours_by_date: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (date, hour) in by_date_hour.keys() {
        hours_by_date
            .entry(date.clone())
            .or_default()
            .push(hour.clone());
    }

    let regenerated_note = "_Regenerated from the trade log — historical market-state/\
        staleness/CPU snapshots aren't recoverable from trade data alone and are omitted for \
        this hour; hours written by the live process carry them as usual._\n\n";

    let mut written = Vec::new();
    for (date, hours) in hours_by_date.iter().rev() {
        let dir = day_dir(report_dir, date);
        std::fs::create_dir_all(&dir).with_context(|| format!("create {dir:?}"))?;

        let mut hours_sorted = hours.clone();
        hours_sorted.sort();
        hours_sorted.reverse(); // newest hour first

        let mut day_trades: Vec<SiglabTradeRecord> = Vec::new();
        for hour in &hours_sorted {
            let trades = &by_date_hour[&(date.clone(), hour.clone())];
            day_trades.extend(trades.iter().cloned());

            let path = trades_path(report_dir, date, hour);
            let body = format!(
                "{HOUR_TRADES_MARKER_START}{}{HOUR_TRADES_MARKER_END}{regenerated_note}",
                render_hour_trades_section(trades)
            );
            std::fs::write(&path, format!("{}{body}", trades_header(date, hour)))
                .with_context(|| format!("write {path:?}"))?;
            written.push(path);
        }

        let s_path = summary_path(report_dir, date);
        let s_body = render_summary_body(date, &dir, cfg, weather_cities, &day_trades);
        std::fs::write(&s_path, s_body).with_context(|| format!("write {s_path:?}"))?;
        written.push(s_path);
    }
    Ok(written)
}

/// One-off: rebuild every `report_dir/{date}/summary_{date}.md` from the trade log's ground
/// truth, WITHOUT touching any `trades_{date}_{HH}.md` file. Unlike
/// `regenerate_from_trade_log`, which rewrites both and in doing so discards each hour's real
/// market-state/staleness/CPU snapshots (not recoverable from the trade log alone), this only
/// ever reads `trades_{date}_{HH}.md` filenames (via `render_hour_index`) to build the hour
/// index table — it never writes them. Use this to backfill a summary-only rendering change
/// (e.g. a new day-level section) into already-written days without disturbing their
/// hour-level history. `since_date`, if set, skips any date before it. Added 2026-07-16 to
/// backfill the collapsible day-summary sections + "Top performing strategies" table.
pub fn regenerate_summaries_from_trade_log(
    trade_log_path: &Path,
    report_dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    since_date: Option<NaiveDate>,
) -> Result<Vec<PathBuf>> {
    let all_trades = recent_trades(trade_log_path, 0.0);

    let mut day_trades: std::collections::BTreeMap<String, Vec<SiglabTradeRecord>> =
        std::collections::BTreeMap::new();
    for t in all_trades {
        let Some(dt) = Utc
            .timestamp_opt(t.logged_at.trunc() as i64, 0)
            .single()
            .map(|d| d.with_timezone(&hkt()))
        else {
            continue;
        };
        let date_naive = dt.date_naive();
        if since_date.is_some_and(|since| date_naive < since) {
            continue;
        }
        let date = dt.format("%Y-%m-%d").to_string();
        day_trades.entry(date).or_default().push(t);
    }

    let mut written = Vec::new();
    for (date, trades) in day_trades.iter().rev() {
        let dir = day_dir(report_dir, date);
        let s_path = summary_path(report_dir, date);
        let s_body = render_summary_body(date, &dir, cfg, weather_cities, trades);
        std::fs::write(&s_path, s_body).with_context(|| format!("write {s_path:?}"))?;
        written.push(s_path);
    }
    Ok(written)
}

/// `[start, end)` unix-second bounds for the real HKT hour named by `date`/`hour` (as parsed
/// out of a `trades_{date}_{hour}.md` filename).
fn hour_bounds(date_str: &str, hour_str: &str) -> Result<(f64, f64)> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .with_context(|| format!("bad date {date_str:?}"))?;
    let hour: u32 = hour_str
        .parse()
        .with_context(|| format!("bad hour {hour_str:?}"))?;
    let naive = date
        .and_hms_opt(hour, 0, 0)
        .with_context(|| format!("bad hour value {hour_str:?}"))?;
    let start = hkt()
        .from_local_datetime(&naive)
        .single()
        .with_context(|| format!("ambiguous/invalid local time {date_str} {hour_str}"))?;
    let start_ts = start.timestamp() as f64;
    Ok((start_ts, start_ts + 3600.0))
}

/// One-off maintenance: re-renders just the merged "Trades this hour" table (the span
/// between `HOUR_TRADES_MARKER_START/END`) of every `trades_{date}_{HH}.md` under every
/// date subfolder of `report_dir`, from `trade_log_path`'s ground truth — used to backfill a
/// rendering change (e.g. a new summary table/column) into hours the live process already
/// wrote and closed. Unlike `regenerate_from_trade_log`, this does **not** touch anything
/// else in the file — each hour's real market-state/staleness/CPU snapshots (not recoverable
/// from the trade log alone) are left exactly as the live process wrote them.
pub fn refresh_hour_trades_tables(
    trade_log_path: &Path,
    report_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let all_trades = recent_trades(trade_log_path, 0.0);
    let mut written = Vec::new();

    let Ok(day_entries) = std::fs::read_dir(report_dir) else {
        return Ok(written);
    };
    for day_entry in day_entries {
        let day_path = day_entry.context("read report dir entry")?.path();
        if !day_path.is_dir() {
            continue;
        }
        let Some(date) = day_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        let prefix = format!("trades_{date}_");

        for entry in std::fs::read_dir(&day_path).with_context(|| format!("read {day_path:?}"))? {
            let path = entry.context("read day dir entry")?.path();
            let Some(hour) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix(&prefix))
                .and_then(|n| n.strip_suffix(".md"))
                .map(str::to_string)
            else {
                continue;
            };

            let (start_ts, end_ts) = hour_bounds(&date, &hour)
                .with_context(|| format!("hour {date} {hour} in {path:?}"))?;
            let hour_trades: Vec<SiglabTradeRecord> = all_trades
                .iter()
                .filter(|t| t.logged_at >= start_ts && t.logged_at < end_ts)
                .cloned()
                .collect();

            let content =
                std::fs::read_to_string(&path).with_context(|| format!("read {path:?}"))?;
            if let (Some(ts), Some(te)) = (
                content.find(HOUR_TRADES_MARKER_START),
                content.find(HOUR_TRADES_MARKER_END),
            ) && te >= ts
            {
                let mut new_content = content[..ts].to_string();
                new_content.push_str(HOUR_TRADES_MARKER_START);
                new_content.push_str(&render_hour_trades_section(&hour_trades));
                new_content.push_str(HOUR_TRADES_MARKER_END);
                new_content.push_str(&content[te + HOUR_TRADES_MARKER_END.len()..]);
                std::fs::write(&path, &new_content).with_context(|| format!("write {path:?}"))?;
                written.push(path);
            }
        }
    }
    Ok(written)
}

/// Writes this run's report: `trades_{date}_{HH}.md` (this hour's merged trade tables,
/// regenerated fresh, plus this run's market-state/staleness/CPU snapshot nested on top of
/// any earlier runs this same hour) and `summary_{date}.md` (fully rewritten every time from
/// the trade log's ground truth). Returns `(summary_path, trades_path)`.
#[allow(clippy::too_many_arguments)]
pub fn write_hourly_report(
    report_dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    snapshots: &SharedSnapshots,
    trade_log_path: &Path,
    stale_log: &SharedStaleLog,
    cgroup_prev: Option<cgroup::Sample>,
    cgroup_now: Option<cgroup::Sample>,
    window_secs: f64,
) -> Result<(PathBuf, PathBuf)> {
    let now = now_hkt();
    let date = now.format("%Y-%m-%d").to_string();
    let hour = now.format("%H").to_string();
    let dir = day_dir(report_dir, &date);
    std::fs::create_dir_all(&dir).context("create day dir")?;

    // ── trades_{date}_{HH}.md ──
    let t_path = trades_path(report_dir, &date, &hour);
    let header = trades_header(&date, &hour);
    let existing = std::fs::read_to_string(&t_path).unwrap_or_default();
    let body = existing
        .strip_prefix(&header)
        .map(str::to_string)
        .unwrap_or_default();
    // Any existing content here is by construction always this same hour (the filename
    // encodes date+hour), so there's no "is this a continuation" check to make — just strip
    // the old merged-trades block (regenerated fresh below) and keep the accumulated
    // RUN_MARKER-prefixed run blocks.
    let old_runs = strip_old_hour_trades(&body);

    let run_label = now.format("%Y-%m-%d %H:%M").to_string();
    let run_block = render_run_section(
        &run_label,
        snapshots,
        trade_log_path,
        stale_log,
        cgroup_prev,
        cgroup_now,
        window_secs,
    );

    let start_of_hour = now
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let hour_trades = recent_trades(trade_log_path, start_of_hour.timestamp() as f64);
    let hour_trades_html = format!(
        "{HOUR_TRADES_MARKER_START}{}{HOUR_TRADES_MARKER_END}",
        render_hour_trades_section(&hour_trades)
    );

    let new_body = format!("{hour_trades_html}{RUN_MARKER}{run_block}{old_runs}");
    std::fs::write(&t_path, format!("{header}{new_body}"))
        .with_context(|| format!("write {t_path:?}"))?;

    // ── summary_{date}.md ──
    let s_path = summary_path(report_dir, &date);
    let start_of_day = now
        .with_hour(0)
        .unwrap()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let day_trades = recent_trades(trade_log_path, start_of_day.timestamp() as f64);
    let s_body = render_summary_body(&date, &dir, cfg, weather_cities, &day_trades);
    std::fs::write(&s_path, s_body).with_context(|| format!("write {s_path:?}"))?;

    Ok((s_path, t_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::MarketKind;

    #[test]
    fn recent_trades_filters_by_time_and_ignores_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        assert!(recent_trades(&path, 0.0).is_empty());

        std::fs::write(
            &path,
            "{\"logged_at\":100.0,\"market_kind\":\"crypto\",\"variant_id\":\"v\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":0.0,\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"WIN\",\"pnl\":0.1}\n\
             {\"logged_at\":200.0,\"market_kind\":\"crypto\",\"variant_id\":\"v\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":0.0,\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"WIN\",\"pnl\":0.2}\n",
        )
        .unwrap();

        let all = recent_trades(&path, 0.0);
        assert_eq!(all.len(), 2);
        let recent = recent_trades(&path, 150.0);
        assert_eq!(recent.len(), 1);
        assert!((recent[0].pnl - 0.2).abs() < 1e-9);
    }

    #[test]
    fn recent_trades_reads_pre_market_field_lines_via_serde_default() {
        // Real lines from the production trade log, logged before the `market` field
        // existed — must still deserialize (as market="") rather than being silently
        // dropped, or every trade logged before this change disappears from every future
        // report.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        std::fs::write(
            &path,
            "{\"logged_at\":1783915185.1,\"market_kind\":\"crypto\",\"variant_id\":\"high_prob_btc\",\"asset\":\"BTC\",\"slug\":\"btc-updown-15m-1783914300\",\"cycle_start\":1783914300.0,\"strategy\":\"high_prob\",\"side\":\"DOWN\",\"entry_ts\":1783915180.0,\"token_price\":0.885,\"exit_price\":0.955,\"outcome\":\"UNWIND\",\"pnl\":0.0791}\n",
        )
        .unwrap();

        let trades = recent_trades(&path, 0.0);
        assert_eq!(
            trades.len(),
            1,
            "old-schema line must not be silently dropped"
        );
        assert_eq!(trades[0].entry_price_ts, 0.0);
        assert_eq!(trades[0].market, "");
        assert!((trades[0].pnl - 0.0791).abs() < 1e-9);
    }

    #[test]
    fn stale_log_caps_growth() {
        let log = new_stale_log();
        for i in 0..5100 {
            log_stale_event(&log, i, "m".to_string(), 1000, 1000);
        }
        assert_eq!(log.lock().unwrap().len(), 5000);
    }

    fn mk_trade(
        market: &str,
        variant_id: &str,
        strategy: &str,
        entry_ts: f64,
        pnl: f64,
    ) -> SiglabTradeRecord {
        SiglabTradeRecord {
            logged_at: entry_ts + 10.0,
            market_kind: MarketKind::Crypto,
            variant_id: variant_id.to_string(),
            asset: market.split('-').next().unwrap_or(market).to_string(),
            market: market.to_string(),
            slug: "s".to_string(),
            cycle_start: 0.0,
            strategy: strategy.to_string(),
            side: "UP".to_string(),
            entry_ts,
            entry_price_ts: entry_ts,
            token_price: 0.5,
            exit_price: 0.6,
            outcome: "WIN".to_string(),
            pnl,
        }
    }

    #[test]
    fn trades_section_splits_one_table_per_market() {
        let trades = vec![
            mk_trade("XRP-15m", "reversal_0.2_0.55", "reversal", 200.0, 0.1),
            mk_trade("XRP-5m", "reversal_0.2_0.55", "reversal", 100.0, 0.2),
            mk_trade("XRP-15m", "reversal_0.3_0.6", "reversal", 50.0, 0.3),
        ];
        let section = render_hour_trades_section(&trades);
        // Two distinct per-market collapsible tables, one per market.
        assert!(section.contains("<summary>XRP-15m — 2 trade(s), total pnl 0.4000</summary>"));
        assert!(section.contains("<summary>XRP-5m — 1 trade(s), total pnl 0.2000</summary>"));
        // Within XRP-15m's own table, sorted by entry time (50 before 200). rfind, not
        // find: both variant_ids also appear earlier in the market/variant summary table
        // above the per-market tables (see render_variant_summary) — the per-market row
        // order is what's under test here, so target the *last* occurrence of each.
        let pos_early = section.rfind("reversal_0.3_0.6").unwrap();
        let pos_late = section.rfind("reversal_0.2_0.55").unwrap();
        assert!(pos_early < pos_late);
    }

    #[test]
    fn trades_section_has_datetime_column_and_market_heading() {
        let trades = vec![mk_trade(
            "BTC-4h",
            "reversal_0.4_0.8",
            "reversal",
            1_700_000_000.0,
            0.5,
        )];
        let section = render_hour_trades_section(&trades);
        assert!(section.contains("entry (HKT)"));
        assert!(section.contains("exit (HKT)"));
        assert!(section.contains("holding (s)"));
        assert!(section.contains("BTC-4h — 1 trade(s)"));
        // entry_ts formatted as a real date, not a raw epoch number.
        assert!(section.contains("2023-"));
    }

    #[test]
    fn trade_summary_aggregates_by_market_and_strategy() {
        let trades = vec![
            mk_trade("XRP-5m", "reversal_0.2_0.55", "reversal", 100.0, 0.1),
            mk_trade("XRP-5m", "reversal_0.3_0.6", "reversal", 110.0, 0.2),
            mk_trade("XRP-5m", "high_prob_xrp", "high_prob", 120.0, -0.05),
        ];
        let summary = render_trade_summary(&trades);
        // Both reversal trades on XRP-5m should collapse into one aggregated row (2 trades,
        // total 0.3), not two separate rows — this is the point of aggregating by
        // (market, strategy) rather than by the finer-grained variant_id. mk_trade's
        // outcome is fixed to "WIN", so the sl/timeout/unwind columns are all 0 here —
        // covered separately by sl_timeout_unwind_counts_are_broken_out_by_outcome below.
        assert!(summary.contains("| XRP-5m | reversal | 2 | 0 | 0 | 0 | 0.3000 |"));
        assert!(summary.contains("| XRP-5m | high_prob | 1 | 0 | 0 | 0 | -0.0500 |"));
        assert!(summary.contains("Total pnl: 0.2500"));
    }

    #[test]
    fn variant_summary_breaks_out_each_reversal_variant_separately() {
        let trades = vec![
            mk_trade("XRP-5m", "reversal_0.2_0.55", "reversal", 100.0, 0.1),
            mk_trade("XRP-5m", "reversal_0.3_0.6", "reversal", 110.0, 0.2),
            mk_trade("XRP-5m", "high_prob_xrp", "high_prob", 120.0, -0.05),
        ];
        let summary = render_variant_summary(&trades);
        // Unlike render_trade_summary, the two reversal trades must NOT collapse -- each
        // variant_id gets its own row.
        assert!(summary.contains("| XRP-5m | reversal_0.2_0.55 | 1 | 0 | 0 | 0 | 0.1000 |"));
        assert!(summary.contains("| XRP-5m | reversal_0.3_0.6 | 1 | 0 | 0 | 0 | 0.2000 |"));
        assert!(summary.contains("| XRP-5m | high_prob_xrp | 1 | 0 | 0 | 0 | -0.0500 |"));
        assert!(summary.contains("Total pnl: 0.2500"));
    }

    #[test]
    fn hour_trades_section_includes_all_three_summary_tables() {
        let trades = vec![mk_trade(
            "XRP-5m",
            "reversal_0.2_0.55",
            "reversal",
            100.0,
            0.1,
        )];
        let section = render_hour_trades_section(&trades);
        assert!(section.contains("PnL by market and strategy"));
        assert!(section.contains("PnL by market and variant"));
        assert!(section.contains("PnL by variant (all markets)"));
        assert!(section.contains("reversal_0.2_0.55"));
    }

    #[test]
    fn variant_totals_summary_sums_one_variant_across_markets() {
        let trades = vec![
            mk_trade("XRP-5m", "reversal_0.2_0.55", "reversal", 100.0, 0.1),
            mk_trade("XRP-15m", "reversal_0.2_0.55", "reversal", 110.0, 0.2),
            mk_trade("BTC-5m", "reversal_0.2_0.55", "reversal", 120.0, -0.05),
            mk_trade("XRP-5m", "reversal_0.3_0.6", "reversal", 130.0, 1.0),
        ];
        let summary = render_variant_totals_summary(&trades);
        // Unlike render_variant_summary (per-market), the three reversal_0.2_0.55 trades
        // across three different markets must collapse into one row.
        assert!(summary.contains("| reversal_0.2_0.55 | 3 | 0 | 0 | 0 | 0.2500 |"));
        assert!(summary.contains("| reversal_0.3_0.6 | 1 | 0 | 0 | 0 | 1.0000 |"));
        assert!(summary.contains("Total pnl: 1.2500"));
    }

    #[test]
    fn top_strategies_ranks_by_pnl_win_rate_and_pnl_per_trade_independently() {
        // "a": 4 trades (3 wins), total 14.0, ppt 3.5, win rate 75% — highest total pnl.
        // "b": 5 trades, all small wins, total 1.0, ppt 0.2, win rate 100% — highest win rate.
        // "c": 2 trades (1 win), total 8.0, ppt 4.0, win rate 50% — highest pnl per trade.
        // Each leaderboard should surface a different variant as its #1 row.
        let trades = vec![
            mk_trade("XRP-5m", "a", "reversal", 100.0, 5.0),
            mk_trade("XRP-5m", "a", "reversal", 110.0, 5.0),
            mk_trade("XRP-5m", "a", "reversal", 120.0, 5.0),
            mk_trade("XRP-5m", "a", "reversal", 130.0, -1.0),
            mk_trade("XRP-5m", "b", "reversal", 100.0, 0.2),
            mk_trade("XRP-5m", "b", "reversal", 110.0, 0.2),
            mk_trade("XRP-5m", "b", "reversal", 120.0, 0.2),
            mk_trade("XRP-5m", "b", "reversal", 130.0, 0.2),
            mk_trade("XRP-5m", "b", "reversal", 140.0, 0.2),
            mk_trade("XRP-5m", "c", "reversal", 100.0, 9.0),
            mk_trade("XRP-5m", "c", "reversal", 110.0, -1.0),
        ];
        let section = render_top_strategies_section(&trades);

        assert!(section.starts_with("### Top performing strategies\n\n"));

        let pnl_board = section.split("**Top 5 by win rate**").next().unwrap();
        assert!(pnl_board.contains("**Top 5 by total pnl**"));
        let a_pnl_line = pnl_board.lines().find(|l| l.starts_with("| a ")).unwrap();
        assert!(a_pnl_line.contains("14.0000"));

        let win_rate_board = section
            .split("**Top 5 by win rate**")
            .nth(1)
            .unwrap()
            .split("**Top 5 by pnl per trade**")
            .next()
            .unwrap();
        let b_line = win_rate_board
            .lines()
            .find(|l| l.starts_with("| b "))
            .unwrap();
        assert!(b_line.contains("100.0%"));

        let ppt_board = section.split("**Top 5 by pnl per trade**").nth(1).unwrap();
        let c_ppt_line = ppt_board.lines().find(|l| l.starts_with("| c ")).unwrap();
        assert!(c_ppt_line.contains("4.0000"));
    }

    #[test]
    fn top_strategies_handles_no_trades() {
        let section = render_top_strategies_section(&[]);
        assert!(section.contains("No trades yet today"));
        assert!(!section.contains("Top 5 by total pnl"));
    }

    #[test]
    fn collapsible_day_summary_wraps_title_in_details_and_keeps_body() {
        let section = "#### Summary: PnL by market and strategy\n\n\
             | market | strategy |\n|---|---|\n| XRP-5m | reversal |\n\n\
             **Total pnl: 1.0000**\n\n";
        let wrapped = collapsible_day_summary(section);
        assert!(
            wrapped.starts_with(
                "<details>\n<summary>Summary: PnL by market and strategy</summary>\n\n"
            )
        );
        assert!(wrapped.contains("| XRP-5m | reversal |"));
        assert!(wrapped.contains("**Total pnl: 1.0000**"));
        assert!(wrapped.trim_end().ends_with("</details>"));
    }

    #[test]
    fn day_summary_body_wraps_all_three_tables_and_shows_top_strategies() {
        let trades = vec![mk_trade(
            "XRP-5m",
            "reversal_0.2_0.55",
            "reversal",
            100.0,
            0.1,
        )];
        let dir = tempfile::tempdir().unwrap();
        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };
        let body = render_summary_body("2026-07-16", dir.path(), &cfg, &[], &trades);

        assert!(body.contains("### Top performing strategies"));
        assert!(body.contains("**Top 5 by total pnl**"));
        // Every one of the three day-summary tables must be inside its own collapsible
        // <details>, not rendered flat under "## Day summary".
        assert!(body.contains("<details>\n<summary>Summary: PnL by market and strategy</summary>"));
        assert!(body.contains("<details>\n<summary>Summary: PnL by market and variant</summary>"));
        assert!(
            body.contains("<details>\n<summary>Summary: PnL by variant (all markets)</summary>")
        );
    }

    #[test]
    fn sl_timeout_unwind_counts_are_broken_out_by_outcome() {
        let mut sl = mk_trade("XRP-5m", "v", "reversal", 100.0, -0.1);
        sl.outcome = "STOPLOSS".to_string();
        let mut timeout = mk_trade("XRP-5m", "v", "reversal", 110.0, 0.01);
        timeout.outcome = "TIMEOUT".to_string();
        let mut unwind = mk_trade("XRP-5m", "v", "reversal", 120.0, 0.15);
        unwind.outcome = "UNWIND".to_string();
        let mut win = mk_trade("XRP-5m", "v", "reversal", 130.0, 1.0);
        win.outcome = "WIN".to_string();

        let summary = render_trade_summary(&[sl, timeout, unwind, win]);
        // 4 trades total (WIN counts toward `trades` but not sl/timeout/unwind — see that
        // column set's doc comment).
        assert!(summary.contains("| XRP-5m | reversal | 4 | 1 | 1 | 1 | 1.0600 |"));
    }

    #[test]
    fn empty_trades_render_gracefully() {
        assert!(render_hour_trades_section(&[]).contains("No trades fired"));
    }

    fn sample_cfg() -> SiglabConfig {
        SiglabConfig {
            markets: vec![crate::config::MarketCfg {
                asset: "BTC".to_string(),
                suffix: "5m".to_string(),
                period_secs: 300,
            }],
            hourly_markets: vec![],
            variants: vec![crate::config::VariantCfg {
                id: "reversal_0.2_0.55".to_string(),
                strategy: "reversal".to_string(),
                assets: vec![],
                no_enter_when_time_left: 0.0,
                max_buy_price: 0.95,
                spread_premium_limit: 1.05,
                spread_discount_limit: 0.95,
                max_price_age_secs: 2.0,
                delta_pct_rev: 0.0008,
                delta_pct_hp: 0.0004,
                price_high_rev: 0.90,
                trade_size_usdc: 1.0,
                reversal: Some(0.55),
                reversal_low_threshold: Some(0.2),
                reversal_start_time: Some(999999.0),
                sl_reversal: Some(0.0),
                unwind_pnl_rev: Some(0.15),
                sl_pnl_rev: Some(0.3),
                unwind_time_rev: Some(30.0),
                price_low: None,
                price_high: None,
                enter_when_time_left: None,
                sl_high_prob: None,
                unwind_pnl_hp: None,
                sl_pnl_hp: None,
                unwind_time_hp: None,
            }],
        }
    }

    #[test]
    fn config_section_lists_variant_grid_and_weather() {
        let weather = vec!["hong-kong".to_string()];
        let section = render_config_section(&sample_cfg(), &weather);
        assert!(section.starts_with(CONFIG_MARKER_START));
        assert!(section.trim_end().ends_with(CONFIG_MARKER_END.trim_end()));
        assert!(section.contains("reversal_0.2_0.55"));
        assert!(section.contains("0.55"));
        assert!(section.contains("hong-kong"));
        assert!(
            !section.contains("duration"),
            "the markets/durations table was dropped 2026-07-14, must not reappear"
        );
        assert!(
            section.contains("v_0.7_0.3_0.7_0.3_0.05"),
            "V-shape grid must be listed alongside the reversal grid"
        );
        assert!(section.contains("v_0.7_0.3_0.7_0.6_0.2"));
    }

    #[test]
    fn config_section_is_replaced_not_duplicated_across_writes() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = dir.path();
        let trade_log = dir.path().join("trades.jsonl");
        std::fs::write(&trade_log, "").unwrap();
        let snapshots: SharedSnapshots = Arc::new(Mutex::new(HashMap::new()));
        let stale_log = new_stale_log();
        let cfg = sample_cfg();

        let (summary_path, _) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();

        // summary_{date}.md is always fully rewritten (not appended), so the config section
        // trivially can't duplicate — this mostly guards against a future refactor
        // accidentally switching it to an append-style write.
        let content = std::fs::read_to_string(&summary_path).unwrap();
        assert_eq!(
            content.matches(CONFIG_MARKER_START).count(),
            1,
            "config section must appear exactly once in summary_{{date}}.md"
        );
        assert_eq!(content.matches("reversal_0.2_0.55").count(), 1);
    }

    #[test]
    fn second_run_in_same_hour_nests_into_one_trades_file() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = dir.path();
        let trade_log = dir.path().join("trades.jsonl");
        std::fs::write(&trade_log, "").unwrap();
        let snapshots: SharedSnapshots = Arc::new(Mutex::new(HashMap::new()));
        let stale_log = new_stale_log();
        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };

        let (_, trades_path1) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_first = std::fs::read_to_string(&trades_path1).unwrap();
        assert_eq!(after_first.matches(RUN_MARKER).count(), 1);

        let (_, trades_path2) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        assert_eq!(
            trades_path1, trades_path2,
            "same real hour must write to the same trades_{{date}}_{{HH}}.md file"
        );
        let after_second = std::fs::read_to_string(&trades_path2).unwrap();
        // Same hour -> two nested runs, still one file.
        assert_eq!(after_second.matches(RUN_MARKER).count(), 2);
    }

    /// The core of this session's item 5: trades from an earlier run in the same hour must
    /// still show up in the hour-level trades section after a second write, merged (not
    /// duplicated, not dropped) into one table rather than split into a separate section per
    /// run.
    #[test]
    fn trades_merge_across_runs_within_the_same_hour_not_split_per_run() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = dir.path();
        let trade_log = dir.path().join("trades.jsonl");
        let snapshots: SharedSnapshots = Arc::new(Mutex::new(HashMap::new()));
        let stale_log = new_stale_log();
        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };
        let now = Utc::now().timestamp() as f64;

        std::fs::write(
            &trade_log,
            format!(
                "{{\"logged_at\":{now},\"market_kind\":\"crypto\",\"variant_id\":\"v1\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{now},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"TIMEOUT\",\"pnl\":0.1}}\n"
            ),
        )
        .unwrap();
        let (_, trades_path1) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_first = std::fs::read_to_string(&trades_path1).unwrap();
        assert!(after_first.contains("v1"));
        assert_eq!(after_first.matches(HOUR_TRADES_MARKER_START).count(), 1);

        // A second trade arrives, then a second run within the same hour.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&trade_log)
            .unwrap();
        use std::io::Write as _;
        writeln!(
            f,
            "{{\"logged_at\":{now},\"market_kind\":\"crypto\",\"variant_id\":\"v2\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{now},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"UNWIND\",\"pnl\":0.1}}"
        )
        .unwrap();
        let (_, trades_path2) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_second = std::fs::read_to_string(&trades_path2).unwrap();

        assert!(
            after_second.contains("v1") && after_second.contains("v2"),
            "both runs' trades must appear, merged"
        );
        assert_eq!(
            after_second.matches(HOUR_TRADES_MARKER_START).count(),
            1,
            "must be one merged trades section, not one per run"
        );
        assert_eq!(after_second.matches(RUN_MARKER).count(), 2);
    }

    #[test]
    fn refresh_hour_trades_tables_recomputes_from_log_without_touching_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = dir.path();
        let trade_log = dir.path().join("trades.jsonl");
        let now = Utc::now().timestamp() as f64;
        std::fs::write(
            &trade_log,
            format!(
                "{{\"logged_at\":{now},\"market_kind\":\"crypto\",\"variant_id\":\"variant_alpha\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{now},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"TIMEOUT\",\"pnl\":0.1}}\n"
            ),
        )
        .unwrap();

        let snapshots: SharedSnapshots = Arc::new(Mutex::new(HashMap::new()));
        crate::snapshot::update(
            &snapshots,
            "BTC-5m",
            crate::snapshot::MarketSnapshot {
                kind: "crypto",
                label: "BTC-5m".to_string(),
                up_price: 0.4242,
                dn_price: 0.5758,
                last_tick_ms: (now * 1000.0) as i64,
            },
        );
        let stale_log = new_stale_log();
        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };

        let (_, trades_path) = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let before = std::fs::read_to_string(&trades_path).unwrap();
        assert!(before.contains("variant_alpha"));
        assert!(!before.contains("variant_beta"));
        // The run-level crypto snapshot row is what refresh must leave untouched.
        assert!(before.contains("0.4242"));

        // A second trade lands in the trade log after the report was already written —
        // refresh must pick it up from ground truth even though it wasn't there at write
        // time.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&trade_log)
            .unwrap();
        use std::io::Write as _;
        writeln!(
            f,
            "{{\"logged_at\":{now},\"market_kind\":\"crypto\",\"variant_id\":\"variant_beta\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{now},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"UNWIND\",\"pnl\":0.2}}"
        )
        .unwrap();

        let written = refresh_hour_trades_tables(&trade_log, report_dir).unwrap();
        assert_eq!(written, vec![trades_path.clone()]);

        let after = std::fs::read_to_string(&trades_path).unwrap();
        assert!(after.contains("variant_alpha") && after.contains("variant_beta"));
        assert!(after.contains("PnL by variant (all markets)"));
        // Untouched by the refresh: the run-level crypto snapshot row.
        assert!(after.contains("0.4242"));
    }

    #[test]
    fn regenerate_from_trade_log_writes_per_day_folders_and_respects_since_date() {
        let dir = tempfile::tempdir().unwrap();
        // report_dir is a subdirectory, not `dir.path()` itself — the test later
        // `remove_dir_all`s report_dir to prove a fresh regenerate run, and that must not
        // also delete trade_log (which lives in the same tempdir root).
        let report_dir = &dir.path().join("reports");
        let trade_log = dir.path().join("trades.jsonl");

        let day1_ts = 1_700_000_000.0;
        let day2_ts = day1_ts + 90.0 * 86_400.0; // ~90 days later, definitely a different day

        let day1 = Utc
            .timestamp_opt(day1_ts as i64, 0)
            .single()
            .unwrap()
            .with_timezone(&hkt())
            .format("%Y-%m-%d")
            .to_string();
        let day2 = Utc
            .timestamp_opt(day2_ts as i64, 0)
            .single()
            .unwrap()
            .with_timezone(&hkt())
            .format("%Y-%m-%d")
            .to_string();
        assert_ne!(day1, day2);

        std::fs::write(
            &trade_log,
            format!(
                "{{\"logged_at\":{day1_ts},\"market_kind\":\"crypto\",\"variant_id\":\"v1\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{day1_ts},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"TIMEOUT\",\"pnl\":0.1}}\n\
                 {{\"logged_at\":{day2_ts},\"market_kind\":\"crypto\",\"variant_id\":\"v2\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{day2_ts},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"UNWIND\",\"pnl\":0.2}}\n"
            ),
        )
        .unwrap();

        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };

        // No filter: both days produced.
        let written = regenerate_from_trade_log(&trade_log, report_dir, &cfg, &[], None).unwrap();
        assert!(day_dir(report_dir, &day1).is_dir());
        assert!(day_dir(report_dir, &day2).is_dir());
        assert!(written.contains(&summary_path(report_dir, &day1)));
        assert!(written.contains(&summary_path(report_dir, &day2)));

        let day1_summary = std::fs::read_to_string(summary_path(report_dir, &day1)).unwrap();
        assert!(day1_summary.contains("v1"));
        assert!(day1_summary.contains("## Hours"));

        // Rerun from scratch with since_date scoped to day2 only — day1 must be genuinely
        // skipped, not just already present from the previous run.
        std::fs::remove_dir_all(report_dir).unwrap();
        let day2_date = NaiveDate::parse_from_str(&day2, "%Y-%m-%d").unwrap();
        let written2 =
            regenerate_from_trade_log(&trade_log, report_dir, &cfg, &[], Some(day2_date)).unwrap();
        assert!(!day_dir(report_dir, &day1).exists());
        assert!(day_dir(report_dir, &day2).is_dir());
        assert!(!written2.contains(&summary_path(report_dir, &day1)));
    }

    #[test]
    fn regenerate_summaries_only_rewrites_summary_but_leaves_hour_files_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = &dir.path().join("reports");
        let trade_log = dir.path().join("trades.jsonl");

        let ts = 1_700_000_000.0;
        let date = Utc
            .timestamp_opt(ts as i64, 0)
            .single()
            .unwrap()
            .with_timezone(&hkt())
            .format("%Y-%m-%d")
            .to_string();
        let hour = Utc
            .timestamp_opt(ts as i64, 0)
            .single()
            .unwrap()
            .with_timezone(&hkt())
            .format("%H")
            .to_string();

        std::fs::write(
            &trade_log,
            format!(
                "{{\"logged_at\":{ts},\"market_kind\":\"crypto\",\"variant_id\":\"v1\",\"asset\":\"BTC\",\"market\":\"BTC-5m\",\"slug\":\"s\",\"cycle_start\":0.0,\"strategy\":\"reversal\",\"side\":\"UP\",\"entry_ts\":{ts},\"token_price\":0.5,\"exit_price\":0.6,\"outcome\":\"TIMEOUT\",\"pnl\":0.1}}\n"
            ),
        )
        .unwrap();

        let cfg = SiglabConfig {
            markets: vec![],
            hourly_markets: vec![],
            variants: vec![],
        };

        // Seed a pre-existing hour file carrying "real" market-state content that a full
        // regenerate_from_trade_log run would normally clobber with a "_Regenerated..._"
        // placeholder note.
        let t_path = trades_path(report_dir, &date, &hour);
        std::fs::create_dir_all(day_dir(report_dir, &date)).unwrap();
        std::fs::write(&t_path, "REAL SNAPSHOT DATA, NOT REGENERATED\n").unwrap();

        let written =
            regenerate_summaries_from_trade_log(&trade_log, report_dir, &cfg, &[], None).unwrap();
        assert_eq!(written, vec![summary_path(report_dir, &date)]);

        let summary = std::fs::read_to_string(summary_path(report_dir, &date)).unwrap();
        assert!(summary.contains("v1"));
        assert!(summary.contains("### Top performing strategies"));
        assert!(summary.contains(&format!(
            "[trades_{date}_{hour}.md](trades_{date}_{hour}.md)"
        )));

        // The hour file itself must be byte-for-byte untouched.
        let hour_file_after = std::fs::read_to_string(&t_path).unwrap();
        assert_eq!(hour_file_after, "REAL SNAPSHOT DATA, NOT REGENERATED\n");
    }
}
