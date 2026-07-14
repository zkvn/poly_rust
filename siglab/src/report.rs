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
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{FixedOffset, TimeZone as _, Timelike, Utc};

use crate::cgroup;
use crate::config::SiglabConfig;
use crate::record::SiglabTradeRecord;
use crate::snapshot::SharedSnapshots;

const HOUR_MARKER_PREFIX: &str = "<!-- siglab-hour:";
const HOUR_BODY_START: &str = "<!-- siglab-hour-body-start -->\n";
const HOUR_BODY_END: &str = "<!-- siglab-hour-body-end -->\n";
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

/// "AM" for HKT hours 00-11, "PM" for 12-23 — each real HKT day is split into two report
/// files along this boundary so a single day's file doesn't grow unbounded (2026-07-14,
/// after the pre-split single-file report hit 2.2MB and became unwieldy to open).
fn half_of_hour(hour: u32) -> &'static str {
    if hour < 12 { "AM" } else { "PM" }
}

pub fn report_path(report_dir: &Path) -> PathBuf {
    let now = now_hkt();
    let date = now.format("%Y-%m-%d");
    let half = half_of_hour(now.hour());
    report_dir.join(format!("signal_report_{date}_{half}.md"))
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
/// Shared by the weather and World Cup sections, which are otherwise identical in shape.
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
        "{run_label} HKT — {} crypto market(s), {} weather bucket(s), {} World Cup bucket(s), {new_trade_count} new trade(s), {} stale event(s)",
        snaps.iter().filter(|s| s.kind == "crypto").count(),
        snaps.iter().filter(|s| s.kind == "weather").count(),
        snaps.iter().filter(|s| s.kind == "worldcup").count(),
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

    // ── World Cup market state — same grouping pattern, one row per event showing its
    //    current highest-probability outcome (e.g. "World Cup Winner" -> whichever team is
    //    currently favored). ──
    out.push_str(&render_grouped_snapshot_section(
        &snaps,
        "worldcup",
        "World Cup market state snapshot",
        "event",
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
fn render_config_section(
    cfg: &SiglabConfig,
    weather_cities: &[String],
    worldcup_events: &[String],
) -> String {
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

    out.push_str(&format!(
        "<details>\n<summary>Weather cities ({})</summary>\n\n| city |\n|---|\n",
        weather_cities.len()
    ));
    for city in weather_cities {
        out.push_str(&format!("| {city} |\n"));
    }
    out.push_str("\n</details>\n\n");

    out.push_str(&format!(
        "<details>\n<summary>World Cup events ({})</summary>\n\n| event |\n|---|\n",
        worldcup_events.len()
    ));
    for event in worldcup_events {
        out.push_str(&format!("| {event} |\n"));
    }
    out.push_str("\n</details>\n\n");

    out.push_str(
        "Weather/World Cup buckets trade the same `reversal_{low}_{high}` 18-combo grid via \
         `bucket_reversal.rs::reversal_grid()` (fixed `sl_pnl=0.3`/`unwind_pnl=0.15`/\
         `max_hold=25s`), not `config/markets.toml` — see that module's doc comment. Crypto \
         reversal variants additionally force-close (labeled `UNWIND`) within 10s of the \
         market's own cycle-end regardless of holding time — weather/World Cup buckets have \
         no cycle-end concept, so that rule doesn't apply to them (see `bucket_reversal.rs`'s \
         doc comment).\n\n",
    );

    out.push_str(CONFIG_MARKER_END);
    out
}

/// Renders one hour's whole `<details>` block (marker comment + a `###` section header —
/// same style as `### Strategy config` — + summary + `inner`, which is the concatenation of
/// that hour's `RUN_MARKER`-prefixed run blocks, newest first). The `###` heading lives
/// outside/before the `<details>` so it's a real jump-to-able Markdown heading, not just
/// collapsible-summary text.
fn render_hour_block(
    hour_key: &str,
    hour_label: &str,
    inner: &str,
    run_count: usize,
    hour_trade_count: usize,
    hour_pnl: f64,
) -> String {
    format!(
        "{HOUR_MARKER_PREFIX}{hour_key} -->\n\
         ### {hour_label} HKT\n\n\
         <details open>\n\
         <summary><strong>{run_count} report run(s), {hour_trade_count} trade(s) this hour, total pnl {hour_pnl:.4}</strong></summary>\n\n\
         {HOUR_BODY_START}\
         {inner}\
         {HOUR_BODY_END}\
         </details>\n\n"
    )
}

/// The report file's fixed leading text — shared by `write_hourly_report` (live) and
/// `regenerate_from_trade_log` (one-off backfill) so both produce byte-identical headers;
/// `write_hourly_report`'s `existing.strip_prefix(&header)` continuation check depends on
/// that. `half` is `"AM"` or `"PM"` (see `half_of_hour`) — each real HKT day is split into
/// two files along that boundary (2026-07-14, after the single-file-per-day report grew to
/// 2.2MB and became unwieldy to open).
fn report_header(date: &str, half: &str) -> String {
    let other = if half == "AM" { "PM" } else { "AM" };
    format!(
        "# siglab signal report — {date} {half}\n\n\
         Auto-generated by siglab, newest hour first — each hour is one collapsible section.\n\
         Each real HKT day is split into two files, AM (00:00-11:59) and PM (12:00-23:59), to\n\
         keep file size manageable — see `signal_report_{date}_{other}.md` for the other half.\n\
         Trades are merged across the whole hour (not split per report-writer run) and\n\
         regenerated fresh on every write; market-state/staleness/CPU snapshots stay one\n\
         collapsible sub-section per run (there can be several now that runs fire every\n\
         `--report-interval-secs`, e.g. every 15 min). See\n\
         `siglab/doc/local_resource_test_2026-07-13.md` for the Docker resource baseline and\n\
         `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md` for what this harness does\n\
         and does not claim. Weather and World Cup markets trade via a self-contained\n\
         `bucket_reversal.rs` reversal engine (18 variants per bucket, no delta/Gamma/resolve —\n\
         see that file's doc comment), separate from crypto's `trader::machine::Machine`.\n\n"
    )
}

/// One-off: rebuild `report_dir/signal_report_{date}.md` entirely from the trade log's
/// ground truth, grouped by real HKT date/hour, using the exact same rendering functions as
/// the live writer — added 2026-07-14 to backfill existing reports into this session's new
/// format (merged-per-hour trades, config table, `### {hour}` headings, sl/timeout/unwind
/// summary columns) without needing to parse the old Markdown. Historical market-state/
/// staleness/CPU snapshots aren't recoverable from the trade log alone (they were never
/// persisted anywhere else), so each regenerated hour carries a note instead of fabricating
/// them; the live process's next natural write still extends whichever hour is current
/// normally (`write_hourly_report`'s continuation logic doesn't require a run marker to be
/// present in a past, no-longer-written-to hour).
pub fn regenerate_from_trade_log(
    trade_log_path: &Path,
    report_dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    worldcup_events: &[String],
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
        let date = dt.format("%Y-%m-%d").to_string();
        let hour = dt.format("%H").to_string();
        by_date_hour.entry((date, hour)).or_default().push(t);
    }

    // Group by (date, half) rather than just date — each real HKT day is split into an AM
    // and a PM file (see `half_of_hour`), so the backfill must produce the same two files
    // the live writer would.
    let mut hours_by_half: std::collections::BTreeMap<(String, &'static str), Vec<String>> =
        std::collections::BTreeMap::new();
    for (date, hour) in by_date_hour.keys() {
        let half = half_of_hour(hour.parse::<u32>().unwrap_or(0));
        hours_by_half
            .entry((date.clone(), half))
            .or_default()
            .push(hour.clone());
    }

    let regenerated_note = "_Regenerated from the trade log (2026-07-14 format backfill) — \
        historical market-state/staleness/CPU snapshots aren't recoverable from trade data \
        alone and are omitted for this hour; hours written by the live process carry them \
        as usual._\n\n";

    let mut written = Vec::new();
    for ((date, half), hours) in hours_by_half.iter().rev() {
        let mut hours_sorted = hours.clone();
        hours_sorted.sort();
        hours_sorted.reverse(); // newest hour first within the file

        let mut body = String::new();
        for (i, hour) in hours_sorted.iter().enumerate() {
            let trades = &by_date_hour[&(date.clone(), hour.clone())];
            let hour_pnl: f64 = trades.iter().map(|t| t.pnl).sum::<f64>() + 0.0;
            let hour_key = format!("{date}T{hour}");
            let hour_label = format!("{date} {hour}:00");
            let inner = format!(
                "{HOUR_TRADES_MARKER_START}{}{HOUR_TRADES_MARKER_END}{regenerated_note}",
                render_hour_trades_section(trades)
            );
            let block =
                render_hour_block(&hour_key, &hour_label, &inner, 0, trades.len(), hour_pnl);
            // Only the newest hour in the file stays expanded — render_hour_block always
            // emits `<details open>`, so collapse every older one explicitly.
            if i == 0 {
                body.push_str(&block);
            } else {
                body.push_str(&block.replacen("<details open>\n", "<details>\n", 1));
            }
        }

        let path = report_dir.join(format!("signal_report_{date}_{half}.md"));
        let mut f = std::fs::File::create(&path).with_context(|| format!("write {path:?}"))?;
        f.write_all(report_header(date, half).as_bytes())?;
        f.write_all(render_config_section(cfg, weather_cities, worldcup_events).as_bytes())?;
        f.write_all(body.as_bytes())?;
        written.push(path);
    }
    Ok(written)
}

/// Writes (inserting) this run's section into today's report file — either nested inside
/// the still-current hour's `<details>` (if the last write was in the same real HKT hour)
/// or as a fresh hour section on top (collapsing the previous hour's section, since only the
/// current hour stays expanded by default).
#[allow(clippy::too_many_arguments)]
pub fn write_hourly_report(
    report_dir: &Path,
    cfg: &SiglabConfig,
    weather_cities: &[String],
    worldcup_events: &[String],
    snapshots: &SharedSnapshots,
    trade_log_path: &Path,
    stale_log: &SharedStaleLog,
    cgroup_prev: Option<cgroup::Sample>,
    cgroup_now: Option<cgroup::Sample>,
    window_secs: f64,
) -> Result<PathBuf> {
    std::fs::create_dir_all(report_dir).context("create report dir")?;
    let path = report_path(report_dir);
    let now_for_header = now_hkt();
    let date = now_for_header.format("%Y-%m-%d").to_string();
    let half = half_of_hour(now_for_header.hour());

    let header = report_header(&date, half);

    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let body = if let Some(stripped) = existing.strip_prefix(&header) {
        stripped.to_string()
    } else {
        // Missing file, or an existing file from a previous day/format — start fresh
        // rather than guess where the header ends.
        String::new()
    };
    // Strip out any previously-written config section — it's always regenerated fresh
    // below from the current config files rather than left stale from whenever the file
    // was first created today.
    let body = match (body.find(CONFIG_MARKER_START), body.find(CONFIG_MARKER_END)) {
        (Some(start), Some(end)) if end >= start => {
            let mut b = body[..start].to_string();
            b.push_str(&body[end + CONFIG_MARKER_END.len()..]);
            b
        }
        _ => body,
    };

    let now = now_hkt();
    let hour_key = now.format("%Y-%m-%dT%H").to_string();
    let hour_label = now.format("%Y-%m-%d %H:00").to_string();
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
    // `Iterator::sum()` folds from `-0.0` for `f64`, so an empty/zero-sum window prints as
    // "-0.0000" unless normalized — `+ 0.0` flips a negative zero back to positive.
    let hour_pnl: f64 = hour_trades.iter().map(|t| t.pnl).sum::<f64>() + 0.0;
    let hour_trades_html = format!(
        "{HOUR_TRADES_MARKER_START}{}{HOUR_TRADES_MARKER_END}",
        render_hour_trades_section(&hour_trades)
    );

    // Strip out the hour's own previously-written trades block from `inner` (it's always
    // regenerated fresh above from every trade logged so far this hour) before re-nesting
    // the remaining (market-state/staleness/cpu-only) run blocks.
    let strip_old_hour_trades = |inner: &str| -> String {
        match (
            inner.find(HOUR_TRADES_MARKER_START),
            inner.find(HOUR_TRADES_MARKER_END),
        ) {
            (Some(start), Some(end)) if end >= start => {
                let mut s = inner[..start].to_string();
                s.push_str(&inner[end + HOUR_TRADES_MARKER_END.len()..]);
                s
            }
            _ => inner.to_string(),
        }
    };

    let marker = format!("{HOUR_MARKER_PREFIX}{hour_key} -->\n");
    let new_body = if body.starts_with(&marker) {
        let bounds = body
            .find(HOUR_BODY_START)
            .map(|p| p + HOUR_BODY_START.len())
            .zip(body.find(HOUR_BODY_END));
        match bounds {
            Some((body_start, body_end)) if body_end >= body_start => {
                let inner_old = strip_old_hour_trades(&body[body_start..body_end]);
                let run_count = inner_old.matches(RUN_MARKER).count() + 1;
                let new_inner = format!("{hour_trades_html}{RUN_MARKER}{run_block}{inner_old}");
                let after_end_marker = body_end + HOUR_BODY_END.len();
                let close_tag = "</details>\n\n";
                let hour_block_end = if body[after_end_marker..].starts_with(close_tag) {
                    after_end_marker + close_tag.len()
                } else {
                    body[after_end_marker..]
                        .find("</details>")
                        .map(|p| after_end_marker + p + "</details>".len())
                        .unwrap_or(after_end_marker)
                };
                let remainder = &body[hour_block_end..];
                let rebuilt = render_hour_block(
                    &hour_key,
                    &hour_label,
                    &new_inner,
                    run_count,
                    hour_trades.len(),
                    hour_pnl,
                );
                format!("{rebuilt}{remainder}")
            }
            _ => {
                // Markers present but malformed somehow — don't lose old data, just start a
                // fresh hour block on top of everything that's already there.
                let fresh = render_hour_block(
                    &hour_key,
                    &hour_label,
                    &format!("{hour_trades_html}{RUN_MARKER}{run_block}"),
                    1,
                    hour_trades.len(),
                    hour_pnl,
                );
                format!("{fresh}{body}")
            }
        }
    } else {
        // New hour (or first write of the day): collapse the previous hour's section — it's
        // the only thing in the file ever rendered with `<details open>`, so this is safe
        // and precise, not a heuristic guess.
        let collapsed = body.replacen("<details open>\n", "<details>\n", 1);
        let fresh = render_hour_block(
            &hour_key,
            &hour_label,
            &format!("{hour_trades_html}{RUN_MARKER}{run_block}"),
            1,
            hour_trades.len(),
            hour_pnl,
        );
        format!("{fresh}{collapsed}")
    };

    let mut f = std::fs::File::create(&path).with_context(|| format!("write {path:?}"))?;
    f.write_all(header.as_bytes())?;
    f.write_all(render_config_section(cfg, weather_cities, worldcup_events).as_bytes())?;
    f.write_all(new_body.as_bytes())?;
    Ok(path)
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
    fn hour_trades_section_includes_both_summary_tables() {
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
        assert!(section.contains("reversal_0.2_0.55"));
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
    fn config_section_lists_variant_grid_and_weather_worldcup() {
        let weather = vec!["hong-kong".to_string()];
        let worldcup = vec!["world-cup-winner".to_string()];
        let section = render_config_section(&sample_cfg(), &weather, &worldcup);
        assert!(section.starts_with(CONFIG_MARKER_START));
        assert!(section.trim_end().ends_with(CONFIG_MARKER_END.trim_end()));
        assert!(section.contains("reversal_0.2_0.55"));
        assert!(section.contains("0.55"));
        assert!(section.contains("hong-kong"));
        assert!(section.contains("world-cup-winner"));
        assert!(
            !section.contains("duration"),
            "the markets/durations table was dropped 2026-07-14, must not reappear"
        );
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

        let path = write_hourly_report(
            report_dir,
            &cfg,
            &[],
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
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            content.matches(CONFIG_MARKER_START).count(),
            1,
            "config section must be replaced wholesale on every write, not duplicated"
        );
        assert_eq!(content.matches("reversal_0.2_0.55").count(), 1);
    }

    #[test]
    fn second_run_in_same_hour_nests_inside_one_hour_details() {
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

        let path1 = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_first = std::fs::read_to_string(&path1).unwrap();
        assert_eq!(after_first.matches(HOUR_MARKER_PREFIX).count(), 1);
        assert_eq!(after_first.matches(RUN_MARKER).count(), 1);
        assert_eq!(after_first.matches("<details open>").count(), 1);

        let path2 = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_second = std::fs::read_to_string(&path2).unwrap();
        // Same hour -> still exactly one hour marker/one open details, but two nested runs.
        assert_eq!(after_second.matches(HOUR_MARKER_PREFIX).count(), 1);
        assert_eq!(after_second.matches(RUN_MARKER).count(), 2);
        assert_eq!(after_second.matches("<details open>").count(), 1);
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
        let path1 = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_first = std::fs::read_to_string(&path1).unwrap();
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
        let path2 = write_hourly_report(
            report_dir,
            &cfg,
            &[],
            &[],
            &snapshots,
            &trade_log,
            &stale_log,
            None,
            None,
            900.0,
        )
        .unwrap();
        let after_second = std::fs::read_to_string(&path2).unwrap();

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
}
