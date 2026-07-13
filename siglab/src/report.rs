//! Hourly signal-summary report, written as Markdown with collapsible per-hour sections.
//! File name is `signal_report_{YYYY-MM-DD}.md` (HKT date) — a new file starts each day,
//! and every hourly run within a day updates the *same* file by inserting a new `<details>`
//! block right after the header (newest hour first).

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{FixedOffset, TimeZone as _, Utc};

use crate::cgroup;
use crate::snapshot::SharedSnapshots;

/// One staleness event, timestamped so the report can filter to "this past hour".
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
        // 5000 events, plenty for an hourly report cadence.
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

pub fn report_path(report_dir: &Path) -> PathBuf {
    let date = now_hkt().format("%Y-%m-%d");
    report_dir.join(format!("signal_report_{date}.md"))
}

/// Reads the last hour's trade records from `trade_log_path` (JSONL), filtering by
/// `logged_at` (unix seconds) >= `since_unix`. Best-effort — a missing/unreadable file (no
/// trades yet) is treated as zero trades, not an error.
fn recent_trades(trade_log_path: &Path, since_unix: f64) -> Vec<crate::record::SiglabTradeRecord> {
    let Ok(content) = std::fs::read_to_string(trade_log_path) else {
        return Vec::new();
    };
    content
        .lines()
        .filter_map(|l| serde_json::from_str::<crate::record::SiglabTradeRecord>(l).ok())
        .filter(|r| r.logged_at >= since_unix)
        .collect()
}

fn render_hour_section(
    snapshots: &SharedSnapshots,
    trade_log_path: &Path,
    stale_log: &SharedStaleLog,
    cgroup_prev: Option<cgroup::Sample>,
    cgroup_now: Option<cgroup::Sample>,
    window_secs: f64,
) -> String {
    let now = now_hkt();
    let now_unix = Utc::now().timestamp() as f64;
    let since_unix = now_unix - window_secs;
    let now_ms = (now_unix * 1000.0) as i64;

    let trades = recent_trades(trade_log_path, since_unix);
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
        "{} HKT — {} crypto market(s), {} weather bucket(s), {} World Cup bucket(s), {} trade(s), {} stale event(s)",
        now.format("%Y-%m-%d %H:00"),
        snaps.iter().filter(|s| s.kind == "crypto").count(),
        snaps.iter().filter(|s| s.kind == "weather").count(),
        snaps.iter().filter(|s| s.kind == "worldcup").count(),
        trades.len(),
        stale_events.len(),
    );
    out.push_str(&format!(
        "<details open>\n<summary><strong>{summary}</strong></summary>\n\n"
    ));

    // ── Trades this hour (crypto Machine + weather/World Cup bucket_reversal) ──
    out.push_str(&render_trade_summary(&trades));
    out.push_str(&render_trade_table(&trades));

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
    //    of every city into the report every hour). ──
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
    out.push_str("<details>\n<summary>Staleness events (past hour, observe-only — no auto action taken)</summary>\n\n");
    if stale_events.is_empty() {
        out.push_str("_No staleness escalations this hour._\n\n");
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

    // ── CPU / memory (past hour) ──
    out.push_str("<details>\n<summary>CPU / memory (past hour)</summary>\n\n");
    match (cgroup_prev, cgroup_now) {
        (Some(prev), Some(now_s)) => {
            let cpu_pct = cgroup::cpu_percent(&prev, &now_s);
            let mem_mib = now_s.mem_bytes as f64 / (1024.0 * 1024.0);
            out.push_str(&format!(
                "- CPU (avg over past hour, one-core=100%): **{cpu_pct:.2}%**\n- Memory (current): **{mem_mib:.1} MiB**\n\n"
            ));
        }
        _ => {
            out.push_str("_cgroup stats unavailable (not running under cgroup v2, e.g. outside Docker)._\n\n");
        }
    }
    out.push_str("</details>\n\n</details>\n\n");

    out
}

/// Aggregated PnL by (market, strategy) for the past hour, shown *above* the per-trade
/// table — the table answers "what happened," this answers "how did each market/strategy
/// combo do," which is the number worth seeing first when several markets/variants fired.
/// Note this aggregates by `strategy` (`"reversal"`/`"high_prob"`), not by the finer-grained
/// `variant_id` — with 18 reversal variants often firing together on the same dip (they
/// share the same underlying price move, just different thresholds), a per-variant summary
/// here would mostly repeat near-identical rows; per-market-per-strategy is the more useful
/// aggregate. The full per-trade table below still shows every variant individually.
fn render_trade_summary(trades: &[crate::record::SiglabTradeRecord]) -> String {
    let mut out =
        String::from("#### Summary: PnL by market and strategy (all trades, past hour)\n\n");
    if trades.is_empty() {
        out.push_str("_No trades fired this hour._\n\n");
        return out;
    }

    let mut agg: HashMap<(String, String), (u32, f64)> = HashMap::new();
    for t in trades {
        let entry = agg
            .entry((t.market.clone(), t.strategy.clone()))
            .or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += t.pnl;
    }
    let mut rows: Vec<_> = agg.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    out.push_str("| market | strategy | trades | total pnl |\n|---|---|---|---|\n");
    for ((market, strategy), (count, total_pnl)) in &rows {
        out.push_str(&format!(
            "| {market} | {strategy} | {count} | {total_pnl:.4} |\n"
        ));
    }
    let grand_total: f64 = trades.iter().map(|t| t.pnl).sum();
    out.push_str(&format!("\n**Total pnl this hour: {grand_total:.4}**\n\n"));
    out
}

/// Per-trade table, sorted by market then trade datetime (entry time) within each market —
/// so all of e.g. XRP-15m's trades sit together in chronological order, then XRP-5m's, etc.,
/// rather than interleaved by whichever variant happened to fire first.
fn render_trade_table(trades: &[crate::record::SiglabTradeRecord]) -> String {
    let mut out = String::from("#### Trades (past hour)\n\n");
    if trades.is_empty() {
        out.push_str("_No trades fired this hour._\n\n");
        return out;
    }

    let mut sorted: Vec<_> = trades.iter().collect();
    sorted.sort_by(|a, b| {
        a.market.cmp(&b.market).then(
            a.entry_ts
                .partial_cmp(&b.entry_ts)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });

    out.push_str(
        "| datetime (HKT) | market | variant | side | outcome | pnl |\n|---|---|---|---|---|---|\n",
    );
    for t in &sorted {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.4} |\n",
            entry_datetime_hkt(t.entry_ts),
            t.market,
            t.variant_id,
            t.side,
            t.outcome,
            t.pnl
        ));
    }
    out.push('\n');
    out
}

fn entry_datetime_hkt(entry_ts: f64) -> String {
    Utc.timestamp_opt(entry_ts as i64, 0)
        .single()
        .map(|dt| {
            dt.with_timezone(&hkt())
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "?".to_string())
}

/// Renders a collapsible section grouping `snaps` of the given `kind` by the prefix before
/// `": "` in their label (e.g. `"hong-kong: 33°C"` groups under `"hong-kong"`), showing one
/// row per group with its current highest-probability outcome — the most informative single
/// number per group without dumping every bucket of every group into the report every hour.
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

/// Writes (inserting, newest-first) this hour's section into today's report file.
pub fn write_hourly_report(
    report_dir: &Path,
    snapshots: &SharedSnapshots,
    trade_log_path: &Path,
    stale_log: &SharedStaleLog,
    cgroup_prev: Option<cgroup::Sample>,
    cgroup_now: Option<cgroup::Sample>,
    window_secs: f64,
) -> Result<PathBuf> {
    std::fs::create_dir_all(report_dir).context("create report dir")?;
    let path = report_path(report_dir);
    let date = now_hkt().format("%Y-%m-%d");

    let header = format!(
        "# siglab signal report — {date}\n\n\
         Auto-generated by siglab every hour (HKT), newest hour first. See\n\
         `siglab/doc/local_resource_test_2026-07-13.md` for the Docker resource baseline and\n\
         `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md` for what this harness does\n\
         and does not claim. Weather and World Cup markets trade via a self-contained\n\
         `bucket_reversal.rs` reversal engine (18 variants per bucket, no delta/Gamma/resolve —\n\
         see that file's doc comment), separate from crypto's `trader::machine::Machine`.\n\n"
    );

    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let body = if let Some(stripped) = existing.strip_prefix(&header) {
        stripped.to_string()
    } else if existing.is_empty() {
        String::new()
    } else {
        // Existing file from a previous day/format — start fresh rather than guess where
        // the header ends.
        String::new()
    };

    let new_section = render_hour_section(
        snapshots,
        trade_log_path,
        stale_log,
        cgroup_prev,
        cgroup_now,
        window_secs,
    );

    let mut f = std::fs::File::create(&path).with_context(|| format!("write {path:?}"))?;
    f.write_all(header.as_bytes())?;
    f.write_all(new_section.as_bytes())?;
    f.write_all(body.as_bytes())?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    ) -> crate::record::SiglabTradeRecord {
        crate::record::SiglabTradeRecord {
            logged_at: entry_ts + 10.0,
            market_kind: crate::record::MarketKind::Crypto,
            variant_id: variant_id.to_string(),
            asset: market.split('-').next().unwrap_or(market).to_string(),
            market: market.to_string(),
            slug: "s".to_string(),
            cycle_start: 0.0,
            strategy: strategy.to_string(),
            side: "UP".to_string(),
            entry_ts,
            token_price: 0.5,
            exit_price: 0.6,
            outcome: "WIN".to_string(),
            pnl,
        }
    }

    #[test]
    fn trade_table_sorts_by_market_then_entry_time() {
        let trades = vec![
            mk_trade("XRP-15m", "reversal_0.2_0.55", "reversal", 200.0, 0.1),
            mk_trade("XRP-5m", "reversal_0.2_0.55", "reversal", 100.0, 0.2),
            mk_trade("XRP-15m", "reversal_0.3_0.6", "reversal", 50.0, 0.3),
        ];
        let table = render_trade_table(&trades);
        // XRP-15m rows (entry 50 then 200) should both precede XRP-5m's row (entry 100),
        // and within XRP-15m, entry_ts=50 should come before entry_ts=200.
        let pos_15m_early = table.find("reversal_0.3_0.6").unwrap();
        let pos_15m_late = table.find("reversal_0.2_0.55").unwrap();
        let pos_5m = table.rfind("XRP-5m").unwrap();
        assert!(pos_15m_early < pos_15m_late);
        assert!(pos_15m_late < pos_5m);
    }

    #[test]
    fn trade_table_has_datetime_and_market_columns() {
        let trades = vec![mk_trade(
            "BTC-4h",
            "reversal_0.4_0.8",
            "reversal",
            1_700_000_000.0,
            0.5,
        )];
        let table = render_trade_table(&trades);
        assert!(table.contains("datetime (HKT)"));
        assert!(table.contains("market"));
        assert!(table.contains("BTC-4h"));
        // entry_ts formatted as a real date, not a raw epoch number.
        assert!(table.contains("2023-"));
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
        // (market, strategy) rather than by the finer-grained variant_id.
        assert!(summary.contains("| XRP-5m | reversal | 2 | 0.3000 |"));
        assert!(summary.contains("| XRP-5m | high_prob | 1 | -0.0500 |"));
        assert!(summary.contains("Total pnl this hour: 0.2500"));
    }

    #[test]
    fn empty_trades_render_gracefully() {
        assert!(render_trade_summary(&[]).contains("No trades fired"));
        assert!(render_trade_table(&[]).contains("No trades fired"));
    }
}
