//! Signal-summary report, written as Markdown with collapsible sections nested two levels
//! deep: one `<details>` per real HKT hour (newest first), each containing one `<details>`
//! per report-writer run that landed within that hour (there can be several now that runs
//! fire every `--report-interval-secs`, e.g. every 15 min instead of hourly), each of which
//! contains its own collapsible "Trades" section broken out into one collapsible table per
//! market. File name is `signal_report_{YYYY-MM-DD}.md` (HKT date) — a new file starts each
//! day, and every run within a day updates the *same* file, either by nesting a new run
//! inside the still-open current-hour section or by starting a fresh hour section (which
//! also collapses the previous hour's section, since only the current hour stays expanded
//! by default).
//!
//! The hour/run boundary is tracked with plain HTML comments (`<!-- siglab-hour:... -->`,
//! `<!-- siglab-hour-body-start/end -->`, `<!-- siglab-run -->`) rather than by parsing the
//! `<details>` tree — much simpler than generic bracket-matching, and safe since these exact
//! strings never otherwise appear in rendered content.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{FixedOffset, TimeZone as _, Timelike, Utc};

use crate::cgroup;
use crate::record::SiglabTradeRecord;
use crate::snapshot::SharedSnapshots;

const HOUR_MARKER_PREFIX: &str = "<!-- siglab-hour:";
const HOUR_BODY_START: &str = "<!-- siglab-hour-body-start -->\n";
const HOUR_BODY_END: &str = "<!-- siglab-hour-body-end -->\n";
const RUN_MARKER: &str = "<!-- siglab-run -->\n";

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

pub fn report_path(report_dir: &Path) -> PathBuf {
    let date = now_hkt().format("%Y-%m-%d");
    report_dir.join(format!("signal_report_{date}.md"))
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

/// Human label for a report-writer run's trade window, e.g. "past 15 min" / "past 1h".
fn window_label(window_secs: f64) -> String {
    let secs = window_secs.round() as i64;
    if secs > 0 && secs % 3600 == 0 {
        format!("past {}h", secs / 3600)
    } else if secs > 0 && secs % 60 == 0 {
        format!("past {} min", secs / 60)
    } else {
        format!("past {secs}s")
    }
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

/// Aggregated PnL by (market, strategy), shown *above* the per-market trade tables — this
/// answers "how did each market/strategy combo do," the per-market tables below answer "what
/// happened." Note this aggregates by `strategy` (`"reversal"`/`"high_prob"`), not by the
/// finer-grained `variant_id` — with 18 reversal variants often firing together on the same
/// dip (they share the same underlying price move, just different thresholds), a
/// per-variant summary here would mostly repeat near-identical rows.
fn render_trade_summary(trades: &[SiglabTradeRecord]) -> String {
    let mut out = String::from("#### Summary: PnL by market and strategy\n\n");
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
        "| datetime (HKT) | variant | side | outcome | pnl |\n|---|---|---|---|---|\n",
    );
    for t in &sorted {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.4} |\n",
            entry_datetime_hkt(t.entry_ts),
            t.variant_id,
            t.side,
            t.outcome,
            t.pnl
        ));
    }
    out.push('\n');
    out
}

/// The whole "Trades" block for one run: an outer collapsible section containing the
/// market/strategy PnL summary, then one collapsible table per market (sorted
/// alphabetically), each individually collapsible so a run with many quiet weather/World
/// Cup buckets doesn't force-render dozens of tables at once.
fn render_trades_section(trades: &[SiglabTradeRecord], label: &str) -> String {
    let mut out = format!(
        "<details>\n<summary>Trades ({label}): {} trade(s)</summary>\n\n",
        trades.len()
    );
    if trades.is_empty() {
        out.push_str("_No trades fired._\n\n</details>\n\n");
        return out;
    }

    out.push_str(&render_trade_summary(trades));

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

/// One report-writer run's section (nested inside its hour's `<details>`): trades, market
/// state snapshots, staleness health, CPU/memory — everything `write_hourly_report` used to
/// emit as its single top-level block, now one level deeper.
#[allow(clippy::too_many_arguments)]
fn render_run_section(
    run_label: &str,
    trade_window_label: &str,
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
        "{run_label} HKT — {} crypto market(s), {} weather bucket(s), {} World Cup bucket(s), {} trade(s), {} stale event(s)",
        snaps.iter().filter(|s| s.kind == "crypto").count(),
        snaps.iter().filter(|s| s.kind == "weather").count(),
        snaps.iter().filter(|s| s.kind == "worldcup").count(),
        trades.len(),
        stale_events.len(),
    );
    out.push_str(&format!("<details>\n<summary>{summary}</summary>\n\n"));

    out.push_str(&render_trades_section(&trades, trade_window_label));

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

/// Renders one hour's whole `<details>` block (marker comment + summary + `inner`, which is
/// the concatenation of that hour's `RUN_MARKER`-prefixed run blocks, newest first).
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
         <details open>\n\
         <summary><strong>{hour_label} HKT — {run_count} report run(s), {hour_trade_count} trade(s) this hour, total pnl {hour_pnl:.4}</strong></summary>\n\n\
         {HOUR_BODY_START}\
         {inner}\
         {HOUR_BODY_END}\
         </details>\n\n"
    )
}

/// Writes (inserting) this run's section into today's report file — either nested inside
/// the still-current hour's `<details>` (if the last write was in the same real HKT hour)
/// or as a fresh hour section on top (collapsing the previous hour's section, since only the
/// current hour stays expanded by default).
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
         Auto-generated by siglab, newest hour first — each hour is one collapsible section\n\
         containing every report-writer run that landed within it, and each run's trades are\n\
         broken out into one collapsible table per market. See\n\
         `siglab/doc/local_resource_test_2026-07-13.md` for the Docker resource baseline and\n\
         `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md` for what this harness does\n\
         and does not claim. Weather and World Cup markets trade via a self-contained\n\
         `bucket_reversal.rs` reversal engine (18 variants per bucket, no delta/Gamma/resolve —\n\
         see that file's doc comment), separate from crypto's `trader::machine::Machine`.\n\n"
    );

    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let body = if let Some(stripped) = existing.strip_prefix(&header) {
        stripped.to_string()
    } else {
        // Missing file, or an existing file from a previous day/format — start fresh
        // rather than guess where the header ends.
        String::new()
    };

    let now = now_hkt();
    let hour_key = now.format("%Y-%m-%dT%H").to_string();
    let hour_label = now.format("%Y-%m-%d %H:00").to_string();
    let run_label = now.format("%Y-%m-%d %H:%M").to_string();
    let trade_window_label = window_label(window_secs);

    let run_block = render_run_section(
        &run_label,
        &trade_window_label,
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

    let marker = format!("{HOUR_MARKER_PREFIX}{hour_key} -->\n");
    let new_body = if body.starts_with(&marker) {
        let bounds = body
            .find(HOUR_BODY_START)
            .map(|p| p + HOUR_BODY_START.len())
            .zip(body.find(HOUR_BODY_END));
        match bounds {
            Some((body_start, body_end)) if body_end >= body_start => {
                let inner_old = &body[body_start..body_end];
                let run_count = inner_old.matches(RUN_MARKER).count() + 1;
                let new_inner = format!("{RUN_MARKER}{run_block}{inner_old}");
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
                    &format!("{RUN_MARKER}{run_block}"),
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
            &format!("{RUN_MARKER}{run_block}"),
            1,
            hour_trades.len(),
            hour_pnl,
        );
        format!("{fresh}{collapsed}")
    };

    let mut f = std::fs::File::create(&path).with_context(|| format!("write {path:?}"))?;
    f.write_all(header.as_bytes())?;
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
        let section = render_trades_section(&trades, "past 15 min");
        // Two distinct per-market collapsible tables, one per market.
        assert!(section.contains("<summary>XRP-15m — 2 trade(s), total pnl 0.4000</summary>"));
        assert!(section.contains("<summary>XRP-5m — 1 trade(s), total pnl 0.2000</summary>"));
        // Within XRP-15m's own table, sorted by entry time (50 before 200).
        let pos_early = section.find("reversal_0.3_0.6").unwrap();
        let pos_late = section.find("reversal_0.2_0.55").unwrap();
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
        let section = render_trades_section(&trades, "past 15 min");
        assert!(section.contains("datetime (HKT)"));
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
        // (market, strategy) rather than by the finer-grained variant_id.
        assert!(summary.contains("| XRP-5m | reversal | 2 | 0.3000 |"));
        assert!(summary.contains("| XRP-5m | high_prob | 1 | -0.0500 |"));
        assert!(summary.contains("Total pnl: 0.2500"));
    }

    #[test]
    fn empty_trades_render_gracefully() {
        assert!(render_trades_section(&[], "past 15 min").contains("No trades fired"));
    }

    #[test]
    fn window_label_formats_common_intervals() {
        assert_eq!(window_label(900.0), "past 15 min");
        assert_eq!(window_label(3600.0), "past 1h");
        assert_eq!(window_label(90.0), "past 90s");
    }

    #[test]
    fn second_run_in_same_hour_nests_inside_one_hour_details() {
        let dir = tempfile::tempdir().unwrap();
        let report_dir = dir.path();
        let trade_log = dir.path().join("trades.jsonl");
        std::fs::write(&trade_log, "").unwrap();
        let snapshots: SharedSnapshots = Arc::new(Mutex::new(HashMap::new()));
        let stale_log = new_stale_log();

        let path1 = write_hourly_report(
            report_dir, &snapshots, &trade_log, &stale_log, None, None, 900.0,
        )
        .unwrap();
        let after_first = std::fs::read_to_string(&path1).unwrap();
        assert_eq!(after_first.matches(HOUR_MARKER_PREFIX).count(), 1);
        assert_eq!(after_first.matches(RUN_MARKER).count(), 1);
        assert_eq!(after_first.matches("<details open>").count(), 1);

        let path2 = write_hourly_report(
            report_dir, &snapshots, &trade_log, &stale_log, None, None, 900.0,
        )
        .unwrap();
        let after_second = std::fs::read_to_string(&path2).unwrap();
        // Same hour -> still exactly one hour marker/one open details, but two nested runs.
        assert_eq!(after_second.matches(HOUR_MARKER_PREFIX).count(), 1);
        assert_eq!(after_second.matches(RUN_MARKER).count(), 2);
        assert_eq!(after_second.matches("<details open>").count(), 1);
    }
}
