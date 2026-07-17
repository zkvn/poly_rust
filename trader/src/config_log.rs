// Append-only JSONL config snapshot log — schema-identical to Python's
// bot/config_log.py, so the existing Python recon stack (snapshot_to_bt_overrides)
// keeps working unmodified regardless of which side (Python or Rust) wrote a line.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, Write as _};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{FixedOffset, TimeZone as _, Utc};
use serde::{Deserialize, Serialize};

use crate::config::StrategyToml;

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub ts: f64,
    pub hkt: String,
    pub asset: String,
    pub event: String,
    pub assets: String,
    pub trade_assets: String,
    pub strategies: Vec<String>,
    pub trade_size: HashMap<String, f64>,
    pub halt_prob: HashMap<String, i64>,
    pub halt_rev: HashMap<String, i64>,
    pub halt_reset_hour_rev: HashMap<String, i64>,
    pub halt_reset_hour_hp: HashMap<String, i64>,
    pub band: HashMap<String, [f64; 2]>,
    pub max_buy: f64,
    pub enter_when: HashMap<String, i64>,
    pub no_enter: i64,
    pub delta_pct_hp: HashMap<String, f64>,
    pub delta_pct_rev: HashMap<String, f64>,
    pub reversal: HashMap<String, f64>,
    pub rev_low: HashMap<String, f64>,
    pub rev_start: HashMap<String, i64>,
    pub price_high_rev: HashMap<String, f64>,
    pub sl_hp: HashMap<String, f64>,
    pub sl_rev: HashMap<String, f64>,
    pub unwind_pnl_rev: HashMap<String, f64>,
    pub sl_pnl_rev: HashMap<String, f64>,
    #[serde(default)]
    pub unwind_pnl_hp: HashMap<String, f64>,
    #[serde(default)]
    pub sl_pnl_hp: HashMap<String, f64>,
    // v_shape (2026-07-17, trader/doc/plan_v_shape_trader_2026-07-17.md) — same
    // wholesale per-asset-map copies as the rev/hp fields above.
    #[serde(default)]
    pub v_high1: HashMap<String, f64>,
    #[serde(default)]
    pub v_low: HashMap<String, f64>,
    #[serde(default)]
    pub v_high2: HashMap<String, f64>,
    #[serde(default)]
    pub delta_pct_v: HashMap<String, f64>,
    #[serde(default)]
    pub sl_v_shape: HashMap<String, f64>,
    #[serde(default)]
    pub sl_pnl_v: HashMap<String, f64>,
    #[serde(default)]
    pub unwind_pnl_v: HashMap<String, f64>,
    #[serde(default)]
    pub unwind_time_v: HashMap<String, f64>,
    #[serde(default)]
    pub halt_v: HashMap<String, i64>,
    #[serde(default)]
    pub halt_reset_hour_v: HashMap<String, i64>,
}

/// Build a snapshot for `asset` from the full multi-asset TOML (mirrors
/// `write_snapshot` in bot/config_log.py — every per-asset dict is copied
/// wholesale, not resolved to a single scalar, so the Python reader's
/// `_resolve(value, asset, default)` fallback logic keeps working).
pub fn build_snapshot(
    toml: &StrategyToml,
    asset: &str,
    event: &str,
    strategies: &[String],
) -> ConfigSnapshot {
    let ts = Utc::now().timestamp() as f64 + (Utc::now().timestamp_subsec_millis() as f64 / 1000.0);
    let dt = hkt().timestamp_opt(ts as i64, 0).unwrap();
    let hkt_str = format!("{} HKT", dt.format("%Y-%m-%d %H:%M:%S"));

    let mut band: HashMap<String, [f64; 2]> = HashMap::new();
    let keys: std::collections::BTreeSet<&String> = toml
        .price_low
        .keys()
        .chain(toml.price_high.keys())
        .collect();
    for k in keys {
        let low = *toml
            .price_low
            .get(k)
            .unwrap_or_else(|| &toml.price_low["default"]);
        let high = *toml
            .price_high
            .get(k)
            .unwrap_or_else(|| &toml.price_high["default"]);
        band.insert(k.clone(), [low, high]);
    }

    ConfigSnapshot {
        ts,
        hkt: hkt_str,
        asset: asset.to_string(),
        event: event.to_string(),
        assets: toml.assets.join(","),
        trade_assets: toml.trade_assets.join(","),
        strategies: strategies.to_vec(),
        trade_size: toml.trade_size_usdc.clone(),
        halt_prob: toml.halt_prob.clone(),
        halt_rev: toml.halt_rev.clone(),
        halt_reset_hour_rev: toml.halt_reset_hour_rev.clone(),
        halt_reset_hour_hp: toml.halt_reset_hour_hp.clone(),
        band,
        max_buy: toml.max_buy_price,
        enter_when: toml.enter_when_time_left.clone(),
        no_enter: toml.no_enter_when_time_left,
        delta_pct_hp: toml.delta_pct_hp.clone(),
        delta_pct_rev: toml.delta_pct_rev.clone(),
        reversal: toml.reversal.clone(),
        rev_low: toml.reversal_low_threshold.clone(),
        rev_start: toml.reversal_start_time.clone(),
        price_high_rev: toml.price_high_rev.clone(),
        sl_hp: toml.sl_high_prob.clone(),
        sl_rev: toml.sl_reversal.clone(),
        unwind_pnl_rev: toml.unwind_pnl_rev.clone(),
        sl_pnl_rev: toml.sl_pnl_rev.clone(),
        unwind_pnl_hp: toml.unwind_pnl_hp.clone(),
        sl_pnl_hp: toml.sl_pnl_hp.clone(),
        v_high1: toml.v_high1.clone(),
        v_low: toml.v_low.clone(),
        v_high2: toml.v_high2.clone(),
        delta_pct_v: toml.delta_pct_v.clone(),
        sl_v_shape: toml.sl_v_shape.clone(),
        sl_pnl_v: toml.sl_pnl_v.clone(),
        unwind_pnl_v: toml.unwind_pnl_v.clone(),
        unwind_time_v: toml.unwind_time_v.clone(),
        halt_v: toml.halt_v.clone(),
        halt_reset_hour_v: toml.halt_reset_hour_v.clone(),
    }
}

/// Append one JSONL line to `{log_dir}/config.log` (matches Python's default path).
pub fn write_snapshot(
    toml: &StrategyToml,
    asset: &str,
    event: &str,
    strategies: &[String],
    log_dir: &str,
) -> Result<()> {
    let snap = build_snapshot(toml, asset, event, strategies);
    std::fs::create_dir_all(log_dir).with_context(|| format!("create {log_dir}"))?;
    let path = Path::new(log_dir).join("config.log");
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {path:?}"))?;
    let line = serde_json::to_string(&snap).context("serialize snapshot")?;
    writeln!(f, "{line}").with_context(|| format!("write {path:?}"))?;
    Ok(())
}

/// Read all valid entries, sorted by ts ascending (matches Python read_all_snapshots).
pub fn read_all_snapshots(log_path: &str) -> Result<Vec<ConfigSnapshot>> {
    if !Path::new(log_path).exists() {
        return Ok(vec![]);
    }
    let file = std::fs::File::open(log_path).with_context(|| format!("open {log_path}"))?;
    let reader = std::io::BufReader::new(file);
    let mut entries: Vec<ConfigSnapshot> = Vec::new();
    for line in reader.lines() {
        let line = line.context("read line")?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<ConfigSnapshot>(line) {
            entries.push(entry);
        }
    }
    entries.sort_by(|a, b| a.ts.partial_cmp(&b.ts).unwrap());
    Ok(entries)
}

/// Latest snapshot for `asset` where entry.ts <= `ts` (matches Python read_latest_snapshot).
pub fn read_latest_snapshot(
    log_path: &str,
    asset: &str,
    ts: f64,
) -> Result<Option<ConfigSnapshot>> {
    let mut result = None;
    for entry in read_all_snapshots(log_path)? {
        if entry.asset == asset && entry.ts <= ts {
            result = Some(entry);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load_latest;

    #[test]
    fn write_and_read_roundtrip() {
        let toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        let dir = std::env::temp_dir().join(format!("config_log_test_{}", std::process::id()));
        let dir_str = dir.to_str().unwrap().to_string();

        write_snapshot(&toml, "BTC", "startup", &["reversal".to_string()], &dir_str)
            .expect("write snapshot");

        let entries = read_all_snapshots(&format!("{dir_str}/config.log")).expect("read");
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.asset, "BTC");
        assert_eq!(e.event, "startup");
        assert_eq!(e.strategies, vec!["reversal".to_string()]);
        // BTC has explicit `reversal`/`delta_pct_rev` overrides in strategy_20260716.toml
        // (btc_5mins studies/unwind_safely/summary_2026-07-16_low03_high055_halt1_dailywf.md
        // candidate combo), so both resolve to BTC's own override value here, not "default".
        assert!((e.reversal.get("BTC").unwrap_or(&e.reversal["default"]) - 0.55).abs() < 1e-9);
        assert!(
            (e.delta_pct_rev
                .get("BTC")
                .unwrap_or(&e.delta_pct_rev["default"])
                - 0.0004)
                .abs()
                < 1e-9
        );
        assert_eq!(*e.halt_rev.get("BTC").unwrap_or(&e.halt_rev["default"]), 1);
        assert!(e.hkt.ends_with(" HKT"));
        assert!(e.assets.contains("BTC"));
        // trade_assets scoped to BTC/SOL/DOGE (2026-07-16 update, dropped BNB in
        // favor of DOGE) — see strategy_20260716.toml's meta comment. `assets`
        // (monitored/configured) still covers all 6, `trade_assets` (actually
        // traded) is the narrower set.
        assert!(e.trade_assets.contains("DOGE"));
        assert!(e.trade_assets.contains("BTC"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn latest_snapshot_filters_by_ts_and_asset() {
        let toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        let dir = std::env::temp_dir().join(format!("config_log_test2_{}", std::process::id()));
        let dir_str = dir.to_str().unwrap().to_string();

        write_snapshot(&toml, "BTC", "startup", &["reversal".to_string()], &dir_str).unwrap();
        write_snapshot(&toml, "ETH", "startup", &["reversal".to_string()], &dir_str).unwrap();

        let log_path = format!("{dir_str}/config.log");
        let far_future = Utc::now().timestamp() as f64 + 3600.0;
        let latest = read_latest_snapshot(&log_path, "BTC", far_future).unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().asset, "BTC");

        let none = read_latest_snapshot(&log_path, "BTC", 0.0).unwrap();
        assert!(none.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
