//! Load strategy_*.toml config — same rule as Python bot/config._load_strategy_toml.
//!
//! Per-asset dicts use a "default" key; get_asset(map, asset) returns the
//! asset-specific value or falls back to the default.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use glob::glob;
use serde::Deserialize;

/// Raw TOML deserialization struct mirroring strategy_*.toml.
#[derive(Debug, Deserialize)]
pub struct StrategyToml {
    pub assets: Vec<String>,
    pub trade_assets: Vec<String>,
    pub max_buy_price: f64,
    pub no_enter_when_time_left: i64,
    pub spread_premium_limit: f64,
    pub spread_discount_limit: f64,
    pub max_price_age_secs: f64,
    pub har_pup_enabled: bool,
    /// FAK BUY retry knob — process-wide, not per-asset (execution.rs's
    /// `LiveConfig`). Previously the live binary ignored this and used
    /// `LiveConfig::default()`'s hardcoded 2, silently diverging from whatever
    /// this file actually specified.
    pub order_max_retries: u32,

    pub strategies: HashMap<String, Vec<String>>,
    pub halt_rev: HashMap<String, i64>,
    pub halt_prob: HashMap<String, i64>,
    pub halt_reset_hour_rev: HashMap<String, i64>,
    pub halt_reset_hour_hp: HashMap<String, i64>,

    pub delta_pct_rev: HashMap<String, f64>,
    pub delta_pct_hp: HashMap<String, f64>,

    pub reversal: HashMap<String, f64>,
    pub reversal_low_threshold: HashMap<String, f64>,
    pub reversal_start_time: HashMap<String, i64>,
    /// Seconds after a position closes before the Gamma resolution watcher
    /// starts polling — Gamma "usually won't give you anything until 20-60s
    /// after cycle end" (see `trader/doc/incident_DOGE_wrong_result_2026-07-09.md`).
    pub gamma_poll_delay_secs: HashMap<String, f64>,
    /// Retry cadence (seconds) once the watcher starts polling.
    pub gamma_poll_interval_secs: HashMap<String, f64>,
    /// Give up (and report a timeout) this many seconds after the position
    /// closed. Previously reused `reversal_start_time` for this; decoupled
    /// 2026-07-11 (see `trader/doc/plan_gammapi_2026-07-11.md`) so the poll
    /// window can be tuned independently of entry timing.
    pub gamma_poll_deadline_secs: HashMap<String, f64>,
    pub price_high_rev: HashMap<String, f64>,
    pub sl_reversal: HashMap<String, f64>,
    pub unwind_pnl_rev: HashMap<String, f64>,
    pub sl_pnl_rev: HashMap<String, f64>,
    /// Max holding time (seconds) before a still-open reversal position is
    /// force-closed at market, regardless of price — `0.0` disables it. See
    /// `trader/doc/plan_unwind_time_2026-07-08.md`.
    pub unwind_time_rev: HashMap<String, f64>,

    pub enter_when_time_left: HashMap<String, i64>,
    pub price_low: HashMap<String, f64>,
    pub price_high: HashMap<String, f64>,
    pub sl_high_prob: HashMap<String, f64>,
    pub unwind_pnl_hp: HashMap<String, f64>,
    pub sl_pnl_hp: HashMap<String, f64>,
    /// Same as `unwind_time_rev`, for high_prob. `0.0` disables it.
    pub unwind_time_hp: HashMap<String, f64>,

    pub trade_size_usdc: HashMap<String, f64>,

    #[serde(default)]
    pub har_beta: HashMap<String, Vec<f64>>,
    #[serde(default)]
    pub har_nu: HashMap<String, f64>,
}

/// Resolved per-asset parameters (all scalars).
#[derive(Debug, Clone)]
pub struct AssetParams {
    pub asset: String,
    pub strategies: Vec<String>,

    // Entry timing
    pub enter_when_time_left: f64,
    pub no_enter_when_time_left: f64,

    // Reversal
    pub reversal: f64,
    pub reversal_low_threshold: f64,
    pub reversal_start_time: f64,
    pub gamma_poll_delay_secs: f64,
    pub gamma_poll_interval_secs: f64,
    pub gamma_poll_deadline_secs: f64,
    pub price_high_rev: f64,
    pub delta_pct_rev: f64,
    pub sl_reversal: f64,
    pub unwind_pnl_rev: f64,
    pub sl_pnl_rev: f64,
    pub unwind_time_rev: f64,

    // High-prob
    pub price_low: f64,
    pub price_high: f64,
    pub delta_pct_hp: f64,
    pub sl_high_prob: f64,
    pub unwind_pnl_hp: f64,
    pub sl_pnl_hp: f64,
    pub unwind_time_hp: f64,

    // Risk
    pub halt_rev: i64,
    pub halt_prob: i64,
    pub halt_reset_hour_rev: i64,
    pub halt_reset_hour_hp: i64,

    // Gates
    pub max_buy_price: f64,
    pub spread_premium_limit: f64,
    pub spread_discount_limit: f64,
    pub max_price_age_secs: f64,

    // Sizing
    pub trade_size_usdc: f64,
}

pub fn get_asset<T: Copy>(map: &HashMap<String, T>, asset: &str) -> Option<T> {
    map.get(asset).or_else(|| map.get("default")).copied()
}

fn req<T: Copy>(map: &HashMap<String, T>, asset: &str, field: &str) -> Result<T> {
    get_asset(map, asset).with_context(|| {
        format!("config field `{field}` missing default and no entry for `{asset}`")
    })
}

impl StrategyToml {
    pub fn resolve(&self, asset: &str) -> Result<AssetParams> {
        Ok(AssetParams {
            asset: asset.to_string(),
            strategies: self
                .strategies
                .get(asset)
                .or_else(|| self.strategies.get("default"))
                .cloned()
                .unwrap_or_default(),
            enter_when_time_left: req(&self.enter_when_time_left, asset, "enter_when_time_left")?
                as f64,
            no_enter_when_time_left: self.no_enter_when_time_left as f64,
            reversal: req(&self.reversal, asset, "reversal")?,
            reversal_low_threshold: req(
                &self.reversal_low_threshold,
                asset,
                "reversal_low_threshold",
            )?,
            reversal_start_time: req(&self.reversal_start_time, asset, "reversal_start_time")?
                as f64,
            gamma_poll_delay_secs: req(
                &self.gamma_poll_delay_secs,
                asset,
                "gamma_poll_delay_secs",
            )?,
            gamma_poll_interval_secs: req(
                &self.gamma_poll_interval_secs,
                asset,
                "gamma_poll_interval_secs",
            )?,
            gamma_poll_deadline_secs: req(
                &self.gamma_poll_deadline_secs,
                asset,
                "gamma_poll_deadline_secs",
            )?,
            price_high_rev: req(&self.price_high_rev, asset, "price_high_rev")?,
            delta_pct_rev: req(&self.delta_pct_rev, asset, "delta_pct_rev")?,
            sl_reversal: req(&self.sl_reversal, asset, "sl_reversal")?,
            unwind_pnl_rev: req(&self.unwind_pnl_rev, asset, "unwind_pnl_rev")?,
            sl_pnl_rev: req(&self.sl_pnl_rev, asset, "sl_pnl_rev")?,
            unwind_time_rev: req(&self.unwind_time_rev, asset, "unwind_time_rev")?,
            price_low: req(&self.price_low, asset, "price_low")?,
            price_high: req(&self.price_high, asset, "price_high")?,
            delta_pct_hp: req(&self.delta_pct_hp, asset, "delta_pct_hp")?,
            sl_high_prob: req(&self.sl_high_prob, asset, "sl_high_prob")?,
            unwind_pnl_hp: req(&self.unwind_pnl_hp, asset, "unwind_pnl_hp")?,
            sl_pnl_hp: req(&self.sl_pnl_hp, asset, "sl_pnl_hp")?,
            unwind_time_hp: req(&self.unwind_time_hp, asset, "unwind_time_hp")?,
            halt_rev: req(&self.halt_rev, asset, "halt_rev")?,
            halt_prob: req(&self.halt_prob, asset, "halt_prob")?,
            halt_reset_hour_rev: req(&self.halt_reset_hour_rev, asset, "halt_reset_hour_rev")?,
            halt_reset_hour_hp: req(&self.halt_reset_hour_hp, asset, "halt_reset_hour_hp")?,
            max_buy_price: self.max_buy_price,
            spread_premium_limit: self.spread_premium_limit,
            spread_discount_limit: self.spread_discount_limit,
            max_price_age_secs: self.max_price_age_secs,
            trade_size_usdc: req(&self.trade_size_usdc, asset, "trade_size_usdc")?,
        })
    }
}

/// Load the latest strategy_*.toml from config_dir (same glob+sort as Python).
///
/// `config_dir` is conventionally `btc_5mins/config` (see README "Strategy
/// config" section) — but as of `strategy_20260705.toml`, that directory
/// holds a symlink to this crate's own `config/`, which is the real,
/// git-tracked copy. `read_to_string` follows symlinks transparently, so
/// this function doesn't need to know or care which side is real.
pub fn load_latest(config_dir: &str) -> Result<StrategyToml> {
    let pattern = format!("{}/strategy_*.toml", config_dir.trim_end_matches('/'));
    let mut paths: Vec<PathBuf> = glob(&pattern)
        .with_context(|| format!("glob {pattern}"))?
        .flatten()
        .collect();
    if paths.is_empty() {
        bail!("no strategy_*.toml found in {config_dir}");
    }
    paths.sort();
    let latest = paths.pop().unwrap();
    let raw = std::fs::read_to_string(&latest).with_context(|| format!("read {latest:?}"))?;
    toml::from_str(&raw).with_context(|| format!("parse {latest:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_resolve_btc() {
        let toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        let p = toml.resolve("BTC").expect("resolve BTC");
        // strategy_20260708.toml full-history top-win-rate recalibration — BTC has no
        // per-asset override for any of these (updated 2026-07-09 test drift fix; the
        // previous hardcoded values dated back to strategy_20260705.toml and silently
        // stopped matching when 20260708's recalibration landed the same day, per the
        // README TODO entry), so every field below resolves to its "default" entry.
        assert!(
            (p.reversal - 0.55).abs() < 1e-9,
            "BTC reversal = 0.55 (default)"
        );
        assert!((p.reversal_low_threshold - 0.20).abs() < 1e-9);
        assert!((p.delta_pct_rev - 0.0010).abs() < 1e-9);
        assert_eq!(p.halt_rev, 2);
        assert_eq!(p.halt_reset_hour_rev, 2);
        assert!((p.unwind_pnl_rev - 0.15).abs() < 1e-9);
        assert!((p.sl_pnl_rev - 0.40).abs() < 1e-9);
        assert!((p.unwind_pnl_hp - 0.07).abs() < 1e-9);
        assert!((p.sl_pnl_hp - 0.35).abs() < 1e-9);
        // unwind_time_rev has per-asset overrides (ETH=28.0, DOGE=30.0); BTC uses
        // the 26.0 default. unwind_time_hp is flat 30.0 for all assets.
        assert!((p.unwind_time_rev - 26.0).abs() < 1e-9);
        assert!((p.unwind_time_hp - 30.0).abs() < 1e-9);
        // gamma_poll_delay_secs/gamma_poll_interval_secs added 2026-07-09 (see
        // README's Gamma-halt section) — flat defaults, no per-asset override yet.
        // gamma_poll_deadline_secs added 2026-07-11 (extended window, decoupled
        // from reversal_start_time — see trader/doc/plan_gammapi_2026-07-11.md).
        assert!((p.gamma_poll_delay_secs - 60.0).abs() < 1e-9);
        assert!((p.gamma_poll_interval_secs - 20.0).abs() < 1e-9);
        assert!((p.gamma_poll_deadline_secs - 600.0).abs() < 1e-9);
    }

    #[test]
    fn unwind_time_falls_back_to_default_and_resolves_asset_override() {
        let mut toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        // Default fallback (no BTC-specific entry in the real config — updated
        // 2026-07-09, BTC's default changed from 30.0 to 26.0 in strategy_20260708.toml).
        let p = toml.resolve("BTC").expect("resolve BTC");
        assert!((p.unwind_time_rev - 26.0).abs() < 1e-9);
        // Asset-specific override takes priority over default when present.
        toml.unwind_time_rev.insert("ETH".to_string(), 12.0);
        let p = toml.resolve("ETH").expect("resolve ETH");
        assert!((p.unwind_time_rev - 12.0).abs() < 1e-9);
        // 0.0 is a valid, meaningful value (disabled) — not treated as missing.
        toml.unwind_time_hp.insert("ETH".to_string(), 0.0);
        let p = toml.resolve("ETH").expect("resolve ETH");
        assert!((p.unwind_time_hp - 0.0).abs() < 1e-9);
    }

    #[test]
    fn default_fallback() {
        let toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        // BTC uses default delta_pct_rev (updated 2026-07-09: ETH gained its own
        // override, 0.0008, in strategy_20260708.toml, so it no longer falls back —
        // BTC is now the asset that demonstrates the fallback path).
        let p = toml.resolve("BTC").expect("resolve BTC");
        assert!((p.delta_pct_rev - 0.0010).abs() < 1e-9);
    }
}
