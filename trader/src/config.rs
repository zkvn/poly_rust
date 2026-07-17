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

    // V-shape strategy (own `_v` set, added 2026-07-17 — see
    // trader/doc/plan_v_shape_trader_2026-07-17.md). All `#[serde(default)]`
    // so every pre-existing strategy_*.toml (pinned by backtest --config-file)
    // still parses; `resolve()` supplies the documented defaults when a map is
    // empty.
    #[serde(default)]
    pub v_high1: HashMap<String, f64>,
    #[serde(default)]
    pub v_low: HashMap<String, f64>,
    #[serde(default)]
    pub v_high2: HashMap<String, f64>,
    /// Defaults to 0.0 = disabled: v_shape is pure CLOB price action, no
    /// Binance-direction requirement (siglab v_shape philosophy).
    #[serde(default)]
    pub delta_pct_v: HashMap<String, f64>,
    /// Absolute SL floor, 0.0 = disabled (mirrors `sl_reversal`'s shape).
    #[serde(default)]
    pub sl_v_shape: HashMap<String, f64>,
    #[serde(default)]
    pub sl_pnl_v: HashMap<String, f64>,
    #[serde(default)]
    pub unwind_pnl_v: HashMap<String, f64>,
    /// Same as `unwind_time_rev`, for v_shape. `0.0` disables it.
    #[serde(default)]
    pub unwind_time_v: HashMap<String, f64>,
    #[serde(default)]
    pub halt_v: HashMap<String, i64>,
    #[serde(default)]
    pub halt_reset_hour_v: HashMap<String, i64>,

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

    // V-shape (defaults documented in trader/doc/plan_v_shape_trader_2026-07-17.md)
    pub v_high1: f64,
    pub v_low: f64,
    pub v_high2: f64,
    pub delta_pct_v: f64,
    pub sl_v_shape: f64,
    pub sl_pnl_v: f64,
    pub unwind_pnl_v: f64,
    pub unwind_time_v: f64,

    // Risk
    pub halt_rev: i64,
    pub halt_prob: i64,
    pub halt_v: i64,
    pub halt_reset_hour_rev: i64,
    pub halt_reset_hour_hp: i64,
    pub halt_reset_hour_v: i64,

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
            // V-shape fields fall back to hardcoded defaults (not `req`) so every
            // pre-v_shape strategy_*.toml still resolves — the canonical
            // v_0.7_0.3_0.7 triple, mid-grid exits, no delta requirement. See
            // trader/doc/plan_v_shape_trader_2026-07-17.md's defaults table.
            v_high1: get_asset(&self.v_high1, asset).unwrap_or(0.70),
            v_low: get_asset(&self.v_low, asset).unwrap_or(0.30),
            v_high2: get_asset(&self.v_high2, asset).unwrap_or(0.70),
            delta_pct_v: get_asset(&self.delta_pct_v, asset).unwrap_or(0.0),
            sl_v_shape: get_asset(&self.sl_v_shape, asset).unwrap_or(0.0),
            sl_pnl_v: get_asset(&self.sl_pnl_v, asset).unwrap_or(0.30),
            unwind_pnl_v: get_asset(&self.unwind_pnl_v, asset).unwrap_or(0.15),
            unwind_time_v: get_asset(&self.unwind_time_v, asset).unwrap_or(25.0),
            halt_rev: req(&self.halt_rev, asset, "halt_rev")?,
            halt_prob: req(&self.halt_prob, asset, "halt_prob")?,
            halt_v: get_asset(&self.halt_v, asset).unwrap_or(1),
            halt_reset_hour_rev: req(&self.halt_reset_hour_rev, asset, "halt_reset_hour_rev")?,
            halt_reset_hour_hp: req(&self.halt_reset_hour_hp, asset, "halt_reset_hour_hp")?,
            halt_reset_hour_v: get_asset(&self.halt_reset_hour_v, asset).unwrap_or(2),
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
    load_file(latest.to_str().unwrap_or_default())
}

/// Load one exact strategy_*.toml by path, bypassing `load_latest`'s
/// directory-glob "newest file wins" selection entirely. Used by
/// `backtest`'s `--config-file` to pin a specific historical config instead
/// of always resolving to whatever is lexicographically latest in the
/// directory right now — closes the daily-recon "always reconciles against
/// today's config, never the config that was actually live at trade time"
/// gap (README `## TODO`, flagged 2026-07-10; see
/// `trader/doc/audit_recon_2026-07-15.md`).
pub fn load_file(path: &str) -> Result<StrategyToml> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    toml::from_str(&raw).with_context(|| format!("parse {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_resolve_btc() {
        let toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        let p = toml.resolve("BTC").expect("resolve BTC");
        // strategy_20260716.toml (btc_5mins studies/unwind_safely/
        // summary_2026-07-16_low03_high055_halt1_dailywf.md candidate combo).
        // BTC now has explicit overrides for reversal/delta_pct_rev/
        // reversal_low_threshold/unwind_pnl_rev; unwind_time_rev's own BTC
        // override was removed (BTC now shares the 25.0 default with SOL/DOGE).
        assert!(
            (p.reversal - 0.55).abs() < 1e-9,
            "BTC reversal = 0.55 (override)"
        );
        assert!((p.reversal_low_threshold - 0.30).abs() < 1e-9);
        assert!((p.delta_pct_rev - 0.0004).abs() < 1e-9);
        assert_eq!(p.halt_rev, 1);
        assert_eq!(p.halt_reset_hour_rev, 2);
        assert!((p.unwind_pnl_rev - 0.15).abs() < 1e-9);
        assert!((p.sl_pnl_rev - 0.30).abs() < 1e-9);
        assert!((p.unwind_pnl_hp - 0.07).abs() < 1e-9);
        assert!((p.sl_pnl_hp - 0.35).abs() < 1e-9);
        // unwind_time_rev has one remaining per-asset override (XRP=20.0); BTC
        // now falls back to the 25.0 default. unwind_time_hp is flat 30.0 for
        // all assets.
        assert!((p.unwind_time_rev - 25.0).abs() < 1e-9);
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
        // Default fallback (no SOL-specific entry in the real config — updated
        // 2026-07-15, strategy_20260715.toml's same-day update gave BTC/XRP their
        // own unwind_time_rev overrides, so SOL is now the asset that falls back
        // to the 25.0 default).
        let p = toml.resolve("SOL").expect("resolve SOL");
        assert!((p.unwind_time_rev - 25.0).abs() < 1e-9);
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
        // XRP uses default delta_pct_rev (updated 2026-07-16: SOL gained its own
        // override, 0.0004, in strategy_20260716.toml, so it no longer falls
        // back — XRP is now the asset that demonstrates the fallback path).
        let p = toml.resolve("XRP").expect("resolve XRP");
        assert!((p.delta_pct_rev - 0.0003).abs() < 1e-9);
    }

    #[test]
    fn load_file_reads_the_exact_file_given_not_the_latest() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/config");
        // strategy_20260713.toml exists but is not the lexicographically-latest
        // file in this directory — load_latest would skip past it. load_file
        // must load exactly what's asked for regardless.
        let pinned = load_file(&format!("{dir}/strategy_20260713.toml")).expect("load pinned");
        let latest = load_latest(dir).expect("load latest");
        assert_ne!(
            pinned.trade_assets, latest.trade_assets,
            "sanity: the pinned historical file must differ from today's latest \
             (strategy_20260713.toml traded ETH too; the latest config narrowed \
             trade_assets to BTC+SOL+DOGE)"
        );
        assert!(pinned.trade_assets.contains(&"ETH".to_string()));
    }

    #[test]
    fn load_file_errors_on_missing_path() {
        assert!(load_file("/nonexistent/strategy_99999999.toml").is_err());
    }

    /// Pre-v_shape configs (no `[v_*]` sections at all) must still parse and resolve,
    /// with every v field landing on its documented default — this is what keeps
    /// backtest `--config-file` pins of historical configs working (see
    /// trader/doc/plan_v_shape_trader_2026-07-17.md).
    #[test]
    fn v_shape_fields_default_when_absent_from_old_configs() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/config");
        let toml = load_file(&format!("{dir}/strategy_20260713.toml")).expect("load old config");
        let p = toml.resolve("BTC").expect("resolve BTC");
        assert!((p.v_high1 - 0.70).abs() < 1e-9);
        assert!((p.v_low - 0.30).abs() < 1e-9);
        assert!((p.v_high2 - 0.70).abs() < 1e-9);
        assert!((p.delta_pct_v - 0.0).abs() < 1e-9, "no delta requirement");
        assert!((p.sl_v_shape - 0.0).abs() < 1e-9, "absolute SL disabled");
        assert!((p.sl_pnl_v - 0.30).abs() < 1e-9);
        assert!((p.unwind_pnl_v - 0.15).abs() < 1e-9);
        assert!((p.unwind_time_v - 25.0).abs() < 1e-9);
        assert_eq!(p.halt_v, 1);
        assert_eq!(p.halt_reset_hour_v, 2);
    }

    /// `[v_*]` sections present in the TOML must win over the hardcoded defaults,
    /// including per-asset overrides via the same `get_asset` default-key fallback
    /// every other per-asset map uses.
    #[test]
    fn v_shape_fields_resolve_overrides_when_present() {
        let mut toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        toml.v_high1.insert("default".to_string(), 0.65);
        toml.v_high2.insert("default".to_string(), 0.55);
        toml.v_high2.insert("ETH".to_string(), 0.60);
        toml.unwind_pnl_v.insert("default".to_string(), 0.05);
        toml.halt_v.insert("default".to_string(), 3);
        let p = toml.resolve("ETH").expect("resolve ETH");
        assert!((p.v_high1 - 0.65).abs() < 1e-9, "default-key override");
        assert!((p.v_high2 - 0.60).abs() < 1e-9, "asset-specific override");
        assert!((p.unwind_pnl_v - 0.05).abs() < 1e-9);
        assert_eq!(p.halt_v, 3);
        let p = toml.resolve("BTC").expect("resolve BTC");
        assert!(
            (p.v_high2 - 0.55).abs() < 1e-9,
            "BTC falls back to default key"
        );
    }
}
