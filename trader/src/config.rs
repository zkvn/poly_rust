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

    /// Which market durations each asset trades — `"5m"`, `"15m"`, `"1h-et"`,
    /// `"4h"` (see `marketdata::MarketDuration`). `#[serde(default)]` and a
    /// `["5m"]` fallback in `durations_for` mean a config without this table
    /// (every config before 2026-07-17) behaves exactly as before: 5m only.
    /// See trader/doc/feature_new_markets_2026-07-17.md §4.1.
    #[serde(default)]
    pub market_durations: HashMap<String, Vec<String>>,
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

/// First hit along an ordered key chain — the generalization of `get_asset`'s
/// `asset` → `"default"` lookup that duration-scoped resolution needs (e.g.
/// `"BTC@15m"` → `"default@15m"` → `"BTC"` → `"default"`). `get_asset(m, a)`
/// is exactly `get_chain(m, &[a, "default"])`.
fn get_chain<T: Copy>(map: &HashMap<String, T>, keys: &[&str]) -> Option<T> {
    keys.iter().find_map(|k| map.get(*k)).copied()
}

fn req_chain<T: Copy>(map: &HashMap<String, T>, keys: &[&str], field: &str) -> Result<T> {
    get_chain(map, keys).with_context(|| {
        format!(
            "config field `{field}` missing default and no entry for `{}`",
            keys.first().copied().unwrap_or("?")
        )
    })
}

impl StrategyToml {
    /// Durations `asset` trades: its own `[market_durations]` entry, else the
    /// table's `default`, else `["5m"]` — so configs predating the table (or
    /// not mentioning this asset) mean exactly what they always did. Labels
    /// are returned as-is; callers validate via `MarketDuration::parse` and
    /// must fail loudly on anything unrecognized.
    pub fn durations_for(&self, asset: &str) -> Vec<String> {
        self.market_durations
            .get(asset)
            .or_else(|| self.market_durations.get("default"))
            .cloned()
            .unwrap_or_else(|| vec!["5m".to_string()])
    }

    /// Resolve per-asset params exactly as always — the 5m path. Delegates to
    /// the same chain-based lookup as `resolve_for_duration` with the classic
    /// `asset` → `"default"` chain, which is behaviorally identical to the
    /// pre-2026-07-17 implementation (`get_asset`).
    pub fn resolve(&self, asset: &str) -> Result<AssetParams> {
        self.resolve_keys(asset, &[asset, "default"])
    }

    /// Duration-aware resolution (trader/doc/feature_new_markets_2026-07-17.md
    /// §4.2): any per-asset map may carry `"{ASSET}@{dur}"` / `"default@{dur}"`
    /// override keys, consulted before the plain asset/default keys. For
    /// `"5m"` this **skips the `@` keys entirely and delegates to `resolve`**,
    /// so 5m resolution provably cannot change.
    pub fn resolve_for_duration(&self, asset: &str, duration: &str) -> Result<AssetParams> {
        if duration == "5m" {
            return self.resolve(asset);
        }
        let asset_dur = format!("{asset}@{duration}");
        let default_dur = format!("default@{duration}");
        self.resolve_keys(asset, &[&asset_dur, &default_dur, asset, "default"])
    }

    fn resolve_keys(&self, asset: &str, keys: &[&str]) -> Result<AssetParams> {
        Ok(AssetParams {
            asset: asset.to_string(),
            strategies: keys
                .iter()
                .find_map(|k| self.strategies.get(*k))
                .cloned()
                .unwrap_or_default(),
            enter_when_time_left: req_chain(
                &self.enter_when_time_left,
                keys,
                "enter_when_time_left",
            )? as f64,
            no_enter_when_time_left: self.no_enter_when_time_left as f64,
            reversal: req_chain(&self.reversal, keys, "reversal")?,
            reversal_low_threshold: req_chain(
                &self.reversal_low_threshold,
                keys,
                "reversal_low_threshold",
            )?,
            reversal_start_time: req_chain(&self.reversal_start_time, keys, "reversal_start_time")?
                as f64,
            gamma_poll_delay_secs: req_chain(
                &self.gamma_poll_delay_secs,
                keys,
                "gamma_poll_delay_secs",
            )?,
            gamma_poll_interval_secs: req_chain(
                &self.gamma_poll_interval_secs,
                keys,
                "gamma_poll_interval_secs",
            )?,
            gamma_poll_deadline_secs: req_chain(
                &self.gamma_poll_deadline_secs,
                keys,
                "gamma_poll_deadline_secs",
            )?,
            price_high_rev: req_chain(&self.price_high_rev, keys, "price_high_rev")?,
            delta_pct_rev: req_chain(&self.delta_pct_rev, keys, "delta_pct_rev")?,
            sl_reversal: req_chain(&self.sl_reversal, keys, "sl_reversal")?,
            unwind_pnl_rev: req_chain(&self.unwind_pnl_rev, keys, "unwind_pnl_rev")?,
            sl_pnl_rev: req_chain(&self.sl_pnl_rev, keys, "sl_pnl_rev")?,
            unwind_time_rev: req_chain(&self.unwind_time_rev, keys, "unwind_time_rev")?,
            price_low: req_chain(&self.price_low, keys, "price_low")?,
            price_high: req_chain(&self.price_high, keys, "price_high")?,
            delta_pct_hp: req_chain(&self.delta_pct_hp, keys, "delta_pct_hp")?,
            sl_high_prob: req_chain(&self.sl_high_prob, keys, "sl_high_prob")?,
            unwind_pnl_hp: req_chain(&self.unwind_pnl_hp, keys, "unwind_pnl_hp")?,
            sl_pnl_hp: req_chain(&self.sl_pnl_hp, keys, "sl_pnl_hp")?,
            unwind_time_hp: req_chain(&self.unwind_time_hp, keys, "unwind_time_hp")?,
            // V-shape fields fall back to hardcoded defaults (not `req`) so every
            // pre-v_shape strategy_*.toml still resolves — the canonical
            // v_0.7_0.3_0.7 triple, mid-grid exits, no delta requirement. See
            // trader/doc/plan_v_shape_trader_2026-07-17.md's defaults table.
            v_high1: get_chain(&self.v_high1, keys).unwrap_or(0.70),
            v_low: get_chain(&self.v_low, keys).unwrap_or(0.30),
            v_high2: get_chain(&self.v_high2, keys).unwrap_or(0.70),
            delta_pct_v: get_chain(&self.delta_pct_v, keys).unwrap_or(0.0),
            sl_v_shape: get_chain(&self.sl_v_shape, keys).unwrap_or(0.0),
            sl_pnl_v: get_chain(&self.sl_pnl_v, keys).unwrap_or(0.30),
            unwind_pnl_v: get_chain(&self.unwind_pnl_v, keys).unwrap_or(0.15),
            unwind_time_v: get_chain(&self.unwind_time_v, keys).unwrap_or(25.0),
            halt_rev: req_chain(&self.halt_rev, keys, "halt_rev")?,
            halt_prob: req_chain(&self.halt_prob, keys, "halt_prob")?,
            halt_v: get_chain(&self.halt_v, keys).unwrap_or(1),
            halt_reset_hour_rev: req_chain(&self.halt_reset_hour_rev, keys, "halt_reset_hour_rev")?,
            halt_reset_hour_hp: req_chain(&self.halt_reset_hour_hp, keys, "halt_reset_hour_hp")?,
            halt_reset_hour_v: get_chain(&self.halt_reset_hour_v, keys).unwrap_or(2),
            max_buy_price: self.max_buy_price,
            spread_premium_limit: self.spread_premium_limit,
            spread_discount_limit: self.spread_discount_limit,
            max_price_age_secs: self.max_price_age_secs,
            trade_size_usdc: req_chain(&self.trade_size_usdc, keys, "trade_size_usdc")?,
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
        // strategy_20260717.toml, second same-day update (btc_5mins
        // studies/bt1_overnight_vshape/summary_2026-07-17.md filtered picks —
        // see that file's meta.source): BTC overrides for reversal/
        // delta_pct_rev/reversal_low_threshold/unwind_pnl_rev/sl_pnl_rev/
        // unwind_time_rev.
        assert!(
            (p.reversal - 0.55).abs() < 1e-9,
            "BTC reversal = 0.55 (override)"
        );
        assert!((p.reversal_low_threshold - 0.30).abs() < 1e-9);
        assert!((p.delta_pct_rev - 0.0009).abs() < 1e-9);
        assert_eq!(p.halt_rev, 1);
        assert_eq!(p.halt_reset_hour_rev, 2);
        assert!((p.unwind_pnl_rev - 0.15).abs() < 1e-9);
        assert!((p.sl_pnl_rev - 0.20).abs() < 1e-9);
        assert!((p.unwind_pnl_hp - 0.15).abs() < 1e-9);
        assert!((p.sl_pnl_hp - 0.20).abs() < 1e-9);
        assert!((p.unwind_time_rev - 10.0).abs() < 1e-9);
        assert!((p.unwind_time_hp - 25.0).abs() < 1e-9);
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

    // ── market_durations / @duration overrides (feature_new_markets_2026-07-17.md) ──

    /// The core no-regression guarantee: a config with no `[market_durations]`
    /// table (every config before 2026-07-17) resolves to 5m-only for every
    /// asset, and `resolve_for_duration(_, "5m")` returns exactly what
    /// `resolve` does.
    #[test]
    fn configs_without_market_durations_mean_5m_only() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/config");
        let toml = load_file(&format!("{dir}/strategy_20260713.toml")).expect("load old config");
        assert!(toml.market_durations.is_empty());
        for asset in ["BTC", "ETH", "SOL", "XRP"] {
            assert_eq!(toml.durations_for(asset), vec!["5m".to_string()]);
        }
        let via_resolve = toml.resolve("BTC").expect("resolve");
        let via_duration = toml.resolve_for_duration("BTC", "5m").expect("resolve 5m");
        // Spot-check every strategy-relevant scalar family (Debug formatting
        // covers the whole struct at once).
        assert_eq!(format!("{via_resolve:?}"), format!("{via_duration:?}"));
    }

    #[test]
    fn market_durations_resolution_order() {
        let mut toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        toml.market_durations
            .insert("default".to_string(), vec!["5m".to_string()]);
        toml.market_durations.insert(
            "BTC".to_string(),
            vec!["5m".to_string(), "15m".to_string(), "4h".to_string()],
        );
        assert_eq!(toml.durations_for("BTC"), vec!["5m", "15m", "4h"]);
        assert_eq!(
            toml.durations_for("SOL"),
            vec!["5m"],
            "default-key fallback"
        );
    }

    /// `@duration` keys: `"{ASSET}@{dur}"` beats `"default@{dur}"` beats the
    /// plain asset key beats `"default"` — and plain (non-@) resolution never
    /// sees them.
    #[test]
    fn duration_scoped_overrides_resolve_in_order() {
        let mut toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        let base_btc = toml
            .resolve("BTC")
            .expect("resolve BTC")
            .reversal_start_time;
        toml.reversal_start_time
            .insert("default@15m".to_string(), 400);
        toml.reversal_start_time.insert("BTC@15m".to_string(), 500);

        let p = toml
            .resolve_for_duration("BTC", "15m")
            .expect("resolve BTC 15m");
        assert!(
            (p.reversal_start_time - 500.0).abs() < 1e-9,
            "ASSET@dur wins"
        );
        let p = toml
            .resolve_for_duration("SOL", "15m")
            .expect("resolve SOL 15m");
        assert!(
            (p.reversal_start_time - 400.0).abs() < 1e-9,
            "default@dur next"
        );
        // A field with no @-keys at all falls through to the plain chain.
        let p = toml
            .resolve_for_duration("BTC", "15m")
            .expect("resolve BTC 15m");
        assert!(
            (p.reversal - 0.55).abs() < 1e-9,
            "plain BTC key still applies"
        );
        // Plain resolution (the 5m path) is untouched by the @-keys.
        let p = toml.resolve("BTC").expect("resolve BTC");
        assert!((p.reversal_start_time - base_btc).abs() < 1e-9);
        let p5 = toml.resolve_for_duration("BTC", "5m").expect("resolve 5m");
        assert!((p5.reversal_start_time - base_btc).abs() < 1e-9);
    }

    /// `[strategies]` participates in the same duration scoping, so a duration
    /// can run a different strategy set than 5m does.
    #[test]
    fn strategies_can_differ_per_duration() {
        let mut toml =
            load_latest(concat!(env!("CARGO_MANIFEST_DIR"), "/config")).expect("load config");
        toml.strategies
            .insert("default@4h".to_string(), vec!["high_prob".to_string()]);
        let p = toml.resolve_for_duration("BTC", "4h").expect("resolve 4h");
        assert_eq!(p.strategies, vec!["high_prob"]);
        let p = toml.resolve("BTC").expect("resolve 5m");
        assert_eq!(
            p.strategies,
            vec!["reversal", "high_prob"],
            "5m list unchanged (strategy_20260717.toml second update: BTC runs both)"
        );
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
