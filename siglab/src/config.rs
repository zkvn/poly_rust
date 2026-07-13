//! siglab's own, fully standalone config schema.
//!
//! Deliberately NOT `trader::config::StrategyToml` — that schema carries fields
//! (`gamma_poll_*`, `halt_reset_hour_*`, `har_beta`/`har_nu`) that belong to the *live*
//! bot's resolution-watcher/HAR-enrichment/halt-accounting concerns, none of which this
//! paper-testing harness has. Reusing it would mean every siglab config file has to fill
//! in a pile of fields that mean nothing here just to satisfy `Deserialize` — the opposite
//! of "clean and standalone." This module defines the minimal shape `trader::machine::
//! Machine` actually reads (verified against `Machine::new_reversal`/`new_high_prob` in
//! `trader/src/machine.rs`) and hand-builds a `trader::config::AssetParams` from it, with
//! the unread live-only fields on that struct filled with inert zeros.
//!
//! This file never reads or writes anything under `../trader/config` or `../price_feed` —
//! siglab's config directory is the only thing it touches.

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use trader::config::AssetParams;

#[derive(Debug, Deserialize)]
pub struct SiglabConfig {
    #[serde(rename = "market")]
    pub markets: Vec<MarketCfg>,
    /// `polymarket.com/crypto/hourly`-style markets — a genuinely different slug format
    /// (ET calendar hour, full coin name) from the slot-based `markets` above, so they get
    /// their own config section rather than overloading `MarketCfg` with an optional field
    /// that only sometimes applies. See `rotation.rs`'s doc comment for why.
    #[serde(rename = "hourly_market", default)]
    pub hourly_markets: Vec<HourlyMarketCfg>,
    #[serde(rename = "variant")]
    pub variants: Vec<VariantCfg>,
}

/// One rotating market to subscribe to: `{asset}-updown-{suffix}-{slot}` on a
/// `period_secs`-second cycle. Matches `trader::marketdata::make_slug`/`current_slot`.
#[derive(Debug, Deserialize, Clone)]
pub struct MarketCfg {
    pub asset: String,
    pub suffix: String,
    pub period_secs: u64,
}

/// One ET-calendar-hour market — `asset` is used for variant matching/Binance reference
/// (same as `MarketCfg`), `coin_name` is the full name Polymarket's hourly slug uses
/// (`"bitcoin"`, not `"BTC"`).
#[derive(Debug, Deserialize, Clone)]
pub struct HourlyMarketCfg {
    pub asset: String,
    pub coin_name: String,
}

/// One named strategy parameter set, applied to every asset in `assets` (or every
/// configured market's asset, if `assets` is empty). Unlike `trader::config::StrategyToml`,
/// there is no per-asset override map — a variant's params are the same across every asset
/// it runs on. That's a deliberate simplification for the Phase 0 harness (proving
/// discovery/staleness/scale/output, not exact parity with the live bot's per-asset
/// calibration); revisit if/when real variant fan-out needs per-asset values.
#[derive(Debug, Deserialize, Clone)]
pub struct VariantCfg {
    pub id: String,
    pub strategy: String, // "reversal" | "high_prob"
    #[serde(default)]
    pub assets: Vec<String>, // empty = applies to every configured market's asset

    // Shared gate params (every Machine needs all of these regardless of strategy —
    // check_gates reads delta_pct_rev/delta_pct_hp/price_high_rev unconditionally and
    // picks the relevant one by the *intent's* entry_type at evaluation time).
    pub no_enter_when_time_left: f64,
    pub max_buy_price: f64,
    pub spread_premium_limit: f64,
    pub spread_discount_limit: f64,
    pub max_price_age_secs: f64,
    pub delta_pct_rev: f64,
    pub delta_pct_hp: f64,
    pub price_high_rev: f64,
    pub trade_size_usdc: f64,

    // Reversal-only (required when strategy = "reversal")
    pub reversal: Option<f64>,
    pub reversal_low_threshold: Option<f64>,
    pub reversal_start_time: Option<f64>,
    pub sl_reversal: Option<f64>,
    pub unwind_pnl_rev: Option<f64>,
    pub sl_pnl_rev: Option<f64>,
    pub unwind_time_rev: Option<f64>,

    // High-prob-only (required when strategy = "high_prob")
    pub price_low: Option<f64>,
    pub price_high: Option<f64>,
    pub enter_when_time_left: Option<f64>,
    pub sl_high_prob: Option<f64>,
    pub unwind_pnl_hp: Option<f64>,
    pub sl_pnl_hp: Option<f64>,
    pub unwind_time_hp: Option<f64>,
}

impl VariantCfg {
    /// True if this variant applies to `asset` (empty `assets` = applies to all).
    pub fn applies_to(&self, asset: &str) -> bool {
        self.assets.is_empty() || self.assets.iter().any(|a| a == asset)
    }

    fn req(&self, field: Option<f64>, name: &str) -> Result<f64> {
        field.with_context(|| {
            format!(
                "variant `{}` (strategy={}): missing required field `{}`",
                self.id, self.strategy, name
            )
        })
    }

    /// Build a `trader::config::AssetParams` for this variant on `asset`. Fields that
    /// `Machine::new_reversal`/`new_high_prob` never read (halt_*, gamma_poll_*) are set to
    /// 0 — confirmed unread by grepping their construction in trader/src/machine.rs; they
    /// only matter to worker.rs's live halt-tracking, which siglab never runs.
    pub fn to_asset_params(&self, asset: &str) -> Result<AssetParams> {
        let (
            reversal,
            reversal_low_threshold,
            reversal_start_time,
            sl_reversal,
            unwind_pnl_rev,
            sl_pnl_rev,
            unwind_time_rev,
        ) = match self.strategy.as_str() {
            "reversal" => (
                self.req(self.reversal, "reversal")?,
                self.req(self.reversal_low_threshold, "reversal_low_threshold")?,
                self.req(self.reversal_start_time, "reversal_start_time")?,
                self.req(self.sl_reversal, "sl_reversal")?,
                self.req(self.unwind_pnl_rev, "unwind_pnl_rev")?,
                self.req(self.sl_pnl_rev, "sl_pnl_rev")?,
                self.req(self.unwind_time_rev, "unwind_time_rev")?,
            ),
            _ => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        };

        let (
            price_low,
            price_high,
            enter_when_time_left,
            sl_high_prob,
            unwind_pnl_hp,
            sl_pnl_hp,
            unwind_time_hp,
        ) = match self.strategy.as_str() {
            "high_prob" => (
                self.req(self.price_low, "price_low")?,
                self.req(self.price_high, "price_high")?,
                self.req(self.enter_when_time_left, "enter_when_time_left")?,
                self.req(self.sl_high_prob, "sl_high_prob")?,
                self.req(self.unwind_pnl_hp, "unwind_pnl_hp")?,
                self.req(self.sl_pnl_hp, "sl_pnl_hp")?,
                self.req(self.unwind_time_hp, "unwind_time_hp")?,
            ),
            _ => (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        };

        Ok(AssetParams {
            asset: asset.to_string(),
            strategies: vec![self.strategy.clone()],
            enter_when_time_left,
            no_enter_when_time_left: self.no_enter_when_time_left,
            reversal,
            reversal_low_threshold,
            reversal_start_time,
            gamma_poll_delay_secs: 0.0,
            gamma_poll_interval_secs: 0.0,
            gamma_poll_deadline_secs: 0.0,
            price_high_rev: self.price_high_rev,
            delta_pct_rev: self.delta_pct_rev,
            sl_reversal,
            unwind_pnl_rev,
            sl_pnl_rev,
            unwind_time_rev,
            price_low,
            price_high,
            delta_pct_hp: self.delta_pct_hp,
            sl_high_prob,
            unwind_pnl_hp,
            sl_pnl_hp,
            unwind_time_hp,
            halt_rev: 0,
            halt_prob: 0,
            halt_reset_hour_rev: 0,
            halt_reset_hour_hp: 0,
            max_buy_price: self.max_buy_price,
            spread_premium_limit: self.spread_premium_limit,
            spread_discount_limit: self.spread_discount_limit,
            max_price_age_secs: self.max_price_age_secs,
            trade_size_usdc: self.trade_size_usdc,
        })
    }
}

/// Weather city list — deliberately its own tiny config type/file (`weather_cities.toml`),
/// not folded into `SiglabConfig` above, since it has nothing to do with crypto
/// markets/variants and keeping them separate means editing one never risks a typo
/// breaking the other.
#[derive(Debug, Deserialize)]
pub struct WeatherConfig {
    pub cities: Vec<String>,
}

pub fn load_weather(path: &Path) -> Result<WeatherConfig> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
    let cfg: WeatherConfig = toml::from_str(&raw).with_context(|| format!("parse {path:?}"))?;
    if cfg.cities.is_empty() {
        bail!("{path:?}: no cities configured");
    }
    Ok(cfg)
}

pub fn load(path: &Path) -> Result<SiglabConfig> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path:?}"))?;
    let cfg: SiglabConfig = toml::from_str(&raw).with_context(|| format!("parse {path:?}"))?;
    if cfg.markets.is_empty() {
        bail!("{path:?}: no [[market]] entries");
    }
    if cfg.variants.is_empty() {
        bail!("{path:?}: no [[variant]] entries");
    }
    for v in &cfg.variants {
        if v.strategy != "reversal" && v.strategy != "high_prob" {
            bail!(
                "variant `{}`: strategy must be \"reversal\" or \"high_prob\", got {:?}",
                v.id,
                v.strategy
            );
        }
        // Fail fast on missing required fields at load time rather than at first tick.
        v.to_asset_params(
            &v.assets
                .first()
                .cloned()
                .unwrap_or_else(|| "BTC".to_string()),
        )
        .with_context(|| format!("variant `{}` failed validation", v.id))?;
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let toml = r#"
[[market]]
asset = "BTC"
suffix = "5m"
period_secs = 300

[[variant]]
id = "reversal_1"
strategy = "reversal"
no_enter_when_time_left = 10
max_buy_price = 0.95
spread_premium_limit = 1.05
spread_discount_limit = 0.95
max_price_age_secs = 2.0
delta_pct_rev = 0.0010
delta_pct_hp = 0.0004
price_high_rev = 0.90
trade_size_usdc = 1.0
reversal = 0.55
reversal_low_threshold = 0.20
reversal_start_time = 120
sl_reversal = 0
unwind_pnl_rev = 0.15
sl_pnl_rev = 0.40
unwind_time_rev = 26.0
"#;
        let cfg: SiglabConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.markets.len(), 1);
        assert_eq!(cfg.variants.len(), 1);
        let p = cfg.variants[0].to_asset_params("BTC").unwrap();
        assert_eq!(p.asset, "BTC");
        assert!((p.reversal - 0.55).abs() < 1e-9);
    }

    #[test]
    fn missing_required_field_errors_not_panics() {
        let v = VariantCfg {
            id: "bad".into(),
            strategy: "reversal".into(),
            assets: vec![],
            no_enter_when_time_left: 10.0,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 2.0,
            delta_pct_rev: 0.001,
            delta_pct_hp: 0.0004,
            price_high_rev: 0.9,
            trade_size_usdc: 1.0,
            reversal: None, // missing on purpose
            reversal_low_threshold: Some(0.2),
            reversal_start_time: Some(120.0),
            sl_reversal: Some(0.0),
            unwind_pnl_rev: Some(0.15),
            sl_pnl_rev: Some(0.40),
            unwind_time_rev: Some(26.0),
            price_low: None,
            price_high: None,
            enter_when_time_left: None,
            sl_high_prob: None,
            unwind_pnl_hp: None,
            sl_pnl_hp: None,
            unwind_time_hp: None,
        };
        assert!(v.to_asset_params("BTC").is_err());
    }

    #[test]
    fn applies_to_respects_asset_list() {
        let mut v_all = mk_variant();
        v_all.assets = vec![];
        assert!(v_all.applies_to("BTC"));
        assert!(v_all.applies_to("ETH"));

        let mut v_scoped = mk_variant();
        v_scoped.assets = vec!["BTC".to_string()];
        assert!(v_scoped.applies_to("BTC"));
        assert!(!v_scoped.applies_to("ETH"));
    }

    fn mk_variant() -> VariantCfg {
        VariantCfg {
            id: "v".into(),
            strategy: "reversal".into(),
            assets: vec![],
            no_enter_when_time_left: 10.0,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 2.0,
            delta_pct_rev: 0.001,
            delta_pct_hp: 0.0004,
            price_high_rev: 0.9,
            trade_size_usdc: 1.0,
            reversal: Some(0.55),
            reversal_low_threshold: Some(0.2),
            reversal_start_time: Some(120.0),
            sl_reversal: Some(0.0),
            unwind_pnl_rev: Some(0.15),
            sl_pnl_rev: Some(0.40),
            unwind_time_rev: Some(26.0),
            price_low: None,
            price_high: None,
            enter_when_time_left: None,
            sl_high_prob: None,
            unwind_pnl_hp: None,
            sl_pnl_hp: None,
            unwind_time_hp: None,
        }
    }
}
