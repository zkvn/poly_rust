//! TOML config for the indicator process. Validation fails loudly at startup
//! (bad beta shape, unknown market label) — same posture as the trader's
//! `MarketDuration::parse`: never skip silently.

use std::collections::HashMap;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Market label → cycle period seconds. Mirrors trader's `MarketDuration`
/// (`1h-et`'s boundaries coincide with UTC hours, so a plain 3600 period is
/// exact for indicator purposes).
pub fn market_period_secs(label: &str) -> Option<u64> {
    match label {
        "5m" => Some(300),
        "15m" => Some(900),
        "1h-et" => Some(3600),
        "4h" => Some(14400),
        _ => None,
    }
}

fn default_emit_interval_ms() -> u64 {
    250
}
fn default_min_ticks() -> usize {
    30
}
fn default_subsample_secs() -> usize {
    5
}
fn default_true() -> bool {
    true
}
fn default_mode() -> String {
    "har".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct HarVolConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Look-back cycle windows, strictly ascending — `[1, 5, 12]` is the
    /// Python `VolHarSignal` c_1_5_12 behavior; `[1, 3, 6]` etc. are config
    /// edits. NOTE: betas are fitted per window set (`ml/features/har_beta.py`
    /// in ../btc_5mins) — changing windows without re-fitting betas silently
    /// applies a mis-calibrated forecast.
    pub windows: Vec<usize>,
    /// Cycle must have ≥ this many 1-Hz samples to yield a valid rv.
    #[serde(default = "default_min_ticks")]
    pub min_ticks: usize,
    /// rv estimator subsample step in seconds (rv_5s → 5).
    #[serde(default = "default_subsample_secs")]
    pub subsample_secs: usize,
    /// Per-asset OLS betas, `windows.len()+1` elements (intercept first).
    /// Key "default" required; per-asset keys override.
    pub beta: HashMap<String, Vec<f64>>,
    /// Per-asset Student-t dof for P(up) HAR mode. Key "default" required.
    pub nu: HashMap<String, f64>,
}

impl HarVolConfig {
    pub fn beta_for(&self, asset: &str) -> Option<&Vec<f64>> {
        self.beta.get(asset).or_else(|| self.beta.get("default"))
    }

    pub fn nu_for(&self, asset: &str) -> f64 {
        self.nu
            .get(asset)
            .or_else(|| self.nu.get("default"))
            .copied()
            .unwrap_or(4.2)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModeConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// "har" (Student-t on the HAR forecast) or "streaming" (Gaussian on
    /// in-cycle streaming vol).
    #[serde(default = "default_mode")]
    pub mode: String,
}

impl Default for ModeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: default_mode(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndicatorConfig {
    pub nats_url: String,
    pub assets: Vec<String>,
    /// Market family the cycle clock follows: 5m/15m/1h-et/4h.
    pub market: String,
    /// Minimum gap between publishes per asset; 0 = publish on every tick.
    #[serde(default = "default_emit_interval_ms")]
    pub emit_interval_ms: u64,
    pub har_vol: HarVolConfig,
    #[serde(default)]
    pub p_up: ModeConfig,
    #[serde(default)]
    pub snr: ModeConfig,
}

impl IndicatorConfig {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string(),
            source,
        })?;
        let cfg: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_string(),
            source,
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn period_secs(&self) -> u64 {
        // validate() guarantees the label parses.
        market_period_secs(&self.market).unwrap_or(300)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let inv = |m: String| Err(ConfigError::Invalid(m));
        if self.assets.is_empty() {
            return inv("assets must be non-empty".into());
        }
        let Some(period) = market_period_secs(&self.market) else {
            return inv(format!(
                "unknown market label {:?} (expected 5m/15m/1h-et/4h)",
                self.market
            ));
        };
        let h = &self.har_vol;
        if h.windows.is_empty() || h.windows[0] == 0 {
            return inv("har_vol.windows must be non-empty and positive".into());
        }
        if !h.windows.windows(2).all(|w| w[0] < w[1]) {
            return inv(format!(
                "har_vol.windows must be strictly ascending: {:?}",
                h.windows
            ));
        }
        let want_len = h.windows.len() + 1;
        if !h.beta.contains_key("default") {
            return inv("har_vol.beta must contain a \"default\" key".into());
        }
        for (asset, beta) in &h.beta {
            if beta.len() != want_len {
                return inv(format!(
                    "har_vol.beta.{asset} must have {want_len} elements \
                     (intercept + one per window), got {}",
                    beta.len()
                ));
            }
        }
        if !h.nu.contains_key("default") {
            return inv("har_vol.nu must contain a \"default\" key".into());
        }
        for (asset, nu) in &h.nu {
            if *nu <= 2.0 {
                return inv(format!(
                    "har_vol.nu.{asset} must be > 2 (variance adjustment √(ν/(ν−2))), got {nu}"
                ));
            }
        }
        if h.subsample_secs == 0 {
            return inv("har_vol.subsample_secs must be positive".into());
        }
        for (name, mode) in [("p_up", &self.p_up.mode), ("snr", &self.snr.mode)] {
            if mode != "har" && mode != "streaming" {
                return inv(format!(
                    "{name}.mode must be \"har\" or \"streaming\", got {mode:?}"
                ));
            }
        }
        // HAR betas are calibrated on the cycle length they were fitted for
        // (300s in production) — other periods run, but the operator should
        // know. Warning, not error: same posture as Python's har_pup_enabled
        // doc comment.
        if period != 300 && (self.p_up.mode == "har" || self.snr.mode == "har") {
            eprintln!(
                "[indicator] WARNING: market {:?} (period {period}s) with HAR-mode p_up/snr — \
                 betas are 5m-calibrated; re-fit before trusting these values",
                self.market
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_toml() -> String {
        r#"
nats_url = "nats://localhost:4222"
assets = ["BTC"]
market = "5m"

[har_vol]
windows = [1, 5, 12]
[har_vol.beta]
default = [1e-5, 0.4, 0.2, 0.3]
[har_vol.nu]
default = 4.2469
"#
        .to_string()
    }

    fn parse(s: &str) -> Result<IndicatorConfig, ConfigError> {
        let cfg: IndicatorConfig = toml::from_str(s).map_err(|source| ConfigError::Parse {
            path: "<test>".into(),
            source,
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn base_config_parses_with_defaults() {
        let cfg = parse(&base_toml()).expect("valid");
        assert_eq!(cfg.period_secs(), 300);
        assert_eq!(cfg.emit_interval_ms, 250);
        assert_eq!(cfg.har_vol.min_ticks, 30);
        assert_eq!(cfg.har_vol.subsample_secs, 5);
        assert!(cfg.p_up.enabled);
        assert_eq!(cfg.p_up.mode, "har");
        assert_eq!(cfg.har_vol.nu_for("BTC"), 4.2469);
        assert_eq!(cfg.har_vol.beta_for("ETH"), cfg.har_vol.beta.get("default"));
    }

    #[test]
    fn per_asset_beta_and_nu_override_default() {
        let toml = base_toml().replace(
            "[har_vol.nu]\ndefault = 4.2469",
            "BTC = [2e-5, 0.5, 0.1, 0.2]\n[har_vol.nu]\ndefault = 4.2469\nBTC = 4.0",
        );
        let cfg = parse(&toml).expect("valid");
        assert_eq!(cfg.har_vol.nu_for("BTC"), 4.0);
        assert_eq!(cfg.har_vol.beta_for("BTC").unwrap()[0], 2e-5);
    }

    #[test]
    fn rejects_bad_market_windows_beta_nu_mode() {
        assert!(
            parse(&base_toml().replace("\"5m\"", "\"1h\"")).is_err(),
            "unknown market"
        );
        assert!(
            parse(&base_toml().replace("[1, 5, 12]", "[12, 5, 1]")).is_err(),
            "descending windows"
        );
        assert!(
            parse(&base_toml().replace("[1e-5, 0.4, 0.2, 0.3]", "[1e-5, 0.4]")).is_err(),
            "beta length mismatch"
        );
        assert!(
            parse(&base_toml().replace("4.2469", "1.5")).is_err(),
            "nu ≤ 2"
        );
        let toml = base_toml() + "\n[p_up]\nmode = \"gaussian\"\n";
        assert!(parse(&toml).is_err(), "unknown mode");
    }

    #[test]
    fn market_labels_map_to_trader_periods() {
        assert_eq!(market_period_secs("5m"), Some(300));
        assert_eq!(market_period_secs("15m"), Some(900));
        assert_eq!(market_period_secs("1h-et"), Some(3600));
        assert_eq!(market_period_secs("4h"), Some(14400));
        assert_eq!(market_period_secs("1h"), None);
    }
}
