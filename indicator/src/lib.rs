//! Standalone indicator engine — consumes NATS price ticks, computes the bt4
//! signal stack (HAR volatility forecast, P(up), SNR) and republishes them on
//! `indicator.<ASSET>` for the trader (or anything else) to consume.
//!
//! Reference implementation: `../btc_5mins/bot/signals.py`
//! (`VolHarSignal` / `PUpSignal` / `SnrSignal`); plan:
//! `trader/doc/feature_vol_2026-07-18.md`.

pub mod config;
pub mod engine;
pub mod math;

pub use config::IndicatorConfig;
pub use engine::{AssetEngine, Emit};
