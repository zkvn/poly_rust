//! Paper trade-record output. Deliberately its own type, not a re-export of
//! `trader::types::TradeRecord` — adding fields to that struct would mean touching
//! `trader/src/types.rs`, and this crate's whole point is to be zero-touch against
//! `trader`/`price_feed` (see siglab/config.rs's doc comment). Copying the handful of
//! fields out is a few extra lines, not a real cost.

use serde::{Deserialize, Serialize};
use trader::types::TradeRecord;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Crypto,
    /// Weather bucket trades produced by `bucket_reversal.rs` — a fresh, self-contained
    /// decision core (not `trader::machine::Machine`), never resolved against a real
    /// outcome (see `doc/plan_weather_worldcup_trading_2026-07-13.md`) — every trade closes
    /// via stop-loss/take-profit/30s timeout, price-and-time only.
    Weather,
    /// Historical only — World Cup support was removed 2026-07-24 (tournament over, see
    /// `doc/plan_better_signal_2026-07-24.md`). Kept so old JSONL trade-log rows and
    /// already-written report days still deserialize; no new trade is ever tagged this way.
    Worldcup,
}

/// `strategy`/`side`/`outcome` are `String` here (not the `&'static str` that
/// `trader::types::TradeRecord`/`Side`/`Outcome` use) specifically so this type can
/// round-trip through `Deserialize` — `report.rs` reads trade records back out of the JSONL
/// log to build the hourly summary, and `&'static str` can't borrow from deserialized input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiglabTradeRecord {
    pub logged_at: f64,
    pub market_kind: MarketKind,
    /// Which named `[[variant]]` from siglab's own config fired — distinguishes many
    /// concurrent parameter sets sharing the same `strategy`/`slug`. Per
    /// `plan_weather_bot.md`'s DeepSeek-review follow-up (#1/#5): once weather markets
    /// produce real trade records, this type also needs an `event_id` grouping a day's N
    /// buckets for one city, since they're mutually exclusive within a negRisk group and
    /// summing PnL across them would be wrong — deferred until real weather resolution
    /// lands; a single crypto market has no such grouping to get wrong.
    pub variant_id: String,
    pub asset: String,
    /// The rotating market's display key, e.g. `"XRP-15m"`/`"BTC-hourly-et"` — distinguishes
    /// which of an asset's several concurrently-running durations a trade came from (see
    /// `rotation::Rotation::market_key`). Added for the hourly report's per-market grouping;
    /// `asset` alone can't tell "XRP-5m" and "XRP-15m" apart. `#[serde(default)]` so trade
    /// records already logged before this field existed still deserialize (as `""`) instead
    /// of being silently dropped by `report.rs`'s `recent_trades`.
    #[serde(default)]
    pub market: String,
    pub slug: String,
    pub cycle_start: f64,
    pub strategy: String,
    pub side: String,
    pub entry_ts: f64,
    /// The actual poly-price observation's own timestamp (`trader::types::TradeRecord::
    /// entry_price_ts`) — distinct from `entry_ts`, the *triggering* tick's timestamp
    /// (poly or binance). See that field's doc comment and
    /// `doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`.
    /// `#[serde(default)]` so records logged before this field existed still deserialize.
    #[serde(default)]
    pub entry_price_ts: f64,
    pub token_price: f64,
    pub exit_price: f64,
    pub outcome: String,
    pub pnl: f64,
}

impl SiglabTradeRecord {
    pub fn from_trader(
        rec: &TradeRecord,
        market_kind: MarketKind,
        variant_id: &str,
        asset: &str,
        market: &str,
        logged_at: f64,
    ) -> Self {
        Self {
            logged_at,
            market_kind,
            variant_id: variant_id.to_string(),
            asset: asset.to_string(),
            market: market.to_string(),
            slug: rec.slug.clone(),
            cycle_start: rec.cycle_start,
            strategy: rec.strategy.to_string(),
            side: rec.side.as_str().to_string(),
            entry_ts: rec.entry_ts,
            entry_price_ts: rec.entry_price_ts,
            token_price: rec.token_price,
            exit_price: rec.exit_price,
            outcome: rec.outcome.as_str().to_string(),
            pnl: rec.pnl,
        }
    }

    /// For `v_shape.rs`'s engine — same rationale as `from_bucket_engine` below (no
    /// `trader::types::TradeRecord` to convert from, since `VShapeEngine` never touches
    /// `trader::machine::Machine` either). `market_kind` is caller-supplied (like
    /// `from_bucket_engine`'s) since 2026-07-17 — `v_shape.rs`'s engine now also runs against
    /// weather/World Cup buckets (never calling `cycle_open`, which permanently disables its
    /// cycle-end force-unwind branch and leaves it behaviorally equivalent to
    /// `bucket_reversal.rs` — see `doc/feature_v_2026-07-17.md`), not just crypto markets.
    /// `strategy` stays hardcoded `"v_shape"` instead of `"reversal"` regardless of
    /// `market_kind`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_v_shape_engine(
        market_kind: MarketKind,
        variant_id: &str,
        asset: &str,
        market: &str,
        slug: &str,
        side_up: bool,
        entry_ts: f64,
        entry_price: f64,
        exit_price: f64,
        outcome: &str,
        pnl: f64,
        logged_at: f64,
    ) -> Self {
        Self {
            logged_at,
            market_kind,
            variant_id: variant_id.to_string(),
            asset: asset.to_string(),
            market: market.to_string(),
            slug: slug.to_string(),
            cycle_start: entry_ts,
            strategy: "v_shape".to_string(),
            side: if side_up { "UP" } else { "DOWN" }.to_string(),
            entry_ts,
            // v_shape.rs has no separate triggering-tick-vs-observation-tick distinction
            // (single on_tick stream, no binance reference feed) — same value, same
            // reasoning as from_bucket_engine's entry_price_ts below.
            entry_price_ts: entry_ts,
            token_price: entry_price,
            exit_price,
            outcome: outcome.to_string(),
            pnl,
        }
    }

    /// For `bucket_reversal.rs`'s engine, which has no `trader::types::TradeRecord` to
    /// convert from — it never touches `trader::machine::Machine` at all.
    #[allow(clippy::too_many_arguments)]
    pub fn from_bucket_engine(
        market_kind: MarketKind,
        variant_id: &str,
        asset: &str,
        market: &str,
        slug: &str,
        side_up: bool,
        entry_ts: f64,
        entry_price: f64,
        exit_price: f64,
        outcome: &str,
        pnl: f64,
        logged_at: f64,
    ) -> Self {
        Self {
            logged_at,
            market_kind,
            variant_id: variant_id.to_string(),
            asset: asset.to_string(),
            market: market.to_string(),
            slug: slug.to_string(),
            cycle_start: entry_ts,
            strategy: "reversal".to_string(),
            side: if side_up { "UP" } else { "DOWN" }.to_string(),
            entry_ts,
            // bucket_reversal.rs has no separate triggering-tick-vs-observation-tick
            // distinction (single on_tick stream, no binance reference feed) — same value.
            entry_price_ts: entry_ts,
            token_price: entry_price,
            exit_price,
            outcome: outcome.to_string(),
            pnl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_v_shape_engine` gained a caller-supplied `market_kind` parameter 2026-07-17 (see
    /// `doc/feature_v_2026-07-17.md`) so `event_monitor.rs` can tag V-shape trades fired on
    /// weather/World Cup buckets as such, rather than the old hardcoded `MarketKind::Crypto`.
    /// Confirms both non-crypto kinds round-trip correctly and `strategy` stays `"v_shape"`
    /// regardless of which market kind is passed.
    #[test]
    fn from_v_shape_engine_tags_non_crypto_market_kinds() {
        let weather = SiglabTradeRecord::from_v_shape_engine(
            MarketKind::Weather,
            "v_0.7_0.3_0.7_0.3_0.05",
            "weather",
            "weather:hong-kong",
            "hong-kong-2026-07-17",
            true,
            10.0,
            0.70,
            0.75,
            "UNWIND",
            0.05,
            20.0,
        );
        assert!(matches!(weather.market_kind, MarketKind::Weather));
        assert_eq!(weather.strategy, "v_shape");
        assert_eq!(weather.side, "UP");

        let worldcup = SiglabTradeRecord::from_v_shape_engine(
            MarketKind::Worldcup,
            "v_0.7_0.3_0.7_0.3_0.05",
            "worldcup",
            "worldcup:world-cup-winner",
            "world-cup-winner",
            false,
            10.0,
            0.70,
            0.40,
            "STOPLOSS",
            -0.3,
            20.0,
        );
        assert!(matches!(worldcup.market_kind, MarketKind::Worldcup));
        assert_eq!(worldcup.strategy, "v_shape");
        assert_eq!(worldcup.side, "DOWN");
    }
}
