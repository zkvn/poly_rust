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
    // Weather markets are monitoring-only in this version (see siglab/src/weather.rs's doc
    // comment) — they never produce a SiglabTradeRecord, so no Weather variant exists here
    // yet. Add one when real weather resolution (plan_weather_bot.md §5 Phase 2/3) lands.
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
    pub slug: String,
    pub cycle_start: f64,
    pub strategy: String,
    pub side: String,
    pub entry_ts: f64,
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
        logged_at: f64,
    ) -> Self {
        Self {
            logged_at,
            market_kind,
            variant_id: variant_id.to_string(),
            asset: asset.to_string(),
            slug: rec.slug.clone(),
            cycle_start: rec.cycle_start,
            strategy: rec.strategy.to_string(),
            side: rec.side.as_str().to_string(),
            entry_ts: rec.entry_ts,
            token_price: rec.token_price,
            exit_price: rec.exit_price,
            outcome: rec.outcome.as_str().to_string(),
            pnl: rec.pnl,
        }
    }
}
