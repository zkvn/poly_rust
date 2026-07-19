//! Cross-strategy gate checks (mirrors Python worker._common_gates / backtest._gate_blocked).
//!
//! Gate order (matches Python):
//!   1. spread premium/discount
//!   2. poly staleness (age)
//!   3. |delta_pct| < per-strategy minimum
//!   4. token_price > max_buy_price
//!   5. reversal: token_price > price_high_rev

use crate::signal::{DeltaPctSignal, LatestPolySignal, SpreadSignal};
use crate::types::{EntryType, TradeIntent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateBlock {
    SpreadPremium,
    SpreadDiscount,
    PolyStale,
    MinDeltaPct,
    MaxBuyPrice,
    PriceHighRev,
}

impl GateBlock {
    pub fn as_str(&self) -> &'static str {
        match self {
            GateBlock::SpreadPremium => "spread_premium",
            GateBlock::SpreadDiscount => "spread_discount",
            GateBlock::PolyStale => "poly_stale",
            GateBlock::MinDeltaPct => "min_delta_pct",
            GateBlock::MaxBuyPrice => "max_buy_price",
            GateBlock::PriceHighRev => "price_high_rev",
        }
    }
}

pub struct GateParams {
    pub spread_premium_limit: f64,
    pub spread_discount_limit: f64,
    pub max_price_age_secs: f64,
    pub delta_pct_rev: f64,
    pub delta_pct_hp: f64,
    /// Min |delta_pct| for v_shape entries — 0.0 (the config default) disables the
    /// check entirely: v_shape is pure CLOB price action with no Binance-direction
    /// requirement (see trader/doc/plan_v_shape_trader_2026-07-17.md).
    pub delta_pct_v: f64,
    pub max_buy_price: f64,
    pub price_high_rev: f64,
}

/// Returns Some(block_reason) if the intent should be rejected, None if it passes.
pub fn check_gates(
    intent: &TradeIntent,
    spread: &SpreadSignal,
    latest_poly: &LatestPolySignal,
    delta_pct: &DeltaPctSignal,
    params: &GateParams,
    now: f64,
) -> Option<GateBlock> {
    let total = spread.value();
    if total > params.spread_premium_limit {
        return Some(GateBlock::SpreadPremium);
    }
    if total < params.spread_discount_limit {
        return Some(GateBlock::SpreadDiscount);
    }
    let age = latest_poly.age(now);
    if age > params.max_price_age_secs {
        return Some(GateBlock::PolyStale);
    }
    let dp = delta_pct.value().abs();
    let min_delta = match intent.entry_type {
        EntryType::Reversal => params.delta_pct_rev,
        EntryType::HighProb => params.delta_pct_hp,
        EntryType::VShape => params.delta_pct_v,
    };
    if dp < min_delta {
        return Some(GateBlock::MinDeltaPct);
    }
    let token_price = intent.token_price();
    if token_price > params.max_buy_price {
        return Some(GateBlock::MaxBuyPrice);
    }
    if intent.entry_type == EntryType::Reversal
        && params.price_high_rev > 0.0
        && token_price > params.price_high_rev
    {
        return Some(GateBlock::PriceHighRev);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{DeltaPctSignal, LatestPolySignal, Signal, SpreadSignal};
    use crate::types::{BinanceTick, CycleContext, PolyTick, Side};

    fn default_params() -> GateParams {
        GateParams {
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 2.0,
            delta_pct_rev: 0.0008,
            delta_pct_hp: 0.0004,
            delta_pct_v: 0.0,
            max_buy_price: 0.95,
            price_high_rev: 0.90,
        }
    }

    fn intent(side: Side, token: f64) -> TradeIntent {
        TradeIntent {
            side,
            entry_type: EntryType::Reversal,
            up: if side == Side::Up { token } else { 1.0 - token },
            dn: if side == Side::Down {
                token
            } else {
                1.0 - token
            },
            binance_price: 50000.0,
        }
    }

    fn baseline_signals(
        open: f64,
        now_price: f64,
        poly_ts: f64,
    ) -> (SpreadSignal, LatestPolySignal, DeltaPctSignal) {
        let ctx = CycleContext {
            start_ts: 0.0,
            end_ts: 300.0,
            open_binance: open,
        };
        let mut spread = SpreadSignal::new();
        let mut lp = LatestPolySignal::new();
        let mut dp = DeltaPctSignal::new();
        dp.reset(&ctx);
        let tick = PolyTick {
            ts: poly_ts,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.0,
            up_ask: 0.0,
        };
        spread.on_poly(tick);
        lp.on_poly(tick);
        dp.on_binance(BinanceTick {
            ts: poly_ts,
            price: now_price,
        });
        (spread, lp, dp)
    }

    #[test]
    fn passes_clean_intent() {
        let (spread, lp, dp) = baseline_signals(50000.0, 50100.0, 100.0);
        let i = intent(Side::Up, 0.75);
        let p = default_params();
        assert!(check_gates(&i, &spread, &lp, &dp, &p, 100.5).is_none());
    }

    #[test]
    fn blocks_stale_poly() {
        let (spread, lp, dp) = baseline_signals(50000.0, 50100.0, 100.0);
        let i = intent(Side::Up, 0.75);
        let p = default_params();
        // now = 103, age = 3s > max_price_age_secs=2
        assert_eq!(
            check_gates(&i, &spread, &lp, &dp, &p, 103.0),
            Some(GateBlock::PolyStale)
        );
    }

    #[test]
    fn blocks_max_buy_price() {
        let (spread, lp, dp) = baseline_signals(50000.0, 50100.0, 100.0);
        let i = intent(Side::Up, 0.96); // > max_buy_price=0.95
        let p = default_params();
        assert_eq!(
            check_gates(&i, &spread, &lp, &dp, &p, 100.5),
            Some(GateBlock::MaxBuyPrice)
        );
    }

    #[test]
    fn blocks_price_high_rev() {
        let (spread, lp, dp) = baseline_signals(50000.0, 50100.0, 100.0);
        let i = intent(Side::Up, 0.91); // > price_high_rev=0.90
        let p = default_params();
        assert_eq!(
            check_gates(&i, &spread, &lp, &dp, &p, 100.5),
            Some(GateBlock::PriceHighRev)
        );
    }

    #[test]
    fn blocks_low_delta_pct() {
        // delta_pct = 0.0001 < 0.0008
        let (spread, lp, dp) = baseline_signals(50000.0, 50005.0, 100.0); // 5/50000 = 0.0001
        let i = intent(Side::Up, 0.75);
        let p = default_params();
        assert_eq!(
            check_gates(&i, &spread, &lp, &dp, &p, 100.5),
            Some(GateBlock::MinDeltaPct)
        );
    }

    /// v_shape intents pass the delta gate with delta_pct_v=0.0 (its config default —
    /// no Binance-direction requirement), even at a delta that blocks a reversal
    /// intent, and are exempt from the reversal-only price_high_rev ceiling.
    #[test]
    fn v_shape_passes_zero_min_delta_and_skips_price_high_rev() {
        let (spread, lp, dp) = baseline_signals(50000.0, 50005.0, 100.0); // |dp| = 0.0001
        let p = default_params();
        let mut i = intent(Side::Up, 0.75);
        i.entry_type = EntryType::VShape;
        assert!(check_gates(&i, &spread, &lp, &dp, &p, 100.5).is_none());

        // 0.91 > price_high_rev=0.90 blocks a reversal intent but not a v_shape one
        // (max_buy_price=0.95 is still the binding ceiling for v_shape). Use a delta
        // large enough to pass the reversal min (0.0008) so PriceHighRev — checked
        // after MinDeltaPct in the fixed gate order — is what actually fires.
        let (spread, lp, dp) = baseline_signals(50000.0, 50100.0, 100.0); // |dp| = 0.002
        let mut i = intent(Side::Up, 0.91);
        i.entry_type = EntryType::VShape;
        assert!(check_gates(&i, &spread, &lp, &dp, &p, 100.5).is_none());
        let i = intent(Side::Up, 0.91); // reversal: still blocked
        assert_eq!(
            check_gates(&i, &spread, &lp, &dp, &p, 100.5),
            Some(GateBlock::PriceHighRev)
        );
    }
}
