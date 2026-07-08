//! Strategy layer — ReversalStrategy and HighProbStrategy.
//!
//! Strategies are pure objects over signal handles. `evaluate` is called after
//! each BinanceTick (same as Python backtest). Gates (spread, delta_pct,
//! staleness, max_price, halt) live in `gates.rs`, not here.

use crate::signal::{DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal};
use crate::types::{CycleContext, EntryType, Side, TradeIntent};

// ── Reversal ──────────────────────────────────────────────────────────────────

/// Fires on dip-and-recover: saw_low latched in the reversal window AND
/// current poly price above reversal threshold AND delta_pct in the right direction.
pub struct ReversalStrategy {
    reversal: f64,
    no_enter_when_time_left: f64,
    pub fired: bool,
    cycle_end_ts: f64,
}

impl ReversalStrategy {
    pub fn new(reversal: f64, no_enter_when_time_left: f64) -> Self {
        Self {
            reversal,
            no_enter_when_time_left,
            fired: false,
            cycle_end_ts: 0.0,
        }
    }

    pub fn reset(&mut self, ctx: &CycleContext) {
        self.fired = false;
        self.cycle_end_ts = ctx.end_ts;
    }

    pub fn mark_fired(&mut self) {
        self.fired = true;
    }

    /// Returns Some(TradeIntent) when reversal conditions are met.
    /// `now` is the current BinanceTick's timestamp.
    pub fn evaluate(
        &self,
        now: f64,
        saw_low_up: &SawLowSignal,
        saw_low_dn: &SawLowSignal,
        latest_poly: &LatestPolySignal,
        delta_pct: &DeltaPctSignal,
        latest_binance: &LatestBinanceSignal,
    ) -> Option<TradeIntent> {
        if self.fired {
            return None;
        }
        let time_left = self.cycle_end_ts - now;
        if time_left < self.no_enter_when_time_left {
            return None;
        }
        let up = latest_poly.up();
        let dn = latest_poly.dn();
        if up <= 0.0 || dn <= 0.0 {
            return None;
        }
        let dp = delta_pct.value();
        let binance_price = latest_binance.value();

        if saw_low_up.saw_low() && up > self.reversal && dp > 0.0 {
            return Some(TradeIntent {
                side: Side::Up,
                entry_type: EntryType::Reversal,
                up,
                dn,
                binance_price,
            });
        }
        if saw_low_dn.saw_low() && dn > self.reversal && dp < 0.0 {
            return Some(TradeIntent {
                side: Side::Down,
                entry_type: EntryType::Reversal,
                up,
                dn,
                binance_price,
            });
        }
        None
    }
}

// ── HighProb ──────────────────────────────────────────────────────────────────

/// Fires when the side's token is in the band (price_low, price_high) during
/// the entry window [no_enter_when_time_left, enter_when_time_left] and
/// delta_pct is in the right direction.
pub struct HighProbStrategy {
    price_low: f64,
    price_high: f64,
    enter_when_time_left: f64,
    no_enter_when_time_left: f64,
    pub fired: bool,
    cycle_end_ts: f64,
}

impl HighProbStrategy {
    pub fn new(
        price_low: f64,
        price_high: f64,
        enter_when_time_left: f64,
        no_enter_when_time_left: f64,
    ) -> Self {
        Self {
            price_low,
            price_high,
            enter_when_time_left,
            no_enter_when_time_left,
            fired: false,
            cycle_end_ts: 0.0,
        }
    }

    pub fn reset(&mut self, ctx: &CycleContext) {
        self.fired = false;
        self.cycle_end_ts = ctx.end_ts;
    }

    pub fn mark_fired(&mut self) {
        self.fired = true;
    }

    pub fn evaluate(
        &self,
        now: f64,
        latest_poly: &LatestPolySignal,
        delta_pct: &DeltaPctSignal,
        latest_binance: &LatestBinanceSignal,
    ) -> Option<TradeIntent> {
        if self.fired {
            return None;
        }
        let time_left = self.cycle_end_ts - now;
        if time_left > self.enter_when_time_left {
            return None;
        }
        if time_left < self.no_enter_when_time_left {
            return None;
        }
        let up = latest_poly.up();
        let dn = latest_poly.dn();
        if up <= 0.0 || dn <= 0.0 {
            return None;
        }
        let dp = delta_pct.value();
        let binance_price = latest_binance.value();

        if self.price_low < up && up < self.price_high && dp > 0.0 {
            return Some(TradeIntent {
                side: Side::Up,
                entry_type: EntryType::HighProb,
                up,
                dn,
                binance_price,
            });
        }
        if self.price_low < dn && dn < self.price_high && dp < 0.0 {
            return Some(TradeIntent {
                side: Side::Down,
                entry_type: EntryType::HighProb,
                up,
                dn,
                binance_price,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::Signal;
    use crate::signal::{DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal};
    use crate::types::{BinanceTick, CycleContext, PolyTick};

    fn make_ctx(end_ts: f64) -> CycleContext {
        CycleContext {
            start_ts: end_ts - 300.0,
            end_ts,
            open_binance: 50000.0,
        }
    }

    #[test]
    fn reversal_fires_down() {
        // DOWN: dn dipped below 0.30 in window, now dn > 0.60, dp < 0
        let end_ts = 1300.0;
        let open = 50000.0;
        // Dip at ts=1180 (time_left=120, inside [10,120])
        let ctx = make_ctx(end_ts);
        let mut sl_up = SawLowSignal::new_up(0.30, 120.0, 10.0);
        let mut sl_dn = SawLowSignal::new_dn(0.30, 120.0, 10.0);
        let mut lp = LatestPolySignal::new();
        let mut dp = DeltaPctSignal::new();
        let mut lb = LatestBinanceSignal::new();
        sl_up.reset(&ctx);
        sl_dn.reset(&ctx);
        dp.reset(&ctx);

        // poly tick: dn=0.20 (below 0.30 threshold), in window (time_left=120)
        sl_dn.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.80,
            dn: 0.20,
        });
        // current price: dn=0.70 (> reversal 0.60), and dp < 0 (price fell)
        lp.on_poly(PolyTick {
            ts: 1250.0,
            up: 0.30,
            dn: 0.70,
        });
        dp.on_binance(BinanceTick {
            ts: 1250.0,
            price: open - 10.0,
        }); // fell → dp < 0
        lb.on_binance(BinanceTick {
            ts: 1250.0,
            price: open - 10.0,
        });

        let mut strat = ReversalStrategy::new(0.60, 10.0);
        strat.reset(&ctx); // sets cycle_end_ts = 1300
        // at ts=1260 (time_left=40, >= no_enter=10)
        let intent = strat.evaluate(1260.0, &sl_up, &sl_dn, &lp, &dp, &lb);
        assert!(intent.is_some());
        let i = intent.unwrap();
        assert_eq!(i.side, Side::Down);
        assert_eq!(i.entry_type, EntryType::Reversal);
    }

    #[test]
    fn reversal_no_fire_without_saw_low() {
        let end_ts = 1300.0;
        let ctx = make_ctx(end_ts);
        let sl_up = SawLowSignal::new_up(0.30, 120.0, 10.0);
        let sl_dn = SawLowSignal::new_dn(0.30, 120.0, 10.0);
        let mut lp = LatestPolySignal::new();
        let mut dp = DeltaPctSignal::new();
        let mut lb = LatestBinanceSignal::new();
        dp.reset(&ctx);
        lp.on_poly(PolyTick {
            ts: 1250.0,
            up: 0.30,
            dn: 0.70,
        });
        dp.on_binance(BinanceTick {
            ts: 1250.0,
            price: 49_990.0,
        });
        lb.on_binance(BinanceTick {
            ts: 1250.0,
            price: 49_990.0,
        });

        let mut strat = ReversalStrategy::new(0.60, 10.0);
        strat.reset(&ctx); // sets cycle_end_ts = 1300
        assert!(
            strat
                .evaluate(1260.0, &sl_up, &sl_dn, &lp, &dp, &lb)
                .is_none()
        );
    }

    #[test]
    fn high_prob_fires_in_band() {
        let end_ts = 1300.0;
        let ctx = make_ctx(end_ts);
        let mut lp = LatestPolySignal::new();
        let mut dp = DeltaPctSignal::new();
        let mut lb = LatestBinanceSignal::new();
        dp.reset(&ctx);

        // UP token in band (0.80, 0.93), dp > 0
        lp.on_poly(PolyTick {
            ts: 1270.0,
            up: 0.86,
            dn: 0.14,
        });
        dp.on_binance(BinanceTick {
            ts: 1270.0,
            price: 50_010.0,
        });
        lb.on_binance(BinanceTick {
            ts: 1270.0,
            price: 50_010.0,
        });

        let mut strat = HighProbStrategy::new(0.80, 0.93, 20.0, 10.0);
        strat.reset(&ctx); // sets cycle_end_ts = 1300
        // at ts=1282, time_left=18, inside [10, 20]
        let intent = strat.evaluate(1282.0, &lp, &dp, &lb);
        assert!(intent.is_some());
        assert_eq!(intent.unwrap().side, Side::Up);
    }

    #[test]
    fn high_prob_no_fire_outside_window() {
        let end_ts = 1300.0;
        let ctx = make_ctx(end_ts);
        let mut lp = LatestPolySignal::new();
        let mut dp = DeltaPctSignal::new();
        let mut lb = LatestBinanceSignal::new();
        dp.reset(&ctx);

        lp.on_poly(PolyTick {
            ts: 1200.0,
            up: 0.86,
            dn: 0.14,
        });
        dp.on_binance(BinanceTick {
            ts: 1200.0,
            price: 50_010.0,
        });
        lb.on_binance(BinanceTick {
            ts: 1200.0,
            price: 50_010.0,
        });

        let mut strat = HighProbStrategy::new(0.80, 0.93, 20.0, 10.0);
        strat.reset(&ctx); // sets cycle_end_ts = 1300
        // at ts=1250, time_left=50, outside enter_when=20
        assert!(strat.evaluate(1250.0, &lp, &dp, &lb).is_none());
    }
}
