//! DeltaPctSignal — (latest_binance - cycle_open) / cycle_open.
//!
//! Returns 0.0 if either price is missing (mirrors Python DeltaPctSignal).

use crate::signal::Signal;
use crate::types::{BinanceTick, CycleContext};

pub struct DeltaPctSignal {
    price: f64,
    open: f64,
}

impl Default for DeltaPctSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaPctSignal {
    pub fn new() -> Self {
        Self { price: 0.0, open: 0.0 }
    }

    pub fn value(&self) -> f64 {
        if self.price <= 0.0 || self.open <= 0.0 {
            return 0.0;
        }
        (self.price - self.open) / self.open
    }
}

impl Signal for DeltaPctSignal {
    fn name(&self) -> &str { "delta_pct" }

    fn reset(&mut self, ctx: &CycleContext) {
        self.open = ctx.open_binance;
        // Unlike LatestPolySignal/LatestBinanceSignal (which deliberately keep the
        // last-known price across cycles), `price` must NOT carry over: once entry
        // evaluation can be triggered by a PolyTick alone (worker.rs/machine.rs
        // `try_enter`), a stale price left over from the previous cycle would look
        // like a same-cycle, ready-to-use delta instead of "not yet known".
        self.price = 0.0;
    }

    fn on_binance(&mut self, t: BinanceTick) {
        if t.price > 0.0 {
            self.price = t.price;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CycleContext;

    fn ctx(open: f64) -> CycleContext {
        CycleContext { start_ts: 0.0, end_ts: 300.0, open_binance: open }
    }

    #[test]
    fn computes_delta_pct() {
        let mut s = DeltaPctSignal::new();
        s.reset(&ctx(50_000.0));
        s.on_binance(BinanceTick { ts: 1.0, price: 50_100.0 });
        let expected = 100.0 / 50_000.0;
        assert!((s.value() - expected).abs() < 1e-12);
    }

    #[test]
    fn zero_before_ready() {
        let s = DeltaPctSignal::new();
        assert_eq!(s.value(), 0.0);
    }

    #[test]
    fn reset_clears_stale_price_from_previous_cycle() {
        let mut s = DeltaPctSignal::new();
        s.reset(&ctx(50_000.0));
        s.on_binance(BinanceTick { ts: 1.0, price: 49_000.0 });
        assert!(s.value() < 0.0, "sanity: price is set before reset");

        // A new cycle must not evaluate against last cycle's leftover Binance price.
        s.reset(&ctx(60_000.0));
        assert_eq!(s.value(), 0.0, "price must be cleared on reset, not carried over");
    }
}
