/// DeltaPctSignal — (latest_binance - cycle_open) / cycle_open.
///
/// Returns 0.0 if either price is missing (mirrors Python DeltaPctSignal).

use crate::signal::Signal;
use crate::types::{BinanceTick, CycleContext};

pub struct DeltaPctSignal {
    price: f64,
    open: f64,
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
}
