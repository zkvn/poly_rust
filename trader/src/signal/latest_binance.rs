/// LatestBinanceSignal — latest Binance spot price and timestamp.

use crate::signal::Signal;
use crate::types::{BinanceTick, CycleContext};

pub struct LatestBinanceSignal {
    price: f64,
    ts: f64,
}

impl LatestBinanceSignal {
    pub fn new() -> Self {
        Self { price: 0.0, ts: 0.0 }
    }

    pub fn value(&self) -> f64 { self.price }
}

impl Signal for LatestBinanceSignal {
    fn name(&self) -> &str { "latest_binance" }
    fn reset(&mut self, _ctx: &CycleContext) {}
    fn on_binance(&mut self, t: BinanceTick) {
        if t.price > 0.0 {
            self.price = t.price;
            self.ts = t.ts;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_latest_price() {
        let mut s = LatestBinanceSignal::new();
        s.on_binance(BinanceTick { ts: 100.0, price: 50_000.0 });
        s.on_binance(BinanceTick { ts: 101.0, price: 50_100.0 });
        assert!((s.value() - 50_100.0).abs() < 1e-6);
    }

    #[test]
    fn ignores_zero_price() {
        let mut s = LatestBinanceSignal::new();
        s.on_binance(BinanceTick { ts: 100.0, price: 50_000.0 });
        s.on_binance(BinanceTick { ts: 101.0, price: 0.0 });
        assert!((s.value() - 50_000.0).abs() < 1e-6);
    }
}
