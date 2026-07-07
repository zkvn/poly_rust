//! Signal layer — trait, TickBus, and one file per signal.

pub mod delta_pct;
pub mod latest_binance;
pub mod latest_poly;
pub mod saw_low;

pub use delta_pct::DeltaPctSignal;
pub use latest_binance::LatestBinanceSignal;
pub use latest_poly::{LatestPolySignal, SpreadSignal};
pub use saw_low::SawLowSignal;

use crate::types::{BinanceTick, CycleContext, PolyTick};

/// Minimal Signal trait — reset at each cycle boundary.
/// Signals are single-threaded in the backtest; no locking required.
pub trait Signal {
    fn name(&self) -> &str;
    fn reset(&mut self, ctx: &CycleContext);
    fn on_binance(&mut self, _t: BinanceTick) {}
    fn on_poly(&mut self, _t: PolyTick) {}
}

/// Fans BinanceTick / PolyTick out to registered callbacks.
/// In the backtest, callbacks are closures capturing &mut Signal refs.
pub struct TickBus {
    binance: Vec<Box<dyn FnMut(BinanceTick)>>,
    poly: Vec<Box<dyn FnMut(PolyTick)>>,
}

impl Default for TickBus {
    fn default() -> Self {
        Self::new()
    }
}

impl TickBus {
    pub fn new() -> Self {
        Self { binance: Vec::new(), poly: Vec::new() }
    }

    pub fn subscribe_binance(&mut self, cb: impl FnMut(BinanceTick) + 'static) {
        self.binance.push(Box::new(cb));
    }

    pub fn subscribe_poly(&mut self, cb: impl FnMut(PolyTick) + 'static) {
        self.poly.push(Box::new(cb));
    }

    pub fn publish_binance(&mut self, t: BinanceTick) {
        for cb in &mut self.binance {
            cb(t);
        }
    }

    pub fn publish_poly(&mut self, t: PolyTick) {
        for cb in &mut self.poly {
            cb(t);
        }
    }
}
