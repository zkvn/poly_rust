/// SawLowSignal — latches True when the side's token price dips below
/// `low_threshold` while time_left is in `[end_time_left, start_time_left]`.
///
/// Window is in time_left (seconds until cycle end):
///   - opens at `start_time_left` (e.g. 120s remaining for BTC reversal_start_time)
///   - closes at `end_time_left` (e.g. 10s = no_enter_when_time_left)
///
/// Once latched, `saw_low()` returns true for the rest of the cycle.

use crate::signal::Signal;
use crate::types::{CycleContext, PolyTick};

pub struct SawLowSignal {
    side: bool, // true = UP side
    low_threshold: f64,
    start_time_left: f64, // window opens (larger value)
    end_time_left: f64,   // window closes (smaller value)
    fired: bool,
    cycle_end_ts: f64,
    name_str: &'static str,
}

impl SawLowSignal {
    pub fn new_up(low_threshold: f64, start_time_left: f64, end_time_left: f64) -> Self {
        Self {
            side: true,
            low_threshold,
            start_time_left,
            end_time_left,
            fired: false,
            cycle_end_ts: 0.0,
            name_str: "saw_low_up",
        }
    }

    pub fn new_dn(low_threshold: f64, start_time_left: f64, end_time_left: f64) -> Self {
        Self {
            side: false,
            low_threshold,
            start_time_left,
            end_time_left,
            fired: false,
            cycle_end_ts: 0.0,
            name_str: "saw_low_dn",
        }
    }

    pub fn saw_low(&self) -> bool {
        self.fired
    }
}

impl Signal for SawLowSignal {
    fn name(&self) -> &str {
        self.name_str
    }

    fn reset(&mut self, ctx: &CycleContext) {
        self.fired = false;
        self.cycle_end_ts = ctx.end_ts;
    }

    fn on_poly(&mut self, t: PolyTick) {
        if self.fired {
            return;
        }
        let time_left = self.cycle_end_ts - t.ts;
        if !(self.end_time_left <= time_left && time_left <= self.start_time_left) {
            return;
        }
        let price = if self.side { t.up } else { t.dn };
        if price > 0.0 && price < self.low_threshold {
            self.fired = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CycleContext;

    fn ctx(start_ts: f64) -> CycleContext {
        CycleContext { start_ts, end_ts: start_ts + 300.0, open_binance: 50000.0 }
    }

    #[test]
    fn latches_on_dip_inside_window() {
        let mut s = SawLowSignal::new_up(0.30, 120.0, 10.0);
        s.reset(&ctx(1000.0));
        // ts=1190 → time_left=110s (inside [10,120])
        s.on_poly(PolyTick { ts: 1190.0, up: 0.25, dn: 0.75 });
        assert!(s.saw_low());
    }

    #[test]
    fn no_latch_before_window_opens() {
        let mut s = SawLowSignal::new_up(0.30, 120.0, 10.0);
        s.reset(&ctx(1000.0));
        // ts=1050 → time_left=250s (> 120s, window not yet open)
        s.on_poly(PolyTick { ts: 1050.0, up: 0.25, dn: 0.75 });
        assert!(!s.saw_low());
    }

    #[test]
    fn no_latch_after_window_closes() {
        let mut s = SawLowSignal::new_up(0.30, 120.0, 10.0);
        s.reset(&ctx(1000.0));
        // ts=1295 → time_left=5s (< 10s, window closed)
        s.on_poly(PolyTick { ts: 1295.0, up: 0.25, dn: 0.75 });
        assert!(!s.saw_low());
    }

    #[test]
    fn no_latch_above_threshold() {
        let mut s = SawLowSignal::new_up(0.30, 120.0, 10.0);
        s.reset(&ctx(1000.0));
        // inside window but price above threshold
        s.on_poly(PolyTick { ts: 1190.0, up: 0.35, dn: 0.65 });
        assert!(!s.saw_low());
    }

    #[test]
    fn reset_clears_latch() {
        let mut s = SawLowSignal::new_up(0.30, 120.0, 10.0);
        s.reset(&ctx(1000.0));
        s.on_poly(PolyTick { ts: 1190.0, up: 0.25, dn: 0.75 });
        assert!(s.saw_low());
        s.reset(&ctx(1300.0));
        assert!(!s.saw_low());
    }
}
