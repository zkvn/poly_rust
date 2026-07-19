//! VShapeSignal — two-stage latch for one side's V-shape pattern: `seen_high` latches
//! once the side's token price reaches `>= high1`; `seen_low_after_high` latches once it
//! later dips `<= low` (a dip *before* high1 was ever seen must not count). Both stages
//! latch permanently within a cycle; `reset` clears them at each cycle boundary.
//!
//! Deliberately no time_left window, unlike `SawLowSignal`'s
//! `[end_time_left, start_time_left]` — siglab's `v_shape.rs` (this signal's origin, see
//! `trader/doc/plan_v_shape_trader_2026-07-17.md`) watches the whole cycle; the only
//! timing constraint (`no_enter_when_time_left`) is enforced by `VShapeStrategy`, matching
//! how `ReversalStrategy` owns that check rather than its `SawLowSignal`s.

use crate::signal::Signal;
use crate::types::{CycleContext, PolyTick};

pub struct VShapeSignal {
    side_up: bool,
    high1: f64,
    low: f64,
    seen_high: bool,
    seen_low_after_high: bool,
    name_str: &'static str,
}

impl VShapeSignal {
    pub fn new_up(high1: f64, low: f64) -> Self {
        Self {
            side_up: true,
            high1,
            low,
            seen_high: false,
            seen_low_after_high: false,
            name_str: "v_shape_up",
        }
    }

    pub fn new_dn(high1: f64, low: f64) -> Self {
        Self {
            side_up: false,
            high1,
            low,
            seen_high: false,
            seen_low_after_high: false,
            name_str: "v_shape_dn",
        }
    }

    /// True once the full high1-then-low prefix has been observed this cycle — the
    /// strategy fires when this is set and the side's current price recovers `>= high2`.
    pub fn dipped_after_high(&self) -> bool {
        self.seen_low_after_high
    }
}

impl Signal for VShapeSignal {
    fn name(&self) -> &str {
        self.name_str
    }

    fn reset(&mut self, _ctx: &CycleContext) {
        self.seen_high = false;
        self.seen_low_after_high = false;
    }

    fn on_poly(&mut self, t: PolyTick) {
        let price = if self.side_up { t.up } else { t.dn };
        if price <= 0.0 {
            return;
        }
        if price >= self.high1 {
            self.seen_high = true;
        }
        if self.seen_high && price <= self.low {
            self.seen_low_after_high = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CycleContext;

    fn ctx() -> CycleContext {
        CycleContext {
            start_ts: 1000.0,
            end_ts: 1300.0,
            open_binance: 50_000.0,
        }
    }

    fn tick(ts: f64, up: f64) -> PolyTick {
        PolyTick {
            ts,
            up,
            dn: 1.0 - up,
            up_bid: 0.0,
            up_ask: 0.0,
        }
    }

    #[test]
    fn low_before_high_does_not_count() {
        let mut s = VShapeSignal::new_up(0.7, 0.3);
        s.reset(&ctx());
        s.on_poly(tick(1010.0, 0.25)); // dip first — no high1 yet
        assert!(!s.dipped_after_high());
        s.on_poly(tick(1020.0, 0.75)); // high1 latches now
        assert!(
            !s.dipped_after_high(),
            "recovery without a post-high dip must not complete the prefix"
        );
        s.on_poly(tick(1030.0, 0.25)); // genuine post-high dip
        assert!(s.dipped_after_high());
    }

    #[test]
    fn full_sequence_latches_and_persists() {
        let mut s = VShapeSignal::new_up(0.7, 0.3);
        s.reset(&ctx());
        s.on_poly(tick(1010.0, 0.72));
        s.on_poly(tick(1020.0, 0.28));
        assert!(s.dipped_after_high());
        // Latch persists regardless of later prices within the cycle.
        s.on_poly(tick(1030.0, 0.55));
        assert!(s.dipped_after_high());
    }

    #[test]
    fn reset_clears_both_stages() {
        let mut s = VShapeSignal::new_up(0.7, 0.3);
        s.reset(&ctx());
        s.on_poly(tick(1010.0, 0.72));
        s.on_poly(tick(1020.0, 0.28));
        assert!(s.dipped_after_high());
        s.reset(&ctx());
        assert!(!s.dipped_after_high());
        // seen_high must be gone too — a dip right after reset must not count.
        s.on_poly(tick(1310.0, 0.25));
        assert!(!s.dipped_after_high());
    }

    #[test]
    fn dn_side_reads_dn_price() {
        let mut s = VShapeSignal::new_dn(0.7, 0.3);
        s.reset(&ctx());
        s.on_poly(tick(1010.0, 0.25)); // dn = 0.75 >= high1
        s.on_poly(tick(1020.0, 0.75)); // dn = 0.25 <= low
        assert!(s.dipped_after_high());
    }

    #[test]
    fn nonpositive_price_ignored() {
        let mut s = VShapeSignal::new_up(0.7, 0.3);
        s.reset(&ctx());
        s.on_poly(tick(1010.0, 0.72));
        s.on_poly(PolyTick {
            ts: 1020.0,
            up: 0.0,
            dn: 1.0,
            up_bid: 0.0,
            up_ask: 0.0,
        }); // up=0.0 <= low but must be ignored as a non-price
        assert!(!s.dipped_after_high());
    }
}
