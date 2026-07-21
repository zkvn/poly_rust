//! LatestPolySignal — tracks the most recent non-zero UP/DN prices + timestamp.
//! SpreadSignal — sum of latest UP + DN prices (spread gate).

use crate::signal::Signal;
use crate::types::{CycleContext, PolyTick, Side};

pub struct LatestPolySignal {
    pub up: f64,
    pub dn: f64,
    pub ts: f64,
    /// Real observed best bid/ask for the UP token (plan_unwind_5u_maker_2026-07-19
    /// §2.2's mid-vs-bid fix) — `0.0` means never observed (old publisher,
    /// backtest replay). Cached the same "only overwrite on a positive
    /// reading" way `up`/`dn` already are.
    pub up_bid: f64,
    pub up_ask: f64,
}

impl Default for LatestPolySignal {
    fn default() -> Self {
        Self::new()
    }
}

impl LatestPolySignal {
    pub fn new() -> Self {
        Self {
            up: 0.0,
            dn: 0.0,
            ts: 0.0,
            up_bid: 0.0,
            up_ask: 0.0,
        }
    }

    pub fn up(&self) -> f64 {
        self.up
    }
    pub fn dn(&self) -> f64 {
        self.dn
    }

    /// The real best bid for `side`, if ever observed — `Side::Up` reads the
    /// UP token's own bid directly; `Side::Down` derives it from the UP
    /// token's *ask* via the unified mint/merge book's complementary-token
    /// identity (`btc_5mins/doc/plan_market_maker_mvp_2026-07-19.md` §1):
    /// DOWN's bid = `1 - up_ask`. `None` means "never observed this tick /
    /// this run" — callers must fall back to the mid (`up()`/`dn()`).
    pub fn best_bid(&self, side: Side) -> Option<f64> {
        match side {
            Side::Up if self.up_bid > 0.0 => Some(self.up_bid),
            Side::Down if self.up_ask > 0.0 => Some(1.0 - self.up_ask),
            _ => None,
        }
    }

    /// The real best ask for `side`, if ever observed — mirrors `best_bid`'s
    /// shape the other way round: `Side::Up` reads the UP token's own ask
    /// directly; `Side::Down` derives it from the UP token's *bid* via the
    /// same complementary-token identity (DOWN's ask = `1 - up_bid`). `None`
    /// means "never observed this tick / this run" — callers must fall back
    /// to the mid.
    pub fn best_ask(&self, side: Side) -> Option<f64> {
        match side {
            Side::Up if self.up_ask > 0.0 => Some(self.up_ask),
            Side::Down if self.up_bid > 0.0 => Some(1.0 - self.up_bid),
            _ => None,
        }
    }

    /// Age of the last poly tick relative to `now` (seconds).
    /// Returns +inf if never received.
    pub fn age(&self, now: f64) -> f64 {
        if self.ts > 0.0 {
            now - self.ts
        } else {
            f64::INFINITY
        }
    }
}

impl Signal for LatestPolySignal {
    fn name(&self) -> &str {
        "latest_poly"
    }

    // Python: reset does NOT clear price — last known price informative across cycles.
    fn reset(&mut self, _ctx: &CycleContext) {}

    fn on_poly(&mut self, t: PolyTick) {
        if t.up > 0.0 {
            self.up = t.up;
        }
        if t.dn > 0.0 {
            self.dn = t.dn;
        }
        if t.up_bid > 0.0 {
            self.up_bid = t.up_bid;
        }
        if t.up_ask > 0.0 {
            self.up_ask = t.up_ask;
        }
        if t.ts > self.ts {
            self.ts = t.ts;
        }
    }
}

pub struct SpreadSignal {
    up: f64,
    dn: f64,
}

impl Default for SpreadSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl SpreadSignal {
    pub fn new() -> Self {
        Self { up: 0.0, dn: 0.0 }
    }

    pub fn value(&self) -> f64 {
        self.up + self.dn
    }
}

impl Signal for SpreadSignal {
    fn name(&self) -> &str {
        "spread"
    }
    fn reset(&mut self, _ctx: &CycleContext) {}
    fn on_poly(&mut self, t: PolyTick) {
        if t.up > 0.0 {
            self.up = t.up;
        }
        if t.dn > 0.0 {
            self.dn = t.dn;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_poly_tracks_price() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        });
        assert!((s.up() - 0.85).abs() < 1e-9);
        assert!((s.dn() - 0.15).abs() < 1e-9);
        assert!((s.age(102.0) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn best_bid_none_before_any_tick() {
        let s = LatestPolySignal::new();
        assert_eq!(s.best_bid(Side::Up), None);
        assert_eq!(s.best_bid(Side::Down), None);
    }

    #[test]
    fn best_bid_up_reads_up_bid_directly() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        });
        assert!((s.best_bid(Side::Up).unwrap() - 0.68).abs() < 1e-9);
    }

    /// DOWN's best bid is derived from the UP token's *ask*, per the unified
    /// mint/merge book's complementary-token identity: DOWN bid = 1 - UP ask.
    #[test]
    fn best_bid_down_derives_from_up_ask() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        });
        assert!((s.best_bid(Side::Down).unwrap() - 0.28).abs() < 1e-9); // 1 - 0.72
    }

    #[test]
    fn best_bid_none_when_bid_ask_never_observed() {
        let mut s = LatestPolySignal::new();
        // Mid-only tick (backtest replay / old publisher) — up_bid/up_ask stay 0.0.
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.0,
            up_ask: 0.0,
        });
        assert_eq!(s.best_bid(Side::Up), None);
        assert_eq!(s.best_bid(Side::Down), None);
    }

    #[test]
    fn best_bid_sticky_across_a_tick_that_only_updates_mid() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        });
        // A later tick with no fresh bid/ask (0.0 sentinel) must not clobber
        // the previously observed values — same "only overwrite on positive"
        // convention up/dn already use.
        s.on_poly(PolyTick {
            ts: 101.0,
            up: 0.71,
            dn: 0.29,
            up_bid: 0.0,
            up_ask: 0.0,
        });
        assert!((s.best_bid(Side::Up).unwrap() - 0.68).abs() < 1e-9);
        assert!((s.best_bid(Side::Down).unwrap() - 0.28).abs() < 1e-9);
    }

    #[test]
    fn best_ask_none_before_any_tick() {
        let s = LatestPolySignal::new();
        assert_eq!(s.best_ask(Side::Up), None);
        assert_eq!(s.best_ask(Side::Down), None);
    }

    #[test]
    fn best_ask_up_reads_up_ask_directly() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        });
        assert!((s.best_ask(Side::Up).unwrap() - 0.72).abs() < 1e-9);
    }

    /// DOWN's best ask is derived from the UP token's *bid*, per the unified
    /// mint/merge book's complementary-token identity: DOWN ask = 1 - UP bid.
    #[test]
    fn best_ask_down_derives_from_up_bid() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        });
        assert!((s.best_ask(Side::Down).unwrap() - 0.32).abs() < 1e-9); // 1 - 0.68
    }

    #[test]
    fn best_ask_none_when_bid_ask_never_observed() {
        let mut s = LatestPolySignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.0,
            up_ask: 0.0,
        });
        assert_eq!(s.best_ask(Side::Up), None);
        assert_eq!(s.best_ask(Side::Down), None);
    }

    #[test]
    fn spread_signal() {
        let mut s = SpreadSignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.85,
            dn: 0.16,
            up_bid: 0.0,
            up_ask: 0.0,
        });
        assert!((s.value() - 1.01).abs() < 1e-9);
    }
}
