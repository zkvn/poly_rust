//! LatestPolySignal — tracks the most recent non-zero UP/DN prices + timestamp.
//! SpreadSignal — sum of latest UP + DN prices (spread gate).

use crate::signal::Signal;
use crate::types::{CycleContext, PolyTick};

pub struct LatestPolySignal {
    pub up: f64,
    pub dn: f64,
    pub ts: f64,
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
        }
    }

    pub fn up(&self) -> f64 {
        self.up
    }
    pub fn dn(&self) -> f64 {
        self.dn
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
        });
        assert!((s.up() - 0.85).abs() < 1e-9);
        assert!((s.dn() - 0.15).abs() < 1e-9);
        assert!((s.age(102.0) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn spread_signal() {
        let mut s = SpreadSignal::new();
        s.on_poly(PolyTick {
            ts: 100.0,
            up: 0.85,
            dn: 0.16,
        });
        assert!((s.value() - 1.01).abs() < 1e-9);
    }
}
