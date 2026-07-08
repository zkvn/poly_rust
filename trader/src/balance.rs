// BalanceGuard — ports bot/trading.py BalanceGuard.
//
// Monitors Polymarket USDC balance once per cycle (+2min into each 5-min
// window). Fires on_halt() when drawdown from the session-start baseline
// exceeds 25%. Fails open: a failed fetch skips the check without halting.
// Halt/resume here are the same no-entry-gate semantics as §8 of the plan —
// this module only decides *when* to call on_halt, not what the halt does.

use std::sync::Mutex;

const DRAWDOWN_LIMIT: f64 = 0.25;
const CHECK_OFFSET_SECS: u64 = 120;
const WINDOW_SECS: u64 = 300;

struct GuardState {
    initial_balance: Option<f64>,
    halt_fired: bool,
}

pub struct BalanceGuard {
    state: Mutex<GuardState>,
}

impl BalanceGuard {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GuardState {
                initial_balance: None,
                halt_fired: false,
            }),
        }
    }

    /// Reset baseline so the next check adopts the current balance as the new floor
    /// (called on `/resume`).
    pub fn reset_baseline(&self) {
        let mut s = self.state.lock().unwrap();
        s.initial_balance = None;
        s.halt_fired = false;
    }

    /// Feed one balance sample. Returns `true` exactly once per session the first
    /// time drawdown crosses the limit — the caller should treat that as "halt now".
    /// A `None` sample (failed fetch) is a no-op (fail-open).
    pub fn check(&self, balance: Option<f64>) -> bool {
        let Some(balance) = balance else { return false };
        let mut s = self.state.lock().unwrap();
        let Some(initial) = s.initial_balance else {
            s.initial_balance = Some(balance);
            return false;
        };
        if initial <= 0.0 {
            return false;
        }
        let drawdown = (initial - balance) / initial;
        if drawdown > DRAWDOWN_LIMIT && !s.halt_fired {
            s.halt_fired = true;
            return true;
        }
        false
    }

    pub fn is_halted(&self) -> bool {
        self.state.lock().unwrap().halt_fired
    }

    pub fn baseline(&self) -> Option<f64> {
        self.state.lock().unwrap().initial_balance
    }
}

impl Default for BalanceGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Seconds to sleep until the next check point (window_start + 120s), matching
/// `_sleep_to_next_check`. `now` and the result are both Unix seconds.
pub fn seconds_until_next_check(now: f64) -> f64 {
    let window_start = ((now as u64) / WINDOW_SECS) * WINDOW_SECS;
    let mut check_time = window_start + CHECK_OFFSET_SECS;
    if (check_time as f64) <= now {
        check_time += WINDOW_SECS as u32 as u64;
    }
    check_time as f64 - now
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_check_sets_baseline_without_halting() {
        let g = BalanceGuard::new();
        assert!(!g.check(Some(100.0)));
        assert_eq!(g.baseline(), Some(100.0));
    }

    #[test]
    fn halts_once_drawdown_exceeds_25_percent() {
        let g = BalanceGuard::new();
        g.check(Some(100.0)); // baseline
        assert!(!g.check(Some(80.0))); // 20% drawdown, under limit
        assert!(g.check(Some(70.0))); // 30% drawdown, fires once
        assert!(g.is_halted());
    }

    #[test]
    fn fires_only_once_per_session() {
        let g = BalanceGuard::new();
        g.check(Some(100.0));
        assert!(g.check(Some(50.0))); // fires
        assert!(!g.check(Some(40.0))); // already fired, no repeat
    }

    #[test]
    fn reset_baseline_rearms_and_clears_halt() {
        let g = BalanceGuard::new();
        g.check(Some(100.0));
        assert!(g.check(Some(50.0)));
        assert!(g.is_halted());

        g.reset_baseline();
        assert!(!g.is_halted());
        assert_eq!(g.baseline(), None);

        // Next check adopts the new baseline.
        assert!(!g.check(Some(50.0)));
        assert_eq!(g.baseline(), Some(50.0));
    }

    #[test]
    fn failed_fetch_is_a_no_op() {
        let g = BalanceGuard::new();
        assert!(!g.check(None));
        assert_eq!(g.baseline(), None);
    }

    #[test]
    fn check_offset_math() {
        // window starts at t=1000*300=300000... use a concrete example instead.
        let window_start = 1_782_000_000u64 / WINDOW_SECS * WINDOW_SECS;
        let now = window_start as f64 + 10.0; // 10s into the window
        let secs = seconds_until_next_check(now);
        assert!((secs - 110.0).abs() < 1e-9); // 120 - 10
    }

    #[test]
    fn check_offset_wraps_to_next_window_if_past_checkpoint() {
        let window_start = 1_782_000_000u64 / WINDOW_SECS * WINDOW_SECS;
        let now = window_start as f64 + 200.0; // past the +120s checkpoint
        let secs = seconds_until_next_check(now);
        assert!((secs - 220.0).abs() < 1e-9); // (300+120) - 200
    }
}
