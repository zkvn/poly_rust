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

/// Rolling cycle-over-cycle balance comparison for the Gamma-unresolved-timeout
/// override (2026-07-09) — "if balance has increased since last cycle's checkpoint,
/// don't halt on an unresolved Gamma result, keep going." Fed from the *same* periodic
/// balance sample `bin/live.rs` already fetches each cycle for `BalanceGuard` (no extra
/// API calls): each call to `record` compares the new sample against the previous one,
/// then that new sample becomes "previous" for the next cycle.
///
/// Deliberately fails safe: with fewer than two samples, or a failed fetch, `increased()`
/// returns `None` — the caller (`Worker::on_api_result_timeout`) treats `None` as "don't
/// skip the halt," matching this codebase's "halt over guess" rule (see
/// `trader/doc/incident_DOGE_wrong_result_2026-07-09.md` §4) rather than assuming growth
/// it can't actually confirm.
struct GammaBalanceState {
    last_sample: Option<f64>,
    increased: Option<bool>,
}

pub struct GammaBalanceTracker {
    state: Mutex<GammaBalanceState>,
}

impl GammaBalanceTracker {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GammaBalanceState {
                last_sample: None,
                increased: None,
            }),
        }
    }

    /// Feed one per-cycle balance sample. `None` (failed fetch) marks the verdict
    /// unknown but leaves the last good sample in place, so the next successful fetch
    /// still compares against real data rather than restarting from scratch.
    pub fn record(&self, balance: Option<f64>) {
        let mut s = self.state.lock().unwrap();
        let Some(balance) = balance else {
            s.increased = None;
            return;
        };
        s.increased = s.last_sample.map(|prev| balance > prev);
        s.last_sample = Some(balance);
    }

    /// `Some(true)` if the most recent sample was higher than the one before it,
    /// `Some(false)` if not, `None` if there isn't yet a same-baseline pair to compare
    /// (startup, or the last fetch failed).
    pub fn increased(&self) -> Option<bool> {
        self.state.lock().unwrap().increased
    }
}

impl Default for GammaBalanceTracker {
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

    #[test]
    fn gamma_tracker_first_sample_has_no_verdict() {
        let t = GammaBalanceTracker::new();
        t.record(Some(100.0));
        assert_eq!(t.increased(), None);
    }

    #[test]
    fn gamma_tracker_detects_increase_and_decrease() {
        let t = GammaBalanceTracker::new();
        t.record(Some(100.0));
        t.record(Some(105.0));
        assert_eq!(t.increased(), Some(true));
        t.record(Some(100.0));
        assert_eq!(t.increased(), Some(false));
    }

    #[test]
    fn gamma_tracker_equal_balance_is_not_an_increase() {
        let t = GammaBalanceTracker::new();
        t.record(Some(100.0));
        t.record(Some(100.0));
        assert_eq!(t.increased(), Some(false));
    }

    #[test]
    fn gamma_tracker_failed_fetch_marks_unknown_but_keeps_last_good_sample() {
        let t = GammaBalanceTracker::new();
        t.record(Some(100.0));
        t.record(Some(110.0));
        assert_eq!(t.increased(), Some(true));

        t.record(None); // failed fetch — verdict goes unknown
        assert_eq!(t.increased(), None);

        // Next successful fetch still compares against the last *good* sample (110.0),
        // not against nothing.
        t.record(Some(120.0));
        assert_eq!(t.increased(), Some(true));
        t.record(Some(90.0));
        assert_eq!(t.increased(), Some(false));
    }

    #[test]
    fn gamma_tracker_starts_unknown() {
        let t = GammaBalanceTracker::new();
        assert_eq!(t.increased(), None);
    }
}
