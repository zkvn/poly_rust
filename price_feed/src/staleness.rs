//! Pure per-asset staleness *observation* core for the live Polymarket bba/price feed.
//!
//! No I/O, no tokio — mirrors trader's worker.rs "sync core, async shell" split.
//!
//! **This is observe-only: it logs, it never takes a recovery action.** An earlier version
//! of this module (`StalenessWatchdog`, still visible in git history / the
//! `wip: bba staleness watchdog` stash) tried to force an unsubscribe+resubscribe as soon as
//! one asset went silent for 5 seconds. Deployed to Oracle on 2026-07-10, it immediately
//! false-positive-fired every ~5s for nearly every asset across all three durations (5m/15m/
//! 4hr) — because `best_bid_ask`/`price_change` are *change* events, not a periodic
//! heartbeat: Polymarket only sends a message when the price actually moves, and plenty of
//! genuinely healthy stretches (untraded assets, long-duration markets, the quiet minute or
//! two right after a cycle opens before price action picks up) go well past 5s with nothing
//! to send. The result was a continuous resubscribe storm — worse than the original ~205s
//! rare-outage bug it was meant to fix. Rolled back same day; see
//! `price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md` for the full incident and the
//! redesign this module is the first phase of.
//!
//! A raw silence timer cannot distinguish "broken" from "quiet" for a change-event stream —
//! there is no threshold that's both fast enough to matter and safe against every asset's
//! and duration's normal quiet stretches. The industry-standard fix for this class of problem
//! is REST snapshot reconciliation: periodically cross-check the WS-cached value against a
//! fresh ground-truth REST poll (Polymarket's `GET /midpoint?token_id=...`), and only treat a
//! mismatch between them as real staleness — not silence duration alone. That's phase 2, not
//! implemented yet. This module is phase 1: pure telemetry, logging how long each asset's bba
//! feed has actually gone quiet in real production conditions (crossing escalating
//! thresholds, not every poll), so phase 2's reconciliation interval and the eventual
//! recovery trigger can be sized from real observed data instead of another guess.

/// Escalating silence thresholds (ms) to log at, in order — chosen wide enough that even the
/// first bucket (10s) is well past normal per-tick gaps, and spread out so a genuine long
/// outage doesn't spam the log every tick while it's happening.
pub const OBSERVE_BUCKETS_MS: [i64; 6] = [10_000, 30_000, 60_000, 120_000, 200_000, 300_000];

/// Given how many buckets have already been logged for the *current* silence episode
/// (`already_logged`, 0 if none yet) and how long the asset has now been silent
/// (`silent_ms`), returns the buckets newly crossed (oldest first — there can be more than
/// one if the caller's poll interval is coarser than the gap between buckets, so a slow
/// check doesn't silently skip a threshold) and the new `already_logged` count to store.
/// Once `already_logged == OBSERVE_BUCKETS_MS.len()`, further silence logs nothing more —
/// the caller resets `already_logged` to 0 as soon as a fresh message arrives, starting a
/// new episode.
pub fn buckets_to_log(already_logged: usize, silent_ms: i64) -> (Vec<i64>, usize) {
    let mut logged = already_logged;
    let mut newly_crossed = Vec::new();
    while logged < OBSERVE_BUCKETS_MS.len() && silent_ms >= OBSERVE_BUCKETS_MS[logged] {
        newly_crossed.push(OBSERVE_BUCKETS_MS[logged]);
        logged += 1;
    }
    (newly_crossed, logged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_logged_below_first_bucket() {
        let (crossed, logged) = buckets_to_log(0, OBSERVE_BUCKETS_MS[0] - 1);
        assert!(crossed.is_empty());
        assert_eq!(logged, 0);
    }

    #[test]
    fn logs_first_bucket_at_boundary() {
        let (crossed, logged) = buckets_to_log(0, OBSERVE_BUCKETS_MS[0]);
        assert_eq!(crossed, vec![OBSERVE_BUCKETS_MS[0]]);
        assert_eq!(logged, 1);
    }

    #[test]
    fn does_not_relog_a_bucket_already_logged() {
        // Silence is still only just past the first bucket, but already_logged says we've
        // logged it — a poll landing again in the same range must not double-log.
        let (crossed, logged) = buckets_to_log(1, OBSERVE_BUCKETS_MS[0] + 1);
        assert!(crossed.is_empty());
        assert_eq!(logged, 1);
    }

    /// A poll interval coarser than the gap between two buckets must not silently skip the
    /// one in between — both get logged in one call, in order.
    #[test]
    fn crosses_multiple_buckets_in_one_check() {
        let (crossed, logged) = buckets_to_log(0, OBSERVE_BUCKETS_MS[2] + 5);
        assert_eq!(
            crossed,
            vec![
                OBSERVE_BUCKETS_MS[0],
                OBSERVE_BUCKETS_MS[1],
                OBSERVE_BUCKETS_MS[2]
            ]
        );
        assert_eq!(logged, 3);
    }

    #[test]
    fn stops_logging_once_past_the_last_bucket() {
        let last = OBSERVE_BUCKETS_MS.len();
        let (crossed, logged) = buckets_to_log(last, OBSERVE_BUCKETS_MS[last - 1] + 999_999);
        assert!(crossed.is_empty());
        assert_eq!(logged, last);
    }

    /// A fresh message resets the caller's `already_logged` to 0 — from there, a silence
    /// episode that immediately re-crosses the first bucket must log again (not stay
    /// suppressed from the previous episode).
    #[test]
    fn fresh_episode_after_reset_logs_again() {
        let (crossed, logged) = buckets_to_log(0, OBSERVE_BUCKETS_MS[0]);
        assert_eq!(crossed, vec![OBSERVE_BUCKETS_MS[0]]);
        assert_eq!(logged, 1);
    }

    /// Golden-incident regression — DOGE, 2026-07-10: silent for 205.4s (see this module's
    /// doc comment / plan doc §1). Confirms observation would have logged every bucket up
    /// through 200s well before the real incident's own duration.
    #[test]
    fn would_have_logged_through_200s_bucket_for_the_2026_07_10_doge_gap() {
        let (crossed, logged) = buckets_to_log(0, 205_400);
        assert_eq!(crossed, vec![10_000, 30_000, 60_000, 120_000, 200_000]);
        assert_eq!(logged, 5);
    }
}
