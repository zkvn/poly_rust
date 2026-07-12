//! REST `/midpoint` ground-truth reconciliation for the bba price feed — phase 2 of
//! `price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md`.
//!
//! Phase 1 (`staleness.rs`) is a pure silence-duration *logger*. This module is the actual
//! detector: unlike a silence timer (which false-positive-stormed in production — see
//! `staleness.rs`'s doc comment), this only ever declares an asset stale when a fresh REST
//! poll's price *disagrees* with the WS-cached value beyond tolerance, debounced over
//! several consecutive polls. A genuinely quiet market's REST poll still agrees with the
//! (unchanged) cache, so this cannot false-positive on a quiet period no matter how long it
//! runs — the property a raw silence timer structurally cannot have.
//!
//! Recovery mechanism: on a confirmed mismatch, log and exit the process — not a surgical
//! per-asset unsubscribe. Investigated whether a targeted `unsubscribe_orderbook` +
//! resubscribe for just the stale asset could work (matching the *mechanism* — as opposed to
//! trigger — of the plan's original §4 design): `polymarket_client_sdk_v2` refcounts
//! subscriptions per asset ID *across every independent subscriber* sharing the connection —
//! in this codebase, one asset's up-token is referenced by 4 separate registrations
//! (`spawn_book_task`'s `subscribe_orderbook`, `spawn_bba_task`'s `subscribe_best_bid_ask`
//! *and* `subscribe_prices`, `spawn_trade_task`'s `subscribe_last_trade_price`), not just the
//! 2 the original design assumed. The SDK exposes no public per-asset refcount accessor
//! (only `active_subscriptions()`'s full registration list, from which the count *could* be
//! derived, but that adds real fragility for something this codebase already has a
//! well-established, simple answer for). `price_feed::collect::run()` already treats a
//! failed NATS connect as fatal, relying on `Restart=always`/`RestartSec=5` (see
//! `poly-collector.service`) to recover — reusing that exact pattern here is simple, matches
//! existing convention, and is trivially correct instead of cleverly-maybe-correct.

use anyhow::{Context as _, Result};
use polymarket_client_sdk_v2::types::U256;

/// Default REST reconciliation poll interval, seconds — override via
/// `price_feed collect --midpoint-poll-secs <N>`.
pub const DEFAULT_POLL_SECS: u64 = 5;

/// Consecutive mismatches required before declaring confirmed staleness — guards against a
/// single transient hiccup (a momentary CDN/cache lag on Polymarket's REST side, or an
/// unlucky race where the WS updates a moment after our REST call fires) restarting the
/// whole collector process needlessly. At the default 5s poll interval this confirms genuine
/// staleness within ~15s — comfortably faster than the 2026-07-10 incidents' 205s, and the
/// debounce means a restart (which briefly interrupts every asset, not just the stale one)
/// only fires on persistent, ground-truth-verified divergence.
///
/// Raised 2 -> 3 (`price_feed/doc/incident_collector_data_loss_2026-07-12.md`): at 2, this
/// mechanism alone triggered 179 restarts in under 2 days, most plausibly benign
/// near-resolution price divergence (see `NEAR_CLOSE_SKIP_SECS`) rather than genuine feed
/// failures — one extra confirmation cheaply cuts transient/borderline cases without giving
/// up meaningfully on detection speed for a real, sustained gap.
pub const CONSECUTIVE_MISMATCHES_REQUIRED: u32 = 3;

/// How far apart the REST midpoint and the WS-cached midpoint may be (both in `[0, 1]`)
/// before counting as a mismatch — wider than normal bid/ask-spread noise between the two
/// sources.
///
/// Widened 0.03 -> 0.04 same incident/reasoning as `CONSECUTIVE_MISMATCHES_REQUIRED` above.
pub const MISMATCH_TOLERANCE: f64 = 0.04;

/// This project's only reconciled market length — `spawn_reconcile_task` (`collect.rs`) is
/// only ever wired to the 5-min feed's slot channel, never 15m/4h, so every slug it evaluates
/// is a `-5m-` cycle.
pub const CYCLE_LENGTH_SECS: f64 = 300.0;

/// Skip the reconcile check entirely in the final stretch before a cycle closes. A market's
/// true price legitimately crashes toward 0 or 1 as the outcome becomes near-certain, and the
/// order book often goes thin/quiet right then too (few market makers still quoting this close
/// to resolution) — which can make the WS-cached mid lag the REST mid for a few ticks even
/// though nothing is actually broken. Found 2026-07-12
/// (`price_feed/doc/incident_collector_data_loss_2026-07-12.md`): a large fraction of this
/// mechanism's 179 restarts in <2 days showed exactly this shape (`rest_mid` near 0 or 1,
/// recurring across many *different*, unrelated assets at essentially random times) —
/// consistent with benign near-resolution divergence being treated as a real feed failure far
/// more often than the ~1-incident/week this was designed for.
pub const NEAR_CLOSE_SKIP_SECS: f64 = 10.0;

/// Seconds remaining until `slug`'s own cycle closes, as of `now` (both wall-clock unix
/// seconds) — `None` if `slug` doesn't parse as a `..-<cycle_start>` slug (defensive; the
/// caller treats an unparseable slug the same as "not near close," never suppressing recovery
/// outright just because a slug looked unusual).
pub fn seconds_until_cycle_close(slug: &str, now: f64) -> Option<f64> {
    let cycle_start: f64 = slug.rsplit('-').next()?.parse().ok()?;
    Some(cycle_start + CYCLE_LENGTH_SECS - now)
}

/// Whether `slug`'s cycle is within `NEAR_CLOSE_SKIP_SECS` of closing — including already past
/// its nominal close (`secs_left <= 0`), since a cycle can linger a moment past close before
/// the next slot rotation lands and the WS/REST divergence risk doesn't disappear at exactly
/// `secs_left == 0`.
pub fn is_near_cycle_close(slug: &str, now: f64) -> bool {
    match seconds_until_cycle_close(slug, now) {
        Some(secs_left) => secs_left <= NEAR_CLOSE_SKIP_SECS,
        None => false,
    }
}

/// Per-asset debounce state for the reconciliation loop.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReconcileState {
    consecutive_mismatches: u32,
}

/// Pure decision core (no I/O) — mirrors `staleness.rs`'s sync-core/async-shell split.
/// Returns `true` (and resets the debounce) exactly when the caller should treat this asset
/// as confirmed-stale; any in-tolerance reading resets the counter immediately, so recovering
/// from a real gap and then going quiet again doesn't leave a stale partial count around.
pub fn check(state: &mut ReconcileState, cached_mid: f64, rest_mid: f64) -> bool {
    if (cached_mid - rest_mid).abs() <= MISMATCH_TOLERANCE {
        state.consecutive_mismatches = 0;
        return false;
    }
    state.consecutive_mismatches += 1;
    if state.consecutive_mismatches >= CONSECUTIVE_MISMATCHES_REQUIRED {
        state.consecutive_mismatches = 0;
        return true;
    }
    false
}

/// Parses Polymarket's `GET /midpoint` response body. Verified live 2026-07-10 (`curl
/// 'https://clob.polymarket.com/midpoint?token_id=...'` → `{"mid":"0.125"}`) — the field is
/// `mid`, **not** `mid_price` as `docs.polymarket.com/api-reference/data/get-midpoint-price`
/// states; trust the verified live response over the docs page.
pub fn parse_midpoint_response(body: &str) -> Result<f64> {
    let v: serde_json::Value = serde_json::from_str(body).context("parse midpoint json")?;
    let s = v["mid"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("midpoint response missing string 'mid' field: {body}"))?;
    s.parse::<f64>().context("'mid' not a valid float")
}

pub async fn fetch_midpoint(http: &reqwest::Client, token_id: U256) -> Result<f64> {
    let url = format!("https://clob.polymarket.com/midpoint?token_id={token_id}");
    let body = http
        .get(&url)
        .send()
        .await
        .context("midpoint request")?
        .text()
        .await
        .context("midpoint body")?;
    parse_midpoint_response(&body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_tolerance_is_not_a_mismatch() {
        let mut s = ReconcileState::default();
        assert!(!check(&mut s, 0.50, 0.50 + MISMATCH_TOLERANCE));
    }

    #[test]
    fn just_over_tolerance_counts_as_mismatch_but_needs_debounce() {
        let mut s = ReconcileState::default();
        assert!(!check(&mut s, 0.50, 0.50 + MISMATCH_TOLERANCE + 0.001));
    }

    #[test]
    fn confirms_after_required_consecutive_mismatches() {
        let mut s = ReconcileState::default();
        for _ in 0..CONSECUTIVE_MISMATCHES_REQUIRED - 1 {
            assert!(!check(&mut s, 0.50, 0.90));
        }
        assert!(check(&mut s, 0.50, 0.90));
    }

    #[test]
    fn counter_resets_after_confirming_so_next_episode_needs_full_debounce_again() {
        let mut s = ReconcileState::default();
        for _ in 0..CONSECUTIVE_MISMATCHES_REQUIRED {
            check(&mut s, 0.50, 0.90);
        }
        // Immediately after confirming, a single further mismatch must not re-confirm —
        // the debounce counter should have reset, not kept accumulating.
        assert!(!check(&mut s, 0.50, 0.90));
    }

    /// An in-tolerance reading between two mismatches resets the debounce — two mismatches
    /// that aren't actually consecutive must not confirm staleness.
    #[test]
    fn an_in_tolerance_reading_resets_the_debounce() {
        let mut s = ReconcileState::default();
        assert!(!check(&mut s, 0.50, 0.90)); // mismatch 1
        assert!(!check(&mut s, 0.50, 0.51)); // back in tolerance — resets
        assert!(!check(&mut s, 0.50, 0.90)); // mismatch 1 again, not 2
    }

    #[test]
    fn parses_real_verified_response_shape() {
        assert_eq!(
            parse_midpoint_response(r#"{"mid":"0.125"}"#).unwrap(),
            0.125
        );
    }

    #[test]
    fn rejects_missing_mid_field() {
        assert!(
            parse_midpoint_response(
                r#"{"error":"No orderbook exists for the requested token id"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_non_numeric_mid() {
        assert!(parse_midpoint_response(r#"{"mid":"not-a-number"}"#).is_err());
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(parse_midpoint_response("not json at all").is_err());
    }

    /// Golden-incident regression — DOGE, 2026-07-10: WS cache frozen at up=0.4300 for the
    /// whole cycle while the real market moved to 0.8650+ (see the plan doc's incident
    /// table). A REST poll during that freeze would have returned something close to the
    /// real price, not the frozen cache — confirms this would have been flagged.
    #[test]
    fn would_have_flagged_the_2026_07_10_doge_gap() {
        let mut s = ReconcileState::default();
        let cached_mid = 0.4300; // frozen WS cache during the real incident
        let rest_mid = 0.8650; // the real price at the time, per python's independent feed
        assert!(!check(&mut s, cached_mid, rest_mid)); // 1st confirmation
        assert!(!check(&mut s, cached_mid, rest_mid)); // 2nd
        assert!(check(&mut s, cached_mid, rest_mid)); // 3rd — confirmed stale
    }

    /// Golden-incident regression — 2026-07-12: this exact shape (rest_mid near zero,
    /// recurring across many unrelated assets) drove 179 restarts in <2 days. A single
    /// isolated near-zero reading, one poll apart, must not confirm at the new threshold —
    /// distinguishes "briefly noisy near resolution" from "persistently stale."
    #[test]
    fn a_single_near_zero_blip_does_not_confirm_at_the_raised_threshold() {
        let mut s = ReconcileState::default();
        assert!(!check(&mut s, 0.11, 0.005));
        assert!(!check(&mut s, 0.11, 0.005));
        // Would have confirmed at the old CONSECUTIVE_MISMATCHES_REQUIRED=2; must not here.
    }

    #[test]
    fn seconds_until_cycle_close_computes_from_slug_trailing_timestamp() {
        // eth-updown-5m-1000 -> cycle_start=1000, closes at 1300.
        assert_eq!(
            seconds_until_cycle_close("eth-updown-5m-1000", 1290.0),
            Some(10.0)
        );
        assert_eq!(
            seconds_until_cycle_close("eth-updown-5m-1000", 1000.0),
            Some(300.0)
        );
    }

    #[test]
    fn seconds_until_cycle_close_none_for_unparseable_slug() {
        assert_eq!(seconds_until_cycle_close("not-a-slug-", 1000.0), None);
        assert_eq!(seconds_until_cycle_close("", 1000.0), None);
    }

    #[test]
    fn is_near_cycle_close_true_inside_the_skip_window() {
        // closes at 1300; 1291 is 9s out, inside the 10s window.
        assert!(is_near_cycle_close("eth-updown-5m-1000", 1291.0));
    }

    #[test]
    fn is_near_cycle_close_true_exactly_at_the_boundary() {
        assert!(is_near_cycle_close("eth-updown-5m-1000", 1290.0)); // exactly 10s out
    }

    #[test]
    fn is_near_cycle_close_false_outside_the_skip_window() {
        // closes at 1300; 1289 is 11s out, just outside the 10s window.
        assert!(!is_near_cycle_close("eth-updown-5m-1000", 1289.0));
    }

    #[test]
    fn is_near_cycle_close_true_past_nominal_close() {
        assert!(is_near_cycle_close("eth-updown-5m-1000", 1305.0)); // 5s past close
    }

    #[test]
    fn is_near_cycle_close_false_for_unparseable_slug_never_suppresses_recovery() {
        assert!(!is_near_cycle_close("garbage", 1000.0));
    }
}
