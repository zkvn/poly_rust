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
/// staleness within ~10s — comfortably faster than the 2026-07-10 incidents' 205s, and the
/// debounce means a restart (which briefly interrupts every asset, not just the stale one)
/// only fires on persistent, ground-truth-verified divergence.
pub const CONSECUTIVE_MISMATCHES_REQUIRED: u32 = 2;

/// How far apart the REST midpoint and the WS-cached midpoint may be (both in `[0, 1]`)
/// before counting as a mismatch — wider than normal bid/ask-spread noise between the two
/// sources.
pub const MISMATCH_TOLERANCE: f64 = 0.03;

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
        assert!(check(&mut s, cached_mid, rest_mid)); // 2nd — confirmed stale
    }
}
