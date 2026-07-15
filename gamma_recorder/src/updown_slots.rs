//! Slot/slug math for Polymarket's `{asset}-updown-{duration}-{slot}` markets.
//!
//! Deliberately named for this one data type (not `slots.rs`): a future Gamma data
//! type (e.g. weather) would key its markets completely differently and would get
//! its own module rather than a forced-generic one.
//!
//! This duplicates the tiny slot/slug formula in `price_feed/src/collect.rs`
//! (`make_slug`/`current_slot_for`) rather than importing it — see
//! `gamma_recorder/doc/plan_gamma_recorder_2026-07-15.md` §5 for why: the two
//! crates share zero code by design, and this is Polymarket's own public
//! market-naming convention, not an internal detail that could drift silently.

use std::time::{SystemTime, UNIX_EPOCH};

/// The three tracked market durations, with their Gamma slug suffix and interval in seconds.
pub const DURATIONS: [(&str, u64); 3] = [("5m", 300), ("15m", 900), ("4h", 14_400)];

pub fn interval_secs(duration: &str) -> Option<u64> {
    DURATIONS
        .iter()
        .find(|(suffix, _)| *suffix == duration)
        .map(|(_, interval)| *interval)
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

/// Floor of `now` to the start of the current slot for this interval.
pub fn current_slot_for(interval: u64, now: i64) -> i64 {
    (now / interval as i64) * interval as i64
}

pub fn make_slug(asset: &str, duration: &str, slot: i64) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), duration, slot)
}

/// Parsed identity of an updown market slug: `{asset}-updown-{duration}-{slot}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlugParts {
    pub asset: String,
    pub duration: String,
    pub slot: i64,
}

/// Parses a slug like `btc-updown-5m-1784042400` into its parts. Returns `None`
/// for anything that doesn't match the exact 4-part `asset-updown-duration-slot`
/// shape with a known duration suffix — callers use this to filter bulk/backfill
/// results down to just updown markets.
pub fn parse_slug(slug: &str) -> Option<SlugParts> {
    let parts: Vec<&str> = slug.split('-').collect();
    let [asset, tag, duration, slot] = parts[..] else {
        return None;
    };
    if tag != "updown" {
        return None;
    }
    interval_secs(duration)?;
    let slot: i64 = slot.parse().ok()?;
    Some(SlugParts {
        asset: asset.to_uppercase(),
        duration: duration.to_string(),
        slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_slug() {
        let parts = parse_slug("btc-updown-5m-1784042400").unwrap();
        assert_eq!(parts.asset, "BTC");
        assert_eq!(parts.duration, "5m");
        assert_eq!(parts.slot, 1_784_042_400);
    }

    #[test]
    fn rejects_non_updown_slug() {
        assert!(parse_slug("btc-quarterly-2025-1784042400").is_none());
    }

    #[test]
    fn rejects_unknown_duration() {
        assert!(parse_slug("btc-updown-1h-1784042400").is_none());
    }

    #[test]
    fn rejects_malformed_slug() {
        assert!(parse_slug("not-a-slug").is_none());
        assert!(parse_slug("").is_none());
    }

    #[test]
    fn make_slug_roundtrips_through_parse_slug() {
        let slug = make_slug("ETH", "15m", 1_700_000_100);
        let parts = parse_slug(&slug).unwrap();
        assert_eq!(parts.asset, "ETH");
        assert_eq!(parts.duration, "15m");
        assert_eq!(parts.slot, 1_700_000_100);
    }

    #[test]
    fn current_slot_for_floors_to_interval() {
        assert_eq!(current_slot_for(300, 1_700_000_399), 1_700_000_100);
        assert_eq!(current_slot_for(300, 1_700_000_400), 1_700_000_400);
    }
}
