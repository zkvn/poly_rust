//! How a crypto market's slug is computed and how often it rotates. Two shapes exist on
//! Polymarket and they use genuinely different slug formats — not a detail worth hiding
//! behind one "just pass a suffix" abstraction:
//!
//! - **Periodic** (5m/15m/4h): `{asset}-updown-{suffix}-{slot}`, `slot` a Unix-epoch
//!   multiple of the period. `trader::marketdata::make_slug`/`current_slot` already do this.
//! - **Hourly, US-Eastern-calendar-hour** (the `polymarket.com/crypto/hourly` markets):
//!   `{coin_name}-up-or-down-{month}-{day}-{year}-{h}{am|pm}-et` — a human-readable ET
//!   date+hour, using the coin's **full name** (`bitcoin`, not `btc`), not a slot number.
//!   Verified live 2026-07-13 (see `siglab/config/markets.toml`'s hourly section) after
//!   initially missing this market family entirely — the periodic slug pattern silently
//!   404s for it (`btc-updown-1h-*`, `btc-updown-60m-*` etc. all empty), which is why it
//!   needed its own discovery path instead of reusing `make_slug`.
//!
//! Both shapes resolve to the same simple `["Up","Down"]` two-market shape once fetched
//! (confirmed for BTC/ETH/SOL/XRP/DOGE), so nothing downstream of slug construction needs
//! to know which kind a given market is — `Machine`/gates/signals are identical either way.

use chrono::{Datelike, TimeZone as _, Timelike};
use trader::marketdata::{current_slot, make_slug};

#[derive(Debug, Clone)]
pub enum Rotation {
    Periodic { suffix: String, period_secs: u64 },
    HourlyEt { coin_name: String },
}

impl Rotation {
    pub fn period_secs(&self) -> u64 {
        match self {
            Rotation::Periodic { period_secs, .. } => *period_secs,
            Rotation::HourlyEt { .. } => 3600,
        }
    }

    /// A short, unique-per-rotation-kind label for this market, used in staleness keys,
    /// snapshots, and log lines — e.g. `"BTC-5m"` or `"BTC-hourly-et"`.
    pub fn market_key(&self, asset: &str) -> String {
        match self {
            Rotation::Periodic { suffix, .. } => format!("{asset}-{suffix}"),
            Rotation::HourlyEt { .. } => format!("{asset}-hourly-et"),
        }
    }

    /// Returns `(slot_id, slug)` for "now". `slot_id` only needs to change when the slug
    /// should rotate — its absolute value is otherwise meaningless (an epoch second for
    /// `Periodic`, an epoch second of the ET hour-start for `HourlyEt`).
    pub fn current_slot_and_slug(&self, asset: &str) -> (i64, String) {
        match self {
            Rotation::Periodic {
                suffix,
                period_secs,
            } => {
                let slot = current_slot(*period_secs);
                (slot as i64, make_slug(asset, slot, suffix))
            }
            Rotation::HourlyEt { coin_name } => {
                let now_et = chrono::Utc::now().with_timezone(&chrono_tz::America::New_York);
                let hour_start = chrono_tz::America::New_York
                    .with_ymd_and_hms(
                        now_et.year(),
                        now_et.month(),
                        now_et.day(),
                        now_et.hour(),
                        0,
                        0,
                    )
                    .single()
                    .unwrap_or(now_et);
                let slot = hour_start.timestamp();
                let h24 = now_et.hour();
                let h12 = match h24 % 12 {
                    0 => 12,
                    h => h,
                };
                let ampm = if h24 < 12 { "am" } else { "pm" };
                let slug = format!(
                    "{coin_name}-up-or-down-{}-{}-{}-{h12}{ampm}-et",
                    now_et.format("%B").to_string().to_lowercase(),
                    now_et.day(),
                    now_et.year(),
                );
                (slot, slug)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn periodic_market_key() {
        let r = Rotation::Periodic {
            suffix: "5m".to_string(),
            period_secs: 300,
        };
        assert_eq!(r.market_key("BTC"), "BTC-5m");
        assert_eq!(r.period_secs(), 300);
    }

    #[test]
    fn hourly_et_market_key_and_period() {
        let r = Rotation::HourlyEt {
            coin_name: "bitcoin".to_string(),
        };
        assert_eq!(r.market_key("BTC"), "BTC-hourly-et");
        assert_eq!(r.period_secs(), 3600);
    }

    #[test]
    fn hourly_et_slug_has_expected_shape() {
        let r = Rotation::HourlyEt {
            coin_name: "bitcoin".to_string(),
        };
        let (_slot, slug) = r.current_slot_and_slug("BTC");
        assert!(slug.starts_with("bitcoin-up-or-down-"));
        assert!(slug.ends_with("-et"));
        assert!(slug.contains("am-et") || slug.contains("pm-et"));
    }
}
