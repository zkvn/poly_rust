//! Weather market discovery — thin wrapper over `event_monitor`'s shared discovery/
//! monitoring core. See `event_monitor.rs`'s doc comment for why this is monitoring-only
//! (not run through `Machine`) and why subscriptions are batched per event, not per bucket.
//!
//! The only thing specific to weather: each city's event slug is date-derived
//! (`highest-temperature-in-{city}-on-{month}-{day}-{year}`) and must be recomputed on every
//! refresh, unlike `worldcup.rs`'s fixed slugs.

use chrono::{Datelike, Utc};

use crate::event_monitor::{EventIdentity, EventSinks, run_event_supervisor};
use crate::market::SharedClients;

fn month_name(m: u32) -> &'static str {
    const NAMES: [&str; 12] = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    NAMES[(m as usize).saturating_sub(1).min(11)]
}

/// Today's event slug for `city`, UTC-date-based. Polymarket's weather events span a
/// multi-hour local resolution window (not exactly UTC midnight), so this is an
/// approximation that self-corrects on the next hourly `refresh` rather than something
/// that needs to be exactly right.
pub fn today_slug(city: &str) -> String {
    let now = Utc::now();
    format!(
        "highest-temperature-in-{city}-on-{}-{}-{}",
        month_name(now.month()),
        now.day(),
        now.year()
    )
}

/// Entry point `main.rs` spawns once per configured city.
pub async fn run_city_supervisor(
    city: String,
    clients: SharedClients,
    sinks: EventSinks,
    refresh_interval_secs: u64,
) {
    let identity = EventIdentity {
        log_key: format!("weather:{city}"),
        snapshot_prefix: format!("weather:{city}"),
        kind: "weather",
        display_name: city.clone(),
    };
    let slug_fn = move || today_slug(&city);
    run_event_supervisor(identity, slug_fn, clients, sinks, refresh_interval_secs).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_name_covers_all_twelve() {
        assert_eq!(month_name(1), "january");
        assert_eq!(month_name(7), "july");
        assert_eq!(month_name(12), "december");
    }

    #[test]
    fn today_slug_has_expected_shape() {
        let slug = today_slug("hong-kong");
        assert!(slug.starts_with("highest-temperature-in-hong-kong-on-"));
        // e.g. "highest-temperature-in-hong-kong-on-july-13-2026"
        let parts: Vec<&str> = slug.rsplitn(3, '-').collect();
        assert_eq!(parts.len(), 3);
        assert!(parts[0].parse::<u32>().is_ok(), "year should be numeric");
    }
}
