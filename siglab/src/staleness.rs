//! Per-market observe-only staleness telemetry.
//!
//! Ported from `price_feed/src/staleness.rs`'s design (not its code — that module is
//! bin-only in the `price_feed` crate, not a lib, so there's nothing to import; siglab
//! doesn't touch price_feed's files either way). The lesson that module encodes is the
//! reason this one exists in this shape: an earlier silence-timer watchdog, deployed
//! 2026-07-10, declared a feed "broken" and force-resubscribed after a fixed quiet
//! interval — and immediately false-positive-stormed, because `best_bid_ask`/
//! `price_change` are *change* events, not a heartbeat, and long quiet stretches are
//! normal, not broken. It was rolled back same day (see
//! `price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md`).
//!
//! This module only ever logs escalating silence buckets — it never unsubscribes,
//! reconnects, or takes any recovery action. Weather markets (once added) will be quiet
//! far more often than crypto, so this restraint matters even more here.
//!
//! Per `plan_weather_bot.md`'s "Thoughts on DeepSeek Review" §6: also tracks a
//! *connection-level* signal distinct from per-market silence — if every currently
//! subscribed market goes quiet past the first bucket at the same time, that's
//! statistically nothing like one market's normal quiet stretch and is worth a distinct,
//! louder log line (still observe-only — no automatic action).

use std::collections::HashMap;

/// Escalating silence thresholds (ms) to log at, in order. Same values as
/// `price_feed/src/staleness.rs::OBSERVE_BUCKETS_MS` — chosen wide enough that even the
/// first bucket (10s) is well past a normal per-tick gap on a liquid crypto market, and
/// spread out so a genuine long outage doesn't spam the log every poll while it's
/// happening.
pub const OBSERVE_BUCKETS_MS: [i64; 6] = [10_000, 30_000, 60_000, 120_000, 200_000, 300_000];

/// Given how many buckets have already been logged for the *current* silence episode
/// (`already_logged`, 0 if none yet) and how long the market has now been silent
/// (`silent_ms`), returns the buckets newly crossed (oldest first) and the new
/// `already_logged` count to store. Once `already_logged == OBSERVE_BUCKETS_MS.len()`,
/// further silence logs nothing more — reset `already_logged` to 0 as soon as a fresh
/// tick arrives, starting a new episode.
pub fn buckets_to_log(already_logged: usize, silent_ms: i64) -> (Vec<i64>, usize) {
    let mut logged = already_logged;
    let mut newly_crossed = Vec::new();
    while logged < OBSERVE_BUCKETS_MS.len() && silent_ms >= OBSERVE_BUCKETS_MS[logged] {
        newly_crossed.push(OBSERVE_BUCKETS_MS[logged]);
        logged += 1;
    }
    (newly_crossed, logged)
}

/// Tracks last-tick time and logged-bucket count per market key (e.g. "BTC-5m"), for
/// however many ticks (poly or binance) siglab wants to watch. Pure/no I/O — the caller
/// decides how to surface `poll()`'s output (eprintln, a log crate, whatever).
#[derive(Default)]
pub struct StalenessTracker {
    last_tick_ms: HashMap<String, i64>,
    logged: HashMap<String, usize>,
}

pub struct StaleEvent {
    pub market: String,
    pub silent_ms: i64,
    pub bucket_ms: i64,
}

impl StalenessTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Call whenever a tick arrives for `market` — resets its silence episode.
    pub fn on_tick(&mut self, market: &str, now_ms: i64) {
        self.last_tick_ms.insert(market.to_string(), now_ms);
        self.logged.insert(market.to_string(), 0);
    }

    /// Call periodically (e.g. every few seconds) for every currently-tracked market.
    /// Returns newly-crossed stale events (possibly more than one per market per call, if
    /// the poll interval is coarser than the gap between buckets) plus, separately, a
    /// correlated-silence warning **per market class** (`classify` maps a market key to a
    /// class label, e.g. "crypto"/"weather") when that class's fraction of markets past
    /// their first bucket exceeds `correlated_threshold` (0.0-1.0) and the class has more
    /// than one market — a single-market class has no "correlated" signal to distinguish.
    ///
    /// Classing matters: an earlier version computed one ratio across every tracked market
    /// regardless of type, and in local testing with ~50 fast-ticking crypto feeds mixed
    /// into ~300 naturally-quiet weather bucket feeds, weather's normal quiet stretches
    /// alone pushed the *combined* ratio over threshold — a false "connection dead" warning
    /// while nothing was actually wrong. Different market classes have wildly different
    /// normal tick cadences, so they need separate baselines, not one shared one.
    pub fn poll(
        &mut self,
        now_ms: i64,
        correlated_threshold: f64,
        classify: impl Fn(&str) -> &'static str,
    ) -> (Vec<StaleEvent>, Vec<(&'static str, bool)>) {
        let mut events = Vec::new();
        let mut past_first_bucket: HashMap<&'static str, usize> = HashMap::new();
        let mut total: HashMap<&'static str, usize> = HashMap::new();

        for (market, &last) in self.last_tick_ms.iter() {
            let class = classify(market);
            *total.entry(class).or_insert(0) += 1;
            let silent_ms = now_ms - last;
            let already = *self.logged.get(market).unwrap_or(&0);
            let (crossed, new_logged) = buckets_to_log(already, silent_ms);
            if !crossed.is_empty() {
                self.logged.insert(market.clone(), new_logged);
                for bucket_ms in crossed {
                    events.push(StaleEvent {
                        market: market.clone(),
                        silent_ms,
                        bucket_ms,
                    });
                }
            }
            if silent_ms >= OBSERVE_BUCKETS_MS[0] {
                *past_first_bucket.entry(class).or_insert(0) += 1;
            }
        }

        let correlated = total
            .into_iter()
            .map(|(class, n)| {
                let stale_n = past_first_bucket.get(class).copied().unwrap_or(0);
                let is_correlated = n > 1 && (stale_n as f64 / n as f64) >= correlated_threshold;
                (class, is_correlated)
            })
            .collect();
        (events, correlated)
    }
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
    fn crosses_multiple_buckets_in_one_poll() {
        let (crossed, logged) = buckets_to_log(0, OBSERVE_BUCKETS_MS[2] + 1);
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
    fn stops_at_last_bucket() {
        let (crossed, logged) = buckets_to_log(OBSERVE_BUCKETS_MS.len(), 10_000_000);
        assert!(crossed.is_empty());
        assert_eq!(logged, OBSERVE_BUCKETS_MS.len());
    }

    fn classify_all_same(_market: &str) -> &'static str {
        "class"
    }

    #[test]
    fn tracker_resets_on_tick() {
        let mut t = StalenessTracker::new();
        t.on_tick("BTC-5m", 0);
        let (events, _) = t.poll(15_000, 1.0, classify_all_same);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].bucket_ms, 10_000);

        // Fresh tick resets the episode — no further event at the same silence duration.
        t.on_tick("BTC-5m", 15_000);
        let (events2, _) = t.poll(20_000, 1.0, classify_all_same);
        assert!(events2.is_empty());
    }

    #[test]
    fn correlated_silence_needs_multiple_markets() {
        let mut t = StalenessTracker::new();
        t.on_tick("BTC-5m", 0);
        let (_, correlated) = t.poll(15_000, 1.0, classify_all_same);
        assert!(
            correlated.iter().all(|(_, c)| !c),
            "single market can't be 'correlated'"
        );
    }

    #[test]
    fn correlated_silence_fires_when_all_markets_quiet() {
        let mut t = StalenessTracker::new();
        t.on_tick("BTC-5m", 0);
        t.on_tick("ETH-5m", 0);
        let (_, correlated) = t.poll(15_000, 1.0, classify_all_same);
        assert!(correlated.iter().any(|(_, c)| *c));
    }

    #[test]
    fn correlated_silence_does_not_fire_when_only_one_of_many_is_quiet() {
        let mut t = StalenessTracker::new();
        t.on_tick("BTC-5m", 0);
        t.on_tick("ETH-5m", 15_000); // fresh
        let (_, correlated) = t.poll(15_000, 0.75, classify_all_same); // 1/2 stale < 0.75 threshold
        assert!(correlated.iter().all(|(_, c)| !c));
    }

    #[test]
    fn correlated_silence_is_scoped_per_class_not_mixed() {
        // Regression test for the false-alarm bug caught in local testing: a fast-ticking
        // crypto market and a quiet weather bucket sharing one un-classed ratio would push
        // the combined fraction over threshold even though only one class is actually
        // quiet. Classing them separately must not flag the fresh class.
        fn classify(market: &str) -> &'static str {
            if market.starts_with("weather:") {
                "weather"
            } else {
                "crypto"
            }
        }
        let mut t = StalenessTracker::new();
        t.on_tick("BTC-5m", 15_000); // crypto: fresh, not stale
        t.on_tick("ETH-5m", 15_000); // crypto: fresh, not stale
        t.on_tick("weather:tokyo:30C", 0); // weather: stale
        t.on_tick("weather:london:20C", 0); // weather: stale
        let (_, correlated) = t.poll(15_000, 1.0, classify);
        let map: HashMap<_, _> = correlated.into_iter().collect();
        assert_eq!(map.get("crypto"), Some(&false));
        assert_eq!(map.get("weather"), Some(&true));
    }
}
