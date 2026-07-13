//! Weather market discovery + monitoring — deliberately **monitoring-only**, not wired
//! through `trader::machine::Machine`. Every temperature bucket really is its own
//! independent binary Yes/No CLOB market (confirmed live via Gamma, see
//! `studies/weather/weather_poly_2026-07-12.md` and `plan_weather_bot.md` §1), so
//! subscribing to one is just `spawn_poly_task`/`PolySub` again — no new plumbing needed
//! there.
//!
//! What's deliberately *not* done here: running these through `Machine`. `Machine::
//! cycle_close()` resolves a held position by comparing `last_binance` against
//! `cycle_open_binance` — correct for crypto (that comparison *is* the market's real
//! resolution rule), but wrong for weather, where the real resolution is the relevant
//! weather station's actual reading, not a price-momentum proxy. Feeding a weather bucket's
//! own price into `on_binance` (as a stand-in `delta_pct` reference, since there's no
//! Chainlink-style feed) and then calling `cycle_close()` on it would silently fabricate
//! win/loss labels from momentum instead of ground truth — worse than not testing a weather
//! strategy at all. Real weather resolution needs its own Yes/No-aware Gamma poll (`Machine`
//! only knows "UP"/"DOWN" outcome labels) and is real, deferred work — see
//! `plan_weather_bot.md` §5 Phase 2/3. Until then, this module tracks live prices and feed
//! health only, which is enough for the hourly report's "what's the market currently
//! pricing" signal without inventing fake trade outcomes.

use std::str::FromStr as _;

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::types::U256;
use tokio::sync::mpsc;

use trader::marketdata::{PolySub, now_secs_f64};
use trader::types::PolyTick;

use crate::market::StalenessTick;
use crate::snapshot::{MarketSnapshot, SharedSnapshots, update as update_snapshot};

#[derive(Debug, Clone)]
pub struct Bucket {
    pub label: String, // groupItemTitle, e.g. "33°C" or "27°C or below"
    pub yes_token: U256,
}

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
/// that needs to be exactly right — see this module's doc comment.
pub fn today_slug(city: &str) -> String {
    let now = Utc::now();
    format!(
        "highest-temperature-in-{city}-on-{}-{}-{}",
        month_name(now.month()),
        now.day(),
        now.year()
    )
}

/// Fetch one city's today event and return its buckets. `Ok(None)` (not `Err`) if the city
/// has no active event today — a normal, expected condition for some cities on some days,
/// not a failure worth aborting discovery over.
pub async fn fetch_buckets(http: &reqwest::Client, city: &str) -> Result<Option<Vec<Bucket>>> {
    let slug = today_slug(city);
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .context("gamma request")?
        .json()
        .await
        .context("gamma json")?;

    let Some(event) = resp.as_array().and_then(|a| a.first()) else {
        return Ok(None);
    };
    let Some(markets) = event["markets"].as_array() else {
        return Ok(None);
    };

    let mut buckets = Vec::new();
    for market in markets {
        let Some(token_ids_raw) = market["clobTokenIds"].as_str() else {
            continue;
        };
        let Some(outcomes_raw) = market["outcomes"].as_str() else {
            continue;
        };
        let token_ids: Vec<String> = serde_json::from_str(token_ids_raw).unwrap_or_default();
        let outcomes: Vec<String> = serde_json::from_str(outcomes_raw).unwrap_or_default();
        let label = market["groupItemTitle"].as_str().unwrap_or("?").to_string();

        let yes_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("yes"));
        if let Some(idx) = yes_idx
            && let Some(tid) = token_ids.get(idx)
            && let Ok(yes_token) = U256::from_str(tid)
        {
            buckets.push(Bucket { label, yes_token });
        }
    }
    Ok(Some(buckets))
}

/// One bucket's tick-consumer loop — spawned once per discovered bucket, one dedicated
/// mpsc channel each (simpler and more robust than fanning many buckets into one shared
/// channel and trying to disambiguate ticks post-hoc, since `PolyTick` itself doesn't carry
/// its source token id).
async fn run_bucket(
    city: String,
    label: String,
    clob: ClobWsClient,
    yes_token: U256,
    snapshots: SharedSnapshots,
    stale_tx: mpsc::UnboundedSender<StalenessTick>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<PolyTick>();
    let _sub = PolySub::start(&clob, yes_token, tx); // held for its Drop (aborts on task exit)
    let key = format!("weather:{city}:{label}");
    while let Some(tick) = rx.recv().await {
        let _ = stale_tx.send((key.clone(), "poly", (tick.ts * 1000.0) as i64));
        update_snapshot(
            &snapshots,
            &key,
            MarketSnapshot {
                kind: "weather",
                label: format!("{city}: {label}"),
                up_price: tick.up,
                dn_price: tick.dn,
                last_tick_ms: (now_secs_f64() * 1000.0) as i64,
            },
        );
    }
}

/// Entry point `main.rs` spawns once per configured city: (re)discovers today's buckets on
/// `refresh_interval_secs` (handles the day rolling over and any bucket-set changes), and
/// spawns/replaces one `run_bucket` task per discovered bucket each time. Runs forever;
/// errors are logged, not propagated, since one city's transient failure shouldn't take
/// down every other city's monitoring.
pub async fn run_city_supervisor(
    city: String,
    http: reqwest::Client,
    clob: ClobWsClient,
    snapshots: SharedSnapshots,
    stale_tx: mpsc::UnboundedSender<StalenessTick>,
    refresh_interval_secs: u64,
) {
    let mut bucket_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        refresh.tick().await;
        match fetch_buckets(&http, &city).await {
            Ok(Some(buckets)) if !buckets.is_empty() => {
                eprintln!("[weather:{city}] {} bucket(s) discovered", buckets.len());
                for t in bucket_tasks.drain(..) {
                    t.abort();
                }
                for b in buckets {
                    let (city, label, clob, snapshots, stale_tx) = (
                        city.clone(),
                        b.label.clone(),
                        clob.clone(),
                        snapshots.clone(),
                        stale_tx.clone(),
                    );
                    bucket_tasks.push(tokio::spawn(run_bucket(
                        city,
                        label,
                        clob,
                        b.yes_token,
                        snapshots,
                        stale_tx,
                    )));
                }
            }
            Ok(Some(_)) => {
                eprintln!(
                    "[weather:{city}] event found but no Yes-token buckets parsed — retrying next refresh"
                );
            }
            Ok(None) => {
                eprintln!("[weather:{city}] no active event today — retrying next refresh");
            }
            Err(e) => {
                eprintln!("[weather:{city}] discovery failed: {e:#} — retrying next refresh");
            }
        }
    }
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
