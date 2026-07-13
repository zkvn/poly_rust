//! Weather market discovery + monitoring — deliberately **monitoring-only**, not wired
//! through `trader::machine::Machine`. Every temperature bucket really is its own
//! independent binary Yes/No CLOB market (confirmed live via Gamma, see
//! `studies/weather/weather_poly_2026-07-12.md` and `plan_weather_bot.md` §1), so
//! subscribing to one is structurally the same as any crypto Up/Down token.
//!
//! What's deliberately *not* done here: running these through `Machine`. `Machine::
//! cycle_close()` resolves a held position by comparing `last_binance` against
//! `cycle_open_binance` — correct for crypto (that comparison *is* the market's real
//! resolution rule), but wrong for weather, where the real resolution is the relevant
//! weather station's actual reading, not a price-momentum proxy. Real weather resolution
//! needs its own Yes/No-aware Gamma poll (`Machine` only knows "UP"/"DOWN" outcome labels)
//! and is real, deferred work — see `plan_weather_bot.md` §5 Phase 2/3. Until then, this
//! module tracks live prices and feed health only.
//!
//! **Subscription strategy — batched per city, not one call per bucket.** An earlier
//! version called `PolySub::start`/`spawn_poly_task` once per bucket token (~525 tokens
//! across 51 cities → ~1,050 individual `subscribe_best_bid_ask`/`subscribe_prices` calls).
//! That caused sustained 200-370% CPU in a live Docker run — traced to
//! `polymarket_client_sdk_v2`'s `ConnectionManager`, which holds one `broadcast::channel`
//! per WS connection and hands every `subscribe_*()` call its own receiver on that *same*
//! channel, filtering client-side; with ~1,050 receivers all filtering the same broadcast,
//! cost is O(subscriptions × message rate). Polymarket's own WS docs
//! (docs.polymarket.com/developers/CLOB/websocket/wss-overview, checked 2026-07-13) confirm
//! the market channel is designed for **one connection subscribed to many `assets_ids` at
//! once**, modifiable without reconnecting — `price_feed/src/collect.rs`'s
//! `spawn_bba_task`/`spawn_book_task` already follow this (one batched subscribe call per
//! poll cycle, demuxed locally by `asset_id`). This module now follows the same pattern:
//! one `subscribe_best_bid_ask`/`subscribe_prices` call per city (not per bucket), covering
//! that city's ~11 tokens in one call each, demultiplexed by `asset_id` into per-bucket
//! snapshots/staleness updates. ~51 cities × 2 calls ≈ 102 subscriptions instead of ~1,050
//! — a ~10x reduction, while staying scoped per-city so an hourly rediscovery only needs to
//! replace one city's subscription, not a single global one covering everything.

use std::collections::HashMap;
use std::str::FromStr as _;

use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use futures::{Stream, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::types::U256;
use tokio::sync::mpsc;

use trader::marketdata::now_secs_f64;

use crate::market::StalenessTick;
use crate::snapshot::{MarketSnapshot, SharedSnapshots, update as update_snapshot};

#[derive(Debug, Clone)]
pub struct Bucket {
    pub label: String, // groupItemTitle, e.g. "33°C" or "27°C or below"
    pub yes_token: U256,
}

fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
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

/// Subscribes once (per reconnect attempt) for *all* of `ids`, merges `best_bid_ask` +
/// `price_change`, and yields `(asset_id, up_price, down_price, server_ts_ms)` — the batched
/// analogue of `trader::marketdata::spawn_poly_task`, which only handles one asset_id.
fn merged_price_stream(
    clob: &ClobWsClient,
    ids: Vec<U256>,
) -> Result<impl Stream<Item = (U256, f64, f64, i64)> + use<>> {
    let bba = clob.subscribe_best_bid_ask(ids.clone())?;
    let pc = clob.subscribe_prices(ids)?;

    let bba_u = bba.filter_map(|r| async move {
        r.ok()
            .map(|m| (m.asset_id, d2f(&m.best_bid), d2f(&m.best_ask), m.timestamp))
    });
    let pc_u = pc.flat_map(|r| {
        let items: Vec<(U256, f64, f64, i64)> = match r {
            Ok(p) => {
                let ts = p.timestamp;
                p.price_changes
                    .into_iter()
                    .filter_map(move |e| match (e.best_bid, e.best_ask) {
                        (Some(b), Some(a)) => Some((e.asset_id, d2f(&b), d2f(&a), ts)),
                        _ => None,
                    })
                    .collect()
            }
            Err(_) => Vec::new(),
        };
        futures::stream::iter(items)
    });

    Ok(futures::stream::select(Box::pin(bba_u), Box::pin(pc_u)))
}

/// One city's live feed: batch-subscribes every currently-discovered bucket's Yes token in
/// a single pair of calls, demuxes incoming ticks by `asset_id` via `labels`, and publishes
/// snapshot/staleness updates. Reconnects (re-subscribing the same `ids`) on stream close.
async fn run_city_feed(
    city: String,
    clob: ClobWsClient,
    ids: Vec<U256>,
    labels: HashMap<U256, String>,
    snapshots: SharedSnapshots,
    stale_tx: mpsc::UnboundedSender<StalenessTick>,
) {
    loop {
        match merged_price_stream(&clob, ids.clone()) {
            Ok(stream) => {
                let mut s = Box::pin(stream);
                while let Some((asset_id, bid, ask, ts_ms)) = s.next().await {
                    if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
                        continue;
                    }
                    let Some(label) = labels.get(&asset_id) else {
                        continue; // message for a token we didn't ask about — ignore
                    };
                    let up = (bid + ask) / 2.0;
                    let key = format!("weather:{city}:{label}");
                    let _ = stale_tx.send((key.clone(), "poly", ts_ms));
                    update_snapshot(
                        &snapshots,
                        &key,
                        MarketSnapshot {
                            kind: "weather",
                            label: format!("{city}: {label}"),
                            up_price: up,
                            dn_price: 1.0 - up,
                            last_tick_ms: (now_secs_f64() * 1000.0) as i64,
                        },
                    );
                }
                eprintln!("[weather:{city}] price stream closed, reconnecting…");
            }
            Err(e) => {
                eprintln!("[weather:{city}] subscribe failed: {e:#}, retrying…");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Entry point `main.rs` spawns once per configured city: (re)discovers today's buckets on
/// `refresh_interval_secs` (handles the day rolling over and any bucket-set changes), and
/// replaces the single batched `run_city_feed` task each time buckets change. Runs forever;
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
    let mut feed_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        refresh.tick().await;
        match fetch_buckets(&http, &city).await {
            Ok(Some(buckets)) if !buckets.is_empty() => {
                eprintln!(
                    "[weather:{city}] {} bucket(s) discovered, batching into 1 subscribe call",
                    buckets.len()
                );
                if let Some(t) = feed_task.take() {
                    t.abort();
                }
                let ids: Vec<U256> = buckets.iter().map(|b| b.yes_token).collect();
                let labels: HashMap<U256, String> = buckets
                    .into_iter()
                    .map(|b| (b.yes_token, b.label))
                    .collect();
                feed_task = Some(tokio::spawn(run_city_feed(
                    city.clone(),
                    clob.clone(),
                    ids,
                    labels,
                    snapshots.clone(),
                    stale_tx.clone(),
                )));
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
