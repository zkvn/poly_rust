//! Shared discovery + monitoring core for any Gamma "negRisk or single-market event" —
//! deliberately **not wired through `trader::machine::Machine`**. `weather.rs` and
//! `worldcup.rs` are both thin wrappers around this module; the only thing that differs
//! between them is how a slug is produced (weather: derived from today's date per city;
//! World Cup: a fixed slug per event, no date involved) — everything about fetching an
//! event's Yes-token buckets, batch-subscribing, demuxing ticks, and driving one
//! `bucket_reversal::BucketReversalEngine` per (bucket, grid variant) is identical, so it
//! lives here once instead of twice.
//!
//! **Why not `Machine`:** `Machine::cycle_close()` resolves a held position by comparing
//! `last_binance` against `cycle_open_binance` — correct for crypto (that comparison *is*
//! the market's real resolution rule), but wrong here: weather resolves against a station
//! reading, World Cup markets resolve against real match/award outcomes — neither has
//! anything to do with price momentum, and there's no equivalent reference feed anyway.
//! Rather than fabricate one, every bucket instead runs `bucket_reversal.rs`'s
//! self-contained engine, which never holds to real resolution at all — every position
//! closes via observed price action or a fixed timeout (see that file's doc comment). Real
//! Yes/No-aware Gamma-outcome resolution was considered and deliberately dropped, not
//! deferred — see `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md`.
//!
//! **Subscription strategy — batched per event, not one call per bucket.** Same lesson as
//! `siglab/doc/incident_ws_2026-07-13.md`: `polymarket_client_sdk_v2`'s `ConnectionManager`
//! holds one `broadcast::channel` per WS connection and hands every `subscribe_*()` call its
//! own receiver on that same channel, filtering client-side — so cost is O(subscriptions ×
//! message rate), not O(subscriptions). One batched `subscribe_best_bid_ask`/
//! `subscribe_prices` call per event (covering all of that event's Yes tokens) keeps
//! subscription count proportional to event count, not bucket count.

use std::collections::HashMap;
use std::str::FromStr as _;

use anyhow::{Context, Result};
use futures::{Stream, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::types::U256;
use tokio::sync::mpsc;

use trader::marketdata::now_secs_f64;

use crate::bucket_reversal::{BucketReversalEngine, reversal_grid};
use crate::market::{SharedClients, StalenessTick};
use crate::record::{MarketKind, SiglabTradeRecord};
use crate::snapshot::{MarketSnapshot, SharedSnapshots, update as update_snapshot};

/// Static identity for one monitored event — bundled to keep `run_event_feed`/
/// `run_event_supervisor` under clippy's argument-count limit. `log_key`/`snapshot_prefix`
/// let the same code serve both callers: weather keys snapshots
/// `"weather:{city}:{bucket}"` with `kind="weather"`; worldcup keys them
/// `"worldcup:{slug}:{bucket}"` with `kind="worldcup"`. `market_kind` is the analogous
/// `crate::record::MarketKind` tag for trade records produced by `bucket_reversal.rs`.
#[derive(Debug, Clone)]
pub struct EventIdentity {
    pub log_key: String,
    pub snapshot_prefix: String,
    pub kind: &'static str,
    pub market_kind: MarketKind,
    pub display_name: String,
}

/// Output destinations shared across every monitored event — mirrors `market::Sinks`.
/// `trade_tx` carries `bucket_reversal.rs` engine outputs, not `trader::machine::Machine`
/// ones — these markets never touch `Machine` (see this module's doc comment).
#[derive(Clone)]
pub struct EventSinks {
    pub snapshots: SharedSnapshots,
    pub stale_tx: mpsc::UnboundedSender<StalenessTick>,
    pub trade_tx: mpsc::UnboundedSender<SiglabTradeRecord>,
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub label: String, // groupItemTitle (multi-outcome events) or the market question itself
    pub yes_token: U256,
}

fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
}

/// Fetch one event's buckets by exact slug. `Ok(None)` (not `Err`) if the event doesn't
/// exist or has no market data right now — expected for e.g. a weather city with no event
/// today, not a failure worth aborting discovery over. Works identically for a single
/// binary market (one bucket) or a multi-outcome negRisk group (many buckets) — the JSON
/// shape is the same either way, just with a `markets` array of length 1 vs N.
pub async fn fetch_event_buckets(
    http: &reqwest::Client,
    slug: &str,
) -> Result<Option<Vec<Bucket>>> {
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
        // groupItemTitle is set for negRisk multi-outcome members; a plain binary event has
        // none, so fall back to the market's own question text.
        let label = market["groupItemTitle"]
            .as_str()
            .or_else(|| market["question"].as_str())
            .unwrap_or("?")
            .to_string();

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

/// One event's live feed: batch-subscribes every currently-discovered bucket's Yes token in
/// a single pair of calls, demuxes incoming ticks by `asset_id` via `labels`, publishes
/// snapshot/staleness updates, and drives one `BucketReversalEngine` per (bucket, grid
/// variant) — 18 per bucket — forwarding any closed position to `trade_tx`. Reconnects
/// (re-subscribing the same `ids`, fresh engines) on stream close.
async fn run_event_feed(
    identity: EventIdentity,
    clob: ClobWsClient,
    ids: Vec<U256>,
    labels: HashMap<U256, String>,
    slug: String,
    sinks: EventSinks,
) {
    let EventIdentity {
        log_key,
        snapshot_prefix,
        kind,
        market_kind,
        display_name,
    } = identity;
    let EventSinks {
        snapshots,
        stale_tx,
        trade_tx,
    } = sinks;

    let grid = reversal_grid();
    let mut engines: HashMap<U256, Vec<BucketReversalEngine>> = ids
        .iter()
        .map(|&token| {
            let set = grid
                .iter()
                .map(|(id, params)| BucketReversalEngine::new(id.clone(), *params))
                .collect();
            (token, set)
        })
        .collect();

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
                    let ts_secs = ts_ms as f64 / 1000.0;
                    let key = format!("{snapshot_prefix}:{label}");
                    let _ = stale_tx.send((key.clone(), "poly", ts_ms));
                    update_snapshot(
                        &snapshots,
                        &key,
                        MarketSnapshot {
                            kind,
                            label: format!("{display_name}: {label}"),
                            up_price: up,
                            dn_price: 1.0 - up,
                            last_tick_ms: (now_secs_f64() * 1000.0) as i64,
                        },
                    );

                    if let Some(bucket_engines) = engines.get_mut(&asset_id) {
                        for engine in bucket_engines.iter_mut() {
                            if let Some(closed) = engine.on_tick(up, ts_secs) {
                                let rec = SiglabTradeRecord::from_bucket_engine(
                                    market_kind,
                                    &engine.variant_id,
                                    &display_name,
                                    &key,
                                    &slug,
                                    closed.side_up,
                                    closed.entry_ts,
                                    closed.entry_price,
                                    closed.exit_price,
                                    closed.outcome,
                                    closed.pnl,
                                    now_secs_f64(),
                                );
                                let _ = trade_tx.send(rec);
                            }
                        }
                    }
                }
                eprintln!("[{log_key}] price stream closed, reconnecting…");
            }
            Err(e) => {
                eprintln!("[{log_key}] subscribe failed: {e:#}, retrying…");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Entry point both `weather.rs` and `worldcup.rs` spawn one of per event: (re)discovers
/// buckets on `refresh_interval_secs` via `slug_fn` (called fresh each refresh — for weather
/// this recomputes today's date-based slug; for worldcup it just returns the same static
/// slug every time, so refresh only re-checks for a changed bucket set), and
/// replaces the single batched `run_event_feed` task each time buckets change. Runs
/// forever; errors are logged, not propagated, since one event's transient failure
/// shouldn't take down every other event's monitoring.
pub async fn run_event_supervisor(
    identity: EventIdentity,
    slug_fn: impl Fn() -> String + Send + 'static,
    clients: SharedClients,
    sinks: EventSinks,
    refresh_interval_secs: u64,
) {
    let log_key = identity.log_key.clone();
    let http = clients.http.clone();
    let clob = clients.clob.clone();
    let mut feed_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        refresh.tick().await;
        let slug = slug_fn();
        match fetch_event_buckets(&http, &slug).await {
            Ok(Some(buckets)) if !buckets.is_empty() => {
                eprintln!(
                    "[{log_key}] {} bucket(s) discovered, batching into 1 subscribe call",
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
                feed_task = Some(tokio::spawn(run_event_feed(
                    identity.clone(),
                    clob.clone(),
                    ids,
                    labels,
                    slug.clone(),
                    sinks.clone(),
                )));
            }
            Ok(Some(_)) => {
                eprintln!(
                    "[{log_key}] event found but no Yes-token buckets parsed — retrying next refresh"
                );
            }
            Ok(None) => {
                eprintln!("[{log_key}] no active event (slug={slug}) — retrying next refresh");
            }
            Err(e) => {
                eprintln!("[{log_key}] discovery failed: {e:#} — retrying next refresh");
            }
        }
    }
}
