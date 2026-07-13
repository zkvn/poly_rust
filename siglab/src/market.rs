//! One rotating crypto market: subscribes the Polymarket feed for its own slug, consumes a
//! *shared* per-asset Binance feed (see `spawn_binance_broadcast` — one real Binance
//! connection per asset, fanned out to every duration task trading that asset, not one
//! connection per (asset, duration) task), and drives one `trader::machine::Machine` per
//! configured variant, emitting paper trade records. This is `trader/src/bin/shadow.rs`'s
//! rotation loop generalized from one asset/one config to however many `(asset, suffix)`
//! markets and variants siglab is configured with — spawned once per market by `main.rs`,
//! sharing one `ClobWsClient`/`reqwest::Client` across all of them (per
//! `plan_weather_bot.md` §7: subscriptions, not connections).
//!
//! An earlier version had each market task call `spawn_binance_task` itself — harmless
//! functionally, but it meant every asset configured with both a 5m and 15m market opened
//! two independent websocket connections to the same Binance `@trade` stream. Caught by
//! actually running the harness locally (`eprintln!` showed duplicate reconnect logs per
//! asset) before the Docker resource-usage pass, so the measured numbers reflect one
//! connection per asset, not the wasteful version.

use anyhow::Result;
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use tokio::sync::{broadcast, mpsc};

use trader::config::AssetParams;
use trader::machine::Machine;
use trader::marketdata::{PolySub, fetch_meta, now_secs_f64};
use trader::types::{BinanceTick, CycleContext};

use crate::rotation::Rotation;

/// Spawn exactly one real Binance `@trade` subscription for `asset` and fan its ticks out
/// to a broadcast channel — call once per unique asset, then `.subscribe()` once per
/// duration task trading that asset.
pub fn spawn_binance_broadcast(asset: &str) -> broadcast::Sender<BinanceTick> {
    let (tx, _rx) = broadcast::channel(1024);
    let (bridge_tx, mut bridge_rx) = mpsc::unbounded_channel();
    trader::marketdata::spawn_binance_task(asset, bridge_tx);
    let out_tx = tx.clone();
    tokio::spawn(async move {
        while let Some(tick) = bridge_rx.recv().await {
            // Ignore send errors — no active subscriber (e.g. between market tasks
            // starting) just means this tick is dropped, not a bug; the next tick will
            // reach whichever subscribers exist by then.
            let _ = out_tx.send(tick);
        }
    });
    tx
}

use crate::record::{MarketKind, SiglabTradeRecord};
use crate::snapshot::{MarketSnapshot, SharedSnapshots, update as update_snapshot};

/// `(variant_id, resolved params)` — `main.rs` builds this by filtering siglab's config's
/// variants to the ones that `applies_to` this market's asset.
pub type VariantSet = Vec<(String, AssetParams)>;

/// Tick-arrival events for the staleness tracker: `(market_key, feed, now_ms)`.
pub type StalenessTick = (String, &'static str, i64);

/// Shared, cheaply-cloneable clients — one `ClobWsClient`/`reqwest::Client` per process,
/// not one per market task (plan_weather_bot.md §7: subscriptions, not connections).
#[derive(Clone)]
pub struct SharedClients {
    pub http: reqwest::Client,
    pub clob: ClobWsClient,
}

/// Output channels/shared state every market task writes into — a single writer/single
/// staleness-sweep task on the other end of the two channels (plan_weather_bot.md §7:
/// fan-in, not N-way file writers), and the shared "current state" map the hourly report
/// reads from.
#[derive(Clone)]
pub struct Sinks {
    pub trade_tx: mpsc::UnboundedSender<SiglabTradeRecord>,
    pub stale_tx: mpsc::UnboundedSender<StalenessTick>,
    pub snapshots: SharedSnapshots,
}

#[allow(unused_assignments, unused_variables)]
pub async fn run_market(
    asset: String,
    rotation: Rotation,
    variants: VariantSet,
    clients: SharedClients,
    sinks: Sinks,
    mut binance_rx: broadcast::Receiver<BinanceTick>,
) -> Result<()> {
    let SharedClients { http, clob } = clients;
    let Sinks {
        trade_tx,
        stale_tx,
        snapshots,
    } = sinks;
    let market_key = rotation.market_key(&asset);
    let period_secs = rotation.period_secs();

    let mut machines: Vec<(String, Machine)> = variants
        .iter()
        .map(|(id, p)| {
            let m = match p.strategies.first().map(String::as_str) {
                Some("high_prob") => Machine::new_high_prob(p),
                _ => Machine::new_reversal(p),
            };
            (id.clone(), m)
        })
        .collect();

    if machines.is_empty() {
        eprintln!("[{market_key}] no variants apply to this asset — task idle, exiting");
        return Ok(());
    }

    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<trader::types::PolyTick>();

    let mut last_binance: f64 = 0.0;
    let mut current_slug: Option<String> = None;
    let mut current_slot_val: i64 = -1;
    let mut poly_sub: Option<PolySub> = None;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    eprintln!(
        "[{market_key}] starting, {} variant(s): {:?}",
        machines.len(),
        machines
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>()
    );

    loop {
        tokio::select! {
            binance_msg = binance_rx.recv() => {
                let tick = match binance_msg {
                    Ok(tick) => tick,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[{market_key}] binance broadcast lagged, dropped {n} ticks");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        eprintln!("[{market_key}] binance broadcast closed unexpectedly");
                        continue;
                    }
                };
                last_binance = tick.price;
                let _ = stale_tx.send((market_key.clone(), "binance", (tick.ts * 1000.0) as i64));
                if current_slug.is_some() {
                    for (_, m) in machines.iter_mut() {
                        m.on_binance(tick);
                    }
                }
            }

            Some(tick) = poly_rx.recv() => {
                let _ = stale_tx.send((market_key.clone(), "poly", (tick.ts * 1000.0) as i64));
                update_snapshot(&snapshots, &market_key, MarketSnapshot {
                    kind: "crypto",
                    label: market_key.clone(),
                    up_price: tick.up,
                    dn_price: tick.dn,
                    last_tick_ms: (tick.ts * 1000.0) as i64,
                });
                if current_slug.is_some() {
                    for (variant_id, m) in machines.iter_mut() {
                        if let Some(rec) = m.on_poly(tick) {
                            let out = SiglabTradeRecord::from_trader(
                                &rec, MarketKind::Crypto, variant_id, &asset, &market_key, now_secs_f64(),
                            );
                            let _ = trade_tx.send(out);
                        }
                    }
                }
            }

            _ = ticker.tick() => {
                let (slot, slug) = rotation.current_slot_and_slug(&asset);
                if slot != current_slot_val {
                    if current_slug.is_some() {
                        for (variant_id, m) in machines.iter_mut() {
                            if let Some(rec) = m.cycle_close() {
                                let out = SiglabTradeRecord::from_trader(
                                    &rec, MarketKind::Crypto, variant_id, &asset, &market_key, now_secs_f64(),
                                );
                                let _ = trade_tx.send(out);
                            }
                        }
                    }

                    if last_binance <= 0.0 {
                        continue;
                    }

                    match fetch_meta(&http, &slug).await {
                        Ok((up_id, _dn_id)) => {
                            poly_sub = Some(PolySub::start(&clob, up_id, poly_tx.clone()));
                            let ctx = CycleContext {
                                start_ts: slot as f64,
                                end_ts: (slot as u64 + period_secs) as f64,
                                open_binance: last_binance,
                            };
                            for (_, m) in machines.iter_mut() {
                                m.cycle_open(&ctx, &slug, false);
                            }
                            current_slug = Some(slug.clone());
                            current_slot_val = slot;
                        }
                        Err(e) => {
                            eprintln!("[{market_key}] meta fetch failed for {slug}: {e:#} — retrying next tick");
                        }
                    }
                }
            }
        }
    }
}
