// Live market data — Binance spot ticks + Polymarket CLOB best-bid/ask, plus Gamma
// slug/token discovery. Read-only: no CLOB writes happen here (A2 shadow phase).

use std::str::FromStr as _;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::StreamExt as _;
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::types::U256;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::types::{BinanceTick, PolyTick};

pub fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn current_slot(interval: u64) -> u64 {
    ((now_secs_f64() as u64) / interval) * interval
}

pub fn make_slug(asset: &str, slot: u64, suffix: &str) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), suffix, slot)
}

fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
}

// ── Gamma meta discovery ──────────────────────────────────────────────────────

/// Resolve (up_token_id, dn_token_id) for the given slug via the Gamma API.
pub async fn fetch_meta(http: &reqwest::Client, slug: &str) -> Result<(U256, U256)> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .context("gamma request")?
        .json()
        .await
        .context("gamma json")?;

    let event = resp
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no event for {slug}"))?;
    let market = event["markets"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no market for {slug}"))?;

    let token_ids: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap_or("[]"))?;
    let outcomes: Vec<String> = serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

    let find = |target: &str| -> Result<U256> {
        outcomes
            .iter()
            .zip(token_ids.iter())
            .find(|(o, _)| o.to_lowercase() == target)
            .map(|(_, tid)| U256::from_str(tid).with_context(|| format!("parse {target} id")))
            .ok_or_else(|| anyhow::anyhow!("no {} token in {}", target, slug))?
    };

    Ok((find("up")?, find("down")?))
}

/// Poll the Gamma API for `slug`'s actual resolution — mirrors
/// `bot/worker.py::_fetch_api_went_up`. Returns `Some(true)` once the "Up" outcome's
/// price reaches >= 0.99, `Some(false)` once "Down" does, `None` if unresolved yet or
/// on any fetch/parse error (broad on purpose, matching Python's `except Exception:
/// return None` — a transient failure here should look like "still pending" to the
/// caller, not abort the poll loop).
pub async fn fetch_gamma_resolution(http: &reqwest::Client, slug: &str) -> Option<bool> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: serde_json::Value = http.get(&url).send().await.ok()?.json().await.ok()?;

    let event = resp.as_array().and_then(|a| a.first())?;
    let market = event["markets"].as_array().and_then(|a| a.first())?;

    let outcomes: Vec<String> = serde_json::from_str(market["outcomes"].as_str()?).ok()?;
    let prices: Vec<String> = serde_json::from_str(market["outcomePrices"].as_str()?).ok()?;

    for (outcome, price_str) in outcomes.iter().zip(prices.iter()) {
        let price: f64 = price_str.parse().ok()?;
        if price >= 0.99 {
            match outcome.trim().to_uppercase().as_str() {
                "UP" => return Some(true),
                "DOWN" => return Some(false),
                _ => {}
            }
        }
    }
    None
}

// ── Binance spot ticks ────────────────────────────────────────────────────────

/// Subscribe to the Binance `@trade` stream for `asset` (e.g. "BTC" -> "btcusdt")
/// and forward each trade price as a `BinanceTick` on `tx`. Reconnects on drop.
pub fn spawn_binance_task(asset: &str, tx: mpsc::UnboundedSender<BinanceTick>) {
    let symbol = format!("{}usdt", asset.to_lowercase());
    let asset = asset.to_string();
    tokio::spawn(async move {
        let url = format!("wss://stream.binance.com:9443/ws/{symbol}@trade");
        loop {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => {
                    let (_write, mut read) = ws.split();
                    // 30 s timeout: Binance @trade can go silent on low-volume assets
                    // and TCP drops are often silent (no Close/Err frame). Without a
                    // timeout, read.next() hangs forever and last_binance stagnates.
                    while let Ok(Some(msg)) =
                        tokio::time::timeout(std::time::Duration::from_secs(30), read.next()).await
                    {
                        match msg {
                            Ok(Message::Text(txt)) => {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                    let price = v["p"].as_str().and_then(|s| s.parse::<f64>().ok());
                                    if let Some(price) = price
                                        && tx
                                            .send(BinanceTick {
                                                ts: now_secs_f64(),
                                                price,
                                            })
                                            .is_err()
                                    {
                                        return; // receiver dropped
                                    }
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    eprintln!("[{asset}] binance ws closed or timed out, reconnecting…");
                }
                Err(e) => eprintln!("[{asset}] binance connect failed: {e:#}, retrying…"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
}

// ── Polymarket poly (best-bid/ask) ticks ──────────────────────────────────────

/// Subscribe to best_bid_ask + price_change for the UP token and forward merged
/// `PolyTick { up, dn = 1-up }` samples on `tx`. Runs until the token set changes
/// (caller aborts the returned JoinHandle to rotate).
pub fn spawn_poly_task(
    clob: ClobWsClient,
    up_id: U256,
    tx: mpsc::UnboundedSender<PolyTick>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match (
                clob.subscribe_best_bid_ask(vec![up_id]),
                clob.subscribe_prices(vec![up_id]),
            ) {
                (Ok(bba), Ok(pc)) => {
                    // Both channels can (or do, per PriceChangeBatchEntry's own asset_id)
                    // deliver updates for tokens beyond the one requested — always filter
                    // by asset_id explicitly rather than trusting the subscription list,
                    // matching price_feed/src/collect.rs's proven pattern.
                    let bba_u = bba.filter_map(move |r| async move {
                        r.ok().and_then(|m| {
                            (m.asset_id == up_id).then(|| (d2f(&m.best_bid), d2f(&m.best_ask)))
                        })
                    });
                    let pc_u = pc.flat_map(move |r| {
                        let items: Vec<(f64, f64)> = match r {
                            Ok(p) => p
                                .price_changes
                                .into_iter()
                                .filter(|e| e.asset_id == up_id)
                                .filter_map(|e| match (e.best_bid, e.best_ask) {
                                    (Some(b), Some(a)) => Some((d2f(&b), d2f(&a))),
                                    _ => None,
                                })
                                .collect(),
                            Err(_) => Vec::new(),
                        };
                        futures::stream::iter(items)
                    });

                    let mut merged = futures::stream::select(Box::pin(bba_u), Box::pin(pc_u));
                    while let Some((bid, ask)) = merged.next().await {
                        if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
                            continue;
                        }
                        let up = (bid + ask) / 2.0;
                        if tx
                            .send(PolyTick {
                                ts: now_secs_f64(),
                                up,
                                dn: 1.0 - up,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    eprintln!("poly ws closed, reconnecting…");
                }
                _ => eprintln!("subscribe best_bid_ask/prices failed, retrying…"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    })
}

pub fn clob_client() -> ClobWsClient {
    ClobWsClient::default()
}

pub fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .build()
        .context("http client")
}

/// Wraps the current cycle's poly subscription task so it can be aborted on rotation.
pub struct PolySub {
    handle: tokio::task::JoinHandle<()>,
}

impl PolySub {
    pub fn start(clob: &ClobWsClient, up_id: U256, tx: mpsc::UnboundedSender<PolyTick>) -> Self {
        Self {
            handle: spawn_poly_task(clob.clone(), up_id, tx),
        }
    }
}

impl Drop for PolySub {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
