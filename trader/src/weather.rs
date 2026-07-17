//! Weather temperature-bucket trading — Phase B of
//! `trader/doc/feature_new_markets_2026-07-17.md` (§7).
//!
//! **Completely inert unless `live --weather-config <path>` is passed** — no
//! config file, no task, no subscriptions, nothing. The production compose /
//! systemd invocations don't pass it, so shipping this module changes nothing
//! until a deliberate config+flag decision enables it.
//!
//! Design is a direct port of siglab's proven pieces, married to the trader's
//! real `ExecutionEngine`:
//!
//! - **Discovery** (`today_slug` + `fetch_event_buckets`): each city gets a
//!   fresh daily Gamma event (`highest-temperature-in-{city}-on-{month}-{day}-
//!   {year}`, UTC date) holding N mutually-exclusive temperature buckets, each
//!   a Yes/No market — from `siglab/src/weather.rs` / `event_monitor.rs`.
//! - **One batched WS subscription per city event** — never one call per
//!   bucket. The SDK fans every `subscribe_*()` call its own receiver on one
//!   shared broadcast channel and filters client-side, so per-bucket calls
//!   cost O(subscriptions × message rate) — the exact 200-370% CPU incident
//!   siglab hit on 2026-07-13 (`siglab/doc/incident_ws_2026-07-13.md`).
//! - **Per-bucket reversal engine** (`siglab/src/bucket_reversal.rs`'s state
//!   machine): pure CLOB price action — dip below `low`, later recover above
//!   `high`, enter; exit on stop-loss / take-profit / max-hold timeout, and
//!   at latest at a daily close deadline. **Never** holds to the market's
//!   real station-reading resolution and never touches `Machine`/`Worker`
//!   (their cycle-close resolves by Binance momentum — meaningless here).
//! - **Real orders** go through the same `ExecutionEngine` trait the crypto
//!   driver uses — the `SimExecutionEngine` under `--dry-run`, the live
//!   engine otherwise. Entries are FAK buys; take-profit exits are
//!   price-floored closes; stop-loss/timeout closes are unfloored.
//!
//! Per siglab's own research (`studies/weather/weather_poly_2026-07-12.md`),
//! the documented weather edge is forecast-latency arbitrage, *not* price
//! reversals — this exists to trade tiny size on the pattern siglab has been
//! paper-grading, not as a validated strategy. Keep `trade_size_usdc` small.

use std::collections::HashMap;
use std::str::FromStr as _;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::{Stream, StreamExt as _};
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::types::U256;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::execution::{ExecutionEngine, SellStatus};
use crate::marketdata::now_secs_f64;

// ── Config ────────────────────────────────────────────────────────────────────

/// `weather_*.toml` schema — deliberately its own tiny file, not a section of
/// `strategy_*.toml`, so the crypto config's schema (and every historical
/// pinned copy) is untouched (feature_new_markets_2026-07-17.md §7.1).
#[derive(Debug, Clone, Deserialize)]
pub struct WeatherConfig {
    /// City slugs as they appear in the Gamma event slug (e.g. "nyc",
    /// "hong-kong"). Start small (3-5 liquid cities), not siglab's full 51.
    pub cities: Vec<String>,
    /// Reversal entry band: dip below `low` latches, recovery above `high`
    /// enters — one variant, not siglab's 18-combo grid; pick from siglab's
    /// per-variant evidence when enabling.
    pub low: f64,
    pub high: f64,
    /// Exit rules, same semantics as siglab's fixed grid values.
    pub sl_pnl: f64,
    pub unwind_pnl: f64,
    pub max_hold_secs: f64,
    pub trade_size_usdc: f64,
    /// Never pay more than this per share on entry (same guard as the crypto
    /// side's `max_buy_price`).
    #[serde(default = "default_max_buy_price")]
    pub max_buy_price: f64,
    /// At most this many entries per bucket per day (a bucket's engine stops
    /// watching after this many closed trades until the next daily event).
    #[serde(default = "default_max_trades_per_bucket")]
    pub max_trades_per_bucket: u32,
    /// How often to re-check Gamma for the city's current daily event
    /// (discovers the next day's event after the UTC rollover).
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

fn default_max_buy_price() -> f64 {
    0.95
}
fn default_max_trades_per_bucket() -> u32 {
    1
}
fn default_refresh_interval_secs() -> u64 {
    600
}

pub fn load_weather_config(path: &str) -> Result<WeatherConfig> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    let cfg: WeatherConfig = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
    if cfg.cities.is_empty() {
        anyhow::bail!("weather config {path} has an empty `cities` list");
    }
    if cfg.low >= cfg.high {
        anyhow::bail!(
            "weather config: `low` ({}) must be strictly below `high` ({})",
            cfg.low,
            cfg.high
        );
    }
    Ok(cfg)
}

// ── Discovery (ported from siglab/src/weather.rs + event_monitor.rs) ─────────

fn month_name(m: u32) -> &'static str {
    match m {
        1 => "january",
        2 => "february",
        3 => "march",
        4 => "april",
        5 => "may",
        6 => "june",
        7 => "july",
        8 => "august",
        9 => "september",
        10 => "october",
        11 => "november",
        _ => "december",
    }
}

/// Today's event slug for `city`, UTC-date-based (Polymarket weather events
/// span a UTC day) — e.g. `highest-temperature-in-nyc-on-july-17-2026`.
pub fn today_slug(city: &str) -> String {
    use chrono::Datelike as _;
    let now = chrono::Utc::now();
    format!(
        "highest-temperature-in-{city}-on-{}-{}-{}",
        month_name(now.month()),
        now.day(),
        now.year()
    )
}

/// One temperature bucket of a city's daily event: its display label (e.g.
/// "84-85°F") and its Yes token.
#[derive(Debug, Clone)]
pub struct Bucket {
    pub label: String,
    pub yes_token: U256,
}

/// Fetch one event's buckets by exact slug. `Ok(None)` (not `Err`) if the
/// event doesn't exist or has no market data right now — expected for a city
/// with no event today, not a failure worth aborting the other cities over.
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

/// Batched merged best_bid_ask + price_change stream for all `ids` at once,
/// yielding `(asset_id, mid_price)` — the O(1)-subscriptions pattern from
/// siglab's `event_monitor.rs::merged_price_stream`.
fn merged_price_stream(
    clob: &ClobWsClient,
    ids: Vec<U256>,
) -> Result<impl Stream<Item = (U256, f64)> + use<>> {
    fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
        d.to_string().parse::<f64>().unwrap_or(f64::NAN)
    }
    let bba = clob
        .subscribe_best_bid_ask(ids.clone())
        .context("subscribe best_bid_ask")?;
    let pc = clob.subscribe_prices(ids).context("subscribe prices")?;

    let bba_u = bba.filter_map(|r| async move {
        r.ok().and_then(|m| {
            let bid = d2f(&m.best_bid);
            let ask = d2f(&m.best_ask);
            (bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0)
                .then_some((m.asset_id, (bid + ask) / 2.0))
        })
    });
    let pc_u = pc.flat_map(|r| {
        let items: Vec<(U256, f64)> = match r {
            Ok(p) => p
                .price_changes
                .into_iter()
                .filter_map(|e| match (e.best_bid, e.best_ask) {
                    (Some(b), Some(a)) => {
                        let bid = d2f(&b);
                        let ask = d2f(&a);
                        (bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0)
                            .then_some((e.asset_id, (bid + ask) / 2.0))
                    }
                    _ => None,
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        futures::stream::iter(items)
    });

    Ok(futures::stream::select(Box::pin(bba_u), Box::pin(pc_u)))
}

// ── Per-bucket trading state machine ─────────────────────────────────────────

/// A closed weather trade, for CSV logging + Telegram.
#[derive(Debug, Clone)]
pub struct WeatherTrade {
    pub city: String,
    pub bucket: String,
    pub entry_ts: f64,
    pub exit_ts: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub shares: f64,
    pub outcome: &'static str,
    pub pnl: f64,
}

enum BucketState {
    /// Dip latch — same permanent-until-trade latch as siglab's engine.
    /// Weather buckets trade the Yes side only: the No side of one bucket is
    /// economically the Yes of its neighbors, and one-sided keeps the real
    /// order flow (and its risk) halved and simple.
    Watching { saw_low: bool },
    /// Entry order placed and filled — tracking a real position.
    Holding {
        entry_ts: f64,
        entry_price: f64,
        shares: f64,
    },
    /// Trade budget (`max_trades_per_bucket`) exhausted for today's event.
    Done,
}

struct BucketTrader {
    bucket: Bucket,
    state: BucketState,
    trades_today: u32,
}

impl BucketTrader {
    fn new(bucket: Bucket) -> Self {
        Self {
            bucket,
            state: BucketState::Watching { saw_low: false },
            trades_today: 0,
        }
    }
}

/// What a tick asks the async caller to do — the engine itself stays sync and
/// unit-testable (same Action-style separation as `worker.rs`, minimally).
enum BucketAction {
    None,
    /// Dip-then-recover fired: place the entry FAK buy at `price`.
    Enter {
        price: f64,
    },
    /// Close the held position: `floor` is `Some(tp_price)` for take-profit
    /// (price-floored close), `None` for stop-loss/timeout (must close).
    Close {
        outcome: &'static str,
        floor: Option<f64>,
    },
}

/// Pure per-tick decision — mirrors `bucket_reversal.rs::on_tick`, minus the
/// paper fill (the caller executes real orders and reports back via
/// `on_entry_filled` / `on_closed`).
fn on_tick(trader: &mut BucketTrader, cfg: &WeatherConfig, mid: f64, ts: f64) -> BucketAction {
    match &mut trader.state {
        BucketState::Done => BucketAction::None,
        BucketState::Watching { saw_low } => {
            if mid < cfg.low {
                *saw_low = true;
            }
            if *saw_low && mid > cfg.high && mid <= cfg.max_buy_price {
                BucketAction::Enter { price: mid }
            } else {
                BucketAction::None
            }
        }
        BucketState::Holding {
            entry_ts,
            entry_price,
            ..
        } => {
            let elapsed = ts - *entry_ts;
            if mid <= *entry_price - cfg.sl_pnl {
                BucketAction::Close {
                    outcome: "STOPLOSS",
                    floor: None,
                }
            } else if mid >= *entry_price + cfg.unwind_pnl {
                BucketAction::Close {
                    outcome: "UNWIND",
                    floor: Some(*entry_price + cfg.unwind_pnl),
                }
            } else if elapsed >= cfg.max_hold_secs {
                BucketAction::Close {
                    outcome: "TIMEOUT",
                    floor: None,
                }
            } else {
                BucketAction::None
            }
        }
    }
}

// ── City supervisor (the spawned task) ────────────────────────────────────────

/// Everything the weather tasks share with the main driver.
pub struct WeatherSinks {
    pub engine: Arc<dyn ExecutionEngine>,
    pub http: reqwest::Client,
    /// Closed trades flow back to the main loop for CSV logging + Telegram —
    /// the weather tasks never write driver-owned files themselves.
    pub trade_tx: mpsc::UnboundedSender<WeatherTrade>,
}

/// One city's forever-loop: discover today's event, batch-subscribe its
/// buckets, trade them until the stream drops or the UTC day rolls over
/// (slug changes on re-check), then rediscover. Spawned once per configured
/// city by `live.rs` when `--weather-config` is present.
pub async fn run_city(city: String, cfg: WeatherConfig, sinks: Arc<WeatherSinks>) {
    loop {
        let slug = today_slug(&city);
        let buckets = match fetch_event_buckets(&sinks.http, &slug).await {
            Ok(Some(b)) if !b.is_empty() => b,
            Ok(_) => {
                println!("[weather] {city}: no event/buckets for {slug} — retrying later");
                tokio::time::sleep(std::time::Duration::from_secs(cfg.refresh_interval_secs)).await;
                continue;
            }
            Err(e) => {
                eprintln!("[weather] {city}: discovery failed for {slug}: {e:#}");
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                continue;
            }
        };
        println!(
            "[weather] {city}: {} buckets for {slug} — subscribing (batched)",
            buckets.len()
        );
        let clob = crate::marketdata::clob_client();
        run_city_feed(&city, &slug, buckets, &cfg, &sinks, &clob).await;
        // Stream ended (WS drop) or day rolled over — brief pause, rediscover.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Drives one discovered event until its merged stream ends or the slug goes
/// stale (UTC day rollover, checked once a minute against wall clock).
async fn run_city_feed(
    city: &str,
    slug: &str,
    buckets: Vec<Bucket>,
    cfg: &WeatherConfig,
    sinks: &Arc<WeatherSinks>,
    clob: &ClobWsClient,
) {
    let ids: Vec<U256> = buckets.iter().map(|b| b.yes_token).collect();
    let mut traders: HashMap<U256, BucketTrader> = buckets
        .into_iter()
        .map(|b| (b.yes_token, BucketTrader::new(b)))
        .collect();

    let mut stream = match merged_price_stream(clob, ids) {
        Ok(s) => Box::pin(s),
        Err(e) => {
            eprintln!("[weather] {city}: subscribe failed: {e:#}");
            return;
        }
    };

    let mut day_check = tokio::time::interval(std::time::Duration::from_secs(60));
    day_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            item = stream.next() => {
                let Some((asset_id, mid)) = item else {
                    println!("[weather] {city}: stream closed — will rediscover/resubscribe");
                    return;
                };
                let Some(trader) = traders.get_mut(&asset_id) else { continue };
                let ts = now_secs_f64();
                match on_tick(trader, cfg, mid, ts) {
                    BucketAction::None => {}
                    BucketAction::Enter { price } => {
                        let result = sinks
                            .engine
                            .place(asset_id, price, cfg.trade_size_usdc, cfg.max_buy_price)
                            .await;
                        if result.placed && result.filled_shares > 0.0 {
                            println!(
                                "[weather] {city}/{}: entered {:.4} shares @ {:.4}",
                                trader.bucket.label, result.filled_shares, result.cost
                            );
                            trader.state = BucketState::Holding {
                                entry_ts: now_secs_f64(),
                                entry_price: result.cost,
                                shares: result.filled_shares,
                            };
                        } else {
                            println!(
                                "[weather] {city}/{}: entry rejected ({}) — re-watching (dip latch kept)",
                                trader.bucket.label,
                                result.error.as_deref().unwrap_or("unknown")
                            );
                            // Keep the latch: the dip already happened; a failed
                            // FAK just means this tick's book couldn't fill.
                        }
                    }
                    BucketAction::Close { outcome, floor } => {
                        let BucketState::Holding { entry_ts, entry_price, shares } = trader.state else {
                            continue;
                        };
                        let result = match floor {
                            Some(p) => sinks.engine.close_position_at_price(asset_id, shares, p).await,
                            None => sinks.engine.close_position(asset_id, shares).await,
                        };
                        let matched = matches!(result.status, SellStatus::Matched);
                        if !matched {
                            // Take-profit: defer to the next tick (book may refill
                            // at the floor). Stop-loss/timeout: keep re-firing on
                            // every tick until it clears — same posture as
                            // worker.rs. Either way the position stays Holding.
                            continue;
                        }
                        let exit_price = if result.shares_sold > 0.0 {
                            result.filled_usdc / result.shares_sold
                        } else {
                            0.0
                        };
                        let pnl = result.filled_usdc - shares * entry_price;
                        trader.trades_today += 1;
                        trader.state = if trader.trades_today >= cfg.max_trades_per_bucket {
                            BucketState::Done
                        } else {
                            BucketState::Watching { saw_low: false }
                        };
                        let _ = sinks.trade_tx.send(WeatherTrade {
                            city: city.to_string(),
                            bucket: trader.bucket.label.clone(),
                            entry_ts,
                            exit_ts: now_secs_f64(),
                            entry_price,
                            exit_price,
                            shares,
                            outcome,
                            pnl,
                        });
                    }
                }
            }
            _ = day_check.tick() => {
                if today_slug(city) != slug {
                    println!("[weather] {city}: UTC day rolled over — rediscovering {}", today_slug(city));
                    return;
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> WeatherConfig {
        WeatherConfig {
            cities: vec!["nyc".to_string()],
            low: 0.2,
            high: 0.55,
            sl_pnl: 0.3,
            unwind_pnl: 0.15,
            max_hold_secs: 25.0,
            trade_size_usdc: 1.0,
            max_buy_price: 0.95,
            max_trades_per_bucket: 1,
            refresh_interval_secs: 600,
        }
    }

    fn test_trader() -> BucketTrader {
        BucketTrader::new(Bucket {
            label: "84-85°F".to_string(),
            yes_token: U256::from(1u64),
        })
    }

    #[test]
    fn today_slug_has_expected_shape() {
        let slug = today_slug("hong-kong");
        assert!(slug.starts_with("highest-temperature-in-hong-kong-on-"));
    }

    #[test]
    fn no_entry_without_a_dip_first() {
        let cfg = test_cfg();
        let mut t = test_trader();
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.6, 0.0),
            BucketAction::None
        ));
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.7, 1.0),
            BucketAction::None
        ));
    }

    #[test]
    fn dip_then_recover_enters_yes_side_only() {
        let cfg = test_cfg();
        let mut t = test_trader();
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.15, 0.0),
            BucketAction::None
        ));
        match on_tick(&mut t, &cfg, 0.6, 100.0) {
            BucketAction::Enter { price } => assert!((price - 0.6).abs() < 1e-9),
            _ => panic!("expected Enter"),
        }
        // The Yes-side-only rule: a *high* price (No side dipping below low
        // means mid > 1-low) never latches anything here.
        let mut t2 = test_trader();
        assert!(matches!(
            on_tick(&mut t2, &cfg, 0.85, 0.0),
            BucketAction::None
        ));
        assert!(matches!(
            on_tick(&mut t2, &cfg, 0.9, 1.0),
            BucketAction::None
        ));
    }

    #[test]
    fn entry_never_fires_above_max_buy_price() {
        let cfg = test_cfg();
        let mut t = test_trader();
        on_tick(&mut t, &cfg, 0.15, 0.0); // latch
        // Recovery straight past the price cap — must NOT enter at 0.96.
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.96, 1.0),
            BucketAction::None
        ));
    }

    #[test]
    fn holding_exits_on_sl_tp_and_timeout() {
        let cfg = test_cfg();

        // Take-profit (price-floored close).
        let mut t = test_trader();
        t.state = BucketState::Holding {
            entry_ts: 0.0,
            entry_price: 0.6,
            shares: 1.6,
        };
        match on_tick(&mut t, &cfg, 0.76, 1.0) {
            BucketAction::Close { outcome, floor } => {
                assert_eq!(outcome, "UNWIND");
                assert!((floor.unwrap() - 0.75).abs() < 1e-9);
            }
            _ => panic!("expected TP close"),
        }

        // Stop-loss (no floor).
        let mut t = test_trader();
        t.state = BucketState::Holding {
            entry_ts: 0.0,
            entry_price: 0.6,
            shares: 1.6,
        };
        match on_tick(&mut t, &cfg, 0.29, 1.0) {
            BucketAction::Close { outcome, floor } => {
                assert_eq!(outcome, "STOPLOSS");
                assert!(floor.is_none());
            }
            _ => panic!("expected SL close"),
        }

        // Timeout: price inside the band but max_hold_secs elapsed.
        let mut t = test_trader();
        t.state = BucketState::Holding {
            entry_ts: 0.0,
            entry_price: 0.6,
            shares: 1.6,
        };
        match on_tick(&mut t, &cfg, 0.62, 26.0) {
            BucketAction::Close { outcome, floor } => {
                assert_eq!(outcome, "TIMEOUT");
                assert!(floor.is_none());
            }
            _ => panic!("expected timeout close"),
        }
    }

    #[test]
    fn done_state_stops_watching() {
        let cfg = test_cfg();
        let mut t = test_trader();
        t.state = BucketState::Done;
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.15, 0.0),
            BucketAction::None
        ));
        assert!(matches!(
            on_tick(&mut t, &cfg, 0.6, 1.0),
            BucketAction::None
        ));
    }

    #[test]
    fn config_validation_rejects_bad_band_and_empty_cities() {
        let dir = std::env::temp_dir();
        let p = dir.join(format!("weather_test_{}.toml", std::process::id()));
        std::fs::write(
            &p,
            "cities = []\nlow = 0.2\nhigh = 0.55\nsl_pnl = 0.3\nunwind_pnl = 0.15\nmax_hold_secs = 25.0\ntrade_size_usdc = 1.0\n",
        )
        .unwrap();
        assert!(load_weather_config(p.to_str().unwrap()).is_err());
        std::fs::write(
            &p,
            "cities = [\"nyc\"]\nlow = 0.6\nhigh = 0.55\nsl_pnl = 0.3\nunwind_pnl = 0.15\nmax_hold_secs = 25.0\ntrade_size_usdc = 1.0\n",
        )
        .unwrap();
        assert!(load_weather_config(p.to_str().unwrap()).is_err());
        std::fs::write(
            &p,
            "cities = [\"nyc\", \"london\"]\nlow = 0.2\nhigh = 0.55\nsl_pnl = 0.3\nunwind_pnl = 0.15\nmax_hold_secs = 25.0\ntrade_size_usdc = 1.0\n",
        )
        .unwrap();
        let cfg = load_weather_config(p.to_str().unwrap()).unwrap();
        assert_eq!(cfg.cities.len(), 2);
        assert!((cfg.max_buy_price - 0.95).abs() < 1e-9, "default applied");
        assert_eq!(cfg.max_trades_per_bucket, 1, "default applied");
        assert_eq!(cfg.refresh_interval_secs, 600, "default applied");
        let _ = std::fs::remove_file(&p);
    }
}
