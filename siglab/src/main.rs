//! siglab — multi-market signal live-testing harness.
//!
//! Subscribes to many rotating Polymarket markets concurrently, drives one
//! `trader::machine::Machine` per (crypto market, configured variant) pair against live
//! ticks, and logs paper trade-record outcomes to JSONL. Also drives one
//! `bucket_reversal::BucketReversalEngine` per (weather/World Cup bucket, grid variant) pair
//! — a separate, self-contained pure-price-action engine, not `Machine` — see
//! `bucket_reversal.rs`'s doc comment for why. **No real orders, no parquet/raw tick
//! recording.** Config, logs, and process are entirely standalone from `trader`/
//! `price_feed` — see `siglab/config.rs`'s doc comment and
//! `siglab/doc/plan_weather_worldcup_trading_2026-07-13.md`.
//!
//! Every hour (HKT), writes/updates `{report_dir}/signal_report_{date}.md` — a
//! collapsible-sections Markdown summary of the last hour's trades, market state,
//! staleness health, and CPU/memory usage. A separate host-side script
//! (`siglab/scripts/push_report.sh`, run by a systemd --user timer, not by this process)
//! commits and pushes that file hourly, independent of this process needing git/SSH
//! credentials itself.

mod bucket_reversal;
mod cgroup;
mod config;
mod event_monitor;
mod market;
mod record;
mod report;
mod rotation;
mod snapshot;
mod staleness;
mod weather;
mod worldcup;

use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;

use trader::marketdata::{clob_client, http_client, now_secs_f64};

use crate::market::{StalenessTick, VariantSet};
use crate::record::SiglabTradeRecord;

#[derive(Parser, Debug)]
#[command(
    name = "siglab",
    about = "Multi-market signal live-testing harness — paper trades only, no orders, no parquet."
)]
struct Args {
    /// Path to siglab's own standalone crypto-market config (never trader's or
    /// price_feed's).
    #[arg(long, default_value = "config/markets.toml")]
    config: PathBuf,

    /// Path to siglab's own standalone weather-city config.
    #[arg(long, default_value = "config/weather_cities.toml")]
    weather_config: PathBuf,

    /// Path to siglab's own standalone FIFA World Cup event-list config.
    #[arg(long, default_value = "config/worldcup_events.toml")]
    worldcup_config: PathBuf,

    /// JSONL trade-record output path.
    #[arg(long, default_value = "siglab_trades.jsonl")]
    log: PathBuf,

    /// Directory the hourly signal_report_{date}.md is written into.
    #[arg(long, default_value = "reports")]
    report_dir: PathBuf,

    /// How often (seconds) to write the hourly report. Default 3600 (real hourly); pass a
    /// smaller value for quick local verification.
    #[arg(long, default_value_t = 3600)]
    report_interval_secs: u64,

    /// How often (seconds) to re-run weather discovery per city (handles the day rolling
    /// over and any bucket-set changes).
    #[arg(long, default_value_t = 3600)]
    weather_refresh_secs: u64,

    /// How often (seconds) to re-run World Cup event discovery (handles a market's bucket
    /// set changing, e.g. a new stage-of-elimination outcome).
    #[arg(long, default_value_t = 3600)]
    worldcup_refresh_secs: u64,

    /// Fraction (0.0-1.0) of currently-tracked feeds that must be past their first
    /// silence bucket simultaneously before logging a "possible connection-level outage"
    /// warning, distinct from ordinary per-market quiet stretches.
    #[arg(long, default_value_t = 0.8)]
    correlated_stale_threshold: f64,

    /// How often (seconds) to sweep staleness state.
    #[arg(long, default_value_t = 5)]
    staleness_poll_secs: u64,

    /// How often (seconds) to print a per-market-class trade-count heartbeat — cheap
    /// operational visibility for an otherwise-unattended process (DeepSeek review #10).
    #[arg(long, default_value_t = 300)]
    heartbeat_secs: u64,

    /// One-off: rebuild every signal_report_*.md in --report-dir from --log's trade-log
    /// ground truth (report::regenerate_from_trade_log), then exit immediately — no WS
    /// connections, no live harness. Used to backfill existing reports into a new rendering
    /// format; see that function's doc comment.
    #[arg(long)]
    regenerate_reports_only: bool,
}

fn append_jsonl(path: &PathBuf, rec: &SiglabTradeRecord) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {path:?}"))?;
    writeln!(f, "{}", serde_json::to_string(rec)?)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Same fix as price_feed/src/main.rs and trader/src/bin/live.rs — required once for
    // the whole process when multiple crates (reqwest, tokio-tungstenite) share rustls >=0.22.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();
    // Arc'd so the report-writer task (spawned below) can hold its own cheap handle and
    // render an always-current config table without cloning the whole config on every write.
    let cfg = std::sync::Arc::new(config::load(&args.config)?);
    let weather_cfg = config::load_weather(&args.weather_config)?;
    let worldcup_cfg = config::load_worldcup(&args.worldcup_config)?;

    eprintln!(
        "[siglab] loaded {} market(s), {} variant(s) from {:?}; {} weather cities from {:?}; \
         {} World Cup events from {:?}",
        cfg.markets.len(),
        cfg.variants.len(),
        args.config,
        weather_cfg.cities.len(),
        args.weather_config,
        worldcup_cfg.events.len(),
        args.worldcup_config
    );

    if args.regenerate_reports_only {
        let paths = report::regenerate_from_trade_log(
            &args.log,
            &args.report_dir,
            &cfg,
            &weather_cfg.cities,
            &worldcup_cfg.events,
        )?;
        eprintln!("[siglab] regenerated {} report(s): {paths:?}", paths.len());
        return Ok(());
    }

    let http = http_client()?;
    let clob = clob_client();

    let (trade_tx, mut trade_rx) = mpsc::unbounded_channel::<SiglabTradeRecord>();
    let (stale_tx, mut stale_rx) = mpsc::unbounded_channel::<StalenessTick>();
    let clients = market::SharedClients {
        http: http.clone(),
        clob: clob.clone(),
    };
    let snapshots = snapshot::new_shared();
    let sinks = market::Sinks {
        trade_tx: trade_tx.clone(),
        stale_tx: stale_tx.clone(),
        snapshots: snapshots.clone(),
    };

    // One real Binance connection per unique asset, fanned out via broadcast to every
    // duration task trading that asset — not one connection per (asset, duration) market
    // (caught by running the harness locally before the first Docker resource pass, see
    // market.rs's doc comment).
    let mut binance_feeds: HashMap<
        String,
        tokio::sync::broadcast::Sender<trader::types::BinanceTick>,
    > = HashMap::new();
    for m in &cfg.markets {
        binance_feeds
            .entry(m.asset.clone())
            .or_insert_with(|| market::spawn_binance_broadcast(&m.asset));
    }
    for m in &cfg.hourly_markets {
        binance_feeds
            .entry(m.asset.clone())
            .or_insert_with(|| market::spawn_binance_broadcast(&m.asset));
    }

    // ── spawn one task per configured crypto market, sharing one clob/http client ──
    let mut handles = Vec::new();
    for m in &cfg.markets {
        let variants: VariantSet = cfg
            .variants
            .iter()
            .filter(|v| v.applies_to(&m.asset))
            .map(|v| v.to_asset_params(&m.asset).map(|p| (v.id.clone(), p)))
            .collect::<Result<_>>()
            .with_context(|| format!("resolving variants for {}-{}", m.asset, m.suffix))?;
        let binance_rx = binance_feeds[&m.asset].subscribe();
        let rotation = rotation::Rotation::Periodic {
            suffix: m.suffix.clone(),
            period_secs: m.period_secs,
        };

        let handle = tokio::spawn(market::run_market(
            m.asset.clone(),
            rotation,
            variants,
            clients.clone(),
            sinks.clone(),
            binance_rx,
        ));
        handles.push(handle);
    }

    // ── spawn one task per configured hourly-ET crypto market ──
    for m in &cfg.hourly_markets {
        let variants: VariantSet = cfg
            .variants
            .iter()
            .filter(|v| v.applies_to(&m.asset))
            .map(|v| v.to_asset_params(&m.asset).map(|p| (v.id.clone(), p)))
            .collect::<Result<_>>()
            .with_context(|| format!("resolving variants for {}-hourly-et", m.asset))?;
        let binance_rx = binance_feeds[&m.asset].subscribe();
        let rotation = rotation::Rotation::HourlyEt {
            coin_name: m.coin_name.clone(),
        };

        let handle = tokio::spawn(market::run_market(
            m.asset.clone(),
            rotation,
            variants,
            clients.clone(),
            sinks.clone(),
            binance_rx,
        ));
        handles.push(handle);
    }

    let event_sinks = event_monitor::EventSinks {
        snapshots: snapshots.clone(),
        stale_tx: stale_tx.clone(),
        trade_tx: trade_tx.clone(),
    };

    // ── spawn one discovery+monitoring supervisor per weather city ──
    for city in &weather_cfg.cities {
        tokio::spawn(weather::run_city_supervisor(
            city.clone(),
            clients.clone(),
            event_sinks.clone(),
            args.weather_refresh_secs,
        ));
    }

    // ── spawn one discovery+monitoring supervisor per World Cup event ──
    for slug in &worldcup_cfg.events {
        tokio::spawn(worldcup::run_event_supervisor_for(
            slug.clone(),
            clients.clone(),
            event_sinks.clone(),
            args.worldcup_refresh_secs,
        ));
    }

    drop(trade_tx);
    drop(stale_tx);

    // ── single-writer trade log + heartbeat counter ──
    let log_path = args.log.clone();
    let writer_task = tokio::spawn(async move {
        let mut counts: HashMap<String, u64> = HashMap::new();
        let mut last_heartbeat = now_secs_f64();
        loop {
            tokio::select! {
                maybe_rec = trade_rx.recv() => {
                    match maybe_rec {
                        Some(rec) => {
                            *counts.entry(rec.variant_id.clone()).or_insert(0) += 1;
                            if let Err(e) = append_jsonl(&log_path, &rec) {
                                eprintln!("[siglab] trade-log write error: {e:#}");
                            } else {
                                eprintln!(
                                    "[TRADE] variant={} asset={} outcome={} pnl={:.4}",
                                    rec.variant_id, rec.asset, rec.outcome, rec.pnl
                                );
                            }
                        }
                        None => break, // all market tasks dropped their sender — shutting down
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    let now = now_secs_f64();
                    if now - last_heartbeat >= 300.0 {
                        last_heartbeat = now;
                        eprintln!("[siglab] heartbeat: trade counts by variant = {counts:?}");
                    }
                }
            }
        }
    });

    // ── staleness sweep — also feeds the hourly report's staleness section ──
    let stale_log = report::new_stale_log();
    let correlated_threshold = args.correlated_stale_threshold;
    let staleness_poll_secs = args.staleness_poll_secs;
    let staleness_task = {
        let stale_log = stale_log.clone();
        tokio::spawn(async move {
            let mut tracker = staleness::StalenessTracker::new();
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(staleness_poll_secs));
            loop {
                tokio::select! {
                    maybe_tick = stale_rx.recv() => {
                        match maybe_tick {
                            Some((market, feed, now_ms)) => tracker.on_tick(&format!("{market}:{feed}"), now_ms),
                            None => break,
                        }
                    }
                    _ = ticker.tick() => {
                        let now_ms = (now_secs_f64() * 1000.0) as i64;
                        // Classed per market kind — see staleness.rs's poll() doc comment:
                        // mixing crypto (fast-ticking) and weather (naturally much quieter)
                        // into one ratio false-alarms on weather's normal quiet stretches.
                        let (events, correlated) = tracker.poll(now_ms, correlated_threshold, |market| {
                            if market.starts_with("weather:") { "weather" } else { "crypto" }
                        });
                        for e in events {
                            eprintln!(
                                "[STALE] {} silent {}ms (crossed {}ms bucket)",
                                e.market, e.silent_ms, e.bucket_ms
                            );
                            report::log_stale_event(&stale_log, now_ms, e.market, e.silent_ms, e.bucket_ms);
                        }
                        for (class, is_correlated) in correlated {
                            if is_correlated {
                                eprintln!(
                                    "[STALE][WARN] most tracked {class} feeds are quiet simultaneously — \
                                     possible connection-level issue, not normal per-market quiet"
                                );
                            }
                        }
                    }
                }
            }
        })
    };

    // ── hourly report writer ──
    let report_task = {
        let cfg = cfg.clone();
        let weather_cities = weather_cfg.cities.clone();
        let worldcup_events = worldcup_cfg.events.clone();
        let snapshots = snapshots.clone();
        let stale_log = stale_log.clone();
        let trade_log_path = args.log.clone();
        let report_dir = args.report_dir.clone();
        let interval_secs = args.report_interval_secs;
        tokio::spawn(async move {
            let mut cgroup_prev = cgroup::sample();
            // tokio::time::interval() fires its *first* tick immediately, not after one
            // full interval — without interval_at, the first report would be written
            // before market discovery has even completed (caught in local testing: an
            // empty "0 crypto markets, 0 weather buckets" section landed in the report,
            // immediately superseded by a real one one interval later). Skip that by
            // starting the periodic clock one interval in the future.
            let start = tokio::time::Instant::now() + std::time::Duration::from_secs(interval_secs);
            let mut ticker =
                tokio::time::interval_at(start, std::time::Duration::from_secs(interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let cgroup_now = cgroup::sample();
                match report::write_hourly_report(
                    &report_dir,
                    &cfg,
                    &weather_cities,
                    &worldcup_events,
                    &snapshots,
                    &trade_log_path,
                    &stale_log,
                    cgroup_prev,
                    cgroup_now,
                    interval_secs as f64,
                ) {
                    Ok(path) => eprintln!("[siglab] wrote hourly report to {path:?}"),
                    Err(e) => eprintln!("[siglab] report write failed: {e:#}"),
                }
                cgroup_prev = cgroup_now;
            }
        })
    };

    eprintln!(
        "[siglab] running {} crypto market task(s), {} weather city task(s), {} World Cup event task(s); trade log {:?}, reports every {}s in {:?}",
        handles.len(),
        weather_cfg.cities.len(),
        worldcup_cfg.events.len(),
        args.log,
        args.report_interval_secs,
        args.report_dir
    );

    // Crypto market tasks run forever (reconnect internally); the process exits if one
    // returns an error (config/discovery bug) — the other tasks keep running otherwise.
    let (result, _idx, _rest) = futures_lite_select(handles).await;
    if let Err(e) = result {
        eprintln!("[siglab] a market task exited unexpectedly: {e:#}");
    }
    writer_task.abort();
    staleness_task.abort();
    report_task.abort();
    Ok(())
}

/// Wait for the first of several `JoinHandle`s to finish, returning its (flattened) result.
/// Small local helper instead of pulling in `futures::future::select_all` for one call site.
async fn futures_lite_select(
    handles: Vec<tokio::task::JoinHandle<Result<()>>>,
) -> (Result<()>, usize, Vec<tokio::task::JoinHandle<Result<()>>>) {
    let (result, idx, rest) = futures::future::select_all(handles).await;
    let flattened = result.unwrap_or_else(|e| Err(anyhow::anyhow!("task panicked: {e}")));
    (flattened, idx, rest)
}
