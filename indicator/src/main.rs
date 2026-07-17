//! `indicator` binary — `run` (live: NATS in → NATS out) and `replay`
//! (parity harness: tick CSV in → indicator CSV out, same engine code path).

use std::io::Write as _;

use anyhow::{Context as _, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt as _;

use indicator::{AssetEngine, IndicatorConfig};

#[derive(Parser)]
#[command(about = "Standalone indicator engine (HAR vol, P(up), SNR)")]
struct Cli {
    /// Path to indicator.toml
    #[arg(long, default_value = "config/indicator.toml")]
    config: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Subscribe to price.binance.<ASSET> on NATS and publish indicator.<ASSET>.
    Run,
    /// Feed a `ts,price` CSV through the engine, write `slot,ts,vol_har,p_up,snr`.
    Replay {
        /// Input CSV (unix-seconds timestamp, price per line; header optional).
        #[arg(long)]
        input: String,
        /// Output CSV path.
        #[arg(long)]
        output: String,
        /// Asset name for per-asset beta/nu lookup.
        #[arg(long, default_value = "BTC")]
        asset: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install ring as the process-wide rustls provider (same as price_feed) —
    // async-nats needs one when TLS URLs are used.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let cfg = IndicatorConfig::load(&cli.config)
        .with_context(|| format!("load config {}", cli.config))?;

    match cli.cmd {
        Cmd::Run => run(cfg).await,
        Cmd::Replay {
            input,
            output,
            asset,
        } => replay(&cfg, &asset, &input, &output),
    }
}

/// Minimal payload view of price_feed's `price.binance.<ASSET>` messages.
#[derive(serde::Deserialize)]
struct BinanceTick {
    ts: f64,
    price: f64,
}

async fn run(cfg: IndicatorConfig) -> Result<()> {
    let nc = async_nats::connect(&cfg.nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", cfg.nats_url))?;
    println!(
        "[indicator] connected {} — assets {:?}, market {}, har windows {:?}, emit every {}ms",
        cfg.nats_url, cfg.assets, cfg.market, cfg.har_vol.windows, cfg.emit_interval_ms
    );

    let mut handles = Vec::new();
    for asset in &cfg.assets {
        let engine = AssetEngine::from_config(&cfg, asset)
            .map_err(|e| anyhow::anyhow!("engine for {asset}: {e}"))?;
        let mut sub = nc
            .subscribe(format!("price.binance.{asset}"))
            .await
            .with_context(|| format!("subscribe price.binance.{asset}"))?;
        let nc = nc.clone();
        let asset = asset.clone();
        let market = cfg.market.clone();
        let emit_gap = cfg.emit_interval_ms as f64 / 1000.0;
        handles.push(tokio::spawn(async move {
            let mut engine = engine;
            let subject = format!("indicator.{asset}");
            let mut last_emit_ts = 0.0f64;
            let mut published: u64 = 0;
            while let Some(msg) = sub.next().await {
                let Ok(tick) = serde_json::from_slice::<BinanceTick>(&msg.payload) else {
                    continue;
                };
                let Some(emit) = engine.on_tick(tick.ts, tick.price) else {
                    continue;
                };
                if emit.ts - last_emit_ts < emit_gap {
                    continue;
                }
                last_emit_ts = emit.ts;
                let payload = emit.to_json(&asset, &market);
                if nc
                    .publish(subject.clone(), payload.into_bytes().into())
                    .await
                    .is_err()
                {
                    eprintln!("[indicator] publish failed for {asset}; continuing");
                }
                published += 1;
                if published == 1 {
                    println!(
                        "[indicator] first publish for {asset}: slot={} vol_har={:?} p_up={:?} snr={:?}",
                        emit.slot, emit.vol_har, emit.p_up, emit.snr
                    );
                }
            }
        }));
    }

    tokio::signal::ctrl_c().await.context("ctrl_c")?;
    println!("[indicator] shutting down");
    for h in handles {
        h.abort();
    }
    Ok(())
}

/// Replay a `ts,price` CSV through the exact production engine and dump one
/// row per processed tick — the Rust half of the Python parity check.
fn replay(cfg: &IndicatorConfig, asset: &str, input: &str, output: &str) -> Result<()> {
    let mut engine =
        AssetEngine::from_config(cfg, asset).map_err(|e| anyhow::anyhow!("engine: {e}"))?;
    let text = std::fs::read_to_string(input).with_context(|| format!("read {input}"))?;
    let out_file = std::fs::File::create(output).with_context(|| format!("create {output}"))?;
    let mut out = std::io::BufWriter::new(out_file);
    writeln!(out, "slot,ts,vol_har,p_up,snr")?;

    let fmt = |v: Option<f64>| v.map(|x| format!("{x:.17e}")).unwrap_or_default();
    let mut rows = 0u64;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("ts") || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split(',');
        let (Some(ts), Some(price)) = (parts.next(), parts.next()) else {
            continue;
        };
        let (Ok(ts), Ok(price)) = (ts.trim().parse::<f64>(), price.trim().parse::<f64>()) else {
            continue;
        };
        if let Some(e) = engine.on_tick(ts, price) {
            writeln!(
                out,
                "{},{:.3},{},{},{}",
                e.slot,
                e.ts,
                fmt(e.vol_har),
                fmt(e.p_up),
                fmt(e.snr)
            )?;
            rows += 1;
        }
    }
    out.flush()?;
    println!("[indicator] replay: {rows} rows → {output}");
    Ok(())
}
