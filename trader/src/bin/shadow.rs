// A2 — shadow live feed logger.
//
// Subscribes to live Binance + Polymarket feeds for one asset, drives the same
// Machine used by the backtest (sim decisions only — no CLOB writes), and logs
// would-be trades to CSV. Purpose: prove live-fed events produce the same
// decisions as the A1 bt1-parity golden, before any live order wiring exists.

use std::fs::OpenOptions;
use std::io::Write as _;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;

use trader::config::load_latest;
use trader::machine::Machine;
use trader::marketdata::{
    clob_client, current_slot, fetch_meta, http_client, make_slug, spawn_binance_task, PolySub,
};
use trader::types::CycleContext;

#[derive(Parser, Debug)]
#[command(name = "shadow", about = "Shadow-live trader: logs would-be trades from live feeds, no CLOB writes")]
struct Args {
    #[arg(long, help = "Asset (e.g. BTC)")]
    asset: String,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/config")]
    config_dir: String,

    #[arg(long, default_value = "shadow_trades.csv")]
    log: String,

    #[arg(long, default_value = "5m")]
    suffix: String,

    #[arg(long, default_value_t = 300)]
    period_secs: u64,
}

fn append_csv_header_if_new(path: &str) -> Result<()> {
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
    writeln!(f, "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl")?;
    Ok(())
}

fn log_trade(path: &str, rec: &trader::types::TradeRecord) -> Result<()> {
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(
        f,
        "{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(),
        rec.slug,
        rec.strategy,
        rec.side.as_str(),
        rec.entry_ts,
        rec.token_price,
        rec.exit_price,
        rec.outcome.as_str(),
        rec.pnl,
    )?;
    Ok(())
}

#[tokio::main]
#[allow(unused_assignments, unused_variables)]
async fn main() -> Result<()> {
    let args = Args::parse();

    append_csv_header_if_new(&args.log)?;

    let toml = load_latest(&args.config_dir)?;
    let params = toml.resolve(&args.asset)?;

    let mut machines: Vec<Machine> = params.strategies.iter().map(|name| match name.as_str() {
        "reversal" => Machine::new_reversal(&params),
        "high_prob" => Machine::new_high_prob(&params),
        _ => Machine::new_reversal(&params),
    }).collect();

    eprintln!(
        "[shadow] asset={} strategies={:?} suffix={} period={}s log={}",
        args.asset, params.strategies, args.suffix, args.period_secs, args.log
    );

    let (binance_tx, mut binance_rx) = mpsc::unbounded_channel();
    spawn_binance_task(&args.asset, binance_tx);

    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel();

    let http = http_client()?;
    let clob = clob_client();

    let mut last_binance: f64 = 0.0;
    let mut current_slug: Option<String> = None;
    let mut current_slot_val: u64 = 0;
    // Held only for its Drop side-effect (aborts the previous cycle's subscription).
    let mut poly_sub: Option<PolySub> = None;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(tick) = binance_rx.recv() => {
                last_binance = tick.price;
                if current_slug.is_some() {
                    for m in machines.iter_mut() {
                        m.on_binance(tick);
                    }
                }
            }

            Some(tick) = poly_rx.recv() => {
                if current_slug.is_some() {
                    for m in machines.iter_mut() {
                        if let Some(rec) = m.on_poly(tick) {
                            println!("[EXIT] {rec:?}");
                            if let Err(e) = log_trade(&args.log, &rec) {
                                eprintln!("log error: {e:#}");
                            }
                        }
                    }
                }
            }

            _ = ticker.tick() => {
                let slot = current_slot(args.period_secs);
                if slot != current_slot_val {
                    // Close out the previous cycle (if any) before rotating.
                    if current_slug.is_some() {
                        for m in machines.iter_mut() {
                            if let Some(rec) = m.cycle_close() {
                                println!("[RESOLVED] {rec:?}");
                                if let Err(e) = log_trade(&args.log, &rec) {
                                    eprintln!("log error: {e:#}");
                                }
                            }
                        }
                    }

                    if last_binance <= 0.0 {
                        // No Binance data yet — wait for the next tick before starting a cycle.
                        continue;
                    }

                    let slug = make_slug(&args.asset, slot, &args.suffix);
                    match fetch_meta(&http, &slug).await {
                        Ok((up_id, _dn_id)) => {
                            eprintln!("[shadow] new cycle slug={slug} open_binance={last_binance}");
                            poly_sub = Some(PolySub::start(&clob, up_id, poly_tx.clone()));
                            let ctx = CycleContext {
                                start_ts: slot as f64,
                                end_ts: (slot + args.period_secs) as f64,
                                open_binance: last_binance,
                            };
                            for m in machines.iter_mut() {
                                m.cycle_open(&ctx, &slug, false);
                            }
                            current_slug = Some(slug);
                            current_slot_val = slot;
                        }
                        Err(e) => {
                            eprintln!("[shadow] meta fetch failed for {slug}: {e:#} — will retry next tick");
                        }
                    }
                }
            }
        }
    }
}
