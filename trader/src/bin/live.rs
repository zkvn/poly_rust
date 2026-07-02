// Live driver — bridges worker.rs's pure sync (state, event) -> actions core
// to the real ExecutionEngine + live feeds (§10: sync core, async shell).
//
// Single (asset, strategy) instance, hard-capped at `--max-trades` completed
// trades (default 1) so a real-money run is bounded regardless of how long it
// takes for the strategy to naturally fire. Uses the PriceMonitor exit arm in
// practice for small sizes (a $1 buy yields far fewer than 5 shares at any
// plausible entry price, so the GTC-resting path is defensive/unexercised
// here, not because it's unimplemented).

use std::str::FromStr as _;

use anyhow::{Context, Result};
use clap::Parser;
use polymarket_client_sdk_v2::types::{Address, U256};
use tokio::sync::mpsc;

use trader::execution::{
    local_signer_from_key, signature_type_from_env, ExecutionEngine, LiveConfig, LiveExecutionEngine, SellStatus,
};
use trader::marketdata::{clob_client, current_slot, fetch_meta, http_client, make_slug, spawn_binance_task, PolySub};
use trader::types::{CycleContext, Outcome, Side, TradeRecord};
use trader::worker::{Action, CloseReason, ControlEvent, Event, Worker};

const DEFAULT_FUND_ADDRESS: &str = "0x9FC2A777C26CCA2C218D8E7BBC340D14058CC13A";
// NOTE: clob-v2.polymarket.com 301-redirects POST /order to clob.polymarket.com,
// and that redirect silently downgrades POST to GET (standard client behavior
// for 301), which turns into a 405 on the real endpoint. Use the real host
// directly — same one bot/config.py's CLOB_HOST has always pointed at.
const CLOB_HOST: &str = "https://clob.polymarket.com";

type Signer = alloy::signers::local::LocalSigner<alloy::signers::k256::ecdsa::SigningKey>;

#[derive(Parser, Debug)]
#[command(name = "live", about = "Live trading driver — one (asset, strategy), real orders, hard-capped trade count")]
struct Args {
    #[arg(long)]
    asset: String,

    #[arg(long, default_value = "reversal")]
    strategy: String,

    #[arg(long, default_value_t = 1.0)]
    size_usdc: f64,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/config")]
    config_dir: String,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/.env")]
    env_file: String,

    #[arg(long, default_value = "live_trades.csv")]
    log: String,

    #[arg(long, default_value = "live_state.json")]
    state_file: String,

    #[arg(long, default_value_t = 1)]
    max_trades: u32,

    #[arg(long, default_value_t = 300)]
    period_secs: u64,
}

fn append_csv_header_if_new(path: &str) -> Result<()> {
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new().create(true).write(true).open(path)?;
    writeln!(f, "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl")?;
    Ok(())
}

fn log_trade(path: &str, rec: &TradeRecord) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(), rec.slug, rec.strategy, rec.side.as_str(),
        rec.entry_ts, rec.token_price, rec.exit_price, rec.outcome.as_str(), rec.pnl)?;
    Ok(())
}

fn persist(worker: &Worker, state_file: &str) {
    let snap = worker.to_persisted();
    if let Ok(json) = serde_json::to_string_pretty(&snap) {
        let _ = std::fs::write(state_file, json);
    }
}

/// Shared mutable driver context threaded through the recursive action loop.
struct Driver<'a> {
    engine: &'a LiveExecutionEngine<Signer>,
    up_id: U256,
    dn_id: U256,
    current_token_id: Option<U256>,
    max_buy_price: f64,
    log_path: String,
    state_file: String,
    trades_completed: u32,
}

impl Driver<'_> {
    /// Execute one `Action` against the live engine; returns the follow-up
    /// `Event` (if any) to feed back into `worker.step`.
    async fn execute(&mut self, action: &Action) -> Option<Event> {
        match action {
            Action::PlaceBuy { side, price, size_usdc } => {
                let token_id = if *side == Side::Up { self.up_id } else { self.dn_id };
                self.current_token_id = Some(token_id);
                let result = self.engine.place(token_id, *price, *size_usdc, self.max_buy_price).await;
                println!("[ORDER] BUY {side:?} @ {price:.4} size=${size_usdc:.2} -> placed={} shares={:.4} cost={:.4} err={:?}",
                    result.placed, result.filled_shares, result.cost, result.error);
                if result.placed && result.filled_shares > 0.0 {
                    Some(Event::OrderFilled { filled_shares: result.filled_shares, cost: result.cost })
                } else {
                    Some(Event::OrderRejected)
                }
            }
            Action::PlaceLimitSell { shares, price } => {
                let Some(token_id) = self.current_token_id else { return None };
                let (order_id, status) = self.engine.place_limit_sell(token_id, *shares, *price).await;
                println!("[ORDER] LIMIT SELL {shares:.4} @ {price:.4} -> status={status:?} order_id={order_id:?}");
                Some(Event::LimitSellPlaced { order_id, status })
            }
            Action::ClosePosition { shares, reason } => {
                let Some(token_id) = self.current_token_id else { return None };
                if matches!(reason, CloseReason::StopLoss) {
                    println!("[SL] stop-loss triggered — closing {shares:.4} shares (sl_pnl floor crossed; up to 5 retries)");
                }
                let result = self.engine.close_position(token_id, *shares).await;
                println!("[ORDER] CLOSE {shares:.4} ({reason:?}) -> status={:?} sold={:.4} usdc={:.4}",
                    result.status, result.shares_sold, result.filled_usdc);
                let sold = result.shares_sold;
                let exit_price = if sold > 0.0 { result.filled_usdc / sold } else { 0.0 };
                let matched = matches!(result.status, SellStatus::Matched);
                let event = match (matched, reason) {
                    (true, CloseReason::TakeProfit) => Event::UnwindFilled { sold_shares: sold, exit_price },
                    (true, CloseReason::StopLoss) => Event::StopSellFilled { sold_shares: sold, exit_price },
                    (false, CloseReason::TakeProfit) => Event::UnwindFailed,
                    (false, CloseReason::StopLoss) => Event::StopSellFailed,
                };
                if matched && sold >= *shares {
                    self.current_token_id = None;
                }
                Some(event)
            }
            Action::CancelLimitSell { order_id } => {
                let ok = self.engine.cancel_limit_sell(order_id).await;
                println!("[ORDER] CANCEL {order_id} -> {ok}");
                None
            }
            Action::Persist | Action::LogTrade(_) => None, // handled by process_actions directly
        }
    }

    /// Runs a batch of actions to completion, recursively feeding follow-up
    /// events back through the worker. `Box::pin` because this is a
    /// self-referential async recursion.
    fn process_actions<'b>(
        &'b mut self,
        worker: &'b mut Worker,
        actions: Vec<Action>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'b>> {
        Box::pin(async move {
            for action in &actions {
                match action {
                    Action::Persist => persist(worker, &self.state_file),
                    Action::LogTrade(rec) => {
                        println!("[TRADE] {rec:?}");
                        if let Err(e) = log_trade(&self.log_path, rec) {
                            eprintln!("log error: {e:#}");
                        }
                        if matches!(rec.outcome, Outcome::Win | Outcome::Loss | Outcome::StopLoss | Outcome::Unwind) {
                            self.trades_completed += 1;
                        }
                    }
                    _ => {
                        if let Some(followup) = self.execute(action).await {
                            let more = worker.step(followup);
                            self.process_actions(worker, more).await;
                        }
                    }
                }
            }
        })
    }
}

#[tokio::main]
#[allow(unused_assignments, unused_variables)]
async fn main() -> Result<()> {
    let args = Args::parse();

    dotenvy::from_path(&args.env_file).with_context(|| format!("load {}", args.env_file))?;
    let key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
    let signer = local_signer_from_key(&key)?;
    let funder_raw = std::env::var("FUND_ADDRESS").unwrap_or_else(|_| DEFAULT_FUND_ADDRESS.to_string());
    let funder = Address::from_str(&funder_raw)?;
    let signature_type = signature_type_from_env()?;

    append_csv_header_if_new(&args.log)?;

    let toml = trader::config::load_latest(&args.config_dir)?;
    let mut params = toml.resolve(&args.asset)?;
    params.trade_size_usdc = args.size_usdc;
    let max_buy_price = params.max_buy_price;

    let mut worker = match args.strategy.as_str() {
        "reversal" => Worker::new_reversal(&args.asset, &params),
        "high_prob" => Worker::new_high_prob(&args.asset, &params),
        other => anyhow::bail!("unknown strategy: {other}"),
    };

    println!(
        "[live] asset={} strategy={} size_usdc=${:.2} max_trades={} log={} state_file={}",
        args.asset, args.strategy, args.size_usdc, args.max_trades, args.log, args.state_file
    );
    println!("[live] REAL MONEY — this will place live orders on production Polymarket.");

    // Route CLOB writes through the EC2 HTTP proxy when running from a geo-restricted
    // region (same var that Python's _patch_clob_proxy reads; empty = direct connect).
    // reqwest reads HTTPS_PROXY from the environment at Client::builder().build() time.
    if let Ok(proxy_url) = std::env::var("CLOB_PROXY_URL") {
        if !proxy_url.is_empty() {
            // Safety: single-threaded at this point in main() — tokio runtime not yet
            // spawning work, and no other thread reads HTTPS_PROXY concurrently.
            unsafe { std::env::set_var("HTTPS_PROXY", &proxy_url) };
            println!("[live] routing CLOB writes via proxy: {proxy_url}");
        }
    }

    let engine =
        LiveExecutionEngine::connect(CLOB_HOST, signer, funder, signature_type, LiveConfig::default()).await?;
    let mut driver = Driver {
        engine: &engine,
        up_id: U256::from(0u64),
        dn_id: U256::from(0u64),
        current_token_id: None,
        max_buy_price,
        log_path: args.log.clone(),
        state_file: args.state_file.clone(),
        trades_completed: 0,
    };

    let (binance_tx, mut binance_rx) = mpsc::unbounded_channel();
    spawn_binance_task(&args.asset, binance_tx);
    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<trader::types::PolyTick>();

    let http = http_client()?;
    let clob = clob_client();

    let mut last_binance: f64 = 0.0;
    let mut current_slug: Option<String> = None;
    let mut current_slot_val: u64 = 0;
    let mut poly_sub: Option<PolySub> = None;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_poly_up: f64 = 0.0;
    let mut last_poly_dn: f64 = 0.0;

    loop {
        if driver.trades_completed >= args.max_trades {
            println!("[live] max_trades ({}) reached — shutting down cleanly.", args.max_trades);
            return Ok(());
        }

        tokio::select! {
            Some(tick) = binance_rx.recv() => {
                last_binance = tick.price;
                if current_slug.is_some() {
                    let actions = worker.step(Event::BinanceTick(tick));
                    driver.process_actions(&mut worker, actions).await;
                }
            }

            Some(tick) = poly_rx.recv() => {
                last_poly_up = tick.up;
                last_poly_dn = tick.dn;
                if current_slug.is_some() {
                    let actions = worker.step(Event::PolyTick(tick));
                    driver.process_actions(&mut worker, actions).await;
                }
            }

            _ = heartbeat.tick() => {
                if let Some(slug) = &current_slug {
                    let time_left = current_slot_val as f64 + args.period_secs as f64 - trader::marketdata::now_secs_f64();
                    println!("[live] heartbeat slug={slug} T-{time_left:.0}s binance={last_binance:.4} up={last_poly_up:.4} dn={last_poly_dn:.4}");
                }
            }

            _ = ticker.tick() => {
                let slot = current_slot(args.period_secs);
                if slot != current_slot_val {
                    if current_slug.is_some() {
                        let actions = worker.step(Event::CycleClose);
                        driver.process_actions(&mut worker, actions).await;
                    }
                    if last_binance <= 0.0 { continue; }

                    let slug = make_slug(&args.asset, slot, "5m");
                    match fetch_meta(&http, &slug).await {
                        Ok((u, d)) => {
                            driver.up_id = u;
                            driver.dn_id = d;
                            poly_sub = Some(PolySub::start(&clob, u, poly_tx.clone()));
                            let ctx = CycleContext {
                                start_ts: slot as f64, end_ts: (slot + args.period_secs) as f64, open_binance: last_binance,
                            };
                            println!("[live] new cycle slug={slug} open_binance={last_binance}");
                            let actions = worker.step(Event::CycleOpen { ctx, slug: slug.clone(), entry_suppressed: false });
                            driver.process_actions(&mut worker, actions).await;
                            current_slug = Some(slug);
                            current_slot_val = slot;
                        }
                        Err(e) => eprintln!("[live] meta fetch failed for {slug}: {e:#}"),
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("[live] shutting down (SIGINT).");
                worker.step(Event::Control(ControlEvent::Halt));
                return Ok(());
            }
        }
    }
}
