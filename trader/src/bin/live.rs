// Live driver — bridges worker.rs's pure sync (state, event) -> actions core
// to the real ExecutionEngine + live feeds (§10: sync core, async shell).
//
// Multi-asset: one process manages N independent (asset, strategy) `Worker`s
// sharing one account/engine/Telegram bot. This is required, not cosmetic —
// running one process per asset means N processes each try to long-poll
// Telegram's getUpdates on the *same* bot token, and only one wins (the
// others get silent 409 Conflicts), breaking remote control for everyone but
// the first-started process. One shared poller avoids that entirely.
//
// Each asset is hard-capped at `--max-trades` completed trades (default 1) so
// a real-money run is bounded regardless of how long it takes for the
// strategy to naturally fire; the process exits once *all* assets are capped
// (or on SIGINT). Uses the PriceMonitor exit arm in practice for small sizes
// (a $1 buy yields far fewer than 5 shares at any plausible entry price, so
// the GTC-resting path is defensive/unexercised here, not because it's
// unimplemented).

use std::str::FromStr as _;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use polymarket_client_sdk_v2::types::{Address, U256};
use tokio::sync::mpsc;

use trader::balance::{seconds_until_next_check, BalanceGuard};
use trader::config::AssetParams;
use trader::execution::{
    local_signer_from_key, signature_type_from_env, ExecutionEngine, LiveConfig, LiveExecutionEngine, SellStatus,
};
use futures::StreamExt as _;
use trader::marketdata::{
    clob_client, current_slot, fetch_gamma_resolution, fetch_meta, http_client, make_slug, now_secs_f64,
    spawn_binance_task, PolySub,
};
use trader::telegram::commands::{parse_command, Command};
use trader::telegram::render::HELP_TEXT;
use trader::telegram::{AuthConfig, TelegramBot};
use trader::types::{BinanceTick, CycleContext, Outcome, PolyTick, Side, TradeRecord};
use trader::worker::{Action, BalanceEvent, CloseReason, ControlEvent, Event, Worker};

const DEFAULT_FUND_ADDRESS: &str = "0x9FC2A777C26CCA2C218D8E7BBC340D14058CC13A";
// NOTE: clob-v2.polymarket.com 301-redirects POST /order to clob.polymarket.com,
// and that redirect silently downgrades POST to GET (standard client behavior
// for 301), which turns into a 405 on the real endpoint. Use the real host
// directly — same one bot/config.py's CLOB_HOST has always pointed at.
const CLOB_HOST: &str = "https://clob.polymarket.com";

type Signer = alloy::signers::local::LocalSigner<alloy::signers::k256::ecdsa::SigningKey>;

#[derive(Parser, Debug)]
#[command(name = "live", about = "Live trading driver — N (asset, strategy) workers, one shared account/Telegram bot")]
struct Args {
    /// Comma-separated asset list, e.g. "DOGE,BTC" (also accepts repeated --asset flags).
    /// Strategy/strategies per asset always come from `config_dir`'s
    /// `strategy_*.toml` (`AssetParams.strategies`) — never a CLI override, so this
    /// process can't silently drift from the config the Python bot reads.
    #[arg(long, value_delimiter = ',', required = true)]
    asset: Vec<String>,

    #[arg(long, default_value_t = 1.0)]
    size_usdc: f64,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/config")]
    config_dir: String,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/.env")]
    env_file: String,

    /// Directory for per-(asset,strategy) trade logs/state files
    /// (live_trades_<asset>_<strategy>.csv, live_state_<asset>_<strategy>.json).
    #[arg(long, default_value = "live_logs")]
    log_dir: String,

    #[arg(long, default_value_t = 1)]
    max_trades: u32,

    #[arg(long, default_value_t = 300)]
    period_secs: u64,

    /// Subscribe to price ticks from a price_feed NATS publisher instead of opening
    /// direct Binance/Poly WS connections (e.g. nats://localhost:4222).
    #[arg(long)]
    nats_url: Option<String>,
}

/// Current time in Hong Kong (UTC+8), matching the Python bot's `_HKT` convention.
fn hkt_now() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
}

/// Display-only side label with an arrow — doesn't touch `Side::as_str()` (used by CSV
/// logging), just makes Telegram messages easier to scan at a glance.
fn arrow_side(side: Side) -> &'static str {
    match side {
        Side::Up => "UP ↑",
        Side::Down => "DOWN ↓",
    }
}

/// Poll Gamma for `slug`'s resolution until it settles, then report the result relative
/// to `side` (the position's own side) back on `tx` as `(asset, strategy, won)`. Mirrors
/// `bot/worker.py::_api_result_watcher`'s cadence: up to 20 attempts, 30s apart (~10 min
/// ceiling), giving up silently (just a log line) if it never resolves in that window.
fn spawn_resolution_watcher(
    http: reqwest::Client,
    slug: String,
    side: Side,
    asset: String,
    strategy: &'static str,
    tx: mpsc::UnboundedSender<(String, &'static str, bool)>,
) {
    tokio::spawn(async move {
        for attempt in 1..=20 {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            match fetch_gamma_resolution(&http, &slug).await {
                Some(went_up) => {
                    let won = match side { Side::Up => went_up, Side::Down => !went_up };
                    let _ = tx.send((asset, strategy, won));
                    return;
                }
                None => println!("[live] API pending (attempt {attempt}/20) for {slug}"),
            }
        }
        println!("[live] gave up waiting for API resolution of {slug} after 20 attempts (~10 min)");
    });
}

fn append_csv_header_if_new(path: &str) -> Result<()> {
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new().create(true).write(true).open(path)?;
    writeln!(f, "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error")?;
    Ok(())
}

/// Comma-joined CSV writer (no `csv` crate) — strip characters that would
/// break the naive comma-split so a raw SDK error message can't corrupt the row.
fn csv_sanitize(s: &str) -> String {
    s.replace(',', ";").replace('\n', " ")
}

fn log_trade(path: &str, rec: &TradeRecord) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    let exit_last_error = rec.exit_last_error.as_deref().map(csv_sanitize).unwrap_or_default();
    writeln!(f, "{},{},{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(), rec.slug, rec.strategy, rec.side.as_str(),
        rec.entry_ts, rec.token_price, rec.exit_price, rec.outcome.as_str(), rec.pnl,
        rec.exit_attempts, exit_last_error)?;
    Ok(())
}

fn persist(worker: &Worker, state_file: &str) {
    let snap = worker.to_persisted();
    if let Ok(json) = serde_json::to_string_pretty(&snap) {
        let _ = std::fs::write(state_file, json);
    }
}

/// Everything one (asset, strategy) pair's cycle needs, mutated in place as
/// ticks/events arrive. `worker` lives inside so `process_actions`/`execute`
/// only need one `&mut` borrow. An asset with multiple configured strategies
/// (e.g. ETH: high_prob + reversal) gets one `AssetSlot` per strategy, each
/// independently tracking its own position/win-loss state.
struct AssetSlot {
    worker: Worker,
    params: AssetParams,
    up_id: U256,
    dn_id: U256,
    current_token_id: Option<U256>,
    max_buy_price: f64,
    log_path: String,
    state_file: String,
    trades_completed: u32,
    wins: u32,
    losses: u32,
    stoplosses: u32,
    unwinds: u32,
    total_pnl: f64,
    last_trade: Option<String>,
    current_slug: Option<String>,
    last_binance: f64,
    last_poly_up: f64,
    last_poly_dn: f64,
    poly_sub: Option<PolySub>,
}

/// Shared context (account connection + Telegram) threaded through the
/// recursive action loop. Holds no per-asset state — that all lives in
/// `AssetSlot` — so it only needs `&self`, letting multiple assets share one
/// `Driver` without fighting over a mutable borrow.
struct Driver<'a> {
    engine: &'a LiveExecutionEngine<Signer>,
    telegram: Option<Arc<TelegramBot>>,
    http: reqwest::Client,
    api_result_tx: mpsc::UnboundedSender<(String, &'static str, bool)>,
}

impl Driver<'_> {
    async fn notify(&self, text: &str) {
        if let Some(bot) = &self.telegram {
            if let Err(e) = bot.send(text).await {
                eprintln!("[telegram] send error: {e:#}");
            }
        }
    }

    /// Full `/status` reply: balance+time header, then TRADE ASSETS / MARKETS
    /// / PNL sections. One row per `(asset, strategy)` slot, sorted so a
    /// multi-strategy asset's rows render adjacently (mirrors Python's
    /// per-asset-then-per-strategy `_status()` breakdown).
    async fn render_status(&self, assets: &[AssetSlot]) -> String {
        let now = hkt_now().format("%H:%M:%S HKT");
        let balance = match self.engine.fetch_balance().await {
            Some(b) => format!("${b:.4}"),
            None => "n/a (fetch failed)".to_string(),
        };
        let mut sections = vec![format!("📊 <b>STATUS</b>  ({now})\nBalance: {balance}")];

        let mut slots: Vec<&AssetSlot> = assets.iter().collect();
        slots.sort_by(|a, b| {
            (a.worker.asset.as_str(), a.worker.strategy_name)
                .cmp(&(b.worker.asset.as_str(), b.worker.strategy_name))
        });

        let mut ta_lines = Vec::new();
        let mut mkt_lines = Vec::new();
        let mut pnl_lines = Vec::new();
        let (mut tw, mut tl, mut ts, mut tu) = (0u32, 0u32, 0u32, 0u32);
        let mut t_pnl = 0.0f64;
        let mut seen_markets = std::collections::HashSet::new();

        for slot in &slots {
            let name = &slot.worker.asset;
            let halted = slot.worker.is_halted();
            let light = if halted { "🟡 halted" } else { "🟢 active" };
            let (sl, delta_gate, halt_n) = if slot.worker.strategy_name == "high_prob" {
                (slot.params.sl_high_prob, slot.params.delta_pct_hp, slot.params.halt_prob)
            } else {
                (slot.params.sl_reversal, slot.params.delta_pct_rev, slot.params.halt_rev)
            };
            ta_lines.push(format!(
                "  {light}  {name}  strategy={}\n    sl={sl:.4}  delta_gate={delta_gate:.5}  halt_after={halt_n}L  unwind_pnl={:.4}  sl_pnl={:.4}  size=${:.2}",
                slot.worker.strategy_name, slot.params.unwind_pnl, slot.params.sl_pnl, slot.params.trade_size_usdc
            ));

            if seen_markets.insert(name.clone()) {
                match &slot.current_slug {
                    Some(slug) => mkt_lines.push(format!(
                        "  {name}  binance=${:.5}  UP={:.4}  DN={:.4}  Δ={:.5}  slug={slug}",
                        slot.last_binance, slot.last_poly_up, slot.last_poly_dn, slot.worker.delta_pct()
                    )),
                    None => mkt_lines.push(format!("  {name}  no active cycle yet")),
                }
            }

            let sign = if slot.total_pnl >= 0.0 { "+" } else { "" };
            pnl_lines.push(format!(
                "  {name}: {}W/{}L/{}SL/{}UW  {sign}${:.4}",
                slot.wins, slot.losses, slot.stoplosses, slot.unwinds, slot.total_pnl
            ));
            pnl_lines.push(format!(
                "    {:<10} {sign}${:.4}",
                slot.worker.strategy_name, slot.total_pnl
            ));
            if let Some(last) = &slot.last_trade {
                pnl_lines.push(format!("    last: {last}"));
            }

            tw += slot.wins;
            tl += slot.losses;
            ts += slot.stoplosses;
            tu += slot.unwinds;
            t_pnl += slot.total_pnl;
        }

        sections.push(format!("<b>TRADE ASSETS</b>\n{}", ta_lines.join("\n")));
        sections.push(format!("<b>MARKETS</b>  ({now})\n{}", mkt_lines.join("\n")));

        let sign = if t_pnl >= 0.0 { "+" } else { "" };
        let mut all_pnl = vec![format!("  Session: {tw}W/{tl}L/{ts}SL/{tu}UW  {sign}${t_pnl:.4}")];
        all_pnl.extend(pnl_lines);
        sections.push(format!("<b>PNL</b>\n{}", all_pnl.join("\n")));

        sections.join("\n\n")
    }

    /// Execute one `Action` against the live engine; returns the follow-up
    /// `Event` (if any) to feed back into `worker.step`.
    async fn execute(&self, slot: &mut AssetSlot, action: &Action) -> Option<Event> {
        match action {
            Action::PlaceBuy { side, price, size_usdc } => {
                let token_id = if *side == Side::Up { slot.up_id } else { slot.dn_id };
                slot.current_token_id = Some(token_id);
                let result = self.engine.place(token_id, *price, *size_usdc, slot.max_buy_price).await;
                println!("[ORDER] {} BUY {side:?} @ {price:.4} size=${size_usdc:.2} -> placed={} shares={:.4} cost={:.4} err={:?}",
                    slot.worker.asset, result.placed, result.filled_shares, result.cost, result.error);

                let dt = hkt_now().format("%H:%M:%S");
                let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                let delta_pct = slot.worker.delta_pct() * 100.0;
                if result.placed && result.filled_shares > 0.0 {
                    self.notify(&format!(
                        "📋 <b>{}</b> Order placed | {dt} | T-{time_left}s | {} | {}\nprice={:.4} | delta={delta_pct:+.3}%",
                        slot.worker.asset, arrow_side(*side), slot.worker.strategy_name, result.cost
                    )).await;
                    Some(Event::OrderFilled { filled_shares: result.filled_shares, cost: result.cost })
                } else {
                    self.notify(&format!(
                        "❗ <b>{}</b> Order REJECTED | {dt} | T-{time_left}s | {} | {}\nsignal price={price:.4} | delta={delta_pct:+.3}% | error={}",
                        slot.worker.asset, arrow_side(*side), slot.worker.strategy_name,
                        result.error.as_deref().unwrap_or("unknown")
                    )).await;
                    Some(Event::OrderRejected)
                }
            }
            Action::PlaceLimitSell { shares, price } => {
                let Some(token_id) = slot.current_token_id else { return None };
                let r = self.engine.place_limit_sell(token_id, *shares, *price).await;
                println!("[ORDER] {} LIMIT SELL {shares:.4} @ {price:.4} -> status={:?} order_id={:?} err={:?}",
                    slot.worker.asset, r.status, r.order_id, r.error);
                Some(Event::LimitSellPlaced { order_id: r.order_id, status: r.status, error: r.error })
            }
            Action::ClosePosition { shares, reason } => {
                let Some(token_id) = slot.current_token_id else { return None };
                if matches!(reason, CloseReason::StopLoss) {
                    println!("[SL] {} stop-loss triggered — closing {shares:.4} shares (sl_pnl floor crossed; up to 5 retries)", slot.worker.asset);
                    // Fire immediately on trigger, independent of whether the close
                    // itself ends up succeeding — side derived from which token is
                    // currently held rather than a new Worker accessor.
                    let side = if token_id == slot.up_id { Side::Up } else { Side::Down };
                    let trigger_price = if side == Side::Up { slot.last_poly_up } else { slot.last_poly_dn };
                    let dt = hkt_now().format("%H:%M:%S");
                    let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                    let delta_pct = slot.worker.delta_pct() * 100.0;
                    self.notify(&format!(
                        "🛑 <b>{}</b> STOP LOSS triggered | {dt} | T-{time_left}s | {} | {}\nprice={trigger_price:.4} | delta={delta_pct:+.3}%",
                        slot.worker.asset, arrow_side(side), slot.worker.strategy_name
                    )).await;
                }
                let result = self.engine.close_position(token_id, *shares).await;
                println!("[ORDER] {} CLOSE {shares:.4} ({reason:?}) -> status={:?} sold={:.4} usdc={:.4} err={:?}",
                    slot.worker.asset, result.status, result.shares_sold, result.filled_usdc, result.error);
                let sold = result.shares_sold;
                let exit_price = if sold > 0.0 { result.filled_usdc / sold } else { 0.0 };
                let matched = matches!(result.status, SellStatus::Matched);
                let event = match (matched, reason) {
                    (true, CloseReason::TakeProfit) => Event::UnwindFilled { sold_shares: sold, exit_price },
                    (true, CloseReason::StopLoss) => Event::StopSellFilled { sold_shares: sold, exit_price },
                    (false, CloseReason::TakeProfit) => Event::UnwindFailed { error: result.error },
                    (false, CloseReason::StopLoss) => Event::StopSellFailed { error: result.error },
                };
                if matched && sold >= *shares {
                    slot.current_token_id = None;
                }
                Some(event)
            }
            Action::CancelLimitSell { order_id } => {
                let ok = self.engine.cancel_limit_sell(order_id).await;
                println!("[ORDER] {} CANCEL {order_id} -> {ok}", slot.worker.asset);
                None
            }
            Action::Persist | Action::LogTrade(_) | Action::LogTradeCorrection { .. } | Action::StopLossVerdict { .. } => None, // handled by process_actions directly
        }
    }

    /// Runs a batch of actions to completion for one asset, recursively
    /// feeding follow-up events back through that asset's worker. `Box::pin`
    /// because this is a self-referential async recursion.
    fn process_actions<'b>(
        &'b self,
        slot: &'b mut AssetSlot,
        actions: Vec<Action>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'b>> {
        Box::pin(async move {
            for action in &actions {
                match action {
                    Action::Persist => persist(&slot.worker, &slot.state_file),
                    Action::LogTrade(rec) => {
                        println!("[TRADE] {rec:?}");
                        if let Err(e) = log_trade(&slot.log_path, rec) {
                            eprintln!("log error: {e:#}");
                        }
                        if matches!(rec.outcome, Outcome::Win | Outcome::Loss | Outcome::StopLoss | Outcome::Unwind) {
                            slot.trades_completed += 1;
                        }
                        match rec.outcome {
                            Outcome::Win => slot.wins += 1,
                            Outcome::Loss => slot.losses += 1,
                            Outcome::StopLoss => slot.stoplosses += 1,
                            Outcome::Unwind => slot.unwinds += 1,
                        }
                        slot.total_pnl += rec.pnl;
                        let summary = format!(
                            "{} {} {} pnl={:.4}",
                            hkt_now().format("%H:%M:%S"),
                            rec.side.as_str(),
                            rec.outcome.as_str(),
                            rec.pnl
                        );
                        slot.last_trade = Some(summary.clone());

                        let icon = match rec.outcome {
                            Outcome::Win | Outcome::Unwind => "✅",
                            Outcome::Loss | Outcome::StopLoss => "❌",
                        };
                        let sign = if rec.pnl >= 0.0 { "+" } else { "-" };
                        self.notify(&format!(
                            "{icon} <b>{} TRADE {}</b> | {} | {} | {}\nentry={:.4} → exit={:.4} | cycle: ${:.2}→${:.2} | pnl={sign}${:.4} | {}W/{}L",
                            slot.worker.asset, rec.outcome.as_str(), hkt_now().format("%H:%M:%S"),
                            arrow_side(rec.side), slot.worker.strategy_name,
                            rec.token_price, rec.exit_price,
                            slot.worker.cycle_open_binance(), slot.last_binance,
                            rec.pnl.abs(), slot.wins, slot.losses
                        )).await;

                        spawn_resolution_watcher(
                            self.http.clone(), rec.slug.clone(), rec.side,
                            slot.worker.asset.clone(), slot.worker.strategy_name,
                            self.api_result_tx.clone(),
                        );
                    }
                    Action::LogTradeCorrection { previous_outcome, previous_pnl, record } => {
                        println!("[TRADE] API-corrected: {previous_outcome:?} -> {record:?}");
                        if let Err(e) = log_trade(&slot.log_path, record) {
                            eprintln!("log error: {e:#}");
                        }
                        match previous_outcome {
                            Outcome::Win => slot.wins = slot.wins.saturating_sub(1),
                            Outcome::Loss => slot.losses = slot.losses.saturating_sub(1),
                            Outcome::StopLoss | Outcome::Unwind => {} // Confirming only ever holds Win/Loss
                        }
                        match record.outcome {
                            Outcome::Win => slot.wins += 1,
                            Outcome::Loss => slot.losses += 1,
                            Outcome::StopLoss | Outcome::Unwind => {}
                        }
                        slot.total_pnl += record.pnl - previous_pnl;
                        self.notify(&format!(
                            "⚠️ <b>{} RESULT CORRECTED</b> | {} | {}\nestimated={} → API={} | pnl {}{:.4} → {}{:.4}",
                            slot.worker.asset, hkt_now().format("%H:%M:%S"), slot.worker.strategy_name,
                            previous_outcome.as_str(), record.outcome.as_str(),
                            if *previous_pnl >= 0.0 { "+" } else { "" }, previous_pnl,
                            if record.pnl >= 0.0 { "+" } else { "" }, record.pnl
                        )).await;
                    }
                    Action::StopLossVerdict { record: _, would_have_won } => {
                        let (icon, verdict, note) = if *would_have_won {
                            ("🔴", "COSTLY", "market would have favored the position — stop cost money")
                        } else {
                            ("🟢", "GOOD", "market moved against the position — stop saved money")
                        };
                        self.notify(&format!(
                            "{icon} <b>{} STOP {verdict}</b> | {} | {}\n{note}",
                            slot.worker.asset, hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    _ => {
                        if let Some(followup) = self.execute(slot, action).await {
                            let more = slot.worker.step(followup);
                            self.process_actions(slot, more).await;
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
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();

    dotenvy::from_path(&args.env_file).with_context(|| format!("load {}", args.env_file))?;
    let key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
    let signer = local_signer_from_key(&key)?;
    let funder_raw = std::env::var("FUND_ADDRESS").unwrap_or_else(|_| DEFAULT_FUND_ADDRESS.to_string());
    let funder = Address::from_str(&funder_raw)?;
    let signature_type = signature_type_from_env()?;

    std::fs::create_dir_all(&args.log_dir).with_context(|| format!("create {}", args.log_dir))?;

    let toml = trader::config::load_latest(&args.config_dir)?;

    println!(
        "[live] assets={} size_usdc=${:.2} max_trades={} log_dir={}",
        args.asset.join(","), args.size_usdc, args.max_trades, args.log_dir
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

    // Telegram control plane (optional — runs without it if unconfigured, same
    // as the discovery-mode fallback in telegram/mod.rs::AuthConfig). Exactly
    // one poller for the whole process — see module doc comment for why.
    let telegram_auth = match (std::env::var("TELEGRAM_BOT_TOKEN"), std::env::var("TELEGRAM_CHAT_ID")) {
        (Ok(token), Ok(raw_chat_id)) => {
            let chat_id: i64 = raw_chat_id.parse().context("TELEGRAM_CHAT_ID must be an integer")?;
            println!("[live] Telegram control enabled (chat_id={chat_id}).");
            Some(AuthConfig { token, chat_id, user_id: 0 })
        }
        _ => {
            println!("[live] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — Telegram control disabled.");
            None
        }
    };
    let telegram_send: Option<Arc<TelegramBot>> = match &telegram_auth {
        Some(auth) => Some(Arc::new(TelegramBot::new(auth.clone())?)),
        None => None,
    };
    let (telegram_tx, mut telegram_rx) = mpsc::unbounded_channel::<String>();
    if let Some(auth) = &telegram_auth {
        let mut poll_bot = TelegramBot::new(auth.clone())?;
        let tx = telegram_tx.clone();
        tokio::spawn(async move {
            loop {
                match poll_bot.poll_once().await {
                    Ok(messages) => {
                        for m in messages {
                            if tx.send(m.text).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[telegram] poll error: {e:#}");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    let http = http_client()?;
    // Clob client only needed for direct Poly WS subscriptions (not the NATS path).
    let clob = if args.nats_url.is_none() { Some(clob_client()) } else { None };

    // binance/poly channels are shared across assets, each tick tagged with its asset
    // name — tokio::select! can't cleanly select over a dynamic set.
    let (binance_tx, mut binance_rx) = mpsc::unbounded_channel::<(String, BinanceTick)>();
    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<(String, PolyTick)>();
    // (asset, strategy, won) handoff from background Gamma-resolution watchers
    // (spawned per closed trade) back into the single-threaded step() loop.
    let (api_result_tx, mut api_result_rx) = mpsc::unbounded_channel::<(String, &'static str, bool)>();

    // One `AssetSlot` per (asset, strategy) pair — strategy list always comes from
    // the shared TOML's `AssetParams.strategies` (e.g. ETH -> [high_prob, reversal]),
    // never a CLI flag, so this can't silently drift from the config the Python bot
    // reads. A dual-strategy asset gets two independent slots, each with its own
    // position/win-loss state, matching Python's per-asset Worker holding a list of
    // strategy objects that each fire/track independently.
    let mut assets: Vec<AssetSlot> = Vec::new();
    for asset in &args.asset {
        let mut params = toml.resolve(asset)?;
        params.trade_size_usdc = args.size_usdc;
        let max_buy_price = params.max_buy_price;
        if params.strategies.is_empty() {
            anyhow::bail!("no strategies configured for asset {asset} (missing both a `{asset}` and `default` entry in the config's [strategies] table)");
        }

        for strategy in &params.strategies {
            let worker = match strategy.as_str() {
                "reversal" => Worker::new_reversal(asset, &params),
                "high_prob" => Worker::new_high_prob(asset, &params),
                other => anyhow::bail!("unknown strategy `{other}` for asset {asset} (from config)"),
            };
            let lower = asset.to_lowercase();
            let log_path = format!("{}/live_trades_{lower}_{strategy}.csv", args.log_dir);
            let state_file = format!("{}/live_state_{lower}_{strategy}.json", args.log_dir);
            append_csv_header_if_new(&log_path)?;

            if args.nats_url.is_none() {
                let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<BinanceTick>();
                spawn_binance_task(asset, raw_tx);
                let out = binance_tx.clone();
                let a = asset.clone();
                tokio::spawn(async move {
                    while let Some(tick) = raw_rx.recv().await {
                        if out.send((a.clone(), tick)).is_err() {
                            return;
                        }
                    }
                });
            }

            assets.push(AssetSlot {
                worker,
                params: params.clone(),
                up_id: U256::from(0u64),
                dn_id: U256::from(0u64),
                current_token_id: None,
                max_buy_price,
                log_path,
                state_file,
                trades_completed: 0,
                wins: 0,
                losses: 0,
                stoplosses: 0,
                unwinds: 0,
                total_pnl: 0.0,
                last_trade: None,
                current_slug: None,
                last_binance: 0.0,
                last_poly_up: 0.0,
                last_poly_dn: 0.0,
                poly_sub: None,
            });
        }
    }

    for slot in &assets {
        println!("[live]   {} -> strategy={}", slot.worker.asset, slot.worker.strategy_name);
    }

    // NATS path: subscribe to price.binance.<ASSET> and price.poly.<ASSET> for each
    // asset, forwarding into the same channels the direct-WS path uses.
    // Set up before engine.connect() so ticks flow and can be verified independently.
    if let Some(ref url) = args.nats_url {
        let nc = async_nats::connect(url).await
            .with_context(|| format!("connect to NATS at {url}"))?;
        println!("[live] NATS price source: {url}");
        for asset in &args.asset {
            let mut sub = nc.subscribe(format!("price.binance.{asset}")).await
                .context("NATS binance subscribe")?;
            let out = binance_tx.clone();
            let a = asset.clone();
            tokio::spawn(async move {
                let mut n: u64 = 0;
                while let Some(msg) = sub.next().await {
                    if let Ok(tick) = serde_json::from_slice::<BinanceTick>(&msg.payload) {
                        n += 1;
                        if n == 1 {
                            println!("[NATS] first binance tick for {a}: price={:.4}", tick.price);
                        }
                        if out.send((a.clone(), tick)).is_err() {
                            return;
                        }
                    }
                }
            });

            let mut sub = nc.subscribe(format!("price.poly.{asset}")).await
                .context("NATS poly subscribe")?;
            let out = poly_tx.clone();
            let a = asset.clone();
            tokio::spawn(async move {
                let mut n: u64 = 0;
                while let Some(msg) = sub.next().await {
                    if let Ok(tick) = serde_json::from_slice::<PolyTick>(&msg.payload) {
                        n += 1;
                        if n == 1 {
                            println!("[NATS] first poly tick for {a}: up={:.4} dn={:.4}", tick.up, tick.dn);
                        }
                        if out.send((a.clone(), tick)).is_err() {
                            return;
                        }
                    }
                }
            });
        }
        println!("[live] NATS subscriptions active — connecting to execution engine…");
    }

    let live_config = LiveConfig {
        order_slippage: toml.order_slippage,
        order_max_retries: toml.order_max_retries,
        ..LiveConfig::default()
    };
    let engine =
        LiveExecutionEngine::connect(CLOB_HOST, signer, funder, signature_type, live_config).await?;
    let driver = Driver { engine: &engine, telegram: telegram_send.clone(), http: http.clone(), api_result_tx: api_result_tx.clone() };
    let asset_strategy_summary = assets.iter()
        .map(|s| format!("{}:{}", s.worker.asset, s.worker.strategy_name))
        .collect::<Vec<_>>()
        .join(", ");
    driver
        .notify(&format!(
            "🟢 live driver started: <b>{asset_strategy_summary}</b> (size=${:.2}, max_trades={})",
            args.size_usdc, args.max_trades
        ))
        .await;

    let balance_guard = BalanceGuard::new();
    let mut balance_deadline =
        tokio::time::Instant::now() + tokio::time::Duration::from_secs_f64(seconds_until_next_check(now_secs_f64()));

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut current_slot_val: u64 = 0;

    loop {
        if assets.iter().all(|s| s.trades_completed >= args.max_trades) {
            println!("[live] all assets reached max_trades ({}) — shutting down cleanly.", args.max_trades);
            driver.notify(&format!("🏁 all assets ({}) reached max_trades ({}) — shut down.", args.asset.join(", "), args.max_trades)).await;
            return Ok(());
        }

        tokio::select! {
            Some((asset, tick)) = binance_rx.recv() => {
                for slot in assets.iter_mut().filter(|s| s.worker.asset == asset) {
                    slot.last_binance = tick.price;
                    if slot.current_slug.is_some() {
                        let actions = slot.worker.step(Event::BinanceTick(tick));
                        driver.process_actions(slot, actions).await;
                    }
                }
            }

            Some((asset, tick)) = poly_rx.recv() => {
                for slot in assets.iter_mut().filter(|s| s.worker.asset == asset) {
                    slot.last_poly_up = tick.up;
                    slot.last_poly_dn = tick.dn;
                    if slot.current_slug.is_some() {
                        let actions = slot.worker.step(Event::PolyTick(tick));
                        driver.process_actions(slot, actions).await;
                    }
                }
            }

            _ = heartbeat.tick() => {
                let time_left = current_slot_val as f64 + args.period_secs as f64 - now_secs_f64();
                for slot in &assets {
                    if let Some(slug) = &slot.current_slug {
                        println!("[live] heartbeat {} ({}) slug={slug} T-{time_left:.0}s binance={:.4} up={:.4} dn={:.4}",
                            slot.worker.asset, slot.worker.strategy_name, slot.last_binance, slot.last_poly_up, slot.last_poly_dn);
                    }
                }
            }

            _ = ticker.tick() => {
                let slot_now = current_slot(args.period_secs);
                if slot_now != current_slot_val {
                    current_slot_val = slot_now;
                    for slot in assets.iter_mut() {
                        let asset = slot.worker.asset.clone();
                        if slot.current_slug.is_some() {
                            let actions = slot.worker.step(Event::CycleClose);
                            driver.process_actions(slot, actions).await;
                        }
                        if slot.last_binance <= 0.0 || slot.trades_completed >= args.max_trades {
                            slot.current_slug = None;
                            continue;
                        }

                        let slug = make_slug(&asset, slot_now, "5m");
                        match fetch_meta(&http, &slug).await {
                            Ok((u, d)) => {
                                slot.up_id = u;
                                slot.dn_id = d;

                                // Direct Poly WS subscription only when not using NATS;
                                // in the NATS path the subscription is already running
                                // from startup (price_feed publishes price.poly.<ASSET>).
                                if let Some(ref clob) = clob {
                                    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<PolyTick>();
                                    slot.poly_sub = Some(PolySub::start(clob, u, raw_tx));
                                    let out = poly_tx.clone();
                                    let a = asset.clone();
                                    tokio::spawn(async move {
                                        while let Some(tick) = raw_rx.recv().await {
                                            if out.send((a.clone(), tick)).is_err() {
                                                return;
                                            }
                                        }
                                    });
                                }

                                let ctx = CycleContext {
                                    start_ts: slot_now as f64, end_ts: (slot_now + args.period_secs) as f64, open_binance: slot.last_binance,
                                };
                                println!("[live] new cycle {asset} ({}) slug={slug} open_binance={}", slot.worker.strategy_name, slot.last_binance);
                                let actions = slot.worker.step(Event::CycleOpen { ctx, slug: slug.clone(), entry_suppressed: false });
                                driver.process_actions(slot, actions).await;
                                slot.current_slug = Some(slug);
                            }
                            Err(e) => eprintln!("[live] meta fetch failed for {asset} {slug}: {e:#}"),
                        }
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("[live] shutting down (SIGINT/SIGTERM).");
                for slot in assets.iter_mut() {
                    slot.worker.step(Event::Control(ControlEvent::Halt));
                }
                driver.notify(&format!("🔴 live driver shutting down: {}", args.asset.join(", "))).await;
                return Ok(());
            }
            _ = async {
                #[cfg(unix)]
                {
                    let mut s = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
                    s.recv().await;
                }
                #[cfg(not(unix))]
                std::future::pending::<()>().await;
            } => {
                println!("[live] shutting down (SIGTERM).");
                for slot in assets.iter_mut() {
                    slot.worker.step(Event::Control(ControlEvent::Halt));
                }
                driver.notify(&format!("🔴 live driver shutting down (SIGTERM): {}", args.asset.join(", "))).await;
                return Ok(());
            }

            Some(text) = telegram_rx.recv() => {
                let Some(cmd) = parse_command(&text) else { continue };
                let reply = match cmd {
                    Command::Status => Some(driver.render_status(&assets).await),
                    Command::Help => Some(HELP_TEXT.to_string()),
                    Command::Halt { asset } if asset.is_empty() => {
                        for slot in assets.iter_mut() {
                            slot.worker.step(Event::Control(ControlEvent::Halt));
                        }
                        Some(format!("🛑 Halted all assets ({}) — new entries suppressed, open positions still managed.", args.asset.join(", ")))
                    }
                    Command::Resume { asset } if asset.is_empty() => {
                        for slot in assets.iter_mut() {
                            slot.worker.step(Event::Control(ControlEvent::Resume));
                        }
                        balance_guard.reset_baseline();
                        Some(format!("▶️ Resumed all assets ({}).", args.asset.join(", ")))
                    }
                    // A named asset may own more than one strategy slot (e.g. ETH:
                    // high_prob + reversal) — halt/resume both together, matching
                    // Python's manual per-asset halt lighting both indicators.
                    Command::Halt { asset } => {
                        let matched: Vec<&mut AssetSlot> = assets.iter_mut()
                            .filter(|s| s.worker.asset.eq_ignore_ascii_case(&asset))
                            .collect();
                        if matched.is_empty() {
                            Some(format!("this driver doesn't manage {asset} — trading {}", args.asset.join(", ")))
                        } else {
                            for slot in matched {
                                slot.worker.step(Event::Control(ControlEvent::Halt));
                            }
                            Some(format!("🛑 Halted {asset} — new entries suppressed, open positions still managed."))
                        }
                    }
                    Command::Resume { asset } => {
                        let matched: Vec<&mut AssetSlot> = assets.iter_mut()
                            .filter(|s| s.worker.asset.eq_ignore_ascii_case(&asset))
                            .collect();
                        if matched.is_empty() {
                            Some(format!("this driver doesn't manage {asset} — trading {}", args.asset.join(", ")))
                        } else {
                            for slot in matched {
                                slot.worker.step(Event::Control(ControlEvent::Resume));
                            }
                            balance_guard.reset_baseline();
                            Some(format!("▶️ Resumed {asset}."))
                        }
                    }
                    Command::Invalid(msg) => Some(msg),
                    _ => Some("not supported by this Rust live driver yet.".to_string()),
                };
                if let Some(text) = reply {
                    driver.notify(&text).await;
                }
            }

            Some((asset, strategy, won)) = api_result_rx.recv() => {
                if let Some(slot) = assets.iter_mut().find(|s| s.worker.asset == asset && s.worker.strategy_name == strategy) {
                    let actions = slot.worker.step(Event::ApiResult { won });
                    driver.process_actions(slot, actions).await;
                }
            }

            _ = tokio::time::sleep_until(balance_deadline) => {
                let bal = engine.fetch_balance().await;
                if balance_guard.check(bal) {
                    println!("[live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.");
                    for slot in assets.iter_mut() {
                        slot.worker.step(Event::Balance(BalanceEvent::DrawdownHalt));
                    }
                    driver.notify(&format!(
                        "🛑 balance drawdown >25% from session baseline — halted new entries on all assets ({}). Send /resume to re-arm.",
                        args.asset.join(", ")
                    )).await;
                }
                balance_deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_secs_f64(seconds_until_next_check(now_secs_f64()));
            }
        }
    }
}
