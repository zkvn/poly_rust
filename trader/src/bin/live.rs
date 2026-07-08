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
// Each (asset, strategy) slot allows at most `--max-trades` trades (default 1)
// *per open cycle* — the counter resets to zero every time a new cycle opens
// for that slot, so this never stops a slot from trading again next cycle. A
// process restart isn't required to keep any strategy trading (previously it
// was: this counter used to be a lifetime total that only ever grew, so a
// slot that hit its cap early — e.g. ETH `high_prob` — went permanently dark
// until the next restart, missing whatever cycles happened in between; see
// trader/doc/incident_missed_eth_2026-07-03.md). Actual risk control — when to
// stop a strategy from entering at all — is the config-driven consecutive-loss
// halt (`halt_rev`/`halt_prob`, auto-resetting at `halt_reset_hour_rev`/
// `halt_reset_hour_hp` HKT daily, see `worker.rs`'s `HaltTracker` usage) plus
// manual `/halt` and the balance drawdown guard — not a trade-count cap.
// Uses the PriceMonitor exit arm in practice for small sizes (a $1 buy yields
// far fewer than 5 shares at any plausible entry price, so the GTC-resting
// path is defensive/unexercised here, not because it's unimplemented).

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
// Matches the vendored SDK's own `Client<Unauthenticated>::default()` endpoint.
const UNWIND_WS_HOST: &str = "wss://ws-subscriptions-clob.polymarket.com";

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

    /// Max trades per (asset, strategy) slot *per open cycle* — resets to 0
    /// every time a new cycle opens, so this never stops a strategy from
    /// trading again next cycle. Not a lifetime/session cap; see the file
    /// header comment.
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

const CSV_HEADER: &str =
    "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error,\
     entry_signal_latency_ms,entry_process_latency_ms,exit_signal_latency_ms,exit_process_latency_ms";

/// Writes the CSV header for a new file, or heals a stale header from an
/// earlier schema generation (9 columns, pre-`exit_attempts`/`exit_last_error`;
/// 11 columns, pre-latency) on an existing file into the current schema —
/// padding any legacy data row with however many empty trailing fields its
/// generation is short, so every row's column count matches the header.
///
/// Without this, `csv.DictReader`-based tooling (`trade_reconcile.py`) doesn't
/// error on the mismatch — it silently drops the extra fields into an unnamed
/// "restkey" bucket, so `row.get("exit_attempts")` always came back `None` and
/// the "Failed Exit Attempts" report section always showed zero, even for rows
/// that do have retry evidence. See `trader/doc/incident_doge_2026-07-03.md`.
fn append_csv_header_if_new(path: &str) -> Result<()> {
    use std::io::{BufRead as _, Write as _};

    if !std::path::Path::new(path).exists() {
        let mut f = std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        writeln!(f, "{CSV_HEADER}")?;
        return Ok(());
    }

    let file = std::fs::File::open(path)?;
    let mut lines = std::io::BufReader::new(file).lines();
    let Some(first) = lines.next().transpose()? else { return Ok(()) }; // empty file
    if first == CSV_HEADER || !first.starts_with("logged_at,") {
        return Ok(()); // already current, or not a header we recognize — leave untouched
    }

    let target_commas = CSV_HEADER.matches(',').count();
    let mut healed = String::new();
    healed.push_str(CSV_HEADER);
    healed.push('\n');
    for line in lines {
        let line = line?;
        // Pad any row short of the current field count, regardless of which
        // older generation it came from — a row already at (or past) the
        // target width is left untouched.
        let short_by = target_commas.saturating_sub(line.matches(',').count());
        healed.push_str(&line);
        healed.push_str(&",".repeat(short_by));
        healed.push('\n');
    }
    std::fs::write(path, healed)?;
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
    writeln!(f, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(), rec.slug, rec.strategy, rec.side.as_str(),
        rec.entry_ts, rec.token_price, rec.exit_price, rec.outcome.as_str(), rec.pnl,
        rec.exit_attempts, exit_last_error,
        rec.entry_signal_latency_ms, rec.entry_process_latency_ms,
        rec.exit_signal_latency_ms, rec.exit_process_latency_ms)?;
    Ok(())
}

/// Extracts the optional `server_ts` field price_feed publishes alongside
/// each NATS tick (the exchange's own event timestamp, seconds since epoch —
/// see price_feed/src/collect.rs's `poly_nats_payload`/`binance_nats_payload`).
/// Parsed separately from the typed `BinanceTick`/`PolyTick`, which don't (and
/// shouldn't) carry this field themselves — it's an observability-only value,
/// irrelevant to strategy logic, and adding it to those shared tick types
/// would ripple into every strategy/backtest/worker test that constructs one.
fn extract_server_ts(payload: &[u8]) -> Option<f64> {
    #[derive(serde::Deserialize)]
    struct ServerTs {
        #[serde(default)]
        server_ts: Option<f64>,
    }
    serde_json::from_slice::<ServerTs>(payload).ok().and_then(|s| s.server_ts)
}

/// `/status`'s win/loss/pnl counters — `Worker` has no notion of these, they're
/// purely this binary's `AssetSlot` bookkeeping. Persisted alongside the
/// worker's own `PersistedState` so a restart with no new trade in between
/// shows an identical `/status` to before it (see
/// trader/doc/incident_no_reset_notification_2026-07-08.md). Every field
/// defaults on a missing/legacy file — same as never having persisted them.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedStats {
    #[serde(default)]
    wins: u32,
    #[serde(default)]
    losses: u32,
    #[serde(default)]
    stoplosses: u32,
    #[serde(default)]
    unwinds: u32,
    #[serde(default)]
    timeouts: u32,
    #[serde(default)]
    total_pnl: f64,
    #[serde(default)]
    last_trade: Option<String>,
}

/// On-disk shape of `live_state_<asset>_<strategy>.json`: the worker's own
/// position/halt invariants, flattened, plus this binary's display counters.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedSlot {
    #[serde(flatten)]
    worker: trader::worker::PersistedState,
    #[serde(default)]
    stats: PersistedStats,
}

fn persist(slot: &AssetSlot) {
    let snap = PersistedSlot {
        worker: slot.worker.to_persisted(),
        stats: PersistedStats {
            wins: slot.wins,
            losses: slot.losses,
            stoplosses: slot.stoplosses,
            unwinds: slot.unwinds,
            timeouts: slot.timeouts,
            total_pnl: slot.total_pnl,
            last_trade: slot.last_trade.clone(),
        },
    };
    if let Ok(json) = serde_json::to_string_pretty(&snap) {
        let _ = std::fs::write(&slot.state_file, json);
    }
}

/// Best-effort load of a previously-persisted slot — a missing file (first
/// run), unparsable JSON, or any other error just means "nothing to restore,"
/// never a startup failure. Loading is intentionally silent on failure since
/// a corrupt/legacy state file is expected to eventually happen (manual edits,
/// a field-shape change) and should never block the live process from coming
/// up un-halted with zero stats — the same as today's from-scratch behavior.
fn load_persisted_slot(state_file: &str) -> Option<PersistedSlot> {
    let contents = std::fs::read_to_string(state_file).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Which upstream feed produced the tick that triggered the action currently
/// being executed. Entries can fire off either feed (`Worker::try_enter` is
/// called from both `on_binance` and `on_poly` — see worker.rs), so
/// `execute()` needs to know which one to label its exchange-latency reading
/// with; exits (`ClosePosition`) are always Poly/CLOB-triggered (only
/// `on_poly` ever produces one), so that arm ignores this and always reports
/// `Clob`.
#[derive(Clone, Copy)]
enum Feed {
    Clob,
    Binance,
}

/// Format an exchange-latency reading for the console/Telegram order logs.
/// `None` means the exchange didn't supply a timestamp for this tick (e.g.
/// Binance's `E` field missing), or no tick has arrived on that feed yet —
/// printed as `n/a` rather than a bogus number.
fn fmt_latency(ms: Option<f64>) -> String {
    match ms {
        Some(v) => format!("{v:.0}ms"),
        None => "n/a".to_string(),
    }
}

/// Real, per-tick network latency for a feed's most recently seen tick: its
/// own local receipt time (`BinanceTick`/`PolyTick::ts`, captured the instant
/// that tick arrived) minus the exchange's own event timestamp for that same
/// tick. Deliberately *not* relative to "now" (order-placement time) — that
/// would conflate genuine one-hop network latency with how long the tick has
/// been sitting stale since (see `fmt_ago` for that, separately). `None` if
/// either timestamp isn't available.
fn exchange_latency_ms(local_ts: Option<f64>, server_ts: Option<f64>) -> Option<f64> {
    match (local_ts, server_ts) {
        (Some(l), Some(s)) => Some((l - s) * 1000.0),
        _ => None,
    }
}

/// Tag appended after a feed's latency reading on the "Order placed" message:
/// `"trigger"` for whichever feed's tick actually fired the entry, otherwise
/// how long ago (relative to *now* — `now_ts`, the order-placement wall
/// time) the *other* feed's last tick was locally received — e.g. entry
/// fires off a Binance tick, the last Poly tick was received 200ms before
/// this order was placed -> `"200ms ago"`. `None` (no tick yet on that feed
/// this run) prints `"n/a"`, matching `fmt_latency`.
/// See trader/doc/incident_missing_clob_latency_2026-07-06.md.
fn fmt_ago(last_tick_ts: Option<f64>, now_ts: f64) -> String {
    match last_tick_ts {
        Some(t) => format!("{:.0}ms ago", (now_ts - t) * 1000.0),
        None => "n/a".to_string(),
    }
}

/// Wall-clock duration (ms) between two local-clock timestamps. Used for both
/// `signal_latency_ms` (`signal_ts` -> `received_ts`, order dispatch started)
/// and `process_latency_ms` (`signal_ts` -> `confirmed_ts`, order confirmed) —
/// `process_latency_ms` is deliberately measured from the same `signal_ts`
/// origin as `signal_latency_ms`, not from `received_ts`, so it reads as the
/// full "trigger signal received locally -> order confirmed locally" duration
/// rather than only the dispatch-to-confirm leg (2026-07-08 redefinition —
/// see README.md's "Latency & observability infrastructure" section).
fn latency_ms(from_ts: f64, to_ts: f64) -> f64 {
    (to_ts - from_ts) * 1000.0
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
    /// Trades logged in the *currently open* cycle only — reset to 0 every
    /// time a new cycle opens for this slot (see the ticker branch below).
    /// Gates `--max-trades` (default 1: at most one trade per cycle), never
    /// whether to open the next cycle at all.
    cycle_trades: u32,
    /// Set once the "STOP LOSS triggered" alert has been sent for the
    /// current position, so retries against an unsellable dust remainder
    /// (below CLOB min order size) don't re-spam the same alert on every
    /// PolyTick — worker.rs intentionally keeps re-firing `ClosePosition`
    /// until the position actually clears, unlike take-profit's one-shot
    /// `TakeProfitAbandoned` latch (see `on_stop_sell_failed`'s doc comment).
    sl_notified: bool,
    /// Same guard as `sl_notified`, for the "TIME LIMIT triggered" alert —
    /// worker.rs's timeout check re-fires `ClosePosition{Timeout}` every
    /// PolyTick the same way stop-loss does.
    timeout_notified: bool,
    wins: u32,
    losses: u32,
    stoplosses: u32,
    unwinds: u32,
    timeouts: u32,
    total_pnl: f64,
    last_trade: Option<String>,
    current_slug: Option<String>,
    last_binance: f64,
    last_poly_up: f64,
    last_poly_dn: f64,
    poly_sub: Option<PolySub>,
    /// The most recently received tick's own exchange timestamp (seconds since
    /// epoch) for each feed — `None` if the exchange didn't supply one, or no
    /// tick has arrived yet on that feed, or this is the direct-WS (non-NATS)
    /// path where price_feed isn't in the loop to capture it at all. Cached
    /// here (rather than threaded through `Action`/`Event`) because it's
    /// updated synchronously immediately before the exact `worker.step()` call
    /// that may produce a `PlaceBuy`/`ClosePosition` off that same tick, so
    /// reading it back in `execute()` is exact, not approximate.
    last_binance_server_ts: Option<f64>,
    last_poly_server_ts: Option<f64>,
    /// The most recently received tick's own *local* timestamp (price_feed's
    /// receipt time, `BinanceTick`/`PolyTick::ts` — same clock domain as
    /// `Action::PlaceBuy`'s `signal_ts`) for each feed — distinct from the
    /// `_server_ts` fields above, which are the exchange's own event time.
    /// Used only to compute how stale the *non-triggering* feed's last known
    /// reading was relative to the tick that actually fired the entry (see
    /// `fmt_ago`) — the `_server_ts` latency numbers alone don't tell you
    /// that, since they're always relative to "now" (`received_ts`), not to
    /// the trigger tick's own moment.
    last_binance_ts: Option<f64>,
    last_poly_ts: Option<f64>,
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
            match bot.send(text).await {
                Ok(_) => println!("[telegram] sent: {}", text.lines().next().unwrap_or("")),
                Err(e) => eprintln!("[telegram] send error: {e:#}"),
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
        let (mut tw, mut tl, mut ts, mut tu, mut tt) = (0u32, 0u32, 0u32, 0u32, 0u32);
        let mut t_pnl = 0.0f64;
        let mut seen_markets = std::collections::HashSet::new();

        for slot in &slots {
            let name = &slot.worker.asset;
            let halted = slot.worker.is_halted();
            let light = if halted { "🟡 halted" } else { "🟢 active" };
            // `low`/`high` are the strategy's entry trigger band — reversal_low_threshold/
            // reversal for reversal (aka "unwind" — the reversal+take-profit-unwind
            // strategy), price_low/price_high for high_prob.
            let (sl, delta_gate, low, high, halt_n, unwind_pnl, sl_pnl, unwind_time) =
                if slot.worker.strategy_name == "high_prob" {
                    (
                        slot.params.sl_high_prob,
                        slot.params.delta_pct_hp,
                        slot.params.price_low,
                        slot.params.price_high,
                        slot.params.halt_prob,
                        slot.params.unwind_pnl_hp,
                        slot.params.sl_pnl_hp,
                        slot.params.unwind_time_hp,
                    )
                } else {
                    (
                        slot.params.sl_reversal,
                        slot.params.delta_pct_rev,
                        slot.params.reversal_low_threshold,
                        slot.params.reversal,
                        slot.params.halt_rev,
                        slot.params.unwind_pnl_rev,
                        slot.params.sl_pnl_rev,
                        slot.params.unwind_time_rev,
                    )
                };
            ta_lines.push(format!(
                "  {light}  {name}  strategy={}\n    sl={sl:.4}  delta_gate={delta_gate:.5}  low={low:.4}  high={high:.4}  halt_after={halt_n}L  unwind_pnl={unwind_pnl:.4}  sl_pnl={sl_pnl:.4}  unwind_time={unwind_time:.1}s  size=${:.2}",
                slot.worker.strategy_name, slot.params.trade_size_usdc
            ));

            if seen_markets.insert(name.clone()) {
                match &slot.current_slug {
                    Some(_) => mkt_lines.push(format!(
                        "  {name}  binance=${:.5}  UP={:.4}  DN={:.4}  Δ={:.5}",
                        slot.last_binance,
                        slot.last_poly_up,
                        slot.last_poly_dn,
                        slot.worker.delta_pct()
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
            tt += slot.timeouts;
            t_pnl += slot.total_pnl;
        }

        sections.push(format!("<b>TRADE ASSETS</b>\n{}", ta_lines.join("\n")));
        sections.push(format!("<b>MARKETS</b>  ({now})\n{}", mkt_lines.join("\n")));

        let sign = if t_pnl >= 0.0 { "+" } else { "" };
        let mut all_pnl = vec![format!("  Session: {tw}W/{tl}L/{ts}SL/{tu}UW/{tt}TO  {sign}${t_pnl:.4}")];
        all_pnl.extend(pnl_lines);
        sections.push(format!("<b>PNL</b>\n{}", all_pnl.join("\n")));

        sections.join("\n\n")
    }

    /// Execute one `Action` against the live engine; returns the follow-up
    /// `Event` (if any) to feed back into `worker.step`.
    async fn execute(&self, slot: &mut AssetSlot, action: &Action, feed: Feed) -> Option<Event> {
        match action {
            Action::PlaceBuy { side, price, size_usdc, signal_ts } => {
                let token_id = if *side == Side::Up { slot.up_id } else { slot.dn_id };
                slot.current_token_id = Some(token_id);
                let received_ts = now_secs_f64();
                let result = self.engine.place(token_id, *price, *size_usdc, slot.max_buy_price).await;
                let confirmed_ts = now_secs_f64();
                let signal_latency_ms = latency_ms(*signal_ts, received_ts);
                let process_latency_ms = latency_ms(*signal_ts, confirmed_ts);
                // Real, per-tick exchange network latency for each feed's last known
                // tick (see `exchange_latency_ms`) — always computed now (previously
                // only the triggering feed's was, silently dropping the other one —
                // see trader/doc/incident_missing_clob_latency_2026-07-06.md), and
                // always the genuine one-hop delay regardless of how stale that tick
                // now is. `feed` only decides which one gets the "(trigger)" tag vs.
                // an "(Nms ago)" staleness note (relative to *now*, `received_ts` —
                // not `signal_ts`, which is the triggering tick's own timestamp) for
                // whichever feed's tick *didn't* fire this entry.
                let clob_latency_ms = exchange_latency_ms(slot.last_poly_ts, slot.last_poly_server_ts);
                let binance_latency_ms = exchange_latency_ms(slot.last_binance_ts, slot.last_binance_server_ts);
                let clob_tag = match feed {
                    Feed::Clob => "trigger".to_string(),
                    Feed::Binance => fmt_ago(slot.last_poly_ts, received_ts),
                };
                let binance_tag = match feed {
                    Feed::Binance => "trigger".to_string(),
                    Feed::Clob => fmt_ago(slot.last_binance_ts, received_ts),
                };
                let clob_latency_str = format!("clob_latency={} ({clob_tag})", fmt_latency(clob_latency_ms));
                let binance_latency_str = format!("binance_latency={} ({binance_tag})", fmt_latency(binance_latency_ms));
                println!("[ORDER] {} BUY {side:?} @ {price:.4} size=${size_usdc:.2} -> placed={} shares={:.4} cost={:.4} err={:?} ({clob_latency_str} {binance_latency_str} process_ms={process_latency_ms:.0} n_attempts={})",
                    slot.worker.asset, result.placed, result.filled_shares, result.cost, result.error, result.attempts);

                let dt = hkt_now().format("%H:%M:%S");
                let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                let delta_pct = slot.worker.delta_pct() * 100.0;
                if result.placed && result.filled_shares > 0.0 {
                    self.notify(&format!(
                        "📋 <b>{}</b> Order placed | {dt} | T-{time_left}s | {} | {}\nprice={:.4} | delta={delta_pct:+.3}% | {clob_latency_str} | {binance_latency_str} | process_latency={process_latency_ms:.0}ms | n_attempts={}",
                        slot.worker.asset, arrow_side(*side), slot.worker.strategy_name, result.cost, result.attempts
                    )).await;
                    Some(Event::OrderFilled { filled_shares: result.filled_shares, cost: result.cost, signal_latency_ms, process_latency_ms })
                } else {
                    self.notify(&format!(
                        "❗ <b>{}</b> Order REJECTED | {dt} | T-{time_left}s | {} | {}\nsignal price={price:.4} | delta={delta_pct:+.3}% | n_attempts={} | error={}",
                        slot.worker.asset, arrow_side(*side), slot.worker.strategy_name,
                        result.attempts,
                        result.error.as_deref().unwrap_or("unknown")
                    )).await;
                    Some(Event::OrderRejected)
                }
            }
            Action::PlaceLimitSell { shares, price } => {
                let token_id = slot.current_token_id?;
                let received_ts = now_secs_f64();
                let r = self.engine.place_limit_sell(token_id, *shares, *price).await;
                let confirmed_ts = now_secs_f64();
                println!("[ORDER] {} LIMIT SELL {shares:.4} @ {price:.4} -> status={:?} order_id={:?} err={:?}",
                    slot.worker.asset, r.status, r.order_id, r.error);
                // No external signal_ts for this action (it's an internal
                // follow-up to the entry fill, not driven by a market tick) —
                // only the process leg is meaningful here.
                Some(Event::LimitSellPlaced {
                    order_id: r.order_id, status: r.status, error: r.error,
                    signal_latency_ms: 0.0, process_latency_ms: latency_ms(received_ts, confirmed_ts),
                })
            }
            Action::ClosePosition { shares, reason, limit_price, signal_ts } => {
                let token_id = slot.current_token_id?;
                if matches!(reason, CloseReason::StopLoss) {
                    println!("[SL] {} stop-loss triggered — closing {shares:.4} shares (sl_pnl floor crossed; up to 5 retries)", slot.worker.asset);
                    // Only alert on the first trigger for this position — worker.rs
                    // intentionally keeps re-firing ClosePosition{StopLoss} every
                    // PolyTick until the position actually clears (needed: unlike
                    // take-profit, we can't just give up on a stop-loss), so without
                    // this guard an unsellable dust remainder (below CLOB min order
                    // size) spams an identical alert on every retry.
                    if !slot.sl_notified {
                        slot.sl_notified = true;
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
                }
                if matches!(reason, CloseReason::Timeout) {
                    println!("[TIMEOUT] {} max holding time elapsed — closing {shares:.4} shares (unwind_time floor crossed; up to 5 retries)", slot.worker.asset);
                    // Same first-trigger-only guard as stop-loss — worker.rs
                    // re-fires ClosePosition{Timeout} every PolyTick until the
                    // position clears.
                    if !slot.timeout_notified {
                        slot.timeout_notified = true;
                        let side = if token_id == slot.up_id { Side::Up } else { Side::Down };
                        let trigger_price = if side == Side::Up { slot.last_poly_up } else { slot.last_poly_dn };
                        let dt = hkt_now().format("%H:%M:%S");
                        let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                        self.notify(&format!(
                            "⏱️ <b>{}</b> TIME LIMIT triggered | {dt} | T-{time_left}s | {} | {}\nprice={trigger_price:.4} | max holding time elapsed — closing at market",
                            slot.worker.asset, arrow_side(side), slot.worker.strategy_name
                        )).await;
                    }
                }
                let received_ts = now_secs_f64();
                // Take-profit closes are bounded at limit_price (== the position's
                // own tp_price — no separate config, see
                // trader/doc/incident_sol_unwind_but_loss_2026-07-06.md); a
                // stop-loss has no floor and must close regardless of price.
                let result = match limit_price {
                    Some(price) => self.engine.close_position_at_price(token_id, *shares, *price).await,
                    None => self.engine.close_position(token_id, *shares).await,
                };
                let confirmed_ts = now_secs_f64();
                let signal_latency_ms = latency_ms(*signal_ts, received_ts);
                let process_latency_ms = latency_ms(*signal_ts, confirmed_ts);
                // Exits are always Poly/CLOB-triggered (only `on_poly` ever
                // produces a ClosePosition — see worker.rs), so this is always
                // the CLOB exchange latency, unlike the entry side above — no
                // "(trigger)"/"(Nms ago)" tag needed here, only one feed applies.
                let clob_latency_ms = exchange_latency_ms(slot.last_poly_ts, slot.last_poly_server_ts);
                let clob_latency_str = format!("clob_latency={}", fmt_latency(clob_latency_ms));
                println!("[ORDER] {} CLOSE {shares:.4} ({reason:?}) -> status={:?} sold={:.4} usdc={:.4} err={:?} ({clob_latency_str} process_ms={process_latency_ms:.0} n_attempts={})",
                    slot.worker.asset, result.status, result.shares_sold, result.filled_usdc, result.error, result.attempts);
                let sold = result.shares_sold;
                let exit_price = if sold > 0.0 { result.filled_usdc / sold } else { 0.0 };
                let matched = matches!(result.status, SellStatus::Matched);
                if matched {
                    let dt = hkt_now().format("%H:%M:%S");
                    let label = match reason {
                        CloseReason::StopLoss => "STOP LOSS",
                        CloseReason::TakeProfit => "TAKE PROFIT",
                        CloseReason::Timeout => "TIME LIMIT",
                    };
                    self.notify(&format!(
                        "📤 <b>{}</b> {label} order executed | {dt} | {}\nsold={sold:.4} @ {exit_price:.4} = ${:.4} | {clob_latency_str} | process_latency={process_latency_ms:.0}ms | n_attempts={}",
                        slot.worker.asset, slot.worker.strategy_name, result.filled_usdc, result.attempts
                    )).await;
                }
                let event = match (matched, reason) {
                    (true, CloseReason::TakeProfit) => Event::UnwindFilled { sold_shares: sold, exit_price, signal_latency_ms, process_latency_ms },
                    (true, CloseReason::StopLoss) => Event::StopSellFilled { sold_shares: sold, exit_price, signal_latency_ms, process_latency_ms },
                    (true, CloseReason::Timeout) => Event::TimeoutSellFilled { sold_shares: sold, exit_price, signal_latency_ms, process_latency_ms },
                    (false, CloseReason::TakeProfit) => Event::UnwindFailed { error: result.error },
                    (false, CloseReason::StopLoss) => Event::StopSellFailed { error: result.error },
                    (false, CloseReason::Timeout) => Event::TimeoutSellFailed { error: result.error },
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
            Action::Persist | Action::LogTrade(_) | Action::LogTradeCorrection { .. } | Action::StopLossVerdict { .. }
            | Action::HaltEngaged | Action::HaltReset => None, // handled by process_actions directly
        }
    }

    /// Runs a batch of actions to completion for one asset, recursively
    /// feeding follow-up events back through that asset's worker. `Box::pin`
    /// because this is a self-referential async recursion.
    fn process_actions<'b>(
        &'b self,
        slot: &'b mut AssetSlot,
        actions: Vec<Action>,
        feed: Feed,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'b>> {
        Box::pin(async move {
            for action in &actions {
                match action {
                    Action::Persist => persist(slot),
                    Action::LogTrade(rec) => {
                        println!("[TRADE] {rec:?}");
                        if let Err(e) = log_trade(&slot.log_path, rec) {
                            eprintln!("log error: {e:#}");
                        }
                        if matches!(rec.outcome, Outcome::Win | Outcome::Loss | Outcome::StopLoss | Outcome::Unwind | Outcome::Timeout) {
                            slot.cycle_trades += 1;
                        }
                        match rec.outcome {
                            Outcome::Win => slot.wins += 1,
                            Outcome::Loss => slot.losses += 1,
                            Outcome::StopLoss => slot.stoplosses += 1,
                            Outcome::Unwind => slot.unwinds += 1,
                            Outcome::Timeout => slot.timeouts += 1,
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
                            // A timeout can land at a profit or a loss — it's not
                            // directionally fixed like stop-loss/take-profit.
                            Outcome::Timeout if rec.pnl >= 0.0 => "⏱️✅",
                            Outcome::Timeout => "⏱️❌",
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
                            Outcome::StopLoss | Outcome::Unwind | Outcome::Timeout => {} // Confirming only ever holds Win/Loss
                        }
                        match record.outcome {
                            Outcome::Win => slot.wins += 1,
                            Outcome::Loss => slot.losses += 1,
                            Outcome::StopLoss | Outcome::Unwind | Outcome::Timeout => {}
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
                    // Loss-streak halt (halt_rev/halt_prob) — distinct from manual /halt
                    // and the balance drawdown halt, which already notify at their own
                    // call sites (Command::Halt, DrawdownHalt).
                    Action::HaltEngaged => {
                        let halt_n = if slot.worker.strategy_name == "high_prob" { slot.params.halt_prob } else { slot.params.halt_rev };
                        self.notify(&format!(
                            "🟡 <b>{} HALTED</b> | {} | {}\n{halt_n} consecutive losses — new entries suppressed until the next daily reset (or /resume).",
                            slot.worker.asset, hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    Action::HaltReset => {
                        self.notify(&format!(
                            "🟢 <b>{} HALT RESET</b> | {} | {}\nDaily loss-streak reset — new entries re-armed.",
                            slot.worker.asset, hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    _ => {
                        if let Some(followup) = self.execute(slot, action, feed).await {
                            let more = slot.worker.step(followup);
                            self.process_actions(slot, more, feed).await;
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
    if let Ok(proxy_url) = std::env::var("CLOB_PROXY_URL")
        && !proxy_url.is_empty() {
        // Safety: single-threaded at this point in main() — tokio runtime not yet
        // spawning work, and no other thread reads HTTPS_PROXY concurrently.
        unsafe { std::env::set_var("HTTPS_PROXY", &proxy_url) };
        println!("[live] routing CLOB writes via proxy: {proxy_url}");
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
    // Third element: the tick's own exchange timestamp (seconds since epoch,
    // from Polymarket CLOB's/Binance's own event time), when the source
    // provided one — `None` on the direct-WS (non-NATS) path, which has no
    // price_feed hop to have captured it.
    let (binance_tx, mut binance_rx) = mpsc::unbounded_channel::<(String, BinanceTick, Option<f64>)>();
    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<(String, PolyTick, Option<f64>)>();
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
            let mut worker = match strategy.as_str() {
                "reversal" => Worker::new_reversal(asset, &params),
                "high_prob" => Worker::new_high_prob(asset, &params),
                other => anyhow::bail!("unknown strategy `{other}` for asset {asset} (from config)"),
            };
            let lower = asset.to_lowercase();
            let log_path = format!("{}/live_trades_{lower}_{strategy}.csv", args.log_dir);
            let state_file = format!("{}/live_state_{lower}_{strategy}.json", args.log_dir);
            append_csv_header_if_new(&log_path)?;

            // Restore halt state + /status counters from before the last
            // restart (position/cycle state is deliberately NOT restored here
            // — see README's "Restart behavior" section). `halt_max`/
            // `halt_reset_hour` always come from the config just loaded above,
            // never from this file, so a config change takes effect immediately.
            let stats = match load_persisted_slot(&state_file) {
                Some(persisted) => {
                    worker.restore_halt(persisted.worker.entry_suppressed, persisted.worker.halt_losses, persisted.worker.halt_last_session);
                    persisted.stats
                }
                None => PersistedStats::default(),
            };

            if args.nats_url.is_none() {
                let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<BinanceTick>();
                spawn_binance_task(asset, raw_tx);
                let out = binance_tx.clone();
                let a = asset.clone();
                tokio::spawn(async move {
                    while let Some(tick) = raw_rx.recv().await {
                        if out.send((a.clone(), tick, None)).is_err() {
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
                cycle_trades: 0,
                sl_notified: false,
                timeout_notified: false,
                wins: stats.wins,
                losses: stats.losses,
                stoplosses: stats.stoplosses,
                unwinds: stats.unwinds,
                timeouts: stats.timeouts,
                total_pnl: stats.total_pnl,
                last_trade: stats.last_trade,
                current_slug: None,
                last_binance: 0.0,
                last_poly_up: 0.0,
                last_poly_dn: 0.0,
                poly_sub: None,
                last_binance_server_ts: None,
                last_poly_server_ts: None,
                last_binance_ts: None,
                last_poly_ts: None,
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
                        let server_ts = extract_server_ts(&msg.payload);
                        if out.send((a.clone(), tick, server_ts)).is_err() {
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
                        let server_ts = extract_server_ts(&msg.payload);
                        if out.send((a.clone(), tick, server_ts)).is_err() {
                            return;
                        }
                    }
                }
            });
        }
        println!("[live] NATS subscriptions active — connecting to execution engine…");
    }

    let live_config = LiveConfig {
        order_max_retries: toml.order_max_retries,
        ..LiveConfig::default()
    };
    let engine =
        LiveExecutionEngine::connect(CLOB_HOST, signer, funder, signature_type, live_config).await?;

    // Real-time USER-channel fill logger (diagnostic sidecar — doesn't feed
    // back into trading decisions). Subscribes to all markets for this
    // account (empty `markets` list — see unwind.rs's `run()` doc comment).
    {
        let watcher = trader::unwind::UnwindWatcher::new();
        let credentials = engine.credentials();
        tokio::spawn(async move {
            if let Err(e) = watcher.run(UNWIND_WS_HOST, credentials, funder, vec![]).await {
                eprintln!("[unwind] watcher exited: {e:#}");
            }
        });
    }

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
        tokio::select! {
            Some((asset, tick, server_ts)) = binance_rx.recv() => {
                for slot in assets.iter_mut().filter(|s| s.worker.asset == asset) {
                    slot.last_binance = tick.price;
                    slot.last_binance_server_ts = server_ts;
                    slot.last_binance_ts = Some(tick.ts);
                    // `Worker`'s own state machine already can't fire a second
                    // entry within one cycle (on_binance only acts from
                    // Watching, and entering leaves Watching until the next
                    // CycleOpen) — this is a second, independent guard on top
                    // of that, so a future state-machine change can't quietly
                    // reopen the same "one trade per cycle" hole this was
                    // written to close.
                    if slot.current_slug.is_some() && slot.cycle_trades < args.max_trades {
                        let actions = slot.worker.step(Event::BinanceTick(tick));
                        driver.process_actions(slot, actions, Feed::Binance).await;
                    }
                }
            }

            Some((asset, tick, server_ts)) = poly_rx.recv() => {
                for slot in assets.iter_mut().filter(|s| s.worker.asset == asset) {
                    slot.last_poly_up = tick.up;
                    slot.last_poly_dn = tick.dn;
                    slot.last_poly_server_ts = server_ts;
                    slot.last_poly_ts = Some(tick.ts);
                    if slot.current_slug.is_some() {
                        let actions = slot.worker.step(Event::PolyTick(tick));
                        driver.process_actions(slot, actions, Feed::Clob).await;
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
                            // CycleClose never produces PlaceBuy/ClosePosition, so the
                            // feed tag is unused here — Clob is an arbitrary default.
                            driver.process_actions(slot, actions, Feed::Clob).await;
                        }
                        if slot.last_binance <= 0.0 {
                            slot.current_slug = None;
                            continue;
                        }
                        // Fresh cycle, fresh allowance — never carried over from
                        // the cycle that just closed.
                        slot.cycle_trades = 0;
                        slot.sl_notified = false;
                        slot.timeout_notified = false;

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
                                            if out.send((a.clone(), tick, None)).is_err() {
                                                return;
                                            }
                                        }
                                    });
                                }

                                let ctx = CycleContext {
                                    start_ts: slot_now as f64, end_ts: (slot_now + args.period_secs) as f64, open_binance: slot.last_binance,
                                };
                                println!("[live] new cycle {asset} ({}) slug={slug} open_binance={}", slot.worker.strategy_name, slot.last_binance);
                                let actions = slot.worker.step(Event::CycleOpen { ctx, slug: slug.clone() });
                                // CycleOpen never produces PlaceBuy/ClosePosition either — see note above.
                                driver.process_actions(slot, actions, Feed::Clob).await;
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
                    // ApiResult never produces PlaceBuy/ClosePosition either — see note above.
                    driver.process_actions(slot, actions, Feed::Clob).await;
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

#[cfg(test)]
mod csv_header_tests {
    use super::*;

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("poly_rust_test_{name}_{}.csv", std::process::id()))
    }

    #[test]
    fn writes_header_for_new_file() {
        let path = scratch_path("new");
        let path_str = path.to_str().unwrap();
        let _ = std::fs::remove_file(&path);
        append_csv_header_if_new(path_str).unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, format!("{CSV_HEADER}\n"));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn leaves_current_header_untouched() {
        let path = scratch_path("current");
        let path_str = path.to_str().unwrap();
        let row = "1.0,slug,strategy,UP,1.0,0.5,1.0,WIN,0.1,0,,0,0,0,0\n";
        std::fs::write(&path, format!("{CSV_HEADER}\n{row}")).unwrap();
        append_csv_header_if_new(path_str).unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, format!("{CSV_HEADER}\n{row}"));
        std::fs::remove_file(&path).unwrap();
    }

    /// Reproduces the stale-header files found across `live_logs/` on
    /// 2026-07-03 (trader/doc/incident_doge_2026-07-03.md §3): a header written
    /// before `exit_attempts`/`exit_last_error` existed, with a mix of legacy
    /// 9-field rows and 11-field rows (pre-latency-columns, itself now a second,
    /// more-recent legacy generation) already appended underneath it. Both
    /// generations must be padded up to the current 15-field schema.
    #[test]
    fn heals_stale_header_and_pads_legacy_rows() {
        let path = scratch_path("stale");
        let path_str = path.to_str().unwrap();
        let stale = "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl\n\
                     1.0,old-slug,high_prob,UP,1.0,0.93,1.0,WIN,0.0753\n\
                     2.0,new-slug,reversal,UP,2.0,0.66,1.0,WIN,0.5152,284,no market price\n";
        std::fs::write(&path, stale).unwrap();

        append_csv_header_if_new(path_str).unwrap();

        let healed = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = healed.lines().collect();
        assert_eq!(lines[0], CSV_HEADER);
        assert_eq!(lines[1], "1.0,old-slug,high_prob,UP,1.0,0.93,1.0,WIN,0.0753,,,,,,", "9-field legacy row padded to 15 fields");
        assert_eq!(lines[2], "2.0,new-slug,reversal,UP,2.0,0.66,1.0,WIN,0.5152,284,no market price,,,,", "11-field legacy row padded to 15 fields");
        for line in &lines {
            assert_eq!(line.matches(',').count(), 14, "every row must have 15 fields: {line}");
        }
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod persisted_slot_tests {
    use super::*;
    use trader::worker::PersistedWorkerState;

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("poly_rust_test_state_{name}_{}.json", std::process::id()))
    }

    fn sample_slot() -> PersistedSlot {
        PersistedSlot {
            worker: trader::worker::PersistedState {
                asset: "ETH".to_string(),
                strategy: "high_prob".to_string(),
                slug: "eth-updown-5m-1000".to_string(),
                cycle_start: 1_000.0,
                cycle_end: 1_300.0,
                state: PersistedWorkerState::Watching,
                entry_suppressed: true,
                halt_losses: 2,
                halt_last_session: Some(chrono::NaiveDate::from_ymd_opt(2026, 7, 8).unwrap()),
            },
            stats: PersistedStats {
                wins: 3,
                losses: 5,
                stoplosses: 1,
                unwinds: 2,
                timeouts: 1,
                total_pnl: -0.4321,
                last_trade: Some("12:00:00 UP LOSS pnl=-0.1000".to_string()),
            },
        }
    }

    /// `load_persisted_slot` round-trips exactly what `persist` wrote — the
    /// core contract this feature depends on: no new-trade restart should be
    /// able to change `/status`'s halt indicator or win/loss/pnl counters.
    #[test]
    fn round_trips_halt_state_and_stats() {
        let path = scratch_path("roundtrip");
        let path_str = path.to_str().unwrap();
        let snap = sample_slot();
        std::fs::write(&path, serde_json::to_string_pretty(&snap).unwrap()).unwrap();

        let loaded = load_persisted_slot(path_str).expect("must parse a file it just wrote");
        assert!(loaded.worker.entry_suppressed);
        assert_eq!(loaded.worker.halt_losses, 2);
        assert_eq!(loaded.worker.halt_last_session, snap.worker.halt_last_session);
        assert_eq!(loaded.stats.wins, 3);
        assert_eq!(loaded.stats.losses, 5);
        assert_eq!(loaded.stats.stoplosses, 1);
        assert_eq!(loaded.stats.unwinds, 2);
        assert_eq!(loaded.stats.timeouts, 1);
        assert_eq!(loaded.stats.total_pnl, -0.4321);
        assert_eq!(loaded.stats.last_trade, snap.stats.last_trade);

        std::fs::remove_file(&path).unwrap();
    }

    /// A state file written before this feature shipped has none of
    /// `entry_suppressed`/`halt_losses`/`halt_last_session`/`stats` — must load
    /// as "un-halted, zero stats" rather than fail to parse (see
    /// trader/doc/incident_no_reset_notification_2026-07-08.md: the whole point
    /// is a restart must never crash or regress just because the on-disk shape
    /// predates this change).
    #[test]
    fn legacy_file_without_new_fields_loads_with_defaults() {
        let path = scratch_path("legacy");
        let path_str = path.to_str().unwrap();
        let legacy = r#"{
            "asset": "ETH",
            "strategy": "high_prob",
            "slug": "eth-updown-5m-1000",
            "cycle_start": 1000.0,
            "cycle_end": 1300.0,
            "state": "Watching"
        }"#;
        std::fs::write(&path, legacy).unwrap();

        let loaded = load_persisted_slot(path_str).expect("legacy shape must still parse");
        assert!(!loaded.worker.entry_suppressed);
        assert_eq!(loaded.worker.halt_losses, 0);
        assert_eq!(loaded.worker.halt_last_session, None);
        assert_eq!(loaded.stats.wins, 0);
        assert_eq!(loaded.stats.total_pnl, 0.0);
        assert_eq!(loaded.stats.last_trade, None);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn missing_file_loads_as_none() {
        let path = scratch_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(load_persisted_slot(path.to_str().unwrap()).is_none());
    }

    #[test]
    fn corrupt_file_loads_as_none_not_a_panic() {
        let path = scratch_path("corrupt");
        std::fs::write(&path, "not valid json{{{").unwrap();
        assert!(load_persisted_slot(path.to_str().unwrap()).is_none());
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod exchange_latency_tests {
    use super::*;

    #[test]
    fn extract_server_ts_reads_the_field_when_present() {
        let payload = br#"{"ts":1751234567.123,"up":0.5,"dn":0.5,"server_ts":1751234567.010}"#;
        assert_eq!(extract_server_ts(payload), Some(1751234567.010));
    }

    #[test]
    fn extract_server_ts_is_none_on_null_or_missing() {
        let with_null = br#"{"ts":1.0,"price":1.0,"server_ts":null}"#;
        assert_eq!(extract_server_ts(with_null), None);

        // Older payloads (pre this feature, or a price_feed that hasn't been
        // redeployed yet) simply lack the field — must not error out.
        let without_field = br#"{"ts":1.0,"price":1.0}"#;
        assert_eq!(extract_server_ts(without_field), None);
    }

    #[test]
    fn fmt_latency_formats_some_and_none() {
        assert_eq!(fmt_latency(Some(12.4)), "12ms");
        assert_eq!(fmt_latency(Some(-3.0)), "-3ms");
        assert_eq!(fmt_latency(None), "n/a");
    }

    #[test]
    fn exchange_latency_ms_is_local_receipt_minus_server_ts() {
        // Tick locally received at 100.117, exchange's own event ts was 100.000
        // -> 117ms real network latency, independent of "now"/staleness.
        let got = exchange_latency_ms(Some(100.117), Some(100.000)).unwrap();
        assert!((got - 117.0).abs() < 1e-6, "got {got}");
        assert_eq!(exchange_latency_ms(None, Some(100.000)), None);
        assert_eq!(exchange_latency_ms(Some(100.117), None), None);
    }

    #[test]
    fn fmt_ago_reports_gap_to_now() {
        // Order placed ("now") at t=100.200, last tick on the other feed was
        // locally received at t=100.000 -> that reading is 200ms stale right now.
        assert_eq!(fmt_ago(Some(100.000), 100.200), "200ms ago");
    }

    #[test]
    fn fmt_ago_zero_when_both_feeds_tick_simultaneously() {
        assert_eq!(fmt_ago(Some(100.200), 100.200), "0ms ago");
    }

    #[test]
    fn fmt_ago_none_when_feed_never_ticked() {
        assert_eq!(fmt_ago(None, 100.200), "n/a");
    }

    #[test]
    fn latency_ms_is_to_minus_from() {
        assert!((latency_ms(1000.0, 1001.75) - 1750.0).abs() < 1e-9);
    }

    #[test]
    fn process_latency_spans_signal_ts_to_confirmed_ts_not_received_ts() {
        // signal_ts=100.000 (tick arrives), received_ts=100.001 (driver starts
        // the order call, negligible signal-processing delay), confirmed_ts=101.751
        // (exchange responds after a failed attempt + 1s retry sleep + round trips).
        // process_latency must span the full signal_ts -> confirmed_ts trip (1751ms),
        // not just received_ts -> confirmed_ts (1750ms) — this is the 2026-07-08
        // redefinition (see README.md's "Latency & observability infrastructure").
        let signal_ts = 100.000;
        let received_ts = 100.001;
        let confirmed_ts = 101.751;
        let signal_latency_ms = latency_ms(signal_ts, received_ts);
        let process_latency_ms = latency_ms(signal_ts, confirmed_ts);
        assert!((signal_latency_ms - 1.0).abs() < 1e-9, "got {signal_latency_ms}");
        assert!((process_latency_ms - 1751.0).abs() < 1e-9, "got {process_latency_ms}");
        assert!(process_latency_ms > latency_ms(received_ts, confirmed_ts));
    }
}
