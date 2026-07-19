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

use futures::StreamExt as _;
use trader::balance::{BalanceGuard, GammaBalanceTracker, seconds_until_next_check};
use trader::config::AssetParams;
use trader::execution::{
    ExecutionEngine, LiveConfig, LiveExecutionEngine, PaperExecutor, PaperOrderSide, SellStatus,
    SimExecutionEngine, local_signer_from_key, signature_type_from_env,
};
use trader::indicator_store::{IndicatorSnapshot, IndicatorStore};
use trader::marketdata::{
    MarketDuration, PolySub, clob_client, current_slot, fetch_gamma_resolution, fetch_meta,
    hourly_et_coin_name, http_client, now_secs_f64, spawn_binance_task,
};
use trader::telegram::commands::{Command, parse_command};
use trader::telegram::render::HELP_TEXT;
use trader::telegram::{AuthConfig, TelegramBot};
use trader::types::{BinanceTick, CycleContext, Outcome, PolyTick, Side, TradeRecord};
use trader::worker::{
    Action, BalanceEvent, CancelQuoteReason, CloseReason, ControlEvent, Event, PupGateOutcome,
    Worker,
};

const DEFAULT_FUND_ADDRESS: &str = "0x9FC2A777C26CCA2C218D8E7BBC340D14058CC13A";
// NOTE: clob-v2.polymarket.com 301-redirects POST /order to clob.polymarket.com,
// and that redirect silently downgrades POST to GET (standard client behavior
// for 301), which turns into a 405 on the real endpoint. Use the real host
// directly — same one bot/config.py's CLOB_HOST has always pointed at.
const CLOB_HOST: &str = "https://clob.polymarket.com";
// Matches the vendored SDK's own `Client<Unauthenticated>::default()` endpoint.
const UNWIND_WS_HOST: &str = "wss://ws-subscriptions-clob.polymarket.com";

/// `current_slot_val` starts at `0` every process start, so the first `ticker`
/// tick after *any* restart always looks like a fresh cycle boundary — even
/// when the real slot has already been running for minutes. A restart landing
/// mid-cycle must not fire `CycleOpen` with a fabricated `open_binance` (see
/// `trader/doc/fix_live_deploy_2026-07-15.md`). The ticker fires every 1s, so
/// a genuine boundary crossing during normal steady-state operation is always
/// caught within ~1-2s — 5s gives headroom for scheduler jitter while still
/// being far below any real "restart landed mid-cycle" gap (100s+ in the
/// incident that found this bug).
const STARTUP_MID_CYCLE_GUARD_SECS: f64 = 5.0;

/// Pure decision for the guard above — extracted so it's unit-testable
/// without needing the full `tokio::select!` loop. Only ever applies to the
/// first tick after process start (`is_first_tick`); every later boundary
/// crossing in the same process run is trusted as genuine.
fn should_suppress_startup_cycle(is_first_tick: bool, elapsed_into_slot: f64) -> bool {
    is_first_tick && elapsed_into_slot > STARTUP_MID_CYCLE_GUARD_SECS
}

type Signer = alloy::signers::local::LocalSigner<alloy::signers::k256::ecdsa::SigningKey>;

#[derive(Parser, Debug)]
#[command(
    name = "live",
    about = "Live trading driver — N (asset, strategy) workers, one shared account/Telegram bot"
)]
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

    /// Seconds into each cycle window the balance-drawdown checks (both the
    /// global 25%-from-session-baseline guard and the per-cycle scoped halt,
    /// 2026-07-11) sample the wallet balance. Was hardcoded to 120; see
    /// `trader/doc/plan_gammapi_2026-07-11.md`.
    #[arg(long, default_value_t = 120)]
    balance_check_offset_secs: u64,

    /// Subscribe to price ticks from a price_feed NATS publisher instead of opening
    /// direct Binance/Poly WS connections (e.g. nats://localhost:4222). Non-5m
    /// markets (see `[market_durations]` in strategy_*.toml) always open their own
    /// direct CLOB WS subscriptions regardless — price_feed only publishes the 5m
    /// market's poly ticks (trader/doc/feature_new_markets_2026-07-17.md §5.3).
    #[arg(long)]
    nats_url: Option<String>,

    /// Dry-run mode: no engine connection, no credentials needed, every
    /// PlaceBuy/ClosePosition is simulated as an instant full fill at the
    /// signal price (`SimExecutionEngine`), Telegram messages are prefixed
    /// [DRY]. For local/Docker validation — running the real order path
    /// locally would double-trade the same wallet as Oracle's live process
    /// (the 2026-07-03 double-process incident class). Default OFF.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Paper-trade mode (plan_unwind_5u_maker_2026-07-19 §2.1): full live
    /// lifecycle (telegram, halt, gamma watcher, config log) against
    /// `PaperExecutor` — fills simulated from *observed* CLOB prices (resting
    /// quotes only fill on a trade-through), no CLOB client at all, no
    /// credentials needed. Telegram messages are prefixed [PAPER]; trades go
    /// to paper_trades_*.csv, never live_trades_*.csv. Also enabled by
    /// `paper_trade = true` in the strategy TOML (either source turns it on);
    /// takes precedence over --dry-run.
    #[arg(long, default_value_t = false)]
    paper: bool,

    /// Path to a weather_*.toml (cities + one reversal parameter set — see
    /// trader::weather). Absent (the default, and the default in every
    /// production invocation) ⇒ the weather module never runs at all.
    /// trader/doc/feature_new_markets_2026-07-17.md §7.
    #[arg(long)]
    weather_config: Option<String>,
}

/// Which execution backend this process runs against. Exactly one of these is
/// picked at startup by `resolve_run_mode` and never changes mid-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    /// Real CLOB orders on the production account.
    Real,
    /// `SimExecutionEngine` — instant fills at the signal price.
    DryRun,
    /// `PaperExecutor` — fills simulated from observed CLOB prices
    /// (plan_unwind_5u_maker_2026-07-19 §2.1).
    Paper,
}

impl RunMode {
    /// Telegram prefix so a non-real run can never be misread as production
    /// output.
    fn prefix(self) -> &'static str {
        match self {
            RunMode::Real => "",
            RunMode::DryRun => "[DRY] ",
            RunMode::Paper => "[PAPER] ",
        }
    }
}

/// `--paper` / `[toml] paper_trade` / `--dry-run` precedence, pure for unit
/// testing (plan §2.4): paper from either source beats dry-run — a config
/// that says paper_trade must never silently run the instant-fill sim
/// instead, and vice versa a `--paper` invocation against an older config is
/// still paper.
fn resolve_run_mode(paper_flag: bool, toml_paper_trade: bool, dry_run_flag: bool) -> RunMode {
    if paper_flag || toml_paper_trade {
        RunMode::Paper
    } else if dry_run_flag {
        RunMode::DryRun
    } else {
        RunMode::Real
    }
}

/// Current time in Hong Kong (UTC+8), matching the Python bot's `_HKT` convention.
fn hkt_now() -> chrono::DateTime<chrono::FixedOffset> {
    chrono::Utc::now().with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
}

/// `"ASSET/strategy"` labels for every worker currently `Confirming` (an unresolved
/// trade still awaiting Gamma) — the scope of the per-cycle balance-decrease halt
/// (2026-07-11, `trader/doc/plan_gammapi_2026-07-11.md`). A single account-wide balance
/// sample can't attribute a drop to one asset+strategy over another when several are
/// pending at once, so every currently-pending one is halted, not just one guessed at —
/// still not "the whole thing" the way the 25%-drawdown backstop is, since idle
/// (`Watching`) slots are untouched. Pure and free of `AssetSlot` so it's unit-testable
/// against plain `Worker`s.
/// `labeled` pairs each worker with its market label (`AssetSlot::market_key()`
/// at the real call site — bare asset for 5m, `"BTC@15m"`-style otherwise) so
/// a Confirming BTC-15m slot reads distinctly from a Confirming BTC-5m one.
fn confirming_asset_labels<'a>(
    labeled: impl IntoIterator<Item = (String, &'a Worker)>,
) -> Vec<String> {
    labeled
        .into_iter()
        .filter(|(_, w)| w.is_confirming())
        .map(|(label, w)| format!("{}/{}", label, w.strategy_name))
        .collect()
}

/// Display-only side label with an arrow — doesn't touch `Side::as_str()` (used by CSV
/// logging), just makes Telegram messages easier to scan at a glance.
fn arrow_side(side: Side) -> &'static str {
    match side {
        Side::Up => "UP ↑",
        Side::Down => "DOWN ↓",
    }
}

/// Gamma resolution watcher timing, all three sourced from the asset's own config
/// (`gamma_poll_deadline_secs`/`gamma_poll_delay_secs`/`gamma_poll_interval_secs`) —
/// grouped into one struct purely to keep `spawn_resolution_watcher`'s argument count
/// under clippy's `too_many_arguments` limit.
#[derive(Debug, Clone, Copy)]
struct GammaPollCadence {
    /// Give up (and report a timeout) this many seconds after the position closed.
    deadline_secs: f64,
    /// Wait this long before the first poll attempt.
    poll_delay_secs: f64,
    /// Retry cadence after that first attempt.
    poll_interval_secs: f64,
}

/// Poll Gamma for `slug`'s resolution until either it settles or `cadence.deadline_secs`
/// has elapsed since the position closed, then report back on `tx` as
/// `(asset, strategy, Some(won))` (relative to `side`, the position's own side) or
/// `(asset, strategy, None)` on a deadline timeout.
///
/// `deadline_secs` is `gamma_poll_deadline_secs` — its own dedicated config field as of
/// 2026-07-11 (`trader/doc/plan_gammapi_2026-07-11.md`; previously it reused
/// `reversal_start_time`, "the earliest either strategy could possibly want to open a
/// new position this cycle anyway," on the theory that blocking new entries on this
/// asset+strategy until then cost nothing — decoupled once the window grew to 10
/// minutes, well past that assumption, and `Worker::try_enter` gates on `WorkerState`
/// regardless, so a `Confirming` position naturally survives a cycle boundary if Gamma
/// is still unresolved when the next cycle opens).
///
/// Polling doesn't start immediately: Gamma "usually won't give you anything until
/// 20-60s after cycle end" (trader/doc/incident_DOGE_wrong_result_2026-07-09.md §4), so
/// the first attempt waits `poll_delay_secs` (clamped to `deadline_secs`, in case a
/// misconfigured delay would otherwise exceed the deadline), then retries every
/// `poll_interval_secs` (2026-07-09; previously an immediate, hardcoded 1s cadence; 20s
/// as of 2026-07-11).
fn spawn_resolution_watcher(
    http: reqwest::Client,
    slug: String,
    side: Side,
    asset: String,
    strategy: &'static str,
    cadence: GammaPollCadence,
    tx: mpsc::UnboundedSender<(String, &'static str, Option<bool>)>,
) {
    let GammaPollCadence {
        deadline_secs,
        poll_delay_secs,
        poll_interval_secs,
    } = cadence;
    tokio::spawn(async move {
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs_f64(deadline_secs);
        tokio::time::sleep(std::time::Duration::from_secs_f64(
            poll_delay_secs.min(deadline_secs).max(0.0),
        ))
        .await;
        loop {
            match fetch_gamma_resolution(&http, &slug).await {
                Some(went_up) => {
                    let won = match side {
                        Side::Up => went_up,
                        Side::Down => !went_up,
                    };
                    let _ = tx.send((asset, strategy, Some(won)));
                    return;
                }
                None if tokio::time::Instant::now() >= deadline => {
                    println!(
                        "[live] gave up waiting for API resolution of {slug} after {deadline_secs:.0}s — halting {asset} ({strategy}) for manual review"
                    );
                    let _ = tx.send((asset, strategy, None));
                    return;
                }
                None => {}
            }
            tokio::time::sleep(std::time::Duration::from_secs_f64(
                poll_interval_secs.max(0.1),
            ))
            .await;
        }
    });
}

const CSV_HEADER: &str = "logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error,\
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
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        writeln!(f, "{CSV_HEADER}")?;
        return Ok(());
    }

    let file = std::fs::File::open(path)?;
    let mut lines = std::io::BufReader::new(file).lines();
    let Some(first) = lines.next().transpose()? else {
        return Ok(());
    }; // empty file
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

const CANCELED_QUOTE_CSV_HEADER: &str = "logged_at,slug,strategy,side,quote_price,reason";

/// Every canceled maker-entry quote (plan_unwind_5u_maker_2026-07-19 §2.2):
/// slug/side/quote price/cancel reason. The 48h evaluation (§2.7) resolves
/// each row against Gamma to compute the counterfactual outcome — the
/// filled-vs-canceled adverse-selection comparison the plan calls the single
/// most important output of the paper run.
fn append_canceled_quote_header_if_new(path: &str) -> Result<()> {
    use std::io::Write as _;
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    writeln!(f, "{CANCELED_QUOTE_CSV_HEADER}")?;
    Ok(())
}

/// Best-effort — a write failure here must never interrupt live trading
/// (same fail-open posture as `log_control_event`). Silently a no-op when
/// there's no active cycle (`current_slug` empty), which can only happen if
/// a quote somehow survives past `on_cycle_close`'s reset (it shouldn't:
/// T-15s cancels it first).
fn log_canceled_quote(slot: &AssetSlot, side: Side, quote_price: f64, reason: CancelQuoteReason) {
    use std::io::Write as _;
    let Some(slug) = &slot.current_slug else {
        return;
    };
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&slot.canceled_quote_log_path)
    else {
        return;
    };
    let _ = writeln!(
        f,
        "{},{},{},{},{quote_price:.4},{reason:?}",
        now_secs_f64(),
        slug,
        slot.worker.strategy_name,
        side.as_str()
    );
}

const PUP_VETO_CSV_HEADER: &str = "logged_at,slug,strategy,side,p_side,price";

/// Every reversal entry the p(up) negative-edge gate blocked
/// (plan_unwind_5u_maker_2026-07-19 §2.3) — the 48h evaluation gamma-resolves
/// each row to check whether the veto actually saved money.
fn append_pup_veto_header_if_new(path: &str) -> Result<()> {
    use std::io::Write as _;
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    writeln!(f, "{PUP_VETO_CSV_HEADER}")?;
    Ok(())
}

/// Same fail-open posture as `log_canceled_quote` — a write failure here must
/// never interrupt live trading.
fn log_pup_veto(slot: &AssetSlot, side: Side, p_side: f64, price: f64) {
    use std::io::Write as _;
    let Some(slug) = &slot.current_slug else {
        return;
    };
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&slot.pup_veto_log_path)
    else {
        return;
    };
    let _ = writeln!(
        f,
        "{},{},{},{},{p_side:.4},{price:.4}",
        now_secs_f64(),
        slug,
        slot.worker.strategy_name,
        side.as_str()
    );
}

const WEATHER_CSV_HEADER: &str =
    "logged_at,city,bucket,entry_ts,exit_ts,entry_price,exit_price,shares,outcome,pnl";

/// Weather trades get their own file + schema (`live_trades_weather.csv`) —
/// they have no slug/strategy/side in the crypto sense, so gluing them into
/// the per-asset CSVs would corrupt every consumer of those.
fn append_weather_csv_header_if_new(path: &str) -> Result<()> {
    use std::io::Write as _;
    if std::path::Path::new(path).exists() {
        return Ok(());
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    writeln!(f, "{WEATHER_CSV_HEADER}")?;
    Ok(())
}

fn log_weather_trade(path: &str, t: &trader::weather::WeatherTrade) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(
        f,
        "{},{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(),
        csv_sanitize(&t.city),
        csv_sanitize(&t.bucket),
        t.entry_ts,
        t.exit_ts,
        t.entry_price,
        t.exit_price,
        t.shares,
        t.outcome,
        t.pnl
    )?;
    Ok(())
}

fn log_trade(path: &str, rec: &TradeRecord) -> Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let exit_last_error = rec
        .exit_last_error
        .as_deref()
        .map(csv_sanitize)
        .unwrap_or_default();
    writeln!(
        f,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        trader::marketdata::now_secs_f64(),
        rec.slug,
        rec.strategy,
        rec.side.as_str(),
        rec.entry_ts,
        rec.token_price,
        rec.exit_price,
        rec.outcome.as_str(),
        rec.pnl,
        rec.exit_attempts,
        exit_last_error,
        rec.entry_signal_latency_ms,
        rec.entry_process_latency_ms,
        rec.exit_signal_latency_ms,
        rec.exit_process_latency_ms
    )?;
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
    serde_json::from_slice::<ServerTs>(payload)
        .ok()
        .and_then(|s| s.server_ts)
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

/// Append-only, timestamped record of every event that can change a slot's
/// `is_halted()` state — user commands (`/halt`, `/resume`, `/reset_losses`)
/// and automatic engine events (loss-streak engage/reset, balance drawdown,
/// Gamma-unresolved halt, a Gamma correction clearing one) alike, one shared
/// `control_log.jsonl` across every asset/strategy in `log_dir`. Read by
/// `trade_reconcile.py` to reconstruct the real historical halt timeline per
/// (asset, strategy) for backtest reconciliation — replaying with the
/// backtest's own from-scratch `HaltTracker` alone can diverge arbitrarily
/// from what live actually experienced (a different simulated trade history,
/// or a manual `/reset_losses` mid-day the backtest has no way to know
/// about); see `trader/doc/plan_align_bt_with_live_2026-07-15.md` and
/// `trader/doc/incident_recon_btc_reversal_2026-07-15.md` for the incident
/// that motivated this. Best-effort — a write failure here must never
/// interrupt live trading, same fail-open posture as `persist`.
fn log_control_event(slot: &AssetSlot, event: &str) {
    #[derive(serde::Serialize)]
    struct ControlLogEntry<'a> {
        ts: f64,
        asset: &'a str,
        strategy: &'a str,
        event: &'a str,
        entry_suppressed: bool,
        halt_losses: i64,
        is_halted: bool,
    }
    let now = chrono::Utc::now();
    // 5m slots log the bare asset (byte-identical to pre-duration entries, so
    // trade_reconcile.py's halt-timeline reconstruction is unaffected);
    // non-5m slots log "BTC@15m"-style keys, which that script's per-(asset,
    // strategy) 5m matching simply never selects.
    let market_key = slot.market_key();
    let entry = ControlLogEntry {
        ts: now.timestamp() as f64 + now.timestamp_subsec_millis() as f64 / 1000.0,
        asset: &market_key,
        strategy: slot.worker.strategy_name,
        event,
        entry_suppressed: slot.worker.manually_suppressed(),
        halt_losses: slot.worker.halt_losses(),
        is_halted: slot.worker.is_halted(),
    };
    let Ok(line) = serde_json::to_string(&entry) else {
        return;
    };
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&slot.control_log_path)
    {
        use std::io::Write as _;
        let _ = writeln!(f, "{line}");
    }
}

/// `Worker::on_control` always returns exactly `[Action::Persist]` — apply the event and act on
/// that signal immediately, rather than leaving `entry_suppressed` unpersisted until some later,
/// unrelated event happens to persist this slot (2026-07-11,
/// `trader/doc/plan_halt_persist_2026-07-11.md`). Deliberately *not* used by the SIGINT/SIGTERM
/// shutdown handlers — see that doc §3 for why persisting a shutdown-time halt would be a real
/// behavior change (every deploy restart would come back halted), not just a bugfix.
fn apply_control(slot: &mut AssetSlot, event: ControlEvent) {
    slot.worker.step(Event::Control(event));
    persist(slot);
    log_control_event(
        slot,
        match event {
            ControlEvent::Halt => "halt",
            ControlEvent::Resume => "resume",
            ControlEvent::ResetLosses => "reset_losses",
        },
    );
}

/// Builds a warning suffix listing any slot that's still halted purely due to
/// the loss-streak counter (`halt_rev`/`halt_prob`) after a `/resume` —
/// `/resume` only ever clears the manual/drawdown gate and deliberately never
/// touches this one (§8), so an unqualified "Resumed" reply would otherwise
/// falsely claim success. See `trader/doc/incident_unable_to_resume_2026-07-15.md`.
fn loss_streak_still_halted_note<'a>(slots: impl Iterator<Item = &'a AssetSlot>) -> Option<String> {
    let still: Vec<String> = slots
        .filter(|s| s.worker.loss_streak_halted())
        .map(|s| {
            format!(
                "{}/{} ({}/{})",
                s.worker.asset,
                s.worker.strategy_name,
                s.worker.halt_losses(),
                s.worker.halt_max()
            )
        })
        .collect();
    if still.is_empty() {
        None
    } else {
        Some(format!(
            "\n⚠️ still halted (loss-streak): {} — /reset_losses to clear now, or wait for the daily reset.",
            still.join(", ")
        ))
    }
}

/// Same as `apply_control`, for the one `BalanceEvent` variant — `Worker::on_balance` also always
/// returns exactly `[Action::Persist]`.
fn apply_balance_halt(slot: &mut AssetSlot) {
    slot.worker.step(Event::Balance(BalanceEvent::DrawdownHalt));
    persist(slot);
    log_control_event(slot, "drawdown_halt");
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
    /// Which market family this slot trades (trader/doc/
    /// feature_new_markets_2026-07-17.md). 5m slots behave exactly as before
    /// this field existed; non-5m slots differ only in slug construction,
    /// rotation period, direct-WS poly feed, and duration-tagged file names.
    duration: MarketDuration,
    /// This slot's own last-seen cycle slot (epoch multiple of its period) —
    /// per-slot because different durations cross boundaries at different
    /// instants. `0` = no boundary seen yet this process run (drives the
    /// startup mid-cycle suppression check, per slot).
    slot_val: u64,
    up_id: U256,
    dn_id: U256,
    current_token_id: Option<U256>,
    max_buy_price: f64,
    log_path: String,
    state_file: String,
    /// Canceled maker-entry quotes (plan_unwind_5u_maker_2026-07-19 §2.2):
    /// slug/side/quote price/cancel reason for every quote that never got a
    /// fill — the filled-vs-canceled comparison this study exists to measure.
    /// Empty/unused for non-maker-entry workers.
    canceled_quote_log_path: String,
    /// Vetoed entries (plan_unwind_5u_maker_2026-07-19 §2.3): slug/side/
    /// p_side/price for every reversal entry the p(up) negative-edge gate
    /// blocked — the 48h evaluation gamma-resolves each row to check whether
    /// the veto actually saved money. Empty/unused when `pup_edge_min_rev`
    /// is disabled.
    pup_veto_log_path: String,
    /// Shared across every slot (one file, `control_log.jsonl` in `log_dir` —
    /// see `log_control_event`).
    control_log_path: String,
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

impl AssetSlot {
    /// The slot's market identity: bare `"BTC"` for 5m (so every pre-existing
    /// key — NATS poly routing, resolution-watcher handoff, control_log asset
    /// field, display strings — is byte-identical to before durations
    /// existed), `"BTC@15m"` / `"BTC@1h-et"` / `"BTC@4h"` otherwise. Used as
    /// the poly-tick routing key (a BTC-5m tick must never reach a BTC-15m
    /// worker — different CLOB tokens), the Gamma-watcher reply key, the
    /// control_log asset label (so trade_reconcile.py's 5m halt-timeline
    /// reconstruction never sees non-5m events under a 5m key), and the
    /// human-facing market label in logs/Telegram.
    fn market_key(&self) -> String {
        if self.duration.is_5m() {
            self.worker.asset.clone()
        } else {
            format!("{}@{}", self.worker.asset, self.duration.suffix())
        }
    }
}

/// Shared context (account connection + Telegram) threaded through the
/// recursive action loop. Holds no per-asset state — that all lives in
/// `AssetSlot` — so it only needs `&self`, letting multiple assets share one
/// `Driver` without fighting over a mutable borrow.
struct Driver<'a> {
    /// The order-execution boundary — the real `LiveExecutionEngine` in
    /// production, `SimExecutionEngine` under `--dry-run`, `PaperExecutor`
    /// under `--paper`/`paper_trade = true`.
    engine: &'a dyn ExecutionEngine,
    /// The concrete live engine when one exists (`None` under dry-run/paper) —
    /// only for the non-trait extras: `fetch_balance` in `/status` and the
    /// periodic balance-guard sampling.
    live_engine: Option<&'a LiveExecutionEngine<Signer>>,
    /// Prefixes every Telegram message with `[DRY]`/`[PAPER]` so a simulated
    /// run can never be misread as production output.
    mode: RunMode,
    telegram: Option<Arc<TelegramBot>>,
    http: reqwest::Client,
    api_result_tx: mpsc::UnboundedSender<(String, &'static str, Option<bool>)>,
}

impl Driver<'_> {
    async fn notify(&self, text: &str) {
        if let Some(bot) = &self.telegram {
            let text = format!("{}{text}", self.mode.prefix());
            match bot.send(&text).await {
                Ok(_) => println!("[telegram] sent: {}", text.lines().next().unwrap_or("")),
                Err(e) => eprintln!("[telegram] send error: {e:#}"),
            }
        }
    }

    /// Full `/status` reply: balance+time header, then TRADE ASSETS / MARKETS
    /// / PNL sections. One row per `(asset, strategy)` slot, sorted so a
    /// multi-strategy asset's rows render adjacently (mirrors Python's
    /// per-asset-then-per-strategy `_status()` breakdown).
    /// Shown at the top of `/status` since the loss-streak halt's daily reset
    /// hour otherwise isn't visible without checking `/params` per asset.
    /// Groups by (strategy, hour) rather than assuming one global value per
    /// strategy, so a config that ever splits `halt_reset_hour_rev`/`_hp` by
    /// asset still renders correctly instead of silently picking one asset's
    /// value. See `HaltTracker`'s daily-HKT-reset doc comment in `worker.rs`.
    fn render_auto_reset_line(assets: &[AssetSlot]) -> String {
        let mut groups: std::collections::BTreeMap<(&'static str, i64), Vec<&str>> =
            std::collections::BTreeMap::new();
        for slot in assets {
            let hour = match slot.worker.strategy_name {
                "high_prob" => slot.params.halt_reset_hour_hp,
                "v_shape" => slot.params.halt_reset_hour_v,
                _ => slot.params.halt_reset_hour_rev,
            };
            groups
                .entry((slot.worker.strategy_name, hour))
                .or_default()
                .push(slot.worker.asset.as_str());
        }
        if groups.is_empty() {
            return "Auto-reset (loss-streak halt): n/a".to_string();
        }
        let parts: Vec<String> = groups
            .into_iter()
            .map(|((strategy, hour), assets)| {
                format!("{strategy} {hour:02}:00 HKT ({})", assets.join(","))
            })
            .collect();
        format!(
            "Auto-reset (loss-streak halt, daily): {}",
            parts.join(" · ")
        )
    }

    async fn render_status(&self, assets: &[AssetSlot]) -> String {
        let now = hkt_now().format("%H:%M:%S HKT");
        let balance = match self.live_engine {
            Some(engine) => match engine.fetch_balance().await {
                Some(b) => format!("${b:.4}"),
                None => "n/a (fetch failed)".to_string(),
            },
            None => "n/a (dry-run)".to_string(),
        };
        let mut sections = vec![format!(
            "📊 <b>STATUS</b>  ({now})\nBalance: {balance}\n{}",
            Self::render_auto_reset_line(assets)
        )];

        let mut slots: Vec<&AssetSlot> = assets.iter().collect();
        slots.sort_by(|a, b| {
            (a.worker.asset.as_str(), a.duration, a.worker.strategy_name).cmp(&(
                b.worker.asset.as_str(),
                b.duration,
                b.worker.strategy_name,
            ))
        });

        let mut ta_lines = Vec::new();
        let mut mkt_lines = Vec::new();
        let mut pnl_lines = Vec::new();
        let (mut tw, mut tl, mut ts, mut tu, mut tt) = (0u32, 0u32, 0u32, 0u32, 0u32);
        let mut t_pnl = 0.0f64;
        let mut seen_markets = std::collections::HashSet::new();

        for slot in &slots {
            // "BTC" for 5m (unchanged), "BTC@15m" etc. for other durations.
            let name = slot.market_key();
            let halted = slot.worker.is_halted();
            // Surfaces *why* it's halted — `/resume` only clears `suppressed`,
            // never `loss-streak` (that needs `/reset_losses` or the daily
            // reset), so an operator seeing just "🟡 halted" after a `/resume`
            // has no way to tell the two apart. See
            // trader/doc/incident_unable_to_resume_2026-07-15.md.
            let light = if !halted {
                "🟢 active".to_string()
            } else {
                let mut reasons = Vec::new();
                if slot.worker.manually_suppressed() {
                    reasons.push("suppressed".to_string());
                }
                if slot.worker.loss_streak_halted() {
                    reasons.push(format!(
                        "loss-streak {}/{}",
                        slot.worker.halt_losses(),
                        slot.worker.halt_max()
                    ));
                }
                format!("🟡 halted ({})", reasons.join(" + "))
            };
            // `low`/`high` are the strategy's entry trigger band — reversal_low_threshold/
            // reversal for reversal (aka "unwind" — the reversal+take-profit-unwind
            // strategy), price_low/price_high for high_prob, v_low/v_high2 (the dip floor
            // and the re-entry bar) for v_shape.
            let (sl, delta_gate, low, high, halt_n, unwind_pnl, sl_pnl, unwind_time, start) =
                match slot.worker.strategy_name {
                    "high_prob" => (
                        slot.params.sl_high_prob,
                        slot.params.delta_pct_hp,
                        slot.params.price_low,
                        slot.params.price_high,
                        slot.params.halt_prob,
                        slot.params.unwind_pnl_hp,
                        slot.params.sl_pnl_hp,
                        slot.params.unwind_time_hp,
                        None,
                    ),
                    "v_shape" => (
                        slot.params.sl_v_shape,
                        slot.params.delta_pct_v,
                        slot.params.v_low,
                        slot.params.v_high2,
                        slot.params.halt_v,
                        slot.params.unwind_pnl_v,
                        slot.params.sl_pnl_v,
                        slot.params.unwind_time_v,
                        None,
                    ),
                    _ => (
                        slot.params.sl_reversal,
                        slot.params.delta_pct_rev,
                        slot.params.reversal_low_threshold,
                        slot.params.reversal,
                        slot.params.halt_rev,
                        slot.params.unwind_pnl_rev,
                        slot.params.sl_pnl_rev,
                        slot.params.unwind_time_rev,
                        Some(slot.params.reversal_start_time),
                    ),
                };
            let start_str = match start {
                Some(s) => format!("  start={s:.0}s"),
                None => String::new(),
            };
            ta_lines.push(format!(
                "  {light}  {name}  strategy={}\n    sl={sl:.4}  delta_gate={delta_gate:.5}  low={low:.4}  high={high:.4}  halt_after={halt_n}L  unwind_pnl={unwind_pnl:.4}  sl_pnl={sl_pnl:.4}  unwind_time={unwind_time:.1}s{start_str}  size=${:.2}",
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
                "  {name}: {}W/{}L/{}SL/{}UW/{}TO  {sign}${:.4}",
                slot.wins,
                slot.losses,
                slot.stoplosses,
                slot.unwinds,
                slot.timeouts,
                slot.total_pnl
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
        let mut all_pnl = vec![format!(
            "  Session: {tw}W/{tl}L/{ts}SL/{tu}UW/{tt}TO  {sign}${t_pnl:.4}"
        )];
        all_pnl.extend(pnl_lines);
        sections.push(format!("<b>PNL</b>\n{}", all_pnl.join("\n")));

        sections.join("\n\n")
    }

    /// Execute one `Action` against the live engine; returns the follow-up
    /// `Event` (if any) to feed back into `worker.step`.
    async fn execute(&self, slot: &mut AssetSlot, action: &Action, feed: Feed) -> Option<Event> {
        match action {
            Action::PlaceBuy {
                side,
                price,
                size_usdc,
                signal_ts,
            } => {
                let token_id = if *side == Side::Up {
                    slot.up_id
                } else {
                    slot.dn_id
                };
                slot.current_token_id = Some(token_id);
                let received_ts = now_secs_f64();
                let result = self
                    .engine
                    .place(token_id, *price, *size_usdc, slot.max_buy_price)
                    .await;
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
                let clob_latency_ms =
                    exchange_latency_ms(slot.last_poly_ts, slot.last_poly_server_ts);
                let binance_latency_ms =
                    exchange_latency_ms(slot.last_binance_ts, slot.last_binance_server_ts);
                let clob_tag = match feed {
                    Feed::Clob => "trigger".to_string(),
                    Feed::Binance => fmt_ago(slot.last_poly_ts, received_ts),
                };
                let binance_tag = match feed {
                    Feed::Binance => "trigger".to_string(),
                    Feed::Clob => fmt_ago(slot.last_binance_ts, received_ts),
                };
                let clob_latency_str =
                    format!("clob_latency={} ({clob_tag})", fmt_latency(clob_latency_ms));
                let binance_latency_str = format!(
                    "binance_latency={} ({binance_tag})",
                    fmt_latency(binance_latency_ms)
                );
                println!(
                    "[ORDER] {} BUY {side:?} @ {price:.4} size=${size_usdc:.2} -> placed={} shares={:.4} cost={:.4} err={:?} ({clob_latency_str} {binance_latency_str} process_ms={process_latency_ms:.0} n_attempts={})",
                    slot.market_key(),
                    result.placed,
                    result.filled_shares,
                    result.cost,
                    result.error,
                    result.attempts
                );

                let dt = hkt_now().format("%H:%M:%S");
                let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                let delta_pct = slot.worker.delta_pct() * 100.0;
                if result.placed && result.filled_shares > 0.0 {
                    self.notify(&format!(
                        "📋 <b>{}</b> Order placed | {dt} | T-{time_left}s | {} | {}\nprice={:.4} | delta={delta_pct:+.3}% | {clob_latency_str} | {binance_latency_str} | process_latency={process_latency_ms:.0}ms | n_attempts={}",
                        slot.market_key(), arrow_side(*side), slot.worker.strategy_name, result.cost, result.attempts
                    )).await;
                    Some(Event::OrderFilled {
                        filled_shares: result.filled_shares,
                        cost: result.cost,
                        signal_latency_ms,
                        process_latency_ms,
                    })
                } else {
                    self.notify(&format!(
                        "❗ <b>{}</b> Order REJECTED | {dt} | T-{time_left}s | {} | {}\nsignal price={price:.4} | delta={delta_pct:+.3}% | n_attempts={} | error={}",
                        slot.market_key(), arrow_side(*side), slot.worker.strategy_name,
                        result.attempts,
                        result.error.as_deref().unwrap_or("unknown")
                    )).await;
                    Some(Event::OrderRejected)
                }
            }
            Action::PlaceLimitSell { shares, price } => {
                let token_id = slot.current_token_id?;
                let received_ts = now_secs_f64();
                let r = self
                    .engine
                    .place_limit_sell(token_id, *shares, *price)
                    .await;
                let confirmed_ts = now_secs_f64();
                println!(
                    "[ORDER] {} LIMIT SELL {shares:.4} @ {price:.4} -> status={:?} order_id={:?} err={:?}",
                    slot.market_key(),
                    r.status,
                    r.order_id,
                    r.error
                );
                // No external signal_ts for this action (it's an internal
                // follow-up to the entry fill, not driven by a market tick) —
                // only the process leg is meaningful here.
                Some(Event::LimitSellPlaced {
                    order_id: r.order_id,
                    status: r.status,
                    error: r.error,
                    signal_latency_ms: 0.0,
                    process_latency_ms: latency_ms(received_ts, confirmed_ts),
                })
            }
            Action::PlaceLimitBuy {
                side,
                price,
                shares,
                signal_ts,
            } => {
                let token_id = if *side == Side::Up {
                    slot.up_id
                } else {
                    slot.dn_id
                };
                slot.current_token_id = Some(token_id);
                let received_ts = now_secs_f64();
                let r = self.engine.place_limit_buy(token_id, *shares, *price).await;
                let confirmed_ts = now_secs_f64();
                let signal_latency_ms = latency_ms(*signal_ts, received_ts);
                let process_latency_ms = latency_ms(*signal_ts, confirmed_ts);
                println!(
                    "[ORDER] {} MAKER ENTRY BUY {shares:.2} @ {price:.4} ({side:?}) -> status={:?} order_id={:?} err={:?}",
                    slot.market_key(),
                    r.status,
                    r.order_id,
                    r.error
                );
                if matches!(r.status, SellStatus::Live) {
                    let dt = hkt_now().format("%H:%M:%S");
                    let time_left = (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                    self.notify(&format!(
                        "📝 <b>{}</b> Maker quote resting | {dt} | T-{time_left}s | {} | {}\nBUY {shares:.2}sh @ {price:.4}",
                        slot.market_key(), arrow_side(*side), slot.worker.strategy_name
                    )).await;
                }
                Some(Event::LimitBuyPlaced {
                    order_id: r.order_id,
                    status: r.status,
                    error: r.error,
                    signal_latency_ms,
                    process_latency_ms,
                })
            }
            Action::ClosePosition {
                shares,
                reason,
                limit_price,
                signal_ts,
            } => {
                let token_id = slot.current_token_id?;
                if matches!(reason, CloseReason::StopLoss) {
                    println!(
                        "[SL] {} stop-loss triggered — closing {shares:.4} shares (sl_pnl floor crossed; up to 5 retries)",
                        slot.market_key()
                    );
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
                        let side = if token_id == slot.up_id {
                            Side::Up
                        } else {
                            Side::Down
                        };
                        let trigger_price = if side == Side::Up {
                            slot.last_poly_up
                        } else {
                            slot.last_poly_dn
                        };
                        let dt = hkt_now().format("%H:%M:%S");
                        let time_left =
                            (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                        let delta_pct = slot.worker.delta_pct() * 100.0;
                        self.notify(&format!(
                            "🛑 <b>{}</b> STOP LOSS triggered | {dt} | T-{time_left}s | {} | {}\nprice={trigger_price:.4} | delta={delta_pct:+.3}%",
                            slot.market_key(), arrow_side(side), slot.worker.strategy_name
                        )).await;
                    }
                }
                if matches!(reason, CloseReason::Timeout) {
                    println!(
                        "[TIMEOUT] {} max holding time elapsed — closing {shares:.4} shares (unwind_time floor crossed; up to 5 retries)",
                        slot.market_key()
                    );
                    // Same first-trigger-only guard as stop-loss — worker.rs
                    // re-fires ClosePosition{Timeout} every PolyTick until the
                    // position clears.
                    if !slot.timeout_notified {
                        slot.timeout_notified = true;
                        let side = if token_id == slot.up_id {
                            Side::Up
                        } else {
                            Side::Down
                        };
                        let trigger_price = if side == Side::Up {
                            slot.last_poly_up
                        } else {
                            slot.last_poly_dn
                        };
                        let dt = hkt_now().format("%H:%M:%S");
                        let time_left =
                            (slot.worker.cycle_end_ts() - now_secs_f64()).max(0.0) as i64;
                        self.notify(&format!(
                            "⏱️ <b>{}</b> TIME LIMIT triggered | {dt} | T-{time_left}s | {} | {}\nprice={trigger_price:.4} | max holding time elapsed — closing at market",
                            slot.market_key(), arrow_side(side), slot.worker.strategy_name
                        )).await;
                    }
                }
                let received_ts = now_secs_f64();
                // Take-profit closes are bounded at limit_price (== the position's
                // own tp_price — no separate config, see
                // trader/doc/incident_sol_unwind_but_loss_2026-07-06.md); a
                // stop-loss has no floor and must close regardless of price.
                let mut result = match limit_price {
                    Some(price) => {
                        self.engine
                            .close_position_at_price(token_id, *shares, *price)
                            .await
                    }
                    None => self.engine.close_position(token_id, *shares).await,
                };
                // Dry-run fill pricing: `SimExecutionEngine::close_position` has
                // no market price and returns `filled_usdc: 0.0`, which would
                // book every simulated stop-loss/timeout exit as a total loss.
                // Price it at the held side's last mid instead — dry-run only
                // (`PaperExecutor` prices its own fills from observed ticks);
                // a real matched close always carries its own real proceeds.
                if self.mode == RunMode::DryRun
                    && matches!(result.status, SellStatus::Matched)
                    && result.filled_usdc == 0.0
                    && result.shares_sold > 0.0
                {
                    let side_price = if token_id == slot.up_id {
                        slot.last_poly_up
                    } else {
                        slot.last_poly_dn
                    };
                    result.filled_usdc = result.shares_sold * side_price;
                }
                let confirmed_ts = now_secs_f64();
                let signal_latency_ms = latency_ms(*signal_ts, received_ts);
                let process_latency_ms = latency_ms(*signal_ts, confirmed_ts);
                // Exits are always Poly/CLOB-triggered (only `on_poly` ever
                // produces a ClosePosition — see worker.rs), so this is always
                // the CLOB exchange latency, unlike the entry side above — no
                // "(trigger)"/"(Nms ago)" tag needed here, only one feed applies.
                let clob_latency_ms =
                    exchange_latency_ms(slot.last_poly_ts, slot.last_poly_server_ts);
                let clob_latency_str = format!("clob_latency={}", fmt_latency(clob_latency_ms));
                println!(
                    "[ORDER] {} CLOSE {shares:.4} ({reason:?}) -> status={:?} sold={:.4} usdc={:.4} err={:?} ({clob_latency_str} process_ms={process_latency_ms:.0} n_attempts={})",
                    slot.market_key(),
                    result.status,
                    result.shares_sold,
                    result.filled_usdc,
                    result.error,
                    result.attempts
                );
                let sold = result.shares_sold;
                let exit_price = if sold > 0.0 {
                    result.filled_usdc / sold
                } else {
                    0.0
                };
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
                        slot.market_key(), slot.worker.strategy_name, result.filled_usdc, result.attempts
                    )).await;
                }
                let event = match (matched, reason) {
                    (true, CloseReason::TakeProfit) => Event::UnwindFilled {
                        sold_shares: sold,
                        exit_price,
                        signal_latency_ms,
                        process_latency_ms,
                    },
                    (true, CloseReason::StopLoss) => Event::StopSellFilled {
                        sold_shares: sold,
                        exit_price,
                        signal_latency_ms,
                        process_latency_ms,
                    },
                    (true, CloseReason::Timeout) => Event::TimeoutSellFilled {
                        sold_shares: sold,
                        exit_price,
                        signal_latency_ms,
                        process_latency_ms,
                    },
                    (false, CloseReason::TakeProfit) => Event::UnwindFailed {
                        error: result.error,
                    },
                    (false, CloseReason::StopLoss) => Event::StopSellFailed {
                        error: result.error,
                    },
                    (false, CloseReason::Timeout) => Event::TimeoutSellFailed {
                        error: result.error,
                    },
                };
                if matched && sold >= *shares {
                    slot.current_token_id = None;
                }
                Some(event)
            }
            Action::CancelLimitSell { order_id } => {
                let ok = self.engine.cancel_resting_order(order_id).await;
                println!("[ORDER] {} CANCEL {order_id} -> {ok}", slot.market_key());
                None
            }
            Action::CancelEntryQuote {
                order_id,
                side,
                quote_price,
                reason,
            } => {
                if let Some(id) = order_id {
                    let ok = self.engine.cancel_resting_order(id).await;
                    println!(
                        "[ORDER] {} CANCEL ENTRY QUOTE {id} side={side:?} quote={quote_price:.4} reason={reason:?} -> {ok}",
                        slot.market_key()
                    );
                } else {
                    println!(
                        "[ORDER] {} CANCEL ENTRY QUOTE (order not yet acked) side={side:?} quote={quote_price:.4} reason={reason:?}",
                        slot.market_key()
                    );
                }
                log_canceled_quote(slot, *side, *quote_price, *reason);
                None
            }
            Action::PupGateNote {
                side,
                p_side,
                price,
                outcome,
            } => {
                match outcome {
                    PupGateOutcome::Veto => {
                        let p_side = p_side.unwrap_or(f64::NAN);
                        println!(
                            "[PUP-GATE] {} VETO side={side:?} p_side={p_side:.4} price={price:.4}",
                            slot.market_key()
                        );
                        log_pup_veto(slot, *side, p_side, *price);
                    }
                    PupGateOutcome::SkippedNoData => {
                        println!(
                            "[PUP-GATE] {} pup_gate=SKIPPED_NO_DATA side={side:?} price={price:.4}",
                            slot.market_key()
                        );
                    }
                }
                None
            }
            Action::Persist
            | Action::LogTrade(_)
            | Action::LogTradeCorrection { .. }
            | Action::StopLossVerdict { .. }
            | Action::HaltEngaged
            | Action::HaltReset
            | Action::HaltClearedByCorrection
            | Action::GammaHaltEngaged { .. }
            | Action::GammaUnresolvedContinued { .. }
            | Action::ApiResultNote(_) => None, // handled by process_actions directly
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
                        if matches!(
                            rec.outcome,
                            Outcome::Win
                                | Outcome::Loss
                                | Outcome::StopLoss
                                | Outcome::Unwind
                                | Outcome::Timeout
                        ) {
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
                        let delta_pct = slot.worker.delta_pct() * 100.0;
                        self.notify(&format!(
                            "{icon} <b>{} TRADE {}</b> | {} | {} | {}\nentry={:.4} → exit={:.4} | cycle: ${:.2}→${:.2} | delta={delta_pct:+.3}% | pnl={sign}${:.4} | {}W/{}L",
                            slot.market_key(), rec.outcome.as_str(), hkt_now().format("%H:%M:%S"),
                            arrow_side(rec.side), slot.worker.strategy_name,
                            rec.token_price, rec.exit_price,
                            slot.worker.cycle_open_binance(), slot.last_binance,
                            rec.pnl.abs(), slot.wins, slot.losses
                        )).await;

                        spawn_resolution_watcher(
                            self.http.clone(),
                            rec.slug.clone(),
                            rec.side,
                            // The watcher's reply key: must be the slot's full
                            // market identity, not the bare asset — a BTC-5m and
                            // a BTC-15m reversal slot can both be Confirming at
                            // once, and (asset, strategy) alone can't tell them
                            // apart. Bare asset for 5m — unchanged.
                            slot.market_key(),
                            slot.worker.strategy_name,
                            GammaPollCadence {
                                deadline_secs: slot.params.gamma_poll_deadline_secs,
                                poll_delay_secs: slot.params.gamma_poll_delay_secs,
                                poll_interval_secs: slot.params.gamma_poll_interval_secs,
                            },
                            self.api_result_tx.clone(),
                        );
                    }
                    Action::LogTradeCorrection {
                        previous_outcome,
                        previous_pnl,
                        record,
                    } => {
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
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name,
                            previous_outcome.as_str(), record.outcome.as_str(),
                            if *previous_pnl >= 0.0 { "+" } else { "" }, previous_pnl,
                            if record.pnl >= 0.0 { "+" } else { "" }, record.pnl
                        )).await;
                    }
                    Action::StopLossVerdict {
                        record: _,
                        would_have_won,
                    } => {
                        let (icon, verdict, note) = if *would_have_won {
                            (
                                "🔴",
                                "COSTLY",
                                "market would have favored the position — stop cost money",
                            )
                        } else {
                            (
                                "🟢",
                                "GOOD",
                                "market moved against the position — stop saved money",
                            )
                        };
                        self.notify(&format!(
                            "{icon} <b>{} STOP {verdict}</b> | {} | {}\n{note}",
                            slot.market_key(),
                            hkt_now().format("%H:%M:%S"),
                            slot.worker.strategy_name
                        ))
                        .await;
                    }
                    // Loss-streak halt (halt_rev/halt_prob) — distinct from manual /halt
                    // and the balance drawdown halt, which already notify at their own
                    // call sites (Command::Halt, DrawdownHalt).
                    Action::HaltEngaged => {
                        log_control_event(slot, "halt_engaged");
                        let halt_n = match slot.worker.strategy_name {
                            "high_prob" => slot.params.halt_prob,
                            "v_shape" => slot.params.halt_v,
                            _ => slot.params.halt_rev,
                        };
                        self.notify(&format!(
                            "🟡 <b>{} HALTED</b> | {} | {}\n{halt_n} consecutive losses — new entries suppressed until the next daily reset (or /resume).",
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    Action::HaltReset => {
                        log_control_event(slot, "halt_reset");
                        self.notify(&format!(
                            "🟢 <b>{} HALT RESET</b> | {} | {}\nDaily loss-streak reset — new entries re-armed.",
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    // A Gamma correction (Loss -> Win) pulled the loss-streak count
                    // back below halt_rev/halt_prob, clearing a halt that had been
                    // engaged partly or wholly on a phantom loss — see
                    // trader/doc/incident_halt_double_count_2026-07-10.md.
                    Action::HaltClearedByCorrection => {
                        log_control_event(slot, "halt_cleared_by_correction");
                        self.notify(&format!(
                            "🟢 <b>{} HALT CLEARED</b> | {} | {}\nA Gamma correction reversed one of the counted losses — new entries re-armed.",
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name
                        )).await;
                    }
                    // Gamma never resolved within gamma_poll_deadline_secs of the position
                    // closing — halt over guess (trader/doc/incident_DOGE_wrong_result_2026-07-09.md
                    // §4): the provisional record stands as logged, unverified, and new
                    // entries are suppressed until a human checks it and /resumes.
                    Action::GammaHaltEngaged { record } => {
                        log_control_event(slot, "gamma_halt_engaged");
                        println!(
                            "[live] {} gave up waiting for Gamma resolution of {} — halting new entries ({})",
                            slot.market_key(),
                            record.slug,
                            slot.worker.strategy_name
                        );
                        self.notify(&format!(
                            "🔴 <b>{} GAMMA UNRESOLVED — HALTED</b> | {} | {}\nNo Polymarket resolution for {} within {:.0}s of cycle close — kept provisional {} (pnl {}{:.4}, unverified). New entries suppressed until /resume; please verify manually.",
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name,
                            record.slug, slot.params.gamma_poll_deadline_secs, record.outcome.as_str(),
                            if record.pnl >= 0.0 { "+" } else { "" }, record.pnl
                        )).await;
                    }
                    // Gamma never resolved, but balance is up vs. last cycle's checkpoint
                    // (2026-07-09) — kept provisional, unverified, but entries are not
                    // suppressed on account of this timeout. `entry_suppressed` is the
                    // worker's *actual current* state, since some other halt (manual
                    // /halt, loss-streak, drawdown) may still apply independently.
                    Action::GammaUnresolvedContinued {
                        record,
                        entry_suppressed,
                    } => {
                        println!(
                            "[live] {} gave up waiting for Gamma resolution of {} — balance up since last cycle's checkpoint, continuing ({})",
                            slot.market_key(),
                            record.slug,
                            slot.worker.strategy_name
                        );
                        let suffix = if *entry_suppressed {
                            " (new entries remain suppressed by a separate halt already in effect.)"
                        } else {
                            " New entries continue normally."
                        };
                        self.notify(&format!(
                            "🟠 <b>{} GAMMA UNRESOLVED — CONTINUING</b> | {} | {}\nNo Polymarket resolution for {} within {:.0}s of cycle close — kept provisional {} (pnl {}{:.4}, unverified). Balance is up since last cycle's checkpoint, so not halting.{suffix} Please verify manually.",
                            slot.market_key(), hkt_now().format("%H:%M:%S"), slot.worker.strategy_name,
                            record.slug, slot.params.gamma_poll_deadline_secs, record.outcome.as_str(),
                            if record.pnl >= 0.0 { "+" } else { "" }, record.pnl
                        )).await;
                    }
                    // Diagnostic-only — see the Action's own doc comment. No Telegram
                    // spam for "everything's fine," just a trace in live.log.
                    Action::ApiResultNote(note) => println!("[live] {note}"),
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

    // Config first: the run mode depends on the TOML's `paper_trade` key as
    // well as the CLI flags (loading it needs no credentials).
    let toml = trader::config::load_latest(&args.config_dir)?;
    let mode = resolve_run_mode(args.paper, toml.paper_trade, args.dry_run);

    // Credentials (and the env file itself) are only *required* for real
    // trading — dry-run/paper must be runnable with no wallet material at all
    // (their whole point is validation without touching the production
    // account; paper still *wants* the env for the Telegram token, so it's
    // loaded when present). Signer/funder are read later, inside the
    // engine-connect branch that actually needs them.
    if mode == RunMode::Real {
        dotenvy::from_path(&args.env_file).with_context(|| format!("load {}", args.env_file))?;
    } else if let Err(e) = dotenvy::from_path(&args.env_file) {
        println!(
            "[live] {mode:?}: env file {} not loaded ({e}) — continuing without credentials.",
            args.env_file
        );
    }

    std::fs::create_dir_all(&args.log_dir).with_context(|| format!("create {}", args.log_dir))?;

    println!(
        "[live] assets={} size_usdc=${:.2} max_trades={} log_dir={}",
        args.asset.join(","),
        args.size_usdc,
        args.max_trades,
        args.log_dir
    );
    match mode {
        RunMode::DryRun => println!(
            "[live] DRY RUN — no orders will be placed; every fill below is simulated (SimExecutionEngine)."
        ),
        RunMode::Paper => println!(
            "[live] PAPER MODE — no real orders; fills simulated from observed CLOB prices \
             (PaperExecutor). Trades → paper_trades_*.csv, real-money trading is paused for \
             this run (plan_unwind_5u_maker_2026-07-19)."
        ),
        RunMode::Real => {
            println!("[live] REAL MONEY — this will place live orders on production Polymarket.")
        }
    }

    // Route CLOB writes through the EC2 HTTP proxy when running from a geo-restricted
    // region (same var that Python's _patch_clob_proxy reads; empty = direct connect).
    // reqwest reads HTTPS_PROXY from the environment at Client::builder().build() time.
    if let Ok(proxy_url) = std::env::var("CLOB_PROXY_URL")
        && !proxy_url.is_empty()
    {
        // Safety: single-threaded at this point in main() — tokio runtime not yet
        // spawning work, and no other thread reads HTTPS_PROXY concurrently.
        unsafe { std::env::set_var("HTTPS_PROXY", &proxy_url) };
        println!("[live] routing CLOB writes via proxy: {proxy_url}");
    }

    // Telegram control plane (optional — runs without it if unconfigured, same
    // as the discovery-mode fallback in telegram/mod.rs::AuthConfig). Exactly
    // one poller for the whole process — see module doc comment for why.
    let telegram_auth = match (
        std::env::var("TELEGRAM_BOT_TOKEN"),
        std::env::var("TELEGRAM_CHAT_ID"),
    ) {
        (Ok(token), Ok(raw_chat_id)) => {
            let chat_id: i64 = raw_chat_id
                .parse()
                .context("TELEGRAM_CHAT_ID must be an integer")?;
            println!("[live] Telegram control enabled (chat_id={chat_id}).");
            Some(AuthConfig {
                token,
                chat_id,
                user_id: 0,
            })
        }
        _ => {
            println!(
                "[live] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — Telegram control disabled."
            );
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
    // Clob client needed for direct Poly WS subscriptions: always in non-NATS
    // mode, and — regardless of NATS — whenever any non-5m market is
    // configured, since price_feed only publishes the 5m market's poly ticks
    // (trader/doc/feature_new_markets_2026-07-17.md §5.3).
    let any_non_5m = args
        .asset
        .iter()
        .any(|a| toml.durations_for(a).iter().any(|d| d != "5m"));
    let clob = if args.nats_url.is_none() || any_non_5m {
        Some(clob_client())
    } else {
        None
    };

    // binance/poly channels are shared across assets, each tick tagged with its asset
    // name — tokio::select! can't cleanly select over a dynamic set.
    // Third element: the tick's own exchange timestamp (seconds since epoch,
    // from Polymarket CLOB's/Binance's own event time), when the source
    // provided one — `None` on the direct-WS (non-NATS) path, which has no
    // price_feed hop to have captured it.
    let (binance_tx, mut binance_rx) =
        mpsc::unbounded_channel::<(String, BinanceTick, Option<f64>)>();
    let (poly_tx, mut poly_rx) = mpsc::unbounded_channel::<(String, PolyTick, Option<f64>)>();
    // (asset, strategy, Some(won)/None-on-timeout) handoff from background
    // Gamma-resolution watchers (spawned per closed trade) back into the
    // single-threaded step() loop.
    let (api_result_tx, mut api_result_rx) =
        mpsc::unbounded_channel::<(String, &'static str, Option<bool>)>();
    // Indicator snapshots from the standalone indicator module (feature_vol_
    // 2026-07-18.md). Channel exists unconditionally so the select! arm below
    // always compiles; senders are only spawned when indicator_enabled.
    let (indicator_tx, mut indicator_rx) = mpsc::unbounded_channel::<IndicatorSnapshot>();

    // One `AssetSlot` per (asset, strategy) pair — strategy list always comes from
    // the shared TOML's `AssetParams.strategies` (e.g. ETH -> [high_prob, reversal]),
    // never a CLI flag, so this can't silently drift from the config the Python bot
    // reads. A dual-strategy asset gets two independent slots, each with its own
    // position/win-loss state, matching Python's per-asset Worker holding a list of
    // strategy objects that each fire/track independently.
    let mut assets: Vec<AssetSlot> = Vec::new();
    // Direct-WS mode only: one Binance connection per unique *asset*, shared by
    // every (strategy, duration) slot of that asset — previously this spawned
    // one per (asset, strategy), the dormant duplication noted in README's TODO
    // (production always uses NATS, which never hits this path). With durations
    // multiplying the slot count, deduplicating here keeps direct-WS mode's
    // connection count flat instead of × strategies × durations.
    let mut binance_spawned: std::collections::HashSet<String> = std::collections::HashSet::new();
    for asset in &args.asset {
        for dur_label in toml.durations_for(asset) {
            let Some(duration) = MarketDuration::parse(&dur_label) else {
                anyhow::bail!(
                    "unknown market duration `{dur_label}` for asset {asset} in [market_durations] \
                     (valid: 5m, 15m, 1h-et, 4h — note the slot-based `1h` market does not exist \
                     on Polymarket; the 60-minute family is `1h-et`)"
                );
            };
            if duration == MarketDuration::HourlyEt && hourly_et_coin_name(asset).is_none() {
                anyhow::bail!(
                    "asset {asset} has no known hourly-ET coin name (see \
                     marketdata::hourly_et_coin_name) — cannot trade 1h-et"
                );
            }
            // For "5m" this is exactly the pre-duration `toml.resolve(asset)`.
            let mut params = toml.resolve_for_duration(asset, &dur_label)?;
            params.trade_size_usdc = args.size_usdc;
            let max_buy_price = params.max_buy_price;
            if params.strategies.is_empty() {
                anyhow::bail!(
                    "no strategies configured for asset {asset} (missing both a `{asset}` and `default` entry in the config's [strategies] table)"
                );
            }

            for strategy in &params.strategies {
                let mut worker = match strategy.as_str() {
                    "reversal" => Worker::new_reversal(asset, &params),
                    "high_prob" => Worker::new_high_prob(asset, &params),
                    "v_shape" => Worker::new_v_shape(asset, &params),
                    other => {
                        anyhow::bail!("unknown strategy `{other}` for asset {asset} (from config)")
                    }
                };
                let lower = asset.to_lowercase();
                // 5m keeps the exact legacy names; other durations get their own
                // suffix-tagged files (fresh, collision-free — nothing migrated).
                let file_tag = if duration.is_5m() {
                    String::new()
                } else {
                    format!("_{}", duration.suffix())
                };
                // Paper runs get fully separate files: analytics depend on
                // live_trades_*.csv meaning real money, and a paper run must
                // neither read nor pollute the real run's halt/stats state
                // (plan_unwind_5u_maker_2026-07-19 §2.1).
                let file_prefix = if mode == RunMode::Paper {
                    "paper"
                } else {
                    "live"
                };
                let log_path = format!(
                    "{}/{file_prefix}_trades_{lower}_{strategy}{file_tag}.csv",
                    args.log_dir
                );
                let state_file = format!(
                    "{}/{file_prefix}_state_{lower}_{strategy}{file_tag}.json",
                    args.log_dir
                );
                let canceled_quote_log_path = format!(
                    "{}/{file_prefix}_quotes_{lower}_{strategy}{file_tag}.csv",
                    args.log_dir
                );
                let pup_veto_log_path = format!(
                    "{}/{file_prefix}_pup_vetoes_{lower}_{strategy}{file_tag}.csv",
                    args.log_dir
                );
                append_csv_header_if_new(&log_path)?;
                append_canceled_quote_header_if_new(&canceled_quote_log_path)?;
                append_pup_veto_header_if_new(&pup_veto_log_path)?;

                // Restore halt state + /status counters from before the last
                // restart (position/cycle state is deliberately NOT restored here
                // — see README's "Restart behavior" section). `halt_max`/
                // `halt_reset_hour` always come from the config just loaded above,
                // never from this file, so a config change takes effect immediately.
                let stats = match load_persisted_slot(&state_file) {
                    Some(persisted) => {
                        worker.restore_halt(
                            persisted.worker.entry_suppressed,
                            persisted.worker.halt_losses,
                            persisted.worker.halt_last_session,
                        );
                        persisted.stats
                    }
                    None => PersistedStats::default(),
                };

                if args.nats_url.is_none() && binance_spawned.insert(asset.clone()) {
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
                    duration,
                    slot_val: 0,
                    up_id: U256::from(0u64),
                    dn_id: U256::from(0u64),
                    current_token_id: None,
                    max_buy_price,
                    log_path,
                    state_file,
                    canceled_quote_log_path,
                    pup_veto_log_path,
                    control_log_path: format!(
                        "{}/{}control_log.jsonl",
                        args.log_dir,
                        // Separate file so trade_reconcile.py's halt-timeline
                        // reconstruction never mixes paper events into the
                        // real-money timeline.
                        if mode == RunMode::Paper { "paper_" } else { "" }
                    ),
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
                // A restart-restored halt (or lack of one) is otherwise invisible
                // to control_log.jsonl — the previous process's own halt/resume/
                // reset_losses events are already in the log, but nothing marks
                // *this* process picking that state back up. Log a snapshot so
                // trade_reconcile.py's timeline reconstruction has a starting
                // point right after every restart, not just gaps between them.
                log_control_event(assets.last().unwrap(), "startup");
            }
        }
    }

    for slot in &assets {
        println!(
            "[live]   {} -> strategy={}",
            slot.market_key(),
            slot.worker.strategy_name
        );
    }

    // NATS path: subscribe to price.binance.<ASSET> and price.poly.<ASSET> for each
    // asset, forwarding into the same channels the direct-WS path uses.
    // Set up before engine.connect() so ticks flow and can be verified independently.
    if let Some(ref url) = args.nats_url {
        let nc = async_nats::connect(url)
            .await
            .with_context(|| format!("connect to NATS at {url}"))?;
        println!("[live] NATS price source: {url}");
        for asset in &args.asset {
            let mut sub = nc
                .subscribe(format!("price.binance.{asset}"))
                .await
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

            let mut sub = nc
                .subscribe(format!("price.poly.{asset}"))
                .await
                .context("NATS poly subscribe")?;
            let out = poly_tx.clone();
            let a = asset.clone();
            tokio::spawn(async move {
                let mut n: u64 = 0;
                while let Some(msg) = sub.next().await {
                    if let Ok(tick) = serde_json::from_slice::<PolyTick>(&msg.payload) {
                        n += 1;
                        if n == 1 {
                            println!(
                                "[NATS] first poly tick for {a}: up={:.4} dn={:.4}",
                                tick.up, tick.dn
                            );
                        }
                        let server_ts = extract_server_ts(&msg.payload);
                        if out.send((a.clone(), tick, server_ts)).is_err() {
                            return;
                        }
                    }
                }
            });

            // Indicator module consumption (feature_vol_2026-07-18.md, phase 1:
            // log-only). Malformed payloads are dropped silently — the subject
            // is additive, never load-bearing.
            if toml.indicator_enabled {
                let mut sub = nc
                    .subscribe(format!("indicator.{asset}"))
                    .await
                    .context("NATS indicator subscribe")?;
                let out = indicator_tx.clone();
                let a = asset.clone();
                tokio::spawn(async move {
                    let mut n: u64 = 0;
                    while let Some(msg) = sub.next().await {
                        if let Some(snap) = IndicatorSnapshot::parse(&msg.payload) {
                            n += 1;
                            if n == 1 {
                                println!(
                                    "[NATS] first indicator snapshot for {a}: {}",
                                    snap.render()
                                );
                            }
                            if out.send(snap).is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        }
        println!("[live] NATS subscriptions active — connecting to execution engine…");
    }

    let live_config = LiveConfig {
        order_max_retries: toml.order_max_retries,
        ..LiveConfig::default()
    };
    let live_engine: Option<Arc<LiveExecutionEngine<Signer>>> = if mode != RunMode::Real {
        None
    } else {
        let key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
        let signer = local_signer_from_key(&key)?;
        let funder_raw =
            std::env::var("FUND_ADDRESS").unwrap_or_else(|_| DEFAULT_FUND_ADDRESS.to_string());
        let funder = Address::from_str(&funder_raw)?;
        let signature_type = signature_type_from_env()?;
        let engine =
            LiveExecutionEngine::connect(CLOB_HOST, signer, funder, signature_type, live_config)
                .await?;

        // Real-time USER-channel fill logger (diagnostic sidecar — doesn't feed
        // back into trading decisions). Subscribes to all markets for this
        // account (empty `markets` list — see unwind.rs's `run()` doc comment).
        // No account, no fills to log — skipped entirely in dry-run.
        {
            let watcher = trader::unwind::UnwindWatcher::new();
            let credentials = engine.credentials();
            tokio::spawn(async move {
                if let Err(e) = watcher
                    .run(UNWIND_WS_HOST, credentials, funder, vec![])
                    .await
                {
                    eprintln!("[unwind] watcher exited: {e:#}");
                }
            });
        }
        Some(Arc::new(engine))
    };
    // Paper engine kept as a concrete handle too (like `live_engine`): the
    // driver's poly-tick arm feeds it observed prices and drains resting-order
    // fills — trait objects can't expose that.
    let paper_engine: Option<Arc<PaperExecutor>> = if mode == RunMode::Paper {
        Some(Arc::new(PaperExecutor::new()))
    } else {
        None
    };
    // The trait-object handle order execution goes through everywhere — the
    // live engine, the paper engine, or the dry-run sim. `Arc` because the
    // weather tasks (if enabled) need a 'static clone; the Driver just
    // borrows it.
    let exec_engine: Arc<dyn ExecutionEngine> = match (&live_engine, &paper_engine) {
        (Some(e), _) => e.clone(),
        (None, Some(p)) => p.clone(),
        (None, None) => Arc::new(SimExecutionEngine::new()),
    };

    let driver = Driver {
        engine: exec_engine.as_ref(),
        live_engine: live_engine.as_deref(),
        mode,
        telegram: telegram_send.clone(),
        http: http.clone(),
        api_result_tx: api_result_tx.clone(),
    };

    // ── Weather markets (Phase B, feature_new_markets_2026-07-17.md §7) ──
    // Inert without --weather-config: no config, no tasks, no subscriptions.
    let (weather_tx, mut weather_rx) = mpsc::unbounded_channel::<trader::weather::WeatherTrade>();
    let weather_log_path = format!("{}/live_trades_weather.csv", args.log_dir);
    if let Some(ref path) = args.weather_config {
        let wcfg = trader::weather::load_weather_config(path)?;
        append_weather_csv_header_if_new(&weather_log_path)?;
        println!(
            "[live] weather enabled: {} cities ({}), band {:.2}/{:.2}, size=${:.2}{}",
            wcfg.cities.len(),
            wcfg.cities.join(","),
            wcfg.low,
            wcfg.high,
            wcfg.trade_size_usdc,
            if mode == RunMode::Real { "" } else { " [SIM]" }
        );
        let sinks = Arc::new(trader::weather::WeatherSinks {
            engine: exec_engine.clone(),
            http: http.clone(),
            trade_tx: weather_tx.clone(),
            dry_run: mode != RunMode::Real,
        });
        for city in &wcfg.cities {
            tokio::spawn(trader::weather::run_city(
                city.clone(),
                wcfg.clone(),
                sinks.clone(),
            ));
        }
    }
    // Original sender dropped here: with no weather tasks the channel closes
    // immediately and the select arm below goes permanently quiet; with tasks,
    // their `WeatherSinks` clone keeps it open.
    drop(weather_tx);
    let asset_strategy_summary = assets
        .iter()
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
    // Rolling cycle-over-cycle balance comparison — feeds both the Gamma-unresolved-
    // timeout override (2026-07-09) and the scoped per-cycle balance-decrease halt
    // (2026-07-11, trader/doc/plan_gammapi_2026-07-11.md) — fed from the same periodic
    // fetch as `balance_guard` below, no extra API calls. See `GammaBalanceTracker`'s
    // own doc comment.
    let gamma_balance = GammaBalanceTracker::new();
    let mut balance_deadline = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs_f64(seconds_until_next_check(
            now_secs_f64(),
            args.balance_check_offset_secs,
            args.period_secs,
        ));

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Direct-WS poly subscriptions for non-5m markets: one per (asset,
    // duration) market — keyed by `market_key()` — shared by every strategy
    // slot trading it, and replaced (old task aborted via PolySub's Drop) at
    // that market's own cycle boundary. 5m slots never appear here: they use
    // NATS (production) or their own per-slot `slot.poly_sub` (direct-WS
    // mode, unchanged).
    let mut poly_subs: std::collections::HashMap<String, PolySub> =
        std::collections::HashMap::new();

    // Latest indicator snapshot per asset — single-owner inside this loop, so
    // no locking. Stale entries read as absent via `fresh()`.
    let mut indicator_store = IndicatorStore::default();

    loop {
        tokio::select! {
            Some(snap) = indicator_rx.recv() => {
                // p(up) negative-edge gate (plan_unwind_5u_maker_2026-07-19
                // §2.3): only a *ready* p_up reading reaches the worker — a
                // warmup snapshot with no p_up key sends nothing, which is
                // indistinguishable from "no snapshot" for the gate's
                // fail-open check (Worker::try_enter). Extract before the
                // store consumes `snap` by value.
                if let Some(&p_up) = snap.vals.get("p_up") {
                    for slot in assets.iter_mut().filter(|s| s.worker.asset == snap.asset) {
                        let _ = slot.worker.step(Event::IndicatorUpdate { p_up, ts: snap.ts });
                    }
                }
                indicator_store.update(snap);
            }

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

            Some((market, tick, server_ts)) = poly_rx.recv() => {
                // Paper mode: record the newly observed prices and deliver any
                // resting-order fills *before* the tick itself steps the
                // workers, so a fill and the tick that caused it arrive in a
                // deterministic order (fill first — a quote the price traded
                // through cannot also be cancelled by the same tick).
                if let Some(paper) = &paper_engine {
                    let tokens = assets
                        .iter()
                        .find(|s| s.market_key() == market && s.current_slug.is_some())
                        .map(|s| (s.up_id, s.dn_id));
                    if let Some((up_id, dn_id)) = tokens {
                        let mut fills = paper.on_price(up_id, tick.up);
                        fills.extend(paper.on_price(dn_id, tick.dn));
                        for fill in fills {
                            // Route by order id: several slots can share the same
                            // market/tokens (multi-strategy assets), but each
                            // order id has exactly one owner. Which accessor to
                            // check depends on the fill's side — an exit SELL
                            // owner is `Holding{GtcResting}`, an entry BUY
                            // owner is `EnteringMaker` (§2.2).
                            let owner_idx = assets.iter().position(|s| match fill.side {
                                PaperOrderSide::Sell => {
                                    s.worker.exit_resting_order_id() == Some(fill.order_id.as_str())
                                }
                                PaperOrderSide::Buy => {
                                    s.worker.entry_resting_order_id()
                                        == Some(fill.order_id.as_str())
                                }
                            });
                            let Some(idx) = owner_idx else {
                                println!("[PAPER-FILL] {market} {} filled but no owning slot (already cancelled?) — dropped", fill.order_id);
                                continue;
                            };
                            let slot = &mut assets[idx];
                            println!(
                                "[PAPER-FILL] {market} resting {:?} {} filled {:.2} @ {:.4}",
                                fill.side, fill.order_id, fill.shares, fill.price
                            );
                            let event = match fill.side {
                                PaperOrderSide::Sell => Event::UnwindFilled {
                                    sold_shares: fill.shares,
                                    exit_price: fill.price,
                                    // Tick-driven maker fill — no order round
                                    // trip to measure.
                                    signal_latency_ms: 0.0,
                                    process_latency_ms: 0.0,
                                },
                                PaperOrderSide::Buy => Event::EntryQuoteFilled {
                                    filled_shares: fill.shares,
                                    cost: fill.price,
                                    signal_latency_ms: 0.0,
                                    process_latency_ms: 0.0,
                                },
                            };
                            let actions = slot.worker.step(event);
                            driver.process_actions(slot, actions, Feed::Clob).await;
                        }
                    }
                }
                // Poly ticks are market-scoped, not asset-scoped: a BTC-5m and a
                // BTC-15m market have different CLOB tokens, so their prices must
                // never cross-feed. The routing key is `market_key()` — the bare
                // asset for 5m (NATS publishes it that way; unchanged), and
                // "ASSET@dur" for non-5m direct subscriptions. (Binance below
                // stays asset-scoped on purpose: one reference price feeds every
                // duration of that asset.)
                for slot in assets.iter_mut().filter(|s| s.market_key() == market) {
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
                let now = now_secs_f64();
                for slot in &assets {
                    if let Some(slug) = &slot.current_slug {
                        // Per-slot time-left: durations rotate on different
                        // clocks, so there is no single process-wide T-value.
                        // For a 5m slot this equals the old global
                        // `current_slot_val + period - now` exactly
                        // (cycle_end_ts is set to slot+period at CycleOpen).
                        let time_left = slot.worker.cycle_end_ts() - now;
                        // Phase-1 indicator visibility: appended to the existing
                        // heartbeat rather than a separate line so the live log
                        // stays greppable per slot. Empty string when disabled,
                        // stale, or not yet received.
                        let ind = indicator_store
                            .fresh(&slot.worker.asset, now, toml.indicator_max_age_secs)
                            .map(|s| format!(" ind[{}]", s.render()))
                            .unwrap_or_default();
                        println!("[live] heartbeat {} ({}) slug={slug} T-{time_left:.0}s binance={:.4} up={:.4} dn={:.4}{ind}",
                            slot.market_key(), slot.worker.strategy_name, slot.last_binance, slot.last_poly_up, slot.last_poly_dn);
                    }
                }
            }

            _ = ticker.tick() => {
                // Per-tick guard: several strategy slots can share one non-5m
                // market; its direct WS subscription is refreshed once per
                // boundary, not once per slot.
                let mut resubscribed: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                // Boundary detection is per slot (durations rotate on different
                // clocks). For a 5m-only config every slot's boundary fires at
                // exactly the instants the old single global `current_slot_val`
                // check did — same `current_slot(300)` value, same 1s ticker.
                for slot in assets.iter_mut() {
                    let period = slot.duration.period_secs();
                    let slot_now = current_slot(period);
                    if slot_now == slot.slot_val {
                        continue;
                    }
                    let is_first_tick = slot.slot_val == 0;
                    slot.slot_val = slot_now;
                    let elapsed_into_slot = now_secs_f64() - slot_now as f64;

                    if should_suppress_startup_cycle(is_first_tick, elapsed_into_slot) {
                        println!(
                            "[live] startup landed {elapsed_into_slot:.0}s into an already-open {} cycle (slot={slot_now}) — suppressing entries until the next clean cycle boundary (trader/doc/fix_live_deploy_2026-07-15.md)",
                            slot.market_key()
                        );
                        continue;
                    }

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

                    let slug = slot.duration.slug(&asset, slot_now);
                    match fetch_meta(&http, &slug).await {
                        Ok((u, d)) => {
                            slot.up_id = u;
                            slot.dn_id = d;

                            if slot.duration.is_5m() {
                                // 5m, direct-WS mode only: per-slot subscription,
                                // routing key = bare asset — exactly the
                                // pre-duration behavior. In the NATS path the
                                // subscription is already running from startup
                                // (price_feed publishes price.poly.<ASSET>).
                                // `args.nats_url.is_none()` must be checked
                                // explicitly now that `clob` can exist in NATS
                                // mode too (for non-5m markets).
                                if args.nats_url.is_none() && let Some(ref clob) = clob {
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
                            } else {
                                // Non-5m: always a direct WS subscription (NATS
                                // never carries these markets), one per (asset,
                                // duration), routing key = "ASSET@dur" so these
                                // ticks can never reach a 5m worker or vice versa.
                                let key = slot.market_key();
                                if resubscribed.insert(key.clone()) {
                                    let clob = clob.as_ref().expect(
                                        "clob client is always constructed when non-5m markets are configured",
                                    );
                                    let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<PolyTick>();
                                    poly_subs.insert(key.clone(), PolySub::start(clob, u, raw_tx));
                                    let out = poly_tx.clone();
                                    tokio::spawn(async move {
                                        while let Some(tick) = raw_rx.recv().await {
                                            if out.send((key.clone(), tick, None)).is_err() {
                                                return;
                                            }
                                        }
                                    });
                                }
                            }

                            let ctx = CycleContext {
                                start_ts: slot_now as f64, end_ts: (slot_now + period) as f64, open_binance: slot.last_binance,
                            };
                            println!("[live] new cycle {} ({}) slug={slug} open_binance={}", slot.market_key(), slot.worker.strategy_name, slot.last_binance);
                            let actions = slot.worker.step(Event::CycleOpen { ctx, slug: slug.clone() });
                            // CycleOpen never produces PlaceBuy/ClosePosition either — see note above.
                            driver.process_actions(slot, actions, Feed::Clob).await;
                            slot.current_slug = Some(slug);
                        }
                        Err(e) => eprintln!("[live] meta fetch failed for {} {slug}: {e:#}", slot.market_key()),
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
                    Command::Halt { asset, strategy: _ } if asset.is_empty() => {
                        for slot in assets.iter_mut() {
                            apply_control(slot, ControlEvent::Halt);
                        }
                        Some(format!("🛑 Halted all assets ({}) — new entries suppressed, open positions still managed.", args.asset.join(", ")))
                    }
                    Command::Resume { asset, strategy: _ } if asset.is_empty() => {
                        for slot in assets.iter_mut() {
                            apply_control(slot, ControlEvent::Resume);
                        }
                        balance_guard.reset_baseline();
                        let mut msg = format!("▶️ Resumed all assets ({}).", args.asset.join(", "));
                        if let Some(note) = loss_streak_still_halted_note(assets.iter()) {
                            msg.push_str(&note);
                        }
                        Some(msg)
                    }
                    Command::ResetLosses { asset } if asset.is_empty() => {
                        for slot in assets.iter_mut() {
                            apply_control(slot, ControlEvent::ResetLosses);
                        }
                        Some(format!("🔄 Reset loss-streak halt counter for all assets ({}).", args.asset.join(", ")))
                    }
                    Command::ResetLosses { asset } => {
                        let matched: Vec<&mut AssetSlot> = assets.iter_mut()
                            .filter(|s| s.worker.asset.eq_ignore_ascii_case(&asset) || s.market_key().eq_ignore_ascii_case(&asset))
                            .collect();
                        if matched.is_empty() {
                            Some(format!("this driver doesn't manage {asset} — trading {}", args.asset.join(", ")))
                        } else {
                            for slot in matched {
                                apply_control(slot, ControlEvent::ResetLosses);
                            }
                            Some(format!("🔄 Reset loss-streak halt counter for {asset}."))
                        }
                    }
                    // A named asset may own more than one strategy slot (e.g. ETH:
                    // high_prob + reversal). With no strategy given, halt/resume both
                    // together, matching Python's manual per-asset halt lighting both
                    // indicators; with a strategy given, scope to that slot only.
                    Command::Halt { asset, strategy } => {
                        let matched: Vec<&mut AssetSlot> = assets.iter_mut()
                            .filter(|s| (s.worker.asset.eq_ignore_ascii_case(&asset) || s.market_key().eq_ignore_ascii_case(&asset))
                                && match &strategy {
                                    Some(st) => s.worker.strategy_name.eq_ignore_ascii_case(st),
                                    None => true,
                                })
                            .collect();
                        if matched.is_empty() {
                            Some(format!("this driver doesn't manage {asset}{} — trading {}",
                                strategy.as_deref().map(|s| format!("/{s}")).unwrap_or_default(),
                                args.asset.join(", ")))
                        } else {
                            for slot in matched {
                                apply_control(slot, ControlEvent::Halt);
                            }
                            let label = match &strategy {
                                Some(s) => format!("{asset}/{s}"),
                                None => asset.clone(),
                            };
                            Some(format!("🛑 Halted {label} — new entries suppressed, open positions still managed."))
                        }
                    }
                    Command::Resume { asset, strategy } => {
                        let matched: Vec<&mut AssetSlot> = assets.iter_mut()
                            .filter(|s| (s.worker.asset.eq_ignore_ascii_case(&asset) || s.market_key().eq_ignore_ascii_case(&asset))
                                && match &strategy {
                                    Some(st) => s.worker.strategy_name.eq_ignore_ascii_case(st),
                                    None => true,
                                })
                            .collect();
                        if matched.is_empty() {
                            Some(format!("this driver doesn't manage {asset}{} — trading {}",
                                strategy.as_deref().map(|s| format!("/{s}")).unwrap_or_default(),
                                args.asset.join(", ")))
                        } else {
                            for slot in matched {
                                apply_control(slot, ControlEvent::Resume);
                            }
                            balance_guard.reset_baseline();
                            let label = match &strategy {
                                Some(s) => format!("{asset}/{s}"),
                                None => asset.clone(),
                            };
                            let mut msg = format!("▶️ Resumed {label}.");
                            let scoped = assets.iter().filter(|s| {
                                (s.worker.asset.eq_ignore_ascii_case(&asset) || s.market_key().eq_ignore_ascii_case(&asset))
                                    && match &strategy {
                                        Some(st) => s.worker.strategy_name.eq_ignore_ascii_case(st),
                                        None => true,
                                    }
                            });
                            if let Some(note) = loss_streak_still_halted_note(scoped) {
                                msg.push_str(&note);
                            }
                            Some(msg)
                        }
                    }
                    Command::Invalid(msg) => Some(msg),
                    _ => Some("not supported by this Rust live driver yet.".to_string()),
                };
                if let Some(text) = reply {
                    driver.notify(&text).await;
                }
            }

            Some(trade) = weather_rx.recv() => {
                if let Err(e) = log_weather_trade(&weather_log_path, &trade) {
                    eprintln!("[weather] trade log error: {e:#}");
                }
                let icon = if trade.pnl >= 0.0 { "✅" } else { "❌" };
                let sign = if trade.pnl >= 0.0 { "+" } else { "-" };
                driver.notify(&format!(
                    "{icon} <b>WEATHER {} {}</b> | {} | {}\nentry={:.4} → exit={:.4} | {:.4} shares | pnl={sign}${:.4}",
                    trade.city, trade.outcome, hkt_now().format("%H:%M:%S"), trade.bucket,
                    trade.entry_price, trade.exit_price, trade.shares, trade.pnl.abs()
                )).await;
            }

            Some((market, strategy, result)) = api_result_rx.recv() => {
                // `market` is the slot's `market_key()` (bare asset for 5m) —
                // see the spawn_resolution_watcher call site.
                if let Some(slot) = assets.iter_mut().find(|s| s.market_key() == market && s.worker.strategy_name == strategy) {
                    let event = match result {
                        Some(won) => Event::ApiResult { won },
                        // Unknown/failed verdict (`increased()` returns `None`) fails
                        // safe to "still halt" — see `GammaBalanceTracker`'s own doc
                        // comment.
                        None => Event::ApiResultTimeout {
                            balance_increased: gamma_balance.increased().unwrap_or(false),
                        },
                    };
                    let actions = slot.worker.step(event);
                    // ApiResult(Timeout) never produces PlaceBuy/ClosePosition either — see note above.
                    driver.process_actions(slot, actions, Feed::Clob).await;
                }
            }

            _ = tokio::time::sleep_until(balance_deadline) => {
                // Dry-run has no account: `bal` stays None, which both trackers
                // treat as "unknown ⇒ don't act" (see balance.rs) — the whole
                // arm is inert on paper runs.
                let bal = match &live_engine {
                    Some(engine) => engine.fetch_balance().await,
                    None => None,
                };
                gamma_balance.record(bal);

                // Scoped halt (2026-07-11, trader/doc/plan_gammapi_2026-07-11.md): balance
                // dropped vs. last cycle's checkpoint while one or more asset+strategy slots
                // still have a trade pending Gamma confirmation — halt only those, leave
                // everything else trading. `increased() == Some(false)` requires two real
                // samples to compare; `None` (startup, or a failed fetch) fails safe by not
                // triggering this, same "unknown ⇒ don't act" convention as the Gamma-timeout
                // override below.
                if gamma_balance.increased() == Some(false) {
                    let halted = confirming_asset_labels(
                        assets.iter().map(|s| (s.market_key(), &s.worker)),
                    );
                    if !halted.is_empty() {
                        println!(
                            "[live] balance decreased vs last cycle's checkpoint — halting new entries on: {}",
                            halted.join(", ")
                        );
                        for slot in assets.iter_mut().filter(|s| s.worker.is_confirming()) {
                            apply_balance_halt(slot);
                        }
                        driver.notify(&format!(
                            "🟡 <b>BALANCE DECREASED — SCOPED HALT</b>\nBalance dropped vs last cycle's checkpoint while {} still had a trade pending Gamma confirmation — halted new entries there only; other assets/strategies continue. Send /resume to re-arm.",
                            halted.join(", ")
                        )).await;
                    }
                }

                // Coarse backstop, unchanged: a broad/systemic drawdown from the session-start
                // baseline still halts everything, regardless of the scoped check above.
                if balance_guard.check(bal) {
                    println!("[live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.");
                    for slot in assets.iter_mut() {
                        apply_balance_halt(slot);
                    }
                    driver.notify(&format!(
                        "🛑 balance drawdown >25% from session baseline — halted new entries on all assets ({}). Send /resume to re-arm.",
                        args.asset.join(", ")
                    )).await;
                }
                balance_deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_secs_f64(seconds_until_next_check(
                        now_secs_f64(),
                        args.balance_check_offset_secs,
                        args.period_secs,
                    ));
            }
        }
    }
}

#[cfg(test)]
mod run_mode_tests {
    use super::*;

    /// Plan §2.4 config-parsing case: `--paper` flag precedence. Paper from
    /// either source (flag or TOML) always wins, including over `--dry-run` —
    /// a paper-configured run must never silently degrade to the instant-fill
    /// sim.
    #[test]
    fn paper_from_either_source_beats_everything() {
        assert_eq!(resolve_run_mode(true, false, false), RunMode::Paper);
        assert_eq!(resolve_run_mode(false, true, false), RunMode::Paper);
        assert_eq!(resolve_run_mode(true, true, false), RunMode::Paper);
        assert_eq!(resolve_run_mode(true, false, true), RunMode::Paper);
        assert_eq!(resolve_run_mode(false, true, true), RunMode::Paper);
    }

    #[test]
    fn dry_run_without_paper_is_dry_run() {
        assert_eq!(resolve_run_mode(false, false, true), RunMode::DryRun);
    }

    #[test]
    fn default_is_real_money() {
        assert_eq!(resolve_run_mode(false, false, false), RunMode::Real);
    }

    /// Every non-real mode gets a distinct, non-empty prefix so a simulated
    /// run's Telegram messages can never be misread as production output —
    /// real money gets none at all.
    #[test]
    fn prefixes_distinguish_every_run_mode() {
        assert_eq!(RunMode::Real.prefix(), "");
        assert_eq!(RunMode::DryRun.prefix(), "[DRY] ");
        assert_eq!(RunMode::Paper.prefix(), "[PAPER] ");
    }
}

#[cfg(test)]
mod restart_guard_tests {
    use super::*;

    /// Clean start right at (or within a tick of) a real boundary — the
    /// common case, e.g. a process that happens to start exactly when a new
    /// 5-minute slot begins. Must NOT suppress.
    #[test]
    fn clean_start_at_boundary_not_suppressed() {
        assert!(!should_suppress_startup_cycle(true, 0.3));
    }

    /// The actual 2026-07-15 incident shape: a restart lands 100s into an
    /// already-open cycle. Must suppress — this is the bug this fix closes.
    #[test]
    fn restart_100s_into_cycle_is_suppressed() {
        assert!(should_suppress_startup_cycle(true, 100.0));
    }

    /// Exactly at the threshold is NOT suppressed (`>`, not `>=`) — documents
    /// the boundary choice so a future edit that flips the comparison
    /// direction fails a test instead of silently changing behavior.
    #[test]
    fn exactly_at_threshold_not_suppressed() {
        assert!(!should_suppress_startup_cycle(
            true,
            STARTUP_MID_CYCLE_GUARD_SECS
        ));
        assert!(should_suppress_startup_cycle(
            true,
            STARTUP_MID_CYCLE_GUARD_SECS + 0.001
        ));
    }

    /// The guard only ever applies to the very first tick after process
    /// start. A steady-state boundary crossing that happens to be
    /// late/delayed (e.g. a slow tick under load) must never suppress —
    /// suppressing here would silently skip a real cycle mid-run, a much
    /// worse regression than the bug being fixed.
    #[test]
    fn late_steady_state_tick_not_suppressed() {
        assert!(!should_suppress_startup_cycle(false, 100.0));
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
        assert_eq!(
            lines[1], "1.0,old-slug,high_prob,UP,1.0,0.93,1.0,WIN,0.0753,,,,,,",
            "9-field legacy row padded to 15 fields"
        );
        assert_eq!(
            lines[2], "2.0,new-slug,reversal,UP,2.0,0.66,1.0,WIN,0.5152,284,no market price,,,,",
            "11-field legacy row padded to 15 fields"
        );
        for line in &lines {
            assert_eq!(
                line.matches(',').count(),
                14,
                "every row must have 15 fields: {line}"
            );
        }
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod persisted_slot_tests {
    use super::*;
    use trader::worker::PersistedWorkerState;

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "poly_rust_test_state_{name}_{}.json",
            std::process::id()
        ))
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
        assert_eq!(
            loaded.worker.halt_last_session,
            snap.worker.halt_last_session
        );
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
        assert!(
            (signal_latency_ms - 1.0).abs() < 1e-9,
            "got {signal_latency_ms}"
        );
        assert!(
            (process_latency_ms - 1751.0).abs() < 1e-9,
            "got {process_latency_ms}"
        );
        assert!(process_latency_ms > latency_ms(received_ts, confirmed_ts));
    }
}

#[cfg(test)]
mod balance_halt_scope_tests {
    use super::*;

    fn test_params(asset: &str) -> AssetParams {
        AssetParams {
            asset: asset.to_string(),
            strategies: vec!["reversal".to_string()],
            enter_when_time_left: 20.0,
            no_enter_when_time_left: 10.0,
            reversal: 0.60,
            reversal_low_threshold: 0.20,
            reversal_start_time: 120.0,
            gamma_poll_delay_secs: 60.0,
            gamma_poll_interval_secs: 20.0,
            gamma_poll_deadline_secs: 600.0,
            price_high_rev: 0.90,
            delta_pct_rev: 0.0008,
            sl_reversal: 0.0,
            unwind_pnl_rev: 0.03,
            sl_pnl_rev: 0.20,
            unwind_time_rev: 0.0,
            price_low: 0.80,
            price_high: 0.93,
            delta_pct_hp: 0.0004,
            sl_high_prob: 0.49,
            unwind_pnl_hp: 0.05,
            sl_pnl_hp: 0.25,
            unwind_time_hp: 0.0,
            v_high1: 0.70,
            v_low: 0.30,
            v_high2: 0.70,
            delta_pct_v: 0.0,
            sl_v_shape: 0.0,
            sl_pnl_v: 0.30,
            unwind_pnl_v: 0.15,
            unwind_time_v: 25.0,
            halt_rev: 2,
            halt_prob: 2,
            halt_v: 1,
            halt_reset_hour_rev: 2,
            halt_reset_hour_hp: 8,
            halt_reset_hour_v: 2,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 300.0,
            trade_size_usdc: 1.0,
            maker_entry: false,
            pup_edge_min_rev: None,
        }
    }

    fn ctx(start: f64) -> CycleContext {
        CycleContext {
            start_ts: start,
            end_ts: start + 300.0,
            open_binance: 60_000.0,
        }
    }

    /// Drives `w` from cycle-open through a filled DOWN reversal entry and a cycle
    /// close into `Confirming` — same shape as worker.rs's own `enter_down_position` +
    /// `CycleClose` test sequence, duplicated here (rather than exported) since it's
    /// only needed to exercise `confirming_asset_labels`'s filtering, not the state
    /// machine itself (already covered by worker.rs's own tests).
    fn drive_to_confirming(w: &mut Worker, slug: &str) {
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: slug.to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }));
        w.step(Event::OrderFilled {
            filled_shares: 10.0,
            cost: 0.70,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        w.step(Event::CycleClose);
        assert!(w.is_confirming(), "setup: expected {slug} to be Confirming");
    }

    #[test]
    fn labels_only_confirming_workers() {
        let eth_params = test_params("ETH");
        let mut eth_hp = Worker::new_reversal("ETH", &eth_params);
        drive_to_confirming(&mut eth_hp, "eth-updown-5m-1000");

        let btc_params = test_params("BTC");
        let btc_watching = Worker::new_reversal("BTC", &btc_params);

        let workers = [&eth_hp, &btc_watching];
        let halted = confirming_asset_labels(workers.map(|w| (w.asset.clone(), w)));
        assert_eq!(halted, vec!["ETH/reversal".to_string()]);
    }

    #[test]
    fn empty_when_nothing_pending() {
        let btc_params = test_params("BTC");
        let btc_watching = Worker::new_reversal("BTC", &btc_params);
        let workers = [&btc_watching];
        assert!(confirming_asset_labels(workers.map(|w| (w.asset.clone(), w))).is_empty());
    }

    #[test]
    fn labels_every_concurrently_pending_worker() {
        // Balance is account-wide — if two assets both have a trade pending Gamma at
        // once, a single decrease can't be attributed to just one, so both are halted
        // (trader/doc/plan_gammapi_2026-07-11.md §3).
        let eth_params = test_params("ETH");
        let mut eth_hp = Worker::new_reversal("ETH", &eth_params);
        drive_to_confirming(&mut eth_hp, "eth-updown-5m-1000");

        let doge_params = test_params("DOGE");
        let mut doge_rev = Worker::new_reversal("DOGE", &doge_params);
        drive_to_confirming(&mut doge_rev, "doge-updown-5m-1000");

        let workers = [&eth_hp, &doge_rev];
        let mut halted = confirming_asset_labels(workers.map(|w| (w.asset.clone(), w)));
        halted.sort();
        assert_eq!(
            halted,
            vec!["DOGE/reversal".to_string(), "ETH/reversal".to_string()]
        );
    }
}

#[cfg(test)]
mod halt_persist_tests {
    use super::*;

    fn test_params(asset: &str) -> AssetParams {
        AssetParams {
            asset: asset.to_string(),
            strategies: vec!["reversal".to_string()],
            enter_when_time_left: 20.0,
            no_enter_when_time_left: 10.0,
            reversal: 0.60,
            reversal_low_threshold: 0.20,
            reversal_start_time: 120.0,
            gamma_poll_delay_secs: 60.0,
            gamma_poll_interval_secs: 20.0,
            gamma_poll_deadline_secs: 600.0,
            price_high_rev: 0.90,
            delta_pct_rev: 0.0008,
            sl_reversal: 0.0,
            unwind_pnl_rev: 0.03,
            sl_pnl_rev: 0.20,
            unwind_time_rev: 0.0,
            price_low: 0.80,
            price_high: 0.93,
            delta_pct_hp: 0.0004,
            sl_high_prob: 0.49,
            unwind_pnl_hp: 0.05,
            sl_pnl_hp: 0.25,
            unwind_time_hp: 0.0,
            v_high1: 0.70,
            v_low: 0.30,
            v_high2: 0.70,
            delta_pct_v: 0.0,
            sl_v_shape: 0.0,
            sl_pnl_v: 0.30,
            unwind_pnl_v: 0.15,
            unwind_time_v: 25.0,
            halt_rev: 2,
            halt_prob: 2,
            halt_v: 1,
            halt_reset_hour_rev: 2,
            halt_reset_hour_hp: 8,
            halt_reset_hour_v: 2,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 300.0,
            trade_size_usdc: 1.0,
            maker_entry: false,
            pup_edge_min_rev: None,
        }
    }

    fn scratch_state_path(name: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "poly_rust_test_halt_persist_{name}_{}.json",
                std::process::id()
            ))
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Minimal real `AssetSlot` — same field values `main()` uses for a fresh
    /// (never-restarted) slot — pointed at a scratch `state_file` so `persist()`
    /// writes somewhere disposable.
    fn test_slot(asset: &str, state_file: String) -> AssetSlot {
        let params = test_params(asset);
        AssetSlot {
            worker: Worker::new_reversal(asset, &params),
            params,
            duration: MarketDuration::M5,
            slot_val: 0,
            up_id: U256::from(0u64),
            dn_id: U256::from(0u64),
            current_token_id: None,
            max_buy_price: 0.95,
            log_path: String::new(),
            canceled_quote_log_path: String::new(),
            pup_veto_log_path: String::new(),
            control_log_path: format!("{state_file}.control.jsonl"),
            state_file,
            cycle_trades: 0,
            sl_notified: false,
            timeout_notified: false,
            wins: 0,
            losses: 0,
            stoplosses: 0,
            unwinds: 0,
            timeouts: 0,
            total_pnl: 0.0,
            last_trade: None,
            current_slug: None,
            last_binance: 0.0,
            last_poly_up: 0.0,
            last_poly_dn: 0.0,
            poly_sub: None,
            last_binance_server_ts: None,
            last_poly_server_ts: None,
            last_binance_ts: None,
            last_poly_ts: None,
        }
    }

    // ── market_key routing (feature_new_markets_2026-07-17.md §5.3) ──

    /// 5m slots key as the bare asset — byte-identical to every key that
    /// existed before durations (NATS routing, control_log, watcher replies).
    #[test]
    fn market_key_is_bare_asset_for_5m_and_tagged_otherwise() {
        let slot = test_slot("BTC", scratch_state_path("mk5m"));
        assert_eq!(slot.market_key(), "BTC");

        let mut slot15 = test_slot("BTC", scratch_state_path("mk15m"));
        slot15.duration = MarketDuration::M15;
        assert_eq!(slot15.market_key(), "BTC@15m");

        let mut slot1h = test_slot("BTC", scratch_state_path("mk1h"));
        slot1h.duration = MarketDuration::HourlyEt;
        assert_eq!(slot1h.market_key(), "BTC@1h-et");
    }

    /// The poly-tick cross-feed guard: a tick keyed "BTC" (the 5m/NATS key)
    /// must match only the 5m slot, and a "BTC@15m" tick only the 15m slot —
    /// same-asset different-duration markets have different CLOB tokens, so a
    /// leak either way trades on the wrong market's prices.
    #[test]
    fn poly_routing_key_never_crosses_durations() {
        let slot5 = test_slot("BTC", scratch_state_path("route5"));
        let mut slot15 = test_slot("BTC", scratch_state_path("route15"));
        slot15.duration = MarketDuration::M15;
        let slots = [&slot5, &slot15];

        let matches = |key: &str| -> Vec<String> {
            slots
                .iter()
                .filter(|s| s.market_key() == key)
                .map(|s| s.market_key())
                .collect()
        };
        assert_eq!(matches("BTC"), vec!["BTC"]);
        assert_eq!(matches("BTC@15m"), vec!["BTC@15m"]);
        assert!(matches("ETH").is_empty());
    }

    /// The actual scenario `trader/doc/plan_halt_persist_2026-07-11.md` closes: a
    /// halt must reach disk before any other event happens to persist the slot, not
    /// just flip the in-memory flag.
    #[test]
    fn apply_control_halt_persists_immediately() {
        let path = scratch_state_path("control");
        let _ = std::fs::remove_file(&path);
        let mut slot = test_slot("BTC", path.clone());

        assert!(
            load_persisted_slot(&path).is_none(),
            "setup: nothing on disk yet"
        );

        apply_control(&mut slot, ControlEvent::Halt);

        assert!(slot.worker.is_halted());
        let on_disk = load_persisted_slot(&path).expect("halt must have persisted to disk");
        assert!(
            on_disk.worker.entry_suppressed,
            "on-disk entry_suppressed must be true immediately, before any other event"
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn apply_control_resume_persists_immediately() {
        let path = scratch_state_path("resume");
        let _ = std::fs::remove_file(&path);
        let mut slot = test_slot("BTC", path.clone());
        apply_control(&mut slot, ControlEvent::Halt);
        assert!(load_persisted_slot(&path).unwrap().worker.entry_suppressed);

        apply_control(&mut slot, ControlEvent::Resume);

        assert!(!slot.worker.is_halted());
        let on_disk = load_persisted_slot(&path).expect("resume must have persisted to disk");
        assert!(!on_disk.worker.entry_suppressed);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn apply_balance_halt_persists_immediately() {
        let path = scratch_state_path("balance");
        let _ = std::fs::remove_file(&path);
        let mut slot = test_slot("ETH", path.clone());

        assert!(
            load_persisted_slot(&path).is_none(),
            "setup: nothing on disk yet"
        );

        apply_balance_halt(&mut slot);

        assert!(slot.worker.is_halted());
        let on_disk = load_persisted_slot(&path).expect("balance halt must have persisted to disk");
        assert!(on_disk.worker.entry_suppressed);

        std::fs::remove_file(&path).unwrap();
    }

    /// Regression test for trader/doc/incident_unable_to_resume_2026-07-15.md:
    /// `/reset_losses` — the command actually meant to clear a tripped
    /// loss-streak halt — must persist immediately, the same as `/halt` and
    /// `/resume` already do (`apply_control_halt_persists_immediately`).
    #[test]
    fn apply_control_reset_losses_persists_immediately() {
        let path = scratch_state_path("reset_losses");
        let _ = std::fs::remove_file(&path);
        let mut slot = test_slot("BTC", path.clone());
        slot.worker.restore_halt(false, 2, None); // halt_rev=2 in test_params -> tripped
        assert!(slot.worker.is_halted(), "sanity: loss-streak halt engaged");

        apply_control(&mut slot, ControlEvent::ResetLosses);

        assert!(!slot.worker.is_halted());
        let on_disk = load_persisted_slot(&path).expect("reset_losses must have persisted to disk");
        assert_eq!(
            on_disk.worker.halt_losses, 0,
            "on-disk halt_losses must be zeroed immediately"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// Reproduces the incident's actual symptom end-to-end at the slot level:
    /// a loss-streak halt survives repeated `/resume`s (the bug report's
    /// "/resume, /resume btc multiple times, /status still shows halted"),
    /// and `loss_streak_still_halted_note` — the helper the Telegram reply
    /// uses — must say so instead of the reply lying about success.
    #[test]
    fn resume_reply_note_flags_a_still_active_loss_streak_halt() {
        let path = scratch_state_path("note");
        let _ = std::fs::remove_file(&path);
        let mut slot = test_slot("BTC", path.clone());
        slot.worker.restore_halt(true, 2, None); // manual halt + tripped loss-streak

        for _ in 0..3 {
            apply_control(&mut slot, ControlEvent::Resume);
            assert!(
                slot.worker.is_halted(),
                "loss-streak halt must survive repeated /resume, matching the incident report"
            );
        }

        let note = loss_streak_still_halted_note(std::iter::once(&slot))
            .expect("note must flag the still-active loss-streak halt");
        assert!(note.contains("BTC/reversal"));
        assert!(note.contains("/reset_losses"));

        apply_control(&mut slot, ControlEvent::ResetLosses);
        assert!(!slot.worker.is_halted());
        assert!(
            loss_streak_still_halted_note(std::iter::once(&slot)).is_none(),
            "note must go away once the loss-streak halt actually clears"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// The control log (`trader/doc/plan_align_bt_with_live_2026-07-15.md`) — read
    /// back a `log_control_event` line and check every field, plus that it's
    /// genuinely append-only across multiple calls.
    #[test]
    fn log_control_event_appends_one_json_line_per_call() {
        let path = scratch_state_path("control_log_basic");
        let control_log = format!("{path}.control.jsonl");
        let _ = std::fs::remove_file(&control_log);
        let mut slot = test_slot("BTC", path.clone());

        log_control_event(&slot, "halt");
        slot.worker.restore_halt(false, 2, None); // halt_rev=2 in test_params -> now tripped
        log_control_event(&slot, "halt_engaged");

        let contents = std::fs::read_to_string(&control_log).expect("control log must exist");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON line per call, append-only");

        let first: serde_json::Value = serde_json::from_str(lines[0]).expect("valid JSON");
        assert_eq!(first["asset"], "BTC");
        assert_eq!(first["strategy"], "reversal");
        assert_eq!(first["event"], "halt");
        assert_eq!(first["is_halted"], false);
        assert!(first["ts"].as_f64().unwrap() > 0.0);

        let second: serde_json::Value = serde_json::from_str(lines[1]).expect("valid JSON");
        assert_eq!(second["event"], "halt_engaged");
        assert_eq!(second["halt_losses"], 2);
        assert_eq!(second["is_halted"], true);

        // No apply_control/persist call in this test, so no state_file was
        // ever written — only the control log needs cleanup here.
        let _ = std::fs::remove_file(&path);
        std::fs::remove_file(&control_log).unwrap();
    }

    /// `apply_control` must log each `ControlEvent` variant under the exact
    /// event name `trade_reconcile.py`'s parser expects.
    #[test]
    fn apply_control_logs_the_correct_event_name_per_variant() {
        let path = scratch_state_path("control_log_apply_control");
        let control_log = format!("{path}.control.jsonl");
        let _ = std::fs::remove_file(&control_log);
        let mut slot = test_slot("ETH", path.clone());

        apply_control(&mut slot, ControlEvent::Halt);
        apply_control(&mut slot, ControlEvent::Resume);
        apply_control(&mut slot, ControlEvent::ResetLosses);

        let contents = std::fs::read_to_string(&control_log).unwrap();
        let events: Vec<String> = contents
            .lines()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["event"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(events, vec!["halt", "resume", "reset_losses"]);

        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&control_log).unwrap();
    }

    #[test]
    fn apply_balance_halt_logs_drawdown_halt_event() {
        let path = scratch_state_path("control_log_balance");
        let control_log = format!("{path}.control.jsonl");
        let _ = std::fs::remove_file(&control_log);
        let mut slot = test_slot("BTC", path.clone());

        apply_balance_halt(&mut slot);

        let contents = std::fs::read_to_string(&control_log).unwrap();
        let entry: serde_json::Value =
            serde_json::from_str(contents.lines().next().unwrap()).unwrap();
        assert_eq!(entry["event"], "drawdown_halt");
        assert_eq!(entry["is_halted"], true);

        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&control_log).unwrap();
    }

    // ── render_auto_reset_line — "list the auto-reset time per strategy" ──────

    #[test]
    fn auto_reset_line_empty_when_no_assets() {
        assert_eq!(
            Driver::render_auto_reset_line(&[]),
            "Auto-reset (loss-streak halt): n/a"
        );
    }

    #[test]
    fn auto_reset_line_shows_reversal_and_high_prob_hours() {
        let mut hp = test_slot("ETH", scratch_state_path("auto_reset_hp"));
        hp.worker = Worker::new_high_prob("ETH", &hp.params);
        let rev = test_slot("BTC", scratch_state_path("auto_reset_rev"));

        let line = Driver::render_auto_reset_line(&[rev, hp]);

        // test_params(): halt_reset_hour_rev = 2, halt_reset_hour_hp = 8.
        assert!(
            line.contains("reversal 02:00 HKT (BTC)"),
            "line was: {line}"
        );
        assert!(
            line.contains("high_prob 08:00 HKT (ETH)"),
            "line was: {line}"
        );
    }

    #[test]
    fn auto_reset_line_groups_multiple_assets_on_the_same_strategy_and_hour() {
        let btc = test_slot("BTC", scratch_state_path("auto_reset_group_btc"));
        let eth = test_slot("ETH", scratch_state_path("auto_reset_group_eth"));

        let line = Driver::render_auto_reset_line(&[btc, eth]);

        assert!(
            line.contains("reversal 02:00 HKT (BTC,ETH)"),
            "same-strategy same-hour assets must be grouped into one entry: {line}"
        );
    }
}
