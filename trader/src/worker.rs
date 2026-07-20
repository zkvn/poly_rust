// Live per-(asset, strategy) typestate machine — plan §7/§8.
//
// Unlike backtest.rs's `machine::Machine` (instant fills, three states), this
// is the full live state set: NotReady is implicit (no Worker exists until a
// cycle loads), Watching/Halted/Entering/Holding/Unwinding/StopExiting/
// Confirming/EnrichOnly. `step(event) -> Vec<Action>` is a pure, synchronous
// function — the async driver (not built here) executes each `Action` via
// `execution::ExecutionEngine` and feeds the *result* back in as a further
// `Event`. This keeps the decision core testable with a scripted event
// sequence and no live I/O (§10: sync core, async shell).

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::backtest::{HaltCorrection, HaltTracker};
use crate::config::AssetParams;
use crate::execution::{MIN_GTC_SHARES, OrderKind, SellStatus, choose_exit_order_kind};
use crate::gates::{GateParams, check_gates};
use crate::signal::{
    DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal, Signal, SpreadSignal,
    VShapeSignal,
};
use crate::strategies::{HighProbStrategy, ReversalStrategy, VShapeStrategy};
use crate::types::{
    BinanceTick, CycleContext, EntryType, Outcome, PolyTick, Side, TradeIntent, TradeRecord,
};

// ── Exit arm (how a Holding position's take-profit is worked) ────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExitArm {
    /// shares >= 5: resting GTC limit SELL on the book; fill arrives via UnwindFilled.
    GtcResting { order_id: String },
    /// shares < 5: no GTC support at that size; watch PolyTick and FAK-sell on TP cross.
    PriceMonitor { tp_price: f64 },
}

// ── Maker entry (plan_unwind_5u_maker_2026-07-19 §2.2) ────────────────────────

/// A resting GTC entry BUY quote, tracked while `WorkerState::EnteringMaker`.
#[derive(Debug, Clone)]
pub struct MakerQuote {
    pub side: Side,
    pub entry_type: EntryType,
    /// The signal price this quote was rested at — both the quote's own
    /// limit price and, on a fill, the position's cost basis.
    pub quote_price: f64,
    /// `None` until `LimitBuyPlaced{status: Live}` reports back the CLOB's
    /// assigned id — see `execute()`'s handling of `Action::PlaceLimitBuy`
    /// for why a cancel racing that response can never actually occur.
    pub order_id: Option<String>,
    /// The tick timestamp (same clock as `Action::PlaceBuy`'s `signal_ts`)
    /// the quote was placed at — carried onto `Action::CancelEntryQuote` so
    /// the driver can log pull-to-cancel latency, the metric
    /// `btc_5mins/doc/plan_market_maker_mvp_2026-07-19.md` §4 calls out as
    /// "worth logging from day 1" (a stale quote sitting through a price jump
    /// is free money for the latency arbs the taker fee was designed to tax).
    pub quoted_at: f64,
}

/// Why a resting entry quote was cancelled — carried onto `Action::CancelEntryQuote`
/// so the driver can log the "would-have-been outcome" fields the plan calls the
/// single most important output of the paper run (the filled-vs-canceled
/// adverse-selection comparison).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelQuoteReason {
    /// The reversal condition or a re-checked gate that justified this quote
    /// no longer holds.
    SignalInvalidated,
    /// `T - 15s` before cycle end reached with no fill yet.
    CycleEndApproaching,
}

// ── p(up) negative-edge gate (plan_unwind_5u_maker_2026-07-19 §2.3) ──────────

/// Snapshots older than this read as absent for the pup gate — and, as of
/// 2026-07-20, for every display that shows a `p_up` reading too (Telegram,
/// `/status`, the console heartbeat): one constant, no separate display-only
/// threshold. Was `10.0` (fail-open window) until the "never trade on stale
/// information" principle (`CLAUDE.md` "Trading principles") replaced the
/// gate's fail-open behavior with fail-closed — see `PupGateOutcome::StaleBlocked`
/// and `trader/doc/plan_stale_data_gate_2026-07-20.md` §1. Tightened to `2.0`
/// in the same change: a fail-*closed* gate should use a genuinely tight
/// freshness bar, not the looser window that only made sense when staleness
/// was merely informational.
pub const PUP_GATE_MAX_AGE_SECS: f64 = 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PupGateOutcome {
    /// `p_side < entry_price + pup_edge_min_rev` — the entry was blocked.
    Veto,
    /// No fresh `p_up` reading (never received, or older than
    /// `PUP_GATE_MAX_AGE_SECS`) — the entry is blocked, same as `Veto`.
    /// Never trade on stale information (`CLAUDE.md` "Trading principles"):
    /// this used to fail open (does NOT veto), on the premise that a dead
    /// indicator daemon must never silently halt trading — reversed
    /// 2026-07-20 after a genuine indicator staleness window let a bad-edge
    /// DOGE entry through (`trader/doc/audit_48hr_unwind_maker_2026-07-20.md`
    /// §1). A Telegram warning is sent instead so a degraded/dead indicator
    /// stays visible without silently blocking-and-hiding the block.
    StaleBlocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HoldingData {
    pub side: Side,
    pub entry_type: EntryType,
    pub token_price: f64,
    pub entry_ts: f64,
    /// The cached poly-price observation's own timestamp (`LatestPolySignal::ts`) at fill
    /// time — see `TradeRecord::entry_price_ts`'s doc comment. `#[serde(default)]` so a
    /// state file persisted before this field existed still deserializes.
    #[serde(default)]
    pub entry_price_ts: f64,
    pub shares: f64,
    pub exit_arm: ExitArm,
    /// Count of failed exit-order attempts (unwind and/or stop-loss) seen
    /// while this position was held — lets a later WIN/LOSS-at-resolution
    /// `TradeRecord` show it wasn't a clean hold, an early exit was tried
    /// and failed first (see `on_unwind_failed`/`on_stop_sell_failed`).
    #[serde(default)]
    pub exit_attempts: u32,
    /// Most recent failed exit attempt's error message, if any.
    #[serde(default)]
    pub exit_last_error: Option<String>,
    /// Dollar pnl already locked in from an earlier *partial* fill against this
    /// same position (`on_unwind_filled`/`on_stop_sell_filled`'s `sold_shares <
    /// h.shares` branch) — carried forward so the eventual terminal pnl
    /// calculation (whichever event finally closes out `shares`) reflects the
    /// whole position's result, not just the leftover residual's. Zero for a
    /// position that's never had a partial exit.
    #[serde(default)]
    pub realized_pnl: f64,
    /// Taker fees (USDC) already incurred against this position: the entry BUY's
    /// fee (charged once, on the full fill, at `on_order_filled`) plus one SELL
    /// fee per completed early-exit fill against it so far. Polymarket charges
    /// takers `shares * fee_rate * price * (1 - price)` per matched order
    /// (`taker_fee`, below) — resolution/redemption itself is not a trade and
    /// incurs no further fee. Subtracted from the gross `shares * (exit -
    /// token_price)` figure at final settlement (`settle_pnl`) so logged `pnl`
    /// reflects real cash, not the exchange-fee-blind gross number (see
    /// `trader/doc/incident_tele_pnl_2026-07-04.md` §2).
    #[serde(default)]
    pub fees: f64,
    /// Entry BUY latency (ms) — signal leg (tick timestamp -> driver receipt)
    /// and process leg (driver receipt -> fill confirmation). Carried onto the
    /// eventual `TradeRecord` unchanged; see `types::TradeRecord`'s own doc
    /// comments for the exit-side counterparts.
    #[serde(default)]
    pub entry_signal_latency_ms: f64,
    #[serde(default)]
    pub entry_process_latency_ms: f64,
}

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WorkerState {
    /// No open position; signals updating. `Halted` is the same state with
    /// entry suppressed (§8: halt is a no-entry gate, not a distinct state) —
    /// modeled here as a bool on `Worker`, not a state variant, so it never
    /// has to be threaded through Holding/Unwinding/StopExiting transitions.
    Watching,
    /// FAK BUY submitted, awaiting `OrderFilled`/`OrderRejected`.
    Entering,
    /// Maker entry (plan_unwind_5u_maker_2026-07-19 §2.2, reversal only):
    /// GTC limit BUY resting on the book at the signal price, awaiting
    /// `LimitBuyPlaced`/`EntryQuoteFilled`, or cancellation on signal
    /// invalidation / T-15s before cycle end (`check_maker_quote_cancel`).
    EnteringMaker(MakerQuote),
    Holding(HoldingData),
    /// Take-profit crossed or GTC fill notified; SELL in flight.
    Unwinding(HoldingData),
    /// Stop-loss floor crossed; FAK SELL in flight.
    StopExiting(HoldingData),
    /// Max holding time (`unwind_time_rev`/`unwind_time_hp`) elapsed with no
    /// other exit having fired; unbounded FAK SELL in flight, same mechanics
    /// as `StopExiting` but a distinct variant so the eventual `Outcome`
    /// (`Timeout`, not `StopLoss`) and Telegram copy can differ. See
    /// `trader/doc/plan_unwind_time_2026-07-08.md`.
    TimingOut(HoldingData),
    /// Held WIN/LOSS awaiting async `ApiResult` confirmation (may flip + fix halt).
    Confirming(TradeRecord),
    /// STOPLOSS/UNWIND awaiting `ApiResult` for the CSV column only (pnl/halt final).
    EnrichOnly(TradeRecord),
}

// ── Events (drives everything; the machine never polls) ──────────────────────

#[derive(Debug, Clone)]
pub enum Event {
    CycleOpen {
        ctx: CycleContext,
        slug: String,
    },
    CycleClose,
    BinanceTick(BinanceTick),
    PolyTick(PolyTick),
    OrderFilled {
        filled_shares: f64,
        cost: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    OrderRejected,
    /// Response to the `Action::PlaceLimitSell` issued right after an entry fill.
    LimitSellPlaced {
        order_id: Option<String>,
        status: SellStatus,
        error: Option<String>,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    /// Response to the `Action::PlaceLimitBuy` issued for a maker entry
    /// (plan_unwind_5u_maker_2026-07-19 §2.2).
    LimitBuyPlaced {
        order_id: Option<String>,
        status: SellStatus,
        error: Option<String>,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    /// A resting maker-entry GTC BUY quote filled — delivered by the paper
    /// driver's observed-price fill routing (`PaperExecutor::on_price`); the
    /// live path doesn't produce this yet (no USER-channel fill wiring for
    /// entries — see README `## TODO`).
    EntryQuoteFilled {
        filled_shares: f64,
        cost: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    /// A fresh `p_up` reading for this worker's asset (plan_unwind_5u_maker_2026-07-19
    /// §2.3) — only sent when the indicator snapshot actually has a ready
    /// `p_up` value (a warmup snapshot with no `p_up` key produces no event
    /// at all, which is indistinguishable from "no snapshot" for the gate's
    /// fail-open purposes). `ts` is the snapshot's own timestamp, checked
    /// against `now` at gate-evaluation time — not against arrival time —
    /// same pattern as `LatestPolySignal::age`.
    IndicatorUpdate {
        p_up: f64,
        ts: f64,
    },
    UnwindFilled {
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    UnwindFailed {
        error: Option<String>,
    },
    StopSellFilled {
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    StopSellFailed {
        error: Option<String>,
    },
    TimeoutSellFilled {
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    },
    TimeoutSellFailed {
        error: Option<String>,
    },
    /// Async market-resolution confirmation (Gamma/CLOB), arriving after cycle end.
    ApiResult {
        won: bool,
    },
    /// Gamma never resolved within the retry deadline (`reversal_start_time` seconds
    /// after the position closed) — see `trader/doc/incident_DOGE_wrong_result_2026-07-09.md`
    /// §4. A `Confirming` worker halts new entries rather than keep an unverified
    /// result silently, *unless* `balance_increased` — see `on_api_result_timeout`.
    /// An `EnrichOnly` worker just gives up the counterfactual verdict regardless (it
    /// never affected pnl/halt to begin with).
    ApiResultTimeout {
        /// Whether the account balance grew from the previous cycle's checkpoint to
        /// this one (`GammaBalanceTracker::increased()`, `None` collapsed to `false`
        /// by the caller — unknown/failed-fetch fails safe to "still halt"). Set by
        /// `bin/live.rs` right before dispatching this event (2026-07-09).
        balance_increased: bool,
    },
    Control(ControlEvent),
    Balance(BalanceEvent),
}

#[derive(Debug, Clone, Copy)]
pub enum ControlEvent {
    Halt,
    Resume,
    /// Zeroes the per-strategy consecutive-loss counter (`halt_rev`/
    /// `halt_prob`) — the `/reset_losses` command's effect. Distinct from
    /// `Resume`, which only clears `entry_suppressed` and deliberately never
    /// touches this counter (§8) — see
    /// `trader/doc/incident_unable_to_resume_2026-07-15.md`.
    ResetLosses,
}

#[derive(Debug, Clone, Copy)]
pub enum BalanceEvent {
    DrawdownHalt,
}

// ── Actions (side effects the async driver must perform) ─────────────────────

/// Distinguishes why a `ClosePosition` was requested — the driver needs this
/// to route the FAK SELL's result to the right follow-up event
/// (`UnwindFilled`/`UnwindFailed` vs `StopSellFilled`/`StopSellFailed`), since
/// `Worker`'s internal state isn't otherwise observable from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    TakeProfit,
    StopLoss,
    /// Max holding time elapsed (`unwind_time_rev`/`unwind_time_hp`) — force
    /// closed at market, same as StopLoss mechanically (unbounded FAK), but a
    /// distinct reason so the driver routes it to `TimeoutSellFilled`/
    /// `TimeoutSellFailed` instead of `StopSellFilled`/`StopSellFailed`.
    Timeout,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// `signal_ts` is the triggering tick's own timestamp — the driver uses it
    /// to compute the "Order placed" Telegram message's signal/process latency.
    PlaceBuy {
        side: Side,
        price: f64,
        size_usdc: f64,
        signal_ts: f64,
    },
    /// `side`/`entry_price` are the position's own entry, carried along so
    /// the driver can send one merged "entry filled -> exit resting"
    /// Telegram message instead of two (audit item 2,
    /// trader/doc/plan_stale_data_gate_2026-07-20.md §2) — the exit order is
    /// always placed in the same synchronous action batch as the entry-fill
    /// confirmation (`finalize_entry_fill`), so there's never a real "later"
    /// moment to report the fill separately. `via_maker_entry` scopes the
    /// merged notification to the maker-entry path only, where there's no
    /// other fill-time notification to duplicate; the FAK path's existing
    /// "Order placed" message already covers the fill, so `PlaceLimitSell`
    /// stays console-only there.
    PlaceLimitSell {
        shares: f64,
        price: f64,
        side: Side,
        entry_price: f64,
        via_maker_entry: bool,
    },
    /// Maker entry (plan_unwind_5u_maker_2026-07-19 §2.2): rest a GTC BUY at
    /// `price` for `shares` (`execution::MIN_GTC_SHARES`) instead of a
    /// marketable FAK buy. `signal_ts` is the triggering tick's own timestamp.
    PlaceLimitBuy {
        side: Side,
        price: f64,
        shares: f64,
        signal_ts: f64,
    },
    /// Cancel a resting maker-entry quote — signal invalidation or T-15s
    /// (`CancelQuoteReason`). `order_id: None` only if `LimitBuyPlaced` hasn't
    /// come back yet; structurally this can't race the placement itself (the
    /// driver's single-threaded action loop always resolves
    /// `PlaceLimitBuy` -> `LimitBuyPlaced` before any other tick can be
    /// processed), so `None` here always means the CLOB call itself failed.
    CancelEntryQuote {
        order_id: Option<String>,
        side: Side,
        quote_price: f64,
        reason: CancelQuoteReason,
        /// When the quote was originally placed — the driver logs
        /// `now - quoted_at` as pull-to-cancel latency.
        quoted_at: f64,
    },
    /// p(up) negative-edge gate (plan_unwind_5u_maker_2026-07-19 §2.3):
    /// logged by the driver (`Veto` needs a CSV row so the 48h evaluation
    /// can gamma-resolve the counterfactual; `StaleBlocked` gets a console
    /// line plus a debounced Telegram warning). Both outcomes **replace**
    /// whatever `try_enter` would otherwise return — the entry never fires
    /// for either (`trader/doc/plan_stale_data_gate_2026-07-20.md` §1).
    PupGateNote {
        side: Side,
        /// `None` for `StaleBlocked` (no reading to report).
        p_side: Option<f64>,
        price: f64,
        outcome: PupGateOutcome,
    },
    /// `limit_price`: `Some(tp_price)` for a take-profit close (bounded — see
    /// `execution::close_position_at_price`), `None` for a stop-loss close
    /// (unbounded — a stop-loss must close regardless of price). `signal_ts`
    /// is the triggering tick's own timestamp, for the "order executed"
    /// Telegram message's latency breakdown.
    ClosePosition {
        shares: f64,
        reason: CloseReason,
        limit_price: Option<f64>,
        signal_ts: f64,
    },
    CancelLimitSell {
        order_id: String,
    },
    /// Write `PersistedState` to the crash-recovery file — call after every transition.
    Persist,
    LogTrade(TradeRecord),
    /// `ApiResult` flipped a Confirming (Win/Loss) record — `previous_outcome`/
    /// `previous_pnl` are the original estimate, `record` is the corrected one.
    LogTradeCorrection {
        previous_outcome: Outcome,
        previous_pnl: f64,
        record: TradeRecord,
    },
    /// `ApiResult` resolved a StopLoss `EnrichOnly` record — counterfactual verdict
    /// only, never touches pnl/result/halt (unlike `LogTradeCorrection`).
    StopLossVerdict {
        record: TradeRecord,
        would_have_won: bool,
    },
    /// The loss-streak halt (`halt_rev`/`halt_prob`, distinct from manual `/halt`
    /// and the balance drawdown halt) just tripped on this worker's strategy —
    /// emitted once, on the exact trade that crossed the threshold.
    HaltEngaged,
    /// The loss-streak halt's daily reset (`halt_reset_hour_rev`/`halt_reset_hour_hp`)
    /// just cleared an *active* halt on this worker's strategy — not emitted on a
    /// session rollover that had nothing to clear.
    HaltReset,
    /// A Gamma `ApiResult` correction (Confirming Loss -> Win) just pulled the
    /// loss-streak count back below `halt_rev`/`halt_prob`, clearing a halt
    /// that had been engaged partly or wholly on a phantom loss — distinct
    /// from `HaltReset` (daily rollover). See
    /// `trader/doc/incident_halt_double_count_2026-07-10.md`.
    HaltClearedByCorrection,
    /// `ApiResultTimeout` hit a `Confirming` (Win/Loss) record — Gamma never resolved
    /// within the deadline, so the provisional record stands as final (unverified) and
    /// new entries are suppressed until `/resume`. Distinct from `HaltEngaged` (loss
    /// streak) and the balance-drawdown halt (`bin/live.rs`'s own message) — this one
    /// means "we don't know if this was right," not "too many/too much lost."
    GammaHaltEngaged {
        record: TradeRecord,
    },
    /// `ApiResultTimeout` hit a `Confirming` record, but `balance_increased` was true —
    /// the provisional record still stands as final (unverified), same as
    /// `GammaHaltEngaged`, but new entries are *not* suppressed on account of this
    /// timeout (2026-07-09; see `trader/doc/incident_DOGE_wrong_result_2026-07-09.md`
    /// and the README's Gamma-halt section for the risk tradeoff this accepts).
    /// `entry_suppressed` is this worker's *actual current* suppression state — still
    /// `true` here if some other halt (manual `/halt`, loss-streak, drawdown) already
    /// applies; this action never clears one.
    GammaUnresolvedContinued {
        record: TradeRecord,
        entry_suppressed: bool,
    },
    /// Diagnostic-only: an `ApiResult`/`ApiResultTimeout` arrived but there was nothing
    /// to do — either the provisional call was already correct, or the event arrived in
    /// a state that no longer expects it (stale). `bin/live.rs` prints this to `live.log`;
    /// no Telegram notification (not actionable) and no CSV/state write.
    ApiResultNote(String),
}

// ── Persisted state (crash recovery) ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistedWorkerState {
    Watching,
    Entering,
    Holding(HoldingData),
    Unwinding(HoldingData),
    StopExiting(HoldingData),
    TimingOut(HoldingData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedState {
    pub asset: String,
    pub strategy: String,
    pub slug: String,
    pub cycle_start: f64,
    pub cycle_end: f64,
    pub state: PersistedWorkerState,
    /// No-entry gate at the time of persisting — `/halt`, the balance-drawdown
    /// guard, or manually cleared by `/resume`. `#[serde(default)]` so a state
    /// file written before this field existed still loads (as `false`, i.e.
    /// "not halted" — the same as today's un-persisted behavior).
    #[serde(default)]
    pub entry_suppressed: bool,
    /// The per-strategy consecutive-loss halt's counter/session at persist
    /// time — restores `HaltTracker` across a restart via `Worker::restore_halt`.
    /// `halt_max`/`halt_reset_hour` are deliberately NOT persisted here; they
    /// always come fresh from config so a config change takes effect on restart.
    #[serde(default)]
    pub halt_losses: i64,
    #[serde(default)]
    pub halt_last_session: Option<NaiveDate>,
}

// ── Strategy variant (mirrors machine.rs) ─────────────────────────────────────

enum StrategyKind {
    Reversal(ReversalStrategy),
    HighProb(HighProbStrategy),
    VShape(VShapeStrategy),
}

#[inline]
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Total pnl for a position being settled at `exit_price` right now: whatever
/// was already locked in from an earlier partial fill (`h.realized_pnl`) plus
/// the currently-held `h.shares`' own result (proceeds `shares * exit_price`
/// minus their cost basis `shares * token_price`, i.e. `shares * (exit_price -
/// token_price)`). Deliberately *not* `shares * exit_price - trade_size`
/// (the nominal $ intended for the whole position) — that undercounts a
/// position that already had shares sold off earlier (or overcounts, on the
/// downside, when the residual settles at zero), since `trade_size` no longer
/// reflects what's actually being valued once `shares` has been reduced by a
/// prior partial unwind/stop-loss fill. Matches `bot/worker.py`'s
/// `shares * (1.0 - cost)` / `-shares * cost` win/loss formulas, generalized
/// to one expression via `exit_price ∈ {0.0, 1.0}` for a natural resolution
/// and the real fill price for an early exit.
#[inline]
fn settle_pnl(h: &HoldingData, exit_price: f64) -> f64 {
    round4(h.realized_pnl + h.shares * (exit_price - h.token_price) - h.fees)
}

/// Polymarket's per-order taker fee (maker fills are free): `shares * rate *
/// price * (1 - price)` — symmetric around p=0.5, charged on every matched
/// taker order (our BUYs and early-exit SELLs are always taker; resolution/
/// redemption is not an order at all and pays nothing). `0.07` is the
/// documented Crypto-category rate (`docs.polymarket.com/polymarket-learn/
/// trading/fees`) — the only category any of `trade_assets` (BTC/ETH/DOGE)
/// currently trade in. See `trader/doc/incident_tele_pnl_2026-07-04.md` §2:
/// neither this port nor `bot/worker.py` used to subtract this at all, so
/// every logged pnl was gross, overstating real cash by the fee on both legs.
const TAKER_FEE_RATE: f64 = 0.07;

#[inline]
fn taker_fee(shares: f64, price: f64) -> f64 {
    shares * TAKER_FEE_RATE * price * (1.0 - price)
}

/// Below this, `shares * 1e6` (Polymarket's on-chain `makerAmount` units) is
/// under the exchange's hard floor of 10_000 — no price can ever make such an
/// order valid, so it's not worth attempting (see incident doc §3: this is
/// what "invalid maker amount" means for a sub-cent residual). A partial-fill
/// leftover this small is written off (excluded from pnl) rather than chased.
const MIN_SELLABLE_SHARES: f64 = 0.01;

// ── Worker ────────────────────────────────────────────────────────────────────

pub struct Worker {
    pub asset: String,
    pub strategy_name: &'static str,
    kind: StrategyKind,
    saw_low_up: SawLowSignal,
    saw_low_dn: SawLowSignal,
    // V-shape two-stage latches — always present and updated (cheap, same pattern
    // as saw_low_* being fed for a high_prob worker), only read by
    // StrategyKind::VShape. Mirrors machine.rs's identical fields.
    v_up: VShapeSignal,
    v_dn: VShapeSignal,
    latest_poly: LatestPolySignal,
    spread: SpreadSignal,
    delta_pct: DeltaPctSignal,
    latest_binance: LatestBinanceSignal,
    state: WorkerState,
    /// No-entry gate — set by `/halt`, the loss-limit tracker, or a balance
    /// drawdown; cleared by `/resume` or the daily reset. Never touches an
    /// in-flight Entering/Holding/Unwinding/StopExiting position (§8).
    entry_suppressed: bool,
    /// Per-strategy consecutive-loss halt, config-driven (`halt_rev`/`halt_prob`
    /// count, `halt_reset_hour_rev`/`halt_reset_hour_hp` daily HKT reset) —
    /// same `HaltTracker` backtest.rs already uses, ported here since the live
    /// binary never wired up an equivalent (this config was parsed but had no
    /// effect on live trading before this fix).
    halt: HaltTracker,
    cycle_open_binance: f64,
    last_binance: f64,
    last_binance_ts_value: f64,
    cycle_start_ts: f64,
    cycle_end_ts: f64,
    cycle_slug: String,
    sl: f64,
    sl_pnl: f64,
    unwind_pnl: f64,
    /// Max holding time (seconds) before a still-open position is force-closed
    /// at market — `0.0` disables it. See `on_poly`'s timeout check and
    /// `trader/doc/plan_unwind_time_2026-07-08.md`.
    unwind_time: f64,
    trade_size: f64,
    gate_params: GateParams,
    /// Set when entering `Entering`, consumed when the fill/reject event lands.
    pending_entry: Option<(Side, EntryType, f64)>,
    /// Maker-entry mode (plan_unwind_5u_maker_2026-07-19 §2.2) — only ever
    /// consulted when `kind` is `StrategyKind::Reversal`.
    maker_entry: bool,
    /// The reversal strategy's own entry threshold, duplicated from
    /// `AssetParams.reversal` (which `StrategyKind::Reversal` already holds
    /// privately) so `check_maker_quote_cancel` can re-check "is the
    /// condition that justified this quote still true" without needing an
    /// accessor into `ReversalStrategy`. Unused outside the reversal path.
    reversal_threshold: f64,
    /// `None` = gate disabled. See `AssetParams.pup_edge_min_rev`.
    pup_edge_min_rev: Option<f64>,
    /// Latest ready `p_up` reading and its own snapshot timestamp, updated by
    /// `Event::IndicatorUpdate`. `None` until the first ready snapshot for
    /// this asset arrives; staleness (`PUP_GATE_MAX_AGE_SECS`) is checked
    /// against `now` at gate-evaluation time, not at update time.
    pup_snapshot: Option<(f64, f64)>,
}

impl Worker {
    // Private, 2 call sites (new_reversal/new_high_prob) — each arg is an
    // independently meaningful strategy-specific scalar, not a good fit for a
    // wrapper struct.
    #[allow(clippy::too_many_arguments)]
    fn common(
        asset: &str,
        strategy_name: &'static str,
        kind: StrategyKind,
        p: &AssetParams,
        sl: f64,
        sl_pnl: f64,
        unwind_pnl: f64,
        unwind_time: f64,
        halt_max: i64,
        halt_reset_hour: i64,
    ) -> Self {
        Self {
            asset: asset.to_string(),
            strategy_name,
            kind,
            saw_low_up: SawLowSignal::new_up(
                p.reversal_low_threshold,
                p.reversal_start_time,
                p.no_enter_when_time_left,
            ),
            saw_low_dn: SawLowSignal::new_dn(
                p.reversal_low_threshold,
                p.reversal_start_time,
                p.no_enter_when_time_left,
            ),
            v_up: VShapeSignal::new_up(p.v_high1, p.v_low),
            v_dn: VShapeSignal::new_dn(p.v_high1, p.v_low),
            latest_poly: LatestPolySignal::new(),
            spread: SpreadSignal::new(),
            delta_pct: DeltaPctSignal::new(),
            latest_binance: LatestBinanceSignal::new(),
            state: WorkerState::Watching,
            entry_suppressed: false,
            halt: HaltTracker::new(halt_max, halt_reset_hour),
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            last_binance_ts_value: 0.0,
            cycle_start_ts: 0.0,
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
            sl,
            sl_pnl,
            unwind_pnl,
            unwind_time,
            trade_size: p.trade_size_usdc,
            pending_entry: None,
            gate_params: GateParams {
                spread_premium_limit: p.spread_premium_limit,
                spread_discount_limit: p.spread_discount_limit,
                max_price_age_secs: p.max_price_age_secs,
                delta_pct_rev: p.delta_pct_rev,
                delta_pct_hp: p.delta_pct_hp,
                delta_pct_v: p.delta_pct_v,
                max_buy_price: p.max_buy_price,
                price_high_rev: p.price_high_rev,
            },
            maker_entry: p.maker_entry,
            reversal_threshold: p.reversal,
            pup_edge_min_rev: p.pup_edge_min_rev,
            pup_snapshot: None,
        }
    }

    pub fn new_reversal(asset: &str, p: &AssetParams) -> Self {
        Self::common(
            asset,
            "reversal",
            StrategyKind::Reversal(ReversalStrategy::new(p.reversal, p.no_enter_when_time_left)),
            p,
            p.sl_reversal,
            p.sl_pnl_rev,
            p.unwind_pnl_rev,
            p.unwind_time_rev,
            p.halt_rev,
            p.halt_reset_hour_rev,
        )
    }

    pub fn new_high_prob(asset: &str, p: &AssetParams) -> Self {
        Self::common(
            asset,
            "high_prob",
            StrategyKind::HighProb(HighProbStrategy::new(
                p.price_low,
                p.price_high,
                p.enter_when_time_left,
                p.no_enter_when_time_left,
            )),
            p,
            p.sl_high_prob,
            p.sl_pnl_hp,
            p.unwind_pnl_hp,
            p.unwind_time_hp,
            p.halt_prob,
            p.halt_reset_hour_hp,
        )
    }

    pub fn new_v_shape(asset: &str, p: &AssetParams) -> Self {
        Self::common(
            asset,
            "v_shape",
            StrategyKind::VShape(VShapeStrategy::new(p.v_high2, p.no_enter_when_time_left)),
            p,
            p.sl_v_shape,
            p.sl_pnl_v,
            p.unwind_pnl_v,
            p.unwind_time_v,
            p.halt_v,
            p.halt_reset_hour_v,
        )
    }

    /// True if entries are suppressed for any reason — manual `/halt`,
    /// balance drawdown, or the per-strategy consecutive-loss halt (config
    /// `halt_rev`/`halt_prob`, auto-clears at `halt_reset_hour_rev`/
    /// `halt_reset_hour_hp` HKT each day).
    pub fn is_halted(&self) -> bool {
        self.entry_suppressed || self.halt.is_halted()
    }

    /// True specifically because the manual/drawdown/gamma-unresolved gate
    /// (`entry_suppressed`) is set — the flag `/resume` clears. Distinct from
    /// `loss_streak_halted`, which `/resume` never touches — see
    /// `trader/doc/incident_unable_to_resume_2026-07-15.md`.
    pub fn manually_suppressed(&self) -> bool {
        self.entry_suppressed
    }

    /// True specifically because the per-strategy consecutive-loss halt
    /// (`halt_rev`/`halt_prob`) has tripped — cleared only by `/reset_losses`
    /// or the daily `halt_reset_hour_rev`/`_hp` rollover, never by `/resume`.
    pub fn loss_streak_halted(&self) -> bool {
        self.halt.is_halted()
    }

    /// Current consecutive-loss count / configured threshold, for status and
    /// control-reply display alongside `loss_streak_halted`.
    pub fn halt_losses(&self) -> i64 {
        self.halt.losses()
    }

    pub fn halt_max(&self) -> i64 {
        self.halt.max()
    }

    pub fn has_open_position(&self) -> bool {
        matches!(
            self.state,
            WorkerState::Entering
                | WorkerState::EnteringMaker(_)
                | WorkerState::Holding(_)
                | WorkerState::Unwinding(_)
                | WorkerState::StopExiting(_)
                | WorkerState::TimingOut(_)
        )
    }

    /// Order id of an in-flight maker-entry GTC BUY quote, if one is
    /// resting — the paper driver routes a simulated resting-order fill back
    /// to the owning worker via this (`Event::EntryQuoteFilled`).
    pub fn entry_resting_order_id(&self) -> Option<&str> {
        match &self.state {
            WorkerState::EnteringMaker(q) => q.order_id.as_deref(),
            _ => None,
        }
    }

    /// True while a closed trade's WIN/LOSS is still provisional, awaiting Gamma
    /// confirmation — used to scope the per-cycle balance-decrease halt (2026-07-11,
    /// `trader/doc/plan_gammapi_2026-07-11.md`) to only the asset+strategy that
    /// actually has exposure pending resolution, instead of halting everything.
    pub fn is_confirming(&self) -> bool {
        matches!(self.state, WorkerState::Confirming(_))
    }

    /// Current `(latest_binance - cycle_open) / cycle_open` — the live reading
    /// of the same gate signal `check_gates` uses, for status display.
    pub fn delta_pct(&self) -> f64 {
        self.delta_pct.value()
    }

    /// Current cycle's close deadline (unix seconds) — for "time left" display.
    pub fn cycle_end_ts(&self) -> f64 {
        self.cycle_end_ts
    }

    /// Current cycle's opening Binance price — for cycle price-move display.
    pub fn cycle_open_binance(&self) -> f64 {
        self.cycle_open_binance
    }

    /// Order id of the held position's resting GTC take-profit SELL, if its
    /// exit arm is `GtcResting` — the paper driver routes a simulated
    /// resting-order fill back to the owning worker via this
    /// (`Event::UnwindFilled`). `Holding` only: once a stop-loss/timeout is
    /// in flight the resting order has already been cancelled.
    pub fn exit_resting_order_id(&self) -> Option<&str> {
        match &self.state {
            WorkerState::Holding(h) => match &h.exit_arm {
                ExitArm::GtcResting { order_id } => Some(order_id),
                ExitArm::PriceMonitor { .. } => None,
            },
            _ => None,
        }
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    pub fn to_persisted(&self) -> PersistedState {
        let state = match &self.state {
            WorkerState::Watching => PersistedWorkerState::Watching,
            // A resting maker-entry quote isn't a filled position — a crash
            // here has nothing to resume either way, same as Entering; the
            // quote itself is lost (matching Entering's existing posture on
            // an in-flight FAK).
            WorkerState::Entering | WorkerState::EnteringMaker(_) => PersistedWorkerState::Entering,
            WorkerState::Holding(h) => PersistedWorkerState::Holding(h.clone()),
            WorkerState::Unwinding(h) => PersistedWorkerState::Unwinding(h.clone()),
            WorkerState::StopExiting(h) => PersistedWorkerState::StopExiting(h.clone()),
            WorkerState::TimingOut(h) => PersistedWorkerState::TimingOut(h.clone()),
            // Resolved/Confirming/EnrichOnly are not open-exposure states; a
            // crash there loses only the async-confirmation follow-up, not a
            // live position, so they persist as Watching (nothing to resume).
            WorkerState::Confirming(_) | WorkerState::EnrichOnly(_) => {
                PersistedWorkerState::Watching
            }
        };
        PersistedState {
            asset: self.asset.clone(),
            strategy: self.strategy_name.to_string(),
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            cycle_end: self.cycle_end_ts,
            state,
            entry_suppressed: self.entry_suppressed,
            halt_losses: self.halt.losses(),
            halt_last_session: self.halt.last_session(),
        }
    }

    /// Restores halt state from a previously-persisted `PersistedState` — the
    /// counterpart to `to_persisted`'s `entry_suppressed`/`halt_losses`/
    /// `halt_last_session`. Config-derived halt params (`max`/`reset_hour`)
    /// stay whatever `new_reversal`/`new_high_prob` already set from the
    /// current config, so a config change between restarts takes effect
    /// immediately rather than being shadowed by the persisted file.
    pub fn restore_halt(
        &mut self,
        entry_suppressed: bool,
        halt_losses: i64,
        halt_last_session: Option<NaiveDate>,
    ) {
        self.entry_suppressed = entry_suppressed;
        self.halt = HaltTracker::restore(
            self.halt.max(),
            self.halt.reset_hour(),
            halt_losses,
            halt_last_session,
        );
    }

    /// Reconcile a reloaded `PersistedState` against the live CLOB before
    /// resuming: a `Holding{GtcResting}` whose order is gone but whose token
    /// balance is still present resumes as `PriceMonitor`; a zero-balance
    /// position (already sold/redeemed) resumes as `Watching`. Pure function —
    /// testable without a live exchange by injecting the open-order/balance facts.
    pub fn reconcile(
        persisted: &PersistedWorkerState,
        open_order_ids: &[String],
        token_balance: f64,
    ) -> WorkerState {
        match persisted {
            PersistedWorkerState::Watching => WorkerState::Watching,
            // The FAK either filled or didn't while we were down; with no fill
            // confirmation available, the safe default is to treat it as not
            // filled either way (no entry details to reconstruct a Holding from).
            PersistedWorkerState::Entering => WorkerState::Watching,
            PersistedWorkerState::Holding(h)
            | PersistedWorkerState::Unwinding(h)
            | PersistedWorkerState::StopExiting(h)
            | PersistedWorkerState::TimingOut(h) => {
                if token_balance <= 0.0 {
                    return WorkerState::Watching; // already resolved/sold while we were down
                }
                let mut h = h.clone();
                if let ExitArm::GtcResting { order_id } = &h.exit_arm
                    && !open_order_ids.contains(order_id)
                {
                    // Resting order is gone but tokens remain — fall back to PriceMonitor.
                    h.exit_arm = ExitArm::PriceMonitor {
                        tp_price: h.token_price + 0.0,
                    };
                }
                WorkerState::Holding(h)
            }
        }
    }

    pub fn resume_from(&mut self, state: WorkerState) {
        self.state = state;
    }

    // ── Event dispatch ────────────────────────────────────────────────────────

    pub fn step(&mut self, event: Event) -> Vec<Action> {
        match event {
            Event::CycleOpen { ctx, slug } => self.on_cycle_open(ctx, slug),
            Event::CycleClose => self.on_cycle_close(),
            Event::BinanceTick(t) => self.on_binance(t),
            Event::PolyTick(t) => self.on_poly(t),
            Event::IndicatorUpdate { p_up, ts } => {
                self.pup_snapshot = Some((p_up, ts));
                vec![]
            }
            Event::OrderFilled {
                filled_shares,
                cost,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_order_filled(filled_shares, cost, signal_latency_ms, process_latency_ms),
            Event::OrderRejected => self.on_order_rejected(),
            Event::LimitSellPlaced {
                order_id,
                status,
                error,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_limit_sell_placed(
                order_id,
                status,
                error,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::LimitBuyPlaced {
                order_id,
                status,
                error,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_limit_buy_placed(
                order_id,
                status,
                error,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::EntryQuoteFilled {
                filled_shares,
                cost,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_entry_quote_filled(
                filled_shares,
                cost,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::UnwindFilled {
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_unwind_filled(
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::UnwindFailed { error } => self.on_unwind_failed(error),
            Event::StopSellFilled {
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_stop_sell_filled(
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::StopSellFailed { error } => self.on_stop_sell_failed(error),
            Event::TimeoutSellFilled {
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            } => self.on_timeout_sell_filled(
                sold_shares,
                exit_price,
                signal_latency_ms,
                process_latency_ms,
            ),
            Event::TimeoutSellFailed { error } => self.on_timeout_sell_failed(error),
            Event::ApiResult { won } => self.on_api_result(won),
            Event::ApiResultTimeout { balance_increased } => {
                self.on_api_result_timeout(balance_increased)
            }
            Event::Control(c) => self.on_control(c),
            Event::Balance(b) => self.on_balance(b),
        }
    }

    fn on_cycle_open(&mut self, ctx: CycleContext, slug: String) -> Vec<Action> {
        // entry_suppressed (halt) is intentionally NOT touched here — it must only
        // change via Event::Control/Event::Balance. This used to take an
        // `entry_suppressed` parameter that live.rs's one real call site hardcoded to
        // `false` on every cycle open (every ~5 min), silently clearing any /halt set
        // via Telegram within minutes with no log line — see
        // trader/doc/incident_halt_reset_2026-07-03.md.
        self.saw_low_up.reset(&ctx);
        self.saw_low_dn.reset(&ctx);
        self.v_up.reset(&ctx);
        self.v_dn.reset(&ctx);
        self.delta_pct.reset(&ctx);
        match &mut self.kind {
            StrategyKind::Reversal(r) => r.reset(&ctx),
            StrategyKind::HighProb(hp) => hp.reset(&ctx),
            StrategyKind::VShape(v) => v.reset(&ctx),
        }
        self.cycle_open_binance = ctx.open_binance;
        self.last_binance = ctx.open_binance;
        self.cycle_start_ts = ctx.start_ts;
        self.cycle_end_ts = ctx.end_ts;
        self.cycle_slug = slug;
        // A fresh cycle never inherits an in-flight *position* from the last one (each
        // cycle's own hold/exit is fully resolved before the next opens) — but a pending
        // async Gamma confirmation (Confirming/EnrichOnly) is not a position and can
        // legitimately still be in flight here: `bin/live.rs`'s ticker fires this event
        // immediately after `CycleClose` closes a trade into `Confirming`, so resetting
        // unconditionally here clobbered every such confirmation within about a second of
        // it being set — see trader/doc/incident_DOGE_wrong_result_2026-07-09.md §3a/§4.
        if !matches!(
            self.state,
            WorkerState::Confirming(_) | WorkerState::EnrichOnly(_)
        ) {
            self.state = WorkerState::Watching;
        }
        // Loss-streak halt's own daily reset — independent of, and never
        // touched by, entry_suppressed (halt/resume, drawdown). Matches
        // backtest.rs::run_backtest's per-cycle `reset_if_new_session` call.
        let mut actions = vec![Action::Persist];
        if self.halt.reset_if_new_session(ctx.start_ts) {
            actions.push(Action::HaltReset);
        }
        actions
    }

    fn on_cycle_close(&mut self) -> Vec<Action> {
        // Any open position — Holding, or Unwinding/StopExiting/TimingOut that
        // hadn't resolved yet — is held to maturity: a failed/incomplete early
        // exit is not an exit (invariant). Only Holding's data is needed to
        // compute the WIN/LOSS outcome.
        let holding = match &self.state {
            WorkerState::Holding(h)
            | WorkerState::Unwinding(h)
            | WorkerState::StopExiting(h)
            | WorkerState::TimingOut(h) => Some(h.clone()),
            _ => None,
        };
        let Some(h) = holding else {
            // Same rationale as on_cycle_open: don't discard a still-pending Gamma
            // confirmation from the position that just closed (see §3a/§4 of the doc).
            if !matches!(
                self.state,
                WorkerState::Confirming(_) | WorkerState::EnrichOnly(_)
            ) {
                self.state = WorkerState::Watching;
            }
            return vec![];
        };

        let price_moved_up = self.last_binance > self.cycle_open_binance;
        let won = match h.side {
            Side::Up => price_moved_up,
            Side::Down => !price_moved_up,
        };
        let exit_price = if won { 1.0 } else { 0.0 };
        let pnl = settle_pnl(&h, exit_price);
        let outcome = if won { Outcome::Win } else { Outcome::Loss };

        let record = TradeRecord {
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name,
            side: h.side,
            entry_ts: h.entry_ts,
            entry_price_ts: h.entry_price_ts,
            token_price: h.token_price,
            exit_price,
            outcome,
            pnl,
            exit_attempts: h.exit_attempts,
            exit_last_error: h.exit_last_error.clone(),
            entry_signal_latency_ms: h.entry_signal_latency_ms,
            entry_process_latency_ms: h.entry_process_latency_ms,
            // No exit order was placed — the position resolved by natural market close.
            exit_signal_latency_ms: 0.0,
            exit_process_latency_ms: 0.0,
        };
        let halt_engaged = self.halt.record_trade(&record, self.strategy_name);
        // Held WIN/LOSS spawns Confirming — an ApiResult mismatch can still flip it.
        self.state = WorkerState::Confirming(record.clone());
        let mut actions = vec![Action::LogTrade(record), Action::Persist];
        if halt_engaged {
            actions.push(Action::HaltEngaged);
        }
        actions
    }

    fn on_binance(&mut self, tick: BinanceTick) -> Vec<Action> {
        self.delta_pct.on_binance(tick);
        self.latest_binance.on_binance(tick);
        self.last_binance = tick.price;
        self.last_binance_ts_value = tick.ts;

        self.try_enter(tick.ts)
    }

    /// Entry evaluation, shared by both `on_binance` and `on_poly` — poly price is
    /// the primary/time-critical signal (the trigger band/threshold), delta_pct is
    /// a directional filter. Gating this exclusively behind BinanceTick (as before)
    /// meant a poly price that crossed its trigger band between Binance ticks sat
    /// unnoticed for up to the Binance feed's own tick interval; calling this from
    /// on_poly too lets a poly-side crossing fire immediately using the latest
    /// cached delta_pct value (see trader/doc/latency_2026-07-04.md §8).
    fn try_enter(&mut self, now: f64) -> Vec<Action> {
        if self.entry_suppressed
            || self.halt.is_halted()
            || !matches!(self.state, WorkerState::Watching)
        {
            return vec![];
        }

        let intent = match &self.kind {
            StrategyKind::Reversal(r) => r.evaluate(
                now,
                &self.saw_low_up,
                &self.saw_low_dn,
                &self.latest_poly,
                &self.delta_pct,
                &self.latest_binance,
            ),
            StrategyKind::HighProb(hp) => hp.evaluate(
                now,
                &self.latest_poly,
                &self.delta_pct,
                &self.latest_binance,
            ),
            StrategyKind::VShape(v) => v.evaluate(
                now,
                &self.v_up,
                &self.v_dn,
                &self.latest_poly,
                &self.latest_binance,
            ),
        };
        let Some(intent) = intent else { return vec![] };

        if check_gates(
            &intent,
            &self.spread,
            &self.latest_poly,
            &self.delta_pct,
            &self.gate_params,
            now,
        )
        .is_some()
        {
            return vec![];
        }

        // Maker entry (plan_unwind_5u_maker_2026-07-19 §2.2), reversal only:
        // rest at the real observed best bid — "join the bid" per the source
        // MVP plan — not the signal's own mid. Falls back to mid when no
        // real bid/ask has been observed yet this run (a fresh
        // `LatestPolySignal`, an old price_feed predating this field, or
        // backtest replay). Computed once, up front, so the pup gate below
        // (§2.3) evaluates against the price that will *actually* be paid —
        // using mid there instead would make the veto needlessly strict
        // whenever the real bid undercuts it.
        let is_maker_reversal = self.maker_entry && matches!(self.kind, StrategyKind::Reversal(_));
        let entry_price = if is_maker_reversal {
            self.latest_poly
                .best_bid(intent.side)
                .unwrap_or_else(|| intent.token_price())
        } else {
            intent.token_price()
        };

        // p(up) negative-edge gate (plan_unwind_5u_maker_2026-07-19 §2.3),
        // reversal only, checked before `mark_fired()` so a block (like any
        // other gate block) doesn't lock the strategy out for the rest of
        // the cycle — a later tick with a more favorable/fresher p_up can
        // still fire. Both outcomes now block the entry outright (never
        // trade on stale information — CLAUDE.md "Trading principles",
        // trader/doc/plan_stale_data_gate_2026-07-20.md §1): there is no
        // "note and proceed anyway" path left.
        if matches!(self.kind, StrategyKind::Reversal(_))
            && let Some(min_edge) = self.pup_edge_min_rev
        {
            let fresh_p_up = self.pup_snapshot.and_then(|(p_up, snap_ts)| {
                (now - snap_ts <= PUP_GATE_MAX_AGE_SECS).then_some(p_up)
            });
            match fresh_p_up {
                None => {
                    return vec![Action::PupGateNote {
                        side: intent.side,
                        p_side: None,
                        price: entry_price,
                        outcome: PupGateOutcome::StaleBlocked,
                    }];
                }
                Some(p_up) => {
                    let p_side = match intent.side {
                        Side::Up => p_up,
                        Side::Down => 1.0 - p_up,
                    };
                    if p_side < entry_price + min_edge {
                        return vec![Action::PupGateNote {
                            side: intent.side,
                            p_side: Some(p_side),
                            price: entry_price,
                            outcome: PupGateOutcome::Veto,
                        }];
                    }
                }
            }
        }

        match &mut self.kind {
            StrategyKind::Reversal(r) => r.mark_fired(),
            StrategyKind::HighProb(hp) => hp.mark_fired(),
            StrategyKind::VShape(v) => v.mark_fired(),
        }

        if is_maker_reversal {
            self.state = WorkerState::EnteringMaker(MakerQuote {
                side: intent.side,
                entry_type: intent.entry_type,
                quote_price: entry_price,
                order_id: None,
                quoted_at: now,
            });
            return vec![
                Action::PlaceLimitBuy {
                    side: intent.side,
                    price: entry_price,
                    shares: MIN_GTC_SHARES,
                    signal_ts: now,
                },
                Action::Persist,
            ];
        }

        self.state = WorkerState::Entering;
        // Stash the intent's side/entry_type/token_price for when the fill lands.
        self.pending_entry = Some((intent.side, intent.entry_type, intent.token_price()));
        vec![
            Action::PlaceBuy {
                side: intent.side,
                price: intent.token_price(),
                size_usdc: self.trade_size,
                signal_ts: now,
            },
            Action::Persist,
        ]
    }

    /// Whether a resting maker-entry quote should be cancelled now: T-15s
    /// before cycle end takes priority (time-based, always fires regardless
    /// of price), otherwise signal invalidation — the reversal condition
    /// that justified the quote (price still above the reversal threshold)
    /// no longer holds, or a re-checked gate (spread/staleness/delta/price
    /// ceiling) now blocks. `strategy.fired` stays latched so `evaluate()`
    /// itself won't refire — these checks are independent of that latch and
    /// safe to re-run every tick.
    fn check_maker_quote_cancel(&mut self, q: MakerQuote, now: f64) -> Vec<Action> {
        const CANCEL_BEFORE_CYCLE_END_SECS: f64 = 15.0;
        if self.cycle_end_ts - now <= CANCEL_BEFORE_CYCLE_END_SECS {
            return self.cancel_maker_quote(q, CancelQuoteReason::CycleEndApproaching);
        }

        let up = self.latest_poly.up();
        let dn = self.latest_poly.dn();
        if up <= 0.0 || dn <= 0.0 {
            return vec![];
        }
        let side_price = match q.side {
            Side::Up => up,
            Side::Down => dn,
        };
        let reversal_still_holds = side_price > self.reversal_threshold;
        let intent = TradeIntent {
            side: q.side,
            entry_type: q.entry_type,
            up,
            dn,
            binance_price: self.latest_binance.value(),
        };
        let gates_still_pass = check_gates(
            &intent,
            &self.spread,
            &self.latest_poly,
            &self.delta_pct,
            &self.gate_params,
            now,
        )
        .is_none();

        if !reversal_still_holds || !gates_still_pass {
            return self.cancel_maker_quote(q, CancelQuoteReason::SignalInvalidated);
        }
        vec![]
    }

    fn cancel_maker_quote(&mut self, q: MakerQuote, reason: CancelQuoteReason) -> Vec<Action> {
        self.state = WorkerState::Watching;
        vec![
            Action::CancelEntryQuote {
                order_id: q.order_id,
                side: q.side,
                quote_price: q.quote_price,
                reason,
                quoted_at: q.quoted_at,
            },
            Action::Persist,
        ]
    }

    fn on_poly(&mut self, tick: PolyTick) -> Vec<Action> {
        self.latest_poly.on_poly(tick);
        self.spread.on_poly(tick);
        self.saw_low_up.on_poly(tick);
        self.saw_low_dn.on_poly(tick);
        self.v_up.on_poly(tick);
        self.v_dn.on_poly(tick);

        if let WorkerState::EnteringMaker(q) = &self.state {
            let q = q.clone();
            return self.check_maker_quote_cancel(q, tick.ts);
        }

        let WorkerState::Holding(h) = &self.state else {
            return self.try_enter(tick.ts);
        };
        let h = h.clone();
        let exit_price = if h.side == Side::Up { tick.up } else { tick.dn };

        // Stop-loss (both PnL-based and absolute) always fires off PolyTick,
        // regardless of exit_arm — cancel any resting GTC first, then FAK-close.
        let sl_hit = (self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl)
            || (self.sl > 0.0 && exit_price < self.sl);
        // Below MIN_SELLABLE_SHARES, any sell attempt is doomed regardless of price
        // (Polymarket's makerAmount floor — see the constant's doc comment / incident
        // doc §3), so don't bother placing one; leave it to resolve at cycle close.
        if sl_hit && h.shares >= MIN_SELLABLE_SHARES {
            self.state = WorkerState::StopExiting(h.clone());
            let mut actions = vec![];
            if let ExitArm::GtcResting { order_id } = &h.exit_arm {
                actions.push(Action::CancelLimitSell {
                    order_id: order_id.clone(),
                });
            }
            actions.push(Action::ClosePosition {
                shares: h.shares,
                reason: CloseReason::StopLoss,
                limit_price: None,
                signal_ts: tick.ts,
            });
            actions.push(Action::Persist);
            return actions;
        }

        // Take-profit: only the PriceMonitor arm reacts to PolyTick directly —
        // a GtcResting arm's fill arrives via UnwindFilled instead. Bounded at
        // tp_price itself (`limit_price: Some(tp_price)`) — the minimum
        // acceptable sell price is automatically the take-profit target, no
        // separate config needed (see trader/doc/incident_sol_unwind_but_loss_2026-07-06.md).
        if let ExitArm::PriceMonitor { tp_price } = h.exit_arm
            && exit_price >= tp_price
            && h.shares >= MIN_SELLABLE_SHARES
        {
            self.state = WorkerState::Unwinding(h.clone());
            return vec![
                Action::ClosePosition {
                    shares: h.shares,
                    reason: CloseReason::TakeProfit,
                    limit_price: Some(tp_price),
                    signal_ts: tick.ts,
                },
                Action::Persist,
            ];
        }

        // Max holding time — checked last, after stop-loss and take-profit both
        // fail to fire on this tick, matching the backtest's exit-chain order
        // exactly (see trader/doc/plan_unwind_time_2026-07-08.md). Force-closes
        // at whatever the current market price is, win or lose — `tick.ts` and
        // `h.entry_ts` are both wall-clock seconds from their own tick's receipt
        // (marketdata.rs), so no unit conversion is needed.
        if self.unwind_time > 0.0
            && (tick.ts - h.entry_ts) >= self.unwind_time
            && h.shares >= MIN_SELLABLE_SHARES
        {
            self.state = WorkerState::TimingOut(h.clone());
            let mut actions = vec![];
            if let ExitArm::GtcResting { order_id } = &h.exit_arm {
                actions.push(Action::CancelLimitSell {
                    order_id: order_id.clone(),
                });
            }
            actions.push(Action::ClosePosition {
                shares: h.shares,
                reason: CloseReason::Timeout,
                limit_price: None,
                signal_ts: tick.ts,
            });
            actions.push(Action::Persist);
            return actions;
        }

        vec![]
    }

    fn on_order_filled(
        &mut self,
        filled_shares: f64,
        cost: f64,
        entry_signal_latency_ms: f64,
        entry_process_latency_ms: f64,
    ) -> Vec<Action> {
        if !matches!(self.state, WorkerState::Entering) {
            return vec![];
        }
        let Some((side, entry_type, _intent_price)) = self.pending_entry.take() else {
            return vec![];
        };
        self.finalize_entry_fill(
            side,
            entry_type,
            filled_shares,
            cost,
            entry_signal_latency_ms,
            entry_process_latency_ms,
            false,
        )
    }

    /// Shared tail of `on_order_filled` (FAK entry) and `on_entry_quote_filled`/
    /// `on_limit_buy_placed`'s `Matched` branch (maker entry): build the
    /// resulting `Holding` — position lifecycle from here on is identical
    /// regardless of how the entry itself was filled. `via_maker_entry`
    /// distinguishes the two callers only for `Action::PlaceLimitSell`'s
    /// merged-notification scoping (see that variant's doc comment).
    #[allow(clippy::too_many_arguments)]
    fn finalize_entry_fill(
        &mut self,
        side: Side,
        entry_type: EntryType,
        filled_shares: f64,
        cost: f64,
        entry_signal_latency_ms: f64,
        entry_process_latency_ms: f64,
        via_maker_entry: bool,
    ) -> Vec<Action> {
        if filled_shares <= 0.0 {
            self.state = WorkerState::Watching;
            return vec![Action::Persist];
        }

        let tp_price = cost + self.unwind_pnl;
        // GTC is only legal at/above Polymarket's resting-order share minimum
        // (execution::choose_exit_order_kind — see trader/README.md); below
        // it, PriceMonitor's bounded FAK (execution::close_position_at_price)
        // is the only legal exit mechanism, not a fallback of convenience.
        let (exit_arm, mut actions) = if choose_exit_order_kind(filled_shares) == OrderKind::Gtc {
            // Attempt a resting GTC; the actual order_id/status comes back via
            // LimitSellPlaced. Use PriceMonitor as the provisional arm so a
            // stop-loss can still fire correctly if that response is slow.
            (
                ExitArm::PriceMonitor { tp_price },
                vec![Action::PlaceLimitSell {
                    shares: filled_shares,
                    price: tp_price,
                    side,
                    entry_price: cost,
                    via_maker_entry,
                }],
            )
        } else {
            (ExitArm::PriceMonitor { tp_price }, vec![])
        };

        let holding = HoldingData {
            side,
            entry_type,
            token_price: cost,
            entry_ts: self.last_binance_ts(),
            entry_price_ts: self.latest_poly.ts,
            shares: filled_shares,
            exit_arm,
            exit_attempts: 0,
            exit_last_error: None,
            realized_pnl: 0.0,
            fees: taker_fee(filled_shares, cost),
            entry_signal_latency_ms,
            entry_process_latency_ms,
        };
        self.state = WorkerState::Holding(holding);
        actions.push(Action::Persist);
        actions
    }

    fn on_order_rejected(&mut self) -> Vec<Action> {
        self.pending_entry = None;
        if matches!(self.state, WorkerState::Entering) {
            self.state = WorkerState::Watching;
        }
        vec![Action::Persist]
    }

    /// Response to `Action::PlaceLimitBuy` (maker entry).
    fn on_limit_buy_placed(
        &mut self,
        order_id: Option<String>,
        status: SellStatus,
        _error: Option<String>,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let WorkerState::EnteringMaker(q) = &mut self.state else {
            return vec![];
        };
        match status {
            SellStatus::Live => {
                q.order_id = order_id;
                vec![Action::Persist]
            }
            SellStatus::Matched => {
                // Crossed the book immediately (marketable limit, not a
                // resting quote) — finalize as a normal entry fill.
                let q = q.clone();
                self.finalize_entry_fill(
                    q.side,
                    q.entry_type,
                    MIN_GTC_SHARES,
                    q.quote_price,
                    signal_latency_ms,
                    process_latency_ms,
                    true,
                )
            }
            SellStatus::Failed | SellStatus::DryRun => {
                // Couldn't rest the quote at all — give up this cycle,
                // mirroring on_order_rejected's FAK-entry posture.
                self.state = WorkerState::Watching;
                vec![Action::Persist]
            }
        }
    }

    /// A resting maker-entry GTC BUY quote filled (paper driver's observed-
    /// price fill routing).
    fn on_entry_quote_filled(
        &mut self,
        filled_shares: f64,
        cost: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let WorkerState::EnteringMaker(q) = &self.state else {
            return vec![];
        };
        let q = q.clone();
        self.finalize_entry_fill(
            q.side,
            q.entry_type,
            filled_shares,
            cost,
            signal_latency_ms,
            process_latency_ms,
            true,
        )
    }

    fn on_limit_sell_placed(
        &mut self,
        order_id: Option<String>,
        status: SellStatus,
        error: Option<String>,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let WorkerState::Holding(h) = &mut self.state else {
            return vec![];
        };
        match status {
            SellStatus::Live => {
                if let Some(id) = order_id {
                    h.exit_arm = ExitArm::GtcResting { order_id: id };
                }
                vec![Action::Persist]
            }
            SellStatus::Matched => {
                // Marketable limit — filled immediately; this *is* the unwind.
                let mut h = h.clone();
                let exit_price = h.token_price + self.unwind_pnl;
                h.fees += taker_fee(h.shares, exit_price);
                let pnl = settle_pnl(&h, exit_price);
                let record = TradeRecord {
                    slug: self.cycle_slug.clone(),
                    cycle_start: self.cycle_start_ts,
                    strategy: self.strategy_name,
                    side: h.side,
                    entry_ts: h.entry_ts,
                    entry_price_ts: h.entry_price_ts,
                    token_price: h.token_price,
                    exit_price,
                    outcome: Outcome::Unwind,
                    pnl,
                    exit_attempts: h.exit_attempts,
                    exit_last_error: h.exit_last_error.clone(),
                    entry_signal_latency_ms: h.entry_signal_latency_ms,
                    entry_process_latency_ms: h.entry_process_latency_ms,
                    exit_signal_latency_ms: signal_latency_ms,
                    exit_process_latency_ms: process_latency_ms,
                };
                self.halt.record_trade(&record, self.strategy_name);
                self.state = WorkerState::EnrichOnly(record.clone());
                vec![Action::LogTrade(record), Action::Persist]
            }
            SellStatus::Failed | SellStatus::DryRun => {
                // Fall back to price-monitor backstop; stop-loss stays armed regardless.
                let tp_price = h.token_price + self.unwind_pnl;
                h.exit_arm = ExitArm::PriceMonitor { tp_price };
                h.exit_attempts += 1;
                h.exit_last_error = error;
                vec![Action::Persist]
            }
        }
    }

    fn on_unwind_filled(
        &mut self,
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        // `Unwinding`: the PriceMonitor arm's bounded FAK close just filled.
        // `Holding` with a `GtcResting` arm: the resting GTC take-profit
        // itself filled (delivered by the paper driver's fill routing — the
        // live path never produces this today, resting exits being
        // defensive/unexercised at sub-5-share sizes; see the file header).
        let h = match &self.state {
            WorkerState::Unwinding(h) => h.clone(),
            WorkerState::Holding(h) if matches!(h.exit_arm, ExitArm::GtcResting { .. }) => {
                h.clone()
            }
            _ => return vec![],
        };
        self.finalize_or_hold_residual(
            h,
            sold_shares,
            exit_price,
            Outcome::Unwind,
            signal_latency_ms,
            process_latency_ms,
        )
    }

    /// Shared tail of `on_unwind_filled`/`on_stop_sell_filled`: bank this fill's
    /// (fee-inclusive) result, then either keep holding a genuine residual or
    /// close the trade out now. A leftover under `MIN_SELLABLE_SHARES` is
    /// written off here rather than carried forward — Polymarket's `makerAmount`
    /// floor means it can never itself be sold (see `MIN_SELLABLE_SHARES` doc
    /// comment / `trader/doc/incident_tele_pnl_2026-07-04.md` §3), and it's
    /// excluded from `pnl` rather than valued at cycle-close's `exit_price` (§2's
    /// "don't count unsellable dust as won-at-$1" fix).
    fn finalize_or_hold_residual(
        &mut self,
        h: HoldingData,
        sold_shares: f64,
        exit_price: f64,
        outcome: Outcome,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let sell_fee = taker_fee(sold_shares, exit_price);
        let realized_pnl = h.realized_pnl + sold_shares * (exit_price - h.token_price);
        let fees = h.fees + sell_fee;
        let leftover = h.shares - sold_shares;

        if leftover >= MIN_SELLABLE_SHARES {
            // Genuine partial fill — residual continues to be managed.
            let residual = HoldingData {
                shares: leftover,
                realized_pnl,
                fees,
                ..h
            };
            self.state = WorkerState::Holding(residual);
            return vec![Action::Persist];
        }

        // Fully sold, or left with an un-sellable dust remainder that's written
        // off (excluded — not added as a `leftover * (exit_price - cost)` term).
        let pnl = round4(realized_pnl - fees);
        let record = TradeRecord {
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name,
            side: h.side,
            entry_ts: h.entry_ts,
            entry_price_ts: h.entry_price_ts,
            token_price: h.token_price,
            exit_price,
            outcome,
            pnl,
            exit_attempts: h.exit_attempts,
            exit_last_error: h.exit_last_error.clone(),
            entry_signal_latency_ms: h.entry_signal_latency_ms,
            entry_process_latency_ms: h.entry_process_latency_ms,
            exit_signal_latency_ms: signal_latency_ms,
            exit_process_latency_ms: process_latency_ms,
        };
        let halt_engaged = self.halt.record_trade(&record, self.strategy_name);
        self.state = WorkerState::EnrichOnly(record.clone());
        let mut actions = vec![Action::LogTrade(record), Action::Persist];
        if halt_engaged {
            actions.push(Action::HaltEngaged);
        }
        actions
    }

    fn on_unwind_failed(&mut self, error: Option<String>) -> Vec<Action> {
        // A failed sell is not an exit — reclassify as held, re-armed at the
        // same tp_price for the next PolyTick to try again. This used to latch
        // into a one-shot "abandoned" state instead (never retrying) because
        // the old exit path was an *unbounded* market order: retrying every
        // tick while price stayed above tp_price meant hammering the CLOB with
        // no backoff, 284 attempts in ~9s in the incident that motivated that
        // design (trader/doc/incident_doge_2026-07-03.md). Now that the exit is
        // execution::close_position_at_price (a single attempt, bounded at
        // tp_price — see trader/doc/incident_sol_unwind_but_loss_2026-07-06.md),
        // re-arming is safe: each retry is gated on a real PolyTick (natural
        // backoff, not an internal loop) and can never fill worse than
        // tp_price. Stop-loss remains fully live regardless (on_poly's sl_hit
        // check doesn't gate on exit_arm at all).
        if let WorkerState::Unwinding(h) = &self.state {
            let mut h = h.clone();
            h.exit_attempts += 1;
            h.exit_last_error = error;
            h.exit_arm = ExitArm::PriceMonitor {
                tp_price: h.token_price + self.unwind_pnl,
            };
            self.state = WorkerState::Holding(h);
        }
        vec![Action::Persist]
    }

    fn on_stop_sell_filled(
        &mut self,
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let WorkerState::StopExiting(h) = &self.state else {
            return vec![];
        };
        let h = h.clone();
        // Absolute-SL-style pnl (proceeds − cost basis of whatever's still
        // held, plus anything already realized from an earlier partial
        // fill) — PnL-SL is computed at trigger time in on_poly in a live
        // system, but here we use the realized exit price uniformly,
        // matching the sim/backtest STOPLOSS formula.
        self.finalize_or_hold_residual(
            h,
            sold_shares,
            exit_price,
            Outcome::StopLoss,
            signal_latency_ms,
            process_latency_ms,
        )
    }

    fn on_stop_sell_failed(&mut self, error: Option<String>) -> Vec<Action> {
        if let WorkerState::StopExiting(h) = &self.state {
            let mut h = h.clone();
            h.exit_attempts += 1;
            h.exit_last_error = error;
            self.state = WorkerState::Holding(h);
        }
        vec![Action::Persist]
    }

    fn on_timeout_sell_filled(
        &mut self,
        sold_shares: f64,
        exit_price: f64,
        signal_latency_ms: f64,
        process_latency_ms: f64,
    ) -> Vec<Action> {
        let WorkerState::TimingOut(h) = &self.state else {
            return vec![];
        };
        let h = h.clone();
        self.finalize_or_hold_residual(
            h,
            sold_shares,
            exit_price,
            Outcome::Timeout,
            signal_latency_ms,
            process_latency_ms,
        )
    }

    fn on_timeout_sell_failed(&mut self, error: Option<String>) -> Vec<Action> {
        // Falls back to Holding exactly like on_stop_sell_failed — on_poly's
        // timeout condition (tick.ts - h.entry_ts >= unwind_time) stays true
        // (more true, as time passes), so the next PolyTick naturally re-fires
        // the close attempt with no separate retry-counter needed.
        if let WorkerState::TimingOut(h) = &self.state {
            let mut h = h.clone();
            h.exit_attempts += 1;
            h.exit_last_error = error;
            self.state = WorkerState::Holding(h);
        }
        vec![Action::Persist]
    }

    fn on_api_result(&mut self, won: bool) -> Vec<Action> {
        match &self.state {
            WorkerState::Confirming(original) => {
                let flip_needed = won != (original.outcome == Outcome::Win);
                if !flip_needed {
                    let note = format!(
                        "{}: Gamma confirmed provisional {:?} — no correction needed",
                        original.slug, original.outcome
                    );
                    self.state = WorkerState::Watching;
                    return vec![Action::ApiResultNote(note), Action::Persist];
                }
                let previous_outcome = original.outcome;
                let previous_pnl = original.pnl;
                let mut record = original.clone();
                self.state = WorkerState::Watching;
                let shares = self.trade_size / record.token_price;
                let exit_price = if won { 1.0 } else { 0.0 };
                record.outcome = if won { Outcome::Win } else { Outcome::Loss };
                record.exit_price = exit_price;
                // Resolution/redemption itself is fee-free (§2 of the incident doc) —
                // only the original entry BUY's taker fee applies here.
                record.pnl = round4(
                    shares * exit_price - self.trade_size - taker_fee(shares, record.token_price),
                );
                // `record_trade` already counted `previous_outcome` once at cycle-close
                // time — undo/apply the delta now that Gamma has overruled it, or a
                // provisional LOSS later confirmed as a WIN overcounts the loss streak
                // forever (trader/doc/incident_halt_double_count_2026-07-10.md).
                let correction = self.halt.correct_trade(
                    previous_outcome,
                    previous_pnl,
                    record.outcome,
                    record.pnl,
                );
                let mut actions = vec![
                    Action::LogTradeCorrection {
                        previous_outcome,
                        previous_pnl,
                        record,
                    },
                    Action::Persist,
                ];
                match correction {
                    HaltCorrection::Engaged => actions.push(Action::HaltEngaged),
                    HaltCorrection::Cleared => actions.push(Action::HaltClearedByCorrection),
                    HaltCorrection::Unchanged => {}
                }
                actions
            }
            WorkerState::EnrichOnly(record) => {
                // Column-only enrichment: never rewrites pnl/result/halt. A
                // counterfactual good/costly verdict only makes sense for an
                // actual stop-loss exit — an unwind (take-profit) already
                // exited on purpose at a profit, so it gets no verdict
                // (mirrors Python's explicit `if is_unwind: continue` skip).
                // `won` is already relative to this record's own side (same
                // convention as the Confirming branch above), so no further
                // relativizing against `record.side` here.
                let verdict = if record.outcome == Outcome::StopLoss {
                    Some(Action::StopLossVerdict {
                        record: record.clone(),
                        would_have_won: won,
                    })
                } else {
                    None
                };
                self.state = WorkerState::Watching;
                match verdict {
                    Some(action) => vec![action, Action::Persist],
                    None => vec![Action::Persist],
                }
            }
            _ => vec![Action::ApiResultNote(format!(
                "ApiResult arrived while state was {:?} — ignoring (stale?)",
                self.state
            ))],
        }
    }

    /// Gamma never resolved within the retry deadline. `Confirming` (a Win/Loss whose
    /// pnl/outcome are still provisional) halts new entries rather than keep an
    /// unverified result — see trader/doc/incident_DOGE_wrong_result_2026-07-09.md §4 —
    /// *unless* `balance_increased`, in which case the account has grown since last
    /// cycle's checkpoint regardless of this one unresolved trade, and entries continue
    /// instead of halting (2026-07-09). `EnrichOnly` (a StopLoss/Unwind/Timeout whose
    /// pnl/outcome are already final — this is only for the advisory counterfactual
    /// verdict) just gives up quietly either way; balance never enters into it.
    fn on_api_result_timeout(&mut self, balance_increased: bool) -> Vec<Action> {
        match &self.state {
            WorkerState::Confirming(record) => {
                let record = record.clone();
                self.state = WorkerState::Watching;
                if balance_increased {
                    vec![
                        Action::GammaUnresolvedContinued {
                            record,
                            entry_suppressed: self.entry_suppressed,
                        },
                        Action::Persist,
                    ]
                } else {
                    self.entry_suppressed = true;
                    vec![Action::GammaHaltEngaged { record }, Action::Persist]
                }
            }
            WorkerState::EnrichOnly(record) => {
                let note = format!(
                    "{}: gave up waiting for Gamma resolution — no counterfactual verdict",
                    record.slug
                );
                self.state = WorkerState::Watching;
                vec![Action::ApiResultNote(note), Action::Persist]
            }
            _ => vec![Action::ApiResultNote(format!(
                "ApiResultTimeout arrived while state was {:?} — ignoring (stale?)",
                self.state
            ))],
        }
    }

    fn on_control(&mut self, event: ControlEvent) -> Vec<Action> {
        match event {
            ControlEvent::Halt => self.entry_suppressed = true,
            ControlEvent::Resume => self.entry_suppressed = false,
            ControlEvent::ResetLosses => self.halt.reset_losses(),
        }
        // No state change — halt/resume never touch an in-flight position.
        // `Action::Persist` still fires so `entry_suppressed` reaches disk
        // immediately rather than waiting for the next trade-lifecycle event
        // (up to ~5 min away at the next cycle open) — see
        // trader/doc/incident_no_reset_notification_2026-07-08.md.
        vec![Action::Persist]
    }

    fn on_balance(&mut self, event: BalanceEvent) -> Vec<Action> {
        match event {
            BalanceEvent::DrawdownHalt => self.entry_suppressed = true,
        }
        vec![Action::Persist]
    }

    fn last_binance_ts(&self) -> f64 {
        // Placeholder for "now" in a live system; tests set this indirectly by
        // checking entry_ts only where it matters for pnl math, not timing.
        self.last_binance_ts_value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BinanceTick, PolyTick};

    fn btc_params() -> AssetParams {
        AssetParams {
            asset: "BTC".to_string(),
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
            unwind_pnl_v: 0.05,
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
            max_price_age_secs: 300.0, // large for unit tests; real config: 2.0
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

    /// Drives a worker from cycle-open through a filled DOWN reversal entry,
    /// returning it positioned in `Holding` with the given `filled_shares`.
    /// Delta_pct is deliberately the *last* piece to arrive (fires on the final
    /// BinanceTick) — see `entry_fires_on_poly_tick_using_cached_delta` for the
    /// complementary case where poly is last and delta_pct is already cached.
    fn enter_down_position(w: &mut Worker, filled_shares: f64) {
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // dip latches saw_low_dn
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // recovery > reversal 0.60; delta_pct not yet known, no fire
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        })); // dp < 0 -> fires entry
        assert!(
            matches!(
                actions.as_slice(),
                [Action::PlaceBuy { .. }, Action::Persist]
            ),
            "expected entry to fire: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Entering));
        w.step(Event::OrderFilled {
            filled_shares,
            cost: 0.70,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
    }

    #[test]
    fn entry_fires_and_transitions_to_entering() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // delta_pct not yet known, no fire
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        })); // dp < 0 -> fires
        assert_eq!(
            actions,
            vec![
                Action::PlaceBuy {
                    side: Side::Down,
                    price: 0.70,
                    size_usdc: 1.0,
                    signal_ts: 1250.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::Entering));
    }

    /// v_shape (2026-07-17, trader/doc/plan_v_shape_trader_2026-07-17.md): the full
    /// high1→low→high2 poly sequence alone fires the entry — no BinanceTick this
    /// cycle at all (delta_pct_v=0.0 disables the delta gate; the strategy itself
    /// never reads delta). Also checks the per-cycle latch reset: the same recovery
    /// price in a fresh cycle without a fresh prefix must NOT fire.
    #[test]
    fn v_shape_entry_fires_on_pure_poly_sequence_and_resets_per_cycle() {
        let p = btc_params();
        let mut w = Worker::new_v_shape("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1100.0,
            up: 0.75,
            dn: 0.25,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // high1 latched
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.25,
            dn: 0.75,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // low-after-high latched
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.70,
            dn: 0.30,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // >= high2 -> entry, no binance tick ever fed
        assert_eq!(
            actions,
            vec![
                Action::PlaceBuy {
                    side: Side::Up,
                    price: 0.70,
                    size_usdc: 1.0,
                    signal_ts: 1240.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::Entering));

        // Reject the order so the worker returns to Watching, then open a new
        // cycle: last cycle's latched prefix must be gone.
        w.step(Event::OrderRejected);
        w.step(Event::CycleOpen {
            ctx: ctx(1_300.0),
            slug: "btc-updown-5m-1300".to_string(),
        });
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1400.0,
            up: 0.72,
            dn: 0.28,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(
            actions.is_empty(),
            "stale prefix from the previous cycle must not fire: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// Complementary case to `entry_fires_and_transitions_to_entering`: delta_pct
    /// is already known (set by an earlier BinanceTick this cycle) by the time poly
    /// recovers, so the entry must fire immediately off the PolyTick itself — no
    /// further BinanceTick required. This is the behavior change from
    /// trader/doc/latency_2026-07-04.md §8 ("trigger entry on poly ticks too").
    #[test]
    fn entry_fires_on_poly_tick_using_cached_delta() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // dip latches saw_low_dn
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        })); // dp < 0, cached
        // No further BinanceTick — the poly recovery tick alone must fire the entry.
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert_eq!(
            actions,
            vec![
                Action::PlaceBuy {
                    side: Side::Down,
                    price: 0.70,
                    size_usdc: 1.0,
                    signal_ts: 1240.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::Entering));
    }

    /// A cached delta_pct must only be trusted within the *same* cycle it was set
    /// in — DeltaPctSignal::reset() clears `price` on every CycleOpen, so a value
    /// left over from a previous cycle can't masquerade as "ready" this cycle.
    #[test]
    fn poly_tick_does_not_fire_using_stale_cross_cycle_delta() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1100.0,
            price: 59_900.0,
        })); // dp < 0, this cycle

        // New cycle: delta_pct is reset, even though the old Binance price is
        // still the most recent one this worker has ever seen.
        w.step(Event::CycleOpen {
            ctx: ctx(1_500.0),
            slug: "btc-updown-5m-1500".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1680.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // dip latches saw_low_dn
        // Recovery with NO BinanceTick yet this cycle — must not fire off stale dp.
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1740.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(
            actions.is_empty(),
            "must not fire on a delta_pct left over from the previous cycle: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn small_fill_uses_price_monitor_arm() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 3.0); // < 5 shares
        match &w.state {
            WorkerState::Holding(h) => {
                assert_eq!(h.shares, 3.0);
                assert!(
                    matches!(h.exit_arm, ExitArm::PriceMonitor { .. }),
                    "expected PriceMonitor arm, got {:?}",
                    h.exit_arm
                );
            }
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn large_fill_attempts_gtc_limit_sell() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        let actions = {
            w.step(Event::CycleOpen {
                ctx: ctx(1_000.0),
                slug: "btc-updown-5m-1000".to_string(),
            });
            w.step(Event::PolyTick(PolyTick {
                ts: 1180.0,
                up: 0.85,
                dn: 0.15,
                up_bid: 0.0,
                up_ask: 0.0,
            }));
            w.step(Event::BinanceTick(BinanceTick {
                ts: 1200.0,
                price: 59_900.0,
            }));
            w.step(Event::PolyTick(PolyTick {
                ts: 1240.0,
                up: 0.30,
                dn: 0.70,
                up_bid: 0.0,
                up_ask: 0.0,
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
            })
        };
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::PlaceLimitSell { shares, .. } if *shares == 10.0)),
            "expected a PlaceLimitSell action for a >=5 share fill: {actions:?}"
        );
    }

    #[test]
    fn limit_sell_live_arms_gtc_resting() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced {
            order_id: Some("order-123".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        match &w.state {
            WorkerState::Holding(h) => assert_eq!(
                h.exit_arm,
                ExitArm::GtcResting {
                    order_id: "order-123".to_string()
                }
            ),
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn limit_sell_failed_falls_back_to_price_monitor() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced {
            order_id: None,
            status: SellStatus::Failed,
            error: Some("test error".to_string()),
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        match &w.state {
            WorkerState::Holding(h) => assert!(matches!(h.exit_arm, ExitArm::PriceMonitor { .. })),
            _ => panic!("expected Holding"),
        }
    }

    /// The live binary parses `halt_rev`/`halt_prob` (consecutive-loss count)
    /// and `halt_reset_hour_rev`/`halt_reset_hour_hp` (daily HKT reset hour)
    /// out of the strategy TOML, but nothing ever consumed them before this
    /// fix — `entry_suppressed` was only ever set by `/halt` or the balance
    /// drawdown guard, so a losing streak never actually halted live trading.
    /// This reproduces `btc_params()`'s `halt_rev = 2`: two losses in the same
    /// HKT session must suppress further entries, and a cycle opening in the
    /// next session (per `halt_reset_hour_rev`) must clear it again — mirrors
    /// `backtest.rs::HaltTracker`, which already did this correctly in the
    /// backtest path only.
    #[test]
    fn halt_by_loss_streak_suppresses_entry_and_resets_next_session() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        // Loss 1 (same session as ctx(1_000.0) inside enter_down_position).
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        })); // now above open -> DOWN loses
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Loss);
        assert!(!w.is_halted(), "1 loss must not halt yet (halt_rev=2)");
        assert!(
            !actions.contains(&Action::HaltEngaged),
            "1st loss must not emit HaltEngaged yet: {actions:?}"
        );

        // Confirming now legitimately survives a cycle boundary (see
        // trader/doc/incident_DOGE_wrong_result_2026-07-09.md §4) — resolve it
        // (API agrees, no flip) before the next entry so it isn't still blocking
        // try_enter for this test's unrelated loss-streak assertion.
        w.step(Event::ApiResult { won: false });

        // Loss 2, same session (enter_down_position reopens the same
        // ctx(1_000.0) cycle) -> hits halt_rev's threshold of 2.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Loss);
        assert!(w.is_halted(), "2nd loss must trip halt_rev=2");
        assert!(
            actions.contains(&Action::HaltEngaged),
            "2nd loss must emit HaltEngaged: {actions:?}"
        );
        // Resolve loss 2's Confirming too (see the note above) — the loss-streak
        // halt below comes from HaltTracker/entry_suppressed, not from Confirming
        // still being outstanding, so this doesn't affect what's being tested.
        w.step(Event::ApiResult { won: false });

        // A new cycle in the *same* session must not clear it, and entries
        // must actually be suppressed now (not just is_halted() reporting true).
        let actions = w.step(Event::CycleOpen {
            ctx: ctx(1_500.0),
            slug: "btc-updown-5m-1500".to_string(),
        });
        assert!(
            w.is_halted(),
            "halt must survive a same-session cycle boundary"
        );
        assert!(
            !actions.contains(&Action::HaltReset),
            "same-session cycle open must not emit HaltReset: {actions:?}"
        );
        w.step(Event::PolyTick(PolyTick {
            ts: 1680.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1700.0,
            price: 59_900.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1740.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1750.0,
            price: 59_900.0,
        }));
        assert!(
            actions.is_empty(),
            "entry must be suppressed while halted, got {actions:?}"
        );

        // A cycle opening in the *next* HKT session (start_ts +100_000s, well
        // over a day later, guaranteed to cross halt_reset_hour_rev's boundary
        // regardless of time-of-day) must clear the halt.
        let actions = w.step(Event::CycleOpen {
            ctx: ctx(101_000.0),
            slug: "btc-updown-5m-101000".to_string(),
        });
        assert!(
            !w.is_halted(),
            "halt must clear once a new HKT session opens"
        );
        assert!(
            actions.contains(&Action::HaltReset),
            "session rollover clearing an active halt must emit HaltReset: {actions:?}"
        );
        w.step(Event::PolyTick(PolyTick {
            ts: 101_180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 101_240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // delta_pct not yet known this cycle, no fire
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 101_250.0,
            price: 59_900.0,
        })); // dp < 0 -> fires
        assert!(
            matches!(
                actions.as_slice(),
                [Action::PlaceBuy { .. }, Action::Persist]
            ),
            "entry must fire again once the halt has cleared for the new session: {actions:?}"
        );
    }

    /// Regression test for trader/doc/incident_halt_double_count_2026-07-10.md:
    /// a provisional LOSS that Gamma later corrects to a WIN must not leave a
    /// phantom loss in the halt streak — a single subsequent real loss must not
    /// trip halt_rev/halt_prob=2 on its own.
    #[test]
    fn halt_correction_undoes_phantom_loss_so_one_real_loss_does_not_halt() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        // Cycle #1: provisional LOSS at close (record_trade counts it: 0 -> 1)...
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        })); // now above open -> DOWN loses
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Loss);

        // ...but Gamma actually says it WON -> the correction must undo that count.
        let actions = w.step(Event::ApiResult { won: true });
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::LogTradeCorrection { .. })),
            "expected a correction: {actions:?}"
        );
        assert!(
            !w.is_halted(),
            "a corrected-to-WIN loss must not leave the halt engaged"
        );

        // Cycle #2: one genuine loss. Without the fix, the phantom count from
        // cycle #1 would still be sitting at 1, so this would wrongly trip
        // halt_rev=2. With the fix, this is the *first* real loss of the session.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Loss);
        assert!(
            !actions.contains(&Action::HaltEngaged),
            "one real loss after a corrected phantom must not trip halt_rev=2: {actions:?}"
        );
        assert!(
            !w.is_halted(),
            "halt must not engage on a single real loss after the phantom was corrected away"
        );
    }

    /// Symmetric case: a provisional WIN that Gamma later corrects to a LOSS must
    /// still count toward the halt streak, even though `record_trade` never saw
    /// it as a loss at cycle-close time.
    #[test]
    fn halt_correction_engages_halt_when_provisional_win_flips_to_loss() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        // Real loss #1, resolved without a flip -> losses: 0 -> 1.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        w.step(Event::CycleClose);
        assert!(!w.is_halted(), "1 real loss must not halt yet (halt_rev=2)");
        w.step(Event::ApiResult { won: false }); // agrees, no flip

        // Cycle #2 provisionally WINs at close (price stayed below open)...
        enter_down_position(&mut w, 10.0);
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Win);
        assert!(!w.is_halted());

        // ...but Gamma says it actually lost -> the correction must push losses
        // 1 -> 2, crossing halt_rev's threshold even though this trade was never
        // separately counted at cycle-close time.
        let actions = w.step(Event::ApiResult { won: false });
        assert!(
            actions.contains(&Action::HaltEngaged),
            "a Win->Loss correction crossing the threshold must emit HaltEngaged: {actions:?}"
        );
        assert!(
            w.is_halted(),
            "the corrected loss must actually halt entries"
        );
    }

    /// A correction can also clear a halt that had *already* engaged, if it
    /// turns out one of the counted losses wasn't real.
    #[test]
    fn halt_correction_clears_an_already_engaged_halt() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        // Loss #1, resolved without a flip (API agrees) -> losses: 0 -> 1.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        w.step(Event::CycleClose);
        w.step(Event::ApiResult { won: false });
        assert!(!w.is_halted());

        // Loss #2 trips halt_rev=2; Confirming(Loss) is left outstanding.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        let actions = w.step(Event::CycleClose);
        assert!(actions.contains(&Action::HaltEngaged));
        assert!(w.is_halted());

        // Gamma actually says loss #2 WON -> the correction pulls losses back
        // to 1, clearing the halt it had just tripped.
        let actions = w.step(Event::ApiResult { won: true });
        assert!(
            actions.contains(&Action::HaltClearedByCorrection),
            "a correction pulling losses back below threshold must clear the halt: {actions:?}"
        );
        assert!(
            !w.is_halted(),
            "halt must actually be cleared, not just the action emitted"
        );
    }

    /// A cycle opening in a fresh HKT session must not emit `Action::HaltReset`
    /// when the loss-streak halt was never active to begin with — this would
    /// otherwise fire a "halt reset" Telegram notification every single day at
    /// `halt_reset_hour_rev` regardless of whether anything actually happened.
    #[test]
    fn halt_reset_on_session_rollover_with_no_active_halt_is_silent() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        assert!(!w.is_halted());

        let actions = w.step(Event::CycleOpen {
            ctx: ctx(101_000.0),
            slug: "btc-updown-5m-101000".to_string(),
        });
        assert!(!w.is_halted());
        assert!(
            !actions.contains(&Action::HaltReset),
            "session rollover with nothing to clear must stay silent: {actions:?}"
        );
    }

    #[test]
    fn halt_mid_holding_does_not_abort_the_position() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        assert!(w.has_open_position());

        w.step(Event::Control(ControlEvent::Halt));
        assert!(w.is_halted());
        // Position must still be intact and still exit-managed.
        assert!(matches!(w.state, WorkerState::Holding(_)));

        // Stop-loss still fires while halted (SL floor: entry 0.70 - sl_pnl 0.20 = 0.50).
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.55,
            dn: 0.45,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ClosePosition { .. }))
        );
    }

    #[test]
    fn halt_suppresses_only_new_entries() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::Control(ControlEvent::Halt));
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }));
        assert!(
            actions.is_empty(),
            "halted worker must not enter: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// Reproduces the 2026-07-03 17:36 incident (trader/doc/incident_halt_reset_2026-07-03.md):
    /// /halt was sent via Telegram but the bot entered a new trade at the next cycle
    /// boundary anyway. Root cause was `Event::CycleOpen` carrying an
    /// `entry_suppressed` parameter that live.rs's real call site hardcoded to
    /// `false` every single cycle open, silently clearing any halt within one cycle
    /// (~5 min). Fix: `CycleOpen` no longer carries that field at all — halt can only
    /// change via `Event::Control`/`Event::Balance`, so it now survives across
    /// however many cycle boundaries pass until an explicit `/resume`.
    #[test]
    fn halt_survives_multiple_cycle_boundaries() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::Control(ControlEvent::Halt));
        assert!(w.is_halted());

        for i in 0..5 {
            w.step(Event::CycleOpen {
                ctx: ctx(1_000.0 + i as f64 * 300.0),
                slug: format!("btc-updown-5m-{}", 1_000 + i * 300),
            });
            assert!(w.is_halted(), "halt must survive cycle boundary {i}");
        }

        w.step(Event::Control(ControlEvent::Resume));
        assert!(!w.is_halted(), "/resume must still clear the halt");
    }

    /// Reproduces trader/doc/incident_unable_to_resume_2026-07-15.md: with
    /// `halt_rev=1` (2026-07-13/07-15 config), a single stop-loss trips the
    /// loss-streak halt via `record_trade`, not `/halt`. `/resume` only ever
    /// clears `entry_suppressed` (§8) — it must NOT silently clear the
    /// loss-streak counter too, no matter how many times it's sent — that
    /// would defeat the point of a separate risk-halt.
    #[test]
    fn resume_does_not_clear_a_loss_streak_halt() {
        let mut p = btc_params();
        p.halt_rev = 1;
        let mut w = Worker::new_reversal("BTC", &p);
        w.restore_halt(false, 1, None); // 1 loss this session, halt_rev=1 -> tripped
        assert!(w.is_halted(), "sanity: loss-streak halt should be engaged");
        assert!(!w.manually_suppressed());
        assert!(w.loss_streak_halted());

        for _ in 0..3 {
            w.step(Event::Control(ControlEvent::Resume));
            assert!(
                w.is_halted(),
                "/resume must never clear a loss-streak halt on its own, however many times it's sent"
            );
        }
    }

    /// Companion to `resume_does_not_clear_a_loss_streak_halt`: `/reset_losses`
    /// is the command actually meant to clear this gate (per `HELP_TEXT`), and
    /// must not disturb an unrelated manual `/halt` sitting alongside it.
    #[test]
    fn reset_losses_clears_loss_streak_but_not_manual_halt() {
        let mut p = btc_params();
        p.halt_rev = 1;
        let mut w = Worker::new_reversal("BTC", &p);
        w.restore_halt(true, 1, None); // manual halt AND a tripped loss-streak
        assert!(w.manually_suppressed());
        assert!(w.loss_streak_halted());
        assert!(w.is_halted());

        w.step(Event::Control(ControlEvent::ResetLosses));
        assert!(
            !w.loss_streak_halted(),
            "/reset_losses must clear the loss-streak counter"
        );
        assert_eq!(w.halt_losses(), 0);
        assert!(
            w.manually_suppressed(),
            "/reset_losses must not touch an unrelated manual halt"
        );
        assert!(
            w.is_halted(),
            "still halted overall — the manual gate is still up until /resume"
        );

        w.step(Event::Control(ControlEvent::Resume));
        assert!(
            !w.is_halted(),
            "clearing both gates in turn must fully un-halt the worker"
        );
    }

    /// `/reset_losses` on an already-clear loss streak (nothing to reset) must
    /// be a harmless no-op, not e.g. push the counter negative.
    #[test]
    fn reset_losses_is_a_no_op_when_not_halted() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        assert!(!w.is_halted());

        w.step(Event::Control(ControlEvent::ResetLosses));
        assert!(!w.is_halted());
        assert_eq!(w.halt_losses(), 0);
    }

    /// trader/doc/incident_no_reset_notification_2026-07-08.md: `/halt`,
    /// `/resume`, and the balance-drawdown halt used to return no actions at
    /// all, so a halt/resume only reached `live_state_*.json` whenever the
    /// *next* trade-lifecycle event happened to persist — up to ~5 minutes
    /// away at the next cycle open. A restart in that window silently lost
    /// the just-issued halt. Both must now flush to disk immediately.
    #[test]
    fn control_and_balance_events_persist_immediately() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        assert_eq!(
            w.step(Event::Control(ControlEvent::Halt)),
            vec![Action::Persist]
        );
        assert_eq!(
            w.step(Event::Control(ControlEvent::Resume)),
            vec![Action::Persist]
        );
        assert_eq!(
            w.step(Event::Balance(BalanceEvent::DrawdownHalt)),
            vec![Action::Persist]
        );
        assert_eq!(
            w.step(Event::Control(ControlEvent::ResetLosses)),
            vec![Action::Persist]
        );
    }

    /// A process restart rebuilds every `Worker` from scratch via
    /// `new_reversal`/`new_high_prob`, which always starts un-halted
    /// (trader/doc/incident_no_reset_notification_2026-07-08.md). This
    /// reproduces that restart across both halt mechanisms — manual/drawdown
    /// (`entry_suppressed`) and the loss-streak counter (`HaltTracker`) — via
    /// `to_persisted`/`restore_halt`, and confirms the restored worker behaves
    /// exactly as the original would have: still halted, survives a
    /// same-session cycle boundary, and still clears on the next daily reset.
    #[test]
    fn halt_state_round_trips_across_a_restart() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);

        // Trip the loss-streak halt (halt_rev=2) — leaves entry_suppressed
        // untouched (false) so this also exercises halt_losses/halt_last_session
        // independently of the manual-halt flag.
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        w.step(Event::CycleClose);
        // Confirming now legitimately survives a cycle boundary (see
        // trader/doc/incident_DOGE_wrong_result_2026-07-09.md §4) — resolve it before
        // the next entry so it isn't still blocking try_enter here.
        w.step(Event::ApiResult { won: false });
        enter_down_position(&mut w, 10.0);
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1290.0,
            price: 60_100.0,
        }));
        w.step(Event::CycleClose);
        assert!(w.is_halted(), "sanity: halt_rev=2 should have tripped");

        let persisted = w.to_persisted();
        assert!(
            !persisted.entry_suppressed,
            "loss-streak halt must not touch entry_suppressed"
        );
        assert_eq!(persisted.halt_losses, 2);
        assert!(persisted.halt_last_session.is_some());

        // Simulate a restart: a fresh worker starts un-halted...
        let mut restarted = Worker::new_reversal("BTC", &p);
        assert!(!restarted.is_halted());
        // ...until restore_halt replays what was persisted just before shutdown.
        restarted.restore_halt(
            persisted.entry_suppressed,
            persisted.halt_losses,
            persisted.halt_last_session,
        );
        assert!(
            restarted.is_halted(),
            "restored worker must come back halted"
        );

        // Behaves exactly like the pre-restart worker from here: survives a
        // same-session cycle boundary, clears on the next HKT session.
        let actions = restarted.step(Event::CycleOpen {
            ctx: ctx(1_500.0),
            slug: "btc-updown-5m-1500".to_string(),
        });
        assert!(
            restarted.is_halted(),
            "halt must survive a same-session cycle boundary post-restart"
        );
        assert!(!actions.contains(&Action::HaltReset));
        let actions = restarted.step(Event::CycleOpen {
            ctx: ctx(101_000.0),
            slug: "btc-updown-5m-101000".to_string(),
        });
        assert!(
            !restarted.is_halted(),
            "halt must still clear on the next daily reset post-restart"
        );
        assert!(actions.contains(&Action::HaltReset));
    }

    /// Same restart scenario as `halt_state_round_trips_across_a_restart`, but
    /// for the manual/drawdown flag (`entry_suppressed`) in isolation — no
    /// loss-streak involved, and no daily reset ever clears it (only `/resume`
    /// does), so a restore must leave it halted indefinitely across cycles.
    #[test]
    fn manual_halt_round_trips_across_a_restart() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::Control(ControlEvent::Halt));

        let persisted = w.to_persisted();
        assert!(persisted.entry_suppressed);
        assert_eq!(persisted.halt_losses, 0);
        assert!(persisted.halt_last_session.is_none());

        let mut restarted = Worker::new_reversal("BTC", &p);
        restarted.restore_halt(
            persisted.entry_suppressed,
            persisted.halt_losses,
            persisted.halt_last_session,
        );
        assert!(
            restarted.is_halted(),
            "restored worker must come back halted"
        );

        restarted.step(Event::CycleOpen {
            ctx: ctx(101_000.0),
            slug: "btc-updown-5m-101000".to_string(),
        });
        assert!(
            restarted.is_halted(),
            "manual halt must not be cleared by a daily reset, restored or not"
        );

        restarted.step(Event::Control(ControlEvent::Resume));
        assert!(
            !restarted.is_halted(),
            "/resume must still clear a restored manual halt"
        );
    }

    #[test]
    fn partial_unwind_fill_leaves_residual_holding() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        // Trigger unwind via PriceMonitor (small-fill style arm stays PriceMonitor
        // until a GTC confirms; force via direct state mutation isn't available,
        // so drive through the natural TP-cross path.)
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // entry 0.70 + unwind 0.03
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        let actions = w.step(Event::UnwindFilled {
            sold_shares: 6.0,
            exit_price: 0.73,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        match &w.state {
            WorkerState::Holding(h) => assert_eq!(h.shares, 4.0, "residual = 10 - 6"),
            _ => panic!("expected residual Holding"),
        }
        assert!(
            !actions.iter().any(|a| matches!(a, Action::LogTrade(_))),
            "partial fill must not log a trade yet"
        );
    }

    #[test]
    fn full_unwind_fill_logs_trade_and_goes_enrich_only() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        let actions = w.step(Event::UnwindFilled {
            sold_shares: 10.0,
            exit_price: 0.73,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });

        let record = actions.iter().find_map(|a| {
            if let Action::LogTrade(r) = a {
                Some(r.clone())
            } else {
                None
            }
        });
        let record = record.expect("expected a LogTrade action");
        assert_eq!(record.outcome, Outcome::Unwind);
        assert!(matches!(w.state, WorkerState::EnrichOnly(_)));
    }

    /// Reproduces the 2026-07-04 10:00:00 ETH `high_prob` incident
    /// (`trader/doc/incident_tele_pnl_2026-07-04.md`): a BUY fill of 1.2048
    /// shares (`$1.00 / 0.83`), a take-profit `close_position` that could only
    /// sell `floor2(1.2048) = 1.20` of them (`execution.rs`'s 2-decimal cap),
    /// leaving a 0.0048-share remainder that can never itself be sold —
    /// `0.0048 * 1e6 = 4,800`, under Polymarket's 10,000-unit `makerAmount`
    /// floor at any price (`MIN_SELLABLE_SHARES`). Two things must hold: (1)
    /// the trade finalizes immediately instead of parking the dust in
    /// `Holding` to be chased or mark-to-marketed at cycle close, and (2) the
    /// logged pnl is the real, fee-inclusive cash result — `1.20 * (0.88 -
    /// 0.83) = 0.06` gross, minus entry fee `1.2048 * 0.07 * 0.83 * 0.17 ≈
    /// 0.0119` and exit fee `1.20 * 0.07 * 0.88 * 0.12 ≈ 0.0089` — not the
    /// `0.0608` the pre-fix code logged (0.06 realized + the dust valued at
    /// $1/share as if it had resolved, which it never actually did).
    #[test]
    fn dust_residual_below_min_sellable_is_written_off_not_chased() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }));
        w.step(Event::OrderFilled {
            filled_shares: 1.2048,
            cost: 0.83,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        }); // tp_price = 0.83 + 0.03 = 0.86

        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.12,
            dn: 0.88,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // crosses tp -> Unwinding
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        let actions = w.step(Event::UnwindFilled {
            sold_shares: 1.20,
            exit_price: 0.88,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let record = actions.iter().find_map(|a| {
            if let Action::LogTrade(r) = a {
                Some(r.clone())
            } else {
                None
            }
        });
        let record = record.expect(
            "dust leftover (0.0048 < MIN_SELLABLE_SHARES) must finalize now, not stay Holding",
        );
        assert_eq!(record.outcome, Outcome::Unwind);
        assert!(
            (record.pnl - 0.0392).abs() < 1e-4,
            "expected ~0.0392 net (0.06 realized - ~0.0207 fees, dust excluded), got {}",
            record.pnl
        );
        assert!(
            matches!(w.state, WorkerState::EnrichOnly(_)),
            "must not linger in Holding chasing unsellable dust"
        );
    }

    /// Reproduces the pnl bug from the 2026-07-03 ETH `high_prob` Telegram
    /// report (`entry=0.8900 -> exit=1.0000, pnl=-$0.9964` for a WIN): a
    /// partial take-profit fill left a small residual, and the eventual
    /// cycle-close resolution's pnl was computed as `residual_shares *
    /// exit_price - trade_size` — discarding both the earlier partial sale's
    /// proceeds *and* the fact that `trade_size` no longer matches what's
    /// actually being settled. The total across both legs must equal what a
    /// human doing the arithmetic by hand would get: 6 shares sold at 0.73
    /// (cost 0.70) + 4 residual shares resolved at $1 (still cost 0.70) =
    /// $8.38 proceeds on a $7.00 stake = +$1.38 gross, not something close to
    /// -$1. Net of Polymarket's taker fee on the two real (taker) legs — entry
    /// `10 * 0.07 * 0.70 * 0.30 = 0.147` and the partial exit `6 * 0.07 * 0.73
    /// * 0.27 = 0.082782` (resolution of the residual 4 shares is not a trade
    /// and is fee-free) — that's `1.38 - 0.229782 = 1.1502`
    /// (`trader/doc/incident_tele_pnl_2026-07-04.md` §2).
    #[test]
    fn partial_unwind_then_cycle_close_totals_both_legs_pnl() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0); // 10 shares @ cost 0.70
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // crosses tp -> Unwinding
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        // Partial fill: 6 of 10 shares sold at 0.73 (the tp price).
        w.step(Event::UnwindFilled {
            sold_shares: 6.0,
            exit_price: 0.73,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        match &w.state {
            WorkerState::Holding(h) => {
                assert_eq!(h.shares, 4.0);
                assert!(
                    (h.realized_pnl - 0.18).abs() < 1e-9,
                    "6*(0.73-0.70) = 0.18, got {}",
                    h.realized_pnl
                );
            }
            _ => panic!("expected residual Holding"),
        }

        // Binance stayed below open (59_900 < 60_000 from enter_down_position),
        // so the DOWN side wins the residual at cycle close.
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Win);
        assert!(
            (record.pnl - 1.1502).abs() < 1e-9,
            "expected +1.1502 net (1.38 gross - 0.229782 fees), got {}",
            record.pnl
        );
    }

    /// Reproduces the 2026-07-03 ETH audit scenario (trader/doc/audit_trades_2026-07-03.md):
    /// a take-profit unwind is triggered but the sell fails (e.g. "balance: 0"),
    /// so the position falls back to Holding and is resolved at cycle close.
    /// The logged WIN/LOSS record must carry the failed-attempt history instead
    /// of looking like a clean hold-to-resolution trade.
    #[test]
    fn failed_unwind_then_cycle_close_carries_exit_attempts_onto_trade_record() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // crosses tp -> Unwinding
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        w.step(Event::UnwindFailed {
            error: Some("balance: 0".to_string()),
        });
        match &w.state {
            WorkerState::Holding(h) => {
                assert_eq!(h.exit_attempts, 1);
                assert_eq!(h.exit_last_error.as_deref(), Some("balance: 0"));
            }
            _ => panic!("expected Holding (failed exit is not an exit)"),
        }

        // Price stayed below open (59900 < 60000) -> DOWN wins at cycle close.
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Win);
        assert_eq!(
            record.exit_attempts, 1,
            "WIN record must show the failed unwind attempt, not look like a clean hold"
        );
        assert_eq!(record.exit_last_error.as_deref(), Some("balance: 0"));
    }

    /// Follow-up to the 2026-07-03 17:33 DOGE incident (trader/doc/incident_doge_2026-07-03.md)
    /// and the 2026-07-06 SOL incident (trader/doc/incident_sol_unwind_but_loss_2026-07-06.md):
    /// a failed take-profit close now re-arms `PriceMonitor { tp_price }` (not a
    /// one-shot abandon) and *does* retry on the next qualifying `PolyTick` —
    /// this is safe today because the close itself is bounded at `tp_price`
    /// (`execution::close_position_at_price`), so a retry can never fill worse
    /// than the take-profit target, and each attempt is gated on a real tick
    /// (not an internal loop), which is itself the backoff that avoided the
    /// original 284-attempts-in-9s hammering.
    #[test]
    fn failed_unwind_retries_close_on_next_qualifying_poly_tick() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // crosses tp -> Unwinding
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        w.step(Event::UnwindFailed {
            error: Some("no market price".to_string()),
        });
        match &w.state {
            WorkerState::Holding(h) => {
                assert_eq!(h.exit_arm, ExitArm::PriceMonitor { tp_price: 0.73 })
            }
            _ => panic!("expected Holding re-armed at tp_price"),
        }

        // Price stays above tp (0.73) on the next tick -> retries the close,
        // bounded at the same tp_price (not an unbounded worse fill).
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1261.0,
            up: 0.20,
            dn: 0.80,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(
            actions.iter().any(|a| matches!(a, Action::ClosePosition { reason: CloseReason::TakeProfit, limit_price: Some(tp), .. } if (*tp - 0.73).abs() < 1e-9)),
            "must retry the take-profit close, bounded at tp_price, on the next qualifying tick: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Unwinding(_)));
    }

    #[test]
    fn stop_loss_fires_and_cancels_resting_gtc_first() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced {
            order_id: Some("order-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });

        // dn drops below entry(0.70) - sl_pnl(0.20) = 0.50 (use 0.49 to clear the
        // f64 boundary cleanly: 0.70 - 0.20 == 0.4999999999999999 in f64).
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.45,
            dn: 0.49,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert_eq!(
            actions,
            vec![
                Action::CancelLimitSell {
                    order_id: "order-1".to_string()
                },
                Action::ClosePosition {
                    shares: 10.0,
                    reason: CloseReason::StopLoss,
                    limit_price: None,
                    signal_ts: 1260.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
    }

    #[test]
    fn failed_stop_sell_reclassifies_as_held() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.45,
            dn: 0.49,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // triggers StopExiting
        assert!(matches!(w.state, WorkerState::StopExiting(_)));

        w.step(Event::StopSellFailed {
            error: Some("test error".to_string()),
        });
        assert!(
            matches!(w.state, WorkerState::Holding(_)),
            "failed exit is not an exit — reclassified as held"
        );
    }

    /// unwind_time (max holding time) — see trader/doc/plan_unwind_time_2026-07-08.md.
    /// entry_ts is 1250.0 (enter_down_position's final BinanceTick) in every case
    /// below; dn=0.60 is chosen to sit strictly between the stop-loss floor
    /// (0.70 - sl_pnl_rev(0.20) = 0.50) and the take-profit target
    /// (0.70 + unwind_pnl_rev(0.03) = 0.73), so only the timeout path can fire.
    #[test]
    fn timeout_force_closes_after_unwind_time_elapsed_with_no_other_exit() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1280.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // 1250 + 30
        assert_eq!(
            actions,
            vec![
                Action::ClosePosition {
                    shares: 10.0,
                    reason: CloseReason::Timeout,
                    limit_price: None,
                    signal_ts: 1280.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::TimingOut(_)));
    }

    #[test]
    fn timeout_does_not_fire_before_threshold_elapsed() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1279.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // 1250 + 29
        assert_eq!(actions, vec![], "must not fire 1s before the threshold");
        assert!(matches!(w.state, WorkerState::Holding(_)));
    }

    #[test]
    fn timeout_disabled_when_unwind_time_zero() {
        let p = btc_params(); // unwind_time_rev defaults to 0.0 (disabled)
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        // Enormous elapsed time — would fire at any positive threshold.
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 100_000.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert_eq!(
            actions,
            vec![],
            "0.0 must mean disabled regardless of elapsed time"
        );
        assert!(matches!(w.state, WorkerState::Holding(_)));
    }

    #[test]
    fn stoploss_takes_priority_over_timeout_on_same_tick() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        // Both conditions true simultaneously: 30s elapsed AND dn(0.49) crosses
        // the stop-loss floor (0.50) — matches the backtest's fixed exit-chain
        // order (stop-loss, then take-profit, then timeout last).
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1280.0,
            up: 0.45,
            dn: 0.49,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert_eq!(
            actions,
            vec![
                Action::ClosePosition {
                    shares: 10.0,
                    reason: CloseReason::StopLoss,
                    limit_price: None,
                    signal_ts: 1280.0
                },
                Action::Persist,
            ]
        );
        assert!(
            matches!(w.state, WorkerState::StopExiting(_)),
            "stop-loss must win over timeout on the same tick"
        );
    }

    #[test]
    fn timeout_sell_failure_falls_back_to_holding_and_refires_next_tick() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1280.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // triggers TimingOut
        assert!(matches!(w.state, WorkerState::TimingOut(_)));

        w.step(Event::TimeoutSellFailed {
            error: Some("test error".to_string()),
        });
        assert!(
            matches!(w.state, WorkerState::Holding(_)),
            "failed exit is not an exit — reclassified as held"
        );

        // Threshold condition is still true (more true, as time passes) — the
        // next PolyTick naturally re-fires, no separate retry-counter needed.
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1281.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert_eq!(
            actions,
            vec![
                Action::ClosePosition {
                    shares: 10.0,
                    reason: CloseReason::Timeout,
                    limit_price: None,
                    signal_ts: 1281.0
                },
                Action::Persist,
            ]
        );
        assert!(matches!(w.state, WorkerState::TimingOut(_)));
    }

    #[test]
    fn timeout_sell_filled_produces_timeout_outcome() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1280.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // triggers TimingOut

        let actions = w.step(Event::TimeoutSellFilled {
            sold_shares: 10.0,
            exit_price: 0.60,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Timeout);
        assert!(matches!(w.state, WorkerState::EnrichOnly(_)));
    }

    #[test]
    fn to_persisted_round_trips_timing_out_state() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1280.0,
            up: 0.40,
            dn: 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // triggers TimingOut

        let snap = w.to_persisted();
        match &snap.state {
            PersistedWorkerState::TimingOut(h) => assert_eq!(h.shares, 10.0),
            _ => panic!("expected TimingOut in persisted snapshot"),
        }

        let json = serde_json::to_string(&snap).unwrap();
        let back: PersistedState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.asset, "BTC");
    }

    #[test]
    fn cycle_close_with_open_position_resolves_win_loss_and_spawns_confirming() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        // Price fell below open (60000 -> 59900) so DOWN wins at cycle close.
        let actions = w.step(Event::CycleClose);
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTrade(r) = a {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::Win);
        assert!(matches!(w.state, WorkerState::Confirming(_)));
    }

    #[test]
    fn is_confirming_true_only_while_awaiting_gamma() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        assert!(!w.is_confirming(), "fresh worker starts Watching");

        enter_down_position(&mut w, 10.0);
        assert!(
            !w.is_confirming(),
            "an open Holding position isn't Confirming"
        );

        w.step(Event::CycleClose); // -> Confirming(WIN)
        assert!(w.is_confirming());

        w.step(Event::ApiResult { won: true }); // Gamma confirms -> Watching
        assert!(!w.is_confirming());
    }

    #[test]
    fn api_result_flips_confirming_outcome_and_recomputes_pnl() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)

        let actions = w.step(Event::ApiResult { won: false }); // API says it actually lost
        let (previous_outcome, previous_pnl, record) = actions
            .iter()
            .find_map(|a| {
                if let Action::LogTradeCorrection {
                    previous_outcome,
                    previous_pnl,
                    record,
                } = a
                {
                    Some((*previous_outcome, *previous_pnl, record.clone()))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(previous_outcome, Outcome::Win);
        assert!(
            previous_pnl > 0.0,
            "original estimate should have been a WIN pnl, got {previous_pnl}"
        );
        assert_eq!(record.outcome, Outcome::Loss);
        // -trade_size(1.0) - buy_fee(shares(1/0.7) * 0.07 * 0.7 * 0.3 = trade_size * 0.07 * 0.3 = 0.021)
        assert!(
            (record.pnl - (-1.021)).abs() < 1e-9,
            "LOSS pnl should be -trade_size - entry fee, got {}",
            record.pnl
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn api_result_on_enrich_only_never_touches_pnl() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::UnwindFilled {
            sold_shares: 10.0,
            exit_price: 0.73,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        }); // -> EnrichOnly(Unwind)

        let actions = w.step(Event::ApiResult { won: true });
        assert!(
            !actions.iter().any(|a| matches!(a, Action::LogTrade(_))),
            "EnrichOnly must never re-log a trade"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::StopLossVerdict { .. })),
            "an UNWIND exit gets no counterfactual verdict, matching Python's is_unwind skip"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn api_result_on_stop_loss_enrich_only_emits_verdict() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        // DOWN position; poly tick crosses the stop-loss floor -> StopExiting.
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.55,
            dn: 0.45,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
        w.step(Event::StopSellFilled {
            sold_shares: 10.0,
            exit_price: 0.45,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        }); // -> EnrichOnly(StopLoss)

        // `won` is already relative to the record's own side (matches the Confirming
        // branch's convention) — `true` here means the position's side actually won,
        // i.e. the stop-loss was costly (holding would have won instead).
        let actions = w.step(Event::ApiResult { won: true });
        let (record, would_have_won) = actions
            .iter()
            .find_map(|a| {
                if let Action::StopLossVerdict {
                    record,
                    would_have_won,
                } = a
                {
                    Some((record.clone(), *would_have_won))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(record.outcome, Outcome::StopLoss);
        assert!(
            would_have_won,
            "API says the position's side actually won -> stop was costly"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::LogTrade(_) | Action::LogTradeCorrection { .. })),
            "verdict never rewrites pnl/result"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// Regression test for trader/doc/incident_DOGE_wrong_result_2026-07-09.md §3a/§4:
    /// `on_cycle_open` used to unconditionally reset `self.state` to `Watching`, which in
    /// production clobbered `Confirming` within about a second of it being set (the very
    /// next `CycleOpen`, fired right after the `CycleClose` that produced it) — silently
    /// dropping every async Gamma correction under normal operation.
    #[test]
    fn confirming_survives_a_cycle_open_boundary() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)
        assert!(matches!(w.state, WorkerState::Confirming(_)));

        w.step(Event::CycleOpen {
            ctx: ctx(1_500.0),
            slug: "btc-updown-5m-1500".to_string(),
        });
        assert!(
            matches!(w.state, WorkerState::Confirming(_)),
            "CycleOpen must not clobber an in-flight Gamma confirmation, got {:?}",
            w.state
        );
    }

    /// Same regression, the defense-in-depth case: a `CycleClose` with nothing currently
    /// held (because entries are blocked while `Confirming`) must also leave `Confirming`
    /// alone rather than reset it via the "nothing to close" fallback.
    #[test]
    fn confirming_survives_a_cycle_close_with_nothing_held() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)
        assert!(matches!(w.state, WorkerState::Confirming(_)));

        let actions = w.step(Event::CycleClose); // nothing held this time
        assert!(actions.is_empty());
        assert!(
            matches!(w.state, WorkerState::Confirming(_)),
            "a no-op CycleClose must not clobber an in-flight Gamma confirmation, got {:?}",
            w.state
        );
    }

    #[test]
    fn api_result_confirms_without_flip_emits_note_and_no_correction() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)

        let actions = w.step(Event::ApiResult { won: true }); // API agrees: DOWN did win
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::LogTradeCorrection { .. })),
            "matching result must not emit a correction: {actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ApiResultNote(_))),
            "matching result should still leave a diagnostic trace: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
        assert!(!w.entry_suppressed, "a clean confirmation must not halt");
    }

    #[test]
    fn api_result_timeout_on_confirming_halts_and_keeps_provisional_record() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)

        let actions = w.step(Event::ApiResultTimeout {
            balance_increased: false,
        });
        let record = actions
            .iter()
            .find_map(|a| {
                if let Action::GammaHaltEngaged { record } = a {
                    Some(record.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            record.outcome,
            Outcome::Win,
            "timeout must keep the original provisional outcome, not guess"
        );
        assert!(matches!(w.state, WorkerState::Watching));
        assert!(
            w.entry_suppressed,
            "an unresolved confirmation must halt new entries"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::LogTrade(_) | Action::LogTradeCorrection { .. })),
            "timeout must never rewrite pnl/result: {actions:?}"
        );
    }

    #[test]
    fn api_result_timeout_on_confirming_with_balance_increased_continues_without_halting() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)

        let actions = w.step(Event::ApiResultTimeout {
            balance_increased: true,
        });
        let (record, entry_suppressed) = actions
            .iter()
            .find_map(|a| {
                if let Action::GammaUnresolvedContinued {
                    record,
                    entry_suppressed,
                } = a
                {
                    Some((record.clone(), *entry_suppressed))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            record.outcome,
            Outcome::Win,
            "timeout must keep the original provisional outcome, not guess"
        );
        assert!(
            !entry_suppressed,
            "balance up since last cycle's checkpoint must not introduce a new halt"
        );
        assert!(matches!(w.state, WorkerState::Watching));
        assert!(
            !w.entry_suppressed,
            "balance-increased timeout must not halt new entries"
        );
        assert!(
            !actions.iter().any(|a| matches!(
                a,
                Action::GammaHaltEngaged { .. }
                    | Action::LogTrade(_)
                    | Action::LogTradeCorrection { .. }
            )),
            "timeout must never halt or rewrite pnl/result when balance is up: {actions:?}"
        );
    }

    #[test]
    fn api_result_timeout_with_balance_increased_reports_a_pre_existing_halt_as_still_suppressed() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)
        w.step(Event::Control(ControlEvent::Halt)); // e.g. manual /halt, independent of gamma

        let actions = w.step(Event::ApiResultTimeout {
            balance_increased: true,
        });
        let entry_suppressed = actions
            .iter()
            .find_map(|a| {
                if let Action::GammaUnresolvedContinued {
                    entry_suppressed, ..
                } = a
                {
                    Some(*entry_suppressed)
                } else {
                    None
                }
            })
            .unwrap();
        assert!(
            entry_suppressed,
            "a pre-existing halt (e.g. manual /halt) must still read as suppressed, \
             even though this timeout itself didn't cause it"
        );
        assert!(
            w.entry_suppressed,
            "balance-increased timeout must never clear a halt set by another source"
        );
    }

    #[test]
    fn api_result_timeout_on_enrich_only_gives_up_quietly_without_halting() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::UnwindFilled {
            sold_shares: 10.0,
            exit_price: 0.73,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        }); // -> EnrichOnly(Unwind), pnl/outcome already final
        assert!(matches!(w.state, WorkerState::EnrichOnly(_)));

        let actions = w.step(Event::ApiResultTimeout {
            balance_increased: false,
        });
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::GammaHaltEngaged { .. } | Action::LogTrade(_))),
            "an EnrichOnly timeout must never halt or rewrite pnl: {actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::ApiResultNote(_))),
            "should still leave a diagnostic trace: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
        assert!(!w.entry_suppressed);
    }

    #[test]
    fn api_result_and_timeout_on_stale_state_are_diagnostic_only() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        assert!(matches!(w.state, WorkerState::Watching));

        let actions = w.step(Event::ApiResult { won: true });
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::ApiResultNote(_)));

        let actions = w.step(Event::ApiResultTimeout {
            balance_increased: false,
        });
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], Action::ApiResultNote(_)));
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn reconcile_holding_with_missing_gtc_order_falls_back_to_price_monitor() {
        let holding = HoldingData {
            side: Side::Down,
            entry_type: EntryType::Reversal,
            token_price: 0.70,
            entry_ts: 1250.0,
            entry_price_ts: 1250.0,
            shares: 10.0,
            exit_arm: ExitArm::GtcResting {
                order_id: "gone-order".to_string(),
            },
            exit_attempts: 0,
            exit_last_error: None,
            realized_pnl: 0.0,
            fees: 0.0,
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
        };
        let persisted = PersistedWorkerState::Holding(holding);

        // Order not in the live open-orders list, but tokens are still held.
        let resumed = Worker::reconcile(&persisted, &[], 10.0);
        match resumed {
            WorkerState::Holding(h) => assert!(matches!(h.exit_arm, ExitArm::PriceMonitor { .. })),
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn reconcile_holding_with_live_gtc_order_keeps_it_armed() {
        let holding = HoldingData {
            side: Side::Down,
            entry_type: EntryType::Reversal,
            token_price: 0.70,
            entry_ts: 1250.0,
            entry_price_ts: 1250.0,
            shares: 10.0,
            exit_arm: ExitArm::GtcResting {
                order_id: "still-live".to_string(),
            },
            exit_attempts: 0,
            exit_last_error: None,
            realized_pnl: 0.0,
            fees: 0.0,
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
        };
        let persisted = PersistedWorkerState::Holding(holding);
        let resumed = Worker::reconcile(&persisted, &["still-live".to_string()], 10.0);
        match resumed {
            WorkerState::Holding(h) => assert_eq!(
                h.exit_arm,
                ExitArm::GtcResting {
                    order_id: "still-live".to_string()
                }
            ),
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn reconcile_zero_balance_position_resumes_as_watching() {
        let holding = HoldingData {
            side: Side::Up,
            entry_type: EntryType::Reversal,
            token_price: 0.70,
            entry_ts: 1250.0,
            entry_price_ts: 1250.0,
            shares: 10.0,
            exit_arm: ExitArm::PriceMonitor { tp_price: 0.73 },
            exit_attempts: 0,
            exit_last_error: None,
            realized_pnl: 0.0,
            fees: 0.0,
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
        };
        let persisted = PersistedWorkerState::Holding(holding);
        let resumed = Worker::reconcile(&persisted, &[], 0.0);
        assert!(matches!(resumed, WorkerState::Watching));
    }

    #[test]
    fn to_persisted_round_trips_holding_state() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        let snap = w.to_persisted();
        assert_eq!(snap.asset, "BTC");
        assert_eq!(snap.strategy, "reversal");
        match &snap.state {
            PersistedWorkerState::Holding(h) => assert_eq!(h.shares, 10.0),
            _ => panic!("expected Holding in persisted snapshot"),
        }

        let json = serde_json::to_string(&snap).unwrap();
        let back: PersistedState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.asset, "BTC");
    }

    #[test]
    fn rejected_order_returns_to_watching() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }));
        assert!(matches!(w.state, WorkerState::Entering));

        w.step(Event::OrderRejected);
        assert!(matches!(w.state, WorkerState::Watching));
    }

    // ── Maker entries (plan_unwind_5u_maker_2026-07-19 §2.2) ─────────────────

    fn maker_params() -> AssetParams {
        AssetParams {
            maker_entry: true,
            ..btc_params()
        }
    }

    /// Drives a worker through the same DOWN reversal signal `enter_down_position`
    /// uses, but with `maker_entry = true` — asserts the entry fires as a
    /// `PlaceLimitBuy` (not `PlaceBuy`) and lands in `EnteringMaker`, then
    /// returns the worker positioned there. `dn` settles at 0.70 (> reversal
    /// 0.60), cycle_end_ts = 1300.0.
    fn quote_down_position(w: &mut Worker) {
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // dip latches saw_low_dn
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        })); // recovery > reversal 0.60; delta_pct not yet known, no fire
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        })); // dp < 0 -> fires entry
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::PlaceLimitBuy {
                        side: Side::Down,
                        shares,
                        ..
                    },
                    Action::Persist
                ] if (*shares - MIN_GTC_SHARES).abs() < 1e-9
            ),
            "expected a maker-entry quote to fire: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::EnteringMaker(_)));
    }

    #[test]
    fn maker_entry_rests_gtc_buy_instead_of_fak() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        let WorkerState::EnteringMaker(q) = &w.state else {
            panic!("expected EnteringMaker, got {:?}", w.state);
        };
        assert_eq!(q.side, Side::Down);
        assert!((q.quote_price - 0.70).abs() < 1e-9);
        assert!(q.order_id.is_none(), "no id until LimitBuyPlaced(Live)");
        assert!(
            (q.quoted_at - 1250.0).abs() < 1e-9,
            "quoted_at must be the entry-fire tick, for pull-to-cancel latency"
        );
    }

    // ── Real best-bid quoting (plan_unwind_5u_maker_2026-07-19 §2.2 mid-vs-bid fix) ──

    /// DOWN's quote price derives from the UP token's real *ask* (`1 -
    /// up_ask`), not the mid — the exact gap the README TODO flagged and
    /// this fix closes.
    #[test]
    fn maker_entry_down_side_quotes_at_real_best_bid_not_mid() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.84,
            up_ask: 0.86,
        })); // dip latches saw_low_dn
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70, // mid — if the bug were still present, the quote would land here
            up_bid: 0.29,
            up_ask: 0.31,
        })); // recovery > reversal 0.60; delta_pct not yet known, no fire
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        })); // dp < 0 -> fires entry
        assert!(
            matches!(
                actions.as_slice(),
                [Action::PlaceLimitBuy { price, .. }, Action::Persist]
                    if (*price - 0.69).abs() < 1e-9 // 1 - up_ask(0.31), NOT mid(0.70)
            ),
            "expected quote at the real best bid 0.69 (1 - up_ask), not mid 0.70: {actions:?}"
        );
        let WorkerState::EnteringMaker(q) = &w.state else {
            panic!("expected EnteringMaker, got {:?}", w.state);
        };
        assert!((q.quote_price - 0.69).abs() < 1e-9);
    }

    /// UP's quote price reads the UP token's real *bid* directly, not the mid.
    #[test]
    fn maker_entry_up_side_quotes_at_real_best_bid_not_mid() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.15,
            dn: 0.85,
            up_bid: 0.14,
            up_ask: 0.16,
        })); // dip latches saw_low_up
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.70, // mid — if the bug were still present, the quote would land here
            dn: 0.30,
            up_bid: 0.68,
            up_ask: 0.72,
        })); // recovery > reversal 0.60
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 60_100.0, // dp > 0 -> fires UP entry
        }));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::PlaceLimitBuy {
                        side: Side::Up,
                        price,
                        ..
                    },
                    Action::Persist
                ] if (*price - 0.68).abs() < 1e-9 // up_bid directly, NOT mid(0.70)
            ),
            "expected quote at the real best bid 0.68 (up_bid), not mid 0.70: {actions:?}"
        );
    }

    /// No real bid/ask observed this run (e.g. an old price_feed, or a
    /// fresh worker that hasn't seen a tick with real spread data) — falls
    /// back to mid exactly like before this fix, not a panic or a zero price.
    #[test]
    fn maker_entry_falls_back_to_mid_when_no_bid_ask_observed() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w); // uses up_bid: 0.0, up_ask: 0.0 throughout
        let WorkerState::EnteringMaker(q) = &w.state else {
            panic!("expected EnteringMaker, got {:?}", w.state);
        };
        assert!(
            (q.quote_price - 0.70).abs() < 1e-9,
            "must fall back to mid (0.70) when no real bid/ask was ever observed"
        );
    }

    /// The pup gate's `price` reference is the same real entry price the
    /// quote actually rests at, not mid — using mid there would make the
    /// veto needlessly strict whenever the real bid undercuts it.
    #[test]
    fn pup_gate_uses_the_real_best_bid_as_entry_price_not_mid() {
        let p = pup_gate_params(0.0);
        let p = AssetParams {
            maker_entry: true,
            ..p
        };
        let mut w = Worker::new_reversal("BTC", &p);
        // p_up=0.35 -> p_side (DOWN) = 0.65. Against mid (0.70) this would
        // veto (0.65 < 0.70); against the real best bid 0.69 (1 - up_ask
        // 0.31) it also vetoes (0.65 < 0.69) but the logged `price` field
        // must reflect 0.69, not 0.70, either way. ts 1249 keeps this within
        // PUP_GATE_MAX_AGE_SECS (2.0s) of the triggering tick at 1250.
        w.step(Event::IndicatorUpdate {
            p_up: 0.35,
            ts: 1249.0,
        });
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.84,
            up_ask: 0.86,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.29,
            up_ask: 0.31,
        }));
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::PupGateNote {
                    outcome: PupGateOutcome::Veto,
                    price,
                    ..
                }] if (*price - 0.69).abs() < 1e-9
            ),
            "PupGateNote.price must be the real best bid (0.69), not mid (0.70): {actions:?}"
        );
    }

    /// QUOTED -> the CLOB acked it as resting: order_id lands on the quote,
    /// worker stays in EnteringMaker.
    #[test]
    fn maker_entry_live_ack_records_order_id_and_keeps_quoting() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        let actions = w.step(Event::LimitBuyPlaced {
            order_id: Some("paper-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 5.0,
            process_latency_ms: 10.0,
        });
        assert_eq!(actions, vec![Action::Persist]);
        let WorkerState::EnteringMaker(q) = &w.state else {
            panic!("expected still EnteringMaker, got {:?}", w.state);
        };
        assert_eq!(q.order_id.as_deref(), Some("paper-1"));
        assert_eq!(w.entry_resting_order_id(), Some("paper-1"));
    }

    /// QUOTED -> FILLED via the resting quote actually trading through
    /// (`Event::EntryQuoteFilled`, the paper driver's fill-routing path).
    #[test]
    fn maker_entry_quote_fill_transitions_to_holding() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        w.step(Event::LimitBuyPlaced {
            order_id: Some("paper-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let actions = w.step(Event::EntryQuoteFilled {
            filled_shares: MIN_GTC_SHARES,
            cost: 0.70,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        assert!(
            actions.iter().any(|a| matches!(a, Action::Persist)),
            "{actions:?}"
        );
        let WorkerState::Holding(h) = &w.state else {
            panic!("expected Holding, got {:?}", w.state);
        };
        assert_eq!(h.side, Side::Down);
        assert!((h.token_price - 0.70).abs() < 1e-9);
        assert!((h.shares - MIN_GTC_SHARES).abs() < 1e-9);
        // Fresh position: no in-flight exit attempts yet.
        assert_eq!(h.exit_attempts, 0);
    }

    /// A marketable maker order (crossed the book immediately) finalizes the
    /// same way a quote fill does, via `LimitBuyPlaced{status: Matched}`.
    #[test]
    fn maker_entry_matched_on_placement_finalizes_as_holding() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        w.step(Event::LimitBuyPlaced {
            order_id: None,
            status: SellStatus::Matched,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        assert!(matches!(w.state, WorkerState::Holding(_)));
    }

    /// The CLOB rejected the resting order outright (e.g. below the GTC
    /// minimum) — give up this cycle, same posture as a rejected FAK entry.
    #[test]
    fn maker_entry_placement_failure_returns_to_watching() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        let actions = w.step(Event::LimitBuyPlaced {
            order_id: None,
            status: SellStatus::Failed,
            error: Some("INVALID_ORDER_MIN_SIZE".to_string()),
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        assert_eq!(actions, vec![Action::Persist]);
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// QUOTED -> CANCELLED at T-15s: cycle_end_ts = 1300.0, so a tick at
    /// ts=1290 (10s left) must cancel regardless of price still holding above
    /// the reversal threshold.
    #[test]
    fn maker_entry_cancels_at_t_minus_15s_before_cycle_end() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        w.step(Event::LimitBuyPlaced {
            order_id: Some("paper-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1290.0, // cycle_end_ts(1300) - 1290 = 10s <= 15s
            up: 0.30,
            dn: 0.70, // still above reversal 0.60 — time, not price, must fire this,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelEntryQuote {
                        order_id: Some(id),
                        side: Side::Down,
                        reason: CancelQuoteReason::CycleEndApproaching,
                        quoted_at,
                        ..
                    },
                    Action::Persist
                ] if id == "paper-1" && (*quoted_at - 1250.0).abs() < 1e-9
            ),
            "quoted_at must be the entry-fire tick (1250.0), for pull-to-cancel latency: {actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// QUOTED -> CANCELLED on signal invalidation: price falls back to/below
    /// the reversal threshold before T-15s, well before cycle end.
    #[test]
    fn maker_entry_cancels_on_signal_invalidation() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        w.step(Event::LimitBuyPlaced {
            order_id: Some("paper-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1260.0, // well before T-15s (1285)
            up: 0.45,
            dn: 0.55, // dropped back to/below reversal 0.60,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelEntryQuote {
                        order_id: Some(id),
                        reason: CancelQuoteReason::SignalInvalidated,
                        quoted_at,
                        ..
                    },
                    Action::Persist
                ] if id == "paper-1" && (*quoted_at - 1250.0).abs() < 1e-9
            ),
            "{actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Watching));
    }

    /// A quote still holding above the reversal threshold, with time left,
    /// produces no cancel action.
    #[test]
    fn maker_entry_quote_survives_a_tick_that_changes_nothing_material() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        w.step(Event::LimitBuyPlaced {
            order_id: Some("paper-1".to_string()),
            status: SellStatus::Live,
            error: None,
            signal_latency_ms: 0.0,
            process_latency_ms: 0.0,
        });
        let actions = w.step(Event::PolyTick(PolyTick {
            ts: 1245.0,
            up: 0.31,
            dn: 0.69, // still > reversal 0.60, plenty of time left,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        assert!(actions.is_empty(), "{actions:?}");
        assert!(matches!(w.state, WorkerState::EnteringMaker(_)));
    }

    /// Partial-cycle restart: a crash mid-quote loses the resting order on
    /// restore, same posture as a plain FAK `Entering` — `to_persisted`
    /// degrades `EnteringMaker` to `PersistedWorkerState::Entering`, which
    /// `reconcile` already resumes as `Watching` (no fill to reconstruct a
    /// position from).
    #[test]
    fn maker_entry_partial_cycle_restart_resumes_as_watching() {
        let p = maker_params();
        let mut w = Worker::new_reversal("BTC", &p);
        quote_down_position(&mut w);
        let persisted = w.to_persisted();
        assert!(matches!(persisted.state, PersistedWorkerState::Entering));
        let resumed = Worker::reconcile(&persisted.state, &[], 0.0);
        assert!(matches!(resumed, WorkerState::Watching));
    }

    /// high_prob/v_shape never consult `maker_entry`, even when the flag is
    /// on — the plan scopes this to reversal only.
    #[test]
    fn maker_entry_flag_is_ignored_by_high_prob() {
        let p = maker_params();
        let mut w = Worker::new_high_prob("BTC", &p);
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1010.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1015.0,
            price: 60_100.0,
        }));
        // Whatever fires (or doesn't) here, it must never be a maker quote —
        // high_prob always uses the FAK PlaceBuy path regardless of the flag.
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PlaceLimitBuy { .. })),
            "{actions:?}"
        );
        assert!(!matches!(w.state, WorkerState::EnteringMaker(_)));
    }

    // ── p(up) negative-edge gate (plan_unwind_5u_maker_2026-07-19 §2.3) ──────

    fn pup_gate_params(min_edge: f64) -> AssetParams {
        AssetParams {
            pup_edge_min_rev: Some(min_edge),
            ..btc_params()
        }
    }

    /// Drives the same DOWN reversal signal `quote_down_position` uses (dn
    /// settles at 0.70 > reversal 0.60, cycle_end_ts = 1300.0) up to and
    /// including the triggering tick, returning its actions unasserted — the
    /// pup gate can turn a would-be entry into a veto, so callers check the
    /// shape themselves.
    fn fire_down_reversal(w: &mut Worker) -> Vec<Action> {
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::PolyTick(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        w.step(Event::BinanceTick(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        }))
    }

    #[test]
    fn pup_gate_disabled_by_default_fires_plain_fak_entry() {
        let p = btc_params(); // pup_edge_min_rev: None
        let mut w = Worker::new_reversal("BTC", &p);
        let actions = fire_down_reversal(&mut w);
        assert_eq!(
            actions,
            vec![
                Action::PlaceBuy {
                    side: Side::Down,
                    price: 0.70,
                    size_usdc: p.trade_size_usdc,
                    signal_ts: 1250.0,
                },
                Action::Persist,
            ]
        );
    }

    #[test]
    fn pup_gate_vetoes_when_p_side_below_entry_price() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        // DOWN entry at price 0.70; p_up=0.35 -> p_side=1-0.35=0.65 < 0.70.
        // ts 1249 keeps this within PUP_GATE_MAX_AGE_SECS (2.0s) of the
        // triggering tick at 1250 in fire_down_reversal.
        w.step(Event::IndicatorUpdate {
            p_up: 0.35,
            ts: 1249.0,
        });
        let actions = fire_down_reversal(&mut w);
        assert_eq!(
            actions,
            vec![Action::PupGateNote {
                side: Side::Down,
                p_side: Some(0.65),
                price: 0.70,
                outcome: PupGateOutcome::Veto,
            }]
        );
        assert!(
            matches!(w.state, WorkerState::Watching),
            "a veto must not lock the strategy out — no mark_fired()"
        );
    }

    #[test]
    fn pup_gate_passes_when_p_side_at_or_above_entry_price() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        // p_up=0.20 -> p_side=0.80 >= 0.70. ts 1249 keeps this within
        // PUP_GATE_MAX_AGE_SECS (2.0s) of the triggering tick at 1250.
        w.step(Event::IndicatorUpdate {
            p_up: 0.20,
            ts: 1249.0,
        });
        let actions = fire_down_reversal(&mut w);
        assert_eq!(
            actions,
            vec![
                Action::PlaceBuy {
                    side: Side::Down,
                    price: 0.70,
                    size_usdc: p.trade_size_usdc,
                    signal_ts: 1250.0,
                },
                Action::Persist,
            ],
            "no PupGateNote on a pass — only veto/skip are logged"
        );
    }

    /// Exactly `p_side == price + min_edge` is a pass, not a veto (`<`, not `<=`).
    #[test]
    fn pup_gate_exact_equality_is_not_a_veto() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        // p_up=0.30 -> p_side=0.70 == price 0.70 exactly. ts 1249 keeps this
        // within PUP_GATE_MAX_AGE_SECS (2.0s) of the triggering tick at 1250.
        w.step(Event::IndicatorUpdate {
            p_up: 0.30,
            ts: 1249.0,
        });
        let actions = fire_down_reversal(&mut w);
        assert!(matches!(actions.first(), Some(Action::PlaceBuy { .. })));
    }

    #[test]
    fn pup_gate_blocks_when_no_snapshot_ever_arrived() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        let actions = fire_down_reversal(&mut w);
        assert_eq!(
            actions,
            vec![Action::PupGateNote {
                side: Side::Down,
                p_side: None,
                price: 0.70,
                outcome: PupGateOutcome::StaleBlocked,
            }],
            "never trade on stale/missing information — no snapshot must block the entry, \
             not fire it (CLAUDE.md \"Trading principles\")"
        );
        assert!(
            matches!(w.state, WorkerState::Watching),
            "a stale-block must not lock the strategy out — no mark_fired()"
        );
    }

    #[test]
    fn pup_gate_blocks_on_a_stale_snapshot() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        // A snapshot that would ALSO veto on p_side alone (0.65 < 0.70), but
        // is stale by the time the entry fires regardless (ts 1250 -
        // snapshot ts 1230 = 20s > PUP_GATE_MAX_AGE_SECS 2.0s).
        w.step(Event::IndicatorUpdate {
            p_up: 0.35,
            ts: 1230.0,
        });
        let actions = fire_down_reversal(&mut w);
        assert_eq!(
            actions,
            vec![Action::PupGateNote {
                side: Side::Down,
                p_side: None,
                price: 0.70,
                outcome: PupGateOutcome::StaleBlocked,
            }],
            "stale snapshot must block the entry: {actions:?}"
        );
    }

    #[test]
    fn pup_gate_fires_once_snapshot_is_fresh_again() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        // Stale snapshot blocks first (mirrors pup_gate_blocks_on_a_stale_snapshot).
        w.step(Event::IndicatorUpdate {
            p_up: 0.20, // p_side 0.80 >= 0.70 -> would pass, but it's stale
            ts: 1230.0,
        });
        assert!(matches!(
            fire_down_reversal(&mut w).first(),
            Some(Action::PupGateNote {
                outcome: PupGateOutcome::StaleBlocked,
                ..
            })
        ));
        assert!(matches!(w.state, WorkerState::Watching));

        // A fresh snapshot on a later tick lets the same still-armed signal
        // fire — the earlier block never called mark_fired().
        w.step(Event::IndicatorUpdate {
            p_up: 0.20,
            ts: 1251.0,
        });
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1252.0,
            price: 59_900.0,
        }));
        assert!(
            matches!(actions.first(), Some(Action::PlaceBuy { .. })),
            "{actions:?}"
        );
    }

    /// A vetoed entry can still fire on a later tick once p_up improves —
    /// the veto doesn't call `mark_fired()`, so the strategy stays armed.
    #[test]
    fn pup_gate_veto_does_not_permanently_block_the_cycle() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::IndicatorUpdate {
            p_up: 0.35, // p_side 0.65 < 0.70 -> veto
            // ts 1249 keeps this within PUP_GATE_MAX_AGE_SECS (2.0s) of the
            // triggering tick at 1250.
            ts: 1249.0,
        });
        let actions = fire_down_reversal(&mut w);
        assert!(matches!(
            actions.first(),
            Some(Action::PupGateNote {
                outcome: PupGateOutcome::Veto,
                ..
            })
        ));
        assert!(matches!(w.state, WorkerState::Watching));

        // p_up improves; conditions (dn=0.70, dp sign) are otherwise
        // unchanged since the cached binance/poly signals didn't move.
        w.step(Event::IndicatorUpdate {
            p_up: 0.20, // p_side 0.80 >= 0.70 -> pass
            ts: 1251.0,
        });
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1252.0,
            price: 59_900.0,
        }));
        assert!(
            matches!(actions.first(), Some(Action::PlaceBuy { .. })),
            "{actions:?}"
        );
        assert!(matches!(w.state, WorkerState::Entering));
    }

    /// high_prob never consults the pup gate, even when enabled — reversal only.
    #[test]
    fn pup_gate_is_ignored_by_high_prob() {
        let p = pup_gate_params(0.0);
        let mut w = Worker::new_high_prob("BTC", &p);
        // No IndicatorUpdate at all — if high_prob consulted the gate it
        // would still fail open (fire), so this alone doesn't prove
        // exemption; the real proof is that no PupGateNote ever appears
        // regardless of a would-be-vetoing snapshot below.
        w.step(Event::IndicatorUpdate {
            p_up: 0.01, // would veto almost anything if consulted
            ts: 1010.0,
        });
        w.step(Event::CycleOpen {
            ctx: ctx(1_000.0),
            slug: "btc-updown-5m-1000".to_string(),
        });
        w.step(Event::PolyTick(PolyTick {
            ts: 1010.0,
            up: 0.85,
            dn: 0.15,
            up_bid: 0.0,
            up_ask: 0.0,
        }));
        let actions = w.step(Event::BinanceTick(BinanceTick {
            ts: 1015.0,
            price: 60_100.0,
        }));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::PupGateNote { .. })),
            "{actions:?}"
        );
    }
}
