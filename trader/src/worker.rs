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

use serde::{Deserialize, Serialize};

use crate::config::AssetParams;
use crate::execution::SellStatus;
use crate::gates::{check_gates, GateParams};
use crate::signal::{
    DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal, Signal, SpreadSignal,
};
use crate::strategies::{HighProbStrategy, ReversalStrategy};
use crate::types::{BinanceTick, CycleContext, EntryType, Outcome, PolyTick, Side, TradeRecord};

// ── Exit arm (how a Holding position's take-profit is worked) ────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExitArm {
    /// shares >= 5: resting GTC limit SELL on the book; fill arrives via UnwindFilled.
    GtcResting { order_id: String },
    /// shares < 5: no GTC support at that size; watch PolyTick and FAK-sell on TP cross.
    PriceMonitor { tp_price: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HoldingData {
    pub side: Side,
    pub entry_type: EntryType,
    pub token_price: f64,
    pub entry_ts: f64,
    pub shares: f64,
    pub exit_arm: ExitArm,
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
    Holding(HoldingData),
    /// Take-profit crossed or GTC fill notified; SELL in flight.
    Unwinding(HoldingData),
    /// Stop-loss floor crossed; FAK SELL in flight.
    StopExiting(HoldingData),
    /// Held WIN/LOSS awaiting async `ApiResult` confirmation (may flip + fix halt).
    Confirming(TradeRecord),
    /// STOPLOSS/UNWIND awaiting `ApiResult` for the CSV column only (pnl/halt final).
    EnrichOnly(TradeRecord),
}

// ── Events (drives everything; the machine never polls) ──────────────────────

#[derive(Debug, Clone)]
pub enum Event {
    CycleOpen { ctx: CycleContext, slug: String, entry_suppressed: bool },
    CycleClose,
    BinanceTick(BinanceTick),
    PolyTick(PolyTick),
    OrderFilled { filled_shares: f64, cost: f64 },
    OrderRejected,
    /// Response to the `Action::PlaceLimitSell` issued right after an entry fill.
    LimitSellPlaced { order_id: Option<String>, status: SellStatus },
    UnwindFilled { sold_shares: f64, exit_price: f64 },
    UnwindFailed,
    StopSellFilled { sold_shares: f64, exit_price: f64 },
    StopSellFailed,
    /// Async market-resolution confirmation (Gamma/CLOB), arriving after cycle end.
    ApiResult { won: bool },
    Control(ControlEvent),
    Balance(BalanceEvent),
}

#[derive(Debug, Clone, Copy)]
pub enum ControlEvent {
    Halt,
    Resume,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    PlaceBuy { side: Side, price: f64, size_usdc: f64 },
    PlaceLimitSell { shares: f64, price: f64 },
    ClosePosition { shares: f64, reason: CloseReason },
    CancelLimitSell { order_id: String },
    /// Write `PersistedState` to the crash-recovery file — call after every transition.
    Persist,
    LogTrade(TradeRecord),
    /// `ApiResult` flipped a Confirming (Win/Loss) record — `previous_outcome`/
    /// `previous_pnl` are the original estimate, `record` is the corrected one.
    LogTradeCorrection { previous_outcome: Outcome, previous_pnl: f64, record: TradeRecord },
    /// `ApiResult` resolved a StopLoss `EnrichOnly` record — counterfactual verdict
    /// only, never touches pnl/result/halt (unlike `LogTradeCorrection`).
    StopLossVerdict { record: TradeRecord, would_have_won: bool },
}

// ── Persisted state (crash recovery) ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistedWorkerState {
    Watching,
    Entering,
    Holding(HoldingData),
    Unwinding(HoldingData),
    StopExiting(HoldingData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedState {
    pub asset: String,
    pub strategy: String,
    pub slug: String,
    pub cycle_start: f64,
    pub cycle_end: f64,
    pub state: PersistedWorkerState,
}

// ── Strategy variant (mirrors machine.rs) ─────────────────────────────────────

enum StrategyKind {
    Reversal(ReversalStrategy),
    HighProb(HighProbStrategy),
}

#[inline]
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

// ── Worker ────────────────────────────────────────────────────────────────────

pub struct Worker {
    pub asset: String,
    pub strategy_name: &'static str,
    kind: StrategyKind,
    saw_low_up: SawLowSignal,
    saw_low_dn: SawLowSignal,
    latest_poly: LatestPolySignal,
    spread: SpreadSignal,
    delta_pct: DeltaPctSignal,
    latest_binance: LatestBinanceSignal,
    state: WorkerState,
    /// No-entry gate — set by `/halt`, the loss-limit tracker, or a balance
    /// drawdown; cleared by `/resume` or the daily reset. Never touches an
    /// in-flight Entering/Holding/Unwinding/StopExiting position (§8).
    entry_suppressed: bool,
    cycle_open_binance: f64,
    last_binance: f64,
    last_binance_ts_value: f64,
    cycle_start_ts: f64,
    cycle_end_ts: f64,
    cycle_slug: String,
    sl: f64,
    sl_pnl: f64,
    unwind_pnl: f64,
    trade_size: f64,
    gate_params: GateParams,
    /// Set when entering `Entering`, consumed when the fill/reject event lands.
    pending_entry: Option<(Side, EntryType, f64)>,
}

impl Worker {
    fn common(asset: &str, strategy_name: &'static str, kind: StrategyKind, p: &AssetParams, sl: f64) -> Self {
        Self {
            asset: asset.to_string(),
            strategy_name,
            kind,
            saw_low_up: SawLowSignal::new_up(p.reversal_low_threshold, p.reversal_start_time, p.no_enter_when_time_left),
            saw_low_dn: SawLowSignal::new_dn(p.reversal_low_threshold, p.reversal_start_time, p.no_enter_when_time_left),
            latest_poly: LatestPolySignal::new(),
            spread: SpreadSignal::new(),
            delta_pct: DeltaPctSignal::new(),
            latest_binance: LatestBinanceSignal::new(),
            state: WorkerState::Watching,
            entry_suppressed: false,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            last_binance_ts_value: 0.0,
            cycle_start_ts: 0.0,
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
            sl,
            sl_pnl: p.sl_pnl,
            unwind_pnl: p.unwind_pnl,
            trade_size: p.trade_size_usdc,
            pending_entry: None,
            gate_params: GateParams {
                spread_premium_limit: p.spread_premium_limit,
                spread_discount_limit: p.spread_discount_limit,
                max_price_age_secs: p.max_price_age_secs,
                delta_pct_rev: p.delta_pct_rev,
                delta_pct_hp: p.delta_pct_hp,
                max_buy_price: p.max_buy_price,
                price_high_rev: p.price_high_rev,
            },
        }
    }

    pub fn new_reversal(asset: &str, p: &AssetParams) -> Self {
        Self::common(
            asset,
            "reversal",
            StrategyKind::Reversal(ReversalStrategy::new(p.reversal, p.no_enter_when_time_left)),
            p,
            p.sl_reversal,
        )
    }

    pub fn new_high_prob(asset: &str, p: &AssetParams) -> Self {
        Self::common(
            asset,
            "high_prob",
            StrategyKind::HighProb(HighProbStrategy::new(
                p.price_low, p.price_high, p.enter_when_time_left, p.no_enter_when_time_left,
            )),
            p,
            p.sl_high_prob,
        )
    }

    pub fn is_halted(&self) -> bool {
        self.entry_suppressed
    }

    pub fn has_open_position(&self) -> bool {
        matches!(self.state, WorkerState::Entering | WorkerState::Holding(_) | WorkerState::Unwinding(_) | WorkerState::StopExiting(_))
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

    // ── Persistence ───────────────────────────────────────────────────────────

    pub fn to_persisted(&self) -> PersistedState {
        let state = match &self.state {
            WorkerState::Watching => PersistedWorkerState::Watching,
            WorkerState::Entering => PersistedWorkerState::Entering,
            WorkerState::Holding(h) => PersistedWorkerState::Holding(h.clone()),
            WorkerState::Unwinding(h) => PersistedWorkerState::Unwinding(h.clone()),
            WorkerState::StopExiting(h) => PersistedWorkerState::StopExiting(h.clone()),
            // Resolved/Confirming/EnrichOnly are not open-exposure states; a
            // crash there loses only the async-confirmation follow-up, not a
            // live position, so they persist as Watching (nothing to resume).
            WorkerState::Confirming(_) | WorkerState::EnrichOnly(_) => PersistedWorkerState::Watching,
        };
        PersistedState {
            asset: self.asset.clone(),
            strategy: self.strategy_name.to_string(),
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            cycle_end: self.cycle_end_ts,
            state,
        }
    }

    /// Reconcile a reloaded `PersistedState` against the live CLOB before
    /// resuming: a `Holding{GtcResting}` whose order is gone but whose token
    /// balance is still present resumes as `PriceMonitor`; a zero-balance
    /// position (already sold/redeemed) resumes as `Watching`. Pure function —
    /// testable without a live exchange by injecting the open-order/balance facts.
    pub fn reconcile(persisted: &PersistedWorkerState, open_order_ids: &[String], token_balance: f64) -> WorkerState {
        match persisted {
            PersistedWorkerState::Watching => WorkerState::Watching,
            PersistedWorkerState::Entering => {
                // The FAK either filled or didn't while we were down; with no
                // fill confirmation available, the safe default is to treat it
                // as not filled (no token balance implies nothing to resume).
                if token_balance > 0.0 {
                    WorkerState::Watching // conservative: can't reconstruct entry details
                } else {
                    WorkerState::Watching
                }
            }
            PersistedWorkerState::Holding(h) | PersistedWorkerState::Unwinding(h) | PersistedWorkerState::StopExiting(h) => {
                if token_balance <= 0.0 {
                    return WorkerState::Watching; // already resolved/sold while we were down
                }
                let mut h = h.clone();
                if let ExitArm::GtcResting { order_id } = &h.exit_arm {
                    if !open_order_ids.contains(order_id) {
                        // Resting order is gone but tokens remain — fall back to PriceMonitor.
                        h.exit_arm = ExitArm::PriceMonitor { tp_price: h.token_price + 0.0 };
                    }
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
            Event::CycleOpen { ctx, slug, entry_suppressed } => self.on_cycle_open(ctx, slug, entry_suppressed),
            Event::CycleClose => self.on_cycle_close(),
            Event::BinanceTick(t) => self.on_binance(t),
            Event::PolyTick(t) => self.on_poly(t),
            Event::OrderFilled { filled_shares, cost } => self.on_order_filled(filled_shares, cost),
            Event::OrderRejected => self.on_order_rejected(),
            Event::LimitSellPlaced { order_id, status } => self.on_limit_sell_placed(order_id, status),
            Event::UnwindFilled { sold_shares, exit_price } => self.on_unwind_filled(sold_shares, exit_price),
            Event::UnwindFailed => self.on_unwind_failed(),
            Event::StopSellFilled { sold_shares, exit_price } => self.on_stop_sell_filled(sold_shares, exit_price),
            Event::StopSellFailed => self.on_stop_sell_failed(),
            Event::ApiResult { won } => self.on_api_result(won),
            Event::Control(c) => self.on_control(c),
            Event::Balance(b) => self.on_balance(b),
        }
    }

    fn on_cycle_open(&mut self, ctx: CycleContext, slug: String, entry_suppressed: bool) -> Vec<Action> {
        self.saw_low_up.reset(&ctx);
        self.saw_low_dn.reset(&ctx);
        self.delta_pct.reset(&ctx);
        match &mut self.kind {
            StrategyKind::Reversal(r) => r.reset(&ctx),
            StrategyKind::HighProb(hp) => hp.reset(&ctx),
        }
        self.cycle_open_binance = ctx.open_binance;
        self.last_binance = ctx.open_binance;
        self.cycle_start_ts = ctx.start_ts;
        self.cycle_end_ts = ctx.end_ts;
        self.cycle_slug = slug;
        self.entry_suppressed = entry_suppressed;
        // A fresh cycle never inherits an in-flight position from the last one
        // (each cycle's trade is fully resolved before the next opens).
        self.state = WorkerState::Watching;
        vec![Action::Persist]
    }

    fn on_cycle_close(&mut self) -> Vec<Action> {
        // Any open position — Holding, or Unwinding/StopExiting that hadn't
        // resolved yet — is held to maturity: a failed/incomplete early exit
        // is not an exit (invariant). Only Holding's data is needed to compute
        // the WIN/LOSS outcome.
        let holding = match &self.state {
            WorkerState::Holding(h) | WorkerState::Unwinding(h) | WorkerState::StopExiting(h) => Some(h.clone()),
            _ => None,
        };
        let Some(h) = holding else {
            self.state = WorkerState::Watching;
            return vec![];
        };

        let price_moved_up = self.last_binance > self.cycle_open_binance;
        let won = match h.side {
            Side::Up => price_moved_up,
            Side::Down => !price_moved_up,
        };
        let exit_price = if won { 1.0 } else { 0.0 };
        let pnl = round4(h.shares * exit_price - self.trade_size);
        let outcome = if won { Outcome::Win } else { Outcome::Loss };

        let record = TradeRecord {
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name,
            side: h.side,
            entry_ts: h.entry_ts,
            token_price: h.token_price,
            exit_price,
            outcome,
            pnl,
        };
        // Held WIN/LOSS spawns Confirming — an ApiResult mismatch can still flip it.
        self.state = WorkerState::Confirming(record.clone());
        vec![Action::LogTrade(record), Action::Persist]
    }

    fn on_binance(&mut self, tick: BinanceTick) -> Vec<Action> {
        self.delta_pct.on_binance(tick);
        self.latest_binance.on_binance(tick);
        self.last_binance = tick.price;
        self.last_binance_ts_value = tick.ts;

        if self.entry_suppressed || !matches!(self.state, WorkerState::Watching) {
            return vec![];
        }

        let intent = match &self.kind {
            StrategyKind::Reversal(r) => r.evaluate(tick.ts, &self.saw_low_up, &self.saw_low_dn, &self.latest_poly, &self.delta_pct, &self.latest_binance),
            StrategyKind::HighProb(hp) => hp.evaluate(tick.ts, &self.latest_poly, &self.delta_pct, &self.latest_binance),
        };
        let Some(intent) = intent else { return vec![] };

        if check_gates(&intent, &self.spread, &self.latest_poly, &self.delta_pct, &self.gate_params, tick.ts).is_some() {
            return vec![];
        }

        match &mut self.kind {
            StrategyKind::Reversal(r) => r.mark_fired(),
            StrategyKind::HighProb(hp) => hp.mark_fired(),
        }
        self.state = WorkerState::Entering;
        // Stash the intent's side/entry_type/token_price for when the fill lands.
        self.pending_entry = Some((intent.side, intent.entry_type, intent.token_price()));
        vec![Action::PlaceBuy { side: intent.side, price: intent.token_price(), size_usdc: self.trade_size }, Action::Persist]
    }

    fn on_poly(&mut self, tick: PolyTick) -> Vec<Action> {
        self.latest_poly.on_poly(tick);
        self.spread.on_poly(tick);
        self.saw_low_up.on_poly(tick);
        self.saw_low_dn.on_poly(tick);

        let WorkerState::Holding(h) = &self.state else { return vec![] };
        let h = h.clone();
        let exit_price = if h.side == Side::Up { tick.up } else { tick.dn };

        // Stop-loss (both PnL-based and absolute) always fires off PolyTick,
        // regardless of exit_arm — cancel any resting GTC first, then FAK-close.
        let sl_hit = (self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl)
            || (self.sl > 0.0 && exit_price < self.sl);
        if sl_hit {
            self.state = WorkerState::StopExiting(h.clone());
            let mut actions = vec![];
            if let ExitArm::GtcResting { order_id } = &h.exit_arm {
                actions.push(Action::CancelLimitSell { order_id: order_id.clone() });
            }
            actions.push(Action::ClosePosition { shares: h.shares, reason: CloseReason::StopLoss });
            actions.push(Action::Persist);
            return actions;
        }

        // Take-profit: only the PriceMonitor arm reacts to PolyTick directly —
        // a GtcResting arm's fill arrives via UnwindFilled instead.
        if let ExitArm::PriceMonitor { tp_price } = h.exit_arm {
            if exit_price >= tp_price {
                self.state = WorkerState::Unwinding(h.clone());
                return vec![Action::ClosePosition { shares: h.shares, reason: CloseReason::TakeProfit }, Action::Persist];
            }
        }

        vec![]
    }

    fn on_order_filled(&mut self, filled_shares: f64, cost: f64) -> Vec<Action> {
        if !matches!(self.state, WorkerState::Entering) {
            return vec![];
        }
        let Some((side, entry_type, _intent_price)) = self.pending_entry.take() else { return vec![] };
        if filled_shares <= 0.0 {
            self.state = WorkerState::Watching;
            return vec![Action::Persist];
        }

        let tp_price = cost + self.unwind_pnl;
        let (exit_arm, mut actions) = if filled_shares >= 5.0 {
            // Attempt a resting GTC; the actual order_id/status comes back via
            // LimitSellPlaced. Use PriceMonitor as the provisional arm so a
            // stop-loss can still fire correctly if that response is slow.
            (ExitArm::PriceMonitor { tp_price }, vec![Action::PlaceLimitSell { shares: filled_shares, price: tp_price }])
        } else {
            (ExitArm::PriceMonitor { tp_price }, vec![])
        };

        let holding = HoldingData { side, entry_type, token_price: cost, entry_ts: self.last_binance_ts(), shares: filled_shares, exit_arm };
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

    fn on_limit_sell_placed(&mut self, order_id: Option<String>, status: SellStatus) -> Vec<Action> {
        let WorkerState::Holding(h) = &mut self.state else { return vec![] };
        match status {
            SellStatus::Live => {
                if let Some(id) = order_id {
                    h.exit_arm = ExitArm::GtcResting { order_id: id };
                }
                vec![Action::Persist]
            }
            SellStatus::Matched => {
                // Marketable limit — filled immediately; this *is* the unwind.
                let h = h.clone();
                let exit_price = h.token_price + self.unwind_pnl;
                let pnl = round4(self.trade_size * self.unwind_pnl / h.token_price);
                let record = TradeRecord {
                    slug: self.cycle_slug.clone(), cycle_start: self.cycle_start_ts,
                    strategy: self.strategy_name, side: h.side, entry_ts: h.entry_ts,
                    token_price: h.token_price, exit_price, outcome: Outcome::Unwind, pnl,
                };
                self.state = WorkerState::EnrichOnly(record.clone());
                vec![Action::LogTrade(record), Action::Persist]
            }
            SellStatus::Failed | SellStatus::DryRun => {
                // Fall back to price-monitor backstop; stop-loss stays armed regardless.
                let tp_price = h.token_price + self.unwind_pnl;
                h.exit_arm = ExitArm::PriceMonitor { tp_price };
                vec![Action::Persist]
            }
        }
    }

    fn on_unwind_filled(&mut self, sold_shares: f64, exit_price: f64) -> Vec<Action> {
        let WorkerState::Unwinding(h) = &self.state else { return vec![] };
        let h = h.clone();
        if sold_shares < h.shares {
            // Partial fill — residual continues to be managed.
            let residual = HoldingData { shares: h.shares - sold_shares, ..h };
            self.state = WorkerState::Holding(residual);
            return vec![Action::Persist];
        }
        let pnl = round4(self.trade_size * self.unwind_pnl / h.token_price);
        let record = TradeRecord {
            slug: self.cycle_slug.clone(), cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name, side: h.side, entry_ts: h.entry_ts,
            token_price: h.token_price, exit_price, outcome: Outcome::Unwind, pnl,
        };
        self.state = WorkerState::EnrichOnly(record.clone());
        vec![Action::LogTrade(record), Action::Persist]
    }

    fn on_unwind_failed(&mut self) -> Vec<Action> {
        // A failed sell is not an exit — reclassify as held, resolved at cycle end.
        if let WorkerState::Unwinding(h) = &self.state {
            self.state = WorkerState::Holding(h.clone());
        }
        vec![Action::Persist]
    }

    fn on_stop_sell_filled(&mut self, sold_shares: f64, exit_price: f64) -> Vec<Action> {
        let WorkerState::StopExiting(h) = &self.state else { return vec![] };
        let h = h.clone();
        if sold_shares < h.shares {
            let residual = HoldingData { shares: h.shares - sold_shares, ..h };
            self.state = WorkerState::Holding(residual);
            return vec![Action::Persist];
        }
        // Absolute-SL-style pnl (proceeds − stake); PnL-SL is computed at trigger
        // time in on_poly in a live system, but here we use the realized exit
        // price uniformly, matching the sim/backtest STOPLOSS formula.
        let shares = self.trade_size / h.token_price;
        let pnl = round4(shares * exit_price - self.trade_size);
        let record = TradeRecord {
            slug: self.cycle_slug.clone(), cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name, side: h.side, entry_ts: h.entry_ts,
            token_price: h.token_price, exit_price, outcome: Outcome::StopLoss, pnl,
        };
        self.state = WorkerState::EnrichOnly(record.clone());
        vec![Action::LogTrade(record), Action::Persist]
    }

    fn on_stop_sell_failed(&mut self) -> Vec<Action> {
        if let WorkerState::StopExiting(h) = &self.state {
            self.state = WorkerState::Holding(h.clone());
        }
        vec![Action::Persist]
    }

    fn on_api_result(&mut self, won: bool) -> Vec<Action> {
        match &self.state {
            WorkerState::Confirming(original) => {
                let flip_needed = won != (original.outcome == Outcome::Win);
                if !flip_needed {
                    self.state = WorkerState::Watching;
                    return vec![Action::Persist];
                }
                let previous_outcome = original.outcome;
                let previous_pnl = original.pnl;
                let mut record = original.clone();
                self.state = WorkerState::Watching;
                let shares = self.trade_size / record.token_price;
                let exit_price = if won { 1.0 } else { 0.0 };
                record.outcome = if won { Outcome::Win } else { Outcome::Loss };
                record.exit_price = exit_price;
                record.pnl = round4(shares * exit_price - self.trade_size);
                vec![Action::LogTradeCorrection { previous_outcome, previous_pnl, record }, Action::Persist]
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
                    Some(Action::StopLossVerdict { record: record.clone(), would_have_won: won })
                } else {
                    None
                };
                self.state = WorkerState::Watching;
                match verdict {
                    Some(action) => vec![action, Action::Persist],
                    None => vec![Action::Persist],
                }
            }
            _ => vec![],
        }
    }

    fn on_control(&mut self, event: ControlEvent) -> Vec<Action> {
        match event {
            ControlEvent::Halt => self.entry_suppressed = true,
            ControlEvent::Resume => self.entry_suppressed = false,
        }
        // No state change — halt/resume never touch an in-flight position.
        vec![]
    }

    fn on_balance(&mut self, event: BalanceEvent) -> Vec<Action> {
        match event {
            BalanceEvent::DrawdownHalt => self.entry_suppressed = true,
        }
        vec![]
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
            price_high_rev: 0.90,
            delta_pct_rev: 0.0008,
            sl_reversal: 0.0,
            unwind_pnl: 0.03,
            sl_pnl: 0.20,
            price_low: 0.80,
            price_high: 0.93,
            delta_pct_hp: 0.0004,
            sl_high_prob: 0.49,
            halt_rev: 2,
            halt_prob: 2,
            halt_reset_hour_rev: 2,
            halt_reset_hour_hp: 8,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 300.0, // large for unit tests; real config: 2.0
            trade_size_usdc: 1.0,
        }
    }

    fn ctx(start: f64) -> CycleContext {
        CycleContext { start_ts: start, end_ts: start + 300.0, open_binance: 60_000.0 }
    }

    /// Drives a worker from cycle-open through a filled DOWN reversal entry,
    /// returning it positioned in `Holding` with the given `filled_shares`.
    fn enter_down_position(w: &mut Worker, filled_shares: f64) {
        w.step(Event::CycleOpen { ctx: ctx(1_000.0), slug: "btc-updown-5m-1000".to_string(), entry_suppressed: false });
        w.step(Event::PolyTick(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 })); // dip latches saw_low_dn
        w.step(Event::BinanceTick(BinanceTick { ts: 1200.0, price: 59_900.0 })); // dp < 0
        w.step(Event::PolyTick(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 })); // recovery > reversal 0.60
        let actions = w.step(Event::BinanceTick(BinanceTick { ts: 1250.0, price: 59_900.0 })); // fires entry
        assert!(matches!(actions.as_slice(), [Action::PlaceBuy { .. }, Action::Persist]), "expected entry to fire: {actions:?}");
        assert!(matches!(w.state, WorkerState::Entering));
        w.step(Event::OrderFilled { filled_shares, cost: 0.70 });
    }

    #[test]
    fn entry_fires_and_transitions_to_entering() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen { ctx: ctx(1_000.0), slug: "btc-updown-5m-1000".to_string(), entry_suppressed: false });
        w.step(Event::PolyTick(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 }));
        w.step(Event::BinanceTick(BinanceTick { ts: 1200.0, price: 59_900.0 }));
        w.step(Event::PolyTick(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 }));
        let actions = w.step(Event::BinanceTick(BinanceTick { ts: 1250.0, price: 59_900.0 }));
        assert_eq!(actions, vec![
            Action::PlaceBuy { side: Side::Down, price: 0.70, size_usdc: 1.0 },
            Action::Persist,
        ]);
        assert!(matches!(w.state, WorkerState::Entering));
    }

    #[test]
    fn small_fill_uses_price_monitor_arm() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 3.0); // < 5 shares
        match &w.state {
            WorkerState::Holding(h) => {
                assert_eq!(h.shares, 3.0);
                assert!(matches!(h.exit_arm, ExitArm::PriceMonitor { .. }), "expected PriceMonitor arm, got {:?}", h.exit_arm);
            }
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn large_fill_attempts_gtc_limit_sell() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        let actions = {
            w.step(Event::CycleOpen { ctx: ctx(1_000.0), slug: "btc-updown-5m-1000".to_string(), entry_suppressed: false });
            w.step(Event::PolyTick(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 }));
            w.step(Event::BinanceTick(BinanceTick { ts: 1200.0, price: 59_900.0 }));
            w.step(Event::PolyTick(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 }));
            w.step(Event::BinanceTick(BinanceTick { ts: 1250.0, price: 59_900.0 }));
            w.step(Event::OrderFilled { filled_shares: 10.0, cost: 0.70 })
        };
        assert!(actions.iter().any(|a| matches!(a, Action::PlaceLimitSell { shares, .. } if *shares == 10.0)),
            "expected a PlaceLimitSell action for a >=5 share fill: {actions:?}");
    }

    #[test]
    fn limit_sell_live_arms_gtc_resting() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced { order_id: Some("order-123".to_string()), status: SellStatus::Live });
        match &w.state {
            WorkerState::Holding(h) => assert_eq!(h.exit_arm, ExitArm::GtcResting { order_id: "order-123".to_string() }),
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn limit_sell_failed_falls_back_to_price_monitor() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced { order_id: None, status: SellStatus::Failed });
        match &w.state {
            WorkerState::Holding(h) => assert!(matches!(h.exit_arm, ExitArm::PriceMonitor { .. })),
            _ => panic!("expected Holding"),
        }
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
        let actions = w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.55, dn: 0.45 }));
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
        assert!(actions.iter().any(|a| matches!(a, Action::ClosePosition { .. })));
    }

    #[test]
    fn halt_suppresses_only_new_entries() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        w.step(Event::CycleOpen { ctx: ctx(1_000.0), slug: "btc-updown-5m-1000".to_string(), entry_suppressed: true });
        w.step(Event::PolyTick(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 }));
        w.step(Event::BinanceTick(BinanceTick { ts: 1200.0, price: 59_900.0 }));
        w.step(Event::PolyTick(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 }));
        let actions = w.step(Event::BinanceTick(BinanceTick { ts: 1250.0, price: 59_900.0 }));
        assert!(actions.is_empty(), "halted worker must not enter: {actions:?}");
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn partial_unwind_fill_leaves_residual_holding() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        // Trigger unwind via PriceMonitor (small-fill style arm stays PriceMonitor
        // until a GTC confirms; force via direct state mutation isn't available,
        // so drive through the natural TP-cross path.)
        w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.27, dn: 0.73 })); // entry 0.70 + unwind 0.03
        assert!(matches!(w.state, WorkerState::Unwinding(_)));

        let actions = w.step(Event::UnwindFilled { sold_shares: 6.0, exit_price: 0.73 });
        match &w.state {
            WorkerState::Holding(h) => assert_eq!(h.shares, 4.0, "residual = 10 - 6"),
            _ => panic!("expected residual Holding"),
        }
        assert!(!actions.iter().any(|a| matches!(a, Action::LogTrade(_))), "partial fill must not log a trade yet");
    }

    #[test]
    fn full_unwind_fill_logs_trade_and_goes_enrich_only() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.27, dn: 0.73 }));
        let actions = w.step(Event::UnwindFilled { sold_shares: 10.0, exit_price: 0.73 });

        let record = actions.iter().find_map(|a| if let Action::LogTrade(r) = a { Some(r.clone()) } else { None });
        let record = record.expect("expected a LogTrade action");
        assert_eq!(record.outcome, Outcome::Unwind);
        assert!(matches!(w.state, WorkerState::EnrichOnly(_)));
    }

    #[test]
    fn stop_loss_fires_and_cancels_resting_gtc_first() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::LimitSellPlaced { order_id: Some("order-1".to_string()), status: SellStatus::Live });

        // dn drops below entry(0.70) - sl_pnl(0.20) = 0.50 (use 0.49 to clear the
        // f64 boundary cleanly: 0.70 - 0.20 == 0.4999999999999999 in f64).
        let actions = w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.45, dn: 0.49 }));
        assert_eq!(actions, vec![
            Action::CancelLimitSell { order_id: "order-1".to_string() },
            Action::ClosePosition { shares: 10.0, reason: CloseReason::StopLoss },
            Action::Persist,
        ]);
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
    }

    #[test]
    fn failed_stop_sell_reclassifies_as_held() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.45, dn: 0.49 })); // triggers StopExiting
        assert!(matches!(w.state, WorkerState::StopExiting(_)));

        w.step(Event::StopSellFailed);
        assert!(matches!(w.state, WorkerState::Holding(_)), "failed exit is not an exit — reclassified as held");
    }

    #[test]
    fn cycle_close_with_open_position_resolves_win_loss_and_spawns_confirming() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);

        // Price fell below open (60000 -> 59900) so DOWN wins at cycle close.
        let actions = w.step(Event::CycleClose);
        let record = actions.iter().find_map(|a| if let Action::LogTrade(r) = a { Some(r.clone()) } else { None }).unwrap();
        assert_eq!(record.outcome, Outcome::Win);
        assert!(matches!(w.state, WorkerState::Confirming(_)));
    }

    #[test]
    fn api_result_flips_confirming_outcome_and_recomputes_pnl() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::CycleClose); // -> Confirming(WIN)

        let actions = w.step(Event::ApiResult { won: false }); // API says it actually lost
        let (previous_outcome, previous_pnl, record) = actions.iter().find_map(|a| {
            if let Action::LogTradeCorrection { previous_outcome, previous_pnl, record } = a {
                Some((*previous_outcome, *previous_pnl, record.clone()))
            } else {
                None
            }
        }).unwrap();
        assert_eq!(previous_outcome, Outcome::Win);
        assert!(previous_pnl > 0.0, "original estimate should have been a WIN pnl, got {previous_pnl}");
        assert_eq!(record.outcome, Outcome::Loss);
        assert!((record.pnl - (-1.0)).abs() < 1e-9, "LOSS pnl should be -trade_size, got {}", record.pnl);
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn api_result_on_enrich_only_never_touches_pnl() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.27, dn: 0.73 }));
        w.step(Event::UnwindFilled { sold_shares: 10.0, exit_price: 0.73 }); // -> EnrichOnly(Unwind)

        let actions = w.step(Event::ApiResult { won: true });
        assert!(!actions.iter().any(|a| matches!(a, Action::LogTrade(_))), "EnrichOnly must never re-log a trade");
        assert!(!actions.iter().any(|a| matches!(a, Action::StopLossVerdict { .. })),
            "an UNWIND exit gets no counterfactual verdict, matching Python's is_unwind skip");
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn api_result_on_stop_loss_enrich_only_emits_verdict() {
        let p = btc_params();
        let mut w = Worker::new_reversal("BTC", &p);
        enter_down_position(&mut w, 10.0);
        // DOWN position; poly tick crosses the stop-loss floor -> StopExiting.
        w.step(Event::PolyTick(PolyTick { ts: 1260.0, up: 0.55, dn: 0.45 }));
        assert!(matches!(w.state, WorkerState::StopExiting(_)));
        w.step(Event::StopSellFilled { sold_shares: 10.0, exit_price: 0.45 }); // -> EnrichOnly(StopLoss)

        // `won` is already relative to the record's own side (matches the Confirming
        // branch's convention) — `true` here means the position's side actually won,
        // i.e. the stop-loss was costly (holding would have won instead).
        let actions = w.step(Event::ApiResult { won: true });
        let (record, would_have_won) = actions.iter().find_map(|a| {
            if let Action::StopLossVerdict { record, would_have_won } = a { Some((record.clone(), *would_have_won)) } else { None }
        }).unwrap();
        assert_eq!(record.outcome, Outcome::StopLoss);
        assert!(would_have_won, "API says the position's side actually won -> stop was costly");
        assert!(!actions.iter().any(|a| matches!(a, Action::LogTrade(_) | Action::LogTradeCorrection { .. })),
            "verdict never rewrites pnl/result");
        assert!(matches!(w.state, WorkerState::Watching));
    }

    #[test]
    fn reconcile_holding_with_missing_gtc_order_falls_back_to_price_monitor() {
        let holding = HoldingData {
            side: Side::Down, entry_type: EntryType::Reversal, token_price: 0.70,
            entry_ts: 1250.0, shares: 10.0,
            exit_arm: ExitArm::GtcResting { order_id: "gone-order".to_string() },
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
            side: Side::Down, entry_type: EntryType::Reversal, token_price: 0.70,
            entry_ts: 1250.0, shares: 10.0,
            exit_arm: ExitArm::GtcResting { order_id: "still-live".to_string() },
        };
        let persisted = PersistedWorkerState::Holding(holding);
        let resumed = Worker::reconcile(&persisted, &["still-live".to_string()], 10.0);
        match resumed {
            WorkerState::Holding(h) => assert_eq!(h.exit_arm, ExitArm::GtcResting { order_id: "still-live".to_string() }),
            _ => panic!("expected Holding"),
        }
    }

    #[test]
    fn reconcile_zero_balance_position_resumes_as_watching() {
        let holding = HoldingData {
            side: Side::Up, entry_type: EntryType::Reversal, token_price: 0.70,
            entry_ts: 1250.0, shares: 10.0,
            exit_arm: ExitArm::PriceMonitor { tp_price: 0.73 },
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
        w.step(Event::CycleOpen { ctx: ctx(1_000.0), slug: "btc-updown-5m-1000".to_string(), entry_suppressed: false });
        w.step(Event::PolyTick(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 }));
        w.step(Event::BinanceTick(BinanceTick { ts: 1200.0, price: 59_900.0 }));
        w.step(Event::PolyTick(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 }));
        w.step(Event::BinanceTick(BinanceTick { ts: 1250.0, price: 59_900.0 }));
        assert!(matches!(w.state, WorkerState::Entering));

        w.step(Event::OrderRejected);
        assert!(matches!(w.state, WorkerState::Watching));
    }
}
