// Per-(asset, strategy) backtest state machine (A1 phase, §7/§8 of plan_rust_module.md).
//
// States: Watching, Holding(data), Halted.
// Exits (SL/unwind) fire on poly ticks; entry evaluation fires on both poly and
// binance ticks (poly is the primary/time-critical trigger, delta_pct is a
// directional filter checked against its latest cached value — see `try_enter`
// and trader/doc/latency_2026-07-04.md §8). Kept in sync with worker.rs's live
// entry logic so backtest results stay representative of live behavior.
// Fills are instant (sim venue) — no Entering/Unwinding/StopExiting needed for backtest.

use crate::config::AssetParams;
use crate::gates::{GateParams, check_gates};
use crate::signal::{
    DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal, Signal, SpreadSignal,
    VShapeSignal,
};
use crate::strategies::{HighProbStrategy, ReversalStrategy, VShapeStrategy};
use crate::types::{BinanceTick, CycleContext, EntryType, Outcome, PolyTick, Side, TradeRecord};

// ── Held position data ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HoldingData {
    pub side: Side,
    pub entry_type: EntryType,
    pub token_price: f64,
    pub entry_ts: f64,
    /// The poly-price observation's own timestamp (`LatestPolySignal::ts`) at the moment
    /// entry fired — see `TradeRecord::entry_price_ts`'s doc comment for why this can
    /// differ from `entry_ts`.
    pub entry_price_ts: f64,
    pub binance_at_entry: f64,
}

// ── State ─────────────────────────────────────────────────────────────────────

pub enum TradeState {
    Watching,
    Holding(HoldingData),
    Halted,
}

// ── Strategy variant ──────────────────────────────────────────────────────────

enum StrategyKind {
    Reversal(ReversalStrategy),
    HighProb(HighProbStrategy),
    VShape(VShapeStrategy),
}

// ── Machine ───────────────────────────────────────────────────────────────────

pub struct Machine {
    pub strategy_name: &'static str,
    kind: StrategyKind,
    // Signals owned by this machine
    saw_low_up: SawLowSignal,
    saw_low_dn: SawLowSignal,
    // V-shape two-stage latches — always present and updated (cheap, same
    // pattern as saw_low_* being fed for a high_prob machine), only read by
    // StrategyKind::VShape.
    v_up: VShapeSignal,
    v_dn: VShapeSignal,
    latest_poly: LatestPolySignal,
    spread: SpreadSignal,
    delta_pct: DeltaPctSignal,
    latest_binance: LatestBinanceSignal,
    // State
    state: TradeState,
    // Cycle context (set at cycle_open)
    cycle_open_binance: f64,
    pub last_binance: f64,
    cycle_start_ts: f64,
    cycle_end_ts: f64,
    cycle_slug: String,
    // Cached config
    sl: f64,     // strategy-specific absolute SL floor (0 = disabled)
    sl_pnl: f64, // PnL-relative SL (0 = disabled)
    unwind_pnl: f64,
    unwind_time: f64, // max holding time cap, seconds (0 = disabled)
    trade_size: f64,
    gate_params: GateParams,
}

#[inline]
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Seconds before cycle-end at which a still-open position is force-closed, labeled
/// `Outcome::Unwind` regardless of whether the take-profit price was actually reached.
/// Added 2026-07-14 so a position entered late in a cycle can no longer ride to a natural
/// WIN/LOSS cycle-close — `unwind_time`'s elapsed-*holding*-time cap alone doesn't cover
/// that case (an entry with less than `unwind_time` left in the cycle would otherwise still
/// resolve via `cycle_close`). `siglab`/`backtest.rs` path only — `worker.rs` (the live
/// driver) is untouched; see `siglab/doc/incident_same_entry_ts_2026-07-14.md`.
const FORCE_UNWIND_BEFORE_CYCLE_END_SECS: f64 = 10.0;

impl Machine {
    pub fn new_reversal(p: &AssetParams) -> Self {
        Self {
            strategy_name: "reversal",
            kind: StrategyKind::Reversal(ReversalStrategy::new(
                p.reversal,
                p.no_enter_when_time_left,
            )),
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
            state: TradeState::Watching,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            cycle_start_ts: 0.0,
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
            sl: p.sl_reversal,
            sl_pnl: p.sl_pnl_rev,
            unwind_pnl: p.unwind_pnl_rev,
            unwind_time: p.unwind_time_rev,
            trade_size: p.trade_size_usdc,
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
        }
    }

    pub fn new_high_prob(p: &AssetParams) -> Self {
        Self {
            strategy_name: "high_prob",
            kind: StrategyKind::HighProb(HighProbStrategy::new(
                p.price_low,
                p.price_high,
                p.enter_when_time_left,
                p.no_enter_when_time_left,
            )),
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
            state: TradeState::Watching,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            cycle_start_ts: 0.0,
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
            sl: p.sl_high_prob,
            sl_pnl: p.sl_pnl_hp,
            unwind_pnl: p.unwind_pnl_hp,
            unwind_time: p.unwind_time_hp,
            trade_size: p.trade_size_usdc,
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
        }
    }

    pub fn new_v_shape(p: &AssetParams) -> Self {
        Self {
            strategy_name: "v_shape",
            kind: StrategyKind::VShape(VShapeStrategy::new(p.v_high2, p.no_enter_when_time_left)),
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
            state: TradeState::Watching,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            cycle_start_ts: 0.0,
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
            sl: p.sl_v_shape,
            sl_pnl: p.sl_pnl_v,
            unwind_pnl: p.unwind_pnl_v,
            unwind_time: p.unwind_time_v,
            trade_size: p.trade_size_usdc,
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
        }
    }

    /// Reset for a new cycle. `entry_suppressed` → Halted (no entry this cycle).
    pub fn cycle_open(&mut self, ctx: &CycleContext, slug: &str, entry_suppressed: bool) {
        // Reset per-cycle signals
        self.saw_low_up.reset(ctx);
        self.saw_low_dn.reset(ctx);
        self.v_up.reset(ctx);
        self.v_dn.reset(ctx);
        self.delta_pct.reset(ctx);
        // latest_poly, spread, latest_binance do NOT reset (persist across cycles)

        match &mut self.kind {
            StrategyKind::Reversal(r) => r.reset(ctx),
            StrategyKind::HighProb(hp) => hp.reset(ctx),
            StrategyKind::VShape(v) => v.reset(ctx),
        }

        self.cycle_open_binance = ctx.open_binance;
        self.last_binance = ctx.open_binance;
        self.cycle_start_ts = ctx.start_ts;
        self.cycle_end_ts = ctx.end_ts;
        self.cycle_slug = slug.to_string();

        self.state = if entry_suppressed {
            TradeState::Halted
        } else {
            TradeState::Watching
        };
    }

    /// Process a poly tick. Returns a TradeRecord if an exit (SL or unwind) fires.
    pub fn on_poly(&mut self, tick: PolyTick) -> Option<TradeRecord> {
        // Update poly signals (always, regardless of state)
        self.latest_poly.on_poly(tick);
        self.spread.on_poly(tick);
        self.saw_low_up.on_poly(tick);
        self.saw_low_dn.on_poly(tick);
        self.v_up.on_poly(tick);
        self.v_dn.on_poly(tick);

        let h = match &self.state {
            TradeState::Holding(h) => h.clone(),
            // Not holding — a poly-side price crossing can complete the entry
            // condition itself (using the latest cached delta_pct); see try_enter.
            _ => {
                self.try_enter(tick.ts);
                return None;
            }
        };

        let exit_price = if h.side == Side::Up { tick.up } else { tick.dn };

        // Exit checks match Python _replay_cycle order exactly:

        // 1. PnL-based SL (checked first)
        if self.sl_pnl > 0.0 && exit_price <= h.token_price - self.sl_pnl {
            let sl_exit = h.token_price - self.sl_pnl;
            let pnl = round4(-self.trade_size * self.sl_pnl / h.token_price);
            return self.emit(h, Outcome::StopLoss, sl_exit, pnl);
        }

        // 2. Absolute SL
        if self.sl > 0.0 && exit_price < self.sl {
            let shares = self.trade_size / h.token_price;
            let pnl = round4(shares * exit_price - self.trade_size);
            return self.emit(h, Outcome::StopLoss, exit_price, pnl);
        }

        // 3. Take-profit unwind
        if self.unwind_pnl > 0.0 && exit_price >= h.token_price + self.unwind_pnl {
            let tp_exit = h.token_price + self.unwind_pnl;
            let pnl = round4(self.trade_size * self.unwind_pnl / h.token_price);
            return self.emit(h, Outcome::Unwind, tp_exit, pnl);
        }

        // 4. Force-unwind near cycle end (checked before the elapsed-holding-time
        // timeout below — guarantees no position rides to a natural WIN/LOSS
        // cycle-close; see FORCE_UNWIND_BEFORE_CYCLE_END_SECS's doc comment).
        if let Some(rec) = self.check_cycle_end_unwind(&h, tick.ts, exit_price) {
            return Some(rec);
        }

        // 5. Max holding time (checked last, after every other exit condition —
        // matches worker.rs's live ordering and the original Python
        // _replay_cycle order; see trader/doc/plan_unwind_time_2026-07-08.md).
        if let Some(rec) = self.check_timeout(&h, tick.ts, exit_price) {
            return Some(rec);
        }

        None
    }

    /// Process a binance tick. Evaluates entry when Watching; while Holding,
    /// only the max-holding-time check applies (try_enter is a no-op unless
    /// Watching, so it's skipped rather than called-and-ignored) — a position
    /// can time out on a binance-only tick since unwind_time is a pure
    /// elapsed-time cap, not conditioned on a poly crossing.
    pub fn on_binance(&mut self, tick: BinanceTick) -> Option<TradeRecord> {
        // Update binance signals (always)
        self.delta_pct.on_binance(tick);
        self.latest_binance.on_binance(tick);
        self.last_binance = tick.price;

        if let TradeState::Holding(h) = &self.state {
            let h = h.clone();
            let exit_price = if h.side == Side::Up {
                self.latest_poly.up()
            } else {
                self.latest_poly.dn()
            };
            if let Some(rec) = self.check_cycle_end_unwind(&h, tick.ts, exit_price) {
                return Some(rec);
            }
            return self.check_timeout(&h, tick.ts, exit_price);
        }

        self.try_enter(tick.ts);
        None
    }

    /// Force-close a held position once fewer than `FORCE_UNWIND_BEFORE_CYCLE_END_SECS`
    /// remain before the cycle ends, at whatever the current market price is — labeled
    /// `Outcome::Unwind` even when the take-profit price was never actually reached; see
    /// that constant's doc comment for why. Checked before `check_timeout` on both the
    /// poly and binance paths.
    fn check_cycle_end_unwind(
        &mut self,
        h: &HoldingData,
        now: f64,
        exit_price: f64,
    ) -> Option<TradeRecord> {
        if self.cycle_end_ts <= 0.0 || self.cycle_end_ts - now > FORCE_UNWIND_BEFORE_CYCLE_END_SECS
        {
            return None;
        }
        let shares = self.trade_size / h.token_price;
        let pnl = round4(shares * exit_price - self.trade_size);
        self.emit(h.clone(), Outcome::Unwind, exit_price, pnl)
    }

    /// Force-close a held position once it's been open >= `unwind_time`, at
    /// whatever the current market price is (win or lose) — a pure
    /// elapsed-time cap, not a PnL-based decision. `exit_price` is the
    /// caller's freshest known price for the held side (the current poly
    /// tick's own price from `on_poly`, or the last cached poly reading from
    /// `on_binance`). Counted toward `HaltTracker`'s loss-streak only when
    /// `pnl` lands negative — `Outcome::is_loss_for_halt(pnl)` — same as
    /// worker.rs's live behavior.
    fn check_timeout(&mut self, h: &HoldingData, now: f64, exit_price: f64) -> Option<TradeRecord> {
        if self.unwind_time <= 0.0 || (now - h.entry_ts) < self.unwind_time {
            return None;
        }
        let shares = self.trade_size / h.token_price;
        let pnl = round4(shares * exit_price - self.trade_size);
        self.emit(h.clone(), Outcome::Timeout, exit_price, pnl)
    }

    /// Entry evaluation, shared by `on_poly` and `on_binance` — see the module
    /// doc comment and worker.rs's identical `try_enter` for why both feeds
    /// need to be able to trigger it.
    fn try_enter(&mut self, now: f64) {
        if !matches!(self.state, TradeState::Watching) {
            return;
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

        let intent = match intent {
            Some(i) => i,
            None => return,
        };

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
            return;
        }

        let token_price = intent.token_price();
        match &mut self.kind {
            StrategyKind::Reversal(r) => r.mark_fired(),
            StrategyKind::HighProb(hp) => hp.mark_fired(),
            StrategyKind::VShape(v) => v.mark_fired(),
        }
        self.state = TradeState::Holding(HoldingData {
            side: intent.side,
            entry_type: intent.entry_type,
            token_price,
            entry_ts: now,
            entry_price_ts: self.latest_poly.ts,
            binance_at_entry: intent.binance_price,
        });
    }

    /// Resolve any open position at cycle end (WIN or LOSS from binance direction).
    pub fn cycle_close(&mut self) -> Option<TradeRecord> {
        let h = match &self.state {
            TradeState::Holding(h) => h.clone(),
            _ => return None,
        };

        let price_moved_up = self.last_binance > self.cycle_open_binance;
        let won = match h.side {
            Side::Up => price_moved_up,
            Side::Down => !price_moved_up,
        };

        let exit_price = if won { 1.0 } else { 0.0 };
        let shares = self.trade_size / h.token_price;
        let pnl = round4(shares * exit_price - self.trade_size);
        let outcome = if won { Outcome::Win } else { Outcome::Loss };
        self.emit(h, outcome, exit_price, pnl)
    }

    pub fn is_holding(&self) -> bool {
        matches!(self.state, TradeState::Holding(_))
    }

    fn emit(
        &mut self,
        h: HoldingData,
        outcome: Outcome,
        exit_price: f64,
        pnl: f64,
    ) -> Option<TradeRecord> {
        let rec = TradeRecord {
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
            exit_attempts: 0,
            exit_last_error: None,
            // Backtest fills are instantaneous (no real order round-trip) —
            // latency is only meaningful for the live driver (worker.rs).
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
            exit_signal_latency_ms: 0.0,
            exit_process_latency_ms: 0.0,
        };
        self.state = TradeState::Watching;
        Some(rec)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AssetParams;

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
            max_price_age_secs: 300.0, // large for unit tests; real value is 2.0
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

    /// Shared entry dance for the timeout tests below — same DOWN-reversal
    /// setup as `unwind_fires_on_poly_tick`, landing in `Holding` with
    /// `entry_ts=1240.0` (entry fires immediately on the recovery poly tick itself, using the delta_pct already cached from the ts=1200 binance tick — same mechanism as `entry_fires_on_poly_tick_using_cached_delta`), `token_price=0.70` (the ts=1240 poly tick's `dn`).
    fn enter_down_position(p: &AssetParams) -> Machine {
        let mut m = Machine::new_reversal(p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);

        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
        });
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        });
        m.on_poly(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
        });
        m.on_binance(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        });
        assert!(m.is_holding(), "setup: expected Holding after entry");
        m
    }

    #[test]
    fn timeout_force_closes_after_unwind_time_elapsed_on_poly_tick() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut m = enter_down_position(&p);

        // entry_ts=1240, threshold=30s -> fires at ts>=1270. dn=0.65 avoids
        // both SL (<=0.50) and unwind (>=0.73) so only timeout can explain it.
        let rec = m.on_poly(PolyTick {
            ts: 1270.0,
            up: 0.35,
            dn: 0.65,
        });
        assert!(rec.is_some(), "expected a TIMEOUT record");
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Timeout);
        assert!((rec.exit_price - 0.65).abs() < 1e-9);
        // pnl = shares*exit - trade_size = (1.0/0.70)*0.65 - 1.0
        assert!(
            (rec.pnl - (0.65 / 0.70 - 1.0)).abs() < 0.0001,
            "pnl={}",
            rec.pnl
        );
        assert!(
            !m.is_holding(),
            "state must return to Watching after timeout"
        );
    }

    #[test]
    fn timeout_force_closes_on_binance_only_tick() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut m = enter_down_position(&p);

        // No further PolyTick after entry — only a BinanceTick past the
        // threshold. exit_price must come from the cached latest_poly.dn()
        // (0.70, from the ts=1240 poly tick), not a poly tick of its own.
        let rec = m.on_binance(BinanceTick {
            ts: 1270.0,
            price: 59_850.0,
        });
        assert!(
            rec.is_some(),
            "expected a TIMEOUT record from the binance path"
        );
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Timeout);
        assert!(
            (rec.exit_price - 0.70).abs() < 1e-9,
            "exit_price={}",
            rec.exit_price
        );
        // exit_price == token_price (0.70) -> flat pnl (shares*0.70 - 1.0 == 0)
        assert!(rec.pnl.abs() < 1e-9, "pnl={}", rec.pnl);
    }

    #[test]
    fn timeout_does_not_fire_before_threshold_elapsed() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;
        let mut m = enter_down_position(&p);

        let rec = m.on_poly(PolyTick {
            ts: 1269.0, // entry_ts + 29s, 1s short of the 30s threshold
            up: 0.35,
            dn: 0.65,
        });
        assert!(rec.is_none(), "must not fire before the threshold elapses");
        assert!(m.is_holding(), "position must still be open");
    }

    #[test]
    fn timeout_disabled_when_unwind_time_zero() {
        let p = btc_params(); // unwind_time_rev defaults to 0.0 (disabled)
        let mut m = enter_down_position(&p);

        // Far past entry (would have timed out at any reasonable unwind_time), but still
        // inside the cycle and outside the FORCE_UNWIND_BEFORE_CYCLE_END_SECS window
        // (cycle ends at 1300.0 — see ctx(1_000.0)/enter_down_position) — isolates
        // "unwind_time=0 disables the elapsed-holding-time check" from the separate,
        // always-on cycle-end force-unwind tested below.
        let rec = m.on_poly(PolyTick {
            ts: 1280.0,
            up: 0.35,
            dn: 0.65,
        });
        assert!(
            rec.is_none(),
            "unwind_time=0.0 must disable the elapsed-holding-time timeout check"
        );
        assert!(m.is_holding());
    }

    /// A position still open inside `FORCE_UNWIND_BEFORE_CYCLE_END_SECS` of cycle-end must
    /// force-close labeled `Unwind`, at the current price, regardless of `unwind_time` (even
    /// disabled, as here) — added 2026-07-14 so late-cycle entries can no longer ride to a
    /// natural WIN/LOSS cycle-close; see FORCE_UNWIND_BEFORE_CYCLE_END_SECS's doc comment.
    #[test]
    fn force_unwinds_within_10s_of_cycle_end_even_with_timeout_disabled() {
        let p = btc_params(); // unwind_time_rev defaults to 0.0 (disabled)
        let mut m = enter_down_position(&p);

        // cycle ends at 1300.0 (ctx(1_000.0)/enter_down_position); 1291.0 is inside the 10s
        // window.
        let rec = m
            .on_poly(PolyTick {
                ts: 1291.0,
                up: 0.35,
                dn: 0.65,
            })
            .expect("must force-unwind within 10s of cycle end");
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!((rec.exit_price - 0.65).abs() < 1e-9);
        assert!(!m.is_holding());
    }

    /// Same rule, reached via a binance-only tick (no fresh poly tick) — mirrors
    /// `timeout_force_closes_on_binance_only_tick`'s coverage of the analogous case for the
    /// elapsed-holding-time check.
    #[test]
    fn force_unwinds_within_10s_of_cycle_end_on_binance_only_tick() {
        let p = btc_params();
        let mut m = enter_down_position(&p);

        let rec = m
            .on_binance(BinanceTick {
                ts: 1291.0,
                price: 59_900.0,
            })
            .expect("must force-unwind within 10s of cycle end on a binance-only tick");
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!(!m.is_holding());
    }

    /// The force-unwind is checked before the elapsed-holding-time timeout, so within the
    /// last 10s of the cycle the outcome is always Unwind, never Timeout, even when both
    /// conditions are simultaneously eligible.
    #[test]
    fn force_unwind_takes_priority_over_timeout_near_cycle_end() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0; // also eligible to fire on this same tick
        let mut m = enter_down_position(&p);

        // entry_ts=1240 (enter_down_position); ts=1291 is both >= entry_ts+30 (timeout-
        // eligible) and within 10s of cycle end (1300.0) — force-unwind must win.
        let rec = m
            .on_poly(PolyTick {
                ts: 1291.0,
                up: 0.35,
                dn: 0.65,
            })
            .expect("expected an exit");
        assert_eq!(rec.outcome, Outcome::Unwind);
    }

    #[test]
    fn stoploss_takes_priority_over_timeout_on_same_tick() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0; // also eligible to fire on this same tick
        let mut m = enter_down_position(&p);

        // ts=1280 (>= entry_ts+30, timeout-eligible) AND dn=0.45 (<= 0.70-0.20
        // sl_pnl floor, SL-eligible) — SL must win per the fixed check order.
        let rec = m.on_poly(PolyTick {
            ts: 1270.0,
            up: 0.55,
            dn: 0.45,
        });
        assert!(rec.is_some());
        assert_eq!(
            rec.unwrap().outcome,
            Outcome::StopLoss,
            "stop-loss must be checked before timeout, matching the fixed exit-chain order"
        );
    }

    #[test]
    fn timeout_pnl_can_be_positive_or_negative() {
        let mut p = btc_params();
        p.unwind_time_rev = 30.0;

        // Positive: exit above entry (0.70), but below the 0.73 unwind threshold.
        let mut m_up = enter_down_position(&p);
        let rec_up = m_up
            .on_poly(PolyTick {
                ts: 1270.0,
                up: 0.28,
                dn: 0.72,
            })
            .unwrap();
        assert_eq!(rec_up.outcome, Outcome::Timeout);
        assert!(rec_up.pnl > 0.0, "pnl={}", rec_up.pnl);

        // Negative: exit below entry (0.70), but above the 0.50 sl_pnl floor.
        let mut m_dn = enter_down_position(&p);
        let rec_dn = m_dn
            .on_poly(PolyTick {
                ts: 1270.0,
                up: 0.35,
                dn: 0.65,
            })
            .unwrap();
        assert_eq!(rec_dn.outcome, Outcome::Timeout);
        assert!(rec_dn.pnl < 0.0, "pnl={}", rec_dn.pnl);
    }

    #[test]
    fn unwind_fires_on_poly_tick() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Dip tick: dn=0.15 < threshold 0.20, ts=1180 → time_left=120 (within [10,120])
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
        });

        // Drop binance → delta_pct < 0 (required for DOWN entry)
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        });

        // Recovery: dn=0.70 > reversal threshold 0.60
        m.on_poly(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
        });

        // Entry evaluation: time_left=50 >= no_enter=10, all gates pass
        m.on_binance(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        });

        assert!(m.is_holding(), "expected Holding after entry");

        // Now trigger the unwind: dn goes above entry + 0.03
        // entry was at dn=0.70, unwind at 0.73
        let rec = m.on_poly(PolyTick {
            ts: 1260.0,
            up: 0.27,
            dn: 0.73,
        });
        assert!(rec.is_some(), "expected UNWIND record");
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!(
            (rec.pnl - 0.0429).abs() < 0.0001,
            "pnl ≈ 1.0*0.03/0.70 = 0.0429, got {}",
            rec.pnl
        );
    }

    #[test]
    fn sl_pnl_fires_before_absolute_sl() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Manually inject Holding state by triggering a reversal
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.80,
            dn: 0.15,
        }); // dn dips below 0.20
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        }); // dp < 0
        m.on_poly(PolyTick {
            ts: 1200.0,
            up: 0.25,
            dn: 0.75,
        }); // dn high
        m.on_binance(BinanceTick {
            ts: 1240.0,
            price: 59_900.0,
        }); // triggers entry

        assert!(m.is_holding());

        // Fire PnL SL: dn drops to entry - 0.20 = 0.75 - 0.20 = 0.55
        let rec = m.on_poly(PolyTick {
            ts: 1260.0,
            up: 0.45,
            dn: 0.55,
        });
        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::StopLoss);
        // pnl = -trade_size * sl_pnl / token_price = -1.0 * 0.20 / 0.75
        assert!(
            (rec.pnl - (-0.20 / 0.75)).abs() < 0.0001,
            "pnl={}, expected {}",
            rec.pnl,
            -0.20 / 0.75
        );
    }

    /// Complementary case to `unwind_fires_on_poly_tick`: delta_pct is already
    /// known (set by an earlier BinanceTick this cycle) by the time poly recovers,
    /// so entry must fire immediately off the PolyTick itself — mirrors worker.rs's
    /// `entry_fires_on_poly_tick_using_cached_delta` so backtest stays representative
    /// of live behavior (trader/doc/latency_2026-07-04.md §8).
    #[test]
    fn entry_fires_on_poly_tick_using_cached_delta() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
        }); // dip latches saw_low_dn
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        }); // dp < 0, cached

        // No further BinanceTick — the poly recovery tick alone must fire the entry.
        m.on_poly(PolyTick {
            ts: 1240.0,
            up: 0.30,
            dn: 0.70,
        });
        assert!(
            m.is_holding(),
            "expected Holding after poly-triggered entry"
        );
    }

    /// A cached delta_pct must only be trusted within the same cycle it was set
    /// in — reset() clears `price`, so a value left over from a previous cycle
    /// can't masquerade as "ready" this cycle.
    #[test]
    fn poly_tick_does_not_fire_using_stale_cross_cycle_delta() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);
        m.on_binance(BinanceTick {
            ts: 1100.0,
            price: 59_900.0,
        }); // dp < 0, this cycle

        // New cycle: delta_pct is reset even though last_binance still holds the
        // old price.
        m.cycle_open(&ctx(1_500.0), "btc-updown-5m-1500", false);
        m.on_poly(PolyTick {
            ts: 1680.0,
            up: 0.85,
            dn: 0.15,
        }); // dip latches saw_low_dn
        m.on_poly(PolyTick {
            ts: 1740.0,
            up: 0.30,
            dn: 0.70,
        }); // recovery, no BinanceTick yet this cycle
        assert!(
            !m.is_holding(),
            "must not fire on a delta_pct left over from the previous cycle"
        );
    }

    /// `entry_ts` is the *triggering* tick's timestamp (poly or binance, whichever
    /// caused `try_enter` to run); `entry_price_ts` must instead reflect when the poly
    /// price that actually satisfied the condition was observed. When a stale cached
    /// poly reading is what qualifies, and a later binance tick is what finally fires
    /// the entry (delta_pct only becomes ready then), the two timestamps must diverge —
    /// this is the mechanism behind the cross-duration `entry_ts` collision documented in
    /// siglab/doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md.
    #[test]
    fn entry_price_ts_reflects_stale_cached_poly_tick_not_triggering_binance_tick() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);

        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.85,
            dn: 0.15,
        }); // dip latches saw_low_dn

        // Recovery is visible now, but delta_pct isn't cached yet (no BinanceTick this
        // cycle) -- try_enter's dp check fails, so no entry yet. latest_poly is left
        // holding this (dn=0.70, ts=1210) reading.
        m.on_poly(PolyTick {
            ts: 1210.0,
            up: 0.30,
            dn: 0.70,
        });
        assert!(
            !m.is_holding(),
            "setup: must not fire without delta_pct cached"
        );

        // 40s later, a BinanceTick finally caches delta_pct in the right direction and
        // triggers try_enter -- entry fires using the stale ts=1210 poly reading, at
        // `now` = this binance tick's own ts=1250.
        let none = m.on_binance(BinanceTick {
            ts: 1250.0,
            price: 59_900.0,
        });
        assert!(none.is_none());
        assert!(
            m.is_holding(),
            "expected entry on the binance-triggered check"
        );

        // Force an immediate stop-loss to read back the recorded HoldingData via the
        // emitted TradeRecord (Machine exposes no direct HoldingData getter).
        let rec = m
            .on_poly(PolyTick {
                ts: 1211.0,
                up: 0.55,
                dn: 0.45, // <= entry(0.70) - sl_pnl_rev(0.20) = 0.50
            })
            .expect("stop-loss should fire");

        assert_eq!(
            rec.entry_ts, 1250.0,
            "entry_ts is the triggering binance tick"
        );
        assert_eq!(
            rec.entry_price_ts, 1210.0,
            "entry_price_ts must be the actual poly observation's own timestamp"
        );
        assert_ne!(
            rec.entry_ts, rec.entry_price_ts,
            "the whole point of the field: these differ when a binance tick fires \
             entry off a stale cached poly reading"
        );
        assert_eq!(
            rec.token_price, 0.70,
            "fill uses the cached poly price, not binance"
        );
    }

    #[test]
    fn halted_machine_skips_entry() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", true); // entry_suppressed

        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.80,
            dn: 0.15,
        });
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        });
        m.on_poly(PolyTick {
            ts: 1200.0,
            up: 0.25,
            dn: 0.75,
        });
        m.on_binance(BinanceTick {
            ts: 1240.0,
            price: 59_900.0,
        });

        assert!(!m.is_holding(), "halted machine must not enter");
        assert!(m.cycle_close().is_none());
    }

    #[test]
    fn win_at_cycle_close() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = CycleContext {
            start_ts: 1_000.0,
            end_ts: 1_300.0,
            open_binance: 60_000.0,
        };
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Enter DOWN position
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.80,
            dn: 0.15,
        });
        m.on_binance(BinanceTick {
            ts: 1200.0,
            price: 59_900.0,
        });
        m.on_poly(PolyTick {
            ts: 1200.0,
            up: 0.25,
            dn: 0.75,
        });
        m.on_binance(BinanceTick {
            ts: 1240.0,
            price: 59_900.0,
        });

        assert!(m.is_holding());

        // Cycle closes with price fell (last_binance < open → DOWN wins)
        let rec = m.cycle_close();
        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Win);
        // pnl = shares*1.0 - trade_size = 1.0/0.75 - 1.0 = 0.3333
        assert!(
            (rec.pnl - (1.0 / 0.75 - 1.0)).abs() < 0.0001,
            "pnl={}",
            rec.pnl
        );
    }

    // ── v_shape (2026-07-17, trader/doc/plan_v_shape_trader_2026-07-17.md) ──────
    //
    // Test list mirrors siglab/src/v_shape.rs's engine tests, re-expressed through
    // Machine's poly/binance tick interface: entry needs the full high1→low→high2
    // sequence with NO binance tick required (delta_pct_v=0.0), then the shared
    // exit chain (TP/SL/timeout/cycle-end force-unwind/cycle_close) applies.

    /// Drives the full V on the UP side and lands in Holding at up=0.70, ts=1240 —
    /// no binance tick is ever fed, proving entry is pure CLOB price action.
    fn enter_v_shape_up(p: &AssetParams) -> Machine {
        let mut m = Machine::new_v_shape(p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);
        m.on_poly(PolyTick {
            ts: 1100.0,
            up: 0.75,
            dn: 0.25,
        }); // high1 latched
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.25,
            dn: 0.75,
        }); // low-after-high latched
        m.on_poly(PolyTick {
            ts: 1240.0,
            up: 0.70,
            dn: 0.30,
        }); // >= high2 -> enters UP at 0.70
        assert!(m.is_holding(), "setup: expected Holding after full V");
        m
    }

    #[test]
    fn v_shape_enters_without_any_binance_tick_and_takes_profit() {
        let p = btc_params();
        let mut m = enter_v_shape_up(&p);
        // TP at entry + unwind_pnl_v = 0.70 + 0.05 = 0.75.
        let rec = m
            .on_poly(PolyTick {
                ts: 1250.0,
                up: 0.75,
                dn: 0.25,
            })
            .expect("take-profit must fire");
        assert_eq!(rec.strategy, "v_shape");
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!((rec.exit_price - 0.75).abs() < 1e-9);
        assert!((rec.pnl - (1.0 * 0.05 / 0.70)).abs() < 1e-4);
        assert!(!m.is_holding());
    }

    #[test]
    fn v_shape_no_entry_without_high1_first() {
        let p = btc_params();
        let mut m = Machine::new_v_shape(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);
        // Dip then recover, but up never reached high1 (0.70) before the dip.
        m.on_poly(PolyTick {
            ts: 1100.0,
            up: 0.25,
            dn: 0.75,
        });
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.72,
            dn: 0.28,
        });
        // (dn side latched its own high1 at ts=1100 (dn=0.75) but never dipped to
        // 0.30 after it, so neither side may fire.)
        assert!(!m.is_holding(), "V prefix incomplete on both sides");
    }

    #[test]
    fn v_shape_stop_loss_fires() {
        let p = btc_params();
        let mut m = enter_v_shape_up(&p);
        // sl_pnl_v = 0.30 -> floor at 0.70 - 0.30 = 0.40; 0.35 clears it with margin.
        let rec = m
            .on_poly(PolyTick {
                ts: 1250.0,
                up: 0.35,
                dn: 0.65,
            })
            .expect("stop-loss must fire");
        assert_eq!(rec.outcome, Outcome::StopLoss);
        assert!((rec.exit_price - 0.40).abs() < 1e-9);
        assert!(rec.pnl < 0.0);
    }

    #[test]
    fn v_shape_times_out_after_unwind_time_v() {
        let p = btc_params(); // unwind_time_v = 25.0 in the fixture
        let mut m = enter_v_shape_up(&p); // entry_ts = 1240
        // 0.72 is between SL floor (0.40) and TP (0.75); ts=1265 is 25s after entry
        // and still outside the 10s cycle-end window (ends 1300) — only timeout fits.
        let rec = m
            .on_poly(PolyTick {
                ts: 1265.0,
                up: 0.72,
                dn: 0.28,
            })
            .expect("timeout must fire at 25s");
        assert_eq!(rec.outcome, Outcome::Timeout);
        assert!((rec.exit_price - 0.72).abs() < 1e-9);
    }

    #[test]
    fn v_shape_force_unwinds_within_10s_of_cycle_end() {
        let mut p = btc_params();
        p.unwind_time_v = 0.0; // disable timeout to isolate the cycle-end rule
        let mut m = enter_v_shape_up(&p);
        let rec = m
            .on_poly(PolyTick {
                ts: 1291.0, // cycle ends 1300 — inside the 10s window
                up: 0.71,
                dn: 0.29,
            })
            .expect("must force-unwind near cycle end");
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!((rec.exit_price - 0.71).abs() < 1e-9);
    }

    #[test]
    fn v_shape_latches_reset_across_cycles() {
        let p = btc_params();
        let mut m = Machine::new_v_shape(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);
        m.on_poly(PolyTick {
            ts: 1100.0,
            up: 0.75,
            dn: 0.25,
        });
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.25,
            dn: 0.75,
        }); // full prefix latched this cycle

        // New cycle: the recovery alone must NOT fire off last cycle's prefix.
        m.cycle_open(&ctx(1_300.0), "btc-updown-5m-1300", false);
        m.on_poly(PolyTick {
            ts: 1400.0,
            up: 0.72,
            dn: 0.28,
        });
        assert!(
            !m.is_holding(),
            "V prefix from a previous cycle must not carry over"
        );
    }

    #[test]
    fn v_shape_resolves_at_cycle_close_via_binance_direction() {
        let p = btc_params();
        let mut m = enter_v_shape_up(&p);
        // UP position; last_binance (from a late tick) above cycle_open_binance -> WIN.
        m.on_binance(BinanceTick {
            ts: 1250.0,
            price: 60_100.0,
        });
        let rec = m.cycle_close().expect("open position resolves at close");
        assert_eq!(rec.outcome, Outcome::Win);
        assert!((rec.pnl - (1.0 / 0.70 - 1.0)).abs() < 1e-4);
    }

    #[test]
    fn v_shape_halted_machine_skips_entry() {
        let p = btc_params();
        let mut m = Machine::new_v_shape(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", true); // entry_suppressed
        m.on_poly(PolyTick {
            ts: 1100.0,
            up: 0.75,
            dn: 0.25,
        });
        m.on_poly(PolyTick {
            ts: 1180.0,
            up: 0.25,
            dn: 0.75,
        });
        m.on_poly(PolyTick {
            ts: 1240.0,
            up: 0.70,
            dn: 0.30,
        });
        assert!(!m.is_holding(), "halted v_shape machine must not enter");
    }
}
