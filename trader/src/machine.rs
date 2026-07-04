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
use crate::gates::{check_gates, GateParams};
use crate::signal::{DeltaPctSignal, LatestBinanceSignal, LatestPolySignal, SawLowSignal, Signal,
                    SpreadSignal};
use crate::strategies::{HighProbStrategy, ReversalStrategy};
use crate::types::{BinanceTick, CycleContext, EntryType, Outcome, PolyTick, Side, TradeRecord};

// ── Held position data ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HoldingData {
    pub side: Side,
    pub entry_type: EntryType,
    pub token_price: f64,
    pub entry_ts: f64,
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
}

// ── Machine ───────────────────────────────────────────────────────────────────

pub struct Machine {
    pub strategy_name: &'static str,
    kind: StrategyKind,
    // Signals owned by this machine
    saw_low_up: SawLowSignal,
    saw_low_dn: SawLowSignal,
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
    cycle_slug: String,
    // Cached config
    sl: f64,        // strategy-specific absolute SL floor (0 = disabled)
    sl_pnl: f64,    // PnL-relative SL (0 = disabled)
    unwind_pnl: f64,
    trade_size: f64,
    gate_params: GateParams,
}

#[inline]
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

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
            latest_poly: LatestPolySignal::new(),
            spread: SpreadSignal::new(),
            delta_pct: DeltaPctSignal::new(),
            latest_binance: LatestBinanceSignal::new(),
            state: TradeState::Watching,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            cycle_start_ts: 0.0,
            cycle_slug: String::new(),
            sl: p.sl_reversal,
            sl_pnl: p.sl_pnl_rev,
            unwind_pnl: p.unwind_pnl_rev,
            trade_size: p.trade_size_usdc,
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
            latest_poly: LatestPolySignal::new(),
            spread: SpreadSignal::new(),
            delta_pct: DeltaPctSignal::new(),
            latest_binance: LatestBinanceSignal::new(),
            state: TradeState::Watching,
            cycle_open_binance: 0.0,
            last_binance: 0.0,
            cycle_start_ts: 0.0,
            cycle_slug: String::new(),
            sl: p.sl_high_prob,
            sl_pnl: p.sl_pnl_hp,
            unwind_pnl: p.unwind_pnl_hp,
            trade_size: p.trade_size_usdc,
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

    /// Reset for a new cycle. `entry_suppressed` → Halted (no entry this cycle).
    pub fn cycle_open(&mut self, ctx: &CycleContext, slug: &str, entry_suppressed: bool) {
        // Reset per-cycle signals
        self.saw_low_up.reset(ctx);
        self.saw_low_dn.reset(ctx);
        self.delta_pct.reset(ctx);
        // latest_poly, spread, latest_binance do NOT reset (persist across cycles)

        match &mut self.kind {
            StrategyKind::Reversal(r) => r.reset(ctx),
            StrategyKind::HighProb(hp) => hp.reset(ctx),
        }

        self.cycle_open_binance = ctx.open_binance;
        self.last_binance = ctx.open_binance;
        self.cycle_start_ts = ctx.start_ts;
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

        None
    }

    /// Process a binance tick. Evaluates entry when Watching.
    pub fn on_binance(&mut self, tick: BinanceTick) {
        // Update binance signals (always)
        self.delta_pct.on_binance(tick);
        self.latest_binance.on_binance(tick);
        self.last_binance = tick.price;

        self.try_enter(tick.ts);
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
        }
        self.state = TradeState::Holding(HoldingData {
            side: intent.side,
            entry_type: intent.entry_type,
            token_price,
            entry_ts: now,
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

    fn emit(&mut self, h: HoldingData, outcome: Outcome, exit_price: f64, pnl: f64) -> Option<TradeRecord> {
        let rec = TradeRecord {
            slug: self.cycle_slug.clone(),
            cycle_start: self.cycle_start_ts,
            strategy: self.strategy_name,
            side: h.side,
            entry_ts: h.entry_ts,
            token_price: h.token_price,
            exit_price,
            outcome,
            pnl,
            exit_attempts: 0,
            exit_last_error: None,
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
            price_high_rev: 0.90,
            delta_pct_rev: 0.0008,
            sl_reversal: 0.0,
            unwind_pnl_rev: 0.03,
            sl_pnl_rev: 0.20,
            price_low: 0.80,
            price_high: 0.93,
            delta_pct_hp: 0.0004,
            sl_high_prob: 0.49,
            unwind_pnl_hp: 0.05,
            sl_pnl_hp: 0.25,
            halt_rev: 2,
            halt_prob: 2,
            halt_reset_hour_rev: 2,
            halt_reset_hour_hp: 8,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 300.0, // large for unit tests; real value is 2.0
            trade_size_usdc: 1.0,
        }
    }

    fn ctx(start: f64) -> CycleContext {
        CycleContext { start_ts: start, end_ts: start + 300.0, open_binance: 60_000.0 }
    }

    #[test]
    fn unwind_fires_on_poly_tick() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Dip tick: dn=0.15 < threshold 0.20, ts=1180 → time_left=120 (within [10,120])
        m.on_poly(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 });

        // Drop binance → delta_pct < 0 (required for DOWN entry)
        m.on_binance(BinanceTick { ts: 1200.0, price: 59_900.0 });

        // Recovery: dn=0.70 > reversal threshold 0.60
        m.on_poly(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 });

        // Entry evaluation: time_left=50 >= no_enter=10, all gates pass
        m.on_binance(BinanceTick { ts: 1250.0, price: 59_900.0 });

        assert!(m.is_holding(), "expected Holding after entry");

        // Now trigger the unwind: dn goes above entry + 0.03
        // entry was at dn=0.70, unwind at 0.73
        let rec = m.on_poly(PolyTick { ts: 1260.0, up: 0.27, dn: 0.73 });
        assert!(rec.is_some(), "expected UNWIND record");
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Unwind);
        assert!((rec.pnl - 0.0429).abs() < 0.0001, "pnl ≈ 1.0*0.03/0.70 = 0.0429, got {}", rec.pnl);
    }

    #[test]
    fn sl_pnl_fires_before_absolute_sl() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Manually inject Holding state by triggering a reversal
        m.on_poly(PolyTick { ts: 1180.0, up: 0.80, dn: 0.15 }); // dn dips below 0.20
        m.on_binance(BinanceTick { ts: 1200.0, price: 59_900.0 }); // dp < 0
        m.on_poly(PolyTick { ts: 1200.0, up: 0.25, dn: 0.75 }); // dn high
        m.on_binance(BinanceTick { ts: 1240.0, price: 59_900.0 }); // triggers entry

        assert!(m.is_holding());

        // Fire PnL SL: dn drops to entry - 0.20 = 0.75 - 0.20 = 0.55
        let rec = m.on_poly(PolyTick { ts: 1260.0, up: 0.45, dn: 0.55 });
        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::StopLoss);
        // pnl = -trade_size * sl_pnl / token_price = -1.0 * 0.20 / 0.75
        assert!((rec.pnl - (-1.0 * 0.20 / 0.75)).abs() < 0.0001,
            "pnl={}, expected {}", rec.pnl, -0.20/0.75);
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

        m.on_poly(PolyTick { ts: 1180.0, up: 0.85, dn: 0.15 }); // dip latches saw_low_dn
        m.on_binance(BinanceTick { ts: 1200.0, price: 59_900.0 }); // dp < 0, cached

        // No further BinanceTick — the poly recovery tick alone must fire the entry.
        m.on_poly(PolyTick { ts: 1240.0, up: 0.30, dn: 0.70 });
        assert!(m.is_holding(), "expected Holding after poly-triggered entry");
    }

    /// A cached delta_pct must only be trusted within the same cycle it was set
    /// in — reset() clears `price`, so a value left over from a previous cycle
    /// can't masquerade as "ready" this cycle.
    #[test]
    fn poly_tick_does_not_fire_using_stale_cross_cycle_delta() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        m.cycle_open(&ctx(1_000.0), "btc-updown-5m-1000", false);
        m.on_binance(BinanceTick { ts: 1100.0, price: 59_900.0 }); // dp < 0, this cycle

        // New cycle: delta_pct is reset even though last_binance still holds the
        // old price.
        m.cycle_open(&ctx(1_500.0), "btc-updown-5m-1500", false);
        m.on_poly(PolyTick { ts: 1680.0, up: 0.85, dn: 0.15 }); // dip latches saw_low_dn
        m.on_poly(PolyTick { ts: 1740.0, up: 0.30, dn: 0.70 }); // recovery, no BinanceTick yet this cycle
        assert!(!m.is_holding(), "must not fire on a delta_pct left over from the previous cycle");
    }

    #[test]
    fn halted_machine_skips_entry() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = ctx(1_000.0);
        m.cycle_open(&c, "btc-updown-5m-1000", true); // entry_suppressed

        m.on_poly(PolyTick { ts: 1180.0, up: 0.80, dn: 0.15 });
        m.on_binance(BinanceTick { ts: 1200.0, price: 59_900.0 });
        m.on_poly(PolyTick { ts: 1200.0, up: 0.25, dn: 0.75 });
        m.on_binance(BinanceTick { ts: 1240.0, price: 59_900.0 });

        assert!(!m.is_holding(), "halted machine must not enter");
        assert!(m.cycle_close().is_none());
    }

    #[test]
    fn win_at_cycle_close() {
        let p = btc_params();
        let mut m = Machine::new_reversal(&p);
        let c = CycleContext { start_ts: 1_000.0, end_ts: 1_300.0, open_binance: 60_000.0 };
        m.cycle_open(&c, "btc-updown-5m-1000", false);

        // Enter DOWN position
        m.on_poly(PolyTick { ts: 1180.0, up: 0.80, dn: 0.15 });
        m.on_binance(BinanceTick { ts: 1200.0, price: 59_900.0 });
        m.on_poly(PolyTick { ts: 1200.0, up: 0.25, dn: 0.75 });
        m.on_binance(BinanceTick { ts: 1240.0, price: 59_900.0 });

        assert!(m.is_holding());

        // Cycle closes with price fell (last_binance < open → DOWN wins)
        let rec = m.cycle_close();
        assert!(rec.is_some());
        let rec = rec.unwrap();
        assert_eq!(rec.outcome, Outcome::Win);
        // pnl = shares*1.0 - trade_size = 1.0/0.75 - 1.0 = 0.3333
        assert!((rec.pnl - (1.0/0.75 - 1.0)).abs() < 0.0001,
            "pnl={}", rec.pnl);
    }
}
