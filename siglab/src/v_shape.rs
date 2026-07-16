//! A minimal, self-contained "V shape" strategy simulator for crypto markets —
//! deliberately not `trader::machine::Machine` (no `gates.rs`, no `delta_pct`/Binance
//! direction requirement — pure CLOB price action, same philosophy as
//! `bucket_reversal.rs`), but unlike that module, this one *does* track real cycle
//! boundaries: crypto markets have a genuine cycle-end, and this strategy reuses the same
//! "force-unwind within 10s of cycle end" rule `trader/src/machine.rs` applies to the
//! reversal/high_prob grid (see `FORCE_UNWIND_BEFORE_CYCLE_END_SECS` below). Added
//! 2026-07-15 per explicit request.
//!
//! Entry condition ("V shape"): a side's token price must reach `>= high1`, then later
//! `<= low` (only counted once `high1` has already been seen), then later `>= high2` again
//! — at which point a position opens on that side. `up` and `dn = 1.0 - up` are tracked as
//! two independent latch chains (same reasoning as `bucket_reversal`'s `saw_low_up`/
//! `saw_low_dn` split, or `trader::signal::SawLowSignal`'s up/dn instances) — even though
//! the fixed grid's first triple uses `high1 == high2`, "up reaches 0.7 first" and "dn
//! reaches 0.7 first" are different real price extremes, not mirror-redundant. See
//! `TRIPLES` in `v_shape_grid()` for the fixed `(high1, low, high2)` combinations traded.

/// One `(high1, low, high2, sl_pnl, unwind_pnl)` V-shape parameter combination.
#[derive(Debug, Clone, Copy)]
pub struct VShapeParams {
    pub high1: f64,
    pub low: f64,
    pub high2: f64,
    pub sl_pnl: f64,
    pub unwind_pnl: f64,
}

/// Fixed max-holding-time cap, same value as the crypto reversal grid's
/// `unwind_time_rev`/`bucket_reversal::MAX_HOLD_SECS`, per explicit request.
pub const UNWIND_TIME_SECS: f64 = 25.0;
pub const TRADE_SIZE_USDC: f64 = 1.0;

/// Seconds before cycle-end at which a still-open position is force-closed regardless of
/// PnL/holding time — same value and rationale as `trader/src/machine.rs`'s constant of the
/// same name (can't import a private const from another crate, so this is a doc-commented
/// duplicate, same relationship `bucket_reversal.rs`'s own `SL_PNL`/`UNWIND_PNL` already have
/// to the crypto grid's values).
const FORCE_UNWIND_BEFORE_CYCLE_END_SECS: f64 = 10.0;

fn fmt_threshold(x: f64) -> String {
    let s = format!("{x:.2}");
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Fixed `(high1, low, high2)` threshold triples, each crossed with `sl_pnl` in {0.3, 0.6} x
/// `unwind_pnl` in {0.05, 0.1, 0.15, 0.2} = 8 variants per triple, per explicit request — not
/// a full grid over the threshold triple itself (unlike `bucket_reversal::reversal_grid`'s 18
/// combos), just these triples with sl/unwind varied. `(0.7, 0.3, 0.7)` was the original
/// triple; `(0.7, 0.3, 0.55)` (shallower `high2` re-entry bar) added 2026-07-16 per explicit
/// request, same treatment.
const TRIPLES: [(f64, f64, f64); 2] = [(0.7, 0.3, 0.7), (0.7, 0.3, 0.55)];

pub fn v_shape_grid() -> Vec<(String, VShapeParams)> {
    const SL: [f64; 2] = [0.3, 0.6];
    const UNWIND: [f64; 4] = [0.05, 0.1, 0.15, 0.2];

    let mut out = Vec::with_capacity(TRIPLES.len() * SL.len() * UNWIND.len());
    for (high1, low, high2) in TRIPLES {
        for sl in SL {
            for unwind in UNWIND {
                let id = format!(
                    "v_{}_{}_{}_{}_{}",
                    fmt_threshold(high1),
                    fmt_threshold(low),
                    fmt_threshold(high2),
                    fmt_threshold(sl),
                    fmt_threshold(unwind),
                );
                out.push((
                    id,
                    VShapeParams {
                        high1,
                        low,
                        high2,
                        sl_pnl: sl,
                        unwind_pnl: unwind,
                    },
                ));
            }
        }
    }
    out
}

/// Two-stage latch for one side (up or dn): `seen_high` latches once price >= `high1`;
/// `seen_low_after_high` latches once price <= `low`, but only counts once `seen_high` is
/// already true (reaching `low` *before* ever reaching `high1` must not satisfy the V
/// shape). Both latch permanently once true, same as `SawLowSignal`/`bucket_reversal`'s
/// single-stage latch — no un-latching within a cycle.
#[derive(Debug, Clone, Copy, Default)]
struct Latch {
    seen_high: bool,
    seen_low_after_high: bool,
}

impl Latch {
    fn advance(&mut self, v: f64, high1: f64, low: f64) {
        if v >= high1 {
            self.seen_high = true;
        }
        if self.seen_high && v <= low {
            self.seen_low_after_high = true;
        }
    }

    fn ready(&self, v: f64, high2: f64) -> bool {
        self.seen_low_after_high && v >= high2
    }
}

#[derive(Debug, Clone, Copy)]
enum State {
    Watching {
        up: Latch,
        dn: Latch,
    },
    Holding {
        side_up: bool,
        entry_price: f64,
        entry_ts: f64,
    },
}

pub struct ClosedTrade {
    pub side_up: bool,
    pub entry_ts: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub outcome: &'static str,
    pub pnl: f64,
}

pub struct VShapeEngine {
    pub variant_id: String,
    params: VShapeParams,
    state: State,
    cycle_end_ts: f64,
    pub cycle_slug: String,
}

fn fresh_watching() -> State {
    State::Watching {
        up: Latch::default(),
        dn: Latch::default(),
    }
}

impl VShapeEngine {
    pub fn new(variant_id: String, params: VShapeParams) -> Self {
        Self {
            variant_id,
            params,
            state: fresh_watching(),
            cycle_end_ts: 0.0,
            cycle_slug: String::new(),
        }
    }

    /// Reset latch state for a new cycle and record its end timestamp (for the
    /// force-unwind-near-cycle-end check) and slug (for the emitted trade record). Does
    /// *not* itself resolve a still-open position — callers must call
    /// `force_close_if_holding` first if one might be open (see `market.rs`'s wiring,
    /// mirroring how it calls `Machine::cycle_close` before `Machine::cycle_open`).
    pub fn cycle_open(&mut self, cycle_end_ts: f64, slug: &str) {
        self.state = fresh_watching();
        self.cycle_end_ts = cycle_end_ts;
        self.cycle_slug = slug.to_string();
    }

    // Only exercised by this module's own tests today (asserting entry/exit transitions) —
    // siglab is a bin-only crate, so unlike `trader::machine::Machine::is_holding` (used
    // externally via the `trader` library crate), nothing else here reaches it yet.
    #[allow(dead_code)]
    pub fn is_holding(&self) -> bool {
        matches!(self.state, State::Holding { .. })
    }

    /// `up` is the market's current Yes-token mid price (`dn = 1.0 - up` is the No side);
    /// `ts` is the tick's unix timestamp (seconds). Returns `Some` exactly when this tick
    /// closes a position (stop-loss, take-profit, force-unwind-near-cycle-end, or timeout)
    /// — entries are silent.
    pub fn on_tick(&mut self, up: f64, ts: f64) -> Option<ClosedTrade> {
        let dn = 1.0 - up;
        match &mut self.state {
            State::Watching {
                up: up_latch,
                dn: dn_latch,
            } => {
                up_latch.advance(up, self.params.high1, self.params.low);
                dn_latch.advance(dn, self.params.high1, self.params.low);

                if up_latch.ready(up, self.params.high2) {
                    self.state = State::Holding {
                        side_up: true,
                        entry_price: up,
                        entry_ts: ts,
                    };
                } else if dn_latch.ready(dn, self.params.high2) {
                    self.state = State::Holding {
                        side_up: false,
                        entry_price: dn,
                        entry_ts: ts,
                    };
                }
                None
            }
            State::Holding {
                side_up,
                entry_price,
                entry_ts,
            } => {
                let side_up = *side_up;
                let entry_price = *entry_price;
                let entry_ts = *entry_ts;
                let current = if side_up { up } else { dn };

                let (exit_price, outcome): (f64, &'static str) =
                    if current <= entry_price - self.params.sl_pnl {
                        (entry_price - self.params.sl_pnl, "STOPLOSS")
                    } else if current >= entry_price + self.params.unwind_pnl {
                        (entry_price + self.params.unwind_pnl, "UNWIND")
                    } else if self.cycle_end_ts > 0.0
                        && self.cycle_end_ts - ts <= FORCE_UNWIND_BEFORE_CYCLE_END_SECS
                    {
                        (current, "UNWIND")
                    } else if ts - entry_ts >= UNWIND_TIME_SECS {
                        (current, "TIMEOUT")
                    } else {
                        return None;
                    };

                let shares = TRADE_SIZE_USDC / entry_price;
                let pnl = round4(shares * exit_price - TRADE_SIZE_USDC);
                self.state = fresh_watching();
                Some(ClosedTrade {
                    side_up,
                    entry_ts,
                    entry_price,
                    exit_price,
                    outcome,
                    pnl,
                })
            }
        }
    }

    /// Safety net: force-close a still-open position before the engine resets for a new
    /// cycle. Should be a rare-to-never path given `on_tick`'s force-unwind-near-cycle-end
    /// check already closes within `FORCE_UNWIND_BEFORE_CYCLE_END_SECS` of cycle end on any
    /// tick — this only guards against the feed going silent for exactly that last window.
    /// `current_up` is the caller's most recently observed `up` price (see `market.rs`'s
    /// `last_up` cache). Labeled `"TIMEOUT"` since it's a forced closure not tied to a price
    /// target, reusing an outcome string `report.rs` already recognizes.
    pub fn force_close_if_holding(&mut self, current_up: f64) -> Option<ClosedTrade> {
        let (side_up, entry_price, entry_ts) = match &self.state {
            State::Holding {
                side_up,
                entry_price,
                entry_ts,
            } => (*side_up, *entry_price, *entry_ts),
            State::Watching { .. } => return None,
        };
        let current = if side_up {
            current_up
        } else {
            1.0 - current_up
        };
        let shares = TRADE_SIZE_USDC / entry_price;
        let pnl = round4(shares * current - TRADE_SIZE_USDC);
        self.state = fresh_watching();
        Some(ClosedTrade {
            side_up,
            entry_ts,
            entry_price,
            exit_price: current,
            outcome: "TIMEOUT",
            pnl,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> VShapeEngine {
        let mut e = VShapeEngine::new(
            "v".to_string(),
            VShapeParams {
                high1: 0.7,
                low: 0.3,
                high2: 0.7,
                sl_pnl: 0.3,
                unwind_pnl: 0.05,
            },
        );
        e.cycle_open(1_000_000.0, "btc-updown-5m-1"); // far-future cycle end by default
        e
    }

    #[test]
    fn v_shape_grid_has_16_unique_combos() {
        let grid = v_shape_grid();
        assert_eq!(grid.len(), 16);
        let mut ids: Vec<_> = grid.iter().map(|(id, _)| id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 16);
        assert!(grid.iter().any(|(id, _)| id == "v_0.7_0.3_0.7_0.3_0.05"));
        assert!(grid.iter().any(|(id, _)| id == "v_0.7_0.3_0.7_0.6_0.2"));
        assert!(grid.iter().any(|(id, _)| id == "v_0.7_0.3_0.55_0.3_0.05"));
        assert!(grid.iter().any(|(id, _)| id == "v_0.7_0.3_0.55_0.6_0.2"));
    }

    #[test]
    fn no_entry_without_reaching_high_first() {
        let mut e = engine();
        // dips to 0.2 (<= low) then recovers to 0.75 (>= high2), but never touched high1
        // first — must not enter.
        assert!(e.on_tick(0.2, 0.0).is_none());
        assert!(e.on_tick(0.75, 1.0).is_none());
        assert!(!e.is_holding());
    }

    #[test]
    fn full_v_sequence_enters_up() {
        let mut e = engine();
        assert!(e.on_tick(0.75, 0.0).is_none()); // high1 latched
        assert!(e.on_tick(0.25, 1.0).is_none()); // low latched (after high)
        assert!(e.on_tick(0.72, 2.0).is_none()); // high2 crossed -> enters, no exit yet
        assert!(e.is_holding());
    }

    /// Tests the `Latch` directly rather than through `VShapeEngine::on_tick` — with
    /// symmetric thresholds (high1==high2==0.7, low==0.3), driving `up` through a
    /// low-before-high sequence *also* drives `dn = 1 - up` through a complete, independent
    /// high-then-low sequence of its own (dn's high/low are just up's low/high), which would
    /// make an engine-level test of this exact invariant ambiguous about which side actually
    /// fired. The invariant itself — reaching `low` before ever reaching `high1` must not
    /// count — is a property of one `Latch`, so test it in isolation.
    #[test]
    fn latch_requires_high_before_low_counts() {
        let mut l = Latch::default();
        l.advance(0.25, 0.7, 0.3); // low seen first, no high yet
        assert!(!l.seen_high);
        assert!(!l.seen_low_after_high);
        assert!(!l.ready(0.75, 0.7));

        l.advance(0.75, 0.7, 0.3); // high seen now
        assert!(l.seen_high);
        assert!(!l.seen_low_after_high);
        assert!(!l.ready(0.75, 0.7)); // recovering again must not fire without a post-high dip

        l.advance(0.25, 0.7, 0.3); // genuine post-high dip
        assert!(l.seen_low_after_high);
        assert!(l.ready(0.75, 0.7));
    }

    #[test]
    fn take_profit_closes_at_exact_threshold() {
        let mut e = engine();
        e.on_tick(0.75, 0.0);
        e.on_tick(0.25, 1.0);
        e.on_tick(0.70, 2.0); // entry at up=0.70
        let closed = e.on_tick(0.75, 3.0).unwrap(); // 0.70 + 0.05 = 0.75 take-profit
        assert_eq!(closed.outcome, "UNWIND");
        assert!((closed.exit_price - 0.75).abs() < 1e-9);
        // shares = 1.0/0.70; pnl = shares*0.75 - 1.0
        assert!((closed.pnl - (0.75 / 0.70 - 1.0)).abs() < 1e-4);
        assert!(!e.is_holding());
    }

    #[test]
    fn stop_loss_closes_at_exact_threshold() {
        let mut e = engine();
        e.on_tick(0.75, 0.0);
        e.on_tick(0.25, 1.0);
        e.on_tick(0.70, 2.0); // entry at up=0.70
        // 0.35 clears the 0.70-0.30=0.40 floor with margin — landing exactly on a computed
        // f64 boundary is a known gotcha in this repo (e.g. `0.70 - 0.20 != 0.50` exactly).
        let closed = e.on_tick(0.35, 3.0).unwrap();
        assert_eq!(closed.outcome, "STOPLOSS");
        assert!((closed.exit_price - (0.70 - 0.3)).abs() < 1e-9);
        assert!(closed.pnl < 0.0);
    }

    #[test]
    fn timeout_closes_after_25_seconds() {
        let mut e = engine();
        e.on_tick(0.75, 100.0);
        e.on_tick(0.25, 101.0);
        e.on_tick(0.70, 102.0); // entry at ts=102
        assert!(e.on_tick(0.72, 126.0).is_none()); // 24s later, no timeout yet
        let closed = e.on_tick(0.72, 127.0).unwrap(); // 25s later
        assert_eq!(closed.outcome, "TIMEOUT");
        assert!((closed.exit_price - 0.72).abs() < 1e-9);
    }

    #[test]
    fn force_unwinds_within_10s_of_cycle_end() {
        let mut e = VShapeEngine::new(
            "v".to_string(),
            VShapeParams {
                high1: 0.7,
                low: 0.3,
                high2: 0.7,
                sl_pnl: 0.3,
                unwind_pnl: 0.05,
            },
        );
        e.cycle_open(300.0, "btc-updown-5m-1"); // cycle ends at ts=300
        e.on_tick(0.75, 100.0);
        e.on_tick(0.25, 101.0);
        e.on_tick(0.70, 102.0); // entry at up=0.70

        // ts=291 is within 10s of cycle end (300) — must force-unwind even though neither
        // SL nor TP was reached and 25s hasn't elapsed since entry (102 -> 291 = 189s
        // actually has elapsed past 25s too, so use a later entry closer to cycle end).
        let closed = e.on_tick(0.71, 291.0).unwrap();
        assert_eq!(closed.outcome, "UNWIND");
        assert!((closed.exit_price - 0.71).abs() < 1e-9);
        assert!(!e.is_holding());
    }

    #[test]
    fn force_close_if_holding_closes_open_position_as_timeout() {
        let mut e = engine();
        e.on_tick(0.75, 0.0);
        e.on_tick(0.25, 1.0);
        e.on_tick(0.70, 2.0); // entry at up=0.70
        assert!(e.is_holding());

        let closed = e.force_close_if_holding(0.68).unwrap();
        assert_eq!(closed.outcome, "TIMEOUT");
        assert!((closed.exit_price - 0.68).abs() < 1e-9);
        assert!(!e.is_holding());
    }

    #[test]
    fn force_close_if_holding_is_none_when_watching() {
        let mut e = engine();
        assert!(e.force_close_if_holding(0.5).is_none());
    }

    #[test]
    fn resets_and_can_fire_again_after_closing() {
        let mut e = engine();
        e.on_tick(0.75, 0.0);
        e.on_tick(0.25, 1.0);
        e.on_tick(0.70, 2.0);
        let first = e.on_tick(0.75, 3.0).unwrap();
        assert_eq!(first.outcome, "UNWIND");

        // No stale latch carried over — needs a fresh full sequence before it can fire
        // again.
        assert!(e.on_tick(0.70, 4.0).is_none());
        e.on_tick(0.80, 5.0);
        e.on_tick(0.20, 6.0);
        assert!(e.on_tick(0.71, 7.0).is_none()); // enters again — entries return None
        let second = e.on_tick(0.76, 8.0).unwrap();
        assert!(second.side_up);
        assert!((second.entry_price - 0.71).abs() < 1e-9);
    }

    #[test]
    fn down_side_is_symmetric() {
        let mut e = engine();
        // dn reaches high1 (0.7) first: dn=0.75 means up=0.25.
        assert!(e.on_tick(0.25, 0.0).is_none());
        // dn dips to low (0.3): dn=0.25 means up=0.75.
        assert!(e.on_tick(0.75, 1.0).is_none());
        // dn recovers to high2 (0.7): dn=0.72 means up=0.28 -> enters DOWN at dn=0.72.
        assert!(e.on_tick(0.28, 2.0).is_none());
        assert!(e.is_holding());

        // Take-profit: dn >= entry(0.72) + 0.05 = 0.77, i.e. up <= 0.23.
        let closed = e.on_tick(0.23, 3.0).unwrap();
        assert!(!closed.side_up);
        assert_eq!(closed.outcome, "UNWIND");
        assert!((closed.entry_price - 0.72).abs() < 1e-9);
        assert!((closed.exit_price - 0.77).abs() < 1e-9);
    }

    #[test]
    fn cycle_open_resets_watching_latches_without_resolving_a_holding_position() {
        // Documents the contract: cycle_open alone does NOT force-close a held position —
        // callers must call force_close_if_holding first (see market.rs's wiring). This
        // guards against a future refactor silently dropping that safety-net call.
        let mut e = engine();
        e.on_tick(0.75, 0.0);
        e.on_tick(0.25, 1.0);
        e.on_tick(0.70, 2.0);
        assert!(e.is_holding());

        e.cycle_open(2_000_000.0, "btc-updown-5m-2");
        assert!(
            !e.is_holding(),
            "cycle_open itself resets to Watching, silently dropping any open position — \
             this is why market.rs must call force_close_if_holding beforehand"
        );
    }
}
