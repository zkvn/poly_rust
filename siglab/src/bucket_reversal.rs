//! A minimal, self-contained reversal-strategy simulator for weather/World Cup buckets —
//! deliberately **not** `trader::machine::Machine`, and touches nothing in `trader/`. See
//! `doc/plan_weather_worldcup_trading_2026-07-13.md` for the full design rationale; in short:
//!
//! - **No reference feed / `delta_pct`.** Crypto's `ReversalStrategy` requires a strict
//!   `dp > 0.0`/`dp < 0.0` to fire, sourced from a Binance reference price these markets
//!   don't have an equivalent of. This engine's entry condition is pure CLOB price action —
//!   dip below a threshold, then recover above a higher one — with no directional
//!   confirmation from anything else.
//! - **No cycle, no resolution, no Gamma.** There is no `CycleContext`, no "cycle closing,"
//!   and no concept of the market's real (Yes/No) outcome anywhere in this file. Every
//!   position is closed purely by observed price action or elapsed time — stop-loss,
//!   take-profit, or a fixed 25-second max hold — so it's always resolved (one way or
//!   another) long before the real-world outcome would even be knowable. This is
//!   deliberate: it sidesteps the "these buckets are mutually exclusive, so how do we know
//!   the true Yes/No outcome" problem entirely, rather than solving it.
//! - **No claim that this pattern has any edge on these markets.** `studies/weather/
//!   weather_poly_2026-07-12.md`'s own research found the one documented real edge in
//!   weather markets is forecast-latency arbitrage, not price reversals. This exists to
//!   measure what a reversal-scalping heuristic's PnL looks like on real (thin, slow-moving)
//!   order books, starting from no expectation that it works.

/// One `(low, high)` reversal parameter combination — same shape and same 18 values as
/// `config/markets.toml`'s crypto reversal grid, generated here instead of duplicated into
/// another TOML file since every value is currently fixed (no per-asset variation).
#[derive(Debug, Clone, Copy)]
pub struct ReversalParams {
    pub low: f64,
    pub high: f64,
}

/// Fixed exit parameters, same across every variant (per explicit request — previously
/// crypto's grid varied per asset; this and the crypto grid are now both uniform).
pub const SL_PNL: f64 = 0.3;
pub const UNWIND_PNL: f64 = 0.15;
pub const MAX_HOLD_SECS: f64 = 25.0; // lowered from 30.0 2026-07-14, matches crypto grid
pub const TRADE_SIZE_USDC: f64 = 1.0;

fn fmt_threshold(x: f64) -> String {
    let s = format!("{x:.2}");
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_string()
}

/// The same 3 (low) x 6 (high) = 18 combinations as `config/markets.toml`'s crypto reversal
/// variants, with matching `reversal_{low}_{high}` naming.
pub fn reversal_grid() -> Vec<(String, ReversalParams)> {
    const LOWS: [f64; 3] = [0.2, 0.3, 0.4];
    const HIGHS: [f64; 6] = [0.55, 0.6, 0.65, 0.7, 0.75, 0.8];
    let mut out = Vec::with_capacity(LOWS.len() * HIGHS.len());
    for low in LOWS {
        for high in HIGHS {
            let id = format!("reversal_{}_{}", fmt_threshold(low), fmt_threshold(high));
            out.push((id, ReversalParams { low, high }));
        }
    }
    out
}

#[derive(Debug, Clone, Copy)]
enum State {
    /// Latches independently and permanently once true (no time window, no un-latching —
    /// matches the crypto grid's "any time during monitoring" behavior) until a position
    /// opens and later closes, which resets both back to `false`.
    Watching { saw_low_up: bool, saw_low_dn: bool },
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

pub struct BucketReversalEngine {
    pub variant_id: String,
    params: ReversalParams,
    state: State,
}

impl BucketReversalEngine {
    pub fn new(variant_id: String, params: ReversalParams) -> Self {
        Self {
            variant_id,
            params,
            state: State::Watching {
                saw_low_up: false,
                saw_low_dn: false,
            },
        }
    }

    /// `up` is the bucket's current Yes-token mid price (`dn = 1.0 - up` is the No side);
    /// `ts` is the tick's unix timestamp (seconds). Returns `Some` exactly when this tick
    /// closes a position (stop-loss, take-profit, or timeout) — entries are silent.
    pub fn on_tick(&mut self, up: f64, ts: f64) -> Option<ClosedTrade> {
        let dn = 1.0 - up;
        match &mut self.state {
            State::Watching {
                saw_low_up,
                saw_low_dn,
            } => {
                if up < self.params.low {
                    *saw_low_up = true;
                }
                if dn < self.params.low {
                    *saw_low_dn = true;
                }
                if *saw_low_up && up > self.params.high {
                    self.state = State::Holding {
                        side_up: true,
                        entry_price: up,
                        entry_ts: ts,
                    };
                } else if *saw_low_dn && dn > self.params.high {
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
                let elapsed = ts - entry_ts;

                let (exit_price, outcome): (f64, &'static str) = if current <= entry_price - SL_PNL
                {
                    (entry_price - SL_PNL, "STOPLOSS")
                } else if current >= entry_price + UNWIND_PNL {
                    (entry_price + UNWIND_PNL, "UNWIND")
                } else if elapsed >= MAX_HOLD_SECS {
                    (current, "TIMEOUT")
                } else {
                    return None;
                };

                let shares = TRADE_SIZE_USDC / entry_price;
                let pnl = round4(shares * exit_price - TRADE_SIZE_USDC);
                self.state = State::Watching {
                    saw_low_up: false,
                    saw_low_dn: false,
                };
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
}

fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reversal_grid_has_18_unique_combos() {
        let grid = reversal_grid();
        assert_eq!(grid.len(), 18);
        let mut ids: Vec<_> = grid.iter().map(|(id, _)| id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 18);
        assert!(grid.iter().any(|(id, _)| id == "reversal_0.2_0.55"));
        assert!(grid.iter().any(|(id, _)| id == "reversal_0.4_0.8"));
    }

    #[test]
    fn no_entry_without_a_dip_first() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        // price goes straight to 0.6 without ever dipping below 0.2 first.
        assert!(e.on_tick(0.6, 0.0).is_none());
        assert!(e.on_tick(0.65, 1.0).is_none());
    }

    #[test]
    fn dip_then_recover_enters_up_no_delta_needed() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        assert!(e.on_tick(0.15, 0.0).is_none()); // dip below 0.2 latches saw_low_up
        assert!(e.on_tick(0.6, 100.0).is_none()); // recovers above 0.55 -> enters, no exit yet
        // now holding at entry_price=0.6; a small move that doesn't cross SL/TP/timeout
        // produces no trade yet.
        assert!(e.on_tick(0.62, 101.0).is_none());
    }

    #[test]
    fn take_profit_closes_at_exact_threshold() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        e.on_tick(0.15, 0.0);
        e.on_tick(0.6, 100.0); // entry at 0.6
        let closed = e.on_tick(0.75, 101.0).unwrap(); // 0.6 + 0.15 = 0.75 take-profit
        assert_eq!(closed.outcome, "UNWIND");
        assert!((closed.exit_price - 0.75).abs() < 1e-9);
        // shares = 1.0/0.6; pnl = shares*0.75 - 1.0 = 0.25
        assert!((closed.pnl - 0.25).abs() < 1e-6);
    }

    #[test]
    fn stop_loss_closes_at_exact_threshold() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        e.on_tick(0.15, 0.0);
        e.on_tick(0.6, 100.0); // entry at 0.6
        let closed = e.on_tick(0.3, 101.0).unwrap(); // 0.6 - 0.3 = 0.3 stop-loss
        assert_eq!(closed.outcome, "STOPLOSS");
        assert!((closed.exit_price - 0.3).abs() < 1e-9);
        assert!(closed.pnl < 0.0);
    }

    #[test]
    fn timeout_closes_after_25_seconds_at_current_price() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        e.on_tick(0.15, 0.0);
        e.on_tick(0.6, 100.0); // entry at ts=100, price=0.6
        // 24s later, no SL/TP crossed, no timeout yet.
        assert!(e.on_tick(0.65, 124.0).is_none());
        // 25s later — timeout fires at whatever the current price is.
        let closed = e.on_tick(0.65, 125.0).unwrap();
        assert_eq!(closed.outcome, "TIMEOUT");
        assert!((closed.exit_price - 0.65).abs() < 1e-9);
    }

    #[test]
    fn resets_and_can_fire_again_after_closing() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        e.on_tick(0.15, 0.0);
        e.on_tick(0.6, 100.0);
        let first = e.on_tick(0.75, 101.0).unwrap();
        assert_eq!(first.outcome, "UNWIND");

        // No stale latch carried over — needs a fresh dip before it can fire again.
        assert!(e.on_tick(0.6, 200.0).is_none());
        e.on_tick(0.1, 201.0);
        assert!(e.on_tick(0.56, 202.0).is_none()); // enters again — entries return None
        let second = e.on_tick(0.80, 203.0).unwrap(); // comfortably past 0.56+0.15 take-profit
        assert!(second.side_up);
        assert!((second.entry_price - 0.56).abs() < 1e-9);
    }

    #[test]
    fn down_side_is_symmetric() {
        let mut e = BucketReversalEngine::new(
            "v".to_string(),
            ReversalParams {
                low: 0.2,
                high: 0.55,
            },
        );
        // dn dips below 0.2 means up > 0.8 first...
        e.on_tick(0.85, 0.0); // dn = 0.15 < 0.2, latches saw_low_dn
        // ...then dn recovers above 0.55, i.e. up drops below 0.45 -> enters DOWN at dn=0.6.
        assert!(e.on_tick(0.4, 100.0).is_none());
        // Take-profit: dn >= entry(0.6) + 0.15 = 0.75, i.e. up <= 0.25.
        let closed = e.on_tick(0.25, 101.0).unwrap();
        assert!(!closed.side_up);
        assert_eq!(closed.outcome, "UNWIND");
        assert!((closed.entry_price - 0.6).abs() < 1e-9);
        assert!((closed.exit_price - 0.75).abs() < 1e-9);
    }
}
