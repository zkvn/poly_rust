//! Shared tick types, cycle context, trade intent, and trade result.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BinanceTick {
    pub ts: f64,
    pub price: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PolyTick {
    pub ts: f64,
    pub up: f64,
    pub dn: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct CycleContext {
    pub start_ts: f64,
    pub end_ts: f64,
    pub open_binance: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Up,
    Down,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Up => "UP",
            Side::Down => "DOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    Reversal,
    HighProb,
}

impl EntryType {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryType::Reversal => "reversal",
            EntryType::HighProb => "high_prob",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TradeIntent {
    pub side: Side,
    pub entry_type: EntryType,
    pub up: f64,
    pub dn: f64,
    pub binance_price: f64,
}

impl TradeIntent {
    pub fn token_price(self) -> f64 {
        match self.side {
            Side::Up => self.up,
            Side::Down => self.dn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Win,
    Loss,
    StopLoss,
    Unwind,
    /// Max-holding-time cap (`unwind_time_rev`/`unwind_time_hp`) force-closed the
    /// position at market — may land at a profit or a loss, unlike StopLoss/Unwind
    /// which are directionally fixed. See `trader/doc/plan_unwind_time_2026-07-08.md`.
    Timeout,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Win => "WIN",
            Outcome::Loss => "LOSS",
            Outcome::StopLoss => "STOPLOSS",
            Outcome::Unwind => "UNWIND",
            Outcome::Timeout => "TIMEOUT",
        }
    }

    /// `Timeout` is deliberately excluded (matches the backtest's "cum_losses
    /// NOT incremented" TIMEOUT comment) — a max-holding-time exit isn't a
    /// signal quality failure the way a real stop-loss/loss is, so it
    /// shouldn't feed the halt loss-streak either way.
    pub fn is_loss_for_halt(self) -> bool {
        matches!(self, Outcome::Loss | Outcome::StopLoss)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeRecord {
    pub slug: String,
    pub cycle_start: f64,
    pub strategy: &'static str,
    pub side: Side,
    pub entry_ts: f64,
    pub token_price: f64,
    pub exit_price: f64,
    pub outcome: Outcome,
    pub pnl: f64,
    /// Count of failed exit-order attempts (unwind and/or stop-loss) seen
    /// before this outcome was logged — distinguishes a clean hold-to-
    /// resolution WIN/LOSS from one where an early exit was tried and failed.
    pub exit_attempts: u32,
    /// Most recent failed exit attempt's error message, if any.
    pub exit_last_error: Option<String>,
    /// Entry BUY latency (ms): time from the triggering tick's own timestamp
    /// to the driver receiving/starting to process it.
    #[serde(default)]
    pub entry_signal_latency_ms: f64,
    /// Entry BUY latency (ms): time from the driver starting to process the
    /// order to the fill confirmation coming back from the CLOB.
    #[serde(default)]
    pub entry_process_latency_ms: f64,
    /// Exit order latency (ms), signal leg — 0.0 when the position resolved
    /// by natural market close rather than an early exit order.
    #[serde(default)]
    pub exit_signal_latency_ms: f64,
    /// Exit order latency (ms), process leg — 0.0 when there was no early exit order.
    #[serde(default)]
    pub exit_process_latency_ms: f64,
}
