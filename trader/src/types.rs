/// Shared tick types, cycle context, trade intent, and trade result.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct BinanceTick {
    pub ts: f64,
    pub price: f64,
}

#[derive(Debug, Clone, Copy)]
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
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Win => "WIN",
            Outcome::Loss => "LOSS",
            Outcome::StopLoss => "STOPLOSS",
            Outcome::Unwind => "UNWIND",
        }
    }

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
}
