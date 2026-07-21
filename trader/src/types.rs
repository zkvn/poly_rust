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
    /// Real best bid/ask for the UP token (plan_unwind_5u_maker_2026-07-19
    /// §2.2's mid-vs-bid TODO) — `0.0` (the `#[serde(default)]` value when
    /// the publisher's payload doesn't carry them, e.g. an old `price_feed`
    /// mid-rollout, or backtest replay data with no recorded spread) means
    /// "not available this tick, treat as unknown," never a real quote —
    /// consumers must fall back to `up`/`dn` (the mid) when `<= 0.0`. DOWN's
    /// own best bid/ask are never observed independently (`marketdata.rs`
    /// only ever subscribes to the UP token) but are exactly derivable from
    /// these via the unified mint/merge book's complementary-token identity
    /// (`docs/plan_market_maker_mvp_2026-07-19.md` §1): DOWN's bid = `1 -
    /// up_ask`, DOWN's ask = `1 - up_bid`.
    #[serde(default)]
    pub up_bid: f64,
    #[serde(default)]
    pub up_ask: f64,
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
    /// V-shape dip-recover-dip pattern (high1 → low → high2), promoted from
    /// siglab's standalone `v_shape.rs` engine 2026-07-17 — see
    /// `trader/doc/plan_v_shape_trader_2026-07-17.md`.
    VShape,
}

impl EntryType {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryType::Reversal => "reversal",
            EntryType::HighProb => "high_prob",
            EntryType::VShape => "v_shape",
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

    /// `Loss`/`StopLoss` always count. `Unwind` never counts — a take-profit
    /// exit is directionally fixed to a gain by construction (the 2026-07-06
    /// SOL incident's price-floor fix keeps a real fill from landing below
    /// `tp_price`). `Timeout` is the one outcome that isn't fixed either way
    /// (a pure elapsed-time cap — see its own doc comment) — it counts only
    /// when `pnl` actually landed negative. Previously `Timeout` was excluded
    /// unconditionally, which let a run of losing ETH TIMEOUT exits overnight
    /// go uncaught by the loss-streak halt; see
    /// `trader/doc/incident_eth_timeout_halt_gap_2026-07-14.md`.
    pub fn is_loss_for_halt(self, pnl: f64) -> bool {
        match self {
            Outcome::Loss | Outcome::StopLoss => true,
            Outcome::Timeout => pnl < 0.0,
            Outcome::Unwind | Outcome::Win => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeRecord {
    pub slug: String,
    pub cycle_start: f64,
    pub strategy: &'static str,
    pub side: Side,
    pub entry_ts: f64,
    /// Timestamp of the actual poly-price observation (`LatestPolySignal::ts`) that
    /// satisfied entry — distinct from `entry_ts`, which is the *triggering* tick's own
    /// timestamp (poly or binance, whichever caused the `try_enter` check to run). Entry
    /// evaluation fires on both feeds; when a binance tick triggers it using an
    /// already-cached poly price, `entry_ts` reflects that binance tick, not when the poly
    /// price was actually seen. Since one asset's binance feed broadcasts identically to
    /// every duration task trading it, this made economically distinct markets (e.g.
    /// BTC-5m and BTC-15m) log identical `entry_ts` values purely because the same shared
    /// binance tick happened to trigger both — see
    /// `siglab/doc/incident_reversal_variant_correlated_timestamps_2026-07-14.md`.
    /// 0.0 for records predating this field (added 2026-07-14) or where the observation
    /// timestamp genuinely wasn't tracked (some `worker.rs` paths — see that field's own
    /// call sites).
    #[serde(default)]
    pub entry_price_ts: f64,
    pub token_price: f64,
    /// The decision-time signal price (the mid) at entry — see
    /// `worker::HoldingData::entry_signal_price`'s doc comment for the full
    /// rationale. `token_price - entry_signal_price` is the entry's
    /// signal-to-execution slippage. `#[serde(default)]` for records
    /// predating this field (added 2026-07-21,
    /// plan_aggressive_taker_entry_2026-07-21.md §2.4).
    #[serde(default)]
    pub entry_signal_price: f64,
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
    /// Entry BUY latency (ms): time from the triggering tick's own timestamp
    /// (same origin as `entry_signal_latency_ms`) to the fill confirmation
    /// coming back from the CLOB — the full "trigger signal received locally
    /// to order confirmed locally" round trip, not just the dispatch-to-
    /// confirm leg (redefined 2026-07-08; see README.md's "Latency &
    /// observability infrastructure" section).
    #[serde(default)]
    pub entry_process_latency_ms: f64,
    /// Exit order latency (ms), signal leg — 0.0 when the position resolved
    /// by natural market close rather than an early exit order.
    #[serde(default)]
    pub exit_signal_latency_ms: f64,
    /// Exit order latency (ms): same "trigger signal received locally to
    /// order confirmed locally" definition as `entry_process_latency_ms`
    /// above — 0.0 when there was no early exit order.
    #[serde(default)]
    pub exit_process_latency_ms: f64,
}
