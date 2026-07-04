// Trade-API module — ExecutionEngine trait + a deterministic sim impl (used by
// the backtest/tests) and a live CLOB impl (ports bot/trading.py TradingEngine).
//
// Strategy code never talks to the SDK directly; it only produces intents that
// resolve to a token_id + price + size, which this module turns into orders.

use std::str::FromStr as _;
use std::time::Duration;

use alloy::signers::Signer;
use async_trait::async_trait;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::{Amount, OrderType, Side as SdkSide, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};

// ── Result types (mirror bot/models.py TradeResult + trading.py return shapes) ─

#[derive(Debug, Clone, PartialEq)]
pub enum SellStatus {
    Live,
    Matched,
    Failed,
    DryRun,
}

#[derive(Debug, Clone)]
pub struct TradeResult {
    pub placed: bool,
    pub filled_shares: f64,
    /// Actual cost per share (fill price), not the limit price.
    pub cost: f64,
    pub error: Option<String>,
    /// Number of order-placement attempts made (1 = no retry needed).
    pub attempts: u32,
}

#[derive(Debug, Clone)]
pub struct CloseResult {
    pub filled_usdc: f64,
    pub status: SellStatus,
    pub shares_sold: f64,
    /// Last retry's error message when `status == Failed`; `None` on success.
    pub error: Option<String>,
}

/// Result of attempting a resting GTC limit sell (unwind take-profit).
#[derive(Debug, Clone)]
pub struct LimitSellResult {
    pub order_id: Option<String>,
    pub status: SellStatus,
    /// Last retry's error message when `status == Failed`; `None` on success.
    pub error: Option<String>,
}

// ── ExecutionEngine trait ─────────────────────────────────────────────────────

/// The trade-API boundary. Strategy/machine code holds a `Box<dyn ExecutionEngine>`
/// or `Arc<dyn ExecutionEngine>` — the sim impl drives backtest + tests, the live
/// impl drives production. Neither the machine nor its tests import the SDK.
#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    /// Market BUY (FAK). `price` is the intent's midpoint; `max_buy_price` caps
    /// the limit the order is allowed to cross at.
    async fn place(&self, token_id: U256, price: f64, size_usdc: f64, max_buy_price: f64) -> TradeResult;

    /// Resting GTC limit SELL (unwind take-profit).
    async fn place_limit_sell(&self, token_id: U256, shares: f64, price: f64) -> LimitSellResult;

    /// Market SELL (FAK) to close a position at stop-loss / cycle end.
    async fn close_position(&self, token_id: U256, shares: f64) -> CloseResult;

    /// Cancel a resting GTC limit sell. Returns true on success or if already gone.
    async fn cancel_limit_sell(&self, order_id: &str) -> bool;

    /// Cancel all resting orders (safety net).
    async fn cancel_all(&self) -> bool;
}

// ── Sim implementation (backtest + tests; no network) ────────────────────────

/// Deterministic fills, no network. `fill_ratio` (0.0..=1.0) lets tests exercise
/// the partial-fill branch (plan §4 DeepSeek 1.3) without a live exchange.
#[derive(Debug, Clone)]
pub struct SimExecutionEngine {
    pub fill_ratio: f64,
}

impl SimExecutionEngine {
    pub fn new() -> Self {
        Self { fill_ratio: 1.0 }
    }

    pub fn with_fill_ratio(fill_ratio: f64) -> Self {
        Self { fill_ratio: fill_ratio.clamp(0.0, 1.0) }
    }
}

impl Default for SimExecutionEngine {
    fn default() -> Self {
        Self::new()
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Truncates down to 2 decimals for constructing a SELL order size from a real
/// share balance. Must never round *up* — `Amount::shares` rejects anything with
/// more than 2 decimal places, so the caller has to pre-quantize, and rounding
/// to nearest can push the requested size above the true balance (e.g.
/// `round2(1.5151) == 1.52` on a 1.515150-share holding → permanent "not enough
/// balance" on every attempt, see `trader/doc/incident_doge_2026-07-03.md`).
/// This matches the reference Python client's own order-size quantization
/// (`py_clob_client_v2.order_builder.helpers.round_down`, `floor(x*10**n)/10**n`),
/// which the Rust SDK does not replicate — `Amount::shares` only validates scale,
/// it doesn't quantize.
fn floor2(x: f64) -> f64 {
    (x * 100.0).floor() / 100.0
}

/// Aggressive BUY entry price for a given attempt: the first attempt (attempt 0)
/// splits the difference between the signal `price` and `max_buy_price` — half the
/// spread — rather than a small fixed slippage, to bias toward actually filling.
/// Every retry after that skips straight to `max_buy_price` instead of
/// incrementally stepping toward it, since a retry means the first, less
/// aggressive price already failed to fill. Supersedes the old
/// `retry_ladder_price` interpolation (2026-07-04, by request).
fn aggressive_entry_price(price: f64, max_buy_price: f64, attempt: u32) -> f64 {
    if attempt == 0 {
        let spread = (max_buy_price - price).max(0.0);
        (price + spread / 2.0).min(max_buy_price)
    } else {
        max_buy_price
    }
}

#[async_trait]
impl ExecutionEngine for SimExecutionEngine {
    async fn place(&self, _token_id: U256, price: f64, size_usdc: f64, max_buy_price: f64) -> TradeResult {
        if price <= 0.0 {
            return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("invalid price".to_string()), attempts: 1 };
        }
        let capped_price = price.min(max_buy_price);
        let requested_shares = round2(size_usdc / capped_price);
        let filled = round2(requested_shares * self.fill_ratio);
        if filled <= 0.0 {
            return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("ORDER_FAILED".to_string()), attempts: 1 };
        }
        TradeResult { placed: true, filled_shares: filled, cost: capped_price, error: None, attempts: 1 }
    }

    async fn place_limit_sell(&self, _token_id: U256, shares: f64, price: f64) -> LimitSellResult {
        if shares <= 0.0 || price <= 0.0 {
            return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some("invalid shares/price".to_string()) };
        }
        LimitSellResult { order_id: Some(format!("sim-{shares}-{price}")), status: SellStatus::Live, error: None }
    }

    async fn close_position(&self, _token_id: U256, shares: f64) -> CloseResult {
        if shares <= 0.0 {
            return CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some("invalid shares".to_string()) };
        }
        let sold = round2(shares * self.fill_ratio);
        CloseResult { filled_usdc: 0.0, status: SellStatus::Matched, shares_sold: sold, error: None }
    }

    async fn cancel_limit_sell(&self, _order_id: &str) -> bool {
        true
    }

    async fn cancel_all(&self) -> bool {
        true
    }
}

// ── Live implementation (ports bot/trading.py TradingEngine) ─────────────────

#[derive(Debug, Clone)]
pub struct LiveConfig {
    /// Extra retries beyond the first attempt (bot/config.py order_max_retries).
    pub order_max_retries: u32,
    /// Retries for "balance: 0" (BUY not yet settled on-chain) when placing the unwind GTC sell.
    pub settle_retries: u32,
    pub settle_sleep: Duration,
    /// Retries for "no orders found" / "not enough balance" when closing at stop-loss.
    pub close_max_retries: u32,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            order_max_retries: 2,
            settle_retries: 3,
            settle_sleep: Duration::from_millis(1500),
            close_max_retries: 5,
        }
    }
}

/// Live CLOB execution engine. Generic over the signer type so callers can use
/// `LocalSigner` (private key) or any other `alloy::signers::Signer` impl.
///
/// NOT exercised against the real CLOB anywhere in this crate yet — connecting
/// requires `POLYMARKET_PRIVATE_KEY` + a funder address (the same secrets the
/// Python bot uses) and placing even a $1 test order needs the user's explicit
/// go-ahead (plan §12 Track B, B2).
pub struct LiveExecutionEngine<S: Signer + Clone + Send + Sync + 'static> {
    client: Client<Authenticated<Normal>>,
    signer: S,
    cfg: LiveConfig,
}

impl<S: Signer + Clone + Send + Sync + 'static> LiveExecutionEngine<S> {
    pub async fn connect(
        host: &str,
        signer: S,
        funder: Address,
        signature_type: SignatureType,
        cfg: LiveConfig,
    ) -> anyhow::Result<Self> {
        let client = Client::new(host, Config::default())?
            .authentication_builder(&signer)
            .funder(funder)
            .signature_type(signature_type)
            .authenticate()
            .await?;
        Ok(Self { client, signer, cfg })
    }

    /// USDC (collateral) balance for the funder wallet, for `BalanceGuard`.
    /// `None` on any error — the caller treats that as fail-open, matching
    /// `bot/trading.py`'s `_fetch_balance`.
    pub async fn fetch_balance(&self) -> Option<f64> {
        use polymarket_client_sdk_v2::clob::types::AssetType;
        use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
        let resp = self
            .client
            .balance_allowance(BalanceAllowanceRequest::builder().asset_type(AssetType::Collateral).build())
            .await
            .ok()?;
        let raw: f64 = resp.balance.to_string().parse().ok()?;
        Some(raw / 1e6)
    }
}

/// Build a `LocalSigner` from a hex private key, chained to Polygon — the
/// common case (mirrors Python's `ClobClient(..., key=poly_private_key, chain_id=137)`).
pub fn local_signer_from_key(private_key: &str) -> anyhow::Result<alloy::signers::local::LocalSigner<alloy::signers::k256::ecdsa::SigningKey>> {
    use alloy::signers::Signer as _;
    let signer = alloy::signers::local::LocalSigner::from_str(private_key)?.with_chain_id(Some(POLYGON));
    Ok(signer)
}

/// Reads `POLY_SIGNATURE_TYPE` (0=Eoa, 1=Proxy, 2=GnosisSafe, 3=Poly1271) from
/// the environment; defaults to `Proxy` (Magic Link accounts) when unset, to
/// match the account type every account before the 2026-07-02 one used.
/// Different accounts are genuinely different wallet types on Polymarket —
/// this is not a constant, it must match the account behind `POLY_PRIVATE_KEY`.
pub fn signature_type_from_env() -> anyhow::Result<SignatureType> {
    let raw = match std::env::var("POLY_SIGNATURE_TYPE") {
        Ok(v) => v,
        Err(_) => return Ok(SignatureType::Proxy),
    };
    match raw.trim() {
        "0" => Ok(SignatureType::Eoa),
        "1" => Ok(SignatureType::Proxy),
        "2" => Ok(SignatureType::GnosisSafe),
        "3" => Ok(SignatureType::Poly1271),
        other => anyhow::bail!("POLY_SIGNATURE_TYPE must be 0-3, got {other:?}"),
    }
}

#[async_trait]
impl<S: Signer + Clone + Send + Sync + 'static> ExecutionEngine for LiveExecutionEngine<S> {
    async fn place(&self, token_id: U256, price: f64, size_usdc: f64, max_buy_price: f64) -> TradeResult {
        if price <= 0.0 {
            return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("invalid price".to_string()), attempts: 0 };
        }
        let estimated_shares = round2(size_usdc / aggressive_entry_price(price, max_buy_price, 0));

        let max_attempts = 1 + self.cfg.order_max_retries;
        let mut last_err: Option<String> = None;

        for attempt in 0..max_attempts {
            let capped_price = aggressive_entry_price(price, max_buy_price, attempt);
            let Ok(price_dec) = Decimal::from_str(&format!("{capped_price:.4}")) else {
                return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("bad price".to_string()), attempts: attempt + 1 };
            };
            let Ok(amount) = Decimal::from_str(&format!("{size_usdc:.4}")).and_then(|d| Ok(Amount::usdc(d))) else {
                return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("bad amount".to_string()), attempts: attempt + 1 };
            };
            let Ok(amount) = amount else {
                return TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: Some("bad amount".to_string()), attempts: attempt + 1 };
            };

            let result = self
                .client
                .market_order()
                .token_id(token_id)
                .side(SdkSide::Buy)
                .amount(amount)
                .price(price_dec)
                .order_type(OrderType::FAK)
                .build_sign_and_post(&self.signer)
                .await;

            match result {
                Ok(resp) if resp.success => {
                    let filled: f64 = resp.taking_amount.to_string().parse().unwrap_or(estimated_shares);
                    let actual_cost = if filled > 0.0 { size_usdc / filled } else { price };
                    return TradeResult { placed: true, filled_shares: filled, cost: actual_cost, error: None, attempts: attempt + 1 };
                }
                Ok(_) => {
                    last_err = Some("order not successful".to_string());
                    eprintln!("[ORDER-RETRY] token={token_id} BUY attempt {}/{max_attempts} price={capped_price:.4} -> order not successful",
                        attempt + 1);
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    eprintln!("[ORDER-RETRY] token={token_id} BUY attempt {}/{max_attempts} price={capped_price:.4} -> {e}",
                        attempt + 1);
                }
            }
            if attempt < max_attempts - 1 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        TradeResult { placed: false, filled_shares: 0.0, cost: 0.0, error: last_err.or(Some("ORDER_FAILED".to_string())), attempts: max_attempts }
    }

    async fn place_limit_sell(&self, token_id: U256, shares: f64, price: f64) -> LimitSellResult {
        if shares <= 0.0 || price <= 0.0 {
            return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some("invalid shares/price".to_string()) };
        }
        // Snap to 0.01 tick, clamp to [0.01, 0.99] (matches Python).
        let tick = 0.01;
        let snapped = ((price / tick).round() * tick).clamp(tick, 1.0 - tick);
        let shares = floor2(shares);

        let Ok(price_dec) = Decimal::from_str(&format!("{snapped:.2}")) else {
            return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some("bad price".to_string()) };
        };
        let Ok(size_dec) = Decimal::from_str(&format!("{shares:.2}")) else {
            return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some("bad size".to_string()) };
        };

        let mut last_err: Option<String> = None;
        for attempt in 0..=self.cfg.settle_retries {
            let result = self
                .client
                .limit_order()
                .token_id(token_id)
                .side(SdkSide::Sell)
                .price(price_dec)
                .size(size_dec)
                .order_type(OrderType::GTC)
                .build_sign_and_post(&self.signer)
                .await;

            match result {
                Ok(resp) => {
                    let taking: f64 = resp.taking_amount.to_string().parse().unwrap_or(0.0);
                    if taking > 0.0 {
                        return LimitSellResult { order_id: None, status: SellStatus::Matched, error: None };
                    }
                    if !resp.order_id.is_empty() {
                        return LimitSellResult { order_id: Some(resp.order_id), status: SellStatus::Live, error: None };
                    }
                    return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some("empty order_id, no fill".to_string()) };
                }
                Err(e) => {
                    let msg = e.to_string();
                    // "balance: 0" -> FAK BUY hasn't settled on-chain yet; retry.
                    if msg.contains("balance: 0") && attempt < self.cfg.settle_retries {
                        eprintln!("[ORDER-RETRY] token={token_id} SELL(unwind) attempt {}/{} -> {msg}",
                            attempt + 1, self.cfg.settle_retries + 1);
                        last_err = Some(msg);
                        tokio::time::sleep(self.cfg.settle_sleep).await;
                        continue;
                    }
                    return LimitSellResult { order_id: None, status: SellStatus::Failed, error: Some(msg) };
                }
            }
        }
        LimitSellResult { order_id: None, status: SellStatus::Failed, error: last_err.or(Some("SETTLE_RETRIES_EXHAUSTED".to_string())) }
    }

    async fn close_position(&self, token_id: U256, shares: f64) -> CloseResult {
        if shares <= 0.0 {
            return CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some("invalid shares".to_string()) };
        }
        // Amount::shares (not ::usdc) — this is a SELL of a held share count, not a USDC-
        // denominated buy. Wrapping the share count as Amount::usdc told the exchange we
        // wanted ~$shares worth of proceeds, which at less-than-$1 prices needs MORE shares
        // than we actually hold, so the order could never match ("no orders found to match" /
        // "not enough balance" on every retry, forever) — see README bug writeup.
        //
        // floor2, not round2: rounding to nearest can push the requested size *above* the
        // true held balance (round2(1.5151) == 1.52 on a 1.515150-share holding), which
        // guarantees a permanent "not enough balance" — see
        // trader/doc/incident_doge_2026-07-03.md.
        let size_dec = floor2(shares);
        let Ok(size_dec) = Decimal::from_str(&format!("{size_dec:.2}")) else {
            return CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some("bad size".to_string()) };
        };

        for attempt in 1..=self.cfg.close_max_retries {
            let result = self
                .client
                .market_order()
                .token_id(token_id)
                .side(SdkSide::Sell)
                .amount(Amount::shares(size_dec).unwrap_or(Amount::shares(Decimal::ZERO).unwrap()))
                .order_type(OrderType::FAK)
                .build_sign_and_post(&self.signer)
                .await;

            match result {
                Ok(resp) if resp.success => {
                    let filled_usdc: f64 = resp.taking_amount.to_string().parse().unwrap_or(0.0);
                    let sold: f64 = resp.making_amount.to_string().parse().unwrap_or(0.0);
                    return CloseResult { filled_usdc, status: SellStatus::Matched, shares_sold: sold, error: None };
                }
                Ok(_) => {
                    return CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some("order not successful".to_string()) };
                }
                Err(e) => {
                    let msg = e.to_string();
                    // Matches bot/trading.py::_close_position's retry cadence: a FAK
                    // no-match is retried immediately (no reason to wait — the book can
                    // change tick to tick), while "not enough balance" gets a 1s sleep
                    // since that specifically means the BUY fill hasn't settled
                    // on-chain yet and hammering it immediately won't help.
                    if attempt < self.cfg.close_max_retries {
                        if msg.contains("no orders found to match with FAK order") {
                            eprintln!("[close] retry {attempt}/{}: {msg}", self.cfg.close_max_retries);
                            continue;
                        }
                        if msg.contains("not enough balance") {
                            eprintln!("[close] retry {attempt}/{}: {msg}", self.cfg.close_max_retries);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                    return CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some(msg) };
                }
            }
        }
        CloseResult { filled_usdc: 0.0, status: SellStatus::Failed, shares_sold: 0.0, error: Some("CLOSE_RETRIES_EXHAUSTED".to_string()) }
    }

    async fn cancel_limit_sell(&self, order_id: &str) -> bool {
        if order_id.is_empty() {
            return true;
        }
        match self.client.cancel_order(order_id).await {
            Ok(resp) => resp.canceled.iter().any(|id| id == order_id) || !resp.not_canceled.contains_key(order_id),
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                msg.contains("not found") || msg.contains("cancelled") || msg.contains("filled")
            }
        }
    }

    async fn cancel_all(&self) -> bool {
        self.client.cancel_all_orders().await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_token() -> U256 {
        U256::from(1u64)
    }

    #[tokio::test]
    async fn sim_full_fill_at_capped_price() {
        let engine = SimExecutionEngine::new();
        let r = engine.place(dummy_token(), 0.80, 1.0, 0.95).await;
        assert!(r.placed);
        assert!((r.cost - 0.80).abs() < 1e-9);
        assert!((r.filled_shares - 1.25).abs() < 1e-9); // 1.0 / 0.80
    }

    #[tokio::test]
    async fn sim_caps_price_at_max_buy_price() {
        let engine = SimExecutionEngine::new();
        let r = engine.place(dummy_token(), 0.99, 1.0, 0.95).await;
        assert!((r.cost - 0.95).abs() < 1e-9);
    }

    #[tokio::test]
    async fn sim_rejects_invalid_price() {
        let engine = SimExecutionEngine::new();
        let r = engine.place(dummy_token(), 0.0, 1.0, 0.95).await;
        assert!(!r.placed);
        assert_eq!(r.error.as_deref(), Some("invalid price"));
    }

    #[tokio::test]
    async fn sim_partial_fill_branch() {
        let engine = SimExecutionEngine::with_fill_ratio(0.5);
        let r = engine.place(dummy_token(), 0.80, 1.0, 0.95).await;
        assert!(r.placed);
        assert!((r.filled_shares - 0.63).abs() < 1e-9, "got {}", r.filled_shares); // round2(1.25*0.5) = round2(0.625) = 0.63
    }

    #[tokio::test]
    async fn sim_close_position_partial() {
        let engine = SimExecutionEngine::with_fill_ratio(0.6);
        let r = engine.close_position(dummy_token(), 1.25).await;
        assert_eq!(r.status, SellStatus::Matched);
        assert!((r.shares_sold - 0.75).abs() < 1e-9); // round2(1.25*0.6)
    }

    #[tokio::test]
    async fn sim_limit_sell_and_cancel() {
        let engine = SimExecutionEngine::new();
        let r = engine.place_limit_sell(dummy_token(), 1.25, 0.83).await;
        assert_eq!(r.status, SellStatus::Live);
        assert!(r.order_id.is_some());
        assert!(engine.cancel_limit_sell(&r.order_id.unwrap()).await);
    }

    #[tokio::test]
    async fn sim_zero_shares_rejected() {
        let engine = SimExecutionEngine::new();
        let r = engine.place_limit_sell(dummy_token(), 0.0, 0.83).await;
        assert_eq!(r.status, SellStatus::Failed);
        assert!(r.order_id.is_none());
    }

    #[test]
    fn aggressive_entry_first_attempt_splits_the_spread() {
        // price 0.80, max_buy_price 0.95 -> spread 0.15 -> first attempt sits at
        // the midpoint, 0.875, not a small fixed-slippage bump off 0.80.
        assert!((aggressive_entry_price(0.80, 0.95, 0) - 0.875).abs() < 1e-9);
    }

    #[test]
    fn aggressive_entry_retry_jumps_straight_to_cap() {
        let price = 0.80_f64;
        let max_buy_price = 0.95_f64;
        for attempt in 1..=5 {
            assert!((aggressive_entry_price(price, max_buy_price, attempt) - max_buy_price).abs() < 1e-9);
        }
    }

    #[test]
    fn aggressive_entry_never_exceeds_max_buy_price() {
        let price = 0.90_f64;
        let max_buy_price = 0.95_f64;
        for attempt in 0..=5 {
            let p = aggressive_entry_price(price, max_buy_price, attempt);
            assert!(p <= max_buy_price + 1e-9, "attempt {attempt} price {p} exceeded cap");
        }
    }

    #[test]
    fn aggressive_entry_price_already_at_or_above_cap_stays_at_cap() {
        // price already >= max_buy_price -> zero/negative spread -> first attempt
        // (and every retry) sits at the ceiling, never above it.
        assert!((aggressive_entry_price(0.97, 0.95, 0) - 0.95).abs() < 1e-9);
        assert!((aggressive_entry_price(0.97, 0.95, 3) - 0.95).abs() < 1e-9);
    }

    #[test]
    fn floor2_never_exceeds_input() {
        // 2026-07-03 17:33 DOGE incident: round2(1.5151) rounded UP to 1.52, exceeding the
        // true held balance (1.515150) and permanently failing "not enough balance" on
        // every close attempt regardless of retries.
        assert!((floor2(1.5151) - 1.51).abs() < 1e-9);
        for shares in [0.001, 0.019, 0.125, 1.5151, 1.999, 9.996] {
            assert!(floor2(shares) <= shares + 1e-9, "floor2({shares}) exceeded input");
        }
    }

    #[test]
    fn floor2_exact_two_decimals_unchanged() {
        assert!((floor2(1.52) - 1.52).abs() < 1e-9);
        assert!((floor2(0.0) - 0.0).abs() < 1e-9);
    }
}
