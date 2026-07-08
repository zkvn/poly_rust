// USER-channel fill watcher — ports bot/unwind_watcher.py.
//
// The SDK exposes the USER channel natively (Client::subscribe_trades on an
// authenticated client), so unlike the Binance feed there's no hand-rolled WS
// client here — just routing + the fill-status filter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures::StreamExt as _;
use polymarket_client_sdk_v2::auth::Credentials;
use polymarket_client_sdk_v2::clob::ws::Client as WsClient;
use polymarket_client_sdk_v2::clob::ws::types::response::{TradeMessage, TradeMessageStatus};
use polymarket_client_sdk_v2::types::{Address, B256};
use polymarket_client_sdk_v2::ws::config::Config as WsConfig;

/// A GTC limit-sell fill is reported once as MATCHED then again as CONFIRMED —
/// both are legitimate fill signals (mirrors Python's `_FILL_STATUSES`).
pub fn is_fill_status(status: &TradeMessageStatus) -> bool {
    matches!(
        status,
        TradeMessageStatus::Matched | TradeMessageStatus::Confirmed
    )
}

pub type FillCallback = Box<dyn Fn(&TradeMessage) + Send + Sync>;

/// Routes incoming user-channel trade messages to per-order-id callbacks.
/// `watch`/`unwatch` may be called from any thread; the callback itself is
/// invoked on whichever task is driving the subscription stream and must be
/// short-lived (mirrors the Python docstring's threading contract).
#[derive(Clone)]
pub struct UnwindWatcher {
    watchers: Arc<Mutex<HashMap<String, FillCallback>>>,
}

impl UnwindWatcher {
    pub fn new() -> Self {
        Self {
            watchers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn watch(&self, order_id: String, callback: FillCallback) {
        self.watchers.lock().unwrap().insert(order_id, callback);
    }

    pub fn unwatch(&self, order_id: &str) {
        self.watchers.lock().unwrap().remove(order_id);
    }

    pub fn watched_count(&self) -> usize {
        self.watchers.lock().unwrap().len()
    }

    /// Dispatch one incoming message: fires the matching callback iff the
    /// message is a fill (MATCHED/CONFIRMED) for a watched `taker_order_id`.
    /// Pure routing — testable without a live WS connection.
    pub fn dispatch(&self, msg: &TradeMessage) {
        if !is_fill_status(&msg.status) {
            return;
        }
        let Some(order_id) = &msg.taker_order_id else {
            return;
        };
        let watchers = self.watchers.lock().unwrap();
        if let Some(cb) = watchers.get(order_id) {
            cb(msg);
        }
    }

    /// Subscribe to the live USER channel and dispatch fills to registered
    /// watchers until the stream closes, then reconnect (rebuilding the
    /// authenticated ws client each time, matching the `_run`/`run_forever`
    /// reconnect loop in bot/unwind_watcher.py).
    ///
    /// `credentials`/`address` are the same L2 API credentials + signer
    /// address the live CLOB execution engine derives (`derive_api_key`) —
    /// this reuses them rather than re-deriving.
    ///
    /// Every message is logged with our own wall-clock receipt timestamp
    /// before dispatch, regardless of whether anything is `watch()`-ing that
    /// order — a passive, always-on real-time record of exchange-reported
    /// fills for latency/slippage forensics (see
    /// `trader/doc/incident_sol_unwind_but_loss_2026-07-06.md` §6), independent
    /// of whatever functional use `dispatch()`'s per-order callbacks are put to.
    pub async fn run(
        &self,
        ws_endpoint: &str,
        credentials: Credentials,
        address: Address,
        markets: Vec<B256>,
    ) -> Result<()> {
        loop {
            let client = WsClient::new(ws_endpoint, WsConfig::default())?;
            let client = client.authenticate(credentials.clone(), address)?;
            match client.subscribe_trades(markets.clone()) {
                Ok(stream) => {
                    let mut s = Box::pin(stream);
                    while let Some(Ok(msg)) = s.next().await {
                        let recv_ts = crate::marketdata::now_secs_f64();
                        println!(
                            "[unwind] fill event recv_ts={recv_ts:.3} status={:?} taker_order_id={:?} side={:?} price={} size={} matchtime={:?}",
                            msg.status,
                            msg.taker_order_id,
                            msg.side,
                            msg.price,
                            msg.size,
                            msg.matchtime,
                        );
                        self.dispatch(&msg);
                    }
                    eprintln!("[unwind] USER trade stream closed, reconnecting…");
                }
                Err(e) => eprintln!("[unwind] subscribe_trades failed: {e:#}, retrying…"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}

impl Default for UnwindWatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::clob::types::Side;
    use polymarket_client_sdk_v2::types::{Decimal, U256};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_msg(order_id: Option<&str>, status: TradeMessageStatus) -> TradeMessage {
        TradeMessage::builder()
            .id("trade-1".to_string())
            .market(B256::ZERO)
            .asset_id(U256::from(1u64))
            .side(Side::Sell)
            .size(Decimal::ZERO)
            .price(Decimal::ZERO)
            .status(status)
            .maybe_taker_order_id(order_id.map(|s| s.to_string()))
            .maker_orders(vec![])
            .build()
    }

    #[test]
    fn fires_callback_on_matched_fill() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );

        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Matched));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fires_callback_on_confirmed_fill() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );

        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Confirmed));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ignores_non_fill_statuses() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );

        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Retrying));
        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Failed));
        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Mined));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn ignores_fills_for_unwatched_orders() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );

        w.dispatch(&make_msg(Some("order-2"), TradeMessageStatus::Matched));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn ignores_messages_without_taker_order_id() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );

        w.dispatch(&make_msg(None, TradeMessageStatus::Matched));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unwatch_stops_future_dispatch() {
        let w = UnwindWatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::clone(&count);
        w.watch(
            "order-1".to_string(),
            Box::new(move |_msg| {
                count2.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert_eq!(w.watched_count(), 1);

        w.unwatch("order-1");
        assert_eq!(w.watched_count(), 0);
        w.dispatch(&make_msg(Some("order-1"), TradeMessageStatus::Matched));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
