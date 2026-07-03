// Auto-redemption of resolved winning positions — ports bot/redemption.py.
//
// Scope note: this module ports the read-only discovery half (fetch + classify
// redeemable positions) faithfully. The actual on-chain redemption transaction
// (Python's `_redeem_position`/`_execute_txn`, which builds and submits a
// relayed contract call against the CTF/UMA-adapter/neg-risk-adapter
// addresses) is intentionally left as an unimplemented `RedeemExecutor` trait
// boundary here — submitting on-chain transactions that move already-claimed
// value is a bigger blast-radius action than a $1 CLOB test order (B2) and
// needs its own design + explicit go-ahead before a real impl lands.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;

const DATA_API_HOST: &str = "https://data-api.polymarket.com";

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedeemablePosition {
    #[serde(default)]
    pub proxy_wallet: String,
    #[serde(default)]
    pub asset: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub size: f64,
    #[serde(default)]
    pub current_value: f64,
    #[serde(default)]
    pub redeemable: bool,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub outcome_index: i64,
}

/// Fetch redeemable positions for `wallet` from the Data API (read-only —
/// no funds move). Mirrors `_fetch_redeemable_positions`: filters to entries
/// where `redeemable && size > 0`, and silently drops rows that fail to parse.
pub async fn fetch_redeemable_positions(http: &reqwest::Client, wallet: &str) -> Result<Vec<RedeemablePosition>> {
    let resp = http
        .get(format!("{DATA_API_HOST}/positions"))
        .query(&[("user", wallet), ("redeemable", "true"), ("sizeThreshold", "0")])
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .context("positions request")?;

    let raw: Vec<serde_json::Value> = resp.json().await.context("positions json")?;
    let positions = raw
        .into_iter()
        .filter_map(|item| serde_json::from_value::<RedeemablePosition>(item).ok())
        .filter(|p| p.redeemable && p.size > 0.0)
        .collect();
    Ok(positions)
}

/// Split positions into (winning, losing) by current value — mirrors Python's
/// `currentValue > 0.01` threshold (dust below that is not worth a redeem tx).
pub fn classify(positions: &[RedeemablePosition]) -> (Vec<&RedeemablePosition>, Vec<&RedeemablePosition>) {
    positions.iter().partition(|p| p.current_value > 0.01)
}

/// The on-chain redemption boundary. A real implementation builds and submits
/// a relayed transaction against the CTF/UMA-adapter/neg-risk-adapter
/// contracts (see bot/redemption.py `_redeem_position`/`_execute_txn`) — left
/// unimplemented here pending explicit go-ahead (see module doc comment).
#[async_trait::async_trait]
pub trait RedeemExecutor: Send + Sync {
    async fn redeem(&self, position: &RedeemablePosition) -> Result<bool>;
}

/// Tracks condition IDs already attempted this session so a position isn't
/// re-submitted every poll (mirrors Python's `self._attempted: set[str]`).
pub struct RedemptionTracker {
    attempted: HashSet<String>,
}

impl RedemptionTracker {
    pub fn new() -> Self {
        Self { attempted: HashSet::new() }
    }

    /// Run one check-and-redeem pass. Losing (dust) positions are marked
    /// attempted immediately without a redeem call (matches Python); winning
    /// positions not yet attempted are redeemed via `executor` and marked on
    /// success. Returns the count of newly-successful redemptions.
    pub async fn check_and_redeem(
        &mut self,
        positions: &[RedeemablePosition],
        executor: &dyn RedeemExecutor,
    ) -> usize {
        let (winning, losing) = classify(positions);
        for p in &losing {
            if !p.condition_id.is_empty() {
                self.attempted.insert(p.condition_id.clone());
            }
        }

        let mut redeemed = 0;
        for p in winning {
            if p.condition_id.is_empty() || self.attempted.contains(&p.condition_id) {
                continue;
            }
            match executor.redeem(p).await {
                Ok(true) => {
                    self.attempted.insert(p.condition_id.clone());
                    redeemed += 1;
                }
                Ok(false) | Err(_) => {}
            }
        }
        redeemed
    }

    pub fn attempted_count(&self) -> usize {
        self.attempted.len()
    }
}

impl Default for RedemptionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn pos(condition_id: &str, current_value: f64, size: f64) -> RedeemablePosition {
        RedeemablePosition {
            proxy_wallet: "0xabc".to_string(),
            asset: "1234".to_string(),
            condition_id: condition_id.to_string(),
            size,
            current_value,
            redeemable: true,
            title: "test market".to_string(),
            outcome: "Yes".to_string(),
            outcome_index: 0,
        }
    }

    struct MockExecutor {
        calls: Arc<AtomicUsize>,
        succeed: bool,
    }

    #[async_trait::async_trait]
    impl RedeemExecutor for MockExecutor {
        async fn redeem(&self, _position: &RedeemablePosition) -> Result<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.succeed)
        }
    }

    #[test]
    fn classify_splits_winning_and_losing() {
        let positions = vec![pos("a", 5.0, 1.0), pos("b", 0.0, 1.0), pos("c", 0.005, 1.0)];
        let (winning, losing) = classify(&positions);
        assert_eq!(winning.len(), 1);
        assert_eq!(losing.len(), 2);
        assert_eq!(winning[0].condition_id, "a");
    }

    #[tokio::test]
    async fn losing_positions_marked_attempted_without_redeem_call() {
        let mut tracker = RedemptionTracker::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = MockExecutor { calls: Arc::clone(&calls), succeed: true };

        let positions = vec![pos("dust1", 0.0, 1.0)];
        let redeemed = tracker.check_and_redeem(&positions, &executor).await;

        assert_eq!(redeemed, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(tracker.attempted_count(), 1);
    }

    #[tokio::test]
    async fn winning_position_redeemed_once() {
        let mut tracker = RedemptionTracker::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = MockExecutor { calls: Arc::clone(&calls), succeed: true };

        let positions = vec![pos("win1", 5.0, 1.0)];
        let redeemed = tracker.check_and_redeem(&positions, &executor).await;
        assert_eq!(redeemed, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second pass with the same position should not re-redeem.
        let redeemed2 = tracker.check_and_redeem(&positions, &executor).await;
        assert_eq!(redeemed2, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_redeem_is_not_marked_attempted() {
        let mut tracker = RedemptionTracker::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = MockExecutor { calls: Arc::clone(&calls), succeed: false };

        let positions = vec![pos("win1", 5.0, 1.0)];
        let redeemed = tracker.check_and_redeem(&positions, &executor).await;
        assert_eq!(redeemed, 0);
        assert_eq!(tracker.attempted_count(), 0);

        // Retried on the next pass since it wasn't marked attempted.
        tracker.check_and_redeem(&positions, &executor).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
