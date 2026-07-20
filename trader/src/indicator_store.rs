//! Consumption side of the standalone `indicator` module
//! (`trader/doc/feature_vol_2026-07-18.md`): parse `indicator.<ASSET>` NATS
//! payloads and hold the latest snapshot per asset.
//!
//! Phase 1 is deliberately decision-neutral — nothing here feeds a gate; the
//! driver only logs snapshots (heartbeat + first receipt) so the docker A/B
//! soak measures pure consumption overhead. The `vals` map is open by design:
//! a new indicator published upstream shows up as a new key with zero trader
//! changes, whether it came from NATS prices or an external source.

use std::collections::HashMap;

use serde::Deserialize;

/// One `indicator.<ASSET>` message. `vals` holds only ready values (warmup
/// keys are absent upstream).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct IndicatorSnapshot {
    pub ts: f64,
    pub asset: String,
    pub market: String,
    pub slot: u64,
    pub vals: HashMap<String, f64>,
}

impl IndicatorSnapshot {
    pub fn parse(payload: &[u8]) -> Option<Self> {
        serde_json::from_slice(payload).ok()
    }

    /// Compact one-line rendering for the heartbeat log: `p_up=0.612 snr=+0.45
    /// vol_har=8.1e-4` (keys sorted for stable output), or `warming` when
    /// nothing is ready yet.
    pub fn render(&self) -> String {
        if self.vals.is_empty() {
            return "warming".to_string();
        }
        let mut keys: Vec<&String> = self.vals.keys().collect();
        keys.sort();
        keys.iter()
            .map(|k| format!("{k}={:.4}", self.vals[k.as_str()]))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// Latest snapshot per asset. Single-owner (lives inside the driver's select
/// loop) — no locking by construction.
#[derive(Debug, Default)]
pub struct IndicatorStore {
    latest: HashMap<String, IndicatorSnapshot>,
}

impl IndicatorStore {
    pub fn update(&mut self, snap: IndicatorSnapshot) {
        self.latest.insert(snap.asset.clone(), snap);
    }

    /// The asset's snapshot if it is recent enough to act on. `max_age_secs`
    /// guards a dead indicator process (stale values must read as absent, the
    /// same posture as `max_price_age_secs` on ticks). Callers gating entries
    /// (phase 2) must additionally check `snap.slot` against their own cycle —
    /// a previous-cycle snapshot is not entry-grade.
    pub fn fresh(&self, asset: &str, now: f64, max_age_secs: f64) -> Option<&IndicatorSnapshot> {
        self.latest
            .get(asset)
            .filter(|s| now - s.ts <= max_age_secs)
    }

    /// The asset's latest snapshot regardless of age — `None` only when the
    /// asset has never been seen at all. Lets a display distinguish "never
    /// received" from "stale" instead of collapsing both into a blank "no
    /// data" (trader/doc/plan_stale_data_gate_2026-07-20.md §1, audit item 1:
    /// a reader should be able to see the last-known reading and its age, not
    /// just that it's too old to act on).
    pub fn raw(&self, asset: &str) -> Option<&IndicatorSnapshot> {
        self.latest.get(asset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> &'static [u8] {
        br#"{"ts":1784812345.201,"asset":"BTC","market":"5m","slot":1784812200,
            "vals":{"vol_har":0.000812,"p_up":0.6113,"snr":0.4479}}"#
    }

    #[test]
    fn parses_indicator_payload() {
        let snap = IndicatorSnapshot::parse(payload()).expect("valid");
        assert_eq!(snap.asset, "BTC");
        assert_eq!(snap.slot, 1_784_812_200);
        assert_eq!(snap.vals.len(), 3);
        assert!((snap.vals["p_up"] - 0.6113).abs() < 1e-12);
    }

    #[test]
    fn parse_rejects_garbage_without_panicking() {
        assert!(IndicatorSnapshot::parse(b"not json").is_none());
        assert!(IndicatorSnapshot::parse(b"{}").is_none());
        // A binance tick payload on the wrong subject must not parse.
        assert!(IndicatorSnapshot::parse(br#"{"ts":1.0,"price":100.0}"#).is_none());
    }

    #[test]
    fn warmup_payload_parses_with_empty_vals() {
        let snap = IndicatorSnapshot::parse(
            br#"{"ts":1.0,"asset":"BTC","market":"5m","slot":0,"vals":{}}"#,
        )
        .expect("valid");
        assert!(snap.vals.is_empty());
        assert_eq!(snap.render(), "warming");
    }

    #[test]
    fn store_returns_fresh_and_hides_stale() {
        let mut store = IndicatorStore::default();
        let snap = IndicatorSnapshot::parse(payload()).expect("valid");
        store.update(snap);
        let now_fresh = 1_784_812_345.201 + 2.0;
        assert!(store.fresh("BTC", now_fresh, 5.0).is_some());
        assert!(store.fresh("BTC", now_fresh + 10.0, 5.0).is_none(), "stale");
        assert!(
            store.fresh("ETH", now_fresh, 5.0).is_none(),
            "unknown asset"
        );
    }

    #[test]
    fn newer_snapshot_replaces_older() {
        let mut store = IndicatorStore::default();
        let mut a = IndicatorSnapshot::parse(payload()).expect("valid");
        store.update(a.clone());
        a.ts += 1.0;
        a.vals.insert("p_up".into(), 0.7);
        store.update(a);
        let got = store.fresh("BTC", 1_784_812_347.0, 5.0).expect("fresh");
        assert!((got.vals["p_up"] - 0.7).abs() < 1e-12);
    }

    #[test]
    fn render_is_sorted_and_stable() {
        let snap = IndicatorSnapshot::parse(payload()).expect("valid");
        assert_eq!(snap.render(), "p_up=0.6113 snr=0.4479 vol_har=0.0008");
    }
}
