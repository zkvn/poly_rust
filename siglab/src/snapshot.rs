//! Shared "current state" snapshot, read by the hourly report writer and updated by every
//! market task (crypto and weather) on each tick. A `std::sync::Mutex` is enough here — the
//! critical section is a single hashmap insert, never held across an `.await`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub kind: &'static str, // "crypto" | "weather"
    pub label: String,      // e.g. "BTC-5m" or "hong-kong: 33°C"
    pub up_price: f64,
    pub dn_price: f64,
    pub last_tick_ms: i64,
}

pub type SharedSnapshots = Arc<Mutex<HashMap<String, MarketSnapshot>>>;

pub fn new_shared() -> SharedSnapshots {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn update(shared: &SharedSnapshots, key: &str, snap: MarketSnapshot) {
    if let Ok(mut map) = shared.lock() {
        map.insert(key.to_string(), snap);
    }
}
