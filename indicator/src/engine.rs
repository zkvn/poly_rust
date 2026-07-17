//! Per-asset cycle engine: maintains the 1-Hz price grid, seals cycles at slot
//! boundaries (feeding the HAR state), and produces indicator snapshots.
//!
//! Sampling model: NATS binance ticks arrive at ~4 Hz; the Python bot consumed
//! a 1-Hz poll of the same shared price. The engine reproduces that by
//! appending **one price per whole second** — the latest known price when each
//! second is first crossed. Gaps (no ticks for n seconds) are filled with the
//! last known price, exactly like Python's poll loop re-reading a stale cell.
//! In `replay` mode inputs are already 1-Hz, so Rust and Python see identical
//! sequences and parity is exact by construction.

use crate::config::IndicatorConfig;
use crate::math::{self, HarState};

/// One published indicator snapshot. `vol_har` / `snr` are `None` during
/// warmup (absent keys in the JSON payload); `p_up`'s defined warmup value is
/// 0.5, matching Python.
#[derive(Debug, Clone, PartialEq)]
pub struct Emit {
    pub ts: f64,
    pub slot: u64,
    pub vol_har: Option<f64>,
    pub p_up: Option<f64>,
    pub snr: Option<f64>,
}

impl Emit {
    /// JSON payload for `indicator.<ASSET>` — `vals` is an open map so new
    /// indicators are new keys, no schema change for consumers.
    pub fn to_json(&self, asset: &str, market: &str) -> String {
        let mut vals = serde_json::Map::new();
        let mut put = |k: &str, v: Option<f64>| {
            if let Some(v) = v
                && v.is_finite()
                && let Some(n) = serde_json::Number::from_f64(v)
            {
                vals.insert(k.to_string(), serde_json::Value::Number(n));
            }
        };
        put("vol_har", self.vol_har);
        put("p_up", self.p_up);
        put("snr", self.snr);
        serde_json::json!({
            "ts": self.ts,
            "asset": asset,
            "market": market,
            "slot": self.slot,
            "vals": vals,
        })
        .to_string()
    }
}

/// Whether an indicator computes at all and which denominator it uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Har,
    Streaming,
}

impl Mode {
    fn from_config(enabled: bool, mode: &str) -> Self {
        match (enabled, mode) {
            (false, _) => Self::Off,
            (true, "streaming") => Self::Streaming,
            _ => Self::Har,
        }
    }
}

pub struct AssetEngine {
    period: u64,
    min_ticks: usize,
    subsample_secs: usize,
    nu: f64,
    p_up_mode: Mode,
    snr_mode: Mode,
    har: Option<HarState>,
    /// Current slot start (unix secs, multiple of `period`); 0 = no tick yet.
    slot: u64,
    cycle_open: f64,
    last_price: f64,
    /// Last whole second a 1-Hz sample was appended for; 0 = none this cycle.
    last_sec: u64,
    prices: Vec<f64>,
}

impl AssetEngine {
    /// Build from config for one asset. Errors only on HAR shape violations
    /// the config validation should already have caught (defense in depth).
    pub fn from_config(cfg: &IndicatorConfig, asset: &str) -> Result<Self, String> {
        let h = &cfg.har_vol;
        let har = if h.enabled {
            let beta = h
                .beta_for(asset)
                .ok_or_else(|| format!("no har beta for {asset} and no default"))?;
            Some(HarState::new(h.windows.clone(), beta.clone())?)
        } else {
            None
        };
        Ok(Self {
            period: cfg.period_secs(),
            min_ticks: h.min_ticks,
            subsample_secs: h.subsample_secs,
            nu: h.nu_for(asset),
            p_up_mode: Mode::from_config(cfg.p_up.enabled, &cfg.p_up.mode),
            snr_mode: Mode::from_config(cfg.snr.enabled, &cfg.snr.mode),
            har,
            slot: 0,
            cycle_open: 0.0,
            last_price: 0.0,
            last_sec: 0,
            prices: Vec::with_capacity(cfg.period_secs() as usize + 4),
        })
    }

    fn slot_of(&self, ts: f64) -> u64 {
        ((ts as u64) / self.period) * self.period
    }

    /// Seal the current cycle into the HAR state and start the next one.
    fn roll_cycle(&mut self, new_slot: u64) {
        if let Some(har) = self.har.as_mut() {
            let rv = math::realized_vol(&self.prices, self.min_ticks, self.subsample_secs);
            har.on_cycle(rv);
        }
        self.prices.clear();
        self.slot = new_slot;
        // Last known price at/before the boundary is the new cycle's open —
        // same source Python's worker hands to CycleContext.cycle_open_binance.
        self.cycle_open = self.last_price;
        self.last_sec = 0;
    }

    /// Feed one tick; returns a snapshot to publish (throttling is the
    /// caller's concern). Non-positive prices are ignored, like every signal
    /// in the reference implementation.
    ///
    /// Grid contract (shared with the parity harness): cycle `[slot,
    /// slot+period)` samples seconds `slot+1 ..= slot+period-1`; the sample
    /// for second S is the first tick price seen at/after S, and seconds with
    /// no tick are filled with the last known price — exactly a 1-Hz poll of
    /// a shared price cell that goes stale on feed outage. A cycle spanned
    /// entirely by an outage therefore seals with a constant series → rv = 0
    /// pushed into the HAR buffer, matching what the Python bot's poll loop
    /// records in the same situation (a deliberate parity choice, not a bug).
    pub fn on_tick(&mut self, ts: f64, price: f64) -> Option<Emit> {
        if price <= 0.0 || !price.is_finite() || ts < 1.0 {
            return None;
        }
        let tick_slot = self.slot_of(ts);
        if self.slot == 0 {
            // First tick ever: start mid-cycle; cycle_open unknown (0.0) so
            // p_up/snr stay at warmup until the first boundary — matches the
            // bot joining mid-cycle. Sampling starts at the join second, not
            // slot+1: backfilling the cycle head with the join price would
            // fabricate history the bot never saw.
            self.slot = tick_slot;
            self.last_price = price;
            self.last_sec = (ts as u64).saturating_sub(1).max(tick_slot);
        } else if tick_slot > self.slot {
            while self.slot < tick_slot {
                // Fill the outgoing cycle's tail with the stale price, seal it.
                self.fill_seconds_until(self.slot + self.period - 1);
                self.roll_cycle(self.slot + self.period);
            }
        } else if tick_slot < self.slot {
            // Clock went backwards (NTP step / out-of-order delivery) — drop.
            return None;
        }
        // Seconds strictly before this tick's second saw the previous price.
        self.fill_seconds_until(ts as u64 - 1);
        self.last_price = price;
        self.fill_seconds_until(ts as u64);
        Some(self.snapshot(ts))
    }

    /// Append 1-Hz samples (the current `last_price`) for every un-sampled
    /// whole second up to `sec`, capped at the current cycle's last sample
    /// second (`slot + period - 1`).
    fn fill_seconds_until(&mut self, sec: u64) {
        let start = if self.last_sec == 0 {
            self.slot + 1
        } else {
            self.last_sec + 1
        };
        let end = sec.min(self.slot + self.period - 1);
        if end < start {
            return;
        }
        for _ in start..=end {
            if self.prices.len() < self.period as usize {
                self.prices.push(self.last_price);
            }
        }
        self.last_sec = end;
    }

    /// Current indicator values at time `ts`.
    pub fn snapshot(&self, ts: f64) -> Emit {
        let seconds_remaining = ((self.slot + self.period) as f64 - ts).max(0.0);
        let sigma = self.har.as_ref().and_then(HarState::value);
        let period = self.period as f64;
        let pick_sigma = |mode: Mode| match mode {
            Mode::Har => sigma,
            _ => None,
        };
        let p_up = match self.p_up_mode {
            Mode::Off => None,
            m => Some(math::compute_pup(
                &self.prices,
                self.cycle_open,
                seconds_remaining,
                pick_sigma(m),
                self.nu,
                period,
            )),
        };
        let snr = match self.snr_mode {
            Mode::Off => None,
            m => math::compute_snr(
                &self.prices,
                self.cycle_open,
                seconds_remaining,
                pick_sigma(m),
                period,
            ),
        };
        Emit {
            ts,
            slot: self.slot,
            vol_har: sigma,
            p_up,
            snr,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IndicatorConfig;

    fn cfg() -> IndicatorConfig {
        let toml = r#"
nats_url = "nats://localhost:4222"
assets = ["BTC"]
market = "5m"
[har_vol]
windows = [1, 5, 12]
min_ticks = 30
[har_vol.beta]
default = [0.0, 1.0, 0.0, 0.0]
[har_vol.nu]
default = 4.2469
"#;
        let cfg: IndicatorConfig = toml::from_str(toml).expect("test toml");
        cfg.validate().expect("valid");
        cfg
    }

    fn engine() -> AssetEngine {
        AssetEngine::from_config(&cfg(), "BTC").expect("engine")
    }

    #[test]
    fn first_cycle_has_no_har_and_pup_at_half() {
        let mut e = engine();
        let emit = e.on_tick(300_000.5, 100.0).expect("emit");
        assert_eq!(emit.slot, 300_000);
        assert_eq!(emit.vol_har, None, "no forecast in first cycle");
        assert_eq!(emit.p_up, Some(0.5), "HAR warmup p_up is 0.5");
        assert_eq!(emit.snr, None, "snr not ready");
    }

    #[test]
    fn one_hz_grid_appends_once_per_second_and_fills_gaps() {
        let mut e = engine();
        e.on_tick(300_000.1, 100.0);
        e.on_tick(300_001.2, 101.0); // second 300_001 → one sample
        e.on_tick(300_001.7, 102.0); // same second → no new sample
        assert_eq!(e.prices, vec![101.0]);
        e.on_tick(300_005.0, 103.0); // gap: 300_002..300_004 filled with 102.0
        assert_eq!(e.prices, vec![101.0, 102.0, 102.0, 102.0, 103.0]);
    }

    #[test]
    fn cycle_boundary_seals_rv_and_sets_open() {
        let mut e = engine();
        // Cycle 1: 300 ticks at 1 Hz, all 100.0 → rv = 0 (valid, ≥30 ticks).
        for s in 0..300u64 {
            e.on_tick((300_000 + s) as f64 + 0.1, 100.0);
        }
        let emit = e.on_tick(300_300.2, 100.0).expect("emit");
        assert_eq!(emit.slot, 300_300, "rolled to next slot");
        // beta = [0,1,0,0] → forecast == rv[-1] == 0.0.
        assert_eq!(emit.vol_har, Some(0.0));
        // cycle_open now known → snr defined (streaming denominator not ready,
        // HAR sigma = 0 → not > 0 → HAR path declines → falls to streaming with
        // <3 prices → None still).
        assert_eq!(emit.snr, None);
    }

    #[test]
    fn har_forecast_tracks_previous_cycle_rv() {
        let mut e = engine();
        // Cycle 1: linear ramp 100.0 → 102.99 (0.01/s).
        for s in 0..300u64 {
            e.on_tick((300_000 + s) as f64 + 0.1, 100.0 + s as f64 * 0.01);
        }
        // Boundary tick.
        let emit = e.on_tick(300_300.1, 103.0).expect("emit");
        let sigma = emit.vol_har.expect("forecast after one full cycle");
        assert!(sigma > 0.0, "ramp cycle has positive rv");
        // p_up now uses HAR Student-t; price at open → ~0.5, price above open → >0.5.
        let up = e.on_tick(300_310.0, 104.0).expect("emit");
        assert!(up.p_up.expect("ready") > 0.5);
        let down = e.on_tick(300_311.0, 102.0).expect("emit");
        assert!(down.p_up.expect("ready") < 0.5);
        let snr = down.snr.expect("ready");
        assert!(snr < 0.0, "below open → negative snr, got {snr}");
    }

    #[test]
    fn stale_feed_fills_cycle_and_pushes_zero_rv_like_python_poll() {
        let mut e = engine();
        // Full valid cycle to warm up (ramp → positive rv).
        for s in 0..300u64 {
            e.on_tick((300_000 + s) as f64 + 0.1, 100.0 + s as f64 * 0.01);
        }
        e.on_tick(300_300.1, 103.0);
        let sigma_before = e.snapshot(300_301.0).vol_har.expect("warm forecast");
        assert!(sigma_before > 0.0);
        // Feed silent for the whole next cycle. The 1-Hz poll semantics fill
        // it with the stale price → constant series → rv = 0.0 pushed; with
        // beta = [0,1,0,0] the forecast collapses to exactly 0.
        let emit = e.on_tick(300_600.5, 103.0).expect("emit");
        assert_eq!(emit.slot, 300_600);
        assert_eq!(emit.vol_har, Some(0.0));
    }

    #[test]
    fn partial_first_cycle_below_min_ticks_yields_no_rv() {
        let mut e = engine();
        // Join 10 s before the boundary: only ~9 samples < min_ticks 30.
        for s in 290..300u64 {
            e.on_tick((300_000 + s) as f64 + 0.1, 100.0);
        }
        let emit = e.on_tick(300_300.2, 100.0).expect("emit");
        assert_eq!(
            emit.vol_har, None,
            "short join cycle must not seed the buffer"
        );
    }

    #[test]
    fn multi_slot_gap_rolls_to_current_slot() {
        let mut e = engine();
        e.on_tick(300_000.5, 100.0);
        let emit = e.on_tick(301_500.5, 105.0).expect("emit"); // 5 slots later
        assert_eq!(emit.slot, 301_500);
    }

    #[test]
    fn backwards_clock_tick_dropped() {
        let mut e = engine();
        e.on_tick(300_300.5, 100.0);
        assert!(e.on_tick(300_299.5, 99.0).is_none());
        assert!(e.on_tick(0.0, 100.0).is_none());
        assert!(e.on_tick(300_301.0, -1.0).is_none());
        assert!(e.on_tick(300_301.0, f64::NAN).is_none());
    }

    #[test]
    fn emit_json_omits_warmup_values() {
        let emit = Emit {
            ts: 1_784_800_000.25,
            slot: 1_784_799_900,
            vol_har: None,
            p_up: Some(0.5),
            snr: None,
        };
        let json = emit.to_json("BTC", "5m");
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(v["asset"], "BTC");
        assert_eq!(v["market"], "5m");
        assert_eq!(v["slot"], 1_784_799_900u64);
        assert_eq!(v["vals"]["p_up"], 0.5);
        assert!(v["vals"].get("vol_har").is_none());
        assert!(v["vals"].get("snr").is_none());
    }

    #[test]
    fn emit_json_carries_all_values_when_ready() {
        let emit = Emit {
            ts: 1.5,
            slot: 0,
            vol_har: Some(8.12e-4),
            p_up: Some(0.6113),
            snr: Some(-0.4479),
        };
        let v: serde_json::Value =
            serde_json::from_str(&emit.to_json("ETH", "15m")).expect("valid json");
        assert!((v["vals"]["vol_har"].as_f64().unwrap() - 8.12e-4).abs() < 1e-15);
        assert!((v["vals"]["snr"].as_f64().unwrap() + 0.4479).abs() < 1e-15);
    }
}
