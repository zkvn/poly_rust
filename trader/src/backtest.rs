// Parquet loading + cycle replay + halt accounting.
//
// Matches Python bot.backtest.run_backtest exactly:
//   - Tick merge-sort: stable (b rows before p rows → binance-first at equal ts)
//   - Halt: per-strategy, check BEFORE cycle, update AFTER
//   - HKT session boundary: (slug_ts − halt_reset_hour * 3600).date() in +08:00

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::array::{Array, Float64Array, LargeStringArray, StringArray};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, TimeZone};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::config::AssetParams;
use crate::machine::Machine;
use crate::types::{BinanceTick, CycleContext, PolyTick, TradeRecord};

// ── HKT timezone (+08:00) ─────────────────────────────────────────────────────

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

/// Python: `(dt - timedelta(hours=reset_hour)).date()` in HKT
pub(crate) fn hkt_session(slug_ts: f64, reset_hour: i64) -> NaiveDate {
    let dt: DateTime<FixedOffset> = hkt().timestamp_opt(slug_ts as i64, 0).unwrap();
    (dt - Duration::hours(reset_hour)).date_naive()
}

// ── Arrow helpers ─────────────────────────────────────────────────────────────

/// Extract a string value from a column that may be Utf8 (i32) or LargeUtf8 (i64).
fn get_str_value(col: &dyn Array, i: usize) -> Option<&str> {
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        if arr.is_null(i) {
            return None;
        }
        Some(arr.value(i))
    } else if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        if arr.is_null(i) {
            return None;
        }
        Some(arr.value(i))
    } else {
        None
    }
}

fn is_null_str(col: &dyn Array, i: usize) -> bool {
    get_str_value(col, i).is_none()
}

// ── Parquet rows ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BinanceRow {
    pub ts: f64,
    pub price: f64,
    pub slug: String,
}

#[derive(Debug, Clone)]
pub struct PolyRow {
    pub ts: f64,
    pub up: f64,
    pub dn: f64,
    pub slug: String,
}

// ── Parquet loading ───────────────────────────────────────────────────────────

pub fn load_binance(path: &str) -> Result<Vec<BinanceRow>> {
    let file = File::open(path).with_context(|| format!("open {path}"))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet open {path}"))?
        .build()
        .with_context(|| format!("parquet build {path}"))?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("read batch {path}"))?;
        let ts_col = batch
            .column_by_name("ts")
            .with_context(|| "missing column ts")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "ts not Float64")?;
        let price_col = batch
            .column_by_name("binance")
            .with_context(|| "missing column binance")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "binance not Float64")?;
        let slug_col = batch
            .column_by_name("slug")
            .with_context(|| "missing column slug")?;
        let slug_col = slug_col.as_ref();
        for i in 0..batch.num_rows() {
            if ts_col.is_null(i) || price_col.is_null(i) || is_null_str(slug_col, i) {
                continue;
            }
            rows.push(BinanceRow {
                ts: ts_col.value(i),
                price: price_col.value(i),
                slug: get_str_value(slug_col, i).unwrap().to_string(),
            });
        }
    }
    Ok(rows)
}

pub fn load_poly(path: &str) -> Result<Vec<PolyRow>> {
    let file = File::open(path).with_context(|| format!("open {path}"))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet open {path}"))?
        .build()
        .with_context(|| format!("parquet build {path}"))?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("read batch {path}"))?;
        let ts_col = batch
            .column_by_name("ts")
            .with_context(|| "missing column ts")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "ts not Float64")?;
        let up_col = batch
            .column_by_name("up")
            .with_context(|| "missing column up")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "up not Float64")?;
        let dn_col = batch
            .column_by_name("dn")
            .with_context(|| "missing column dn")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .with_context(|| "dn not Float64")?;
        let slug_col = batch
            .column_by_name("slug")
            .with_context(|| "missing column slug")?;
        let slug_col = slug_col.as_ref();
        for i in 0..batch.num_rows() {
            if ts_col.is_null(i)
                || up_col.is_null(i)
                || dn_col.is_null(i)
                || is_null_str(slug_col, i)
            {
                continue;
            }
            rows.push(PolyRow {
                ts: ts_col.value(i),
                up: up_col.value(i),
                dn: dn_col.value(i),
                slug: get_str_value(slug_col, i).unwrap().to_string(),
            });
        }
    }
    Ok(rows)
}

/// Filter rows to the HKT calendar day for `date` (YYYY-MM-DD).
/// Keeps slugs whose embedded timestamp falls within [00:00, 23:59:59] HKT on that day.
pub fn filter_by_date(
    b_rows: Vec<BinanceRow>,
    p_rows: Vec<PolyRow>,
    date: &str,
) -> Result<(Vec<BinanceRow>, Vec<PolyRow>)> {
    let parts: Vec<i32> = date.split('-').map(|s| s.parse::<i32>().unwrap()).collect();
    let (y, m, d) = (parts[0], parts[1] as u32, parts[2] as u32);
    let hkt = hkt();
    let ts_start = hkt.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap().timestamp() as f64;
    let ts_end = hkt
        .with_ymd_and_hms(y, m, d, 23, 59, 59)
        .unwrap()
        .timestamp() as f64;

    // Collect slugs from poly that fall in the day
    let valid_slugs: std::collections::HashSet<String> = p_rows
        .iter()
        .filter_map(|r| {
            let slug_ts: f64 = r.slug.rsplit('-').next()?.parse().ok()?;
            if slug_ts >= ts_start && slug_ts <= ts_end {
                Some(r.slug.clone())
            } else {
                None
            }
        })
        .collect();

    let b = b_rows
        .into_iter()
        .filter(|r| valid_slugs.contains(&r.slug))
        .collect();
    let p = p_rows
        .into_iter()
        .filter(|r| valid_slugs.contains(&r.slug))
        .collect();
    Ok((b, p))
}

// ── Tick merge (Python parity) ────────────────────────────────────────────────

// Python: sorted(b_rows + p_rows, key=lambda r: r[1]) — stable sort, b comes first
// Equivalent: insert binance rows first, poly rows second; stable-sort by ts only.
// => same-ts binance ticks precede poly ticks (Python stable sort invariant).

enum MergedTick {
    Binance(BinanceTick),
    Poly(PolyTick),
}

fn merge_ticks(b_cycle: &[BinanceRow], p_cycle: &[PolyRow]) -> Vec<MergedTick> {
    let b_count = b_cycle.len();
    // Collect (ts, insertion_index, is_poly, row_index)
    let mut keyed: Vec<(f64, usize, bool, usize)> = Vec::with_capacity(b_count + p_cycle.len());
    for (i, r) in b_cycle.iter().enumerate() {
        keyed.push((r.ts, i, false, i));
    }
    for (i, r) in p_cycle.iter().enumerate() {
        keyed.push((r.ts, b_count + i, true, i)); // insertion_index > any binance → comes second
    }
    // Stable sort by ts; ties broken by insertion index (b before p)
    keyed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap().then(a.1.cmp(&b.1)));

    keyed
        .into_iter()
        .map(|(_ts, _ins, is_poly, idx)| {
            if is_poly {
                MergedTick::Poly(PolyTick {
                    ts: p_cycle[idx].ts,
                    up: p_cycle[idx].up,
                    dn: p_cycle[idx].dn,
                })
            } else {
                MergedTick::Binance(BinanceTick {
                    ts: b_cycle[idx].ts,
                    price: b_cycle[idx].price,
                })
            }
        })
        .collect()
}

// ── Cycle replay ──────────────────────────────────────────────────────────────

const CYCLE_LENGTH_S: f64 = 300.0;

fn replay_cycle(
    slug: &str,
    b_cycle: &[BinanceRow],
    p_cycle: &[PolyRow],
    machines: &mut [Machine],
    halted_rev: bool,
    halted_hp: bool,
) -> Vec<TradeRecord> {
    if b_cycle.is_empty() || p_cycle.is_empty() {
        return vec![];
    }

    let slug_ts: f64 = slug.rsplit('-').next().unwrap().parse().unwrap();
    let cycle_end_ts = slug_ts + CYCLE_LENGTH_S;

    // cycle_open_binance = first binance tick sorted by ts
    let mut b_sorted = b_cycle.to_vec();
    b_sorted.sort_by(|a, b| a.ts.partial_cmp(&b.ts).unwrap());
    let cycle_open_binance = b_sorted[0].price;

    let ctx = CycleContext {
        start_ts: slug_ts,
        end_ts: cycle_end_ts,
        open_binance: cycle_open_binance,
    };

    for m in machines.iter_mut() {
        let suppressed = match m.strategy_name {
            "reversal" => halted_rev,
            "high_prob" => halted_hp,
            _ => false,
        };
        m.cycle_open(&ctx, slug, suppressed);
    }

    let ticks = merge_ticks(b_cycle, p_cycle);
    let mut completed = Vec::new();

    for tick in ticks {
        match tick {
            MergedTick::Poly(pt) => {
                for m in machines.iter_mut() {
                    if let Some(rec) = m.on_poly(pt) {
                        completed.push(rec);
                    }
                }
            }
            MergedTick::Binance(bt) => {
                for m in machines.iter_mut() {
                    m.on_binance(bt);
                }
            }
        }
    }

    // Resolve any remaining open positions
    for m in machines.iter_mut() {
        if let Some(rec) = m.cycle_close() {
            completed.push(rec);
        }
    }

    completed
}

// ── HaltTracker ───────────────────────────────────────────────────────────────

pub(crate) struct HaltTracker {
    max: i64, // 0 = disabled
    reset_hour: i64,
    losses: i64,
    last_session: Option<NaiveDate>,
}

impl HaltTracker {
    pub(crate) fn new(max: i64, reset_hour: i64) -> Self {
        Self {
            max,
            reset_hour,
            losses: 0,
            last_session: None,
        }
    }

    /// Rebuilds a tracker with a previously-observed loss count/session — used
    /// to restore halt state across a process restart (`worker.rs::to_persisted`/
    /// `Worker::restore_halt`). `max`/`reset_hour` still come fresh from config,
    /// never from the persisted file, so a config change takes effect immediately.
    pub(crate) fn restore(
        max: i64,
        reset_hour: i64,
        losses: i64,
        last_session: Option<NaiveDate>,
    ) -> Self {
        Self {
            max,
            reset_hour,
            losses,
            last_session,
        }
    }

    pub(crate) fn losses(&self) -> i64 {
        self.losses
    }

    pub(crate) fn last_session(&self) -> Option<NaiveDate> {
        self.last_session
    }

    pub(crate) fn max(&self) -> i64 {
        self.max
    }

    pub(crate) fn reset_hour(&self) -> i64 {
        self.reset_hour
    }

    /// Returns whether this call just cleared an *active* halt — i.e. the
    /// session rolled over *and* the streak was actually halted beforehand —
    /// not merely whether the session changed. Callers that want a Telegram
    /// notification only when the reset actually mattered (`worker.rs`) rely
    /// on this; `backtest.rs::run_backtest`'s own call site ignores it.
    pub(crate) fn reset_if_new_session(&mut self, slug_ts: f64) -> bool {
        let session = hkt_session(slug_ts, self.reset_hour);
        if Some(session) != self.last_session {
            let was_halted = self.is_halted();
            self.losses = 0;
            self.last_session = Some(session);
            return was_halted;
        }
        false
    }

    pub(crate) fn is_halted(&self) -> bool {
        self.max > 0 && self.losses >= self.max
    }

    /// Returns whether this trade is the one that just tripped the halt —
    /// i.e. the streak transitioned from not-halted to halted on this exact
    /// call, not merely "this trade was a loss." A losing trade that lands
    /// while already halted (an open position resolving after halt engaged
    /// mid-cycle) must not re-signal.
    pub(crate) fn record_trade(&mut self, rec: &TradeRecord, strategy_name: &str) -> bool {
        if rec.strategy == strategy_name && rec.outcome.is_loss_for_halt() {
            let was_halted = self.is_halted();
            self.losses += 1;
            return !was_halted && self.is_halted();
        }
        false
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Run the full backtest over all slugs in the price data.
/// Returns one TradeRecord per trade, in slug order (matching Python output order).
pub fn run_backtest(
    params: &AssetParams,
    b_rows: Vec<BinanceRow>,
    p_rows: Vec<PolyRow>,
) -> Vec<TradeRecord> {
    // Group by slug
    let mut b_by_slug: HashMap<String, Vec<BinanceRow>> = HashMap::new();
    for r in b_rows {
        b_by_slug.entry(r.slug.clone()).or_default().push(r);
    }
    let mut p_by_slug: HashMap<String, Vec<PolyRow>> = HashMap::new();
    let mut slugs: Vec<String> = Vec::new();
    for r in p_rows {
        let s = r.slug.clone();
        if !p_by_slug.contains_key(&s) {
            slugs.push(s.clone());
        }
        p_by_slug.entry(s).or_default().push(r);
    }
    slugs.sort(); // matches Python: sorted(p_all["slug"].unique())

    // Build one Machine per strategy (shared state across cycles, resets via cycle_open)
    let mut machines: Vec<Machine> = params
        .strategies
        .iter()
        .map(|name| {
            match name.as_str() {
                "reversal" => Machine::new_reversal(params),
                "high_prob" => Machine::new_high_prob(params),
                _ => Machine::new_reversal(params), // fallback
            }
        })
        .collect();

    let mut halt_rev = HaltTracker::new(params.halt_rev, params.halt_reset_hour_rev);
    let mut halt_hp = HaltTracker::new(params.halt_prob, params.halt_reset_hour_hp);

    let mut all_trades: Vec<TradeRecord> = Vec::new();

    for slug in &slugs {
        let slug_ts: f64 = slug.rsplit('-').next().unwrap().parse().unwrap_or(0.0);

        // Reset session counters
        halt_rev.reset_if_new_session(slug_ts);
        halt_hp.reset_if_new_session(slug_ts);

        // Determine which strategies are halted
        let halted_rev = halt_rev.is_halted();
        let halted_hp = halt_hp.is_halted();

        // Skip if all strategies are halted
        let any_active = params.strategies.iter().any(|name| match name.as_str() {
            "reversal" => !halted_rev,
            "high_prob" => !halted_hp,
            _ => true,
        });
        if !any_active {
            continue;
        }

        let b_cycle = b_by_slug.get(slug).map(|v| v.as_slice()).unwrap_or(&[]);
        let p_cycle = p_by_slug.get(slug).map(|v| v.as_slice()).unwrap_or(&[]);

        let trades = replay_cycle(slug, b_cycle, p_cycle, &mut machines, halted_rev, halted_hp);

        // Update halt counters AFTER the cycle
        for rec in &trades {
            halt_rev.record_trade(rec, "reversal");
            halt_hp.record_trade(rec, "high_prob");
        }

        all_trades.extend(trades);
    }

    all_trades
}

/// Load and filter price data for an asset+date from prices_dir.
/// Returns (binance_rows, poly_rows).
pub fn load_price_data(
    asset: &str,
    date: &str,
    prices_dir: &str,
) -> Result<(Vec<BinanceRow>, Vec<PolyRow>)> {
    let dir = prices_dir.trim_end_matches('/');
    // Try date-specific files first
    let b_dated = format!("{dir}/{asset}_binance_{date}.parquet");
    let p_dated = format!("{dir}/{asset}_poly_{date}.parquet");
    if Path::new(&b_dated).exists() && Path::new(&p_dated).exists() {
        return Ok((load_binance(&b_dated)?, load_poly(&p_dated)?));
    }
    // Fall back to merged files with date filter
    let b_path = format!("{dir}/{asset}_binance.parquet");
    let p_path = format!("{dir}/{asset}_poly.parquet");
    let b_rows = load_binance(&b_path)?;
    let p_rows = load_poly(&p_path)?;
    filter_by_date(b_rows, p_rows, date)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Golden parity test: Rust backtest on BTC 2026-06-20 must match Python bt1,
    /// plus one additional trade from the poly-triggered-entry change
    /// (trader/doc/latency_2026-07-04.md §8, `Machine::try_enter` called from
    /// both `on_poly` and `on_binance`).
    ///
    /// Python result (no-halt): 1 trade, reversal DOWN, entry=0.845, unwind exit=0.875, pnl=0.0355
    ///
    /// The 2nd trade (cycle btc-updown-5m-1781891400) is new: poly's `up` price
    /// spiked 0.145 -> 0.605 in under half a second (ts 1781891587.0 -> .5) while
    /// Binance ticks in that window land roughly once per second. The old
    /// on-Binance-only-triggered entry check could only evaluate the entry
    /// condition at those once-per-second Binance moments, using whatever poly
    /// price happened to be cached then, and missed this transient crossing
    /// entirely. The new design checks on the poly tick itself, using the
    /// already-cached Binance delta — catching a real, briefly-true entry
    /// condition the old cadence-gated design couldn't see. Verified against the
    /// raw parquet ticks around this timestamp before accepting this as correct.
    #[test]
    fn btc_20260620_golden() {
        let prices_dir = "/home/kev/apps/btc_5mins/prices";
        if !Path::new(&format!("{prices_dir}/BTC_binance.parquet")).exists() {
            eprintln!("SKIP: price data not found at {prices_dir}");
            return;
        }

        // BTC's resolved params from strategy_20260703.toml — the config in effect
        // when this golden trace (and the poly-tick-entry fix it documents) was
        // captured and verified, 2026-07-04. Hardcoded rather than `load_latest`
        // deliberately: this test pins one specific historical trace, and a later
        // strategy recalibration (e.g. strategy_20260705.toml's higher BTC
        // `reversal` threshold) changes which trades fire on this same price data,
        // which would silently invalidate the very scenario this test documents
        // without this test itself ever noticing why.
        let mut params = crate::config::AssetParams {
            asset: "BTC".to_string(),
            strategies: vec!["reversal".to_string()],
            enter_when_time_left: 20.0,
            no_enter_when_time_left: 10.0,
            reversal: 0.60,
            reversal_low_threshold: 0.20,
            reversal_start_time: 120.0,
            price_high_rev: 0.9,
            delta_pct_rev: 0.0008,
            sl_reversal: 0.0,
            unwind_pnl_rev: 0.03,
            sl_pnl_rev: 0.20,
            unwind_time_rev: 0.0,
            price_low: 0.80,
            price_high: 0.93,
            delta_pct_hp: 0.0004,
            sl_high_prob: 0.49,
            unwind_pnl_hp: 0.05,
            sl_pnl_hp: 0.25,
            unwind_time_hp: 0.0,
            halt_rev: 2,
            halt_prob: 2,
            halt_reset_hour_rev: 2,
            halt_reset_hour_hp: 8,
            max_buy_price: 0.95,
            spread_premium_limit: 1.05,
            spread_discount_limit: 0.95,
            max_price_age_secs: 2.0,
            trade_size_usdc: 1.0,
        };
        // Match Python --no-halt
        params.halt_rev = 0;
        params.halt_prob = 0;

        let (b_rows, p_rows) =
            load_price_data("BTC", "2026-06-20", prices_dir).expect("load price data");

        let trades = run_backtest(&params, b_rows, p_rows);

        assert_eq!(
            trades.len(),
            2,
            "expected 2 trades, got {}: {:?}",
            trades.len(),
            trades
        );

        let t0 = &trades[0];
        assert_eq!(t0.slug, "btc-updown-5m-1781886600", "slug mismatch");
        assert_eq!(t0.strategy, "reversal");
        assert_eq!(t0.side, crate::types::Side::Down);
        assert!(
            (t0.token_price - 0.845).abs() < 1e-9,
            "entry token_price: got {}, want 0.845",
            t0.token_price
        );
        assert!(
            (t0.exit_price - 0.875).abs() < 1e-9,
            "exit_price: got {}, want 0.875",
            t0.exit_price
        );
        assert_eq!(t0.outcome, crate::types::Outcome::Unwind);
        assert!(
            (t0.pnl - 0.0355).abs() < 1e-4,
            "pnl: got {}, want 0.0355",
            t0.pnl
        );

        let t1 = &trades[1];
        assert_eq!(t1.slug, "btc-updown-5m-1781891400", "slug mismatch");
        assert_eq!(t1.strategy, "reversal");
        assert_eq!(t1.side, crate::types::Side::Up);
        assert!(
            (t1.token_price - 0.605).abs() < 1e-9,
            "entry token_price: got {}, want 0.605",
            t1.token_price
        );
        assert!(
            (t1.exit_price - 0.635).abs() < 1e-9,
            "exit_price: got {}, want 0.635",
            t1.exit_price
        );
        assert_eq!(t1.outcome, crate::types::Outcome::Unwind);
        assert!(
            (t1.pnl - 0.0496).abs() < 1e-4,
            "pnl: got {}, want 0.0496",
            t1.pnl
        );
    }

    #[test]
    fn hkt_session_boundary() {
        // 2026-06-20 01:00 HKT. With reset_hour=2:
        //   session = (01:00 HKT - 2h).date() = (2026-06-19 23:00 HKT).date() = 2026-06-19
        let hkt_offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let ts = hkt_offset
            .with_ymd_and_hms(2026, 6, 20, 1, 0, 0)
            .unwrap()
            .timestamp() as f64;
        let s = hkt_session(ts, 2);
        assert_eq!(s, NaiveDate::from_ymd_opt(2026, 6, 19).unwrap());

        // 2026-06-20 03:00 HKT. With reset_hour=2:
        //   session = (03:00 - 2h).date() = (01:00 HKT) = 2026-06-20
        let ts2 = hkt_offset
            .with_ymd_and_hms(2026, 6, 20, 3, 0, 0)
            .unwrap()
            .timestamp() as f64;
        let s2 = hkt_session(ts2, 2);
        assert_eq!(s2, NaiveDate::from_ymd_opt(2026, 6, 20).unwrap());
    }

    /// Minimal `TradeRecord` for `HaltTracker` tests — only `strategy` and
    /// `outcome` matter to `record_trade`, the rest are dummy values.
    fn halt_test_record(strategy: &'static str, outcome: crate::types::Outcome) -> TradeRecord {
        TradeRecord {
            slug: "test-slug".to_string(),
            cycle_start: 0.0,
            strategy,
            side: crate::types::Side::Down,
            entry_ts: 0.0,
            token_price: 0.5,
            exit_price: 0.5,
            outcome,
            pnl: 0.0,
            exit_attempts: 0,
            exit_last_error: None,
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
            exit_signal_latency_ms: 0.0,
            exit_process_latency_ms: 0.0,
        }
    }

    /// `record_trade` must return `true` on exactly the loss that crosses the
    /// threshold — not on losses before it, and not on further losses once
    /// already halted (an already-open position resolving as a loss after the
    /// halt engaged must not re-signal `Action::HaltEngaged` on every worker
    /// call site that shares this tracker).
    #[test]
    fn halt_tracker_record_trade_signals_only_on_the_crossing_loss() {
        let mut h = HaltTracker::new(2, 2);
        let loss = halt_test_record("reversal", crate::types::Outcome::Loss);

        assert!(
            !h.record_trade(&loss, "reversal"),
            "1st loss must not cross halt_max=2 yet"
        );
        assert!(!h.is_halted());

        assert!(
            h.record_trade(&loss, "reversal"),
            "2nd loss must cross halt_max=2"
        );
        assert!(h.is_halted());

        assert!(
            !h.record_trade(&loss, "reversal"),
            "3rd loss while already halted must not re-signal"
        );
        assert!(h.is_halted());
    }

    /// Non-loss outcomes and trades from a different strategy must never
    /// count toward the streak or signal a halt.
    #[test]
    fn halt_tracker_record_trade_ignores_non_loss_and_other_strategy() {
        let mut h = HaltTracker::new(1, 2); // max=1: a single qualifying loss halts immediately

        assert!(!h.record_trade(
            &halt_test_record("reversal", crate::types::Outcome::Win),
            "reversal"
        ));
        assert!(!h.record_trade(
            &halt_test_record("reversal", crate::types::Outcome::Unwind),
            "reversal"
        ));
        assert!(!h.is_halted(), "wins/unwinds must never halt");

        // A Timeout (unwind_time force-exit) is excluded from the halt loss-streak
        // regardless of pnl sign — matches the backtest's "cum_losses NOT
        // incremented" TIMEOUT semantics. Try both a losing and a winning-pnl
        // timeout record to confirm the exclusion isn't accidentally pnl-gated.
        let mut losing_timeout = halt_test_record("reversal", crate::types::Outcome::Timeout);
        losing_timeout.pnl = -0.5;
        assert!(!h.record_trade(&losing_timeout, "reversal"));
        let mut winning_timeout = halt_test_record("reversal", crate::types::Outcome::Timeout);
        winning_timeout.pnl = 0.5;
        assert!(!h.record_trade(&winning_timeout, "reversal"));
        assert!(!h.is_halted(), "timeout exits must never halt, win or lose");

        assert!(
            !h.record_trade(
                &halt_test_record("high_prob", crate::types::Outcome::Loss),
                "reversal"
            ),
            "a loss from a different strategy must be ignored by this tracker"
        );
        assert!(!h.is_halted());

        assert!(h.record_trade(
            &halt_test_record("reversal", crate::types::Outcome::Loss),
            "reversal"
        ));
        assert!(h.is_halted());
    }

    /// `reset_if_new_session` must only report `true` when it actually clears
    /// an *active* halt — not on every session rollover regardless of state
    /// (that would spam a Telegram notification every day at
    /// `halt_reset_hour_rev`/`halt_reset_hour_hp` even when nothing happened).
    #[test]
    fn halt_tracker_reset_signals_only_when_clearing_an_active_halt() {
        let mut h = HaltTracker::new(1, 2);

        // First session rollover ever, nothing halted -> silent.
        assert!(!h.reset_if_new_session(1_000.0));
        assert!(!h.is_halted());

        // Trip the halt.
        let loss = halt_test_record("reversal", crate::types::Outcome::Loss);
        assert!(h.record_trade(&loss, "reversal"));
        assert!(h.is_halted());

        // Same session (no rollover) -> no-op, no signal, halt persists.
        assert!(!h.reset_if_new_session(1_100.0));
        assert!(h.is_halted());

        // New session, halt was active -> clears it, signals true.
        assert!(h.reset_if_new_session(1_000.0 + 100_000.0));
        assert!(!h.is_halted());

        // Another new session, nothing left to clear -> silent again.
        assert!(!h.reset_if_new_session(1_000.0 + 200_000.0));
    }

    #[test]
    fn merge_ticks_binance_first_at_equal_ts() {
        let b = vec![BinanceRow {
            ts: 1000.0,
            price: 50000.0,
            slug: "s".into(),
        }];
        let p = vec![PolyRow {
            ts: 1000.0,
            up: 0.6,
            dn: 0.4,
            slug: "s".into(),
        }];
        let merged = merge_ticks(&b, &p);
        assert_eq!(merged.len(), 2);
        assert!(
            matches!(merged[0], MergedTick::Binance(_)),
            "binance must be first at equal ts"
        );
        assert!(matches!(merged[1], MergedTick::Poly(_)));
    }
}
