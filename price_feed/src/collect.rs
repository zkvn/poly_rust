use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use arrow::array::{
    ArrayRef, Float32Builder, Float64Array, Float64Builder, ListBuilder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Duration as ChronoDuration, FixedOffset, Utc};
use futures::StreamExt as _;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::clob::ws::types::response::BookUpdate;
use polymarket_client_sdk_v2::types::U256;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use crate::reconcile;

// ── Time helpers ─────────────────────────────────────────────────────────────

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn current_slot_for(interval: u64) -> u64 {
    (now_secs() / interval) * interval
}

/// Formats an optional exchange-side event timestamp (ms since epoch, `<= 0`
/// meaning the exchange didn't supply one — the existing convention this
/// file already uses for `server_ts_ms`) as a NATS JSON field value: either
/// the timestamp in seconds (matching `ts`'s own units/precision) or `null`.
fn server_ts_json(server_ts_ms: i64) -> String {
    if server_ts_ms > 0 {
        format!("{:.3}", server_ts_ms as f64 / 1000.0)
    } else {
        "null".to_string()
    }
}

/// Build the `price.binance.*` NATS payload. `ts` is the sample's own real
/// receive timestamp (`received_at_ms`, ms since epoch) — deliberately *not*
/// the 250ms sampler tick's fire time, which is quantized to a 0.25s grid for
/// parquet bucketing and can round up to 125ms into the future of when the
/// price was actually received, producing a negative signal_latency downstream
/// in trader/src/bin/live.rs. `server_ts` is Binance's own event timestamp
/// (the `E` field) — lets the trader compute real exchange network latency
/// (received_at_ms - server_ts) instead of just the local NATS+processing hop.
fn binance_nats_payload(received_at_ms: i64, price: f64, server_ts_ms: i64) -> String {
    let ts = received_at_ms as f64 / 1000.0;
    let server_ts = server_ts_json(server_ts_ms);
    format!(r#"{{"ts":{ts:.3},"price":{price:.6},"server_ts":{server_ts}}}"#)
}

/// Build the `price.poly.*` NATS payload. `server_ts` is the CLOB's own event
/// timestamp for this bba/price-change update — see `binance_nats_payload`'s
/// doc comment for why this (not `ts`) is what measures real exchange latency.
fn poly_nats_payload(received_at_ms: i64, up_mid: f64, server_ts_ms: i64) -> String {
    let ts = received_at_ms as f64 / 1000.0;
    let dn = 1.0 - up_mid;
    let server_ts = server_ts_json(server_ts_ms);
    format!(r#"{{"ts":{ts:.3},"up":{up_mid:.6},"dn":{dn:.6},"server_ts":{server_ts}}}"#)
}

fn make_slug(asset: &str, slot: u64, suffix: &str) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), suffix, slot)
}

// One sealed file per (asset, type, hour) instead of per day — see `ParquetBuf::seal_and_rename`.
fn hkt_hour_string() -> String {
    Utc::now()
        .with_timezone(&hkt())
        .format("%Y-%m-%d_%H")
        .to_string()
}

// ── Shared per-asset state ────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct AssetState {
    latest_book: Option<BookUpdate>,
    book_server_ts_ms: i64,   // BookUpdate.timestamp (server-side ms)
    book_received_at_ms: i64, // client-side ms when the WS message was received
    latest_trade: f64,
    slug: String,
    // Real top-of-book for the UP token from the best_bid_ask / price_change feed.
    // The `book` (orderbook snapshot) channel for these updown markets only ever reports
    // the outermost ticks (~0.01/0.99), pinning its midpoint at 0.5 — so the live price
    // must come from here instead. None until the first such message arrives.
    latest_bba: Option<BbaSample>,
}

/// Real best bid/ask for one token, sourced from `best_bid_ask` (custom-feature, low
/// latency) or `price_change` (always-on) — whichever delivers first.
#[derive(Clone, Copy)]
struct BbaSample {
    best_bid: f64,
    best_ask: f64,
    server_ts_ms: i64,
    received_at_ms: i64,
}

// ── Gamma asset discovery ─────────────────────────────────────────────────────

async fn discover_assets(http: &reqwest::Client) -> Result<Vec<String>> {
    let start_min = (Utc::now() - ChronoDuration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ");
    let url = format!(
        "https://gamma-api.polymarket.com/events?tag_id=102127&active=true&closed=false&start_date_min={start_min}&limit=100"
    );
    let resp: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .context("discover request")?
        .json()
        .await
        .context("discover json")?;

    let mut assets = std::collections::BTreeSet::new();
    if let Some(arr) = resp.as_array() {
        for event in arr {
            if let Some(slug) = event["slug"].as_str()
                && let Some(pos) = slug.find("-updown-")
            {
                assets.insert(slug[..pos].to_uppercase());
            }
        }
    }
    if assets.is_empty() {
        anyhow::bail!("discovery returned no assets — Gamma API may be down");
    }
    eprintln!(
        "discovered {} assets: {}",
        assets.len(),
        assets.iter().cloned().collect::<Vec<_>>().join(", ")
    );
    Ok(assets.into_iter().collect())
}

// ── Gamma meta fetch ─────────────────────────────────────────────────────────

async fn fetch_meta(
    http: &reqwest::Client,
    asset: &str,
    slot: u64,
    suffix: &str,
) -> Result<(U256, U256, String)> {
    let slug = make_slug(asset, slot, suffix);
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .context("gamma request")?
        .json()
        .await
        .context("gamma json")?;

    let event = resp
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no event for {slug}"))?;
    let market = event["markets"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no market for {slug}"))?;

    let token_ids: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap_or("[]"))?;
    let outcomes: Vec<String> = serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

    let find = |target: &str| -> Result<U256> {
        outcomes
            .iter()
            .zip(token_ids.iter())
            .find(|(o, _)| o.to_lowercase() == target)
            .map(|(_, tid)| U256::from_str(tid).with_context(|| format!("parse {target} id")))
            .ok_or_else(|| anyhow::anyhow!("no {} token in {}", target, slug))?
    };

    let up_id = find("up")?;
    let dn_id = find("down")?;
    Ok((up_id, dn_id, slug))
}

// ── Arrow schemas ─────────────────────────────────────────────────────────────

fn poly_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Float64, false),
        Field::new("up", DataType::Float64, false),
        Field::new("dn", DataType::Float64, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("server_ts", DataType::Float64, true), // ms; null for carried rows pre-schema
        Field::new("latency_ms", DataType::Float64, true), // receive_ms - server_ts; null for old rows
    ])
}

fn binance_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Float64, false),
        Field::new("binance", DataType::Float64, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("server_ts", DataType::Float64, true), // Binance `E` field (ms)
        Field::new("latency_ms", DataType::Float64, true), // receive_ms - server_ts
    ])
}

fn book_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Float64, false),
        Field::new("asset", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("best_bid", DataType::Float64, false),
        Field::new("best_ask", DataType::Float64, false),
        Field::new("last_trade", DataType::Float64, false),
        Field::new(
            "bid_prices",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
        Field::new(
            "bid_sizes",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
        Field::new(
            "ask_prices",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
        Field::new(
            "ask_sizes",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
        Field::new("server_ts", DataType::Float64, true),
        Field::new("latency_ms", DataType::Float64, true),
    ])
}

// ── Parquet writer ────────────────────────────────────────────────────────────

struct ParquetBuf {
    path: PathBuf,
    writer: ArrowWriter<fs::File>,
    rows_since_flush: usize,
    last_flush: std::time::Instant,
}

impl ParquetBuf {
    // `schema` is only needed to build the `ArrowWriter` below — it's baked into
    // `writer` from here on, so it isn't kept as a struct field (see
    // `README.md`'s "ParquetBuf.schema field removed" note).
    fn open(path: PathBuf, schema: Schema) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("open {path:?}"))?;
        let writer =
            ArrowWriter::try_new(file, Arc::new(schema), Some(props)).context("ArrowWriter")?;
        Ok(Self {
            path,
            writer,
            rows_since_flush: 0,
            last_flush: std::time::Instant::now(),
        })
    }

    // Opens a fresh writer at `tmp_path` for the current hour, carrying forward rows
    // from whichever of `tmp_path` (a crash left it mid-write) or `final_path` (a
    // previous process already gracefully sealed this hour, e.g. a mid-hour restart
    // during a deploy) exists. Without this, a restart within the same hour would
    // start an empty .tmp that, on the next seal, renames over and destroys the
    // already-sealed final file from before the restart.
    fn open_for_hour(tmp_path: PathBuf, final_path: &Path, schema: Schema) -> Result<Self> {
        let carry_source = if tmp_path.exists() {
            Some(tmp_path.clone())
        } else if final_path.exists() {
            Some(final_path.to_path_buf())
        } else {
            None
        };
        let carry = carry_source
            .as_ref()
            .and_then(|p| Self::try_carry(p, &schema));
        let mut buf = Self::open(tmp_path, schema)?;
        if let Some(batches) = carry {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "  carry: loaded {rows} rows from {:?}",
                carry_source.unwrap()
            );
            for batch in batches {
                buf.writer.write(&batch).context("carry write")?;
            }
        }
        Ok(buf)
    }

    fn try_carry(path: &PathBuf, expected: &Schema) -> Option<Vec<RecordBatch>> {
        let file = fs::File::open(path).ok()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
        let old_schema = builder.schema().clone();

        // All old fields must exist by name in the new schema.
        let compatible = old_schema
            .fields()
            .iter()
            .all(|f| expected.field_with_name(f.name()).is_ok());
        if !compatible {
            eprintln!("  carry: incompatible schema (unknown columns), discarding {path:?}");
            return None;
        }

        let needs_adapt = old_schema.fields().len() < expected.fields().len();
        let batches: Vec<RecordBatch> = builder.build().ok()?.flatten().collect();

        if needs_adapt {
            eprintln!(
                "  carry: schema evolving {} → {} cols, padding nulls for {path:?}",
                old_schema.fields().len(),
                expected.fields().len()
            );
            batches
                .into_iter()
                .map(|b| adapt_to_schema(b, expected))
                .collect::<Result<_>>()
                .ok()
        } else {
            Some(batches)
        }
    }

    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.rows_since_flush += batch.num_rows();
        self.writer.write(&batch).context("parquet write")?;
        if self.rows_since_flush >= 500 || self.last_flush.elapsed() >= Duration::from_secs(10) {
            self.writer.flush().context("flush")?;
            self.rows_since_flush = 0;
            self.last_flush = std::time::Instant::now();
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.writer
            .close()
            .with_context(|| format!("close {:?}", self.path))?;
        Ok(())
    }

    // Close the writer (writes the PAR1 footer to the .tmp path) then atomically
    // rename it to `final_path`. O(1) — no re-read/re-encode of existing rows,
    // unlike the old carry-based reseal. Called on every hour boundary and on
    // graceful shutdown, so files on disk are always either a complete sealed
    // `.parquet` or an in-progress `.parquet.tmp` (excluded from rsync).
    fn seal_and_rename(self, final_path: &Path) -> Result<()> {
        let tmp_path = self.path.clone();
        self.writer
            .close()
            .with_context(|| format!("seal {tmp_path:?}"))?;
        fs::rename(&tmp_path, final_path)
            .with_context(|| format!("rename {tmp_path:?} -> {final_path:?}"))?;
        Ok(())
    }
}

// Scan `raw_dir` for `.parquet.tmp` files left behind by a crash in a now-stale hour
// (i.e. not matching `current_hour_key`) and seal them: read whatever rows they hold
// via the existing carry path, write a fresh properly-closed file at the final name,
// then remove the orphaned .tmp. Bounded by at most one hour's worth of rows — unlike
// the old full-day reseal, this only runs once at startup, not every hour.
fn seal_orphaned_tmp(raw_dir: &Path, current_hour_key: &str) -> Result<()> {
    let entries = match fs::read_dir(raw_dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".parquet.tmp") || name.contains(current_hour_key) {
            continue;
        }
        let schema = if name.contains("_poly_") {
            poly_schema()
        } else if name.contains("_book_") {
            book_schema()
        } else if name.contains("_binance_") {
            binance_schema()
        } else {
            continue;
        };
        let final_name = &name[..name.len() - 4]; // strip ".tmp"
        let final_path = raw_dir.join(final_name);
        if final_path.exists() {
            eprintln!(
                "startup: {final_name} already sealed, leaving orphaned {name} for manual review"
            );
            continue;
        }
        eprintln!("startup: recovering orphaned {name} -> {final_name}");
        let Some(batches) = ParquetBuf::try_carry(&path, &schema) else {
            eprintln!("  [{name}] no recoverable rows, leaving orphaned tmp in place");
            continue;
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        match ParquetBuf::open(final_path.clone(), schema) {
            Ok(mut buf) => {
                let mut ok = true;
                for batch in batches {
                    if let Err(e) = buf.writer.write(&batch) {
                        eprintln!("  [{name}] write failed: {e:#}");
                        ok = false;
                        break;
                    }
                }
                if ok {
                    match buf.finish() {
                        Ok(()) => {
                            let _ = fs::remove_file(&path);
                            eprintln!("  [{name}] recovered {rows} rows -> {final_name}");
                        }
                        Err(e) => eprintln!("  [{name}] close failed: {e:#}"),
                    }
                }
            }
            Err(e) => eprintln!("  [{name}] open failed: {e:#}"),
        }
    }
    Ok(())
}

// Pad an old RecordBatch to a wider schema by filling missing Float64 columns with nulls.
fn adapt_to_schema(batch: RecordBatch, schema: &Schema) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let cols: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| {
            if let Ok(idx) = batch.schema().index_of(f.name()) {
                Ok(batch.column(idx).clone())
            } else {
                let mut b = Float64Builder::with_capacity(n);
                for _ in 0..n {
                    b.append_null();
                }
                Ok(Arc::new(b.finish()) as ArrayRef)
            }
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(Arc::new(schema.clone()), cols).context("adapt schema")
}

// ── Decimal helper ────────────────────────────────────────────────────────────

fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
}

// ── Nullable Float64 array helper ─────────────────────────────────────────────

fn opt_f64_col(v: Option<f64>) -> ArrayRef {
    let mut b = Float64Builder::new();
    match v {
        Some(x) => b.append_value(x),
        None => b.append_null(),
    }
    Arc::new(b.finish()) as ArrayRef
}

// ── Poly + book row builders ──────────────────────────────────────────────────

fn poly_row(
    schema: &Schema,
    ts: f64,
    up: f64,
    slug: &str,
    server_ts: Option<f64>,
    latency_ms: Option<f64>,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(Float64Array::from(vec![up])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0 - up])) as ArrayRef,
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
            opt_f64_col(server_ts),
            opt_f64_col(latency_ms),
        ],
    )
    .context("poly row")
}

fn binance_row(
    schema: &Schema,
    ts: f64,
    price: f64,
    slug: &str,
    server_ts: Option<f64>,
    latency_ms: Option<f64>,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(Float64Array::from(vec![price])) as ArrayRef,
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
            opt_f64_col(server_ts),
            opt_f64_col(latency_ms),
        ],
    )
    .context("binance row")
}

#[allow(clippy::too_many_arguments)]
fn book_row(
    schema: &Schema,
    ts: f64,
    asset: &str,
    slug: &str,
    side: &str,
    best_bid: f64,
    best_ask: f64,
    last_trade: f64,
    bid_prices: &[f32],
    bid_sizes: &[f32],
    ask_prices: &[f32],
    ask_sizes: &[f32],
    server_ts: Option<f64>,
    latency_ms: Option<f64>,
) -> Result<RecordBatch> {
    fn list_col(data: &[f32]) -> ArrayRef {
        let mut b = ListBuilder::new(Float32Builder::new());
        b.values().append_slice(data);
        b.append(true);
        Arc::new(b.finish()) as ArrayRef
    }
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(StringArray::from(vec![asset])) as ArrayRef,
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
            Arc::new(StringArray::from(vec![side])) as ArrayRef,
            Arc::new(Float64Array::from(vec![best_bid])) as ArrayRef,
            Arc::new(Float64Array::from(vec![best_ask])) as ArrayRef,
            Arc::new(Float64Array::from(vec![last_trade])) as ArrayRef,
            list_col(bid_prices),
            list_col(bid_sizes),
            list_col(ask_prices),
            list_col(ask_sizes),
            opt_f64_col(server_ts),
            opt_f64_col(latency_ms),
        ],
    )
    .context("book row")
}

// ── Per-asset poly+book writer pair ──────────────────────────────────────────

struct AssetWriters {
    asset: String,
    poly: ParquetBuf,
    book: ParquetBuf,
    poly_schema: Schema,
    book_schema: Schema,
    hour_key: String, // "2026-07-02_14" — sealed file is {asset}_{type}_{hour_key}.parquet
    raw_dir: PathBuf,
}

impl AssetWriters {
    fn new(asset: &str, raw_dir: &Path) -> Result<Self> {
        let hour_key = hkt_hour_string();
        let ps = poly_schema();
        let bs = book_schema();
        let poly_tmp = raw_dir.join(format!("{asset}_poly_{hour_key}.parquet.tmp"));
        let book_tmp = raw_dir.join(format!("{asset}_book_{hour_key}.parquet.tmp"));
        let poly_final = raw_dir.join(format!("{asset}_poly_{hour_key}.parquet"));
        let book_final = raw_dir.join(format!("{asset}_book_{hour_key}.parquet"));
        eprintln!("[{asset}] opening poly={poly_tmp:?} book={book_tmp:?}");
        Ok(Self {
            asset: asset.to_string(),
            poly: ParquetBuf::open_for_hour(poly_tmp, &poly_final, ps.clone())?,
            book: ParquetBuf::open_for_hour(book_tmp, &book_final, bs.clone())?,
            poly_schema: ps,
            book_schema: bs,
            hour_key,
            raw_dir: raw_dir.to_path_buf(),
        })
    }

    // Seal both files when the wall-clock hour advances: close (writes the PAR1
    // footer) + atomic rename to the final name, then open a fresh .tmp for the
    // new hour. O(1) — no re-read of prior rows, unlike the old carry-based
    // reseal that reprocessed the whole day every hour.
    fn seal_if_hour_changed(&mut self) -> Result<()> {
        let current_hour = hkt_hour_string();
        if current_hour == self.hour_key {
            return Ok(());
        }
        eprintln!(
            "[{}] sealing hour {} → {}",
            self.asset, self.hour_key, current_hour
        );
        let poly_final = self
            .raw_dir
            .join(format!("{}_poly_{}.parquet", self.asset, self.hour_key));
        let book_final = self
            .raw_dir
            .join(format!("{}_book_{}.parquet", self.asset, self.hour_key));
        let poly_tmp = self
            .raw_dir
            .join(format!("{}_poly_{current_hour}.parquet.tmp", self.asset));
        let book_tmp = self
            .raw_dir
            .join(format!("{}_book_{current_hour}.parquet.tmp", self.asset));

        let old_poly = std::mem::replace(
            &mut self.poly,
            ParquetBuf::open(poly_tmp, self.poly_schema.clone())?,
        );
        old_poly.seal_and_rename(&poly_final)?;
        let old_book = std::mem::replace(
            &mut self.book,
            ParquetBuf::open(book_tmp, self.book_schema.clone())?,
        );
        old_book.seal_and_rename(&book_final)?;
        self.hour_key = current_hour;
        Ok(())
    }

    // 7 real params (+ self): private, 3 call sites (5m/15m/4hr writers, collect.rs::run),
    // each independently meaningful (raw tick data + both timestamp sources + the optional
    // bba override) — a wrapper struct would add a layer without a real clarity gain here,
    // matching the same call already made for Worker::common in ../trader/src/worker.rs.
    #[allow(clippy::too_many_arguments)]
    fn write_sample(
        &mut self,
        ts: f64,
        book: &BookUpdate,
        last_trade: f64,
        slug: &str,
        server_ts_ms: i64,
        received_at_ms: i64,
        bba: Option<BbaSample>,
    ) -> Result<()> {
        // Price + timing come from the real best_bid_ask/price_change feed when available;
        // the orderbook channel only supplies the depth ladders below (its top-of-book is
        // pinned at the outer ticks for these markets). Fall back to book-derived values
        // until the first bba quote arrives, so we never gap.
        let (best_bid, best_ask, src_server_ts_ms, src_received_at_ms) = match bba {
            Some(b) => (b.best_bid, b.best_ask, b.server_ts_ms, b.received_at_ms),
            None => {
                let bb = book.bids.first().map(|l| d2f(&l.price)).unwrap_or(0.0);
                let ba = book.asks.first().map(|l| d2f(&l.price)).unwrap_or(0.0);
                (bb, ba, server_ts_ms, received_at_ms)
            }
        };
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return Ok(());
        }
        let up_mid = (best_bid + best_ask) / 2.0;

        let server_ts = if src_server_ts_ms > 0 {
            Some(src_server_ts_ms as f64)
        } else {
            None
        };
        let latency_ms = if src_received_at_ms > 0 && src_server_ts_ms > 0 {
            Some((src_received_at_ms - src_server_ts_ms) as f64)
        } else {
            None
        };

        self.poly.write(poly_row(
            &self.poly_schema,
            ts,
            up_mid,
            slug,
            server_ts,
            latency_ms,
        )?)?;

        // Build depth ladders reversed: worst→best (matches Python's REST order)
        let mut bid_p: Vec<f32> = book.bids.iter().map(|l| d2f(&l.price) as f32).collect();
        let mut bid_s: Vec<f32> = book.bids.iter().map(|l| d2f(&l.size) as f32).collect();
        let mut ask_p: Vec<f32> = book.asks.iter().map(|l| d2f(&l.price) as f32).collect();
        let mut ask_s: Vec<f32> = book.asks.iter().map(|l| d2f(&l.size) as f32).collect();
        bid_p.reverse();
        bid_s.reverse();
        ask_p.reverse();
        ask_s.reverse();

        // UP row
        self.book.write(book_row(
            &self.book_schema,
            ts,
            &self.asset,
            slug,
            "UP",
            best_bid,
            best_ask,
            last_trade,
            &bid_p,
            &bid_s,
            &ask_p,
            &ask_s,
            server_ts,
            latency_ms,
        )?)?;

        // DN row (1-complement of UP)
        let dn_bid_p: Vec<f32> = ask_p.iter().map(|p| 1.0 - p).collect();
        let dn_ask_p: Vec<f32> = bid_p.iter().map(|p| 1.0 - p).collect();
        self.book.write(book_row(
            &self.book_schema,
            ts,
            &self.asset,
            slug,
            "DN",
            1.0 - best_ask,
            1.0 - best_bid,
            last_trade,
            &dn_bid_p,
            &ask_s,
            &dn_ask_p,
            &bid_s,
            server_ts,
            latency_ms,
        )?)?;

        Ok(())
    }

    // Seal the in-progress hour on shutdown too — otherwise the final partial
    // hour stays as a footerless .tmp forever, never becoming a real .parquet.
    fn finish(self) -> Result<()> {
        let poly_final = self
            .raw_dir
            .join(format!("{}_poly_{}.parquet", self.asset, self.hour_key));
        let book_final = self
            .raw_dir
            .join(format!("{}_book_{}.parquet", self.asset, self.hour_key));
        self.poly.seal_and_rename(&poly_final)?;
        self.book.seal_and_rename(&book_final)
    }
}

// ── Binance state + writer (asset-level, period-independent — one per asset,
// shared across the 5m/15m/4hr durations, matching Python's single
// prices/{ASSET}_binance.parquet) ────────────────────────────────────────────

#[derive(Clone, Copy, Default)]
struct BinanceState {
    price: f64,
    server_ts_ms: i64,   // Binance `E` field (event time, ms)
    received_at_ms: i64, // client-side ms when the WS message was received
}

struct BinanceWriters {
    asset: String,
    buf: ParquetBuf,
    schema: Schema,
    hour_key: String,
    raw_dir: PathBuf,
}

impl BinanceWriters {
    fn new(asset: &str, raw_dir: &Path) -> Result<Self> {
        let hour_key = hkt_hour_string();
        let schema = binance_schema();
        let tmp = raw_dir.join(format!("{asset}_binance_{hour_key}.parquet.tmp"));
        let final_path = raw_dir.join(format!("{asset}_binance_{hour_key}.parquet"));
        eprintln!("[{asset}] opening binance={tmp:?}");
        Ok(Self {
            asset: asset.to_string(),
            buf: ParquetBuf::open_for_hour(tmp, &final_path, schema.clone())?,
            schema,
            hour_key,
            raw_dir: raw_dir.to_path_buf(),
        })
    }

    fn seal_if_hour_changed(&mut self) -> Result<()> {
        let current_hour = hkt_hour_string();
        if current_hour == self.hour_key {
            return Ok(());
        }
        eprintln!(
            "[{}] sealing binance hour {} → {}",
            self.asset, self.hour_key, current_hour
        );
        let final_path = self
            .raw_dir
            .join(format!("{}_binance_{}.parquet", self.asset, self.hour_key));
        let tmp = self
            .raw_dir
            .join(format!("{}_binance_{current_hour}.parquet.tmp", self.asset));
        let old = std::mem::replace(&mut self.buf, ParquetBuf::open(tmp, self.schema.clone())?);
        old.seal_and_rename(&final_path)?;
        self.hour_key = current_hour;
        Ok(())
    }

    fn write_sample(
        &mut self,
        ts: f64,
        price: f64,
        slug: &str,
        server_ts_ms: i64,
        received_at_ms: i64,
    ) -> Result<()> {
        if price <= 0.0 {
            return Ok(());
        }
        let server_ts = if server_ts_ms > 0 {
            Some(server_ts_ms as f64)
        } else {
            None
        };
        let latency_ms = if received_at_ms > 0 && server_ts_ms > 0 {
            Some((received_at_ms - server_ts_ms) as f64)
        } else {
            None
        };
        self.buf.write(binance_row(
            &self.schema,
            ts,
            price,
            slug,
            server_ts,
            latency_ms,
        )?)
    }

    fn finish(self) -> Result<()> {
        let final_path = self
            .raw_dir
            .join(format!("{}_binance_{}.parquet", self.asset, self.hour_key));
        self.buf.seal_and_rename(&final_path)
    }
}

/// One WS connection to `wss://stream.binance.com:9443/ws/{symbol}@trade` per
/// asset. Public endpoint, no auth/subscribe handshake. Reconnects with a 2s
/// backoff on drop. Writes the latest trade price + Binance's own `E` (event
/// time) into the shared per-asset slot — the 250ms sampler (in `run()`) reads
/// it, so this task never blocks on file I/O.
fn spawn_binance_task(asset: String, idx: usize, state: Arc<Mutex<Vec<BinanceState>>>) {
    let symbol = format!("{}usdt", asset.to_lowercase());
    tokio::spawn(async move {
        let url = format!("wss://stream.binance.com:9443/ws/{symbol}@trade");
        loop {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => {
                    eprintln!("[{asset}] binance ws connected: {url}");
                    let (_write, mut read) = ws.split();
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(txt)) => {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                    let price = v["p"].as_str().and_then(|s| s.parse::<f64>().ok());
                                    if let Some(price) = price {
                                        let server_ts_ms = v["E"].as_i64().unwrap_or(0);
                                        let received_at_ms = now_ms();
                                        let mut st = state.lock().unwrap();
                                        if idx < st.len() {
                                            st[idx] = BinanceState {
                                                price,
                                                server_ts_ms,
                                                received_at_ms,
                                            };
                                        }
                                    }
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    eprintln!("[{asset}] binance ws closed, reconnecting…");
                }
                Err(e) => eprintln!("[{asset}] binance connect failed: {e:#}, retrying…"),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

// ── Task spawners ─────────────────────────────────────────────────────────────

fn spawn_meta_task(
    assets: Vec<String>,
    state: Arc<Mutex<Vec<AssetState>>>,
    slot_tx: watch::Sender<Vec<Option<(U256, U256, String)>>>,
    http: Arc<reqwest::Client>,
    slot_interval: u64,
    suffix: &'static str,
) {
    tokio::spawn(async move {
        let mut slots: Vec<Option<(u64, U256, U256, String)>> = vec![None; assets.len()];

        loop {
            let current = current_slot_for(slot_interval);
            let mut changed = false;

            for (i, asset) in assets.iter().enumerate() {
                let stale = slots[i]
                    .as_ref()
                    .map(|(s, ..)| *s != current)
                    .unwrap_or(true);

                if stale {
                    match fetch_meta(&http, asset, current, suffix).await {
                        Ok((up, dn, slug)) => {
                            eprintln!("[{asset}/{suffix}] slot {current} slug={slug}");
                            {
                                let mut st = state.lock().unwrap();
                                st[i].slug = slug.clone();
                            }
                            slots[i] = Some((current, up, dn, slug));
                            changed = true;
                        }
                        Err(e) => eprintln!("[{asset}/{suffix}] meta error: {e:#}"),
                    }
                }
            }

            if changed {
                let payload = slots
                    .iter()
                    .map(|s| s.as_ref().map(|(_, u, d, sl)| (*u, *d, sl.clone())))
                    .collect();
                let _ = slot_tx.send(payload);
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

fn spawn_book_task(
    clob: ClobWsClient,
    state: Arc<Mutex<Vec<AssetState>>>,
    mut slot_rx: watch::Receiver<Vec<Option<(U256, U256, String)>>>,
) {
    tokio::spawn(async move {
        let mut book_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            if slot_rx.changed().await.is_err() {
                break;
            }
            let tokens = slot_rx.borrow_and_update().clone();

            if let Some(h) = book_task.take() {
                h.abort();
            }

            let mut ids: Vec<U256> = Vec::new();
            let mut map: Vec<(U256, usize, bool)> = Vec::new();

            for (i, slot) in tokens.iter().enumerate() {
                if let Some((up, dn, _)) = slot {
                    ids.push(*up);
                    ids.push(*dn);
                    map.push((*up, i, true));
                    map.push((*dn, i, false));
                }
            }

            if ids.is_empty() {
                continue;
            }

            let clob = clob.clone();
            let state = Arc::clone(&state);
            book_task = Some(tokio::spawn(async move {
                loop {
                    match clob.subscribe_orderbook(ids.clone()) {
                        Ok(stream) => {
                            let mut s = Box::pin(stream);
                            while let Some(Ok(update)) = s.next().await {
                                if let Some(&(_, idx, is_up)) =
                                    map.iter().find(|(id, _, _)| *id == update.asset_id)
                                    && is_up
                                {
                                    let received_at_ms = now_ms();
                                    let server_ts_ms = update.timestamp;
                                    let mut st = state.lock().unwrap();
                                    if idx < st.len() {
                                        st[idx].book_server_ts_ms = server_ts_ms;
                                        st[idx].book_received_at_ms = received_at_ms;
                                        st[idx].latest_book = Some(update);
                                    }
                                }
                            }
                            eprintln!("book stream closed, reconnecting…");
                        }
                        Err(e) => eprintln!("subscribe_orderbook failed: {e:#}, retrying…"),
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
    });
}

// Real-price feed for the UP token: subscribes to best_bid_ask (custom feature, low
// latency) AND price_change (always-on, no custom-feature dependency) and merges them.
// Whichever delivers a quote updates `latest_bba`. This is the source the sampler uses
// for the poly midpoint and the book best_bid/best_ask — replacing the orderbook channel,
// whose top-of-book is stuck at the outer ticks for these markets (the 0.5 bug).
fn spawn_bba_task(
    clob: ClobWsClient,
    state: Arc<Mutex<Vec<AssetState>>>,
    mut slot_rx: watch::Receiver<Vec<Option<(U256, U256, String)>>>,
    nats: Option<async_nats::Client>,
    assets: Vec<String>,
) {
    tokio::spawn(async move {
        let mut task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            if slot_rx.changed().await.is_err() {
                break;
            }
            let tokens = slot_rx.borrow_and_update().clone();

            if let Some(h) = task.take() {
                h.abort();
            }

            // UP tokens only — the DN price is recorded as the 1-complement downstream.
            let mut up_ids: Vec<U256> = Vec::new();
            let mut map: Vec<(U256, usize)> = Vec::new();
            for (i, slot) in tokens.iter().enumerate() {
                if let Some((up, _dn, _)) = slot {
                    up_ids.push(*up);
                    map.push((*up, i));
                }
            }
            if up_ids.is_empty() {
                continue;
            }

            let clob = clob.clone();
            let state = Arc::clone(&state);
            let nats = nats.clone();
            let assets = assets.clone();
            task = Some(tokio::spawn(async move {
                loop {
                    match (
                        clob.subscribe_best_bid_ask(up_ids.clone()),
                        clob.subscribe_prices(up_ids.clone()),
                    ) {
                        (Ok(bba), Ok(pc)) => {
                            // Unify both feeds into (asset_id, best_bid, best_ask, server_ts_ms).
                            let bba_u = bba.filter_map(|r| async move {
                                r.ok().map(|m| {
                                    (m.asset_id, d2f(&m.best_bid), d2f(&m.best_ask), m.timestamp)
                                })
                            });
                            let pc_u = pc.flat_map(|r| {
                                let items: Vec<(U256, f64, f64, i64)> = match r {
                                    Ok(p) => {
                                        let ts = p.timestamp;
                                        p.price_changes
                                            .into_iter()
                                            .filter_map(move |e| match (e.best_bid, e.best_ask) {
                                                (Some(b), Some(a)) => {
                                                    Some((e.asset_id, d2f(&b), d2f(&a), ts))
                                                }
                                                _ => None,
                                            })
                                            .collect()
                                    }
                                    Err(_) => Vec::new(),
                                };
                                futures::stream::iter(items)
                            });

                            let mut merged =
                                futures::stream::select(Box::pin(bba_u), Box::pin(pc_u));
                            while let Some((asset_id, bid, ask, server_ts_ms)) = merged.next().await
                            {
                                if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0
                                {
                                    continue;
                                }
                                if let Some(&(_, idx)) = map.iter().find(|(id, _)| *id == asset_id)
                                {
                                    let received_at_ms = now_ms();
                                    // Release lock before any await.
                                    {
                                        let mut st = state.lock().unwrap();
                                        if idx < st.len() {
                                            st[idx].latest_bba = Some(BbaSample {
                                                best_bid: bid,
                                                best_ask: ask,
                                                server_ts_ms,
                                                received_at_ms,
                                            });
                                        }
                                    }
                                    if let Some(ref nc) = nats
                                        && idx < assets.len()
                                    {
                                        let up_mid = (bid + ask) / 2.0;
                                        let payload =
                                            poly_nats_payload(received_at_ms, up_mid, server_ts_ms);
                                        let subject = format!("price.poly.{}", assets[idx]);
                                        let _ =
                                            nc.publish(subject, payload.into_bytes().into()).await;
                                    }
                                }
                            }
                            eprintln!("bba/price stream closed, reconnecting…");
                        }
                        _ => eprintln!("subscribe best_bid_ask/prices failed, retrying…"),
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
    });
}

/// REST `/midpoint` ground-truth reconciliation for the 5m feed — see `reconcile.rs`'s doc
/// comment for the detection design and why a confirmed mismatch exits the process rather
/// than attempting a surgical per-asset unsubscribe. Polls every `poll_secs` (default
/// `reconcile::DEFAULT_POLL_SECS`, override via `--midpoint-poll-secs`); does nothing for an
/// asset that has no WS-cached sample yet (nothing to reconcile against).
fn spawn_reconcile_task(
    http: Arc<reqwest::Client>,
    state: Arc<Mutex<Vec<AssetState>>>,
    slot_rx: watch::Receiver<Vec<Option<(U256, U256, String)>>>,
    assets: Vec<String>,
    poll_secs: u64,
) {
    tokio::spawn(async move {
        let n = assets.len();
        let mut recon_state: Vec<reconcile::ReconcileState> =
            (0..n).map(|_| Default::default()).collect();
        let mut ticker = tokio::time::interval(Duration::from_secs(poll_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            let tokens = slot_rx.borrow().clone();

            for (i, slot) in tokens.iter().enumerate() {
                let Some((up_id, ..)) = slot else { continue };
                let cached_mid = {
                    let st = state.lock().unwrap();
                    st.get(i)
                        .and_then(|s| s.latest_bba)
                        .map(|b| (b.best_bid + b.best_ask) / 2.0)
                };
                let Some(cached_mid) = cached_mid else {
                    continue;
                };
                let asset_name = assets.get(i).cloned().unwrap_or_else(|| i.to_string());

                match reconcile::fetch_midpoint(&http, *up_id).await {
                    Ok(rest_mid) => {
                        if reconcile::check(&mut recon_state[i], cached_mid, rest_mid) {
                            eprintln!(
                                "[RECONCILE-STALE] {asset_name} rest_mid={rest_mid:.4} cached_mid={cached_mid:.4} diff={:.4} — confirmed via {} consecutive mismatches, exiting for systemd to restart",
                                (rest_mid - cached_mid).abs(),
                                reconcile::CONSECUTIVE_MISMATCHES_REQUIRED,
                            );
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("[{asset_name}] midpoint fetch failed: {e:#}");
                    }
                }
            }
        }
    });
}

fn spawn_trade_task(
    clob: ClobWsClient,
    state: Arc<Mutex<Vec<AssetState>>>,
    mut slot_rx: watch::Receiver<Vec<Option<(U256, U256, String)>>>,
) {
    tokio::spawn(async move {
        let mut trade_task: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            if slot_rx.changed().await.is_err() {
                break;
            }
            let tokens = slot_rx.borrow_and_update().clone();

            if let Some(h) = trade_task.take() {
                h.abort();
            }

            let mut ids: Vec<U256> = Vec::new();
            let mut map: Vec<(U256, usize)> = Vec::new();

            for (i, slot) in tokens.iter().enumerate() {
                if let Some((up, dn, _)) = slot {
                    ids.push(*up);
                    ids.push(*dn);
                    map.push((*up, i));
                    map.push((*dn, i));
                }
            }

            if ids.is_empty() {
                continue;
            }

            let clob = clob.clone();
            let state = Arc::clone(&state);
            trade_task = Some(tokio::spawn(async move {
                loop {
                    match clob.subscribe_last_trade_price(ids.clone()) {
                        Ok(stream) => {
                            let mut s = Box::pin(stream);
                            while let Some(Ok(update)) = s.next().await {
                                if let Some(&(_, idx)) =
                                    map.iter().find(|(id, _)| *id == update.asset_id)
                                {
                                    let price = d2f(&update.price);
                                    if price.is_finite() {
                                        let mut st = state.lock().unwrap();
                                        if idx < st.len() {
                                            st[idx].latest_trade = price;
                                        }
                                    }
                                }
                            }
                            eprintln!("last-trade stream closed, reconnecting…");
                        }
                        Err(e) => eprintln!("subscribe_last_trade_price failed: {e:#}, retrying…"),
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        }
    });
}

// ── Snapshot helper ───────────────────────────────────────────────────────────

type Snap = Option<(BookUpdate, i64, i64, f64, String, Option<BbaSample>)>;

fn snapshot(state: &Arc<Mutex<Vec<AssetState>>>) -> Vec<Snap> {
    let st = state.lock().unwrap();
    st.iter()
        .map(|s| {
            s.latest_book.clone().map(|b| {
                (
                    b,
                    s.book_server_ts_ms,
                    s.book_received_at_ms,
                    s.latest_trade,
                    s.slug.clone(),
                    s.latest_bba,
                )
            })
        })
        .collect()
}

// ── Main entry point ──────────────────────────────────────────────────────────

/// `raw_dir_base` names the 5-min (and binance) directory directly; `_15_mins`
/// and `_4hr` suffixes are appended for those durations. Lets a second
/// instance run against a scratch directory (e.g. `raw_new`) alongside the
/// production collector for parallel-run validation (doc/BINANCE_RECORDER_PLAN.md §6)
/// without colliding with its output. `nats_url`, if set, publishes live
/// Binance + 5m Poly ticks so the trader can subscribe instead of opening its
/// own duplicate feeds (README.md "Oracle infra: NATS price bridge").
pub async fn run(
    assets: Vec<String>,
    raw_dir_base: &str,
    nats_url: Option<String>,
    midpoint_poll_secs: u64,
) -> Result<()> {
    let raw_5m = PathBuf::from(raw_dir_base);
    let raw_15m = PathBuf::from(format!("{raw_dir_base}_15_mins"));
    let raw_4hr = PathBuf::from(format!("{raw_dir_base}_4hr"));
    fs::create_dir_all(&raw_5m).with_context(|| format!("create {raw_5m:?}"))?;
    fs::create_dir_all(&raw_15m).with_context(|| format!("create {raw_15m:?}"))?;
    fs::create_dir_all(&raw_4hr).with_context(|| format!("create {raw_4hr:?}"))?;

    // Recover any .tmp files orphaned by a crash in a now-stale hour before opening
    // this run's writers (which only carry-recover the *current* hour's .tmp, if any).
    let startup_hour = hkt_hour_string();
    seal_orphaned_tmp(&raw_5m, &startup_hour)?;
    seal_orphaned_tmp(&raw_15m, &startup_hour)?;
    seal_orphaned_tmp(&raw_4hr, &startup_hour)?;

    let http = Arc::new(
        reqwest::Client::builder()
            .user_agent("Mozilla/5.0")
            .build()
            .context("http client")?,
    );

    let assets: Vec<String> = if assets.is_empty() {
        eprintln!("no assets specified — auto-discovering from Polymarket…");
        discover_assets(&http).await?
    } else {
        assets.iter().map(|a| a.to_uppercase()).collect()
    };
    let n = assets.len();

    eprintln!(
        "collector starting for: {}  (5m + 15m + 4hr)",
        assets.join(", ")
    );

    // Connect to NATS if requested. Binance + 5m Poly ticks are published there
    // (from the samplers below, reusing their existing feed — see ticker_250ms
    // and the state_5m branch of ticker_200ms) so the trader can subscribe
    // instead of opening its own duplicate Binance/Poly WS connections.
    let nats: Option<async_nats::Client> = match nats_url {
        Some(ref url) => {
            let nc = async_nats::connect(url)
                .await
                .with_context(|| format!("connect to NATS at {url}"))?;
            eprintln!("NATS connected: {url}");
            Some(nc)
        }
        None => None,
    };

    let clob = ClobWsClient::default();

    // ── 5-min Polymarket feed ────────────────────────────────────────────────
    let state_5m = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_5m, slot_rx_5m) = watch::channel(vec![None; n]);
    spawn_meta_task(
        assets.clone(),
        Arc::clone(&state_5m),
        slot_tx_5m,
        Arc::clone(&http),
        300,
        "5m",
    );
    spawn_book_task(clob.clone(), Arc::clone(&state_5m), slot_rx_5m.clone());
    // 5m bba task also publishes to NATS when enabled (trader piggybacks on this feed).
    spawn_bba_task(
        clob.clone(),
        Arc::clone(&state_5m),
        slot_rx_5m.clone(),
        nats.clone(),
        assets.clone(),
    );
    spawn_reconcile_task(
        Arc::clone(&http),
        Arc::clone(&state_5m),
        slot_rx_5m.clone(),
        assets.clone(),
        midpoint_poll_secs,
    );
    spawn_trade_task(clob.clone(), Arc::clone(&state_5m), slot_rx_5m);

    // ── 15-min Polymarket feed ───────────────────────────────────────────────
    let state_15m = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_15m, slot_rx_15m) = watch::channel(vec![None; n]);
    spawn_meta_task(
        assets.clone(),
        Arc::clone(&state_15m),
        slot_tx_15m,
        Arc::clone(&http),
        900,
        "15m",
    );
    spawn_book_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m.clone());
    spawn_bba_task(
        clob.clone(),
        Arc::clone(&state_15m),
        slot_rx_15m.clone(),
        None,
        vec![],
    );
    spawn_trade_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m);

    // ── 4-hr Polymarket feed ─────────────────────────────────────────────────
    let state_4hr = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_4hr, slot_rx_4hr) = watch::channel(vec![None; n]);
    spawn_meta_task(
        assets.clone(),
        Arc::clone(&state_4hr),
        slot_tx_4hr,
        Arc::clone(&http),
        14400,
        "4h",
    );
    spawn_book_task(clob.clone(), Arc::clone(&state_4hr), slot_rx_4hr.clone());
    spawn_bba_task(
        clob.clone(),
        Arc::clone(&state_4hr),
        slot_rx_4hr.clone(),
        None,
        vec![],
    );
    spawn_trade_task(clob.clone(), Arc::clone(&state_4hr), slot_rx_4hr);

    // ── Binance feed (asset-level, period-independent — one WS + one writer
    // per asset, shared across durations; see BinanceWriters doc comment) ────
    let binance_state = Arc::new(Mutex::new(vec![BinanceState::default(); n]));
    for (i, asset) in assets.iter().enumerate() {
        spawn_binance_task(asset.clone(), i, Arc::clone(&binance_state));
    }
    let mut binance_writers: Vec<BinanceWriters> = assets
        .iter()
        .map(|a| BinanceWriters::new(a, &raw_5m))
        .collect::<Result<_>>()?;

    // ── Writers ──────────────────────────────────────────────────────────────
    let mut writers_5m: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_5m))
        .collect::<Result<_>>()?;
    let mut writers_15m: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_15m))
        .collect::<Result<_>>()?;
    let mut writers_4hr: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_4hr))
        .collect::<Result<_>>()?;

    // ── Samplers ──────────────────────────────────────────────────────────────
    let mut ticker_200ms = tokio::time::interval(Duration::from_millis(200));
    ticker_200ms.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_250ms = tokio::time::interval(Duration::from_millis(250));
    ticker_250ms.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_1s = tokio::time::interval(Duration::from_secs(1));
    ticker_1s.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sigterm = signal(SignalKind::terminate()).context("sigterm")?;

    // ── bba staleness observation (5m feed only) — logging only, takes no recovery action.
    // See staleness.rs's doc comment for why: a raw silence timer that *acts* on a
    // change-event stream false-positive-stormed in production on 2026-07-10 (deployed,
    // caused continuous needless resubscribes, rolled back same day). This phase-1 telemetry
    // exists to observe real per-asset silence-gap distributions before phase 2 (REST
    // `/midpoint` reconciliation, not yet implemented) ever triggers a real recovery again.
    let mut bba_last_seen_ms: Vec<i64> = vec![0; n];
    let mut bba_logged_bucket: Vec<usize> = vec![0; n];

    loop {
        tokio::select! {
            _ = ticker_200ms.tick() => {
                let ts = (now_secs_f64() * 5.0).round() / 5.0;

                // Seal check must run unconditionally per asset, before any early-continue on
                // missing data — otherwise an asset with no sample this tick (e.g. before the
                // first book snapshot arrives) silently skips its hourly seal too.
                for (i, snap) in snapshot(&state_5m).into_iter().enumerate() {
                    if let Err(e) = writers_5m[i].seal_if_hour_changed() { eprintln!("[{}] 5m seal: {e:#}", assets[i]); }
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_5m[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 5m write: {e:#}", assets[i]); }

                    // Staleness observation (logging only — see the comment above this
                    // loop's ticker setup for why this never takes a recovery action).
                    if let Some(sample) = bba {
                        if sample.received_at_ms != bba_last_seen_ms[i] {
                            bba_last_seen_ms[i] = sample.received_at_ms;
                            bba_logged_bucket[i] = 0;
                        } else {
                            let silent_ms = now_ms() - sample.received_at_ms;
                            let (crossed, new_logged) =
                                crate::staleness::buckets_to_log(bba_logged_bucket[i], silent_ms);
                            for bucket_ms in crossed {
                                eprintln!(
                                    "[OBSERVE-STALE] {} bba feed silent for >={bucket_ms}ms (actual {silent_ms}ms) — logging only, no action taken",
                                    assets[i]
                                );
                            }
                            bba_logged_bucket[i] = new_logged;
                        }
                    }
                }
                for (i, snap) in snapshot(&state_15m).into_iter().enumerate() {
                    if let Err(e) = writers_15m[i].seal_if_hour_changed() { eprintln!("[{}] 15m seal: {e:#}", assets[i]); }
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_15m[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 15m write: {e:#}", assets[i]); }
                }
            }

            _ = ticker_250ms.tick() => {
                let ts = (now_secs_f64() * 4.0).round() / 4.0;

                let samples: Vec<BinanceState> = binance_state.lock().unwrap().clone();
                let slugs: Vec<String> = state_5m.lock().unwrap().iter().map(|s| s.slug.clone()).collect();
                // Same ordering fix as above: HYPE has no Binance market, so sample.price is
                // always 0 and would otherwise skip seal_if_hour_changed forever, leaving its
                // binance .tmp file un-sealed by the normal hourly rotation.
                for (i, sample) in samples.into_iter().enumerate() {
                    if let Err(e) = binance_writers[i].seal_if_hour_changed() { eprintln!("[{}] binance seal: {e:#}", assets[i]); }
                    if sample.price <= 0.0 { continue; }
                    if let Some(ref nc) = nats {
                        let payload = binance_nats_payload(sample.received_at_ms, sample.price, sample.server_ts_ms);
                        let _ = nc.publish(format!("price.binance.{}", assets[i]), payload.into_bytes().into()).await;
                    }
                    let slug = slugs.get(i).cloned().unwrap_or_default();
                    if slug.is_empty() { continue; }
                    if let Err(e) = binance_writers[i].write_sample(ts, sample.price, &slug, sample.server_ts_ms, sample.received_at_ms) {
                        eprintln!("[{}] binance write: {e:#}", assets[i]);
                    }
                }
            }

            _ = ticker_1s.tick() => {
                let ts = now_secs() as f64;

                for (i, snap) in snapshot(&state_4hr).into_iter().enumerate() {
                    if let Err(e) = writers_4hr[i].seal_if_hour_changed() { eprintln!("[{}] 4hr seal: {e:#}", assets[i]); }
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_4hr[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 4hr write: {e:#}", assets[i]); }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nshutting down — flushing writers…");
                flush_all(writers_5m, writers_15m, writers_4hr, binance_writers);
                return Ok(());
            }

            _ = sigterm.recv() => {
                eprintln!("SIGTERM — flushing writers…");
                flush_all(writers_5m, writers_15m, writers_4hr, binance_writers);
                return Ok(());
            }
        }
    }
}

fn flush_all(
    writers_5m: Vec<AssetWriters>,
    writers_15m: Vec<AssetWriters>,
    writers_4hr: Vec<AssetWriters>,
    binance_writers: Vec<BinanceWriters>,
) {
    for w in writers_5m.into_iter().chain(writers_15m).chain(writers_4hr) {
        if let Err(e) = w.finish() {
            eprintln!("close error: {e:#}");
        }
    }
    for w in binance_writers {
        if let Err(e) = w.finish() {
            eprintln!("close error: {e:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the negative-signal_latency bug: the published
    /// `ts` must be the sample's exact real receive time, not a value snapped
    /// to a coarser grid (which is what the old `(now_secs_f64()*4.0).round()/4.0`
    /// ticker-fire timestamp did, up to 125ms off in either direction).
    #[test]
    fn binance_nats_payload_uses_exact_received_at_ms_unrounded() {
        // 1751234567.123s, deliberately not aligned to any 0.25s/0.2s grid point.
        let received_at_ms: i64 = 1_751_234_567_123;
        let payload = binance_nats_payload(received_at_ms, 42.5, 1_751_234_567_010);
        assert_eq!(
            payload,
            r#"{"ts":1751234567.123,"price":42.500000,"server_ts":1751234567.010}"#
        );
    }

    #[test]
    fn binance_nats_payload_formats_price_with_six_decimals() {
        let payload = binance_nats_payload(1_000, 0.1, 900);
        assert_eq!(
            payload,
            r#"{"ts":1.000,"price":0.100000,"server_ts":0.900}"#
        );
    }

    /// Binance's `E` field defaults to 0 when missing from the WS message
    /// (see `server_ts_ms = v["E"].as_i64().unwrap_or(0)` above) — the NATS
    /// payload must publish `null`, not a bogus `0.000` timestamp the trader
    /// would otherwise treat as a real (and wildly wrong) exchange latency.
    #[test]
    fn binance_nats_payload_omits_server_ts_when_zero() {
        let payload = binance_nats_payload(1_000, 0.1, 0);
        assert_eq!(payload, r#"{"ts":1.000,"price":0.100000,"server_ts":null}"#);
    }

    #[test]
    fn poly_nats_payload_includes_server_ts_and_complement_dn() {
        let payload = poly_nats_payload(1_751_234_567_123, 0.65, 1_751_234_567_010);
        assert_eq!(
            payload,
            r#"{"ts":1751234567.123,"up":0.650000,"dn":0.350000,"server_ts":1751234567.010}"#
        );
    }

    #[test]
    fn poly_nats_payload_omits_server_ts_when_unavailable() {
        let payload = poly_nats_payload(1_000, 0.5, -1);
        assert_eq!(
            payload,
            r#"{"ts":1.000,"up":0.500000,"dn":0.500000,"server_ts":null}"#
        );
    }
}
