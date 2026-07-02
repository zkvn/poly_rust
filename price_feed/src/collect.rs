use std::fs;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use arrow::array::{ArrayRef, Float32Builder, Float64Array, Float64Builder, ListBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Duration as ChronoDuration, FixedOffset, TimeZone as _, Utc};
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

// ── Time helpers ─────────────────────────────────────────────────────────────

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn now_secs_f64() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn current_slot_for(interval: u64) -> u64 {
    (now_secs() / interval) * interval
}

fn make_slug(asset: &str, slot: u64, suffix: &str) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), suffix, slot)
}

fn hkt_date_string() -> String {
    Utc::now().with_timezone(&hkt()).format("%Y-%m-%d").to_string()
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
    let start_min = (Utc::now() - ChronoDuration::hours(1))
        .format("%Y-%m-%dT%H:%M:%SZ");
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
            if let Some(slug) = event["slug"].as_str() {
                if let Some(pos) = slug.find("-updown-") {
                    assets.insert(slug[..pos].to_uppercase());
                }
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
    let outcomes: Vec<String> =
        serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

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
        Field::new("server_ts", DataType::Float64, true),  // ms; null for carried rows pre-schema
        Field::new("latency_ms", DataType::Float64, true), // receive_ms - server_ts; null for old rows
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
    schema: Schema,
    writer: ArrowWriter<fs::File>,
    rows_since_flush: usize,
    last_flush: std::time::Instant,
}

impl ParquetBuf {
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
        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .context("ArrowWriter")?;
        Ok(Self {
            path,
            schema,
            writer,
            rows_since_flush: 0,
            last_flush: std::time::Instant::now(),
        })
    }

    fn open_with_carry(path: PathBuf, schema: Schema) -> Result<Self> {
        let carry = if path.exists() {
            Self::try_carry(&path, &schema)
        } else {
            None
        };
        let mut buf = Self::open(path, schema)?;
        if let Some(batches) = carry {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!("  carry: loaded {rows} rows from {:?}", buf.path);
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

    // Write PAR1 footer to make the file valid parquet, then reopen with carry
    // to continue appending. Called hourly so files are always readable on disk.
    fn seal(self) -> Result<Self> {
        let path = self.path.clone();
        let schema = self.schema.clone();
        self.writer.close().with_context(|| format!("seal {:?}", path))?;
        Self::open_with_carry(path, schema)
    }
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
    date: String,
    raw_dir: PathBuf,
}

impl AssetWriters {
    fn new(asset: &str, raw_dir: &PathBuf) -> Result<Self> {
        let date = hkt_date_string();
        let ps = poly_schema();
        let bs = book_schema();
        let poly_path = raw_dir.join(format!("{asset}_poly_{date}.parquet"));
        let book_path = raw_dir.join(format!("{asset}_book_{date}.parquet"));
        eprintln!("[{asset}] opening poly={poly_path:?} book={book_path:?}");
        Ok(Self {
            asset: asset.to_string(),
            poly: ParquetBuf::open_with_carry(poly_path, ps.clone())?,
            book: ParquetBuf::open_with_carry(book_path, bs.clone())?,
            poly_schema: ps,
            book_schema: bs,
            date,
            raw_dir: raw_dir.clone(),
        })
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        let today = hkt_date_string();
        if today == self.date {
            return Ok(());
        }
        eprintln!("[{}] rotating {} → {}", self.asset, self.date, today);
        let poly_path = self
            .raw_dir
            .join(format!("{}_poly_{today}.parquet", self.asset));
        let book_path = self
            .raw_dir
            .join(format!("{}_book_{today}.parquet", self.asset));
        let old_poly = std::mem::replace(
            &mut self.poly,
            ParquetBuf::open(poly_path, self.poly_schema.clone())?,
        );
        old_poly.finish()?;
        let old_book = std::mem::replace(
            &mut self.book,
            ParquetBuf::open(book_path, self.book_schema.clone())?,
        );
        old_book.finish()?;
        self.date = today;
        Ok(())
    }

    // Write PAR1 footer to both files and reopen with carry so they are
    // always readable on disk even if the process crashes or is upgraded.
    fn seal(&mut self) -> Result<()> {
        let devnull = PathBuf::from("/dev/null");
        let old_poly = std::mem::replace(
            &mut self.poly,
            ParquetBuf::open(devnull.clone(), self.poly_schema.clone())?,
        );
        self.poly = old_poly.seal()?;
        let old_book = std::mem::replace(
            &mut self.book,
            ParquetBuf::open(devnull, self.book_schema.clone())?,
        );
        self.book = old_book.seal()?;
        Ok(())
    }

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

    fn finish(self) -> Result<()> {
        self.poly.finish()?;
        self.book.finish()
    }
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
                                {
                                    if is_up {
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
            task = Some(tokio::spawn(async move {
                loop {
                    match (
                        clob.subscribe_best_bid_ask(up_ids.clone()),
                        clob.subscribe_prices(up_ids.clone()),
                    ) {
                        (Ok(bba), Ok(pc)) => {
                            // Unify both feeds into (asset_id, best_bid, best_ask, server_ts_ms).
                            let bba_u = bba.filter_map(|r| async move {
                                r.ok()
                                    .map(|m| (m.asset_id, d2f(&m.best_bid), d2f(&m.best_ask), m.timestamp))
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
                            while let Some((asset_id, bid, ask, server_ts_ms)) = merged.next().await {
                                if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
                                    continue;
                                }
                                if let Some(&(_, idx)) = map.iter().find(|(id, _)| *id == asset_id) {
                                    let received_at_ms = now_ms();
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
                (b, s.book_server_ts_ms, s.book_received_at_ms, s.latest_trade, s.slug.clone(), s.latest_bba)
            })
        })
        .collect()
}

// ── Main entry point ──────────────────────────────────────────────────────────

pub async fn run(assets: Vec<String>) -> Result<()> {
    let raw_5m  = PathBuf::from("raw");
    let raw_15m = PathBuf::from("raw_15_mins");
    let raw_4hr = PathBuf::from("raw_4hr");
    fs::create_dir_all(&raw_5m).context("create raw/")?;
    fs::create_dir_all(&raw_15m).context("create raw_15_mins/")?;
    fs::create_dir_all(&raw_4hr).context("create raw_4hr/")?;

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

    let clob = ClobWsClient::default();

    // ── 5-min Polymarket feed ────────────────────────────────────────────────
    let state_5m = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_5m, slot_rx_5m) = watch::channel(vec![None; n]);
    spawn_meta_task(assets.clone(), Arc::clone(&state_5m), slot_tx_5m, Arc::clone(&http), 300, "5m");
    spawn_book_task(clob.clone(), Arc::clone(&state_5m), slot_rx_5m.clone());
    spawn_bba_task(clob.clone(), Arc::clone(&state_5m), slot_rx_5m.clone());
    spawn_trade_task(clob.clone(), Arc::clone(&state_5m), slot_rx_5m);

    // ── 15-min Polymarket feed ───────────────────────────────────────────────
    let state_15m = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_15m, slot_rx_15m) = watch::channel(vec![None; n]);
    spawn_meta_task(assets.clone(), Arc::clone(&state_15m), slot_tx_15m, Arc::clone(&http), 900, "15m");
    spawn_book_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m.clone());
    spawn_bba_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m.clone());
    spawn_trade_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m);

    // ── 4-hr Polymarket feed ─────────────────────────────────────────────────
    let state_4hr = Arc::new(Mutex::new(vec![AssetState::default(); n]));
    let (slot_tx_4hr, slot_rx_4hr) = watch::channel(vec![None; n]);
    spawn_meta_task(assets.clone(), Arc::clone(&state_4hr), slot_tx_4hr, Arc::clone(&http), 14400, "4h");
    spawn_book_task(clob.clone(), Arc::clone(&state_4hr), slot_rx_4hr.clone());
    spawn_bba_task(clob.clone(), Arc::clone(&state_4hr), slot_rx_4hr.clone());
    spawn_trade_task(clob.clone(), Arc::clone(&state_4hr), slot_rx_4hr);

    // ── Writers ──────────────────────────────────────────────────────────────
    let mut writers_5m:  Vec<AssetWriters> = assets.iter().map(|a| AssetWriters::new(a, &raw_5m)).collect::<Result<_>>()?;
    let mut writers_15m: Vec<AssetWriters> = assets.iter().map(|a| AssetWriters::new(a, &raw_15m)).collect::<Result<_>>()?;
    let mut writers_4hr: Vec<AssetWriters> = assets.iter().map(|a| AssetWriters::new(a, &raw_4hr)).collect::<Result<_>>()?;

    // ── Samplers ──────────────────────────────────────────────────────────────
    let mut ticker_200ms = tokio::time::interval(Duration::from_millis(200));
    ticker_200ms.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_1s = tokio::time::interval(Duration::from_secs(1));
    ticker_1s.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticker_1hr = tokio::time::interval(Duration::from_secs(3600));
    ticker_1hr.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sigterm = signal(SignalKind::terminate()).context("sigterm")?;

    loop {
        tokio::select! {
            _ = ticker_200ms.tick() => {
                let ts = (now_secs_f64() * 5.0).round() / 5.0;

                for (i, snap) in snapshot(&state_5m).into_iter().enumerate() {
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_5m[i].rotate_if_needed() { eprintln!("[{}] 5m rotate: {e:#}", assets[i]); }
                    if let Err(e) = writers_5m[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 5m write: {e:#}", assets[i]); }
                }
                for (i, snap) in snapshot(&state_15m).into_iter().enumerate() {
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_15m[i].rotate_if_needed() { eprintln!("[{}] 15m rotate: {e:#}", assets[i]); }
                    if let Err(e) = writers_15m[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 15m write: {e:#}", assets[i]); }
                }
            }

            _ = ticker_1s.tick() => {
                let ts = now_secs() as f64;

                for (i, snap) in snapshot(&state_4hr).into_iter().enumerate() {
                    let Some((book, srv_ts, rcv_ts, last_trade, slug, bba)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_4hr[i].rotate_if_needed() { eprintln!("[{}] 4hr rotate: {e:#}", assets[i]); }
                    if let Err(e) = writers_4hr[i].write_sample(ts, &book, last_trade, &slug, srv_ts, rcv_ts, bba) { eprintln!("[{}] 4hr write: {e:#}", assets[i]); }
                }
            }

            _ = ticker_1hr.tick() => {
                eprintln!("hourly seal — writing footer to all parquet files …");
                for w in &mut writers_5m  { if let Err(e) = w.seal() { eprintln!("[{}] 5m seal: {e:#}", w.asset); } }
                for w in &mut writers_15m { if let Err(e) = w.seal() { eprintln!("[{}] 15m seal: {e:#}", w.asset); } }
                for w in &mut writers_4hr { if let Err(e) = w.seal() { eprintln!("[{}] 4hr seal: {e:#}", w.asset); } }
                eprintln!("hourly seal done");
            }

            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nshutting down — flushing writers…");
                flush_all(writers_5m, writers_15m, writers_4hr);
                return Ok(());
            }

            _ = sigterm.recv() => {
                eprintln!("SIGTERM — flushing writers…");
                flush_all(writers_5m, writers_15m, writers_4hr);
                return Ok(());
            }
        }
    }
}

fn flush_all(
    writers_5m: Vec<AssetWriters>,
    writers_15m: Vec<AssetWriters>,
    writers_4hr: Vec<AssetWriters>,
) {
    for w in writers_5m
        .into_iter()
        .chain(writers_15m)
        .chain(writers_4hr)
    {
        if let Err(e) = w.finish() {
            eprintln!("close error: {e:#}");
        }
    }
}
