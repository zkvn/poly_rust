use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use arrow::array::{ArrayRef, Float32Builder, Float64Builder, ListBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Duration as ChronoDuration, FixedOffset, TimeZone as _, Utc};
use futures::{SinkExt as _, StreamExt as _};
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
    latest_trade: f64,
    slug: String,
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
                // slug pattern: {asset}-updown-{5m|15m}-{slot}
                if let Some(pos) = slug.find("-updown-") {
                    let asset = slug[..pos].to_uppercase();
                    assets.insert(asset);
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
    ])
}

fn hl_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts", DataType::Float64, false),
        Field::new("asset", DataType::Utf8, false),
        Field::new("mid", DataType::Float64, false),
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
        let writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))
            .context("ArrowWriter")?;
        Ok(Self {
            path,
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
        let ok = builder
            .schema()
            .fields()
            .iter()
            .zip(expected.fields().iter())
            .all(|(a, b)| a.name() == b.name());
        if !ok {
            eprintln!("  carry: schema mismatch, discarding {path:?}");
            return None;
        }
        Some(builder.build().ok()?.flatten().collect())
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
}

// ── Decimal helper ────────────────────────────────────────────────────────────

fn d2f(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
}

// ── Poly + book row builders ──────────────────────────────────────────────────

fn poly_row(schema: &Schema, ts: f64, up: f64, slug: &str) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(arrow::array::Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![up])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![1.0 - up])) as ArrayRef,
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
        ],
    )
    .context("poly row")
}

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
            Arc::new(arrow::array::Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(StringArray::from(vec![asset])) as ArrayRef,
            Arc::new(StringArray::from(vec![slug])) as ArrayRef,
            Arc::new(StringArray::from(vec![side])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![best_bid])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![best_ask])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![last_trade])) as ArrayRef,
            list_col(bid_prices),
            list_col(bid_sizes),
            list_col(ask_prices),
            list_col(ask_sizes),
        ],
    )
    .context("book row")
}

fn hl_row(schema: &Schema, ts: f64, asset: &str, mid: f64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(arrow::array::Float64Array::from(vec![ts])) as ArrayRef,
            Arc::new(StringArray::from(vec![asset])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![mid])) as ArrayRef,
        ],
    )
    .context("hl row")
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

    fn write_sample(
        &mut self,
        ts: f64,
        book: &BookUpdate,
        last_trade: f64,
        slug: &str,
    ) -> Result<()> {
        let best_bid = book.bids.first().map(|l| d2f(&l.price)).unwrap_or(0.0);
        let best_ask = book.asks.first().map(|l| d2f(&l.price)).unwrap_or(0.0);
        if best_bid <= 0.0 || best_ask <= 0.0 {
            return Ok(());
        }
        let up_mid = (best_bid + best_ask) / 2.0;

        self.poly
            .write(poly_row(&self.poly_schema, ts, up_mid, slug)?)?;

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
        )?)?;

        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.poly.finish()?;
        self.book.finish()
    }
}

// ── Per-asset Hyperliquid writer ──────────────────────────────────────────────

struct HlWriter {
    asset: String,
    buf: ParquetBuf,
    schema: Schema,
    date: String,
    raw_dir: PathBuf,
}

impl HlWriter {
    fn new(asset: &str, raw_dir: &PathBuf) -> Result<Self> {
        let date = hkt_date_string();
        let schema = hl_schema();
        let path = raw_dir.join(format!("{asset}_hl_{date}.parquet"));
        eprintln!("[{asset}] opening hl={path:?}");
        Ok(Self {
            asset: asset.to_string(),
            buf: ParquetBuf::open_with_carry(path, schema.clone())?,
            schema,
            date,
            raw_dir: raw_dir.clone(),
        })
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        let today = hkt_date_string();
        if today == self.date {
            return Ok(());
        }
        let path = self
            .raw_dir
            .join(format!("{}_hl_{today}.parquet", self.asset));
        let old = std::mem::replace(
            &mut self.buf,
            ParquetBuf::open(path, self.schema.clone())?,
        );
        old.finish()?;
        self.date = today;
        Ok(())
    }

    fn write_sample(&mut self, ts: f64, mid: f64) -> Result<()> {
        self.buf.write(hl_row(&self.schema, ts, &self.asset, mid)?)
    }

    fn finish(self) -> Result<()> {
        self.buf.finish()
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
                                        let mut st = state.lock().unwrap();
                                        if idx < st.len() {
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

// Hyperliquid allMids WS — returns shared mid-price map updated in background
fn spawn_hl_task(assets: Vec<String>) -> Arc<Mutex<HashMap<String, f64>>> {
    let mids: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(HashMap::new()));
    let mids2 = Arc::clone(&mids);

    tokio::spawn(async move {
        loop {
            match tokio_tungstenite::connect_async("wss://api.hyperliquid.xyz/ws").await {
                Ok((ws, _)) => {
                    let (mut write, mut read) = ws.split();
                    let sub = serde_json::json!({
                        "method": "subscribe",
                        "subscription": {"type": "allMids"}
                    });
                    if write
                        .send(Message::Text(sub.to_string().into()))
                        .await
                        .is_err()
                    {
                        eprintln!("HL subscribe send failed, reconnecting…");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    eprintln!("[HL] connected");
                    while let Some(Ok(msg)) = read.next().await {
                        if let Message::Text(text) = msg {
                            if let Ok(val) =
                                serde_json::from_str::<serde_json::Value>(text.as_str())
                            {
                                if val["channel"] == "allMids" {
                                    if let Some(obj) = val["data"]["mids"].as_object() {
                                        let mut m = mids2.lock().unwrap();
                                        for asset in &assets {
                                            if let Some(v) = obj.get(asset.as_str()) {
                                                let price = v
                                                    .as_str()
                                                    .and_then(|s| s.parse::<f64>().ok())
                                                    .or_else(|| v.as_f64());
                                                if let Some(p) = price {
                                                    m.insert(asset.clone(), p);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    eprintln!("[HL] WS closed, reconnecting…");
                }
                Err(e) => eprintln!("[HL] connect error: {e:#}"),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    mids
}

// ── Main entry point ──────────────────────────────────────────────────────────

pub async fn run(assets: Vec<String>) -> Result<()> {
    let raw_5m = PathBuf::from("raw");
    let raw_15m = PathBuf::from("raw_15_mins");
    fs::create_dir_all(&raw_5m).context("create raw/")?;
    fs::create_dir_all(&raw_15m).context("create raw_15_mins/")?;

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
        "collector starting for: {}  (5m + 15m + HL)",
        assets.join(", ")
    );

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
    spawn_trade_task(clob.clone(), Arc::clone(&state_15m), slot_rx_15m);

    // ── Hyperliquid feed ─────────────────────────────────────────────────────
    let hl_mids = spawn_hl_task(assets.clone());

    // ── Writers ──────────────────────────────────────────────────────────────
    let mut writers_5m: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_5m))
        .collect::<Result<_>>()?;
    let mut writers_15m: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_15m))
        .collect::<Result<_>>()?;
    let mut hl_writers: Vec<HlWriter> = assets
        .iter()
        .map(|a| HlWriter::new(a, &raw_5m))
        .collect::<Result<_>>()?;

    // ── Sampler ───────────────────────────────────────────────────────────────
    let mut ticker = tokio::time::interval(Duration::from_millis(200));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sigterm = signal(SignalKind::terminate()).context("sigterm")?;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let ts = (now_secs_f64() * 5.0).round() / 5.0;

                // Snapshot shared state under brief lock holds
                let snaps_5m: Vec<Option<(BookUpdate, f64, String)>> = {
                    let st = state_5m.lock().unwrap();
                    st.iter()
                        .map(|s| s.latest_book.clone().map(|b| (b, s.latest_trade, s.slug.clone())))
                        .collect()
                };
                let snaps_15m: Vec<Option<(BookUpdate, f64, String)>> = {
                    let st = state_15m.lock().unwrap();
                    st.iter()
                        .map(|s| s.latest_book.clone().map(|b| (b, s.latest_trade, s.slug.clone())))
                        .collect()
                };
                let hl_snap: HashMap<String, f64> = hl_mids.lock().unwrap().clone();

                // Write 5m
                for (i, snap) in snaps_5m.into_iter().enumerate() {
                    let Some((book, last_trade, slug)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_5m[i].rotate_if_needed() {
                        eprintln!("[{}] 5m rotate: {e:#}", assets[i]);
                    }
                    if let Err(e) = writers_5m[i].write_sample(ts, &book, last_trade, &slug) {
                        eprintln!("[{}] 5m write: {e:#}", assets[i]);
                    }
                }

                // Write 15m
                for (i, snap) in snaps_15m.into_iter().enumerate() {
                    let Some((book, last_trade, slug)) = snap else { continue };
                    if slug.is_empty() { continue; }
                    if let Err(e) = writers_15m[i].rotate_if_needed() {
                        eprintln!("[{}] 15m rotate: {e:#}", assets[i]);
                    }
                    if let Err(e) = writers_15m[i].write_sample(ts, &book, last_trade, &slug) {
                        eprintln!("[{}] 15m write: {e:#}", assets[i]);
                    }
                }

                // Write HL
                for (i, hw) in hl_writers.iter_mut().enumerate() {
                    if let Some(&mid) = hl_snap.get(&assets[i]) {
                        if mid > 0.0 {
                            if let Err(e) = hw.rotate_if_needed() {
                                eprintln!("[{}] hl rotate: {e:#}", assets[i]);
                            }
                            if let Err(e) = hw.write_sample(ts, mid) {
                                eprintln!("[{}] hl write: {e:#}", assets[i]);
                            }
                        }
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nshutting down — flushing writers…");
                flush_all(writers_5m, writers_15m, hl_writers);
                return Ok(());
            }

            _ = sigterm.recv() => {
                eprintln!("SIGTERM — flushing writers…");
                flush_all(writers_5m, writers_15m, hl_writers);
                return Ok(());
            }
        }
    }
}

fn flush_all(
    writers_5m: Vec<AssetWriters>,
    writers_15m: Vec<AssetWriters>,
    hl_writers: Vec<HlWriter>,
) {
    for w in writers_5m.into_iter().chain(writers_15m) {
        if let Err(e) = w.finish() {
            eprintln!("close error: {e:#}");
        }
    }
    for w in hl_writers {
        if let Err(e) = w.finish() {
            eprintln!("close error: {e:#}");
        }
    }
}
