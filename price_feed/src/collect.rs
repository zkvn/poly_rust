use std::fs;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{FixedOffset, TimeZone as _, Utc};
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

// ── HKT helpers (mirrors markets.rs) ─────────────────────────────────────────

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn current_slot() -> u64 {
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    (s / 300) * 300
}

fn make_slug(asset: &str, slot: u64) -> String {
    format!("{}-updown-5m-{}", asset.to_lowercase(), slot)
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
    up_id: Option<U256>,
    dn_id: Option<U256>,
}

// ── Gamma meta fetch ─────────────────────────────────────────────────────────

struct SlotTokens {
    slot: u64,
    slug: String,
    up_id: U256,
    dn_id: U256,
}

async fn fetch_meta(http: &reqwest::Client, asset: &str, slot: u64) -> Result<SlotTokens> {
    let slug = make_slug(asset, slot);
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
        .ok_or_else(|| anyhow::anyhow!("no market in event for {slug}"))?;

    let token_ids: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap_or("[]"))?;
    let outcomes: Vec<String> =
        serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

    let find = |target: &str| -> Result<U256> {
        outcomes
            .iter()
            .zip(token_ids.iter())
            .find(|(o, _)| o.to_lowercase() == target)
            .map(|(_, tid)| {
                U256::from_str(tid).with_context(|| format!("parse {target} token id"))
            })
            .ok_or_else(|| anyhow::anyhow!("no {} token in {}", target, slug))?
    };

    let up_id = find("up")?;
    let dn_id = find("down")?;

    Ok(SlotTokens {
        slot,
        slug,
        up_id,
        dn_id,
    })
}

// ── Parquet schemas ───────────────────────────────────────────────────────────

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

// ── Parquet writer with rotation + carry ─────────────────────────────────────

struct ParquetBuf {
    schema: Schema,
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

        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
            .context("create ArrowWriter")?;

        Ok(Self {
            schema,
            path,
            writer,
            rows_since_flush: 0,
            last_flush: std::time::Instant::now(),
        })
    }

    /// Open file for writing, carrying forward any existing valid rows.
    fn open_with_carry(path: PathBuf, schema: Schema) -> Result<Self> {
        let carry = if path.exists() {
            Self::read_carry(&path, &schema)
        } else {
            None
        };

        let mut buf = Self::open(path, schema)?;

        if let Some(batches) = carry {
            for batch in batches {
                buf.writer.write(&batch).context("carry write")?;
            }
            eprintln!("  carry: replayed {} batches", 0); // count logged inside read_carry
        }

        Ok(buf)
    }

    fn read_carry(path: &PathBuf, expected_schema: &Schema) -> Option<Vec<RecordBatch>> {
        let file = fs::File::open(path).ok()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;

        let file_schema = builder.schema().clone();
        // Verify field names match
        let ok = file_schema
            .fields()
            .iter()
            .zip(expected_schema.fields().iter())
            .all(|(a, b)| a.name() == b.name());
        if !ok {
            eprintln!("  carry: schema mismatch, discarding {path:?}");
            return None;
        }

        let reader = builder.build().ok()?;
        let batches: Vec<RecordBatch> = reader.flatten().collect();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        eprintln!("  carry: loaded {rows} rows from {path:?}");
        Some(batches)
    }

    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let n = batch.num_rows();
        self.writer.write(&batch).context("parquet write")?;
        self.rows_since_flush += n;

        if self.rows_since_flush >= 500 || self.last_flush.elapsed() >= Duration::from_secs(10) {
            self.writer.flush().context("parquet flush")?;
            self.rows_since_flush = 0;
            self.last_flush = std::time::Instant::now();
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.writer.close().with_context(|| format!("close {:?}", self.path))?;
        Ok(())
    }
}

// ── Row builders ─────────────────────────────────────────────────────────────

fn decimal_to_f64(d: &polymarket_client_sdk_v2::types::Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(f64::NAN)
}

fn make_poly_batch(schema: &Schema, ts: f64, up: f64, slug: &str) -> Result<RecordBatch> {
    let ts_arr = Arc::new(arrow::array::Float64Array::from(vec![ts])) as ArrayRef;
    let up_arr = Arc::new(arrow::array::Float64Array::from(vec![up])) as ArrayRef;
    let dn_arr = Arc::new(arrow::array::Float64Array::from(vec![1.0 - up])) as ArrayRef;
    let slug_arr = Arc::new(StringArray::from(vec![slug])) as ArrayRef;

    RecordBatch::try_new(Arc::new(schema.clone()), vec![ts_arr, up_arr, dn_arr, slug_arr])
        .context("poly batch")
}

fn make_book_batch(
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
    let n = 1usize;

    let ts_arr = Arc::new(arrow::array::Float64Array::from(vec![ts])) as ArrayRef;
    let asset_arr = Arc::new(StringArray::from(vec![asset])) as ArrayRef;
    let slug_arr = Arc::new(StringArray::from(vec![slug])) as ArrayRef;
    let side_arr = Arc::new(StringArray::from(vec![side])) as ArrayRef;
    let bb_arr = Arc::new(arrow::array::Float64Array::from(vec![best_bid])) as ArrayRef;
    let ba_arr = Arc::new(arrow::array::Float64Array::from(vec![best_ask])) as ArrayRef;
    let lt_arr = Arc::new(arrow::array::Float64Array::from(vec![last_trade])) as ArrayRef;

    fn list_col(data: &[f32], n: usize) -> ArrayRef {
        let mut b = ListBuilder::new(Float32Builder::new());
        for _ in 0..n {
            b.values().append_slice(data);
            b.append(true);
        }
        Arc::new(b.finish()) as ArrayRef
    }

    let bp_arr = list_col(bid_prices, n);
    let bs_arr = list_col(bid_sizes, n);
    let ap_arr = list_col(ask_prices, n);
    let as_arr = list_col(ask_sizes, n);

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            ts_arr, asset_arr, slug_arr, side_arr, bb_arr, ba_arr, lt_arr, bp_arr, bs_arr,
            ap_arr, as_arr,
        ],
    )
    .context("book batch")
}

// ── Writer pair (poly + book) per asset ──────────────────────────────────────

struct AssetWriters {
    asset: String,
    poly: ParquetBuf,
    book: ParquetBuf,
    date: String,
    poly_schema: Schema,
    book_schema: Schema,
    raw_dir: PathBuf,
}

impl AssetWriters {
    fn new(asset: &str, raw_dir: &PathBuf) -> Result<Self> {
        let date = hkt_date_string();
        let ps = poly_schema();
        let bs = book_schema();

        let poly_path = raw_dir.join(format!("{asset}_poly_{date}.parquet"));
        let book_path = raw_dir.join(format!("{asset}_book_{date}.parquet"));

        eprintln!(
            "[{asset}] opening poly={poly_path:?} book={book_path:?}"
        );

        let poly = ParquetBuf::open_with_carry(poly_path, ps.clone())?;
        let book = ParquetBuf::open_with_carry(book_path, bs.clone())?;

        Ok(Self {
            asset: asset.to_string(),
            poly,
            book,
            date,
            poly_schema: ps,
            book_schema: bs,
            raw_dir: raw_dir.clone(),
        })
    }

    /// Rotate to a new date file if HKT calendar day has changed.
    fn rotate_if_needed(&mut self) -> Result<()> {
        let today = hkt_date_string();
        if today == self.date {
            return Ok(());
        }
        eprintln!("[{}] rotating: {} → {}", self.asset, self.date, today);

        let poly_path = self.raw_dir.join(format!("{}_poly_{today}.parquet", self.asset));
        let book_path = self.raw_dir.join(format!("{}_book_{today}.parquet", self.asset));

        // Swap in new writers; finish the old ones (take ownership to call finish())
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

    fn write_poly(&mut self, ts: f64, up: f64, slug: &str) -> Result<()> {
        let batch = make_poly_batch(&self.poly_schema, ts, up, slug)?;
        self.poly.write_batch(batch)
    }

    fn write_book(
        &mut self,
        ts: f64,
        side: &str,
        best_bid: f64,
        best_ask: f64,
        last_trade: f64,
        bid_prices: &[f32],
        bid_sizes: &[f32],
        ask_prices: &[f32],
        ask_sizes: &[f32],
        slug: &str,
    ) -> Result<()> {
        let batch = make_book_batch(
            &self.book_schema,
            ts,
            &self.asset,
            slug,
            side,
            best_bid,
            best_ask,
            last_trade,
            bid_prices,
            bid_sizes,
            ask_prices,
            ask_sizes,
        )?;
        self.book.write_batch(batch)
    }

    fn finish(self) -> Result<()> {
        self.poly.finish()?;
        self.book.finish()?;
        Ok(())
    }
}

// ── Main entry point ──────────────────────────────────────────────────────────

pub async fn run(assets: Vec<String>) -> Result<()> {
    let raw_dir = PathBuf::from("raw");
    fs::create_dir_all(&raw_dir).context("create raw/")?;

    let assets: Vec<String> = assets.iter().map(|a| a.to_uppercase()).collect();
    let n = assets.len();

    eprintln!("collector starting for: {}", assets.join(", "));

    // Shared state per asset
    let state: Arc<Mutex<Vec<AssetState>>> = Arc::new(Mutex::new(vec![AssetState::default(); n]));

    // Watch channels for slot tokens: meta task → WS tasks
    let (slot_tx, slot_rx) = watch::channel::<Vec<Option<(U256, U256, String)>>>(vec![None; n]);

    let clob = ClobWsClient::default();
    let http = Arc::new(
        reqwest::Client::builder()
            .user_agent("Mozilla/5.0")
            .build()
            .context("http client")?,
    );

    // Meta task: polls Gamma, pushes updated slot tokens
    {
        let assets = assets.clone();
        let state = Arc::clone(&state);
        let slot_tx = slot_tx;
        let http = Arc::clone(&http);

        tokio::spawn(async move {
            let mut slots: Vec<Option<(u64, U256, U256, String)>> = vec![None; assets.len()];

            loop {
                let current = current_slot();
                let mut changed = false;

                for (i, asset) in assets.iter().enumerate() {
                    let needs_refresh = slots[i]
                        .as_ref()
                        .map(|(slot, ..)| *slot != current)
                        .unwrap_or(true);

                    if needs_refresh {
                        match fetch_meta(&http, asset, current).await {
                            Ok(meta) => {
                                eprintln!(
                                    "[{asset}] slot {} → slug={} up={} dn={}",
                                    current,
                                    meta.slug,
                                    meta.up_id,
                                    meta.dn_id,
                                );
                                {
                                    let mut st = state.lock().unwrap();
                                    st[i].slug = meta.slug.clone();
                                    st[i].up_id = Some(meta.up_id);
                                    st[i].dn_id = Some(meta.dn_id);
                                }
                                slots[i] = Some((current, meta.up_id, meta.dn_id, meta.slug));
                                changed = true;
                            }
                            Err(e) => eprintln!("[{asset}] meta error: {e:#}"),
                        }
                    }
                }

                if changed {
                    let payload: Vec<Option<(U256, U256, String)>> = slots
                        .iter()
                        .map(|s| s.as_ref().map(|(_, u, d, sl)| (*u, *d, sl.clone())))
                        .collect();
                    let _ = slot_tx.send(payload);
                }

                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }

    // Orderbook WS task — stores only UP-token book updates; DN rows are derived as 1-complement
    {
        let clob = clob.clone();
        let state = Arc::clone(&state);
        let mut rx = slot_rx.clone();

        tokio::spawn(async move {
            let mut book_task: Option<tokio::task::JoinHandle<()>> = None;

            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let tokens = rx.borrow_and_update().clone();

                if let Some(h) = book_task.take() {
                    h.abort();
                }

                let mut new_ids: Vec<U256> = Vec::new();
                let mut new_map: Vec<(U256, usize, bool)> = Vec::new();

                for (i, slot) in tokens.iter().enumerate() {
                    if let Some((up, dn, _)) = slot {
                        new_ids.push(*up);
                        new_ids.push(*dn);
                        new_map.push((*up, i, true));
                        new_map.push((*dn, i, false));
                    }
                }

                if new_ids.is_empty() {
                    continue;
                }

                let clob = clob.clone();
                let state = Arc::clone(&state);
                book_task = Some(tokio::spawn(async move {
                    loop {
                        match clob.subscribe_orderbook(new_ids.clone()) {
                            Ok(stream) => {
                                let mut s = Box::pin(stream);
                                while let Some(Ok(update)) = s.next().await {
                                    if let Some(&(_, idx, is_up)) =
                                        new_map.iter().find(|(id, _, _)| *id == update.asset_id)
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

    // Last-trade WS task
    {
        let clob = clob.clone();
        let state = Arc::clone(&state);
        let mut rx = slot_rx.clone();

        tokio::spawn(async move {
            let mut trade_task: Option<tokio::task::JoinHandle<()>> = None;

            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let tokens = rx.borrow_and_update().clone();

                if let Some(h) = trade_task.take() {
                    h.abort();
                }

                let mut new_ids: Vec<U256> = Vec::new();
                let mut new_map: Vec<(U256, usize)> = Vec::new();

                for (i, slot) in tokens.iter().enumerate() {
                    if let Some((up, dn, _)) = slot {
                        new_ids.push(*up);
                        new_ids.push(*dn);
                        new_map.push((*up, i));
                        new_map.push((*dn, i));
                    }
                }

                if new_ids.is_empty() {
                    continue;
                }

                let clob = clob.clone();
                let state = Arc::clone(&state);
                trade_task = Some(tokio::spawn(async move {
                    loop {
                        match clob.subscribe_last_trade_price(new_ids.clone()) {
                            Ok(stream) => {
                                let mut s = Box::pin(stream);
                                while let Some(Ok(update)) = s.next().await {
                                    if let Some(&(_, idx)) =
                                        new_map.iter().find(|(id, _)| *id == update.asset_id)
                                    {
                                        let price = decimal_to_f64(&update.price);
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
                            Err(e) => {
                                eprintln!("subscribe_last_trade_price failed: {e:#}, retrying…")
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }));
            }
        });
    }

    // Sampler + writers (main loop)
    let mut writers: Vec<AssetWriters> = assets
        .iter()
        .map(|a| AssetWriters::new(a, &raw_dir))
        .collect::<Result<Vec<_>>>()?;

    let mut interval = tokio::time::interval(Duration::from_millis(200));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut sigterm = signal(SignalKind::terminate()).context("sigterm")?;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let now = now_secs_f64();
                let ts = (now * 5.0).round() / 5.0;

                let snapshots: Vec<Option<(BookUpdate, f64, String)>> = {
                    let st = state.lock().unwrap();
                    st.iter()
                        .map(|s| {
                            s.latest_book.clone().map(|b| (b, s.latest_trade, s.slug.clone()))
                        })
                        .collect()
                };

                for (i, snap) in snapshots.into_iter().enumerate() {
                    let Some((book, last_trade, slug)) = snap else { continue };
                    if slug.is_empty() { continue; }

                    if let Err(e) = writers[i].rotate_if_needed() {
                        eprintln!("[{}] rotate error: {e:#}", assets[i]);
                    }

                    // Compute mid from UP token's book (bids.first() = best bid)
                    let best_bid = book.bids.first().map(|l| decimal_to_f64(&l.price)).unwrap_or(0.0);
                    let best_ask = book.asks.first().map(|l| decimal_to_f64(&l.price)).unwrap_or(0.0);

                    if best_bid <= 0.0 || best_ask <= 0.0 {
                        continue; // Zero Means Zero — skip rows with no real book
                    }

                    let up_mid = (best_bid + best_ask) / 2.0;

                    // poly row
                    if let Err(e) = writers[i].write_poly(ts, up_mid, &slug) {
                        eprintln!("[{}] poly write error: {e:#}", assets[i]);
                    }

                    // Build depth ladders — reverse so best is last (Python order: worst→best)
                    let mut bid_prices: Vec<f32> = book.bids.iter().map(|l| decimal_to_f64(&l.price) as f32).collect();
                    let mut bid_sizes: Vec<f32> = book.bids.iter().map(|l| decimal_to_f64(&l.size) as f32).collect();
                    let mut ask_prices: Vec<f32> = book.asks.iter().map(|l| decimal_to_f64(&l.price) as f32).collect();
                    let mut ask_sizes: Vec<f32> = book.asks.iter().map(|l| decimal_to_f64(&l.size) as f32).collect();

                    bid_prices.reverse();
                    bid_sizes.reverse();
                    ask_prices.reverse();
                    ask_sizes.reverse();

                    // UP book row
                    if let Err(e) = writers[i].write_book(
                        ts, "UP", best_bid, best_ask, last_trade,
                        &bid_prices, &bid_sizes, &ask_prices, &ask_sizes, &slug,
                    ) {
                        eprintln!("[{}] book UP write error: {e:#}", assets[i]);
                    }

                    // DN row: dn_mid = 1 - up_mid, best_bid/ask are symmetric
                    let dn_best_bid = 1.0 - best_ask;
                    let dn_best_ask = 1.0 - best_bid;

                    // DN depth is the complement of UP depth, reversed
                    let dn_bid_prices: Vec<f32> = ask_prices.iter().map(|p| 1.0 - p).collect();
                    let dn_bid_sizes: Vec<f32> = ask_sizes.clone();
                    let dn_ask_prices: Vec<f32> = bid_prices.iter().map(|p| 1.0 - p).collect();
                    let dn_ask_sizes: Vec<f32> = bid_sizes.clone();

                    if let Err(e) = writers[i].write_book(
                        ts, "DN", dn_best_bid, dn_best_ask, last_trade,
                        &dn_bid_prices, &dn_bid_sizes, &dn_ask_prices, &dn_ask_sizes, &slug,
                    ) {
                        eprintln!("[{}] book DN write error: {e:#}", assets[i]);
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nctrl-c: flushing and closing writers…");
                for w in writers {
                    if let Err(e) = w.finish() {
                        eprintln!("close error: {e:#}");
                    }
                }
                return Ok(());
            }

            _ = sigterm.recv() => {
                eprintln!("SIGTERM: flushing and closing writers…");
                for w in writers {
                    if let Err(e) = w.finish() {
                        eprintln!("close error: {e:#}");
                    }
                }
                return Ok(());
            }
        }
    }
}
