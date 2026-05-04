//! Chainlink price feed via Polymarket RTDS v2.
//!
//! Subscribes to BTC/USD, ETH/USD, SOL/USD, BNB/USD from
//! wss://ws-live-data.polymarket.com and:
//!   1. Emits one JSON line per update to stdout (for Python bot.py)
//!   2. Buffers rows and flushes to a daily parquet file every FLUSH_EVERY rows
//!      or every FLUSH_SECS seconds — whichever comes first
//!
//! Parquet files are named prices/2025-04-27_143022.parquet (date + start time)
//! so a restart mid-day never overwrites previously saved data.
//!
//! Ctrl-C triggers a graceful shutdown: remaining buffer is flushed and the
//! parquet file is properly closed before exit.

use std::fs::{self, File};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{FixedOffset, TimeZone};
use futures::StreamExt as _;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use polymarket_client_sdk_v2::rtds::Client;
use tokio::time;

// Chainlink slash-format symbols → bot asset labels
const SYMBOLS: &[(&str, &str)] = &[
    ("btc/usd", "BTC"),
    ("eth/usd", "ETH"),
    ("sol/usd", "SOL"),
    ("bnb/usd", "BNB"),
];

// Flush to parquet every N rows ...
const FLUSH_EVERY: usize = 100;
// ... or every N seconds — whichever comes first
const FLUSH_SECS: u64 = 60;

// Parquet output directory — created automatically
const OUTPUT_DIR: &str = "prices";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("asset", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts_ms", DataType::Int64, false), // Unix ms from Chainlink oracle
        Field::new("ts_hkt", DataType::Utf8, false), // Human-readable HKT string
    ]))
}

/// Returns a unique parquet path using today's date + process start time.
/// e.g. "prices/2025-04-27_143022.parquet"
/// A restart mid-day produces a new filename, never overwriting existing data.
fn make_path(hkt: &FixedOffset) -> String {
    let now = chrono::Utc::now().with_timezone(hkt);
    format!("{OUTPUT_DIR}/{}.parquet", now.format("%Y-%m-%d_%H%M%S"))
}

/// Returns just the date portion for midnight rotation detection.
fn today_date(hkt: &FixedOffset) -> String {
    chrono::Utc::now()
        .with_timezone(hkt)
        .format("%Y-%m-%d")
        .to_string()
}

fn open_writer(path: &str, schema: Arc<Schema>) -> anyhow::Result<ArrowWriter<File>> {
    let file = File::create(path)?; // safe — filename is unique per run
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    Ok(ArrowWriter::try_new(file, schema, Some(props))?)
}

// ---------------------------------------------------------------------------
// Row buffer
// ---------------------------------------------------------------------------

struct Buffer {
    assets: Vec<String>,
    prices: Vec<f64>,
    ts_ms: Vec<i64>,
    ts_hkt: Vec<String>,
}

impl Buffer {
    fn new() -> Self {
        Self {
            assets: Vec::with_capacity(FLUSH_EVERY),
            prices: Vec::with_capacity(FLUSH_EVERY),
            ts_ms: Vec::with_capacity(FLUSH_EVERY),
            ts_hkt: Vec::with_capacity(FLUSH_EVERY),
        }
    }

    fn push(&mut self, asset: &str, price: f64, ts_ms: i64, ts_hkt: String) {
        self.assets.push(asset.to_owned());
        self.prices.push(price);
        self.ts_ms.push(ts_ms);
        self.ts_hkt.push(ts_hkt);
    }

    fn len(&self) -> usize {
        self.assets.len()
    }

    fn flush(&mut self, writer: &mut ArrowWriter<File>, schema: Arc<Schema>) -> anyhow::Result<()> {
        if self.assets.is_empty() {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(self.assets.clone())),
                Arc::new(Float64Array::from(self.prices.clone())),
                Arc::new(Int64Array::from(self.ts_ms.clone())),
                Arc::new(StringArray::from(self.ts_hkt.clone())),
            ],
        )?;
        writer.write(&batch)?;
        writer.flush()?;
        self.assets.clear();
        self.prices.clear();
        self.ts_ms.clear();
        self.ts_hkt.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------

pub async fn run() -> anyhow::Result<()> {
    let hkt = FixedOffset::east_opt(8 * 3600).unwrap();
    let schema = schema();

    fs::create_dir_all(OUTPUT_DIR)?;

    let mut current_date = today_date(&hkt);
    let mut current_path = make_path(&hkt);
    let mut writer = open_writer(&current_path, schema.clone())?;
    let mut buf = Buffer::new();
    let mut last_flush = Instant::now();

    // Time-based flush ticker
    let mut flush_ticker = time::interval(Duration::from_secs(FLUSH_SECS));
    flush_ticker.tick().await; // consume the immediate first tick

    eprintln!("Writing parquet to: {current_path}");

    let client = Client::default();
    let stream = client.subscribe_chainlink_prices(None)?;
    let mut stream = Box::pin(stream);

    loop {
        tokio::select! {
            // --- Price update from RTDS ---
            maybe_result = stream.next() => {
                let Some(result) = maybe_result else { break };

                match result {
                    Ok(price) => {
                        let sym = price.symbol.to_lowercase();
                        let dt = hkt.timestamp_millis_opt(price.timestamp).unwrap();
                        let ts_hkt = dt.format("%Y-%m-%d %H:%M:%S HKT").to_string();

                        if let Some((_, asset)) = SYMBOLS.iter().find(|(s, _)| *s == sym) {
                            // 1. Stdout → Python bot.py
                            println!(
                                "{{\"asset\":\"{asset}\",\"price\":{},\"ts\":\"{ts_hkt}\"}}",
                                price.value,
                            );

                            // 2. Buffer for parquet
                            let price_f64: f64 = price.value.try_into().unwrap_or(f64::NAN);
                            buf.push(asset, price_f64, price.timestamp, ts_hkt);

                            // Midnight HKT rotation
                            let date = today_date(&hkt);
                            if date != current_date {
                                buf.flush(&mut writer, schema.clone())?;
                                writer.close()?;
                                current_date = date;
                                current_path = make_path(&hkt);
                                writer = open_writer(&current_path, schema.clone())?;
                                eprintln!("Rotated → {current_path}");
                            }

                            // Row-count flush
                            if buf.len() >= FLUSH_EVERY {
                                buf.flush(&mut writer, schema.clone())?;
                                last_flush = Instant::now();
                                eprintln!("Flushed {FLUSH_EVERY} rows → {current_path}");
                            }
                        }
                    }
                    Err(e) => eprintln!("RTDS error: {e}"),
                }
            }

            // --- Time-based flush (every FLUSH_SECS seconds) ---
            _ = flush_ticker.tick() => {
                if !buf.assets.is_empty() {
                    buf.flush(&mut writer, schema.clone())?;
                    eprintln!(
                        "Time-based flush ({} rows, {}s since last) → {current_path}",
                        buf.len(),
                        last_flush.elapsed().as_secs(),
                    );
                    last_flush = Instant::now();
                }
            }

            // --- Graceful shutdown on Ctrl-C ---
            _ = tokio::signal::ctrl_c() => {
                eprintln!("Ctrl-C received — flushing and closing...");
                break;
            }
        }
    }

    // Final flush before exit
    buf.flush(&mut writer, schema)?;
    writer.close()?;
    eprintln!("Shutdown complete. Parquet closed: {current_path}");

    Ok(())
}
