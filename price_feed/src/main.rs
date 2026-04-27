//! Chainlink price feed via Polymarket RTDS v2.
//!
//! Subscribes to BTC/USD, ETH/USD, SOL/USD, BNB/USD from
//! wss://ws-live-data.polymarket.com and emits one JSON line per update:
//!   {"asset":"BTC","price":67234.5}
//!
//! Python bot.py reads these from stdout.

use chrono::{FixedOffset, TimeZone};
use futures::StreamExt as _;
use polymarket_client_sdk_v2::rtds::Client;
// Chainlink slash-format symbols → bot asset labels
const SYMBOLS: &[(&str, &str)] = &[
    ("btc/usd", "BTC"),
    ("eth/usd", "ETH"),
    ("sol/usd", "SOL"),
    ("bnb/usd", "BNB"),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::default(); // wss://ws-live-data.polymarket.com

    // Subscribe to all Chainlink symbols at once (None = no filter),
    // then filter locally to our 4 assets.
    let stream = client.subscribe_chainlink_prices(None)?;
    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(price) => {
                let sym = price.symbol.to_lowercase();

                let hkt = FixedOffset::east_opt(8 * 3600).unwrap();
                let dt = hkt.timestamp_millis_opt(price.timestamp).unwrap();
                let ts_str = dt.format("%Y-%m-%d %H:%M:%S HKT").to_string();

                if let Some((_, asset)) = SYMBOLS.iter().find(|(s, _)| *s == sym) {
                    // price.value is rust_decimal::Decimal — Display prints it cleanly
                    println!(
                        "{{\"asset\":\"{asset}\",\"price\":{},\"ts\":\"{ts_str}\"}}",
                        price.value,
                    );
                }
            }
            Err(e) => eprintln!("RTDS error: {e}"),
        }
    }

    Ok(())
}
