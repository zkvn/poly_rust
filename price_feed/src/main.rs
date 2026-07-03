mod chainlink;
mod collect;
mod markets;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "price_feed")]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Stream 5-min Up/Down market prices with a live TUI
    Markets {
        /// Assets to track, e.g. btc sol eth bnb
        #[arg(required = true, num_args = 1..)]
        assets: Vec<String>,
        /// Skip the best_bid_ask custom-feature feed; use price_change directly
        #[arg(long)]
        no_custom_features: bool,
        /// Headless: print prices to stdout for 30s instead of the TUI (for debugging)
        #[arg(long)]
        probe: bool,
    },
    /// Headless CLOB data collector — writes raw/*.parquet files
    Collect {
        /// Assets to collect; omit to auto-discover from Polymarket (recommended)
        #[arg(num_args = 0..)]
        assets: Vec<String>,
        /// Base name for the 5-min (and binance) output dir; _15_mins/_4hr
        /// suffixes are appended for those durations. Use a scratch value
        /// (e.g. raw_new) to run alongside a production collector without
        /// colliding with its output.
        #[arg(long, default_value = "raw")]
        raw_dir: String,
        /// Publish live ticks to NATS (e.g. nats://localhost:4222); omit to disable
        #[arg(long)]
        nats_url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install ring as the default rustls crypto provider once for the whole process.
    // Required for rustls ≥0.22 when multiple crates (reqwest, tokio-tungstenite) share rustls.
    let _ = rustls::crypto::ring::default_provider().install_default();
    match Cli::parse().command {
        None => chainlink::run().await,
        Some(Cmd::Markets { assets, no_custom_features, probe }) => {
            let custom_features = !no_custom_features;
            if probe {
                markets::probe(assets, custom_features).await
            } else {
                markets::run(assets, custom_features).await
            }
        }
        Some(Cmd::Collect { assets, raw_dir, nats_url }) => collect::run(assets, &raw_dir, nats_url).await,
    }
}
