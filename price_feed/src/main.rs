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
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        Some(Cmd::Collect { assets }) => collect::run(assets).await,
    }
}
