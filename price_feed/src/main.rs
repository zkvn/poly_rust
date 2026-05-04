mod chainlink;
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
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        None => chainlink::run().await,
        Some(Cmd::Markets { assets }) => markets::run(assets).await,
    }
}
