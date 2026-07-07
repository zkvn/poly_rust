// B2 — live CLOB validation probe (plan §12 Track B, B2).
//
// Standalone binary, deliberately separate from the trading engine: it only
// proves execution.rs's LiveExecutionEngine round-trips a real order against
// production (there is no Polymarket sandbox for these markets). Reads
// POLY_PRIVATE_KEY from a .env file (never printed); FUND_ADDRESS defaults to
// the same address bot/config.py hardcodes for this account.
//
//   cargo run --bin api_probe -- balance
//   cargo run --bin api_probe -- roundtrip --asset DOGE --size-usdc 1.0

use std::str::FromStr as _;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::Address;

use trader::execution::{ExecutionEngine, LiveConfig, LiveExecutionEngine, local_signer_from_key, signature_type_from_env};
use trader::marketdata::{clob_client, current_slot, fetch_meta, http_client, make_slug};

const DEFAULT_FUND_ADDRESS: &str = "0x9FC2A777C26CCA2C218D8E7BBC340D14058CC13A";
// NOTE: clob-v2.polymarket.com 301-redirects POST /order to clob.polymarket.com,
// and that redirect silently downgrades POST to GET (standard client behavior
// for 301), which turns into a 405 on the real endpoint. Use the real host
// directly — same one bot/config.py's CLOB_HOST has always pointed at.
const CLOB_HOST: &str = "https://clob.polymarket.com";

#[derive(Parser, Debug)]
#[command(name = "api_probe", about = "Live CLOB validation probe (B2) — real orders, tiny size")]
struct Args {
    #[arg(long, default_value = "/home/kev/apps/btc_5mins/.env")]
    env_file: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Read-only: authenticate and print USDC balance/allowance. No order placed.
    Balance,
    /// Place a tiny market BUY then immediately close it (market SELL) on a
    /// live 5-min up/down market. Costs roughly the bid-ask spread on the
    /// requested size, nothing more.
    Roundtrip {
        #[arg(long, default_value = "DOGE")]
        asset: String,
        #[arg(long, default_value_t = 1.0)]
        size_usdc: f64,
    },
}

fn load_env(path: &str) -> Result<()> {
    dotenvy::from_path(path).with_context(|| format!("load {path}"))?;
    Ok(())
}

fn private_key() -> Result<String> {
    std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set in env file")
}

fn fund_address() -> Result<Address> {
    let raw = std::env::var("FUND_ADDRESS").unwrap_or_else(|_| DEFAULT_FUND_ADDRESS.to_string());
    Address::from_str(&raw).with_context(|| format!("parse FUND_ADDRESS {raw}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    load_env(&args.env_file)?;

    let key = private_key()?;
    let signer = local_signer_from_key(&key)?;
    let funder = fund_address()?;
    let signature_type = signature_type_from_env()?;
    println!("[probe] signer (EOA) address: {}", signer.address());
    println!("[probe] funder (FUND_ADDRESS): {funder}");
    println!("[probe] signature_type: {signature_type:?}");

    // Route CLOB writes through the EC2 HTTP proxy when running from a geo-restricted
    // region (same var that Python's _patch_clob_proxy reads; empty = direct connect).
    if let Ok(proxy_url) = std::env::var("CLOB_PROXY_URL")
        && !proxy_url.is_empty() {
        // Safety: single-threaded here — tokio runtime not yet spawning work.
        unsafe { std::env::set_var("HTTPS_PROXY", &proxy_url) };
        println!("[probe] routing CLOB writes via proxy: {proxy_url}");
    }

    match args.cmd {
        Cmd::Balance => {
            let client = Client::new(CLOB_HOST, Config::default())?
                .authentication_builder(&signer)
                .funder(funder)
                .signature_type(signature_type)
                .authenticate()
                .await?;
            let resp = client
                .balance_allowance(BalanceAllowanceRequest::builder().asset_type(AssetType::Collateral).build())
                .await?;
            // API returns collateral balance in base units (6 decimals), matching
            // Python's `float(raw) / 1e6` in BalanceGuard._fetch_balance.
            let raw: f64 = resp.balance.to_string().parse().unwrap_or(0.0);
            println!("balance: {:.4} USDC (raw base units: {})", raw / 1e6, resp.balance);
            println!("allowances: {} contract(s)", resp.allowances.len());
        }

        Cmd::Roundtrip { asset, size_usdc } => {
            println!("=== B2 roundtrip probe: {asset} size=${size_usdc:.2} ===");

            let http = http_client()?;
            let clob = clob_client();
            let slot = current_slot(300);
            let slug = make_slug(&asset, slot, "5m");
            println!("slug: {slug}");

            let (up_id, _dn_id) = fetch_meta(&http, &slug).await?;
            println!("up_token_id: {up_id}");

            // Grab one best_bid_ask sample to get a current price.
            let price = {
                use futures::StreamExt as _;
                let stream = clob.subscribe_best_bid_ask(vec![up_id])?;
                let mut s = Box::pin(stream);
                let sample = tokio::time::timeout(std::time::Duration::from_secs(10), s.next())
                    .await
                    .context("timed out waiting for a price quote")?
                    .context("price stream ended")??;
                let bid: f64 = sample.best_bid.to_string().parse()?;
                let ask: f64 = sample.best_ask.to_string().parse()?;
                (bid + ask) / 2.0
            };
            println!("current up price (midpoint): {price:.4}");

            println!(
                "\nAbout to place a REAL market BUY of ~${size_usdc:.2} at ~{price:.4}, then immediately \
                 close it with a market SELL. Expected cost: roughly the bid-ask spread on ${size_usdc:.2} \
                 (a few cents), not the full size.\n"
            );

            let engine =
                LiveExecutionEngine::connect(CLOB_HOST, signer, funder, signature_type, LiveConfig::default()).await?;

            let buy = engine.place(up_id, price, size_usdc, 0.99).await;
            println!("BUY result: placed={} filled_shares={:.4} cost/share={:.4} error={:?}",
                buy.placed, buy.filled_shares, buy.cost, buy.error);

            if !buy.placed || buy.filled_shares <= 0.0 {
                println!("BUY did not fill — stopping before attempting a close.");
                return Ok(());
            }

            let close = engine.close_position(up_id, buy.filled_shares).await;
            println!("CLOSE result: status={:?} shares_sold={:.4} filled_usdc={:.4}",
                close.status, close.shares_sold, close.filled_usdc);

            let net = close.filled_usdc - (buy.filled_shares * buy.cost);
            println!("\nNet cost of round trip: ${net:.4} (negative = spread cost paid, as expected)");
        }
    }

    Ok(())
}
