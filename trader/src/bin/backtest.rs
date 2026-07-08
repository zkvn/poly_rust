use clap::Parser;
use trader::backtest::{load_price_data, run_backtest};
use trader::config::load_latest;

#[derive(Parser, Debug)]
#[command(name = "backtest", about = "Rust backtest — mirrors Python bot.backtest bt1")]
struct Args {
    #[arg(long, help = "Asset (e.g. BTC, ETH)")]
    asset: String,

    #[arg(long, help = "Date YYYY-MM-DD (HKT)")]
    date: String,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/prices",
          help = "Directory containing <ASSET>_binance.parquet / <ASSET>_poly.parquet")]
    prices_dir: String,

    #[arg(long, default_value = "/home/kev/apps/btc_5mins/config",
          help = "Directory containing strategy_*.toml")]
    config_dir: String,

    #[arg(long, help = "Disable halt (sets halt_rev=halt_prob=0)")]
    no_halt: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let toml = load_latest(&args.config_dir)?;
    let mut params = toml.resolve(&args.asset)?;
    if args.no_halt {
        params.halt_rev = 0;
        params.halt_prob = 0;
    }

    let (b_rows, p_rows) = load_price_data(&args.asset, &args.date, &args.prices_dir)?;
    let trades = run_backtest(&params, b_rows, p_rows);

    if trades.is_empty() {
        println!("No trades.");
        return Ok(());
    }

    // Header
    println!("{:<35} {:<10} {:<5} {:>10} {:>14} {:<10} {:>8}",
        "slug", "strategy", "side", "token_px", "exit_token_px", "outcome", "pnl");
    println!("{}", "-".repeat(100));

    let mut total_pnl = 0.0_f64;
    let (mut wins, mut losses, mut stoplosses, mut unwinds, mut timeouts) = (0usize, 0, 0, 0, 0);

    for t in &trades {
        let outcome_str = t.outcome.as_str();
        println!("{:<35} {:<10} {:<5} {:>10.3} {:>14.3} {:<10} {:>8.4}",
            t.slug, t.strategy, t.side.as_str(), t.token_price, t.exit_price, outcome_str, t.pnl);
        total_pnl += t.pnl;
        match t.outcome {
            trader::types::Outcome::Win => wins += 1,
            trader::types::Outcome::Loss => losses += 1,
            trader::types::Outcome::StopLoss => stoplosses += 1,
            trader::types::Outcome::Unwind => unwinds += 1,
            // machine.rs (this replay engine) doesn't implement unwind_time —
            // only worker.rs (the live driver) does — so this never fires today.
            // Counted anyway for exhaustiveness and in case that changes later.
            trader::types::Outcome::Timeout => timeouts += 1,
        }
    }

    let total_pnl = (total_pnl * 10_000.0).round() / 10_000.0;
    println!("\nTotal: trades={} wins={} losses={} stoplosses={} unwinds={} timeouts={} pnl={}",
        trades.len(), wins, losses, stoplosses, unwinds, timeouts, total_pnl);

    Ok(())
}
