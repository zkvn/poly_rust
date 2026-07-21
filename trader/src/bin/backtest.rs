use clap::{Parser, ValueEnum};
use trader::backtest::{load_price_data_for_duration, run_backtest};
use trader::config::{load_file, load_latest};
use trader::types::TradeRecord;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Aligned, human-readable table (default) — unchanged from before this
    /// flag existed.
    Table,
    /// `slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts` —
    /// header always printed, one row per trade, no summary line. For
    /// scripts (e.g. trader/scripts/trade_reconcile.py's backtest
    /// reconciliation) to parse via csv.DictReader instead of regexing the
    /// table. `entry_ts` (added for the Entry Time / T-seconds column in
    /// trade_reconcile.py's BT reconciliation tables) is appended last to
    /// keep the first 7 columns byte-stable for any other consumer.
    Csv,
}

#[derive(Parser, Debug)]
#[command(
    name = "backtest",
    about = "Rust backtest — mirrors Python bot.backtest bt1"
)]
struct Args {
    #[arg(long, help = "Asset (e.g. BTC, ETH)")]
    asset: String,

    #[arg(long, help = "Date YYYY-MM-DD (HKT)")]
    date: String,

    #[arg(
        long,
        default_value = "/home/kev/apps/btc_5mins/prices",
        help = "Directory containing <ASSET>_binance.parquet / <ASSET>_poly.parquet"
    )]
    prices_dir: String,

    #[arg(
        long,
        default_value = "/home/kev/apps/btc_5mins/config",
        help = "Directory containing strategy_*.toml — ignored if --config-file is given"
    )]
    config_dir: String,

    #[arg(
        long,
        help = "Load this exact strategy_*.toml instead of --config-dir's lexicographically-latest \
                file — pins a specific historical config (e.g. trade_reconcile.py's BT \
                reconciliation, reconciling a past window against the config that was actually \
                live then, not today's)"
    )]
    config_file: Option<String>,

    #[arg(long, help = "Disable halt (sets halt_rev=halt_prob=0)")]
    no_halt: bool,

    /// Market duration to replay: "5m" (default — today's exact filenames and
    /// behavior), "15m", or "4h" (load the `{ASSET}_poly_{dur}_{date}.parquet`
    /// files `build_backtest_prices.py --source` writes, resolve `@{dur}`
    /// config overrides). Hourly-ET/weather have no recorded ticks — not
    /// accepted here. See trader/doc/feature_new_markets_2026-07-17.md §6.
    #[arg(long, default_value = "5m")]
    duration: String,

    #[arg(long, value_enum, default_value_t = OutputFormat::Table, help = "Output format")]
    format: OutputFormat,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !matches!(args.duration.as_str(), "5m" | "15m" | "4h") {
        anyhow::bail!(
            "--duration must be one of 5m/15m/4h (got `{}`) — hourly-ET and weather \
             markets have no recorded tick history to replay",
            args.duration
        );
    }

    let toml = match &args.config_file {
        Some(path) => load_file(path)?,
        None => load_latest(&args.config_dir)?,
    };
    // For "5m" this is exactly `toml.resolve(&args.asset)` — see
    // config.rs::resolve_for_duration.
    let mut params = toml.resolve_for_duration(&args.asset, &args.duration)?;
    if args.no_halt {
        params.halt_rev = 0;
        params.halt_prob = 0;
        params.halt_v = 0;
    }

    let (b_rows, p_rows) =
        load_price_data_for_duration(&args.asset, &args.date, &args.prices_dir, &args.duration)?;
    let trades = run_backtest(&params, b_rows, p_rows);

    let out = match args.format {
        OutputFormat::Csv => format_csv(&trades),
        OutputFormat::Table => format_table(&trades),
    };
    print!("{out}");

    Ok(())
}

/// `slug,strategy,side,token_price,exit_price,outcome,pnl` — header always
/// present (even with zero trades) so a script's `csv.DictReader` never has
/// to special-case an empty result.
fn format_csv(trades: &[TradeRecord]) -> String {
    let mut out = String::from("slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts\n");
    for t in trades {
        out.push_str(&format!(
            "{},{},{},{:.6},{:.6},{},{:.6},{:.3}\n",
            t.slug,
            t.strategy,
            t.side.as_str(),
            t.token_price,
            t.exit_price,
            t.outcome.as_str(),
            t.pnl,
            t.entry_ts
        ));
    }
    out
}

fn format_table(trades: &[TradeRecord]) -> String {
    if trades.is_empty() {
        return "No trades.\n".to_string();
    }

    let mut out = String::new();
    out.push_str(&format!(
        "{:<35} {:<10} {:<5} {:>10} {:>14} {:<10} {:>8}\n",
        "slug", "strategy", "side", "token_px", "exit_token_px", "outcome", "pnl"
    ));
    out.push_str(&"-".repeat(100));
    out.push('\n');

    let mut total_pnl = 0.0_f64;
    let (mut wins, mut losses, mut stoplosses, mut unwinds, mut timeouts) = (0usize, 0, 0, 0, 0);

    for t in trades {
        out.push_str(&format!(
            "{:<35} {:<10} {:<5} {:>10.3} {:>14.3} {:<10} {:>8.4}\n",
            t.slug,
            t.strategy,
            t.side.as_str(),
            t.token_price,
            t.exit_price,
            t.outcome.as_str(),
            t.pnl
        ));
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
    out.push_str(&format!(
        "\nTotal: trades={} wins={} losses={} stoplosses={} unwinds={} timeouts={} pnl={}\n",
        trades.len(),
        wins,
        losses,
        stoplosses,
        unwinds,
        timeouts,
        total_pnl
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use trader::types::{Outcome, Side};

    fn sample_trade() -> TradeRecord {
        TradeRecord {
            slug: "eth-updown-5m-1783046100".to_string(),
            cycle_start: 1783046100.0,
            strategy: "high_prob",
            side: Side::Up,
            entry_ts: 1783046105.0,
            entry_price_ts: 1783046105.0,
            token_price: 0.93,
            entry_signal_price: 0.93,
            exit_price: 1.0,
            outcome: Outcome::Win,
            pnl: 0.0753,
            exit_attempts: 0,
            exit_last_error: None,
            entry_signal_latency_ms: 0.0,
            entry_process_latency_ms: 0.0,
            exit_signal_latency_ms: 0.0,
            exit_process_latency_ms: 0.0,
        }
    }

    #[test]
    fn csv_header_always_present_even_with_no_trades() {
        assert_eq!(
            format_csv(&[]),
            "slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts\n"
        );
    }

    #[test]
    fn csv_row_matches_trade_fields() {
        let out = format_csv(&[sample_trade()]);
        let mut lines = out.lines();
        assert_eq!(
            lines.next().unwrap(),
            "slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts"
        );
        assert_eq!(
            lines.next().unwrap(),
            "eth-updown-5m-1783046100,high_prob,UP,0.930000,1.000000,WIN,0.075300,1783046105.000"
        );
        assert!(lines.next().is_none(), "csv format prints no summary line");
    }

    #[test]
    fn csv_rows_have_no_trailing_summary_or_separator_lines() {
        // Regression guard: format_table has a "---" separator + "Total: ..."
        // trailer that format_csv must never grow, or trade_reconcile.py's
        // csv.DictReader would choke parsing those as data rows.
        let out = format_csv(&[sample_trade(), sample_trade()]);
        assert_eq!(out.lines().count(), 3); // header + 2 rows
        assert!(!out.contains("Total:"));
        assert!(!out.contains("---"));
    }

    #[test]
    fn table_format_prints_no_trades_message_when_empty() {
        assert_eq!(format_table(&[]), "No trades.\n");
    }

    #[test]
    fn table_format_includes_totals_line() {
        let out = format_table(&[sample_trade()]);
        assert!(out.contains("eth-updown-5m-1783046100"));
        assert!(out.contains("Total: trades=1 wins=1 losses=0 stoplosses=0 unwinds=0 timeouts=0"));
    }
}
