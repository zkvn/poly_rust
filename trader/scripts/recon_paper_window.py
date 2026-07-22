"""
recon_paper_window.py — one-off driver for a custom-window, paper-mode trade
reconciliation. Not part of the daily cron path (trade_reconcile.py --today,
real-money live_trades_*.csv, 8pm-HKT-anchored windows) — this is a separate
script specifically so nothing here can affect that production path.

Reuses trade_reconcile.py's already-tested functions directly (find_trade_logs,
load_and_filter, annotate_rows, compute_performance_stats,
_safe_run_backtest_reconciliation, write_markdown_summary) against:
  - pattern="paper_trades_*.csv" (paper trades, not live_trades_*.csv)
  - a custom window (deploy time -> now), not an 8pm-HKT-anchored 24h window

See trader/doc/plan_aggressive_taker_entry_2026-07-21.md §6 and
trader/doc/recon_taker_entry_24h_2026-07-22.md (trader-facing companion doc,
written by hand from this run's data + live.log's own p_up readings, since
per-trade p(up) isn't in any column this script's CSV parsing covers).

Usage:
    python recon_paper_window.py "2026-07-21 12:56:00" "2026-07-22 13:32:00"
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from trade_reconcile import (  # noqa: E402
    HKT, REPO_ROOT, PRICE_FEED_SYNC_SCRIPT, PRICE_FEED_RAW_DIR,
    find_trade_logs, load_and_filter, parse_gamma_timeout_events,
    annotate_rows, compute_performance_stats, _safe_run_backtest_reconciliation,
    write_markdown_summary, safe_check_data_quality, console,
)


def main():
    start_str, end_str = sys.argv[1], sys.argv[2]
    window_start = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=HKT)
    window_end = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=HKT)
    console.print(f"[dim]Paper window: {window_start} -> {window_end}[/dim]")

    log_dir = REPO_ROOT / "live_logs"
    # Deliberately NOT results/daily_recon/ — write_markdown_summary's filename
    # is derived only from window_start/window_end dates, so a paper-mode run
    # over the same calendar window as the production live_trades_*.csv cron
    # recon would silently overwrite that real report (caught 2026-07-22: this
    # script's first run clobbered a same-day "0 real trades" production
    # report before it was caught and restored via git checkout).
    out_dir = REPO_ROOT / "results" / "daily_recon_paper"

    console.print("[dim]Syncing price_feed/raw from Oracle for the data quality check...[/dim]")
    sync_ok = False
    try:
        from trade_reconcile import sync_price_feed_from_oracle
        sync_ok = sync_price_feed_from_oracle(PRICE_FEED_SYNC_SCRIPT)
    except Exception as e:
        console.print(f"[yellow]sync failed: {e}[/yellow]")
    data_quality_result = safe_check_data_quality(PRICE_FEED_RAW_DIR, window_start, window_end) if sync_ok else None

    files = find_trade_logs(log_dir, pattern="paper_trades_*.csv")
    if not files:
        console.print("[yellow]No paper_trades_*.csv found[/yellow]")
        raise SystemExit(1)

    rows = load_and_filter(files, window_start.timestamp(), window_end.timestamp())
    console.print(f"[dim]Loaded {len(rows)} paper trades after filtering & dedup.[/dim]")

    gamma_timeout_events = parse_gamma_timeout_events(log_dir / "live.log")
    annotated, summary = annotate_rows(rows, gamma_timeout_events)
    perf_stats = compute_performance_stats(annotated)

    bt_result = _safe_run_backtest_reconciliation(window_start, window_end, annotated)

    md_path = write_markdown_summary(
        summary, perf_stats, window_start, window_end, out_dir,
        bt_result=bt_result, data_quality_result=data_quality_result,
        source_pattern="paper_trades_*.csv",
    )
    console.print(f"[green]Wrote {md_path}[/green]")


if __name__ == "__main__":
    main()
