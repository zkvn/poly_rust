"""
trade_reconcile.py — poly_rust daily trade reconciliation (Rust trader)

Adapted from btc_5mins/scripts/trade_reconcile.py for this project's simpler
TradeRecord schema (trader/src/types.rs; header is self-healed on read by
`live.rs::append_csv_header_if_new` if an older CSV predates exit_attempts/
exit_last_error — see trader/doc/incident_doge_2026-07-03.md):

    logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error

logged_at/entry_ts are Unix epoch seconds (float, UTC) — window filtering is
plain arithmetic, no HKT string parsing needed. asset is derived from the
slug prefix (e.g. "eth-updown-5m-1783046100" -> ETH); strategy comes straight
from the `strategy` column (worker.rs's EntryType::as_str()). outcome is
already WIN/LOSS/STOPLOSS/UNWIND — worker.rs's Confirming state already
reconciles WIN/LOSS against Polymarket's own ApiResult before logging, so
the Gamma cross-check here is a regression check on that correction logic
(and on STOPLOSS/UNWIND quality), not a "did the algo predict right" check.

Usage:
    python trade_reconcile.py --today            # 24h window anchored 8pm HKT (default)
    python trade_reconcile.py --dt 20260703       # 24h window from 8pm on given date
    python trade_reconcile.py --wallet 0x...      # override FUND_ADDRESS from .env
    python trade_reconcile.py --no-push           # skip git commit+push
"""
import argparse
import csv
import glob
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich import box

REPO_ROOT = Path(__file__).resolve().parent.parent  # trader/
load_dotenv(REPO_ROOT / ".env")
console = Console()

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

HKT = timezone(timedelta(hours=8))

CSV_COLUMNS = [
    "logged_at", "slug", "strategy", "side", "entry_ts", "token_price",
    "exit_price", "outcome", "pnl", "exit_attempts", "exit_last_error",
]

SLUG_ASSET_PREFIX = {
    "btc": "BTC", "eth": "ETH", "sol": "SOL",
    "doge": "DOGE", "xrp": "XRP", "bnb": "BNB", "hype": "HYPE",
}


def asset_from_slug(slug: str) -> str:
    prefix = slug.split("-", 1)[0].lower()
    return SLUG_ASSET_PREFIX.get(prefix, prefix.upper())


def _safe_float(val) -> float:
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Gamma outcome resolution
# ---------------------------------------------------------------------------

def fetch_gamma_outcome(slug: str) -> Optional[str]:
    """Return 'UP' or 'DOWN' once the market is resolved on Gamma, else None."""
    import requests
    try:
        resp = requests.get(f"{GAMMA_API}/events?slug={slug}", timeout=10)
        resp.raise_for_status()
        events = resp.json()
        if not events:
            return None
        markets = events[0].get("markets", [])
        if not markets:
            return None
        mkt = markets[0]
        raw_outcomes = mkt.get("outcomes", "[]")
        raw_prices = mkt.get("outcomePrices", "[]")
        if isinstance(raw_outcomes, str):
            raw_outcomes = json.loads(raw_outcomes)
        if isinstance(raw_prices, str):
            raw_prices = json.loads(raw_prices)
        for outcome, price_str in zip(raw_outcomes, raw_prices):
            if float(price_str) >= 0.99:
                o = str(outcome).strip().upper()
                if o in ("UP", "DOWN"):
                    return o
        return None
    except Exception:
        return None


def _fetch_token_ids_for_slug(slug: str) -> Optional[tuple]:
    import requests
    try:
        resp = requests.get(f"{GAMMA_API}/events?slug={slug}", timeout=10)
        resp.raise_for_status()
        events = resp.json()
        if not events:
            return None
        markets = events[0].get("markets", [])
        if not markets:
            return None
        ids = markets[0].get("clobTokenIds", [])
        if isinstance(ids, str):
            ids = json.loads(ids)
        if len(ids) < 2:
            return None
        return ids[0], ids[1]  # up, dn
    except Exception:
        return None


def _fetch_clob_price_history(token_id: str) -> list:
    import requests
    for fidelity in (1, 60):
        try:
            resp = requests.get(
                f"{CLOB_API}/prices-history",
                params={"market": token_id, "interval": "1m", "fidelity": fidelity},
                timeout=10,
            )
            resp.raise_for_status()
            hist = resp.json().get("history", [])
            if hist:
                return sorted(hist, key=lambda x: x["t"])
        except Exception:
            pass
    return []


# ---------------------------------------------------------------------------
# Load + filter trade logs
# ---------------------------------------------------------------------------

def find_trade_logs(log_dir: Path) -> list:
    return sorted(log_dir.glob("live_trades_*.csv"))


def load_and_filter(paths: list, from_ts: float, to_ts: float) -> list:
    """Load all rows, filter to [from_ts, to_ts) on logged_at, dedupe on full row."""
    all_rows = []
    for p in paths:
        warned = False
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if None in row and not warned:
                    # DictReader silently dumps columns beyond the header count into
                    # row[None] instead of erroring — this is exactly how the "Failed
                    # Exit Attempts" report went quiet for a while: a stale 9-column
                    # header meant exit_attempts/exit_last_error always landed here
                    # instead of under their real names, so row.get("exit_attempts")
                    # always returned None (see trader/doc/incident_doge_2026-07-03.md).
                    # live.rs::append_csv_header_if_new now self-heals this on the
                    # trader's next restart, but warn loudly if it's still stale.
                    print(f"WARNING: {p.name} has more columns than its header "
                          f"(extra fields {row[None]!r}) — exit_attempts/exit_last_error "
                          f"may be misreported until the trader restarts and heals it",
                          file=sys.stderr)
                    warned = True
                all_rows.append(row)

    filtered = []
    for row in all_rows:
        try:
            logged_at = float(row.get("logged_at", ""))
        except (TypeError, ValueError):
            continue
        if from_ts <= logged_at < to_ts:
            filtered.append(row)

    seen, deduped = set(), []
    for row in filtered:
        # Use only the known schema columns for the dedup key — a malformed row
        # (unescaped comma in exit_last_error) makes DictReader stuff overflow
        # fields into row[None] as a list, which isn't hashable inside a tuple
        # and previously crashed this loop mid-run (see 18:20 2026-07-03 cron log).
        key = tuple(row.get(c) for c in CSV_COLUMNS)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def annotate_rows(rows: list) -> tuple:
    """Cross-check WIN/LOSS rows against Gamma; classify STOPLOSS/UNWIND quality.

    Returns (annotated_rows, summary) where summary has direction (resolved/
    correct/wrong/accuracy/pending) and stoploss (good/costly) counts —
    mirroring btc_5mins's shape so downstream markdown rendering matches.
    """
    slugs = sorted({row["slug"] for row in rows if row.get("slug")})
    outcome_map = {}
    for slug in slugs:
        result = fetch_gamma_outcome(slug)
        status = result or "PENDING"
        console.print(f"[dim]  {slug}: {status}[/dim]")
        if result is not None:
            outcome_map[slug] = result

    matches = mismatches = pending = 0
    good_stops = costly_stops = 0
    mismatch_details = []
    annotated = []

    for row in rows:
        slug = row.get("slug", "")
        side = row.get("side", "").strip().upper()
        outcome = row.get("outcome", "").strip().upper()
        actual = outcome_map.get(slug, "PENDING")

        if actual == "PENDING":
            actual_result = "PENDING"
            pending += 1
        elif outcome == "STOPLOSS":
            actual_result = "WIN" if side == actual else "LOSS"
            if actual_result == "WIN":
                costly_stops += 1
            else:
                good_stops += 1
        elif outcome == "UNWIND":
            # Unwind is an exit, not a directional prediction — no match/mismatch,
            # but still worth knowing whether the underlying side would've won.
            actual_result = "WIN" if side == actual else "LOSS"
        else:
            actual_result = "WIN" if side == actual else "LOSS"
            if actual_result == outcome:
                matches += 1
            else:
                mismatches += 1
                mismatch_details.append({
                    "time": datetime.fromtimestamp(float(row["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
                    "slug": slug, "side": side, "algo": outcome, "actual": actual_result,
                })

        row = dict(row)
        row["actual_result"] = actual_result
        row["asset"] = asset_from_slug(slug)
        annotated.append(row)

    resolved = matches + mismatches
    summary = {
        "total_rows": len(annotated),
        "direction": {
            "resolved": resolved, "correct": matches, "wrong": mismatches,
            "accuracy": (matches / resolved * 100) if resolved else 0,
            "pending": pending,
        },
        "stoploss": {"good": good_stops, "costly": costly_stops},
        "mismatch_details": mismatch_details,
    }
    return annotated, summary


# ---------------------------------------------------------------------------
# Performance stats
# ---------------------------------------------------------------------------

def compute_performance_stats(rows: list) -> dict:
    if not rows:
        return {}

    n = len(rows)
    sides = _counter(rows, "side")
    outcomes = _counter(rows, "outcome")
    assets = _counter(rows, "asset")
    strategies = _counter(rows, "strategy")
    pnl_total = sum(_safe_float(r.get("pnl")) for r in rows)

    ts_sorted = sorted(rows, key=lambda r: _safe_float(r.get("logged_at")))
    span_first = datetime.fromtimestamp(_safe_float(ts_sorted[0]["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S")
    span_last = datetime.fromtimestamp(_safe_float(ts_sorted[-1]["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S")

    def _breakdown(rows_, key_fn):
        groups = defaultdict(list)
        for r in rows_:
            groups[key_fn(r)].append(r)
        out = {}
        for key, subset in sorted(groups.items()):
            wins = sum(1 for r in subset if r["outcome"] == "WIN")
            losses = sum(1 for r in subset if r["outcome"] == "LOSS")
            sl = sum(1 for r in subset if r["outcome"] == "STOPLOSS")
            unwind = sum(1 for r in subset if r["outcome"] == "UNWIND")
            total = len(subset)
            pnl = sum(_safe_float(r.get("pnl")) for r in subset)
            out[key] = {
                "total": total, "wins": wins, "losses": losses, "sl": sl, "unwind": unwind,
                "win_rate": f"{(wins + unwind) / total * 100:.1f}%" if total else "—",
                "pnl": pnl,
            }
        return out

    asset_stats = _breakdown(rows, lambda r: r["asset"])
    strategy_breakdown = _breakdown(rows, lambda r: (r["asset"], r["strategy"]))

    sl_detail = []
    for r in rows:
        if r.get("outcome", "").upper() != "STOPLOSS":
            continue
        actual = r.get("actual_result", "").upper()
        quality = "COSTLY" if actual == "WIN" else "GOOD"
        sl_detail.append({
            "time": datetime.fromtimestamp(_safe_float(r["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
            "asset": r["asset"], "strategy": r["strategy"], "side": r["side"],
            "pnl": _safe_float(r.get("pnl")),
            "token_price": _safe_float(r.get("token_price")),
            "exit_price": _safe_float(r.get("exit_price")),
            "quality": quality,
        })
    sl_detail.sort(key=lambda x: x["time"])

    trade_history = []
    for r in rows:
        trade_history.append({
            "time": datetime.fromtimestamp(_safe_float(r["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
            "asset": r["asset"], "strategy": r["strategy"], "side": r["side"],
            "outcome": r["outcome"], "token_price": _safe_float(r.get("token_price")),
            "exit_price": _safe_float(r.get("exit_price")), "pnl": _safe_float(r.get("pnl")),
        })
    trade_history.sort(key=lambda t: t["time"])

    sl_unwind_rows = [r for r in rows if r.get("outcome", "").upper() in ("STOPLOSS", "UNWIND")]

    # WIN/LOSS rows that had a failed early-exit attempt before falling back to
    # hold-to-resolution — invisible in the trade history alone (looks like a
    # clean hold). See trader/doc/audit_trades_2026-07-03.md for the original gap.
    failed_exit_rows = [
        r for r in rows
        if r.get("outcome", "").upper() in ("WIN", "LOSS") and int(r.get("exit_attempts") or 0) > 0
    ]

    return {
        "span_first": span_first, "span_last": span_last, "total_rows": n,
        "sides": sides, "outcomes": outcomes, "assets": assets, "strategies": strategies,
        "pnl_total": pnl_total, "asset_stats": asset_stats,
        "strategy_breakdown": strategy_breakdown,
        "sl_detail": sl_detail, "trade_history": trade_history,
        "sl_unwind_rows": sl_unwind_rows, "failed_exit_rows": failed_exit_rows,
    }


def _counter(rows, key):
    c = defaultdict(int)
    for r in rows:
        c[r.get(key, "")] += 1
    return dict(c)


def _fmt_counter(d: dict) -> str:
    return ", ".join(f"{k}: {v}" for k, v in d.items())


# ---------------------------------------------------------------------------
# Stoploss & Unwind audit (CLOB price history around the trade)
# ---------------------------------------------------------------------------

def _build_sl_unwind_audit(rows: list) -> list:
    results = []
    for row in rows:
        outcome = row.get("outcome", "").strip().upper()
        if outcome not in ("STOPLOSS", "UNWIND"):
            continue

        slug = row.get("slug", "")
        side = row.get("side", "").strip().upper()
        is_up = side == "UP"
        entry_ts = _safe_float(row.get("entry_ts"))
        token_price = _safe_float(row.get("token_price"))
        exit_price = _safe_float(row.get("exit_price"))
        pnl = _safe_float(row.get("pnl"))
        actual = row.get("actual_result", "").upper()

        if outcome == "STOPLOSS":
            quality = "COSTLY ✗" if actual == "WIN" else "GOOD ✓"
        else:
            quality = "WIN-EQUIVALENT" if actual == "WIN" else "LOSS-UNWIND"

        audit = {
            "time": datetime.fromtimestamp(_safe_float(row["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
            "asset": row.get("asset", ""), "side": side, "strategy": row.get("strategy", ""),
            "outcome": outcome, "quality": quality,
            "token_price": token_price, "exit_price": exit_price, "pnl": pnl,
            "exit_attempts": int(row.get("exit_attempts") or 0),
            "exit_last_error": row.get("exit_last_error", ""),
            "clob_history": [],
        }

        if entry_ts:
            token_ids = _fetch_token_ids_for_slug(slug)
            if token_ids:
                token_id = token_ids[0] if is_up else token_ids[1]
                hist = _fetch_clob_price_history(token_id)
                window = 900  # +/- 15 min
                nearby = [h for h in hist if abs(h["t"] - entry_ts) <= window][:30]
                if nearby:
                    audit["clob_history"] = [
                        {"time_hkt": datetime.fromtimestamp(h["t"], tz=HKT).strftime("%H:%M:%S"), "price": h["p"]}
                        for h in nearby
                    ]

        results.append(audit)
    return results


def _render_sl_unwind_audit(lines: list, audit: list) -> None:
    for entry in audit:
        lines.append(f"### {entry['outcome']}: {entry['asset']} {entry['side']} @ {entry['time']} ({entry['strategy']})")
        lines.append("")
        lines.append(f"**Verdict:** {entry['quality']}")
        lines.append("")
        lines.extend([
            "| Field | Value |",
            "|---|---|",
            f"| Entry token price | {entry['token_price']:.4f} |",
            f"| Exit price        | {entry['exit_price']:.4f} |",
            f"| PnL               | {entry['pnl']:+.4f} |",
            f"| Failed attempts before this exit | {entry['exit_attempts']} |",
        ])
        if entry["exit_attempts"]:
            lines.append(f"| Last error before exit | {entry['exit_last_error']} |")
        lines.append("")
        if entry["clob_history"]:
            lines.append("**CLOB Price History (token held)**")
            lines.append("")
            lines.extend(["| Time (HKT) | Price |", "|---|---|"])
            for h in entry["clob_history"]:
                lines.append(f"| {h['time_hkt']} | {h['price']:.4f} |")
            lines.append("")
        else:
            lines.append("*No CLOB tick data available — audit based on CSV fields only.*")
            lines.append("")


def _render_failed_exit_audit(lines: list, rows: list) -> None:
    """WIN/LOSS rows resolved by hold-to-resolution after a failed early-exit
    attempt — without this, they're indistinguishable from a clean hold in the
    trade history table. See trader/doc/audit_trades_2026-07-03.md."""
    lines.extend([
        "| Time | Asset | Strategy | Side | Outcome | Attempts | Last error |",
        "|---|---|---|---|---|---|---|",
    ])
    for r in rows:
        time_str = datetime.fromtimestamp(_safe_float(r["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S")
        lines.append(
            f"| {time_str} | {r.get('asset', '')} | {r.get('strategy', '')} | {r.get('side', '')} | "
            f"{r.get('outcome', '')} | {int(r.get('exit_attempts') or 0)} | {r.get('exit_last_error', '')} |"
        )
    lines.append("")


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------

def write_markdown_summary(
    summary: dict, perf_stats: dict, window_start: datetime, window_end: datetime,
    out_dir: Path,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    ws_str = window_start.strftime("%Y-%m-%d")
    we_str = window_end.strftime("%Y-%m-%d")
    period = f"{ws_str}_to_{we_str}"
    title_period = f"{window_start:%Y-%m-%d %A} to {window_end:%Y-%m-%d %A}"
    filename = f"trade_recon_{period}.md"
    out_path = out_dir / filename

    now_hkt = datetime.now(tz=HKT).strftime("%Y-%m-%d %H:%M HKT")

    direction = summary.get("direction", {})
    resolved = direction.get("resolved", 0)
    correct = direction.get("correct", 0)
    wrong = direction.get("wrong", 0)
    accuracy = direction.get("accuracy", 0)
    pending = direction.get("pending", 0)
    stoploss = summary.get("stoploss", {})
    good_stops = stoploss.get("good", 0)
    costly_stops = stoploss.get("costly", 0)
    total_stops = good_stops + costly_stops
    total_rows = summary.get("total_rows", 0)

    lines = [
        f"# Trade Reconciliation — {title_period}",
        "",
        f"**Run:** {now_hkt}",
        f"**Source:** `trader/live_logs/live_trades_*.csv`",
        "",
    ]

    if perf_stats:
        ps = perf_stats
        p_pnl = ps.get("pnl_total", 0)
        p_outcomes = ps.get("outcomes", {})
        p_wins = p_outcomes.get("WIN", 0)
        p_losses = p_outcomes.get("LOSS", 0)
        p_sl = p_outcomes.get("STOPLOSS", 0)
        p_unwind = p_outcomes.get("UNWIND", 0)
        p_total = ps.get("total_rows", 0)
        p_wr = (p_wins + p_unwind) / p_total * 100 if p_total else 0
        p_assets = ", ".join(ps.get("assets", {}).keys())
        lines.append(
            f"> **Performance:** {p_total} trades, {p_assets} | {p_wins} wins, {p_losses} losses, "
            f"{p_sl} SL, {p_unwind} unwind | win rate {p_wr:.1f}% | PnL {p_pnl:+.4f} USDC"
        )
    api_narrative = f"{total_rows} rows, {resolved} resolved, {correct} correct, {accuracy:.1f}% accuracy"
    if total_stops:
        api_narrative += f", {good_stops} good stops, {costly_stops} costly"
    lines.append(f"> **Gamma cross-check:** {api_narrative}")
    lines.append("")

    if not perf_stats:
        lines.append("> **No trades in this window.** The bot was not active during this period.")
        lines.append("")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        console.print(f"[green]✓ Stub report → {out_path}[/green]")
        return out_path

    ps = perf_stats
    lines.append("## Performance")
    lines.append("")
    lines.extend([
        f"**Span:** {ps['span_first']} → {ps['span_last']}",
        "",
        "| | Value |",
        "|---|---|",
        f"| Total trades | {ps['total_rows']} |",
        f"| Strategies | {_fmt_counter(ps['strategies'])} |",
        f"| Side split | {_fmt_counter(ps['sides'])} |",
        f"| Outcomes | {_fmt_counter(ps['outcomes'])} |",
        f"| Assets | {_fmt_counter(ps['assets'])} |",
        f"| PnL total | {ps['pnl_total']:+.4f} USDC |",
        "",
    ])

    asset_stats = ps.get("asset_stats", {})
    if asset_stats:
        lines.extend([
            "### Per-Asset Win/Loss", "",
            "| Asset | Total | Wins | Losses | SL | Unwind | Win Rate | PnL |",
            "|---|---|---|---|---|---|---|---|",
        ])
        for asset, st in asset_stats.items():
            lines.append(
                f"| {asset} | {st['total']} | {st['wins']} | {st['losses']} | "
                f"{st['sl']} | {st['unwind']} | {st['win_rate']} | {st['pnl']:+.4f} |"
            )
        all_total = sum(st["total"] for st in asset_stats.values())
        all_wins = sum(st["wins"] for st in asset_stats.values())
        all_losses = sum(st["losses"] for st in asset_stats.values())
        all_sl = sum(st["sl"] for st in asset_stats.values())
        all_unwind = sum(st["unwind"] for st in asset_stats.values())
        all_wr = f"{(all_wins + all_unwind) / all_total * 100:.1f}%" if all_total else "—"
        all_pnl = sum(st["pnl"] for st in asset_stats.values())
        lines.append(
            f"| **ALL** | **{all_total}** | **{all_wins}** | **{all_losses}** | "
            f"**{all_sl}** | **{all_unwind}** | **{all_wr}** | **{all_pnl:+.4f}** |"
        )
        lines.append("")

    strat_bd = ps.get("strategy_breakdown", {})
    if strat_bd:
        lines.extend([
            "### By Asset + Strategy", "",
            "| Asset | Strategy | Total | Wins | Losses | SL | Unwind | Win Rate | PnL |",
            "|---|---|---|---|---|---|---|---|---|",
        ])
        for (asset, strategy), st in strat_bd.items():
            lines.append(
                f"| {asset} | {strategy} | {st['total']} | {st['wins']} | {st['losses']} | "
                f"{st['sl']} | {st['unwind']} | {st['win_rate']} | {st['pnl']:+.4f} |"
            )
        lines.append("")

    sl_detail = ps.get("sl_detail", [])
    if sl_detail:
        good = sum(1 for s in sl_detail if s["quality"] == "GOOD")
        costly = sum(1 for s in sl_detail if s["quality"] == "COSTLY")
        lines.extend([
            "### Stop Loss Detail", "",
            f"{len(sl_detail)} stop-loss exits: {good} good (avoided loss), {costly} costly (exited winner).",
            "",
            "| Time | Asset | Strategy | Side | PnL | Entry Price | Exit Price | Quality |",
            "|---|---|---|---|---|---|---|---|",
        ])
        for s in sl_detail:
            quality_fmt = "GOOD ✓" if s["quality"] == "GOOD" else "COSTLY ✗"
            lines.append(
                f"| {s['time']} | {s['asset']} | {s['strategy']} | {s['side']} | "
                f"{s['pnl']:+.4f} | {s['token_price']:.4f} | {s['exit_price']:.4f} | {quality_fmt} |"
            )
        lines.append("")

    trade_history = ps.get("trade_history", [])
    if trade_history:
        lines.extend([
            "### Trade History", "",
            "| Time | Asset | Strategy | Side | Outcome | Entry Price | Exit Price | PnL |",
            "|---|---|---|---|---|---|---|---|",
        ])
        for t in trade_history:
            lines.append(
                f"| {t['time']} | {t['asset']} | {t['strategy']} | {t['side']} | {t['outcome']} | "
                f"{t['token_price']:.4f} | {t['exit_price']:.4f} | {t['pnl']:+.4f} |"
            )
        total_pnl = sum(t["pnl"] for t in trade_history)
        lines.append(f"| **Total** | | | | | | | **{total_pnl:+.4f}** |")
        lines.append("")

    sl_unwind_rows = ps.get("sl_unwind_rows", [])
    if sl_unwind_rows:
        lines.append("## Stoploss & Unwind Audit")
        lines.append("")
        try:
            audit = _build_sl_unwind_audit(sl_unwind_rows)
            _render_sl_unwind_audit(lines, audit)
        except Exception as e:
            lines.append(f"*Audit failed: {e}*")
            lines.append("")

    failed_exit_rows = ps.get("failed_exit_rows", [])
    if failed_exit_rows:
        lines.append("## Failed Exit Attempts (held to resolution)")
        lines.append("")
        lines.append(
            "WIN/LOSS trades where an early exit (unwind take-profit or "
            "stop-loss) was attempted and failed before the position was "
            "held to market resolution — not a clean hold."
        )
        lines.append("")
        _render_failed_exit_audit(lines, failed_exit_rows)

    lines.append("## Gamma Cross-Check")
    lines.append("")
    lines.extend([
        "### Summary", "",
        "| Metric | Value |", "|---|---|",
        f"| Total rows | {total_rows} |",
        f"| Resolved (WIN/LOSS) | {resolved} |",
        f"| Matched worker.rs outcome | {correct} |",
        f"| Mismatched | {wrong} |",
        f"| Match rate | {accuracy:.1f}% |",
        f"| Pending (market unresolved) | {pending} |",
        f"| STOPLOSS: good (avoided loss) | {good_stops} |",
        f"| STOPLOSS: costly (exited winner) | {costly_stops} |",
    ])
    lines.append("")
    lines.append("### Mismatches")
    lines.append("")
    mismatches = summary.get("mismatch_details", [])
    if mismatches:
        lines.extend(["| Time | Slug | Side | Logged | Gamma Actual |", "|---|---|---|---|---|"])
        for m in mismatches:
            lines.append(f"| {m['time']} | {m['slug']} | {m['side']} | {m['algo']} | {m['actual']} |")
        lines.append("")
        lines.append(
            "**Any row here means worker.rs's own ApiResult-correction path "
            "(`Action::LogTradeCorrection`) let a wrong WIN/LOSS through — treat as a bug, "
            "not noise.**"
        )
    else:
        if pending == total_rows:
            lines.append("All trades still pending resolution.")
        elif wrong == 0:
            lines.append("None — worker.rs's outcome matched Gamma on every resolved trade. \U0001f3af")
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    console.print(f"[green]✓ Markdown summary → {out_path}[/green]")
    return out_path


def git_commit_push(file_paths: list, message: str) -> None:
    try:
        rel_paths = [str(Path(p).resolve().relative_to(REPO_ROOT.parent)) for p in file_paths]
        subprocess.run(["git", "-C", str(REPO_ROOT.parent), "add", *rel_paths], check=True)
        subprocess.run(["git", "-C", str(REPO_ROOT.parent), "commit", "-m", message], check=True)
        subprocess.run(["git", "-C", str(REPO_ROOT.parent), "push"], check=True)
        console.print(f"[green]✓ Committed & pushed → {message}[/green]")
    except subprocess.CalledProcessError as e:
        console.print(f"[yellow]⚠ Git failed: {e}[/yellow]")


# ---------------------------------------------------------------------------
# Window resolution
# ---------------------------------------------------------------------------

def _resolve_window(dt_arg: Optional[str]) -> tuple:
    now = datetime.now(tz=HKT)
    if dt_arg:
        try:
            dt_target = datetime.strptime(dt_arg, "%Y%m%d").replace(tzinfo=HKT)
        except ValueError:
            raise SystemExit(f"Invalid --dt format: {dt_arg!r}. Use YYYYMMDD, e.g. 20260703.")
        window_start = dt_target.replace(hour=20, minute=0, second=0, microsecond=0)
    else:
        if now.hour >= 20:
            window_start = now.replace(hour=20, minute=0, second=0, microsecond=0)
        else:
            window_start = (now - timedelta(days=1)).replace(hour=20, minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(days=1)
    return window_start, window_end


def main() -> None:
    parser = argparse.ArgumentParser(description="poly_rust trade reconciliation")
    parser.add_argument("--wallet", type=str, help="Override FUND_ADDRESS from .env (informational only)")
    parser.add_argument("--no-push", action="store_true", help="Skip git commit+push of the markdown report")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--today", action="store_true", help="Reconcile current 24h window anchored at 8pm HKT (default)")
    mode.add_argument("--dt", type=str, help="Reconcile 24h window from 8pm on YYYYMMDD")
    args = parser.parse_args()

    window_start, window_end = _resolve_window(args.dt)
    console.print(f"[dim]Window: {window_start:%Y-%m-%d %H:%M} → {window_end:%Y-%m-%d %H:%M} HKT[/dim]")

    log_dir = REPO_ROOT / "live_logs"
    out_dir = REPO_ROOT / "results" / "daily_recon"

    files = find_trade_logs(log_dir)
    if not files:
        console.print(f"[yellow]No trade logs found in {log_dir}/[/yellow]")
        raise SystemExit(0)

    rows = load_and_filter(files, window_start.timestamp(), window_end.timestamp())
    if not rows:
        console.print(f"[yellow]No trades in window {window_start:%Y-%m-%d %H:%M} → {window_end:%Y-%m-%d %H:%M} HKT.[/yellow]")
        md_path = write_markdown_summary({"direction": {}, "stoploss": {}, "total_rows": 0}, {}, window_start, window_end, out_dir)
        if not args.no_push:
            git_commit_push([md_path], f"recon: {window_start:%Y-%m-%d} — 0 trades")
        raise SystemExit(0)

    console.print(f"[dim]Loaded {len(rows)} trades after filtering & dedup.[/dim]")
    console.print("[dim]Querying Gamma API for market outcomes...[/dim]")
    annotated, summary = annotate_rows(rows)
    perf_stats = compute_performance_stats(annotated)

    md_path = write_markdown_summary(summary, perf_stats, window_start, window_end, out_dir)

    if not args.no_push:
        direction = summary["direction"]
        git_commit_push(
            [md_path],
            f"recon: {window_start:%Y-%m-%d} — {direction['correct']}/{direction['resolved']} "
            f"matched ({direction['accuracy']:.0f}%)",
        )


if __name__ == "__main__":
    main()
