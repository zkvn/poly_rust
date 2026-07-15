"""
trade_reconcile.py — poly_rust daily trade reconciliation (Rust trader)

Adapted from btc_5mins/scripts/trade_reconcile.py for this project's simpler
TradeRecord schema (trader/src/types.rs; header is self-healed on read by
`live.rs::append_csv_header_if_new` if an older CSV predates exit_attempts/
exit_last_error — see trader/doc/incident_doge_2026-07-03.md):

    logged_at,slug,strategy,side,entry_ts,token_price,exit_price,outcome,pnl,exit_attempts,exit_last_error,
    entry_signal_latency_ms,entry_process_latency_ms,exit_signal_latency_ms,exit_process_latency_ms

Latency columns (added 2026-07-06, trader/doc/incident_sol_unwind_but_loss_2026-07-06.md):
signal latency is tick-timestamp -> driver-receipt, process latency is
driver-receipt -> order-confirmed; exit_* is 0 for a WIN/LOSS resolved by
natural market close (no exit order was ever placed).

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
import io
import json
import os
import re
import subprocess
import sys
import tomllib
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

# ---------------------------------------------------------------------------
# Backtest reconciliation — reuses trader/scripts/build_backtest_prices.py
# and the Rust `backtest` binary (trader/src/bin/backtest.rs --format csv).
# Read-only: builds scratch price data + shells out to a separate backtest
# binary, never touches the live trading process (`live` binary/worker.rs).
# See trader/doc/feature_bt_recon_2026-07-10.md for the design.
# ---------------------------------------------------------------------------
CONFIG_DIR = REPO_ROOT / "config"
BACKTEST_BINARY = REPO_ROOT / "target" / "release" / "backtest"
BACKTEST_PRICES_DIR = REPO_ROOT / "backtest_prices"
BUILD_PRICES_SCRIPT = REPO_ROOT / "scripts" / "build_backtest_prices.py"
# price_feed/raw/ is synced from Oracle by its own independent process — not
# guaranteed fresh at the moment this script runs (found 2026-07-10: a stale
# local raw/ made every asset "fail" with a missing-file error that looked
# like a same-day sealing gap but was actually just sync lag). Sync it
# ourselves before building backtest price data rather than assuming
# something else already did.
PRICE_FEED_SYNC_SCRIPT = REPO_ROOT.parent / "price_feed" / "scripts" / "sync_oracle.sh"
LIVE_LOG_PATH = REPO_ROOT / "live_logs" / "live.log"

# ---------------------------------------------------------------------------
# Data quality — independent tick-coverage observer
# (price_feed/doc/incident_collector_data_loss_2026-07-12.md). Runs every
# daily recon so a repeat of that incident (collector crash-looping, ~85% of
# ticks silently missing for 2+ days) shows up in the report automatically
# instead of requiring someone to notice by chance.
# ---------------------------------------------------------------------------
PRICE_FEED_RAW_DIR = REPO_ROOT.parent / "price_feed" / "raw"
sys.path.insert(0, str(REPO_ROOT.parent / "price_feed" / "scripts"))
try:
    from data_quality import safe_check_data_quality  # noqa: E402
except ImportError:
    def safe_check_data_quality(*_args, **_kwargs):  # type: ignore[misc]
        return {"hours_checked": 0, "flagged": [], "error": "data_quality module not importable"}

CSV_COLUMNS = [
    "logged_at", "slug", "strategy", "side", "entry_ts", "token_price",
    "exit_price", "outcome", "pnl", "exit_attempts", "exit_last_error",
    "entry_signal_latency_ms", "entry_process_latency_ms",
    "exit_signal_latency_ms", "exit_process_latency_ms",
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
    """fidelity=1 on the "1m" interval range now 400s ("minimum 'fidelity' for
    '1m' range is 10" — a Polymarket API change since this was first written,
    found 2026-07-10 while investigating why the Stoploss/Unwind Audit kept
    coming back empty). fidelity=10 gives ~10-min-spaced bars, which is coarse
    but still usually lands inside the +/-15min audit window
    (_build_sl_unwind_audit) for a 5-min-cycle market; fidelity=60 (~1hr
    spacing) is kept as a last-resort fallback but rarely has a bar close
    enough to the trade to be useful — see that function's window filter."""
    import requests
    for fidelity in (10, 60):
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


GAMMA_TIMEOUT_CONTINUED_RE = re.compile(
    r"gave up waiting for Gamma resolution of (\S+) — balance up since last cycle's checkpoint, continuing"
)
GAMMA_TIMEOUT_HALTED_RE = re.compile(
    r"gave up waiting for Gamma resolution of (\S+) — halting new entries"
)


def parse_gamma_timeout_events(live_log_path: Path) -> dict:
    """Scan live.log for the balance-increase Gamma-timeout override
    (trader/src/balance.rs::GammaBalanceTracker, added 2026-07-09 —
    Action::GammaUnresolvedContinued / Action::GammaHaltEngaged in worker.rs)
    so annotate_rows can tell "worker.rs's ApiResult-correction path let a
    wrong WIN/LOSS through" apart from "Gamma never resolved in time and the
    worker did exactly what it's designed to do": keep going if the account
    balance was up since the last cycle's checkpoint (checked ~2min into
    every cycle, same sample BalanceGuard already fetches — no extra API
    calls), otherwise halt new entries and flag for manual review.

    Returns {slug: "CONTINUED" | "HALTED"}. A missing/unreadable log just
    means this context is unavailable for this run — not fatal, the affected
    rows fall back to being treated as ordinary mismatches like before this
    existed.
    """
    events = {}
    try:
        text = live_log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return events
    for m in GAMMA_TIMEOUT_CONTINUED_RE.finditer(text):
        events[m.group(1)] = "CONTINUED"
    for m in GAMMA_TIMEOUT_HALTED_RE.finditer(text):
        events.setdefault(m.group(1), "HALTED")
    return events


def annotate_rows(rows: list, gamma_timeout_events: Optional[dict] = None) -> tuple:
    """Cross-check WIN/LOSS rows against Gamma; classify STOPLOSS/UNWIND/TIMEOUT
    quality; carve out the Gamma-timeout balance-override cases (see
    parse_gamma_timeout_events) from the "wrong WIN/LOSS" bug bucket.

    Returns (annotated_rows, summary) where summary has direction (resolved/
    correct/wrong/accuracy/pending), stoploss (good/costly) counts, and
    gamma_timeout (continued/halted/details) — mirroring btc_5mins's shape
    for the first two so downstream markdown rendering matches.
    """
    gamma_timeout_events = gamma_timeout_events or {}
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
    gamma_timeout_continued = gamma_timeout_halted = 0
    mismatch_details = []
    gamma_timeout_details = []
    annotated = []

    for row in rows:
        slug = row.get("slug", "")
        side = row.get("side", "").strip().upper()
        outcome = row.get("outcome", "").strip().upper()
        actual = outcome_map.get(slug, "PENDING")
        timeout_event = gamma_timeout_events.get(slug)

        if actual == "PENDING":
            actual_result = "PENDING"
            pending += 1
        elif outcome == "STOPLOSS":
            actual_result = "WIN" if side == actual else "LOSS"
            if actual_result == "WIN":
                costly_stops += 1
            else:
                good_stops += 1
        elif outcome in ("UNWIND", "TIMEOUT"):
            # Both are exits whose pnl/outcome was already final at the moment
            # it was logged — Gamma is only useful here for an advisory "would
            # the side have won anyway" counterfactual, never a right/wrong
            # verdict on the trade itself. TIMEOUT (added 2026-07-08, see
            # plan_unwind_time_2026-07-08.md) used to fall through to the
            # WIN/LOSS branch below by omission, where it could never equal
            # "WIN"/"LOSS" and was therefore *always* flagged as a Gamma
            # mismatch regardless of whether the timed-out exit was correct —
            # found 2026-07-10 while investigating a TIMEOUT row that showed
            # up as a "bug" in every report since the outcome existed.
            actual_result = "WIN" if side == actual else "LOSS"
        elif timeout_event is not None and outcome in ("WIN", "LOSS"):
            # Gamma never resolved within the retry deadline for this
            # Confirming trade, so worker.rs used the balance-increase
            # override instead of guessing — this is the worker doing exactly
            # what it's designed to do when Gamma is slow/unavailable, not
            # the LogTradeCorrection path failing, so it's tracked separately
            # rather than counted as a "wrong WIN/LOSS through" bug.
            actual_result = "WIN" if side == actual else "LOSS"
            if timeout_event == "CONTINUED":
                gamma_timeout_continued += 1
            else:
                gamma_timeout_halted += 1
            gamma_timeout_details.append({
                "time": datetime.fromtimestamp(float(row["logged_at"]), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
                "slug": slug, "side": side, "logged": outcome, "event": timeout_event,
                "hindsight": actual_result, "hindsight_match": actual_result == outcome,
            })
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
        "gamma_timeout": {
            "continued": gamma_timeout_continued, "halted": gamma_timeout_halted,
            "details": gamma_timeout_details,
        },
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
            # Signal (tick-timestamp -> driver-receipt) and process (driver-receipt
            # -> order-confirmed) latency, entry and exit legs shown separately —
            # previously summed into one combined figure per leg, which hid which
            # half (network/tick delay vs. our own order round-trip) dominated.
            "entry_signal_latency_ms": _safe_float(r.get("entry_signal_latency_ms")),
            "entry_process_latency_ms": _safe_float(r.get("entry_process_latency_ms")),
            "exit_signal_latency_ms": _safe_float(r.get("exit_signal_latency_ms")),
            "exit_process_latency_ms": _safe_float(r.get("exit_process_latency_ms")),
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
# Backtest reconciliation: Live vs BT / BT vs Live
# ---------------------------------------------------------------------------

def resolve_trade_assets(config_dir: Path) -> list:
    """Read `trade_assets` from the latest strategy_*.toml — same file
    selection as Rust's config::load_latest (lexicographic sort, take last),
    so this always backtests whatever assets are actually configured live,
    not just whatever live happened to trade in the window."""
    files = sorted(config_dir.glob("strategy_*.toml"))
    if not files:
        raise FileNotFoundError(f"no strategy_*.toml found in {config_dir}")
    with open(files[-1], "rb") as f:
        cfg = tomllib.load(f)
    return list(cfg["trade_assets"])


def slug_cycle_ts(slug: str) -> float:
    """Cycle-start unix ts embedded as the slug's trailing number
    (e.g. 'eth-updown-5m-1783046100' -> 1783046100.0) — the same value used
    elsewhere in this project as the source of truth for cycle time."""
    try:
        return float(slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return 0.0


# This project only ever trades 5-min updown cycles (see every slug's
# "-5m-" segment) — no per-market cycle length to look up.
CYCLE_LEN_SECS = 300.0


def t_minus_str(slug: str, entry_ts: Optional[float]) -> str:
    """T-Ns before cycle close at the moment of entry, matching the same
    T-Ns convention worker.rs already logs live (e.g. "T-9s" order-placed
    heartbeat lines) — so this column reads the same way as those logs."""
    if not entry_ts:
        return "—"
    secs_left = slug_cycle_ts(slug) + CYCLE_LEN_SECS - entry_ts
    return f"T-{secs_left:.0f}s" if secs_left >= 0 else f"T+{-secs_left:.0f}s"


def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """(b - a) / a * 100, or None if either side is missing or a is zero
    (nothing to divide by)."""
    if not a or b is None:
        return None
    return (b - a) / a * 100


def _fmt_pct(v: Optional[float]) -> str:
    return f"{v:+.1f}%" if v is not None else "—"


def _fmt_price(v: Optional[float]) -> str:
    return f"{v:.4f}" if v is not None else "—"


def parse_backtest_csv(csv_text: str, asset: str) -> list:
    """Parse `backtest --format csv` stdout into normalized row dicts."""
    rows = []
    for row in csv.DictReader(io.StringIO(csv_text)):
        slug = row.get("slug", "")
        rows.append({
            "asset": asset,
            "slug": slug,
            "strategy": row.get("strategy", ""),
            "side": row.get("side", "").strip().upper(),
            "outcome": row.get("outcome", "").strip().upper(),
            "pnl": _safe_float(row.get("pnl")),
            "cycle_ts": slug_cycle_ts(slug),
            "entry_ts": _safe_float(row.get("entry_ts")),
            "token_price": _safe_float(row.get("token_price")),
            "exit_price": _safe_float(row.get("exit_price")),
        })
    return rows


def load_underlying_price_series(assets: list, dates: list, prices_dir: Path) -> dict:
    """slug -> time-sorted [(ts, binance_price), ...] from the same local
    {asset}_binance_{date}.parquet files run_backtest_reconciliation already
    builds/syncs for the BT replay — reuses that data instead of making any
    extra network calls just for the Δ% columns.

    Entry Δ%/Cycle Δ% originally used the CLOB (Polymarket order-book)
    price — i.e. an implied probability in [0, 1] — as both numerator and
    denominator. That's the wrong price entirely: see
    trader/doc/incident_delta_pct_2026-07-12.md. A probability swinging from
    0.44 to 0.95 is a real, large *probability* move but says nothing about
    how far the actual asset moved, and trends toward the [0, 1] boundary
    near cycle close regardless of the underlying's magnitude — that's what
    produced deltas like +113% or +15000% for assets that only move
    fractions of a percent in 5 minutes. The underlying (Binance) price is
    the quantity these two columns should have been measuring all along:
    it's also literally what the Polymarket "updown" market resolves
    against (price at cycle close vs. price at cycle open)."""
    import pandas as pd
    out: dict = {}
    for asset in assets:
        for date in dates:
            path = prices_dir / f"{asset}_binance_{date}.parquet"
            if not path.exists():
                continue
            try:
                df = pd.read_parquet(path)
            except Exception:
                continue
            if df.empty:
                continue
            for slug, group in df.sort_values("ts").groupby("slug"):
                out.setdefault(slug, []).extend(
                    zip(group["ts"].tolist(), group["binance"].tolist())
                )
    for slug, ticks in out.items():
        ticks.sort(key=lambda p: p[0])
    return out


def _safe_load_underlying_price_series(assets: list, dates: list, prices_dir: Path) -> dict:
    """Same defensive-by-design rule as the rest of the BT reconciliation
    pipeline: a missing/corrupt price file must degrade the Δ% columns to
    "—", never take down the report."""
    try:
        return load_underlying_price_series(assets, dates, prices_dir)
    except Exception as e:
        console.print(f"[yellow]⚠ Could not load underlying price series for Δ% columns: {e}[/yellow]")
        return {}


def _underlying_price_at(ticks: list, ts: Optional[float]) -> Optional[float]:
    """Price of the tick nearest `ts` (by |Δt|) in a time-sorted [(ts, price), ...]
    series, or None if there are no ticks (missing local price data — e.g. HYPE
    has no Binance market) or no target timestamp."""
    if not ticks or not ts:
        return None
    return min(ticks, key=lambda p: abs(p[0] - ts))[1]


def _cycle_open_close(ticks: list) -> tuple:
    """(open_price, close_price) — first/last tick in a time-sorted
    [(ts, price), ...] series, or (None, None) if empty."""
    if not ticks:
        return None, None
    return ticks[0][1], ticks[-1][1]


# ---------------------------------------------------------------------------
# BT vs Live mismatch-reason classification — trader/doc/incident_bt_vs_live_
# discrepancy_2026-07-12.md + trader/doc/plan_timeout_backtest_and_mismatch_
# reason_2026-07-12.md. Investigation-grade labels, not hard verdicts: a
# missed cycle inside a reconstructed halt window is a confident match, but
# "config changed"/"sparse tick data" are pointers for a human to verify, not
# proof of causation (see plan doc's "Non-goals" — most config edits don't
# touch entry-decision params, so that label alone doesn't mean the change
# caused the mismatch).
# ---------------------------------------------------------------------------

# Real time recovered from a heartbeat line's own slug+T-Ns offset, same
# technique as trader/doc/audit_retry_doge_2026-07-03.md.
_HEARTBEAT_RE = re.compile(r"heartbeat \S+ \(\S+\) slug=(\S+) T-(-?\d+)s")

# Ordered (pattern, reason) — first match wins if several could apply to the
# same line (they don't overlap in practice, but order is still meaningful:
# a more specific label should win over a generic one). NOTE: this treats
# every halt as a single *global* on/off window, not per-asset/per-strategy
# scoped, even though some sources (e.g. Gamma-unresolved, a single-asset
# `/halt <ASSET>`) are narrower in reality — a deliberate simplification
# (see plan doc): the dominant, highest-impact source observed in practice
# (balance-drawdown) already is global, and getting per-asset scoping exactly
# right would require modeling far more of worker.rs's control-event state
# than this investigation-grade classifier needs to be useful.
_HALT_OPEN_PATTERNS = [
    (re.compile(r"BALANCE DRAWDOWN"), "balance drawdown >25% (session)"),
    (re.compile(r"GAMMA UNRESOLVED — HALTED"), "Gamma-unresolved halt"),
    # Must require "halt" itself, not just the 🛑 emoji — that emoji is also
    # used on unrelated "STOP LOSS triggered" trade alerts (found 2026-07-12:
    # a false-positive halt-open at 14:49:52 from an ETH stop-loss line,
    # ~2min before the real balance-drawdown halt at 14:51:57, which threw
    # off both the reconstructed start time and the reason label).
    (re.compile(r"telegram\] sent: 🛑.*[Hh]alt"), "manual /halt"),
]
_HALT_CLOSE_RE = re.compile(r"telegram\] sent: ▶️ Resumed|HALT RESET")


def build_halt_windows(live_log_path: Path, window_end: datetime) -> list:
    """[(start_ts, end_ts, reason), ...] reconstructed from live.log: track
    the most recently-seen heartbeat's real timestamp, attach it to the next
    halt-open/halt-close line encountered. An open halt with no matching
    close before `window_end` (e.g. still active at report-generation time)
    stays open through `window_end`. A missing/unreadable log just means
    this context is unavailable — not fatal, same fallback pattern as
    `parse_gamma_timeout_events`."""
    windows = []
    try:
        lines = live_log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return windows

    last_real_ts = None
    open_start = None
    open_reason = None
    for line in lines:
        hb = _HEARTBEAT_RE.search(line)
        if hb:
            slug, t_minus = hb.group(1), int(hb.group(2))
            last_real_ts = slug_cycle_ts(slug) + CYCLE_LEN_SECS - t_minus
            continue
        if last_real_ts is None:
            continue

        if open_start is None:
            for pattern, reason in _HALT_OPEN_PATTERNS:
                if pattern.search(line):
                    open_start, open_reason = last_real_ts, reason
                    break
        elif _HALT_CLOSE_RE.search(line):
            windows.append((open_start, last_real_ts, open_reason))
            open_start = open_reason = None

    if open_start is not None:
        windows.append((open_start, window_end.timestamp(), open_reason))
    return windows


def _safe_build_halt_windows(live_log_path: Path, window_end: datetime) -> list:
    try:
        return build_halt_windows(live_log_path, window_end)
    except Exception as e:
        console.print(f"[yellow]⚠ Could not build halt windows for mismatch reasons: {e}[/yellow]")
        return []


def get_config_last_change_ts(config_dir: Path) -> Optional[float]:
    """Unix ts of the most recent git commit touching the resolved (latest)
    strategy_*.toml — best-effort via `git log`; None if git/the repo isn't
    available or the file has no history, same optional-enrichment fallback
    pattern as everything else in this module."""
    try:
        files = sorted(config_dir.glob("strategy_*.toml"))
        if not files:
            return None
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--", files[-1].name],
            cwd=config_dir, capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        return float(result.stdout.strip())
    except Exception:
        return None


def _safe_get_config_last_change_ts(config_dir: Path) -> Optional[float]:
    try:
        return get_config_last_change_ts(config_dir)
    except Exception as e:
        console.print(f"[yellow]⚠ Could not resolve config change time for mismatch reasons: {e}[/yellow]")
        return None


SPARSE_TICK_THRESHOLD = 60  # a full 5-min cycle at this project's usual ~4Hz
# binance sample rate is ~1200 ticks; 60 is a conservative floor well below
# any normal cycle, chosen to only flag genuinely gappy cycles, not quiet ones.


def classify_mismatch_reason(
    cycle_ts: float, halt_windows: list, config_change_ts: Optional[float],
    window_end_ts: float, tick_count: Optional[int],
) -> str:
    """Priority order, first match wins — see module doc comment above."""
    for start, end, reason in halt_windows:
        if start <= cycle_ts <= end:
            start_str = datetime.fromtimestamp(start, tz=HKT).strftime("%H:%M")
            end_str = datetime.fromtimestamp(end, tz=HKT).strftime("%H:%M")
            return f"live halted: {reason} {start_str}–{end_str}"
    if config_change_ts is not None and cycle_ts <= config_change_ts <= window_end_ts:
        change_str = datetime.fromtimestamp(config_change_ts, tz=HKT).strftime("%Y-%m-%d %H:%M")
        return f"config changed {change_str} same-window (verify params)"
    if tick_count is not None and tick_count < SPARSE_TICK_THRESHOLD:
        return f"sparse tick data ({tick_count} ticks this cycle)"
    return "unexplained"


def filter_bt_rows_to_window(rows: list, from_ts: float, to_ts: float) -> list:
    return [r for r in rows if from_ts <= r["cycle_ts"] < to_ts]


def _normalize_live_rows(rows: list) -> list:
    """Reduce the already-window-filtered `annotated` live rows down to the
    fields the BT comparison needs, with pnl coerced to float."""
    out = []
    for r in rows:
        out.append({
            "time": datetime.fromtimestamp(_safe_float(r.get("logged_at")), tz=HKT).strftime("%Y-%m-%d %H:%M:%S"),
            "asset": r.get("asset", ""),
            "slug": r.get("slug", ""),
            "strategy": r.get("strategy", ""),
            "side": r.get("side", "").strip().upper(),
            "outcome": r.get("outcome", "").strip().upper(),
            "pnl": _safe_float(r.get("pnl")),
            "entry_ts": _safe_float(r.get("entry_ts")),
            "token_price": _safe_float(r.get("token_price")),
            "exit_price": _safe_float(r.get("exit_price")),
        })
    return out


def build_live_vs_bt(live_rows: list, bt_rows: list, assets_with_data: set,
                      underlying_prices: Optional[dict] = None,
                      mismatch_ctx: Optional[dict] = None) -> tuple:
    """One row per live trade: does the backtest agree?

    Status classification:
      MATCH            - bt fired the same (slug, side), same outcome
      OUTCOME DIFF     - bt fired the same (slug, side), different outcome
      SIDE DIFF        - bt fired the opposite side for that slug
      BT DID NOT FIRE  - bt had price data for the asset but skipped the cycle
      NO PRICE DATA    - bt couldn't run at all for this asset/date

    `reason` (see classify_mismatch_reason) is only computed for genuine
    mismatches (OUTCOME DIFF/SIDE DIFF/BT DID NOT FIRE) — MATCH needs no
    explanation, and NO PRICE DATA already is one.
    """
    underlying_prices = underlying_prices or {}
    mismatch_ctx = mismatch_ctx or {}
    halt_windows = mismatch_ctx.get("halt_windows", [])
    config_change_ts = mismatch_ctx.get("config_change_ts")
    window_end_ts = mismatch_ctx.get("window_end_ts", 0.0)

    bt_lookup: dict = {}
    for r in bt_rows:
        bt_lookup.setdefault((r["slug"], r["side"]), r)

    n_match = n_outcome_diff = n_side_diff = n_not_fired = n_no_data = 0
    total_live_pnl = total_bt_pnl = 0.0
    table = []

    for lt in live_rows:
        total_live_pnl += lt["pnl"]
        opp_side = "DOWN" if lt["side"] == "UP" else "UP"
        bt_same = bt_lookup.get((lt["slug"], lt["side"]))
        bt_opp = bt_lookup.get((lt["slug"], opp_side))

        ticks = underlying_prices.get(lt["slug"], [])
        open_p, close_p = _cycle_open_close(ticks)
        entry_p = _underlying_price_at(ticks, lt.get("entry_ts"))
        cycle_ts = slug_cycle_ts(lt["slug"])
        mismatch_reason = classify_mismatch_reason(
            cycle_ts, halt_windows, config_change_ts, window_end_ts,
            len(ticks) if ticks else None,
        )
        extra = {
            "entry_time": t_minus_str(lt["slug"], lt.get("entry_ts")),
            "entry_price": lt.get("token_price"),
            "exit_price": lt.get("exit_price"),
            "cycle_delta_pct": _pct_change(open_p, close_p),
            "entry_delta_pct": _pct_change(open_p, entry_p),
        }

        if bt_same:
            total_bt_pnl += bt_same["pnl"]
            diff_pnl = lt["pnl"] - bt_same["pnl"]
            if bt_same["outcome"] == lt["outcome"]:
                n_match += 1
                status, reason = "MATCH", "—"
            else:
                n_outcome_diff += 1
                status = f"OUTCOME DIFF (live={lt['outcome']} bt={bt_same['outcome']})"
                reason = mismatch_reason
            table.append({**lt, **extra, "bt_outcome": bt_same["outcome"], "bt_pnl": bt_same["pnl"],
                          "diff_pnl": diff_pnl, "status": status, "reason": reason})
        elif bt_opp:
            n_side_diff += 1
            total_bt_pnl += bt_opp["pnl"]
            table.append({**lt, **extra, "bt_outcome": bt_opp["outcome"], "bt_pnl": bt_opp["pnl"],
                          "diff_pnl": lt["pnl"] - bt_opp["pnl"],
                          "status": f"SIDE DIFF (bt side={opp_side})", "reason": mismatch_reason})
        elif lt["asset"] not in assets_with_data:
            n_no_data += 1
            table.append({**lt, **extra, "bt_outcome": None, "bt_pnl": None, "diff_pnl": None,
                          "status": "NO PRICE DATA", "reason": "—"})
        else:
            n_not_fired += 1
            table.append({**lt, **extra, "bt_outcome": None, "bt_pnl": None, "diff_pnl": None,
                          "status": "BT DID NOT FIRE", "reason": mismatch_reason})

    summary = {
        "n_live": len(live_rows), "n_match": n_match, "n_outcome_diff": n_outcome_diff,
        "n_side_diff": n_side_diff, "n_not_fired": n_not_fired, "n_no_data": n_no_data,
        "total_live_pnl": total_live_pnl, "total_bt_pnl": total_bt_pnl,
    }
    return table, summary


def build_bt_vs_live(bt_rows: list, live_rows: list, underlying_prices: Optional[dict] = None,
                      mismatch_ctx: Optional[dict] = None) -> list:
    """Cycles the backtest fired but live did not trade at all (either side).

    A live trade on the *opposite* side of a bt-fired cycle already shows up
    as SIDE DIFF in the Live vs BT table, so this only needs to exclude
    slugs live touched on any side — not check side equality itself. Every
    row here is by definition a mismatch, so `reason` is always computed.
    """
    underlying_prices = underlying_prices or {}
    mismatch_ctx = mismatch_ctx or {}
    halt_windows = mismatch_ctx.get("halt_windows", [])
    config_change_ts = mismatch_ctx.get("config_change_ts")
    window_end_ts = mismatch_ctx.get("window_end_ts", 0.0)

    live_slugs = {lt["slug"] for lt in live_rows}
    missed = []
    for r in bt_rows:
        if r["slug"] in live_slugs:
            continue
        ticks = underlying_prices.get(r["slug"], [])
        open_p, close_p = _cycle_open_close(ticks)
        entry_p = _underlying_price_at(ticks, r.get("entry_ts"))
        reason = classify_mismatch_reason(
            r["cycle_ts"], halt_windows, config_change_ts, window_end_ts,
            len(ticks) if ticks else None,
        )
        missed.append({
            "time": datetime.fromtimestamp(r["cycle_ts"], tz=HKT).strftime("%Y-%m-%d %H:%M:%S") if r["cycle_ts"] else "",
            "asset": r["asset"], "strategy": r["strategy"], "side": r["side"],
            "outcome": r["outcome"], "pnl": r["pnl"],
            "entry_time": t_minus_str(r["slug"], r.get("entry_ts")),
            "entry_price": r.get("token_price"),
            "exit_price": r.get("exit_price"),
            "cycle_delta_pct": _pct_change(open_p, close_p),
            "entry_delta_pct": _pct_change(open_p, entry_p),
            "reason": reason,
        })
    missed.sort(key=lambda x: x["time"])
    return missed


def sync_price_feed_from_oracle(sync_script: Path) -> bool:
    """Pull fresh sealed hourly shards from Oracle before building backtest
    price data — price_feed/raw/'s local copy is synced by its own
    independent process (or a stale manual run) and isn't guaranteed to be
    current when this script runs. Additive-only rsync (no --delete,
    excludes *.tmp), read-only against Oracle, never touches the live
    trading process. A failed sync (Oracle unreachable, VPN down, etc.) is
    not fatal — falls back to whatever local data already exists, same as
    a missing file for any other reason."""
    if not sync_script.exists():
        console.print(f"[yellow]⚠ price_feed sync skipped: {sync_script} not found[/yellow]")
        return False
    result = subprocess.run([str(sync_script)], capture_output=True, text=True)
    if result.returncode != 0:
        console.print(
            f"[yellow]⚠ price_feed sync from Oracle failed: "
            f"{result.stderr.strip()[-500:]}[/yellow]"
        )
        return False
    return True


def build_price_data(assets: list, date: str, out_dir: Path, build_script: Path) -> bool:
    result = subprocess.run(
        [sys.executable, str(build_script), "--asset", ",".join(assets),
         "--date", date, "--out-dir", str(out_dir)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        console.print(
            f"[yellow]⚠ build_backtest_prices.py failed for {date}: "
            f"{result.stderr.strip()[-500:]}[/yellow]"
        )
        return False
    return True


def run_rust_backtest(asset: str, date: str, prices_dir: Path, config_dir: Path, binary: Path) -> str:
    result = subprocess.run(
        [str(binary), "--asset", asset, "--date", date,
         "--prices-dir", str(prices_dir), "--config-dir", str(config_dir),
         "--format", "csv"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def run_backtest_reconciliation(window_start: datetime, window_end: datetime, annotated_live_rows: list) -> Optional[tuple]:
    """Orchestrates the whole BT reconciliation pipeline for one window.

    Defensive by design: any failure here (Oracle unreachable, binary
    missing, price data missing, a single asset's backtest crashing)
    degrades to a skipped section or a partial one — it must never raise
    out of here and take down the rest of the (already-working) recon
    report. Never touches the live trading process; syncs price_feed/raw
    from Oracle (read-only, additive), then reads it plus trader/config and
    shells out to the separate `backtest` binary.
    """
    if not BACKTEST_BINARY.exists():
        console.print(
            f"[yellow]⚠ Backtest reconciliation skipped: binary not found at "
            f"{BACKTEST_BINARY} (run `cargo build --release --bin backtest` in trader/)[/yellow]"
        )
        return None

    try:
        assets = resolve_trade_assets(CONFIG_DIR)
    except Exception as e:
        console.print(f"[yellow]⚠ Backtest reconciliation skipped: could not resolve trade_assets ({e})[/yellow]")
        return None

    dates = sorted({window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")})

    console.print("[dim]Syncing price_feed/raw from Oracle...[/dim]")
    sync_price_feed_from_oracle(PRICE_FEED_SYNC_SCRIPT)

    BACKTEST_PRICES_DIR.mkdir(parents=True, exist_ok=True)
    all_bt_rows: list = []
    assets_with_data: set = set()

    for date in dates:
        if not build_price_data(assets, date, BACKTEST_PRICES_DIR, BUILD_PRICES_SCRIPT):
            continue
        for asset in assets:
            try:
                csv_text = run_rust_backtest(asset, date, BACKTEST_PRICES_DIR, CONFIG_DIR, BACKTEST_BINARY)
            except subprocess.CalledProcessError as e:
                err = (e.stderr or str(e)).strip()[-300:]
                console.print(f"[yellow]  {asset}/{date}: backtest failed ({err})[/yellow]")
                continue
            assets_with_data.add(asset)
            all_bt_rows.extend(parse_backtest_csv(csv_text, asset))

    bt_in_window = filter_bt_rows_to_window(all_bt_rows, window_start.timestamp(), window_end.timestamp())
    live_norm = _normalize_live_rows(annotated_live_rows)
    underlying_prices = _safe_load_underlying_price_series(assets, dates, BACKTEST_PRICES_DIR)
    mismatch_ctx = {
        "halt_windows": _safe_build_halt_windows(LIVE_LOG_PATH, window_end),
        "config_change_ts": _safe_get_config_last_change_ts(CONFIG_DIR),
        "window_end_ts": window_end.timestamp(),
    }

    live_vs_bt_rows, bt_summary = build_live_vs_bt(
        live_norm, bt_in_window, assets_with_data, underlying_prices, mismatch_ctx)
    bt_vs_live_rows = build_bt_vs_live(bt_in_window, live_norm, underlying_prices, mismatch_ctx)

    return live_vs_bt_rows, bt_summary, bt_vs_live_rows


def _fmt_pnl(v) -> str:
    return f"{v:+.4f}" if v is not None else "—"


def _cfg_table(cfg: dict, key: str) -> dict:
    """A `[key]` table in strategy_*.toml is always {"default": x, ASSET: y, ...} —
    return it as-is, or wrap a bare scalar (top-level key, no per-asset table) in
    {"default": x} so callers have one shape to deal with."""
    val = cfg.get(key)
    if isinstance(val, dict):
        return val
    return {"default": val}


def _cfg_asset(cfg: dict, key: str, asset: str):
    tbl = _cfg_table(cfg, key)
    return tbl.get(asset, tbl.get("default"))


def _fmt_cfgval(v, decimals: int = 4) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{decimals}f}"
    return str(v)


def render_strategy_config(lines: list, config_dir: Path) -> None:
    """Which strategy_*.toml is actually live right now, and its key
    trade-affecting parameters — surfaced at the top of the report since every
    other section (Performance, Backtest Reconciliation) is only meaningful in
    light of whatever config produced it. Same file-selection as Rust's
    `config::load_latest` (`resolve_trade_assets`) — lexicographically latest
    in `config_dir`."""
    lines.append("## Strategy Config")
    lines.append("")

    try:
        files = sorted(config_dir.glob("strategy_*.toml"))
        if not files:
            lines.append(f"*No strategy_*.toml found in {config_dir}.*")
            lines.append("")
            return
        latest = files[-1]
        with open(latest, "rb") as f:
            cfg = tomllib.load(f)
    except Exception as e:
        lines.append(f"*Could not load strategy config: {e}*")
        lines.append("")
        return

    trade_assets = cfg.get("trade_assets", [])
    strategies = cfg.get("strategies", {}).get("default", [])

    lines.append(f"**Active file:** `{latest.name}`  (`meta.ts` {cfg.get('ts', '—')})")
    lines.append("")
    lines.extend([
        "| | Value |", "|---|---|",
        f"| Trade assets | {', '.join(trade_assets) or '—'} |",
        f"| Strategies | {', '.join(strategies) or '—'} |",
        f"| halt_rev / halt_prob | {_fmt_cfgval(_cfg_asset(cfg, 'halt_rev', 'default'), 0)} / "
        f"{_fmt_cfgval(_cfg_asset(cfg, 'halt_prob', 'default'), 0)} |",
        f"| halt_reset_hour (rev / hp) | {_fmt_cfgval(_cfg_asset(cfg, 'halt_reset_hour_rev', 'default'), 0)} / "
        f"{_fmt_cfgval(_cfg_asset(cfg, 'halt_reset_hour_hp', 'default'), 0)} HKT |",
    ])
    lines.append("")

    if trade_assets:
        lines.extend([
            "### Reversal Params (traded assets)", "",
            "| Asset | reversal | low_thresh | delta_pct_rev | sl_reversal | sl_pnl_rev | "
            "unwind_pnl_rev | unwind_time_rev |",
            "|---|---|---|---|---|---|---|---|",
        ])
        for asset in trade_assets:
            lines.append(
                f"| {asset} | {_fmt_cfgval(_cfg_asset(cfg, 'reversal', asset))} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'reversal_low_threshold', asset))} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'delta_pct_rev', asset), 5)} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'sl_reversal', asset))} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'sl_pnl_rev', asset))} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'unwind_pnl_rev', asset))} | "
                f"{_fmt_cfgval(_cfg_asset(cfg, 'unwind_time_rev', asset), 1)} |"
            )
        lines.append("")

    source = cfg.get("source")
    if source:
        lines.append("<details>")
        lines.append("<summary>Notes (meta.source)</summary>")
        lines.append("")
        lines.append(source)
        lines.append("")
        lines.append("</details>")
        lines.append("")


def render_data_quality(lines: list, result: dict) -> None:
    """price_feed/doc/incident_collector_data_loss_2026-07-12.md's proposed observer,
    rendered every day so a repeat of that incident (2+ days of silent ~85% data loss) shows
    up here automatically."""
    lines.append("## Data Quality")
    lines.append("")
    lines.append(
        "Independent tick-coverage check of `price_feed/raw/`'s sealed hourly files for every "
        "fully-elapsed hour in this window — added after the collector crash-loop incident "
        "(`price_feed/doc/incident_collector_data_loss_2026-07-12.md`) went unnoticed for 2+ "
        "days. Flags an hour as **GAP** (file exists, <50% of its 60 minutes have any tick) or "
        "**MISSING** (no sealed file at all)."
    )
    lines.append("")

    if result.get("error"):
        lines.append(f"*Check failed: {result['error']}*")
        lines.append("")
        return

    hours_checked = result.get("hours_checked", 0)
    flagged = result.get("flagged", [])
    if hours_checked == 0:
        lines.append("*No fully-elapsed hours to check yet in this window.*")
        lines.append("")
        return

    if not flagged:
        lines.append(f"{hours_checked} asset-hours checked — no gaps. 🎯")
        lines.append("")
        return

    lines.append(
        f"**{len(flagged)}/{hours_checked} asset-hours flagged.** "
        f"If this spans many consecutive hours across every asset, suspect the collector "
        f"itself (crash-loop, NATS/WS outage) rather than a per-asset feed issue — see the "
        f"incident doc above for exactly this shape."
    )
    lines.append("")
    lines.extend([
        "| Date | Hour (HKT) | Asset | Kind | Status | Coverage |",
        "|---|---|---|---|---|---|",
    ])
    for f in flagged:
        cov = f"{f['coverage_pct']:.1f}%" if f.get("coverage_pct") is not None else "—"
        lines.append(
            f"| {f['date']} | {f['hour']:02d}:00 | {f['asset']} | {f['kind']} | "
            f"{f['status']} | {cov} |"
        )
    lines.append("")


def render_bt_reconciliation(lines: list, live_vs_bt_rows: list, summary: dict, bt_vs_live_rows: list) -> None:
    lines.append("## Backtest Reconciliation")
    lines.append("")
    lines.append(
        "Independent cross-check against the Rust backtest engine's replay "
        "over the same price data — different from the Gamma cross-check "
        "above, which validates worker.rs's own WIN/LOSS correction logic "
        "against Polymarket's resolution, not the trading logic itself."
    )
    lines.append("")

    n_live = summary.get("n_live", 0)
    n_match = summary.get("n_match", 0)
    n_outcome_diff = summary.get("n_outcome_diff", 0)
    n_side_diff = summary.get("n_side_diff", 0)
    n_not_fired = summary.get("n_not_fired", 0)
    n_no_data = summary.get("n_no_data", 0)
    n_missed = len(bt_vs_live_rows)
    missed_pnl = sum(r["pnl"] for r in bt_vs_live_rows)

    lines.append(
        f"> **Live vs BT:** {n_live} live trades — {n_match} matched, "
        f"{n_outcome_diff} outcome-diff, {n_side_diff} side-diff, "
        f"{n_not_fired} bt-not-fired"
        + (f", {n_no_data} no price data" if n_no_data else "")
    )
    lines.append(
        f"> **BT vs Live:** {n_missed} cycle(s) live missed entirely"
        + (f" (would-be PnL {missed_pnl:+.4f} USDC)" if n_missed else "")
    )
    lines.append("")

    lines.append("### Live vs BT")
    lines.append("")
    if live_vs_bt_rows:
        lines.extend([
            "| Time | Asset | Strategy | Side | Entry Time | Entry Px | Exit Px | Cycle Δ% | Entry Δ% | "
            "Live Outcome | Live PnL | BT Outcome | BT PnL | Diff PnL | Status | Reason |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ])
        for r in live_vs_bt_rows:
            lines.append(
                f"| {r['time']} | {r['asset']} | {r['strategy']} | {r['side']} | "
                f"{r.get('entry_time', '—')} | {_fmt_price(r.get('entry_price'))} | "
                f"{_fmt_price(r.get('exit_price'))} | {_fmt_pct(r.get('cycle_delta_pct'))} | "
                f"{_fmt_pct(r.get('entry_delta_pct'))} | "
                f"{r['outcome']} | {r['pnl']:+.4f} | {r.get('bt_outcome') or '—'} | "
                f"{_fmt_pnl(r.get('bt_pnl'))} | {_fmt_pnl(r.get('diff_pnl'))} | {r['status']} | "
                f"{r.get('reason', '—')} |"
            )
        lines.append("")
    else:
        lines.append("*No live trades in window to reconcile against the backtest.*")
        lines.append("")

    lines.append("<details>")
    lines.append("<summary>Notes</summary>")
    lines.append("")
    lines.append(
        "**Entry Time** is T-seconds-before-cycle-close at the moment of entry "
        "(same convention as worker.rs's live \"T-Ns\" heartbeat logs). "
        "**Entry Px**/**Exit Px** are the actual CLOB (Polymarket order-book) "
        "prices traded. **Cycle Δ%** and **Entry Δ%**, by contrast, are computed "
        "from the *underlying* (Binance) asset price, not CLOB — a CLOB "
        "probability swinging from 0.44 to 0.95 isn't a meaningful \"price "
        "move,\" it's just the market pricing in a near-certain outcome; see "
        "`trader/doc/incident_delta_pct_2026-07-12.md`. **Cycle Δ%** is the "
        "underlying's move over the whole cycle, open→close "
        "(`(close − open) / open`) — this is literally what the market "
        "resolves against. **Entry Δ%** is how far the underlying had already "
        "moved from cycle open to the moment of entry "
        "(`(entry − cycle_open) / cycle_open`) — how much of that move had "
        "already happened before the trade was placed. **Reason** classifies "
        "*why* a mismatch happened, where determinable — see "
        "`trader/doc/incident_bt_vs_live_discrepancy_2026-07-12.md` and "
        "`trader/doc/plan_timeout_backtest_and_mismatch_reason_2026-07-12.md` "
        "for the method and its limits (labels are investigation-grade "
        "pointers, not proof of causation, except for a halt window — a "
        "cycle live was actually suppressed for is a confident explanation)."
    )
    lines.append("")
    lines.append("</details>")
    lines.append("")

    lines.append("### BT vs Live (cycles live missed)")
    lines.append("")
    if bt_vs_live_rows:
        lines.append(
            f"{n_missed} cycle(s) the backtest fired but live did not trade at all "
            f"(either side) — would-be PnL {missed_pnl:+.4f} USDC."
        )
        lines.append("")
        lines.extend([
            "| Cycle (HKT) | Asset | Strategy | Side | Entry Time | Entry Px | Exit Px | "
            "Cycle Δ% | Entry Δ% | BT Outcome | BT PnL | Reason |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|",
        ])
        for r in bt_vs_live_rows:
            lines.append(
                f"| {r['time']} | {r['asset']} | {r['strategy']} | {r['side']} | "
                f"{r.get('entry_time', '—')} | {_fmt_price(r.get('entry_price'))} | "
                f"{_fmt_price(r.get('exit_price'))} | {_fmt_pct(r.get('cycle_delta_pct'))} | "
                f"{_fmt_pct(r.get('entry_delta_pct'))} | "
                f"{r['outcome']} | {r['pnl']:+.4f} | {r.get('reason', '—')} |"
            )
        lines.append(f"| **Total** | | | | | | | | | | **{missed_pnl:+.4f}** | |")
        lines.append("")
    else:
        lines.append("None — every cycle the backtest fired, live also traded. \U0001f3af")
        lines.append("")


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

def _make_sections_collapsible(lines: list) -> list:
    """Wrap every top-level '## ' section in a collapsible <details> block.
    Closed by default: the blockquote one-liners at the top of the report
    (Performance/Gamma/Data quality) already surface the headline numbers, so
    a long section — the Data Quality gap table in particular, which can run
    to 200+ rows during an incident — doesn't force scrolling past it to
    reach everything below. The original '## Header' line is kept inside the
    block (not just in <summary>) so in-page anchors keep working once
    expanded."""
    out: list = []
    open_section = False

    def close_section() -> None:
        nonlocal open_section
        if open_section:
            while out and out[-1] == "":
                out.pop()
            out.append("")
            out.append("</details>")
            out.append("")
            open_section = False

    for line in lines:
        if line.startswith("## "):
            close_section()
            out.append("<details>")
            out.append(f"<summary><h2>{line[3:].strip()}</h2></summary>")
            out.append("")
            out.append(line)
            open_section = True
        else:
            out.append(line)
    close_section()
    return out


def write_markdown_summary(
    summary: dict, perf_stats: dict, window_start: datetime, window_end: datetime,
    out_dir: Path, bt_result: Optional[tuple] = None,
    data_quality_result: Optional[dict] = None,
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
    dq = data_quality_result or {}
    dq_flagged = dq.get("flagged", [])
    dq_checked = dq.get("hours_checked", 0)
    if dq.get("error"):
        lines.append(f"> **Data quality:** check failed ({dq['error']})")
    elif dq_checked:
        lines.append(
            f"> **Data quality:** {len(dq_flagged)}/{dq_checked} asset-hours flagged"
            + (" — see bottom" if dq_flagged else " — no gaps 🎯")
        )
    lines.append("")
    render_strategy_config(lines, CONFIG_DIR)

    if not perf_stats:
        lines.append("> **No trades in this window.** The bot was not active during this period.")
        lines.append("")
    else:
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
                "| Time | Asset | Strategy | Side | Outcome | Entry Price | Exit Price | PnL | "
                "Entry Signal (ms) | Entry Process (ms) | Exit Signal (ms) | Exit Process (ms) |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|",
            ])
            for t in trade_history:
                lines.append(
                    f"| {t['time']} | {t['asset']} | {t['strategy']} | {t['side']} | {t['outcome']} | "
                    f"{t['token_price']:.4f} | {t['exit_price']:.4f} | {t['pnl']:+.4f} | "
                    f"{t['entry_signal_latency_ms']:.0f} | {t['entry_process_latency_ms']:.0f} | "
                    f"{t['exit_signal_latency_ms']:.0f} | {t['exit_process_latency_ms']:.0f} |"
                )
            total_pnl = sum(t["pnl"] for t in trade_history)
            lines.append(f"| **Total** | | | | | | | **{total_pnl:+.4f}** | | | | |")
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

        gamma_timeout = summary.get("gamma_timeout", {})
        gt_continued = gamma_timeout.get("continued", 0)
        gt_halted = gamma_timeout.get("halted", 0)
        gt_details = gamma_timeout.get("details", [])

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
            f"| Gamma timeout — continued (balance up, as designed) | {gt_continued} |",
            f"| Gamma timeout — halted (pending manual review, as designed) | {gt_halted} |",
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
                "not noise.** (Rows where Gamma simply never resolved in time are listed "
                "separately below, not here — see \"Gamma Timeout\".)"
            )
        else:
            if pending == total_rows:
                lines.append("All trades still pending resolution.")
            elif wrong == 0:
                lines.append("None — worker.rs's outcome matched Gamma on every resolved trade. \U0001f3af")
        lines.append("")

        if gt_details:
            lines.append("### Gamma Timeout (as designed)")
            lines.append("")
            lines.append(
                "Gamma never resolved these within the retry deadline, so worker.rs used the "
                "balance-increase override (`trader/src/balance.rs::GammaBalanceTracker`, added "
                "2026-07-09) instead of guessing: **CONTINUED** = account balance was up since the "
                "last cycle's checkpoint (sampled ~1-2min into every cycle), so new entries kept "
                "going; **HALTED** = it wasn't, so new entries were suppressed pending manual "
                "review. Both are the worker behaving exactly as designed, not a correction-logic "
                "bug — `Hindsight` shows whether the provisional call happened to match Gamma's "
                "eventual resolution, for information only."
            )
            lines.append("")
            lines.extend([
                "| Time | Slug | Side | Logged | Event | Hindsight | Hindsight Match |",
                "|---|---|---|---|---|---|---|",
            ])
            for g in gt_details:
                match_str = "✓" if g["hindsight_match"] else "✗"
                lines.append(
                    f"| {g['time']} | {g['slug']} | {g['side']} | {g['logged']} | {g['event']} | "
                    f"{g['hindsight']} | {match_str} |"
                )
            lines.append("")

    # Backtest reconciliation always renders, even on a 0-live-trade day —
    # that's exactly the case where "did live miss a trade the backtest
    # would have taken" is most worth knowing, not the case to skip it in.
    if bt_result is not None:
        live_vs_bt_rows, bt_summary, bt_vs_live_rows = bt_result
        render_bt_reconciliation(lines, live_vs_bt_rows, bt_summary, bt_vs_live_rows)
    else:
        lines.append("## Backtest Reconciliation")
        lines.append("")
        lines.append("*Unavailable this run — see recon_cron.log for the reason.*")
        lines.append("")

    # Rendered last — the blockquote one-liner at the top of the report already
    # surfaces the headline number, so the detailed gap table (200+ rows during
    # an incident) doesn't force scrolling past it to reach the sections that
    # matter on a normal day.
    render_data_quality(lines, dq)

    lines = _make_sections_collapsible(lines)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    console.print(f"[green]✓ Markdown summary → {out_path}[/green]")
    return out_path


def git_commit_push(file_paths: list, message: str) -> None:
    try:
        rel_paths = [str(Path(p).resolve().relative_to(REPO_ROOT.parent)) for p in file_paths]
        subprocess.run(["git", "-C", str(REPO_ROOT.parent), "add", *rel_paths], check=True)
        # `-- rel_paths` scopes the commit to exactly these paths' staged changes,
        # regardless of anything else that happens to be staged in the index at
        # this moment (e.g. a concurrent interactive `git add`) — plain `git
        # commit -m message` commits the *whole* index, which previously let an
        # unrelated manual commit-in-progress get silently swept into a
        # "recon: ..." commit under this message (observed 2026-07-07).
        subprocess.run(["git", "-C", str(REPO_ROOT.parent), "commit", "-m", message, "--", *rel_paths], check=True)
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


def _safe_run_backtest_reconciliation(window_start: datetime, window_end: datetime, annotated_live_rows: list) -> Optional[tuple]:
    """Belt-and-suspenders wrapper around run_backtest_reconciliation — that
    function is already internally defensive, but this is optional/
    additive reporting and must never be able to take the whole recon run
    down with it, however it fails."""
    try:
        return run_backtest_reconciliation(window_start, window_end, annotated_live_rows)
    except Exception as e:
        console.print(f"[yellow]⚠ Backtest reconciliation failed unexpectedly: {e}[/yellow]")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="poly_rust trade reconciliation")
    parser.add_argument("--wallet", type=str, help="Override FUND_ADDRESS from .env (informational only)")
    parser.add_argument("--no-push", action="store_true", help="Skip git commit+push of the markdown report")
    parser.add_argument("--no-bt", action="store_true", help="Skip the backtest reconciliation section (faster, for quick manual runs)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--today", action="store_true", help="Reconcile current 24h window anchored at 8pm HKT (default)")
    mode.add_argument("--dt", type=str, help="Reconcile 24h window from 8pm on YYYYMMDD")
    args = parser.parse_args()

    window_start, window_end = _resolve_window(args.dt)
    console.print(f"[dim]Window: {window_start:%Y-%m-%d %H:%M} → {window_end:%Y-%m-%d %H:%M} HKT[/dim]")

    log_dir = REPO_ROOT / "live_logs"
    out_dir = REPO_ROOT / "results" / "daily_recon"

    # Independent of --no-bt / whether there are any trades this window — a repeat of
    # incident_collector_data_loss_2026-07-12.md matters most exactly on a quiet day.
    console.print("[dim]Syncing price_feed/raw from Oracle for the data quality check...[/dim]")
    sync_price_feed_from_oracle(PRICE_FEED_SYNC_SCRIPT)
    data_quality_result = safe_check_data_quality(PRICE_FEED_RAW_DIR, window_start, window_end)

    files = find_trade_logs(log_dir)
    if not files:
        console.print(f"[yellow]No trade logs found in {log_dir}/[/yellow]")
        raise SystemExit(0)

    rows = load_and_filter(files, window_start.timestamp(), window_end.timestamp())
    if not rows:
        console.print(f"[yellow]No trades in window {window_start:%Y-%m-%d %H:%M} → {window_end:%Y-%m-%d %H:%M} HKT.[/yellow]")
        bt_result = None if args.no_bt else _safe_run_backtest_reconciliation(window_start, window_end, [])
        md_path = write_markdown_summary(
            {"direction": {}, "stoploss": {}, "total_rows": 0}, {}, window_start, window_end, out_dir,
            bt_result=bt_result, data_quality_result=data_quality_result,
        )
        if not args.no_push:
            git_commit_push([md_path], f"recon: {window_start:%Y-%m-%d} — 0 trades")
        raise SystemExit(0)

    console.print(f"[dim]Loaded {len(rows)} trades after filtering & dedup.[/dim]")
    console.print("[dim]Querying Gamma API for market outcomes...[/dim]")
    gamma_timeout_events = parse_gamma_timeout_events(log_dir / "live.log")
    annotated, summary = annotate_rows(rows, gamma_timeout_events)
    perf_stats = compute_performance_stats(annotated)

    bt_result = None if args.no_bt else _safe_run_backtest_reconciliation(window_start, window_end, annotated)

    md_path = write_markdown_summary(
        summary, perf_stats, window_start, window_end, out_dir,
        bt_result=bt_result, data_quality_result=data_quality_result,
    )

    if not args.no_push:
        direction = summary["direction"]
        git_commit_push(
            [md_path],
            f"recon: {window_start:%Y-%m-%d} — {direction['correct']}/{direction['resolved']} "
            f"matched ({direction['accuracy']:.0f}%)",
        )


if __name__ == "__main__":
    main()
