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


def load_cycle_open_prices(assets: list, dates: list, prices_dir: Path) -> dict:
    """slug -> {"UP": price, "DOWN": price} taken from the earliest poly tick
    per slug (closest sample to cycle open) in the same local
    {asset}_poly_{date}.parquet files run_backtest_reconciliation already
    builds/syncs for the BT replay — reuses that data instead of making any
    extra network calls just for the Entry Δ% column."""
    import pandas as pd
    out: dict = {}
    for asset in assets:
        for date in dates:
            path = prices_dir / f"{asset}_poly_{date}.parquet"
            if not path.exists():
                continue
            try:
                df = pd.read_parquet(path)
            except Exception:
                continue
            if df.empty:
                continue
            first = df.sort_values("ts").groupby("slug", as_index=False).first()
            for _, row in first.iterrows():
                out[row["slug"]] = {"UP": float(row["up"]), "DOWN": float(row["dn"])}
    return out


def _safe_load_cycle_open_prices(assets: list, dates: list, prices_dir: Path) -> dict:
    """Same defensive-by-design rule as the rest of the BT reconciliation
    pipeline: a missing/corrupt price file must degrade the Entry Δ% column
    to "—", never take down the report."""
    try:
        return load_cycle_open_prices(assets, dates, prices_dir)
    except Exception as e:
        console.print(f"[yellow]⚠ Could not load cycle-open prices for Entry Δ%: {e}[/yellow]")
        return {}


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
                      cycle_open_prices: Optional[dict] = None) -> tuple:
    """One row per live trade: does the backtest agree?

    Status classification:
      MATCH            - bt fired the same (slug, side), same outcome
      OUTCOME DIFF     - bt fired the same (slug, side), different outcome
      SIDE DIFF        - bt fired the opposite side for that slug
      BT DID NOT FIRE  - bt had price data for the asset but skipped the cycle
      NO PRICE DATA    - bt couldn't run at all for this asset/date
    """
    cycle_open_prices = cycle_open_prices or {}
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

        entry_price = lt.get("token_price")
        exit_price = lt.get("exit_price")
        open_price = cycle_open_prices.get(lt["slug"], {}).get(lt["side"])
        extra = {
            "entry_time": t_minus_str(lt["slug"], lt.get("entry_ts")),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "cycle_delta_pct": _pct_change(entry_price, exit_price),
            "entry_delta_pct": _pct_change(open_price, entry_price),
        }

        if bt_same:
            total_bt_pnl += bt_same["pnl"]
            diff_pnl = lt["pnl"] - bt_same["pnl"]
            if bt_same["outcome"] == lt["outcome"]:
                n_match += 1
                status = "MATCH"
            else:
                n_outcome_diff += 1
                status = f"OUTCOME DIFF (live={lt['outcome']} bt={bt_same['outcome']})"
            table.append({**lt, **extra, "bt_outcome": bt_same["outcome"], "bt_pnl": bt_same["pnl"],
                          "diff_pnl": diff_pnl, "status": status})
        elif bt_opp:
            n_side_diff += 1
            total_bt_pnl += bt_opp["pnl"]
            table.append({**lt, **extra, "bt_outcome": bt_opp["outcome"], "bt_pnl": bt_opp["pnl"],
                          "diff_pnl": lt["pnl"] - bt_opp["pnl"],
                          "status": f"SIDE DIFF (bt side={opp_side})"})
        elif lt["asset"] not in assets_with_data:
            n_no_data += 1
            table.append({**lt, **extra, "bt_outcome": None, "bt_pnl": None, "diff_pnl": None,
                          "status": "NO PRICE DATA"})
        else:
            n_not_fired += 1
            table.append({**lt, **extra, "bt_outcome": None, "bt_pnl": None, "diff_pnl": None,
                          "status": "BT DID NOT FIRE"})

    summary = {
        "n_live": len(live_rows), "n_match": n_match, "n_outcome_diff": n_outcome_diff,
        "n_side_diff": n_side_diff, "n_not_fired": n_not_fired, "n_no_data": n_no_data,
        "total_live_pnl": total_live_pnl, "total_bt_pnl": total_bt_pnl,
    }
    return table, summary


def build_bt_vs_live(bt_rows: list, live_rows: list, cycle_open_prices: Optional[dict] = None) -> list:
    """Cycles the backtest fired but live did not trade at all (either side).

    A live trade on the *opposite* side of a bt-fired cycle already shows up
    as SIDE DIFF in the Live vs BT table, so this only needs to exclude
    slugs live touched on any side — not check side equality itself.
    """
    cycle_open_prices = cycle_open_prices or {}
    live_slugs = {lt["slug"] for lt in live_rows}
    missed = []
    for r in bt_rows:
        if r["slug"] in live_slugs:
            continue
        entry_price = r.get("token_price")
        exit_price = r.get("exit_price")
        open_price = cycle_open_prices.get(r["slug"], {}).get(r["side"])
        missed.append({
            "time": datetime.fromtimestamp(r["cycle_ts"], tz=HKT).strftime("%Y-%m-%d %H:%M:%S") if r["cycle_ts"] else "",
            "asset": r["asset"], "strategy": r["strategy"], "side": r["side"],
            "outcome": r["outcome"], "pnl": r["pnl"],
            "entry_time": t_minus_str(r["slug"], r.get("entry_ts")),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "cycle_delta_pct": _pct_change(entry_price, exit_price),
            "entry_delta_pct": _pct_change(open_price, entry_price),
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
    cycle_open_prices = _safe_load_cycle_open_prices(assets, dates, BACKTEST_PRICES_DIR)

    live_vs_bt_rows, bt_summary = build_live_vs_bt(live_norm, bt_in_window, assets_with_data, cycle_open_prices)
    bt_vs_live_rows = build_bt_vs_live(bt_in_window, live_norm, cycle_open_prices)

    return live_vs_bt_rows, bt_summary, bt_vs_live_rows


def _fmt_pnl(v) -> str:
    return f"{v:+.4f}" if v is not None else "—"


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
    lines.append(
        "**Entry Time** is T-seconds-before-cycle-close at the moment of entry "
        "(same convention as worker.rs's live \"T-Ns\" heartbeat logs). "
        "**Cycle Δ%** is the held token's own price move, entry→exit "
        "(`(exit − entry) / entry`). **Entry Δ%** is how far the price had "
        "already moved from cycle open to the moment of entry "
        "(`(entry − cycle_open) / cycle_open`) — how much signal had built up "
        "before the trade was placed."
    )
    lines.append("")
    if live_vs_bt_rows:
        lines.extend([
            "| Time | Asset | Strategy | Side | Entry Time | Entry Px | Exit Px | Cycle Δ% | Entry Δ% | "
            "Live Outcome | Live PnL | BT Outcome | BT PnL | Diff PnL | Status |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ])
        for r in live_vs_bt_rows:
            lines.append(
                f"| {r['time']} | {r['asset']} | {r['strategy']} | {r['side']} | "
                f"{r.get('entry_time', '—')} | {_fmt_price(r.get('entry_price'))} | "
                f"{_fmt_price(r.get('exit_price'))} | {_fmt_pct(r.get('cycle_delta_pct'))} | "
                f"{_fmt_pct(r.get('entry_delta_pct'))} | "
                f"{r['outcome']} | {r['pnl']:+.4f} | {r.get('bt_outcome') or '—'} | "
                f"{_fmt_pnl(r.get('bt_pnl'))} | {_fmt_pnl(r.get('diff_pnl'))} | {r['status']} |"
            )
        lines.append("")
    else:
        lines.append("*No live trades in window to reconcile against the backtest.*")
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
            "Cycle Δ% | Entry Δ% | BT Outcome | BT PnL |",
            "|---|---|---|---|---|---|---|---|---|---|---|",
        ])
        for r in bt_vs_live_rows:
            lines.append(
                f"| {r['time']} | {r['asset']} | {r['strategy']} | {r['side']} | "
                f"{r.get('entry_time', '—')} | {_fmt_price(r.get('entry_price'))} | "
                f"{_fmt_price(r.get('exit_price'))} | {_fmt_pct(r.get('cycle_delta_pct'))} | "
                f"{_fmt_pct(r.get('entry_delta_pct'))} | "
                f"{r['outcome']} | {r['pnl']:+.4f} |"
            )
        lines.append(f"| **Total** | | | | | | | | | | **{missed_pnl:+.4f}** |")
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

def write_markdown_summary(
    summary: dict, perf_stats: dict, window_start: datetime, window_end: datetime,
    out_dir: Path, bt_result: Optional[tuple] = None,
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
            bt_result=bt_result,
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

    md_path = write_markdown_summary(summary, perf_stats, window_start, window_end, out_dir, bt_result=bt_result)

    if not args.no_push:
        direction = summary["direction"]
        git_commit_push(
            [md_path],
            f"recon: {window_start:%Y-%m-%d} — {direction['correct']}/{direction['resolved']} "
            f"matched ({direction['accuracy']:.0f}%)",
        )


if __name__ == "__main__":
    main()
