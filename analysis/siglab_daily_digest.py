#!/usr/bin/env python3
"""Reads siglab's live JSONL trade log, applies siglab_backtest_stats.py's toolkit, and
writes a bottom-up daily digest (`{report_dir}/{date}/digest_{date}.md`) plus an appended
`{report_dir}/candidate_ledger.csv`.

See `siglab/doc/plan_better_signal_2026-07-24.md` for the full design, especially its
"Revision after DeepSeek review" section — this script implements that revised design, not
the plan's original first draft. Key decisions, in one place:

- **Verdicts are computed on the cumulative trade history up to and including the target
  date, not on that single day's trades alone.** A single day's handful of trades per combo
  is far too thin a sample for even a binomial test to say anything honest. The digest is a
  daily *checkpoint* that re-evaluates an expanding sample each morning, not 24 independent
  daily judgments — see `compute_combo_stats`.
- **The primary per-combo signal is an exact binomial test** of trade-level win/loss (a
  trade "wins" if `pnl > 0`) against the applicable null win rate (barrier or
  market-implied, blended by trade-type mix — see `blended_null_and_eligible_trades`),
  Benjamini-Hochberg corrected *globally* across every combo evaluated in one run. This
  replaces the original plan's DSR-per-combo-per-day idea, which DeepSeek's review correctly
  flagged as false precision at siglab's current data volume (daily-return Sharpe needs far
  more independent observations than 11-40 days provides).
- **PBO/DSR are computed once per `(market, strategy)` group** (one 16-or-18-variant grid at
  a time), using *weekly* (not daily) PnL blocks to reduce per-block noise, and gated behind
  a real warm-up (`GROUP_DIAGNOSTIC_MIN_WEEKS`) — informational only, never a per-variant
  verdict gate. See `compute_group_diagnostics`.
- **`rf_param_importance` is not computed at all** — dropped per DeepSeek's review (each
  group has only 16-18 highly-correlated combos, nowhere near enough for a reliable random
  forest).
- **"Markets monitored" uses trade recency as a coverage proxy, not literal tick
  staleness** — this script only ever sees the JSONL trade log, not `report.rs`'s in-memory
  `SharedSnapshots` (which is never persisted to disk), so "hours since last trade" is the
  honest signal available here, not "seconds since last tick".
- **PnL is idealized** — no bid-ask spread, taker fee, or slippage is modeled anywhere in
  siglab's paper-trade simulation. Every statistical claim in the digest is prominently
  caveated as being about relative signal quality, not a tradable net edge.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tomllib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from siglab_backtest_stats import (  # noqa: E402
    benjamini_hochberg,
    binomial_test_win_rate,
    daily_pnl_panel,
    deflated_sharpe_ratio,
    null_win_rate_barrier,
    null_win_rate_market_implied,
    pbo_cscv,
)

HKT = timezone(timedelta(hours=8))
BARRIER_OUTCOMES = {"STOPLOSS", "UNWIND"}
RESOLUTION_OUTCOMES = {"WIN", "LOSS"}

MIN_TRADES_FOR_VERDICT = 50
ALPHA = 0.05
GROUP_DIAGNOSTIC_MIN_WEEKS = 8  # 8 weekly blocks needed before PBO/DSR are shown at all
GROUP_DIAGNOSTIC_N_SPLITS = 8  # C(8,4)=70 splits over weekly (not daily) blocks


# ---------------------------------------------------------------------------
# Reading the trade log
# ---------------------------------------------------------------------------


def read_trades(log_path: Path) -> list[dict]:
    """Reads the JSONL trade log, tolerating malformed lines — the log is actively
    appended to by a live process, so the very last line can be a partial write (caught
    mid-flush). A bad line is skipped, not fatal; see the module docstring."""
    trades = []
    skipped = 0
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
    if skipped:
        print(f"[digest] skipped {skipped} malformed/incomplete JSONL line(s)", file=sys.stderr)
    return trades


def hkt_day(unix_ts: float) -> str:
    return datetime.fromtimestamp(unix_ts, tz=HKT).strftime("%Y-%m-%d")


def hkt_iso_week(unix_ts: float) -> str:
    dt = datetime.fromtimestamp(unix_ts, tz=HKT)
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def build_dataframe(trades: list[dict]) -> pd.DataFrame:
    """Normalizes raw trade dicts into a DataFrame with the columns every downstream
    function needs. Drops rows missing `market` (pre-2026-07-17 records logged before that
    field existed, `#[serde(default)]`'d to `""` on the Rust side — see `record.rs`) since
    they can't be grouped into a combo; these are historical and won't recur."""
    if not trades:
        return pd.DataFrame(
            columns=[
                "market",
                "strategy",
                "variant_id",
                "market_kind",
                "outcome",
                "pnl",
                "entry_ts",
                "token_price",
                "day",
                "week",
            ]
        )
    df = pd.DataFrame(trades)
    df = df[df["market"].astype(bool)].copy()
    df["day"] = df["entry_ts"].apply(hkt_day)
    df["week"] = df["entry_ts"].apply(hkt_iso_week)
    return df


# ---------------------------------------------------------------------------
# Barrier params — reversal's sl/tp come from config/markets.toml; v_shape's are encoded
# directly in the variant_id string (`v_{high1}_{low}_{high2}_{sl_pnl}_{unwind_pnl}`, per
# the naming convention documented in siglab/src/report.rs's config-section renderer) so no
# config lookup is needed for it.
# ---------------------------------------------------------------------------


def load_reversal_barrier_params(config_path: Path) -> dict[str, tuple[float, float]]:
    """Returns `{variant_id: (sl_pnl_rev, unwind_pnl_rev)}` for every `strategy = "reversal"`
    `[[variant]]` in `config/markets.toml`."""
    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)
    out = {}
    for v in cfg.get("variant", []):
        if v.get("strategy") == "reversal":
            sl = v.get("sl_pnl_rev")
            tp = v.get("unwind_pnl_rev")
            if sl is not None and tp is not None:
                out[v["id"]] = (float(sl), float(tp))
    return out


def barrier_params_for(
    strategy: str, variant_id: str, reversal_params: dict[str, tuple[float, float]]
) -> tuple[float, float] | None:
    if strategy == "reversal":
        return reversal_params.get(variant_id)
    if strategy == "v_shape":
        # v_{high1}_{low}_{high2}_{sl_pnl}_{unwind_pnl}
        parts = variant_id.split("_")
        if len(parts) == 6 and parts[0] == "v":
            try:
                return float(parts[4]), float(parts[5])
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# Per-combo statistics — the primary, trade-count-driven signal.
# ---------------------------------------------------------------------------


def blended_null_and_eligible_trades(
    combo_trades: pd.DataFrame, sl_tp: tuple[float, float] | None
) -> tuple[pd.DataFrame, float | None]:
    """Splits a combo's trades into the ones an outcome-appropriate null hypothesis
    actually applies to (STOPLOSS/UNWIND -> barrier null, WIN/LOSS -> market-implied null),
    excludes TIMEOUT (no clean null — see `siglab_backtest_stats`'s module docstring),
    and returns `(eligible_trades, blended_null_win_rate)`. When a combo has both barrier-
    and resolution-type trades, the blended null is their counts-weighted average — the
    same "blend by trade-type mix" approach `../btc_5mins`'s source module recommends for
    a mixed sweep combo.
    """
    barrier_trades = combo_trades[combo_trades["outcome"].isin(BARRIER_OUTCOMES)]
    resolution_trades = combo_trades[combo_trades["outcome"].isin(RESOLUTION_OUTCOMES)]

    barrier_null = null_win_rate_barrier(*sl_tp) if sl_tp is not None else None
    resolution_null = (
        null_win_rate_market_implied(resolution_trades["token_price"])
        if not resolution_trades.empty
        else None
    )

    n_barrier = len(barrier_trades) if barrier_null is not None else 0
    n_resolution = len(resolution_trades) if resolution_null is not None else 0
    total = n_barrier + n_resolution
    if total == 0:
        return combo_trades.iloc[0:0], None

    eligible = pd.concat(
        [barrier_trades if barrier_null is not None else barrier_trades.iloc[0:0],
         resolution_trades if resolution_null is not None else resolution_trades.iloc[0:0]]
    )
    blended = (
        (barrier_null or 0.0) * n_barrier + (resolution_null or 0.0) * n_resolution
    ) / total
    return eligible, blended


@dataclass
class ComboRow:
    market: str
    strategy: str
    variant_id: str
    market_kind: str
    total_trades: int
    eligible_trades: int
    timeout_trades: int
    wins: int
    realized_win_rate: float | None
    null_win_rate: float | None
    edge: float | None
    p_value: float | None
    total_pnl: float


def compute_combo_stats(
    df: pd.DataFrame, reversal_params: dict[str, tuple[float, float]]
) -> pd.DataFrame:
    """One row per `(market, strategy, variant_id)` combo, computed over ALL trades in
    `df` up to and including the target date (an expanding, cumulative sample — see the
    module docstring for why, not a single day's trades)."""
    rows: list[ComboRow] = []
    group_cols = ["market", "strategy", "variant_id"]
    for (market, strategy, variant_id), combo_trades in df.groupby(group_cols):
        sl_tp = barrier_params_for(strategy, variant_id, reversal_params)
        eligible, null_wr = blended_null_and_eligible_trades(combo_trades, sl_tp)
        timeout_n = int((combo_trades["outcome"] == "TIMEOUT").sum())

        wins = int((eligible["pnl"] > 0).sum()) if not eligible.empty else 0
        n_eligible = len(eligible)
        p_value = None
        realized_wr = None
        edge = None
        if n_eligible > 0 and null_wr is not None and 0.0 < null_wr < 1.0:
            realized_wr = wins / n_eligible
            edge = realized_wr - null_wr
            p_value = binomial_test_win_rate(wins, n_eligible, null_wr)["p_value"]

        rows.append(
            ComboRow(
                market=market,
                strategy=strategy,
                variant_id=variant_id,
                market_kind=combo_trades["market_kind"].iloc[0],
                total_trades=len(combo_trades),
                eligible_trades=n_eligible,
                timeout_trades=timeout_n,
                wins=wins,
                realized_win_rate=realized_wr,
                null_win_rate=null_wr,
                edge=edge,
                p_value=p_value,
                total_pnl=float(combo_trades["pnl"].sum()),
            )
        )
    return pd.DataFrame([r.__dict__ for r in rows])


def apply_bh_correction(combo_df: pd.DataFrame) -> pd.DataFrame:
    """BH-FDR correction across every combo with a computable p-value in this run —
    global, not per-group, since the digest evaluates every group at once (see
    `siglab_backtest_stats.benjamini_hochberg`'s docstring for why per-group deflation
    alone isn't enough here)."""
    combo_df = combo_df.copy()
    testable = combo_df["p_value"].notna()
    combo_df["q_value"] = np.nan
    if testable.any():
        result = benjamini_hochberg(combo_df.loc[testable, "p_value"].tolist(), alpha=ALPHA)
        combo_df.loc[testable, "q_value"] = result["q_values"]
    return combo_df


def assign_verdict(row: pd.Series) -> str:
    if row["eligible_trades"] < MIN_TRADES_FOR_VERDICT or pd.isna(row["q_value"]):
        return "INSUFFICIENT-SAMPLE"
    if row["q_value"] < ALPHA:
        return "PROMOTE-CANDIDATE" if row["edge"] > 0 else "REJECT"
    return "WATCH"


# ---------------------------------------------------------------------------
# candidate_ledger.csv — idempotent daily upsert, atomic write.
# ---------------------------------------------------------------------------

LEDGER_COLUMNS = [
    "date",
    "market",
    "strategy",
    "variant_id",
    "total_trades",
    "eligible_trades",
    "realized_win_rate",
    "null_win_rate",
    "edge",
    "p_value",
    "q_value",
    "verdict",
]


def merge_ledger_rows(ledger_path: Path, date: str, combo_df: pd.DataFrame) -> pd.DataFrame:
    """Pure in-memory upsert by `(date, market, strategy, variant_id)` — if this script is
    re-run for the same date (a manual retry, or a fixed bug re-run), the old rows for that
    date are replaced, not duplicated. Does **not** write to disk — see `write_ledger`,
    called separately (and deliberately last, after the digest markdown — see `main`) so a
    push racing the two file writes grabs a self-consistent "digest without today's very
    latest ledger row" rather than the other way around."""
    new_rows = combo_df.copy()
    new_rows.insert(0, "date", date)
    new_rows = new_rows[LEDGER_COLUMNS]

    if ledger_path.exists():
        existing = pd.read_csv(ledger_path, dtype={"date": str})
        existing = existing[existing["date"] != date]
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        merged = new_rows
    return merged.sort_values(["date", "market", "strategy", "variant_id"])


def write_ledger(ledger_path: Path, merged: pd.DataFrame) -> None:
    """Atomic write (temp file + rename) so a crash mid-write can't corrupt the ledger the
    streak computation depends on."""
    tmp_path = ledger_path.with_suffix(".csv.tmp")
    merged.to_csv(tmp_path, index=False, quoting=csv.QUOTE_MINIMAL)
    tmp_path.replace(ledger_path)


def update_ledger(ledger_path: Path, date: str, combo_df: pd.DataFrame) -> pd.DataFrame:
    """Merge + write in one step — convenience wrapper kept for callers (including tests)
    that don't need to control write ordering relative to another file. `main()` calls
    `merge_ledger_rows`/`write_ledger` directly instead, to control that ordering."""
    merged = merge_ledger_rows(ledger_path, date, combo_df)
    write_ledger(ledger_path, merged)
    return merged


def compute_streaks(ledger_df: pd.DataFrame, as_of_date: str) -> dict[tuple, int]:
    """Consecutive trailing days (ending at `as_of_date`) a combo has held
    PROMOTE-CANDIDATE, read from ledger history.

    **What this streak means, and doesn't mean**: because each day's verdict is computed
    on an *expanding* cumulative sample (see `compute_combo_stats`), consecutive days are
    highly serially correlated, not independent re-confirmations — DeepSeek's review
    correctly flagged the plan's original framing of this as "3 independent days of
    evidence" as wrong. Read it instead as: "this combo's cumulative-sample verdict has
    stayed PROMOTE-CANDIDATE across N consecutive daily checkpoints, i.e. it hasn't
    flipped back as more data arrived." The real statistical bar is `MIN_TRADES_FOR_VERDICT`
    + BH-corrected significance on the *current* cumulative sample, not streak length —
    the streak is a stability/recency indicator layered on top, not independent evidence.
    """
    if ledger_df.empty:
        return {}
    streaks: dict[tuple, int] = {}
    for key, group in ledger_df.groupby(["market", "strategy", "variant_id"]):
        by_date = group.set_index("date")["verdict"].to_dict()
        streak = 0
        cursor = datetime.strptime(as_of_date, "%Y-%m-%d")
        while True:
            d = cursor.strftime("%Y-%m-%d")
            if by_date.get(d) == "PROMOTE-CANDIDATE":
                streak += 1
                cursor -= timedelta(days=1)
            else:
                break
        if streak > 0:
            streaks[key] = streak
    return streaks


# ---------------------------------------------------------------------------
# Group-level PBO/DSR — informational only, weekly blocks, warm-up gated.
# ---------------------------------------------------------------------------


def compute_group_diagnostics(df: pd.DataFrame) -> dict[tuple[str, str], dict]:
    """One PBO + best-combo DSR reading per `(market, strategy)` group, using weekly (not
    daily) PnL blocks — DeepSeek's review flagged daily blocks over an 11-40 day history as
    unsound (the Sharpe estimate within a 1-4-day block is nearly pure noise). Weekly
    blocks need `GROUP_DIAGNOSTIC_MIN_WEEKS` * 7 days of history before this returns
    anything but an "insufficient history" placeholder — informational only, never feeds
    a per-variant verdict."""
    out: dict[tuple[str, str], dict] = {}
    for (market, strategy), group_trades in df.groupby(["market", "strategy"]):
        n_weeks = group_trades["week"].nunique()
        if n_weeks < GROUP_DIAGNOSTIC_MIN_WEEKS:
            out[(market, strategy)] = {
                "status": "insufficient_history",
                "n_weeks": int(n_weeks),
                "weeks_needed": GROUP_DIAGNOSTIC_MIN_WEEKS,
            }
            continue

        variant_ids = sorted(group_trades["variant_id"].unique())
        per_combo_dfs = [
            group_trades[group_trades["variant_id"] == vid][["week", "pnl"]].rename(
                columns={"week": "day"}
            )
            for vid in variant_ids
        ]
        panel = daily_pnl_panel(per_combo_dfs)
        pbo_result = pbo_cscv(panel, n_splits=GROUP_DIAGNOSTIC_N_SPLITS)

        sharpes = panel.mean(axis=0) / panel.std(axis=0, ddof=1).replace(0, np.nan)
        if sharpes.notna().any():
            # Series.idxmax() raises ValueError on an all-NaN Series (ddof=1 std is
            # undefined for every zero-variance/all-identical-weekly-PnL column) — guarded
            # above, not just skipna'd, since skipna doesn't help when *every* value is NaN.
            best_idx = int(sharpes.idxmax())
            dsr_result = deflated_sharpe_ratio(
                sharpe_hat=float(sharpes.iloc[best_idx]),
                n_trials=len(variant_ids),
                trial_sharpe_var=float(sharpes.var(ddof=1)) if sharpes.notna().sum() > 1 else 0.0,
                n_obs=panel.shape[0],
            )
            out[(market, strategy)] = {
                "status": "ok",
                "n_weeks": int(n_weeks),
                "best_variant": variant_ids[best_idx],
                "pbo": pbo_result["pbo"],
                "dsr_pvalue": dsr_result["dsr_pvalue"],
                "dsr_zscore": dsr_result["dsr_zscore"],
            }
        else:
            # Every variant has zero-variance (or all-zero) weekly PnL in this window —
            # Sharpe/DSR are undefined, but PBO (computed directly from raw panel values,
            # not from `sharpes`) still is, so still report it.
            out[(market, strategy)] = {
                "status": "ok",
                "n_weeks": int(n_weeks),
                "best_variant": None,
                "pbo": pbo_result["pbo"],
                "dsr_pvalue": None,
                "dsr_zscore": None,
            }
    return out


# ---------------------------------------------------------------------------
# Markets monitored — coverage/health, not a performance verdict.
# ---------------------------------------------------------------------------


def markets_monitored_table(df: pd.DataFrame, as_of_ts: float, window_hours: int = 24) -> pd.DataFrame:
    since = as_of_ts - window_hours * 3600
    recent = df[df["entry_ts"] >= since]
    if recent.empty:
        return pd.DataFrame(columns=["market", "market_kind", "trades", "win_rate", "pnl", "hours_since_last_trade"])

    rows = []
    for market, g in recent.groupby("market"):
        last_ts = g["entry_ts"].max()
        rows.append(
            {
                "market": market,
                "market_kind": g["market_kind"].iloc[0],
                "trades": len(g),
                "win_rate": float((g["pnl"] > 0).mean()),
                "pnl": float(g["pnl"].sum()),
                "hours_since_last_trade": round((as_of_ts - last_ts) / 3600, 1),
            }
        )
    out = pd.DataFrame(rows).sort_values(["market_kind", "market"])
    return out


# ---------------------------------------------------------------------------
# Rendering — bottom-up: bottom line, recommendations, markets monitored, detail.
# ---------------------------------------------------------------------------


def render_digest_md(
    date: str,
    combo_df: pd.DataFrame,
    streaks: dict[tuple, int],
    group_diag: dict[tuple, dict],
    markets_df: pd.DataFrame,
    history_days: int,
) -> str:
    promote = combo_df[combo_df["verdict"] == "PROMOTE-CANDIDATE"].copy()
    reject_today = combo_df[combo_df["verdict"] == "REJECT"]
    watch = combo_df[combo_df["verdict"] == "WATCH"]

    lines: list[str] = []
    lines.append(f"# siglab daily signal digest — {date}\n")
    lines.append(
        f"Auto-generated by `analysis/siglab_daily_digest.py`. Covers {history_days} day(s) "
        f"of cumulative history through {date} HKT. See "
        f"`siglab/doc/plan_better_signal_2026-07-24.md` for the full methodology and its "
        f"\"Revision after DeepSeek review\" section for why verdicts use an exact binomial "
        f"test (not per-combo PBO/DSR) as the primary signal at this data volume.\n"
    )
    lines.append(
        "**Caveat that applies to every number below**: siglab is a paper-trading harness — "
        "PnL is idealized (no bid-ask spread, taker fee, or slippage modeled). Treat every "
        "statistic here as relative signal quality between variants, not a claim about a "
        "tradable net edge.\n"
    )

    # ── 1. Bottom line ──
    lines.append("## Bottom line\n")
    if promote.empty:
        lines.append(
            f"No combo currently clears the significance bar "
            f"(≥{MIN_TRADES_FOR_VERDICT} trades, BH-corrected q<{ALPHA}, positive edge) after "
            f"{history_days} day(s) of history. "
            f"{len(watch)} combo(s) are in WATCH (positive-looking but not yet significant or "
            f"under the trade-count bar), {len(reject_today)} in REJECT.\n"
        )
    else:
        top = promote.sort_values("q_value").iloc[0]
        streak = streaks.get((top["market"], top["strategy"], top["variant_id"]), 1)
        lines.append(
            f"**{len(promote)} combo(s) at PROMOTE-CANDIDATE** after {history_days} day(s) of "
            f"history. Top: `{top['variant_id']}` on `{top['market']}` ({top['strategy']}) — "
            f"edge {top['edge']:+.3f}, q={top['q_value']:.4f}, {int(top['eligible_trades'])} "
            f"tested trades, {streak} consecutive day(s) holding this verdict. "
            f"{len(watch)} more in WATCH, {len(reject_today)} in REJECT.\n"
        )

    # ── 2. Recommendations ──
    lines.append("## Recommendations\n")
    rec_full = pd.concat([promote, watch]).sort_values(
        ["verdict", "q_value"], ascending=[True, True], na_position="last"
    )
    TOP_N_RECOMMENDATIONS = 20
    rec = rec_full.head(TOP_N_RECOMMENDATIONS)
    if rec.empty:
        lines.append("_Nothing in PROMOTE-CANDIDATE or WATCH yet._\n")
    else:
        if len(rec_full) > TOP_N_RECOMMENDATIONS:
            lines.append(
                f"Top {TOP_N_RECOMMENDATIONS} by significance, of {len(rec_full)} total in "
                f"PROMOTE-CANDIDATE/WATCH — the rest are in the full scored table under "
                f"\"Supporting detail\" below.\n\n"
            )
        lines.append(
            "| verdict | market | strategy | variant | streak (days) | tested trades | edge | q-value |\n"
            "|---|---|---|---|---|---|---|---|\n"
        )
        for _, r in rec.iterrows():
            streak = streaks.get((r["market"], r["strategy"], r["variant_id"]), 0)
            q = "-" if pd.isna(r["q_value"]) else f"{r['q_value']:.4f}"
            edge = "-" if pd.isna(r["edge"]) else f"{r['edge']:+.3f}"
            lines.append(
                f"| {r['verdict']} | {r['market']} | {r['strategy']} | {r['variant_id']} | "
                f"{streak} | {int(r['eligible_trades'])} | {edge} | {q} |\n"
            )
    lines.append("")

    # ── 3. Markets monitored ──
    lines.append("## Markets monitored (trailing 24h)\n")
    lines.append(
        "_Coverage/health, not a verdict — a market with zero recent trades but a fresh "
        "`hours_since_last_trade` is quiet-but-healthy; a stale one may be a dead feed. "
        "`hours_since_last_trade` measures trade recency, not literal tick staleness "
        "(tick-level data isn't persisted anywhere this script can read)._\n"
    )
    if markets_df.empty:
        lines.append("_No trades in the last 24h._\n")
    else:
        lines.append("| market | kind | trades | win rate | pnl | hours since last trade |\n|---|---|---|---|---|---|\n")
        for _, r in markets_df.iterrows():
            lines.append(
                f"| {r['market']} | {r['market_kind']} | {r['trades']} | "
                f"{r['win_rate']:.2%} | {r['pnl']:+.3f} | {r['hours_since_last_trade']:.1f} |\n"
            )
    lines.append("")

    # ── 4. Supporting detail ──
    lines.append("## Supporting detail\n")
    lines.append("<details>\n<summary>Group-level PBO / Deflated Sharpe (informational only)</summary>\n\n")
    lines.append(
        f"Weekly PnL blocks, {GROUP_DIAGNOSTIC_N_SPLITS}-split CSCV, gated behind "
        f"{GROUP_DIAGNOSTIC_MIN_WEEKS} weeks of history — **never used to gate a per-variant "
        f"verdict above**, see the module docstring for why.\n\n"
    )
    ready_groups = {k: v for k, v in group_diag.items() if v["status"] == "ok"}
    if not ready_groups:
        max_weeks = max((v["n_weeks"] for v in group_diag.values()), default=0)
        lines.append(
            f"_No `(market, strategy)` group has {GROUP_DIAGNOSTIC_MIN_WEEKS} weeks of history "
            f"yet across all {len(group_diag)} groups tracked — furthest along has "
            f"{max_weeks} week(s). Not shown as a table until at least one group clears "
            f"warm-up (avoids a {len(group_diag)}-row table that would say \"insufficient "
            f"history\" on every line)._\n\n"
        )
    else:
        lines.append("| market | strategy | weeks | best variant | PBO | DSR p-value |\n|---|---|---|---|---|---|\n")
        for (market, strategy), diag in sorted(ready_groups.items()):
            best_variant = diag["best_variant"] or "-"
            dsr_p = "-" if diag["dsr_pvalue"] is None else f"{diag['dsr_pvalue']:.4f}"
            lines.append(
                f"| {market} | {strategy} | {diag['n_weeks']} | {best_variant} | "
                f"{diag['pbo']:.3f} | {dsr_p} |\n"
            )
        lines.append(
            f"\n_{len(group_diag) - len(ready_groups)} more group(s) still below the "
            f"{GROUP_DIAGNOSTIC_MIN_WEEKS}-week warm-up, not shown._\n"
        )
    lines.append("\n</details>\n\n")

    lines.append(
        "<details>\n<summary>Combos with a computed verdict (excludes INSUFFICIENT-SAMPLE)</summary>\n\n"
    )
    scored = combo_df[combo_df["verdict"] != "INSUFFICIENT-SAMPLE"].sort_values(
        ["market", "strategy", "variant_id"]
    )
    n_insufficient = len(combo_df) - len(scored)
    lines.append(
        f"{len(scored)} of {len(combo_df)} combos tracked have reached "
        f"{MIN_TRADES_FOR_VERDICT}+ tested trades and a computed verdict; the other "
        f"{n_insufficient} are INSUFFICIENT-SAMPLE and omitted here to keep this readable — "
        f"the full set (every combo, every day) is in `candidate_ledger.csv` alongside this "
        f"digest.\n\n"
    )
    if scored.empty:
        lines.append("_Nothing has reached a computed verdict yet._\n")
    else:
        lines.append(
            "| market | strategy | variant | total trades | tested (non-TIMEOUT) | win rate | "
            "null | edge | p-value | q-value | verdict |\n|---|---|---|---|---|---|---|---|---|---|---|\n"
        )
        for _, r in scored.iterrows():
            wr = "-" if pd.isna(r["realized_win_rate"]) else f"{r['realized_win_rate']:.2%}"
            nwr = "-" if pd.isna(r["null_win_rate"]) else f"{r['null_win_rate']:.2%}"
            edge = "-" if pd.isna(r["edge"]) else f"{r['edge']:+.3f}"
            p = "-" if pd.isna(r["p_value"]) else f"{r['p_value']:.4f}"
            q = "-" if pd.isna(r["q_value"]) else f"{r['q_value']:.4f}"
            lines.append(
                f"| {r['market']} | {r['strategy']} | {r['variant_id']} | {r['total_trades']} | "
                f"{r['eligible_trades']} | {wr} | {nwr} | {edge} | {p} | {q} | {r['verdict']} |\n"
            )
    lines.append("\n</details>\n")

    return "".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True, help="Path to siglab_trades.jsonl")
    parser.add_argument("--config", type=Path, required=True, help="Path to siglab's config/markets.toml")
    parser.add_argument("--report-dir", type=Path, required=True, help="siglab/doc/report")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="HKT date (YYYY-MM-DD) to generate the digest for; defaults to today (HKT)",
    )
    args = parser.parse_args()

    target_date = args.date or datetime.now(tz=HKT).strftime("%Y-%m-%d")
    as_of_ts = (
        datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=HKT) + timedelta(days=1)
    ).timestamp()

    print(f"[digest] reading {args.log}", file=sys.stderr)
    trades = read_trades(args.log)
    df = build_dataframe(trades)
    df = df[df["entry_ts"] <= as_of_ts]
    print(f"[digest] {len(df)} trades through {target_date} HKT (of {len(trades)} total in log)", file=sys.stderr)

    history_days = df["day"].nunique() if not df.empty else 0
    reversal_params = load_reversal_barrier_params(args.config)

    combo_df = compute_combo_stats(df, reversal_params)
    combo_df = apply_bh_correction(combo_df)
    combo_df["verdict"] = combo_df.apply(assign_verdict, axis=1)

    ledger_path = args.report_dir / "candidate_ledger.csv"
    ledger_df = merge_ledger_rows(ledger_path, target_date, combo_df)
    streaks = compute_streaks(ledger_df, target_date)

    group_diag = compute_group_diagnostics(df)
    markets_df = markets_monitored_table(df, as_of_ts)

    digest_md = render_digest_md(target_date, combo_df, streaks, group_diag, markets_df, history_days)

    day_dir = args.report_dir / target_date
    day_dir.mkdir(parents=True, exist_ok=True)
    digest_path = day_dir / f"digest_{target_date}.md"
    # Digest written before the ledger, deliberately — see merge_ledger_rows's docstring:
    # this makes the rarer of the two possible push-race outcomes the harmless one.
    digest_path.write_text(digest_md, encoding="utf-8")
    write_ledger(ledger_path, ledger_df)

    print(f"[digest] wrote {digest_path}", file=sys.stderr)
    print(f"[digest] wrote {ledger_path} ({len(ledger_df)} total rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
