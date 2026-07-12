"""
data_quality.py — independent tick-coverage observer for price_feed/raw/.

Checks how much of each fully-elapsed hour's expected tick density actually landed in the
sealed hourly parquet files, per asset/kind, and flags hours below a coverage threshold.

Built in direct response to the 2026-07-10 22:30 collector crash-loop incident
(price_feed/doc/incident_collector_data_loss_2026-07-12.md): ~85% of ticks silently went
missing for 2+ days before anyone noticed, found only by chance during an unrelated
investigation. This makes that class of gap visible automatically in the daily recon report
(trader/scripts/trade_reconcile.py imports and calls this module) instead of requiring someone
to manually SSH into Oracle and read journalctl.

Only reads *sealed* `{asset}_{kind}_{date}_{HH}.parquet` files — the still-open current hour's
`.tmp` is excluded by construction (it doesn't match that filename pattern), so there's no need
to special-case "the current hour looks incomplete because it hasn't finished yet."

Usage (standalone):
    python data_quality.py --raw-dir ../raw --hours-back 24
"""
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

HKT = timezone(timedelta(hours=8))

MINUTES_PER_HOUR = 60
# Below this fraction of an hour's 60 minutes actually having >=1 tick, flag the hour. Real
# collector operation (pre-incident, healthy days) runs close to 93%+; this is deliberately
# generous so normal quiet-market variance doesn't spam false positives — see
# incident_collector_data_loss_2026-07-12.md's own numbers (~93% healthy vs ~14-15% during the
# crash-loop) for calibration headroom. A real incident blows straight through this either way.
DEFAULT_MIN_COVERAGE_PCT = 50.0


def hourly_minute_coverage(path: Path) -> Optional[float]:
    """% of the 60 minutes in this sealed hourly file that have >=1 row, or None if the file
    doesn't exist / can't be read (missing/corrupt data is reported as a gap by the caller,
    never silently skipped — that silence is exactly what let the 2026-07-10 incident run
    undetected for 2+ days)."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["ts"])
    except Exception:
        return None
    if df.empty:
        return 0.0
    minutes = set((df["ts"] // 60).astype(int))
    return len(minutes) / MINUTES_PER_HOUR * 100


def iter_elapsed_hours(window_start: datetime, window_end: datetime, now: datetime):
    """Yields (date_str, hour) for each hour boundary in [window_start, window_end) that has
    *fully elapsed* by `now` — i.e. its sealed file should already exist if the collector was
    healthy. Excludes the still-open current hour and anything in the future, so a report run
    mid-window never flags "today's last hour" just because it hasn't finished yet."""
    cur = window_start.replace(minute=0, second=0, microsecond=0)
    if cur < window_start:
        cur = cur + timedelta(hours=1)
    while cur < window_end:
        hour_end = cur + timedelta(hours=1)
        if hour_end <= now:
            yield cur.strftime("%Y-%m-%d"), cur.hour
        cur = hour_end


def discover_recorded_asset_kinds(raw_dir: Path) -> set:
    """(asset, kind) pairs that have at least one file anywhere in raw_dir with actual rows in
    it — used to skip checking a combination that's structurally never recorded (e.g. HYPE has
    no Binance market: the collector's unconditional per-tick hourly-seal check still creates
    `HYPE_binance_*.parquet` files, but they're always 0 rows — see README's "Assets recorded"
    note). A plain "does a file exist" check would flag every HYPE/binance hour as a gap
    forever, drowning out real gaps. Uses parquet footer metadata only (`num_rows`, no full
    data read) to stay fast across a whole raw_dir; checks up to 5 of each pair's newest files
    (filename-sorted, which sorts by date/hour) before giving up on that pair."""
    import pyarrow.parquet as pq

    candidates: dict = {}
    for p in raw_dir.glob("*_*_*.parquet"):
        parts = p.stem.split("_")
        if len(parts) < 3:
            continue
        asset, kind = parts[0], parts[1]
        if kind not in ("poly", "binance", "book"):
            continue
        candidates.setdefault((asset, kind), []).append(p)

    pairs = set()
    for (asset, kind), paths in candidates.items():
        for p in sorted(paths, reverse=True)[:5]:
            try:
                if pq.ParquetFile(p).metadata.num_rows > 0:
                    pairs.add((asset, kind))
                    break
            except Exception:
                continue
    return pairs


def check_data_quality(
    raw_dir: Path,
    window_start: datetime,
    window_end: datetime,
    now: Optional[datetime] = None,
    kinds: tuple = ("poly", "binance"),
    min_coverage_pct: float = DEFAULT_MIN_COVERAGE_PCT,
) -> dict:
    """Checks every fully-elapsed hour in [window_start, window_end) for every (asset, kind)
    pair that's ever been recorded in raw_dir. Returns:
      {"hours_checked": N, "flagged": [{"asset","kind","date","hour","coverage_pct","status"}, ...]}
    `status` is "GAP" (file exists, coverage below threshold) or "MISSING" (no sealed file at
    all for that hour). Defensive by design (matches the rest of the daily recon pipeline): a
    missing/unreadable raw_dir just means nothing to check, never raises."""
    now = now or datetime.now(tz=window_start.tzinfo)
    hours = list(iter_elapsed_hours(window_start, window_end, now))
    if not raw_dir.exists():
        return {"hours_checked": 0, "flagged": []}

    pairs = sorted(p for p in discover_recorded_asset_kinds(raw_dir) if p[1] in kinds)
    flagged = []
    for asset, kind in pairs:
        for date, hh in hours:
            path = raw_dir / f"{asset}_{kind}_{date}_{hh:02d}.parquet"
            coverage = hourly_minute_coverage(path)
            if coverage is None:
                flagged.append({
                    "asset": asset, "kind": kind, "date": date, "hour": hh,
                    "coverage_pct": None, "status": "MISSING",
                })
            elif coverage < min_coverage_pct:
                flagged.append({
                    "asset": asset, "kind": kind, "date": date, "hour": hh,
                    "coverage_pct": coverage, "status": "GAP",
                })

    flagged.sort(key=lambda f: (f["date"], f["hour"], f["asset"], f["kind"]))
    return {"hours_checked": len(pairs) * len(hours), "flagged": flagged}


def safe_check_data_quality(
    raw_dir: Path, window_start: datetime, window_end: datetime,
    now: Optional[datetime] = None,
) -> dict:
    """Same defensive-by-design rule as the rest of the daily recon pipeline: a data-quality
    check failure must degrade the report section, never take down the rest of an
    already-working report."""
    try:
        return check_data_quality(raw_dir, window_start, window_end, now)
    except Exception as e:
        print(f"WARNING: data quality check failed: {e}")
        return {"hours_checked": 0, "flagged": [], "error": str(e)}


def _main() -> None:
    parser = argparse.ArgumentParser(description="price_feed raw/ tick-coverage observer")
    parser.add_argument("--raw-dir", type=str, default="../raw")
    parser.add_argument("--hours-back", type=int, default=24)
    parser.add_argument("--min-coverage-pct", type=float, default=DEFAULT_MIN_COVERAGE_PCT)
    args = parser.parse_args()

    now = datetime.now(tz=HKT)
    window_start = now - timedelta(hours=args.hours_back)
    result = check_data_quality(
        Path(args.raw_dir), window_start, now, now=now,
        min_coverage_pct=args.min_coverage_pct,
    )
    n_flagged = len(result["flagged"])
    print(f"{result['hours_checked']} asset-hours checked, {n_flagged} flagged")
    for f in result["flagged"]:
        cov = f"{f['coverage_pct']:.1f}%" if f["coverage_pct"] is not None else "—"
        print(f"  {f['date']} {f['hour']:02d}:00 {f['asset']:<5} {f['kind']:<7} {f['status']:<7} {cov}")


if __name__ == "__main__":
    _main()
