"""
build_backtest_prices.py — assemble single-file-per-asset-per-date parquet
for trader/src/backtest.rs::load_price_data from poly_rust's raw sources.

Why this exists: trader/src/backtest.rs's date-specific path expects one
{asset}_binance_{date}.parquet + {asset}_poly_{date}.parquet pair covering a
full HKT calendar day. Neither this project's own price_feed/raw/ (hourly-
sharded: {asset}_poly_{date}_HH.parquet, plus a same-day daily file that's
often still unsealed/footerless — see README.md "Parquet file integrity")
nor btc_5mins/prices/{asset}_poly.parquet (merged, but stale — stopped
updating around 2026-07-01 when the old Python poly collector was retired
in favor of this project's Rust price_feed) satisfy that directly.

This script:
  - poly: concatenates all sealed hourly shards + the daily file for the
    given date from price_feed/raw/. If the daily file itself is unsealed
    (no footer — happens right after a collector restart, since it writes
    continuously to the plain dated file until the next hourly boundary),
    recovers it via recover_live_tmp.py's raw-page decoder instead of
    failing (recover_live_tmp.py itself assumes only *.tmp files can be
    unsealed; the plain daily file can be too, e.g. after a mid-hour
    collector restart — this is the case encountered building today's
    2026-07-03 report, see trader/doc/plan_daily_recon.md).
  - binance: btc_5mins/prices/{asset}_binance.parquet is already fresh and
    merged (unlike poly), so this just date-filters it via each row's
    slug-embedded window timestamp.

Usage:
    python build_backtest_prices.py --asset ETH,BTC,DOGE --date 2026-07-03
    python build_backtest_prices.py --asset ETH --date 2026-07-03 --out-dir /tmp/bt_prices
"""
import argparse
import glob
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
RAW_DIR = SCRIPT_DIR.parent.parent / "price_feed" / "raw"
BINANCE_SRC_DIR = Path("/home/kev/apps/btc_5mins/prices")
DEFAULT_OUT_DIR = SCRIPT_DIR.parent / "backtest_prices"

sys.path.insert(0, str(SCRIPT_DIR.parent.parent / "price_feed" / "scripts"))
from recover_live_tmp import recover_poly_rust_tmp  # noqa: E402


def _read_or_recover(path: Path) -> pd.DataFrame:
    """Read a parquet file normally; if it's unsealed (no footer), recover it."""
    try:
        return pq.read_table(str(path)).to_pandas()
    except Exception as e:
        print(f"  {path.name}: normal read failed ({e}); recovering via raw-page decode...")
        return recover_poly_rust_tmp(str(path))


def build_poly(asset: str, date: str, out_dir: Path) -> None:
    frames = []

    daily = RAW_DIR / f"{asset}_poly_{date}.parquet"
    if daily.exists():
        frames.append(_read_or_recover(daily))

    sealed = sorted(RAW_DIR.glob(f"{asset}_poly_{date}_*.parquet"))
    for f in sealed:
        frames.append(_read_or_recover(f))

    tmp_files = sorted(RAW_DIR.glob(f"{asset}_poly_{date}_*.parquet.tmp"))
    for f in tmp_files:
        frames.append(_read_or_recover(f))

    if not frames:
        print(f"  {asset}: no poly files found for {date} in {RAW_DIR}")
        return

    df = pd.concat(frames, ignore_index=True, sort=False)
    before = len(df)
    df = df[df["up"] != 0.5].copy()  # stuck-price rows (pre-2026-06-14 23:04:57 HKT bug)
    df = df[["ts", "up", "dn", "slug"]].drop_duplicates(subset=["ts", "slug"]).sort_values("ts")
    after = len(df)

    out_path = out_dir / f"{asset}_poly_{date}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  {asset} poly: {before} -> {after} rows -> {out_path}")


def build_binance(asset: str, date: str, out_dir: Path) -> None:
    src = BINANCE_SRC_DIR / f"{asset}_binance.parquet"
    if not src.exists():
        print(f"  {asset}: no binance source at {src}")
        return

    df = pq.read_table(str(src), columns=["ts", "binance", "slug"]).to_pandas()
    slug_ts = df["slug"].str.extract(r"-(\d+)$")[0].astype(float)

    y, m, d = map(int, date.split("-"))
    import datetime as dt
    hkt = dt.timezone(dt.timedelta(hours=8))
    day_start = dt.datetime(y, m, d, 0, 0, 0, tzinfo=hkt).timestamp()
    day_end = day_start + 86400

    mask = slug_ts.between(day_start, day_end, inclusive="left")
    out = df[mask].sort_values("ts")

    out_path = out_dir / f"{asset}_binance_{date}.parquet"
    out.to_parquet(out_path, index=False)
    print(f"  {asset} binance: {len(out)} rows (of {len(df)} total) -> {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--asset", required=True, help="comma-separated, e.g. BTC,ETH,DOGE")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (HKT calendar day)")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for asset in [a.strip().upper() for a in args.asset.split(",") if a.strip()]:
        print(f"[{asset} / {args.date}]")
        build_poly(asset, args.date, out_dir)
        build_binance(asset, args.date, out_dir)


if __name__ == "__main__":
    main()
