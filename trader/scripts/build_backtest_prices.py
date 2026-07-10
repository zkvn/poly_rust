"""
build_backtest_prices.py — assemble single-file-per-asset-per-date parquet
for trader/src/backtest.rs::load_price_data from poly_rust's raw sources.

Why this exists: trader/src/backtest.rs's date-specific path expects one
{asset}_binance_{date}.parquet + {asset}_poly_{date}.parquet pair covering a
full HKT calendar day. This project's own price_feed/raw/ (hourly-sharded:
{asset}_{type}_{date}_HH.parquet, plus a same-day daily file that's often
still unsealed/footerless — see README.md "Parquet file integrity") doesn't
satisfy that directly.

Both poly and binance: concatenate all sealed hourly shards + the daily file
for the given date from price_feed/raw/. If a file is unsealed (no footer —
happens right after a collector restart, since it writes continuously to
the plain dated file until the next hourly boundary), recover it via
recover_rust_parquet.py's raw-page decoder instead of failing
(recover_rust_parquet.py itself assumes only *.tmp files can be unsealed;
the plain daily file can be too, e.g. after a mid-hour collector restart —
this is the case encountered building the 2026-07-03 report, see
trader/doc/plan_daily_recon.md).

binance used to date-filter btc_5mins/prices/{asset}_binance.parquet (a
merged file from the old Python collector) instead of reading price_feed/raw/
directly — that file silently stopped updating on 2026-07-05 when the old
collector was fully retired (this project's own price_feed has recorded
binance ticks into price_feed/raw/ all along, just never got wired in here).
Every backtest date after 2026-07-05 was therefore replaying on an empty
binance series -- no price data means no signal, so the engine could never
fire a single trade regardless of config, which looked like "the backtest
can't reproduce live" but was actually "the backtest has no binance data at
all" (found 2026-07-10, trader/doc/feature_bt_recon_2026-07-10.md). Switched
to the same price_feed/raw/ sourcing as poly.

Usage:
    python build_backtest_prices.py --asset ETH,BTC,DOGE --date 2026-07-03
    python build_backtest_prices.py --asset ETH --date 2026-07-03 --out-dir /tmp/bt_prices
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
RAW_DIR = SCRIPT_DIR.parent.parent / "price_feed" / "raw"
DEFAULT_OUT_DIR = SCRIPT_DIR.parent / "backtest_prices"

sys.path.insert(0, str(SCRIPT_DIR.parent.parent / "price_feed" / "scripts"))
# recover_live_tmp.py was renamed to recover_rust_parquet.py (poly/binance/book
# recovery split into separate functions) after this script was first written —
# found broken (ModuleNotFoundError) while wiring up the BT reconciliation
# feature's price-data build step; fixed here since nothing about this script's
# own recovery logic changed, only the import target.
from recover_rust_parquet import (  # noqa: E402
    recover_rust_binance_parquet,
    recover_rust_poly_parquet,
)


def _read_or_recover(path: Path, recover_fn) -> pd.DataFrame:
    """Read a parquet file normally; if it's unsealed (no footer), recover it
    with the type-specific decoder (poly and binance have different schemas)."""
    try:
        return pq.read_table(str(path)).to_pandas()
    except Exception as e:
        print(f"  {path.name}: normal read failed ({e}); recovering via raw-page decode...")
        return recover_fn(str(path))


def _gather_shards(asset: str, kind: str, date: str, recover_fn) -> list:
    """Collect the daily file + sealed hourly shards + any still-open .tmp
    for {asset}_{kind}_{date}* from price_feed/raw/ — shared by build_poly
    and build_binance, which differ only in the recovery decoder and the
    columns kept afterward."""
    frames = []

    daily = RAW_DIR / f"{asset}_{kind}_{date}.parquet"
    if daily.exists():
        frames.append(_read_or_recover(daily, recover_fn))

    sealed = sorted(RAW_DIR.glob(f"{asset}_{kind}_{date}_*.parquet"))
    for f in sealed:
        frames.append(_read_or_recover(f, recover_fn))

    tmp_files = sorted(RAW_DIR.glob(f"{asset}_{kind}_{date}_*.parquet.tmp"))
    for f in tmp_files:
        frames.append(_read_or_recover(f, recover_fn))

    return frames


def build_poly(asset: str, date: str, out_dir: Path) -> None:
    frames = _gather_shards(asset, "poly", date, recover_rust_poly_parquet)
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
    frames = _gather_shards(asset, "binance", date, recover_rust_binance_parquet)
    if not frames:
        print(f"  {asset}: no binance files found for {date} in {RAW_DIR}")
        return

    df = pd.concat(frames, ignore_index=True, sort=False)
    before = len(df)
    df = df[["ts", "binance", "slug"]].drop_duplicates(subset=["ts", "slug"]).sort_values("ts")
    after = len(df)

    out_path = out_dir / f"{asset}_binance_{date}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  {asset} binance: {before} -> {after} rows -> {out_path}")


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
