#!/usr/bin/env python3
"""Parity harness for the Rust `indicator` crate vs ../btc_5mins bot/signals.py.

Three steps (see trader/doc/feature_vol_2026-07-18.md §4):

  1. --make-ticks : load a day of price_feed's BTC binance parquet, build a
     strict gap-free 1-Hz series (forward-filled, aligned to a 5m boundary)
     and write `ticks.csv` (ts,price). This one file feeds BOTH sides.
  2. --ref       : drive bot/signals.py's VolHarSignal/PUpSignal/SnrSignal
     over ticks.csv under the shared grid contract (cycle [slot, slot+300)
     samples seconds slot+1..slot+299; boundary-second rows reset the signals
     and emit post-reset values without appending a sample) and write
     `ref.csv` (slot,ts,vol_har,p_up,snr).
  3. --compare   : per-column max |Δ| between ref.csv and the Rust replay's
     output (same shape). Tolerance gate 1e-6 absolute; expectation ~1e-12
     (identical arithmetic) except p_up ~1e-9 (scipy stdtr vs puruspe betai).

Run with ../btc_5mins's venv (needs pandas/scipy + bot.signals):

  cd /home/kev/apps/btc_5mins && ./venv/bin/python \
      /home/kev/apps/poly_rust/scripts/indicator_parity_ref.py --all \
      --date 2026-07-17 --asset BTC --workdir <dir> --rust-csv <replay-output>
"""

from __future__ import annotations

import argparse
import glob
import math
import sys
from pathlib import Path

BTC5_ROOT = Path("/home/kev/apps/btc_5mins")
sys.path.insert(0, str(BTC5_ROOT))

PERIOD = 300

# Production BTC values (config/strategy_20260612.toml [har_beta]/[har_nu] —
# same numbers as poly_rust/indicator/config/indicator.toml).
BETA = {
    "BTC": [6.75316682223936e-05, 0.3808532101541894, 0.23010976882783898, 0.32151716443117506],
}
NU = {"BTC": 4.2469}


def make_ticks(date: str, asset: str, out_path: Path) -> None:
    import pandas as pd

    pattern = f"/home/kev/apps/poly_rust/price_feed/raw/{asset}_binance_{date}_*.parquet"
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"no parquet files match {pattern}")
    df = pd.concat([pd.read_parquet(f, columns=["ts", "binance"]) for f in files])
    df = df[df["binance"] > 0].sort_values("ts")
    print(f"[ticks] {len(files)} files, {len(df)} raw samples")

    # Strict 1-Hz grid: for each whole second, the last price at/before it
    # (forward-fill). Start at the first 5m boundary with data before it, end
    # at the last full second.
    first_ts, last_ts = float(df["ts"].iloc[0]), float(df["ts"].iloc[-1])
    start = (int(first_ts) // PERIOD + 1) * PERIOD
    end = int(last_ts)
    secs = pd.RangeIndex(start, end + 1)
    s = pd.Series(df["binance"].values, index=df["ts"].values)
    # searchsorted-based asof: index of last raw sample <= each second.
    import numpy as np

    idx = np.searchsorted(s.index.values, secs.values + 1e-9, side="right") - 1
    prices = s.values[idx]
    out = pd.DataFrame({"ts": secs.astype(float), "price": prices})
    out.to_csv(out_path, index=False, float_format="%.10f")
    n_cycles = (end - start) // PERIOD
    print(f"[ticks] {len(out)} 1-Hz rows, {n_cycles} full cycles → {out_path}")


def run_ref(ticks_path: Path, out_path: Path, asset: str) -> None:
    from bot.signals import CycleContext, PUpSignal, SnrSignal, VolHarSignal

    vol = VolHarSignal(beta=BETA[asset], nu=NU[asset])
    pup = PUpSignal(vol_har=vol)
    snr = SnrSignal(vol_har=vol)

    fmt = lambda v: "" if v is None else f"{v:.17e}"
    n = 0
    with open(ticks_path) as f_in, open(out_path, "w") as f_out:
        f_out.write("slot,ts,vol_har,p_up,snr\n")
        header = f_in.readline()
        assert header.startswith("ts"), header
        last_price = 0.0
        slot = 0
        for line in f_in:
            ts_s, price_s = line.strip().split(",")
            ts, price = float(ts_s), float(price_s)
            sec = int(ts)
            row_slot = (sec // PERIOD) * PERIOD
            if slot == 0:
                slot = row_slot  # join mid-stream; cycle_open stays unknown (0.0)
            elif row_slot > slot:
                # Boundary: seal the cycle. cycle_open = last known price
                # before this row — identical to the Rust engine's roll_cycle.
                ctx = CycleContext(
                    cycle_start_ts=float(row_slot),
                    cycle_end_ts=float(row_slot + PERIOD),
                    cycle_open_binance=last_price,
                )
                vol.reset(ctx)
                pup.reset(ctx)
                snr.reset(ctx)
                slot = row_slot
            last_price = price
            if sec % PERIOD != 0:
                # Grid contract: boundary seconds carry no 1-Hz sample.
                from bot.signals import BinanceTick

                t = BinanceTick(ts=ts, price=price)
                vol.on_tick(t)
                pup.on_tick(t)
                snr.on_tick(t)
            p = pup.value(now=ts)
            z = snr.value(now=ts)
            v = vol.value()
            f_out.write(f"{slot},{ts:.3f},{fmt(v)},{fmt(p)},{fmt(z)}\n")
            n += 1
    print(f"[ref] {n} rows → {out_path}")


def compare(ref_path: Path, rust_path: Path) -> int:
    import pandas as pd

    ref = pd.read_csv(ref_path)
    rust = pd.read_csv(rust_path)
    print(f"[compare] ref {len(ref)} rows vs rust {len(rust)} rows")
    if len(ref) != len(rust):
        print("[compare] FAIL: row count mismatch")
        return 1
    if not (ref["slot"].values == rust["slot"].values).all():
        print("[compare] FAIL: slot mismatch")
        return 1

    tol = 1e-6
    fail = False
    for col in ["vol_har", "p_up", "snr"]:
        a, b = ref[col], rust[col]
        both_nan = a.isna() & b.isna()
        one_nan = a.isna() ^ b.isna()
        if one_nan.any():
            i = int(one_nan.idxmax())
            print(f"[compare] FAIL {col}: None/value mismatch at row {i} "
                  f"(ref={a[i]!r} rust={b[i]!r}), {int(one_nan.sum())} rows total")
            fail = True
            continue
        diff = (a - b).abs()
        max_diff = float(diff[~both_nan].max()) if (~both_nan).any() else 0.0
        n_vals = int((~both_nan).sum())
        status = "OK " if max_diff < tol else "FAIL"
        print(f"[compare] {status} {col:8s} max|Δ|={max_diff:.3e} over {n_vals} ready rows "
              f"({int(both_nan.sum())} warmup rows)")
        if max_diff >= tol:
            fail = True
    return 1 if fail else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", default="BTC")
    ap.add_argument("--date", default="2026-07-17")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--rust-csv", default=None, help="Rust replay output (for --compare/--all)")
    ap.add_argument("--make-ticks", action="store_true")
    ap.add_argument("--ref", action="store_true")
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    wd = Path(args.workdir)
    wd.mkdir(parents=True, exist_ok=True)
    ticks = wd / f"ticks_{args.asset}_{args.date}.csv"
    ref = wd / f"ref_{args.asset}_{args.date}.csv"

    if args.make_ticks or args.all:
        make_ticks(args.date, args.asset, ticks)
    if args.ref or args.all:
        run_ref(ticks, ref, args.asset)
    if args.compare or args.all:
        if not args.rust_csv:
            raise SystemExit("--rust-csv required for --compare")
        sys.exit(compare(ref, Path(args.rust_csv)))


if __name__ == "__main__":
    main()
