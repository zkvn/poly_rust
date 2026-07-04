#!/usr/bin/env python3
"""One-off latency study over price_feed's recorded parquet — computes
server_ts -> local receive latency (latency_ms column) distributions per
asset/feed, plus WS message-arrival gap stats used as a proxy for whether the
200ms/250ms sampler is coalescing/dropping intermediate ticks.

Usage: python3 price_feed/analysis/latency_study.py [date, e.g. 2026-07-03]
Reads from price_feed/raw/ (5-min duration — what the live trader actually
consumes via the NATS bba bridge).
"""
import sys
import glob
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-07-03"
RAW_DIR = "raw"

QUANTILES = [0.50, 0.90, 0.95, 0.99]


def load_kind(kind: str, date: str) -> pd.DataFrame:
    paths = sorted(glob.glob(f"{RAW_DIR}/*_{kind}_{date}*.parquet"))
    frames = []
    for p in paths:
        asset = p.split("/")[-1].split(f"_{kind}_")[0]
        try:
            t = pq.read_table(p, columns=["ts", "server_ts", "latency_ms"] if kind != "book" else
                               ["ts", "server_ts", "latency_ms", "side"])
        except Exception as e:
            print(f"  ! skip {p}: {e}")
            continue
        df = t.to_pandas()
        df["asset"] = asset
        df["file"] = p
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def summarize(df: pd.DataFrame, label: str):
    print(f"\n=== {label} ===")
    if df.empty:
        print("  no data")
        return
    total = len(df)
    have_latency = df["latency_ms"].notna().sum()
    print(f"  rows={total}  with_latency_ms={have_latency} ({have_latency/total:.1%})")
    for asset, g in df.groupby("asset"):
        lat = g["latency_ms"].dropna()
        if lat.empty:
            print(f"  {asset:6s} n=0 (no latency_ms — pre-schema rows or bba never arrived)")
            continue
        qs = lat.quantile(QUANTILES)
        print(f"  {asset:6s} n={len(lat):6d}  mean={lat.mean():7.1f}ms  "
              f"p50={qs[0.50]:7.1f}  p90={qs[0.90]:7.1f}  p95={qs[0.95]:7.1f}  "
              f"p99={qs[0.99]:7.1f}  max={lat.max():8.1f}ms  min={lat.min():6.1f}ms")


def arrival_gaps(df: pd.DataFrame, label: str):
    print(f"\n=== {label}: inter-arrival gaps (server_ts, ms) — sampler coalescing proxy ===")
    if df.empty:
        return
    for asset, g in df.groupby("asset"):
        st = g["server_ts"].dropna().sort_values().to_numpy()
        if len(st) < 3:
            continue
        gaps = np.diff(st)
        gaps = gaps[gaps > 0]
        if len(gaps) == 0:
            continue
        qs = np.quantile(gaps, QUANTILES)
        pct_at_sampler_floor = (gaps <= 210).mean() * 100  # ~= sampler tick period
        print(f"  {asset:6s} n_gaps={len(gaps):6d}  median_gap={np.median(gaps):7.1f}ms  "
              f"p90={qs[1]:7.1f}  p99={qs[3]:7.1f}  "
              f"%gaps<=210ms={pct_at_sampler_floor:5.1f}%")


if __name__ == "__main__":
    print(f"Loading date={DATE} from {RAW_DIR}/ ...")

    poly = load_kind("poly", DATE)
    summarize(poly, "POLY (best_bid_ask/price_change merged — what the trader consumes via NATS)")
    arrival_gaps(poly, "POLY")

    book = load_kind("book", DATE)
    book_up = book[book["side"] == "UP"] if not book.empty else book
    summarize(book_up, "BOOK (UP side only — orderbook channel, recorder-only, not traded on)")

    binance = load_kind("binance", DATE)
    summarize(binance, "BINANCE (spot @trade stream, 250ms-sampled before NATS publish + parquet write)")
    arrival_gaps(binance, "BINANCE")
