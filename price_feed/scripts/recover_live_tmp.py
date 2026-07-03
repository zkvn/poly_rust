#!/usr/bin/env python3
"""Merge today's era-split parquet files (old daily + hourly-sealed + live
.tmp) into a single up-to-date {asset}_{type}_{date}.parquet per asset.

Background: the collector hourly-seals raw/*.parquet, so a given day's data
can be split across an old pre-seal daily file, N hourly-sealed files
(_HH.parquet), and one live unsealed file for the current hour
(_HH.parquet.tmp) that has no footer yet (README.md's "Parquet file
integrity" section). This script recovers the live .tmp via raw page-byte
decoding and merges everything into one deduped, sorted file per asset, so
downstream analysis always has the freshest data instead of being stuck at
the last hourly seal (or the last manual sync).

Why raw page decoding instead of just re-syncing: sync_oracle.sh only pulls
whatever bytes exist at sync time. The current hour's file is *always*
unsealed until it rolls over, so there's no clean way to read it with a
standard parquet reader (pyarrow) at all -- recovery is mandatory for
"latest" data, not just a fallback for a bad sync.

Recovery relies on btc_5mins/bot/parquet_utils.py (a sibling project, see
BTC_5MINS_BOT below) for low-level page/thrift decoding primitives
(_scan_pages, _float64_dict, _string_dict, _decode_hybrid, etc.). Its own
recover_poly_rust_parquet()/recover_book_parquet() do NOT work on files
written by *this* repo's Rust collector, for two reasons fixed locally here
(see _data_indices_robust and _data_indices_list_fixed below) instead of
patching the shared file:

  1. Non-nullable scalar columns have no definition-levels section at all.
     parquet_utils._data_indices() always reads a 4-byte def-levels length
     prefix, which for a required column is actually the start of real data
     -- it decodes a garbage bit_width and raises IndexError (silently
     swallowed by the caller's try/except, so recovery just returns 0 rows).
     Confirmed via `t.schema` on a sealed file: every scalar/list column is
     `not null` except server_ts/latency_ms.

  2. Rust's arrow writer never emits definition level 3 for list columns
     (list<float32> "not null" -- only the *list* can be conceptually empty
     Def=0, never null; elements are always present at Def=2). The shared
     recover_book_parquet() hardcodes max_def=(1<<def_bw)-1=3 as the
     "present" sentinel, which never matches, so every list decodes empty.

  3. recover_book_parquet() also hardcodes an 11-column stride, but this
     repo's book schema has 13 columns (11 + server_ts, latency_ms) -- see
     README.md. The stride bug isn't hit on a small tmp file with one row
     group, but silently misaligns every row group after the first on
     larger files, so it's fixed here too (stride=13, skip the last 2 cols).

Usage:
    python3 recover_live_tmp.py --type poly --date 2026-07-02
    python3 recover_live_tmp.py --type book --date 2026-07-02
    python3 recover_live_tmp.py --type poly --date 2026-07-02 --asset BNB,BTC

Run `sync_oracle.sh` first to get a byte-complete copy of the live .tmp --
a stale/mid-flush rsync snapshot can truncate the last page and still fail
recovery (this is a separate failure mode from the missing-def-levels bug
above; both must be handled).
"""
from __future__ import annotations

import argparse
import glob
import os
import struct
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

BTC_5MINS_BOT = "/home/kev/apps/btc_5mins/bot"
sys.path.insert(0, BTC_5MINS_BOT)
import parquet_utils as pu  # noqa: E402  (path must be set up first)

RAW = os.path.join(os.path.dirname(__file__), "..", "raw")
ASSETS = ["BNB", "BTC", "DOGE", "ETH", "HYPE", "SOL", "XRP"]


def _data_indices_robust(data: bytes, p: dict) -> list[int]:
    """Like parquet_utils._data_indices() but works for REQUIRED columns
    (no definition-levels prefix), by sanity-checking the would-be
    def-levels length against the page's actual remaining bytes."""
    raw = pu._page_bytes(data, p)
    nv = p["nv"]
    if len(raw) >= 4:
        def_len_guess = struct.unpack_from("<I", raw, 0)[0]
        if 0 <= def_len_guess <= len(raw) - 5:
            pos2 = 4 + def_len_guess
            bit_width = raw[pos2]; pos2 += 1
            return pu._decode_hybrid(raw[pos2:], nv, bit_width)
    bit_width = raw[0]
    return pu._decode_hybrid(raw[1:], nv, bit_width)


def _data_indices_list_fixed(data: bytes, p: dict, rep_bw: int = 1, def_bw: int = 2,
                              present_level: int = 2) -> tuple[list[int], list[int], list[int]]:
    """Like parquet_utils._data_indices_list() but with the correct
    "present" definition level for a not-null list<not-null-item> column
    (present_level=2), instead of the hardcoded max_def=3 that never
    matches Rust-written list columns."""
    raw = pu._page_bytes(data, p); pos2 = 0
    rep_len = struct.unpack_from("<I", raw, pos2)[0]; pos2 += 4
    rep_levels = pu._decode_hybrid(raw[pos2:pos2 + rep_len], p["nv"], rep_bw); pos2 += rep_len
    def_len = struct.unpack_from("<I", raw, pos2)[0]; pos2 += 4
    def_levels = pu._decode_hybrid(raw[pos2:pos2 + def_len], p["nv"], def_bw); pos2 += def_len
    bit_width = raw[pos2]; pos2 += 1
    n_values = sum(1 for x in def_levels if x == present_level)
    indices = pu._decode_hybrid(raw[pos2:], n_values, bit_width)
    return rep_levels, def_levels, indices


def recover_poly_rust_tmp(path: str) -> pd.DataFrame:
    """Recover a live (footerless) poly_rust poly .tmp file.
    Columns: ts, up, dn, slug (server_ts/latency_ms not recovered -- callers
    don't need them, and they're nullable so their page layout differs)."""
    data = open(path, "rb").read()
    assert data[:4] == b"PAR1", "not a parquet file"
    pages = pu._scan_pages(data)
    dict_idxs = [i for i, p in enumerate(pages) if p["type"] == 2]
    all_ts, all_up, all_dn, all_slug = [], [], [], []

    for ci in range(0, len(dict_idxs) - 5, 6):
        d0, d1, d2, d3, d4, _d5 = dict_idxs[ci:ci + 6]
        try:
            rg_ts, rg_up, rg_dn, rg_slug = [], [], [], []
            ts_d = pu._float64_dict(data, pages[d0])
            for dp in pages[d0 + 1:d1]:
                rg_ts.extend(ts_d[j] for j in _data_indices_robust(data, dp))
            up_d = pu._float64_dict(data, pages[d1])
            for dp in pages[d1 + 1:d2]:
                rg_up.extend(up_d[j] for j in _data_indices_robust(data, dp))
            dn_d = pu._float64_dict(data, pages[d2])
            for dp in pages[d2 + 1:d3]:
                rg_dn.extend(dn_d[j] for j in _data_indices_robust(data, dp))
            sl_d = pu._string_dict(data, pages[d3])
            for dp in pages[d3 + 1:d4]:
                rg_slug.extend(sl_d[j] for j in _data_indices_robust(data, dp))
            if len({len(rg_ts), len(rg_up), len(rg_dn), len(rg_slug)}) != 1:
                continue  # partial/truncated row group (mid-flush) -- skip it
            all_ts.extend(rg_ts); all_up.extend(rg_up); all_dn.extend(rg_dn); all_slug.extend(rg_slug)
        except Exception:
            continue
    return pd.DataFrame({"ts": all_ts, "up": all_up, "dn": all_dn, "slug": all_slug})


def recover_book_rust_tmp(path: str) -> pd.DataFrame:
    """Recover a live (footerless) poly_rust book .tmp file.
    13-column schema (README.md): ts, asset, slug, side, best_bid, best_ask,
    last_trade, bid_prices, bid_sizes, ask_prices, ask_sizes, server_ts,
    latency_ms -- the last 2 are skipped (nullable, not needed by callers)."""
    data = open(path, "rb").read()
    assert data[:4] == b"PAR1", "not a parquet file"
    pages = pu._scan_pages(data)
    dict_idxs = [i for i, p in enumerate(pages) if p["type"] == 2]

    keys = ["ts", "asset", "slug", "side", "best_bid", "best_ask", "last_trade",
            "bid_prices", "bid_sizes", "ask_prices", "ask_sizes"]
    cols: dict[str, list] = {k: [] for k in keys}

    for ci in range(0, len(dict_idxs) - 12, 13):
        d = dict_idxs[ci:ci + 13]
        try:
            rg: dict[str, list] = {k: [] for k in keys}

            ts_d = pu._float64_dict(data, pages[d[0]])
            for dp in pages[d[0] + 1:d[1]]:
                rg["ts"].extend(ts_d[j] for j in _data_indices_robust(data, dp))

            asset_d = pu._string_dict(data, pages[d[1]])
            for dp in pages[d[1] + 1:d[2]]:
                rg["asset"].extend(asset_d[j] for j in _data_indices_robust(data, dp))

            slug_d = pu._string_dict(data, pages[d[2]])
            for dp in pages[d[2] + 1:d[3]]:
                rg["slug"].extend(slug_d[j] for j in _data_indices_robust(data, dp))

            side_d = pu._string_dict(data, pages[d[3]])
            for dp in pages[d[3] + 1:d[4]]:
                rg["side"].extend(side_d[j] for j in _data_indices_robust(data, dp))

            bb_d = pu._float64_dict(data, pages[d[4]])
            for dp in pages[d[4] + 1:d[5]]:
                rg["best_bid"].extend(bb_d[j] for j in _data_indices_robust(data, dp))

            ba_d = pu._float64_dict(data, pages[d[5]])
            for dp in pages[d[5] + 1:d[6]]:
                rg["best_ask"].extend(ba_d[j] for j in _data_indices_robust(data, dp))

            lt_d = pu._float64_dict(data, pages[d[6]])
            for dp in pages[d[6] + 1:d[7]]:
                rg["last_trade"].extend(lt_d[j] for j in _data_indices_robust(data, dp))

            for col, di, d_next in (
                ("bid_prices", d[7], d[8]), ("bid_sizes", d[8], d[9]),
                ("ask_prices", d[9], d[10]), ("ask_sizes", d[10], d[11]),
            ):
                dict_vals = pu._float32_dict(data, pages[di])
                for dp in pages[di + 1:d_next]:
                    rep, _, idxs = _data_indices_list_fixed(data, dp)
                    rg[col].extend(
                        np.array(lst, dtype=np.float32)
                        for lst in pu._rep_to_lists(rep, [dict_vals[j] for j in idxs]))

            n = {len(v) for v in rg.values()}
            if len(n) != 1:
                continue  # partial/truncated row group (mid-flush) -- skip it
            for k in keys:
                cols[k].extend(rg[k])
        except Exception:
            continue

    return pd.DataFrame(cols)


def _read_or_recover(path: str, recover_fn) -> tuple[pd.DataFrame, int]:
    """Read a parquet file normally; if it's unsealed (no footer — the plain
    daily file between hourly reseals, or a live *.tmp), fall back to the
    raw-page recovery decoder. Returns (df, recovered_row_count)."""
    try:
        return pq.read_table(path).to_pandas(), 0
    except Exception:
        rec = recover_fn(path)
        return rec, len(rec)


def merge_asset(asset: str, kind: str, date: str) -> None:
    recover_fn = recover_poly_rust_tmp if kind == "poly" else recover_book_rust_tmp
    dedup_keys = ["ts", "slug"] if kind == "poly" else ["ts", "slug", "side"]

    frames = []
    recovered_rows = 0

    # The plain daily file is the collector's live target between hourly
    # reseals (README's "Hourly seal" section) -- it can be footerless at
    # read time even though it's not a *.tmp, so it needs the same recovery
    # fallback as the tmp candidates below.
    daily = os.path.join(RAW, f"{asset}_{kind}_{date}.parquet")
    if os.path.exists(daily):
        df, n = _read_or_recover(daily, recover_fn)
        recovered_rows += n
        frames.append(df)

    sealed = sorted(glob.glob(os.path.join(RAW, f"{asset}_{kind}_{date}_*.parquet")))
    for f in sealed:
        df, n = _read_or_recover(f, recover_fn)
        recovered_rows += n
        frames.append(df)

    tmp_candidates = sorted(glob.glob(os.path.join(RAW, f"{asset}_{kind}_{date}_*.parquet.tmp")))
    for tmp in tmp_candidates:
        df, n = _read_or_recover(tmp, recover_fn)
        recovered_rows += n
        frames.append(df)

    if not frames:
        print(f"{asset}: no files found for {kind}/{date}, skipping")
        return

    merged = pd.concat(frames, ignore_index=True, sort=False)
    before = len(merged)
    merged = merged.drop_duplicates(subset=dedup_keys).sort_values("ts").reset_index(drop=True)
    after = len(merged)

    merged.to_parquet(daily, index=False)
    print(f"{asset}: {before} -> {after} rows (deduped {before - after}), "
          f"recovered_rows={recovered_rows}, max_ts={merged['ts'].max()}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--type", choices=["poly", "book"], required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--asset", default=",".join(ASSETS), help="comma-separated, default all 7")
    args = ap.parse_args()

    for asset in args.asset.split(","):
        merge_asset(asset, args.type, args.date)


if __name__ == "__main__":
    main()
