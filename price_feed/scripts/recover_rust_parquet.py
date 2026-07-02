"""Recovery for footerless parquet files written by the Rust price_feed collector.

Byte layout differs from the old PyArrow-written files (see btc_5mins/bot/parquet_utils.py):

  REQUIRED scalar column data page (ts, up, dn, slug, asset, side, best_bid, best_ask,
  last_trade):  NO length-prefixed levels at all (max_def=0, max_rep=0) — page starts
  directly with [bit_width byte][RLE-encoded dictionary indices].

  OPTIONAL scalar column data page (server_ts, latency_ms): [def_len u32][def levels,
  bit_width=1][bit_width byte][RLE-encoded dictionary indices for non-null positions].
  This matches parquet_utils._data_indices already.

  List<Float32> column data page (bid_prices, bid_sizes, ask_prices, ask_sizes): outer
  list REQUIRED, inner item OPTIONAL -> max_rep=1, max_def=2 (0=empty list, 1=null item
  [never observed], 2=present item). Layout: [rep_len u32][rep levels, bw=1][def_len u32]
  [def levels, bw=2][bit_width byte][RLE dict indices for count(def==2) positions].

  Critically, max_def=2 here, NOT (1<<def_bw)-1=3 as parquet_utils._data_indices_list
  assumes by default (that default was written for a different, differently-nested
  PyArrow schema) — using the wrong max_def silently decodes 0 values.

Poly schema (6 col, stride 2 pages/col/row-group): ts, up, dn, slug, server_ts, latency_ms
Book schema (13 col, stride 2 pages/col/row-group): ts, asset, slug, side, best_bid,
  best_ask, last_trade, bid_prices, bid_sizes, ask_prices, ask_sizes, server_ts, latency_ms

Usage:
  python recover_rust_parquet.py "raw_4hr/*.parquet"          # dry run, report row counts
  python recover_rust_parquet.py --write "raw_4hr/*.parquet"  # overwrite with recovered data
"""

import sys
import struct

import numpy as np
import pandas as pd

sys.path.insert(0, "/home/kev/apps/btc_5mins")
from bot.parquet_utils import (
    _scan_pages, _page_bytes, _decode_hybrid,
    _float64_dict, _float32_dict, _string_dict, _data_indices,
)


def _data_indices_req(data: bytes, p: dict) -> list[int]:
    """REQUIRED scalar column: no levels, page starts at [bit_width][RLE dict indices]."""
    raw = _page_bytes(data, p)
    bit_width = raw[0]
    return _decode_hybrid(raw[1:], p["nv"], bit_width)


def _data_indices_list_req(data: bytes, p: dict) -> tuple[list[int], list[int]]:
    """List<T> column, outer REQUIRED / item OPTIONAL: max_rep=1, max_def=2.

    Returns (rep_levels, dict_indices_or_none) where dict_indices_or_none has one
    entry per rep_level position: an int dict index if def==2, else None.
    """
    raw = _page_bytes(data, p)
    nv = p["nv"]
    rep_len = struct.unpack_from("<I", raw, 0)[0]
    pos = 4 + rep_len
    rep_levels = _decode_hybrid(raw[4:pos], nv, 1)
    def_len = struct.unpack_from("<I", raw, pos)[0]
    pos2 = pos + 4
    def_levels = _decode_hybrid(raw[pos2:pos2 + def_len], nv, 2)
    value_start = pos2 + def_len
    bit_width = raw[value_start]
    n_values = sum(1 for x in def_levels if x == 2)
    idxs = _decode_hybrid(raw[value_start + 1:], n_values, bit_width)
    it = iter(idxs)
    out = [next(it) if d == 2 else None for d in def_levels]
    return rep_levels, out


def _rep_to_lists_opt(rep_levels: list[int], vals: list) -> list[list]:
    """Reconstruct list-of-lists; vals entries are values or None (empty-list marker)."""
    result: list[list] = []
    current: list | None = None
    for rep, v in zip(rep_levels, vals):
        if rep == 0:
            if current is not None:
                result.append(current)
            current = [] if v is None else [v]
        else:
            if v is not None:
                current.append(v)
    if current is not None:
        result.append(current)
    return result


def recover_rust_poly_parquet(path: str) -> pd.DataFrame:
    data = open(path, "rb").read()
    assert data[:4] == b"PAR1", "not a parquet file"
    pages = _scan_pages(data)
    dict_idxs = [i for i, p in enumerate(pages) if p["type"] == 2]

    cols = {k: [] for k in ["ts", "up", "dn", "slug", "server_ts", "latency_ms"]}
    for ci in range(0, len(dict_idxs) - 5, 6):
        d = dict_idxs[ci:ci + 6]
        d_next = dict_idxs[ci + 6] if ci + 6 < len(dict_idxs) else len(pages)
        try:
            ts_d = _float64_dict(data, pages[d[0]])
            for dp in pages[d[0]+1:d[1]]:
                cols["ts"].extend(ts_d[j] for j in _data_indices_req(data, dp))
            up_d = _float64_dict(data, pages[d[1]])
            for dp in pages[d[1]+1:d[2]]:
                cols["up"].extend(up_d[j] for j in _data_indices_req(data, dp))
            dn_d = _float64_dict(data, pages[d[2]])
            for dp in pages[d[2]+1:d[3]]:
                cols["dn"].extend(dn_d[j] for j in _data_indices_req(data, dp))
            sl_d = _string_dict(data, pages[d[3]])
            for dp in pages[d[3]+1:d[4]]:
                cols["slug"].extend(sl_d[j] for j in _data_indices_req(data, dp))
            st_d = _float64_dict(data, pages[d[4]])
            for dp in pages[d[4]+1:d[5]]:
                idxs = _data_indices(data, dp)
                cols["server_ts"].extend(st_d[j] for j in idxs)
            lt_d = _float64_dict(data, pages[d[5]])
            for dp in pages[d[5]+1:d_next]:
                idxs = _data_indices(data, dp)
                cols["latency_ms"].extend(lt_d[j] for j in idxs)
        except Exception as e:
            print(f"  [row group {ci}] decode error: {e}")
            continue

    # server_ts/latency_ms are OPTIONAL — _data_indices only returns non-null positions,
    # so their length can legitimately be shorter than ts/up/dn/slug. Pad with NaN.
    n = len(cols["ts"])
    for k in ("server_ts", "latency_ms"):
        if len(cols[k]) < n:
            cols[k] = cols[k] + [float("nan")] * (n - len(cols[k]))
        cols[k] = cols[k][:n]
    return pd.DataFrame({k: v[:n] for k, v in cols.items()})


def recover_rust_book_parquet(path: str) -> pd.DataFrame:
    data = open(path, "rb").read()
    assert data[:4] == b"PAR1", "not a parquet file"
    pages = _scan_pages(data)
    dict_idxs = [i for i, p in enumerate(pages) if p["type"] == 2]

    scalar_cols = ["ts", "asset", "slug", "side", "best_bid", "best_ask", "last_trade"]
    list_cols = ["bid_prices", "bid_sizes", "ask_prices", "ask_sizes"]
    cols: dict[str, list] = {k: [] for k in scalar_cols + list_cols + ["server_ts", "latency_ms"]}

    for ci in range(0, len(dict_idxs) - 12, 13):
        d = dict_idxs[ci:ci + 13]
        d_next = dict_idxs[ci + 13] if ci + 13 < len(dict_idxs) else len(pages)
        try:
            ts_d = _float64_dict(data, pages[d[0]])
            for dp in pages[d[0]+1:d[1]]:
                cols["ts"].extend(ts_d[j] for j in _data_indices_req(data, dp))
            asset_d = _string_dict(data, pages[d[1]])
            for dp in pages[d[1]+1:d[2]]:
                cols["asset"].extend(asset_d[j] for j in _data_indices_req(data, dp))
            slug_d = _string_dict(data, pages[d[2]])
            for dp in pages[d[2]+1:d[3]]:
                cols["slug"].extend(slug_d[j] for j in _data_indices_req(data, dp))
            side_d = _string_dict(data, pages[d[3]])
            for dp in pages[d[3]+1:d[4]]:
                cols["side"].extend(side_d[j] for j in _data_indices_req(data, dp))
            bb_d = _float64_dict(data, pages[d[4]])
            for dp in pages[d[4]+1:d[5]]:
                cols["best_bid"].extend(bb_d[j] for j in _data_indices_req(data, dp))
            ba_d = _float64_dict(data, pages[d[5]])
            for dp in pages[d[5]+1:d[6]]:
                cols["best_ask"].extend(ba_d[j] for j in _data_indices_req(data, dp))
            lt_d = _float64_dict(data, pages[d[6]])
            for dp in pages[d[6]+1:d[7]]:
                cols["last_trade"].extend(lt_d[j] for j in _data_indices_req(data, dp))

            bp_d = _float32_dict(data, pages[d[7]])
            for dp in pages[d[7]+1:d[8]]:
                rep, idxs = _data_indices_list_req(data, dp)
                vals = [bp_d[j] if j is not None else None for j in idxs]
                cols["bid_prices"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            bs_d = _float32_dict(data, pages[d[8]])
            for dp in pages[d[8]+1:d[9]]:
                rep, idxs = _data_indices_list_req(data, dp)
                vals = [bs_d[j] if j is not None else None for j in idxs]
                cols["bid_sizes"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            ap_d = _float32_dict(data, pages[d[9]])
            for dp in pages[d[9]+1:d[10]]:
                rep, idxs = _data_indices_list_req(data, dp)
                vals = [ap_d[j] if j is not None else None for j in idxs]
                cols["ask_prices"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            as_d = _float32_dict(data, pages[d[10]])
            for dp in pages[d[10]+1:d[11]]:
                rep, idxs = _data_indices_list_req(data, dp)
                vals = [as_d[j] if j is not None else None for j in idxs]
                cols["ask_sizes"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            st_d = _float64_dict(data, pages[d[11]])
            for dp in pages[d[11]+1:d[12]]:
                cols["server_ts"].extend(st_d[j] for j in _data_indices(data, dp))
            lat_d = _float64_dict(data, pages[d[12]])
            for dp in pages[d[12]+1:d_next]:
                cols["latency_ms"].extend(lat_d[j] for j in _data_indices(data, dp))
        except Exception as e:
            print(f"  [row group {ci}] decode error: {e}")
            continue

    n = min(len(cols[k]) for k in scalar_cols + list_cols)
    for k in ("server_ts", "latency_ms"):
        if len(cols[k]) < n:
            cols[k] = cols[k] + [float("nan")] * (n - len(cols[k]))
        cols[k] = cols[k][:n]
    return pd.DataFrame({k: v[:n] for k, v in cols.items()})


def recover_file(path: str, write: bool) -> None:
    kind = "poly" if "_poly_" in path else "book" if "_book_" in path else None
    if kind is None:
        print(f"{path}: skipping — not a poly/book file (binance recovery not implemented)")
        return
    fn = recover_rust_poly_parquet if kind == "poly" else recover_rust_book_parquet
    try:
        df = fn(path)
    except Exception as e:
        print(f"{path}: FAILED — {e}")
        return
    extra = ""
    if "latency_ms" in df.columns and df["latency_ms"].notna().any():
        extra = f" median_latency_ms={df['latency_ms'].median():.1f}"
    slugs = df["slug"].nunique() if "slug" in df else "?"
    print(f"{path}: {len(df)} rows, slugs={slugs}{extra}")
    if write and len(df) > 0:
        df.to_parquet(path)
        print(f"  written back to {path}")


if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser(
        description="Recover footerless/corrupted parquet files written by the Rust price_feed "
                    "collector (missing PAR1 footer from a crash before ArrowWriter.close()).")
    parser.add_argument("paths", nargs="+", help="parquet file path(s) or glob pattern(s)")
    parser.add_argument("--write", action="store_true",
                        help="overwrite the source file with the recovered data (default: dry run, report only)")
    args = parser.parse_args()

    files = sorted({f for pattern in args.paths for f in glob.glob(pattern)} or set(args.paths))
    for path in files:
        recover_file(path, args.write)
