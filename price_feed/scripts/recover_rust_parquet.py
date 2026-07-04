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
    _parse_thrift, _page_bytes, _decode_hybrid,
    _float64_dict, _float32_dict, _string_dict, _data_indices,
)


def _scan_pages(data: bytes) -> list[dict]:
    """Like parquet_utils._scan_pages, but also records each data page's encoding.

    Needed because arrow-rs falls back from RLE_DICTIONARY (8) to PLAIN (0) for a
    column once that column's dictionary page exceeds the writer's size threshold
    (ts is nearly all-unique, so on a full day's file this triggers reliably
    partway through a row group) — the rest of the column-chunk's data pages are
    then raw values, not dictionary indices, and must be decoded differently.
    """
    pos = 4
    pages = []
    while pos < len(data) - 10:
        hdr, next_pos = _parse_thrift(data, pos)
        if 1 not in hdr or 3 not in hdr:
            break
        pt = hdr[1]
        csz = hdr[3]
        nv = None
        encoding = None
        if 5 in hdr and isinstance(hdr[5], dict):
            nv = hdr[5].get(1)
            encoding = hdr[5].get(2)
        if 7 in hdr and isinstance(hdr[7], dict):
            nv = hdr[7].get(1)
        pages.append({"hdr_end": next_pos, "type": pt, "csz": csz, "nv": nv, "encoding": encoding})
        pos = next_pos + csz
    return pages


def _scalar_values_req(data: bytes, p: dict, dic: list, kind: str) -> list:
    """REQUIRED scalar column data page: dictionary-indexed (normal case) or PLAIN
    (raw values) if the column's dictionary overflowed partway through the file."""
    raw = _page_bytes(data, p)
    nv = p["nv"]
    if p.get("encoding") == 0:  # PLAIN
        if kind == "str":
            vals: list = []
            pos = 0
            for _ in range(nv):
                ln = struct.unpack_from("<I", raw, pos)[0]
                pos += 4
                vals.append(raw[pos:pos + ln].decode("utf-8"))
                pos += ln
            return vals
        fmt = {"f8": "d", "f4": "f"}[kind]
        return list(struct.unpack_from(f"<{nv}{fmt}", raw))
    bit_width = raw[0]
    idxs = _decode_hybrid(raw[1:], nv, bit_width)
    return [dic[j] for j in idxs]


def _list_values_req(data: bytes, p: dict, dic: list) -> tuple[list[int], list]:
    """List<Float32> column, outer REQUIRED / item OPTIONAL: max_rep=1, max_def=2.

    Values are dictionary-indexed (normal case) or PLAIN floats if the column's
    dictionary overflowed partway through the file (same fallback as
    _scalar_values_req). Returns (rep_levels, values) where values has one entry
    per rep_level position: a float if def==2, else None.
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
    n_values = sum(1 for x in def_levels if x == 2)
    if p.get("encoding") == 0:  # PLAIN
        flat = list(struct.unpack_from(f"<{n_values}f", raw, value_start))
    else:
        bit_width = raw[value_start]
        idxs = _decode_hybrid(raw[value_start + 1:], n_values, bit_width)
        flat = [dic[j] for j in idxs]
    it = iter(flat)
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


def recover_rust_binance_parquet(path: str) -> pd.DataFrame:
    """Binance schema (5 col, stride 1 dict page/col/row-group): ts, binance, slug,
    server_ts, latency_ms — same layout as poly minus the second float column."""
    data = open(path, "rb").read()
    assert data[:4] == b"PAR1", "not a parquet file"
    pages = _scan_pages(data)
    dict_idxs = [i for i, p in enumerate(pages) if p["type"] == 2]

    cols = {k: [] for k in ["ts", "binance", "slug", "server_ts", "latency_ms"]}
    for ci in range(0, len(dict_idxs) - 4, 5):
        d = dict_idxs[ci:ci + 5]
        d_next = dict_idxs[ci + 5] if ci + 5 < len(dict_idxs) else len(pages)
        try:
            ts_d = _float64_dict(data, pages[d[0]])
            for dp in pages[d[0]+1:d[1]]:
                cols["ts"].extend(_scalar_values_req(data, dp, ts_d, "f8"))
            bn_d = _float64_dict(data, pages[d[1]])
            for dp in pages[d[1]+1:d[2]]:
                cols["binance"].extend(_scalar_values_req(data, dp, bn_d, "f8"))
            sl_d = _string_dict(data, pages[d[2]])
            for dp in pages[d[2]+1:d[3]]:
                cols["slug"].extend(_scalar_values_req(data, dp, sl_d, "str"))
            st_d = _float64_dict(data, pages[d[3]])
            for dp in pages[d[3]+1:d[4]]:
                idxs = _data_indices(data, dp)
                cols["server_ts"].extend(st_d[j] for j in idxs)
            lt_d = _float64_dict(data, pages[d[4]])
            for dp in pages[d[4]+1:d_next]:
                idxs = _data_indices(data, dp)
                cols["latency_ms"].extend(lt_d[j] for j in idxs)
        except Exception as e:
            print(f"  [row group {ci}] decode error: {e}")
            continue

    n = len(cols["ts"])
    for k in ("server_ts", "latency_ms"):
        if len(cols[k]) < n:
            cols[k] = cols[k] + [float("nan")] * (n - len(cols[k]))
        cols[k] = cols[k][:n]
    return pd.DataFrame({k: v[:n] for k, v in cols.items()})


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
                cols["ts"].extend(_scalar_values_req(data, dp, ts_d, "f8"))
            up_d = _float64_dict(data, pages[d[1]])
            for dp in pages[d[1]+1:d[2]]:
                cols["up"].extend(_scalar_values_req(data, dp, up_d, "f8"))
            dn_d = _float64_dict(data, pages[d[2]])
            for dp in pages[d[2]+1:d[3]]:
                cols["dn"].extend(_scalar_values_req(data, dp, dn_d, "f8"))
            sl_d = _string_dict(data, pages[d[3]])
            for dp in pages[d[3]+1:d[4]]:
                cols["slug"].extend(_scalar_values_req(data, dp, sl_d, "str"))
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
                cols["ts"].extend(_scalar_values_req(data, dp, ts_d, "f8"))
            asset_d = _string_dict(data, pages[d[1]])
            for dp in pages[d[1]+1:d[2]]:
                cols["asset"].extend(_scalar_values_req(data, dp, asset_d, "str"))
            slug_d = _string_dict(data, pages[d[2]])
            for dp in pages[d[2]+1:d[3]]:
                cols["slug"].extend(_scalar_values_req(data, dp, slug_d, "str"))
            side_d = _string_dict(data, pages[d[3]])
            for dp in pages[d[3]+1:d[4]]:
                cols["side"].extend(_scalar_values_req(data, dp, side_d, "str"))
            bb_d = _float64_dict(data, pages[d[4]])
            for dp in pages[d[4]+1:d[5]]:
                cols["best_bid"].extend(_scalar_values_req(data, dp, bb_d, "f8"))
            ba_d = _float64_dict(data, pages[d[5]])
            for dp in pages[d[5]+1:d[6]]:
                cols["best_ask"].extend(_scalar_values_req(data, dp, ba_d, "f8"))
            lt_d = _float64_dict(data, pages[d[6]])
            for dp in pages[d[6]+1:d[7]]:
                cols["last_trade"].extend(_scalar_values_req(data, dp, lt_d, "f8"))

            bp_d = _float32_dict(data, pages[d[7]])
            for dp in pages[d[7]+1:d[8]]:
                rep, vals = _list_values_req(data, dp, bp_d)
                cols["bid_prices"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            bs_d = _float32_dict(data, pages[d[8]])
            for dp in pages[d[8]+1:d[9]]:
                rep, vals = _list_values_req(data, dp, bs_d)
                cols["bid_sizes"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            ap_d = _float32_dict(data, pages[d[9]])
            for dp in pages[d[9]+1:d[10]]:
                rep, vals = _list_values_req(data, dp, ap_d)
                cols["ask_prices"].extend(
                    np.array(lst, dtype=np.float32) for lst in _rep_to_lists_opt(rep, vals))

            as_d = _float32_dict(data, pages[d[10]])
            for dp in pages[d[10]+1:d[11]]:
                rep, vals = _list_values_req(data, dp, as_d)
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


def check_file(path: str) -> bool:
    """Fast readability check (footer present, parses cleanly) — no page-scan recovery."""
    try:
        pd.read_parquet(path)
        return True
    except Exception as e:
        print(f"{path}: BAD — {e}")
        return False


def recover_file(path: str, write: bool) -> None:
    if "_poly_" in path:
        kind = "poly"
    elif "_book_" in path:
        kind = "book"
    elif "_binance_" in path:
        kind = "binance"
    else:
        kind = None
    if kind is None:
        print(f"{path}: skipping — not a poly/book/binance file")
        return
    fn = {
        "poly": recover_rust_poly_parquet,
        "book": recover_rust_book_parquet,
        "binance": recover_rust_binance_parquet,
    }[kind]
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
    parser.add_argument("--check", action="store_true",
                        help="only check whether files are readable (fast, no recovery); prints BAD files "
                             "and a summary count, ignores --write")
    args = parser.parse_args()

    files = sorted({f for pattern in args.paths for f in glob.glob(pattern)} or set(args.paths))

    if args.check:
        bad = [f for f in files if not check_file(f)]
        print(f"\n{len(files)} files checked, {len(bad)} bad")
        raise SystemExit(1 if bad else 0)

    for path in files:
        recover_file(path, args.write)
