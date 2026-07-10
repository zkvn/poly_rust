#!/usr/bin/env python3
"""Regression tests for build_backtest_prices.py (2026-07-10 fix).

build_binance() used to date-filter btc_5mins/prices/{asset}_binance.parquet
— a merged file from the old Python collector that silently stopped
updating on 2026-07-05. Every backtest date after that had zero binance
rows, so the Rust backtest engine had no price series to compute a signal
from and could never fire a single trade regardless of config — this
looked like "the backtest can't reproduce live trading" but was actually
"the backtest has no binance data at all" for any recent date. Fixed to
source from price_feed/raw/ the same way build_poly() already does.

Uses small synthetic parquet fixtures — no real network/Oracle access, no
dependency on the actual (large) price_feed/raw/ contents.

Run with the same interpreter as the cron job:
    /home/kev/apps/btc_5mins/venv/bin/python trader/scripts/test_build_backtest_prices.py
"""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parent / "build_backtest_prices.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_backtest_prices", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


class BuildBinanceSourcesFromPriceFeedRawTests(unittest.TestCase):
    """Regression guard: binance must come from price_feed/raw/ hourly
    shards, not any btc_5mins/prices/*.parquet merged file — there must be
    no such reference left in the module at all."""

    def test_module_has_no_btc_5mins_binance_source_left(self):
        # "btc_5mins" legitimately appears in the module docstring's history
        # note; what must actually be gone is the constant that pointed
        # build_binance() at that stale merged file as its data source.
        self.assertFalse(hasattr(mod, "BINANCE_SRC_DIR"))

    def test_build_binance_concatenates_hourly_shards_from_raw_dir(self):
        with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as out_dir:
            raw = Path(raw_dir)
            pd.DataFrame({
                "ts": [1.0, 2.0], "binance": [100.0, 101.0],
                "slug": ["eth-updown-5m-1", "eth-updown-5m-1"],
                "server_ts": [1000.0, 2000.0], "latency_ms": [5.0, 6.0],
            }).to_parquet(raw / "ETH_binance_2026-07-09_00.parquet", index=False)
            pd.DataFrame({
                "ts": [3.0], "binance": [102.0], "slug": ["eth-updown-5m-1"],
                "server_ts": [3000.0], "latency_ms": [7.0],
            }).to_parquet(raw / "ETH_binance_2026-07-09_01.parquet", index=False)

            with patch.object(mod, "RAW_DIR", raw):
                mod.build_binance("ETH", "2026-07-09", Path(out_dir))

            out_path = Path(out_dir) / "ETH_binance_2026-07-09.parquet"
            self.assertTrue(out_path.exists())
            df = pd.read_parquet(out_path)
            self.assertEqual(sorted(df.columns), ["binance", "slug", "ts"])
            self.assertEqual(len(df), 3)
            self.assertEqual(list(df["ts"]), [1.0, 2.0, 3.0])

    def test_build_binance_dedupes_on_ts_and_slug(self):
        with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as out_dir:
            raw = Path(raw_dir)
            row = pd.DataFrame({
                "ts": [1.0], "binance": [100.0], "slug": ["eth-updown-5m-1"],
                "server_ts": [1000.0], "latency_ms": [5.0],
            })
            row.to_parquet(raw / "ETH_binance_2026-07-09_00.parquet", index=False)
            row.to_parquet(raw / "ETH_binance_2026-07-09.parquet", index=False)  # overlapping daily file

            with patch.object(mod, "RAW_DIR", raw):
                mod.build_binance("ETH", "2026-07-09", Path(out_dir))

            df = pd.read_parquet(Path(out_dir) / "ETH_binance_2026-07-09.parquet")
            self.assertEqual(len(df), 1)

    def test_build_binance_writes_nothing_when_no_shards_found(self):
        """No silent empty-file write — an empty binance series is exactly
        what caused the original bug (zero trades with no visible error)."""
        with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as out_dir:
            with patch.object(mod, "RAW_DIR", Path(raw_dir)):
                mod.build_binance("ETH", "2026-07-09", Path(out_dir))
            self.assertFalse((Path(out_dir) / "ETH_binance_2026-07-09.parquet").exists())


class BuildPolyUnchangedBehaviorTests(unittest.TestCase):
    """build_poly() wasn't the buggy path, but _gather_shards is now shared
    with build_binance — confirm poly's stuck-price filter still applies."""

    def test_build_poly_drops_stuck_up_0_5_rows(self):
        with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as out_dir:
            raw = Path(raw_dir)
            pd.DataFrame({
                "ts": [1.0, 2.0], "up": [0.6, 0.5], "dn": [0.4, 0.5],
                "slug": ["eth-updown-5m-1", "eth-updown-5m-1"],
            }).to_parquet(raw / "ETH_poly_2026-07-09_00.parquet", index=False)

            with patch.object(mod, "RAW_DIR", raw):
                mod.build_poly("ETH", "2026-07-09", Path(out_dir))

            df = pd.read_parquet(Path(out_dir) / "ETH_poly_2026-07-09.parquet")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["up"], 0.6)


class ReadOrRecoverTests(unittest.TestCase):
    def test_falls_back_to_recover_fn_on_unreadable_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "corrupt.parquet"
            path.write_bytes(b"not a real parquet file")
            sentinel = pd.DataFrame({"ts": [1.0]})
            result = mod._read_or_recover(path, lambda p: sentinel)
            pd.testing.assert_frame_equal(result, sentinel)

    def test_reads_normally_when_file_is_valid(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "valid.parquet"
            df = pd.DataFrame({"ts": [1.0, 2.0]})
            df.to_parquet(path, index=False)

            def boom(p):
                raise AssertionError("recover_fn should not be called for a valid file")

            result = mod._read_or_recover(path, boom)
            pd.testing.assert_frame_equal(result.reset_index(drop=True), df)


if __name__ == "__main__":
    unittest.main()
