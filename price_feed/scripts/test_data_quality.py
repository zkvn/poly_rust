#!/usr/bin/env python3
"""Unit tests for data_quality.py — price_feed/doc/incident_collector_data_loss_2026-07-12.md's
proposed tick-coverage observer.

Run with the same interpreter as the daily recon cron job:
    /home/kev/apps/btc_5mins/venv/bin/python price_feed/scripts/test_data_quality.py
"""
import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parent / "data_quality.py"
HKT = timezone(timedelta(hours=8))


def _load_module():
    spec = importlib.util.spec_from_file_location("data_quality", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


def _write_hour(raw_dir: Path, asset: str, kind: str, date: str, hour: int, minute_ticks: list):
    """minute_ticks: list of minute-of-hour ints (each contributes exactly one row at
    :00 seconds of that minute) — e.g. [0, 1, 2] -> 3 rows, 3/60 minutes covered."""
    base = datetime.strptime(f"{date} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=HKT)
    rows = []
    for m in minute_ticks:
        ts = (base + timedelta(minutes=m)).timestamp()
        rows.append({"ts": ts, "price": 1.0, "slug": "x"})
    df = pd.DataFrame(rows)
    path = raw_dir / f"{asset}_{kind}_{date}_{hour:02d}.parquet"
    df.to_parquet(path, index=False)
    return path


class HourlyMinuteCoverageTests(unittest.TestCase):
    def test_full_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_hour(Path(d), "ETH", "binance", "2026-07-12", 10, list(range(60)))
            self.assertAlmostEqual(mod.hourly_minute_coverage(path), 100.0)

    def test_partial_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_hour(Path(d), "ETH", "binance", "2026-07-12", 10, [0, 1, 2])
            self.assertAlmostEqual(mod.hourly_minute_coverage(path), 3 / 60 * 100)

    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ETH_binance_2026-07-12_10.parquet"
            self.assertIsNone(mod.hourly_minute_coverage(path))

    def test_duplicate_ticks_in_the_same_minute_only_count_once(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            base = datetime(2026, 7, 12, 10, 0, 0, tzinfo=HKT)
            rows = [{"ts": (base + timedelta(seconds=s)).timestamp(), "price": 1.0, "slug": "x"}
                    for s in (0, 5, 10, 15, 55)]  # all within minute 0
            pd.DataFrame(rows).to_parquet(raw_dir / "ETH_binance_2026-07-12_10.parquet", index=False)
            coverage = mod.hourly_minute_coverage(raw_dir / "ETH_binance_2026-07-12_10.parquet")
            self.assertAlmostEqual(coverage, 1 / 60 * 100)

    def test_corrupt_file_returns_none_not_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ETH_binance_2026-07-12_10.parquet"
            path.write_bytes(b"not a real parquet file")
            self.assertIsNone(mod.hourly_minute_coverage(path))


class IterElapsedHoursTests(unittest.TestCase):
    def test_yields_only_fully_elapsed_hours(self):
        window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
        window_end = datetime(2026, 7, 12, 20, 0, tzinfo=HKT)
        now = datetime(2026, 7, 12, 10, 30, tzinfo=HKT)  # 10:00-10:59 still in progress
        hours = list(mod.iter_elapsed_hours(window_start, window_end, now))
        self.assertEqual(hours, [("2026-07-12", 8), ("2026-07-12", 9)])

    def test_excludes_hours_after_window_end(self):
        window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
        window_end = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
        now = datetime(2026, 7, 12, 20, 0, tzinfo=HKT)  # far in the future relative to window
        hours = list(mod.iter_elapsed_hours(window_start, window_end, now))
        self.assertEqual(hours, [("2026-07-12", 8), ("2026-07-12", 9)])

    def test_crosses_a_date_boundary(self):
        window_start = datetime(2026, 7, 11, 23, 0, tzinfo=HKT)
        window_end = datetime(2026, 7, 12, 2, 0, tzinfo=HKT)
        now = datetime(2026, 7, 12, 20, 0, tzinfo=HKT)
        hours = list(mod.iter_elapsed_hours(window_start, window_end, now))
        self.assertEqual(
            hours,
            [("2026-07-11", 23), ("2026-07-12", 0), ("2026-07-12", 1)],
        )


class DiscoverRecordedAssetKindsTests(unittest.TestCase):
    def test_finds_pairs_with_actual_rows(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 10, [0, 1])
            pairs = mod.discover_recorded_asset_kinds(raw_dir)
            self.assertIn(("ETH", "binance"), pairs)

    def test_excludes_a_pair_whose_files_are_always_empty(self):
        """Regression test for the HYPE/binance false-positive: HYPE has no Binance market,
        so its files exist (the collector's unconditional hourly-seal check still creates
        them) but always have 0 rows — this pair must not be flagged as MISSING forever."""
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            for hh in range(5):
                _write_hour(raw_dir, "HYPE", "binance", "2026-07-12", hh, [])  # 0 rows each
            pairs = mod.discover_recorded_asset_kinds(raw_dir)
            self.assertNotIn(("HYPE", "binance"), pairs)

    def test_ignores_non_matching_filenames(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            (raw_dir / "not_a_data_file.txt").write_text("x")
            (raw_dir / "sync_oracle.log").write_text("x")
            pairs = mod.discover_recorded_asset_kinds(raw_dir)
            self.assertEqual(pairs, set())


class CheckDataQualityTests(unittest.TestCase):
    def test_flags_a_gap_hour(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 8, list(range(3)))  # 5% coverage
            window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
            window_end = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            now = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            result = mod.check_data_quality(raw_dir, window_start, window_end, now=now)
            gap = [f for f in result["flagged"] if f["status"] == "GAP"]
            self.assertEqual(len(gap), 1)
            self.assertEqual(gap[0]["hour"], 8)
            self.assertAlmostEqual(gap[0]["coverage_pct"], 5.0)

    def test_flags_a_missing_hour(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            # Establish the pair exists via a healthy hour, then leave hour 9 missing entirely.
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 8, list(range(60)))
            window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
            window_end = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            now = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            result = mod.check_data_quality(raw_dir, window_start, window_end, now=now)
            missing = [f for f in result["flagged"] if f["status"] == "MISSING"]
            self.assertEqual(len(missing), 1)
            self.assertEqual(missing[0]["hour"], 9)

    def test_healthy_hours_are_not_flagged(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 8, list(range(60)))
            window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
            window_end = datetime(2026, 7, 12, 9, 0, tzinfo=HKT)
            now = datetime(2026, 7, 12, 9, 0, tzinfo=HKT)
            result = mod.check_data_quality(raw_dir, window_start, window_end, now=now)
            self.assertEqual(result["flagged"], [])
            self.assertEqual(result["hours_checked"], 1)

    def test_missing_raw_dir_returns_empty_not_fatal(self):
        window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
        window_end = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
        result = mod.check_data_quality(
            Path("/nonexistent/raw"), window_start, window_end,
            now=datetime(2026, 7, 12, 10, 0, tzinfo=HKT),
        )
        self.assertEqual(result, {"hours_checked": 0, "flagged": []})

    def test_flagged_rows_sorted_by_date_hour_asset_kind(self):
        with tempfile.TemporaryDirectory() as d:
            raw_dir = Path(d)
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 8, [0])
            _write_hour(raw_dir, "BTC", "binance", "2026-07-12", 8, [0])
            _write_hour(raw_dir, "ETH", "binance", "2026-07-12", 9, [0])
            window_start = datetime(2026, 7, 12, 8, 0, tzinfo=HKT)
            window_end = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            now = datetime(2026, 7, 12, 10, 0, tzinfo=HKT)
            result = mod.check_data_quality(raw_dir, window_start, window_end, now=now)
            keys = [(f["date"], f["hour"], f["asset"]) for f in result["flagged"]]
            self.assertEqual(keys, sorted(keys))


class SafeCheckDataQualityTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        from unittest.mock import patch
        with patch.object(mod, "check_data_quality", side_effect=RuntimeError("boom")):
            result = mod.safe_check_data_quality(
                Path("/tmp"),
                datetime(2026, 7, 12, 8, 0, tzinfo=HKT),
                datetime(2026, 7, 12, 10, 0, tzinfo=HKT),
            )
        self.assertEqual(result["flagged"], [])
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
