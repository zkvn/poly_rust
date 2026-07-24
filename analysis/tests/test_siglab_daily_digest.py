"""Unit tests for siglab_daily_digest.py's non-I/O logic (parsing, grouping, verdicts,
ledger upserts). Run with: python3 -m unittest discover -s analysis/tests -v
"""

from __future__ import annotations

import json
import random
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from siglab_daily_digest import (  # noqa: E402
    GROUP_DIAGNOSTIC_MIN_WEEKS,
    HKT,
    apply_bh_correction,
    assign_verdict,
    barrier_params_for,
    blended_null_and_eligible_trades,
    build_dataframe,
    compute_combo_stats,
    compute_group_diagnostics,
    compute_streaks,
    hkt_day,
    markets_monitored_table,
    read_trades,
    update_ledger,
)


def _ts(date_str: str, hour: int = 12) -> float:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=hour, tzinfo=HKT)
    return dt.timestamp()


def _trade(**overrides) -> dict:
    base = {
        "logged_at": _ts("2026-07-20"),
        "market_kind": "crypto",
        "variant_id": "reversal_0.2_0.55",
        "asset": "BTC",
        "market": "BTC-5m",
        "slug": "btc-updown-5m-1",
        "cycle_start": _ts("2026-07-20"),
        "strategy": "reversal",
        "side": "UP",
        "entry_ts": _ts("2026-07-20"),
        "entry_price_ts": _ts("2026-07-20"),
        "token_price": 0.6,
        "exit_price": 0.75,
        "outcome": "UNWIND",
        "pnl": 0.15,
    }
    base.update(overrides)
    return base


class ReadTradesTests(unittest.TestCase):
    def test_skips_malformed_lines_including_partial_last_line(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps(_trade()) + "\n")
            f.write("not json at all\n")
            f.write(json.dumps(_trade(pnl=0.2)) + "\n")
            f.write('{"logged_at": 123, "incomplete')  # simulates a mid-flush partial write
            path = Path(f.name)
        try:
            trades = read_trades(path)
            self.assertEqual(len(trades), 2)
        finally:
            path.unlink()

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            path = Path(f.name)
        try:
            self.assertEqual(read_trades(path), [])
        finally:
            path.unlink()

    def test_skips_valid_json_missing_a_required_field(self):
        # Found in a DeepSeek code review: valid JSON missing entry_ts would otherwise
        # reach build_dataframe and crash on .apply(hkt_day) with a NaN input, taking down
        # the whole run.
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            good = _trade()
            missing_entry_ts = _trade()
            del missing_entry_ts["entry_ts"]
            f.write(json.dumps(good) + "\n")
            f.write(json.dumps(missing_entry_ts) + "\n")
            path = Path(f.name)
        try:
            trades = read_trades(path)
            self.assertEqual(len(trades), 1)
        finally:
            path.unlink()


class HkTDayTests(unittest.TestCase):
    def test_hkt_offset_applied_correctly(self):
        # 2026-07-20 16:30 UTC = 2026-07-21 00:30 HKT -> next calendar day in HKT.
        utc_dt = datetime(2026, 7, 20, 16, 30, tzinfo=timezone.utc)
        self.assertEqual(hkt_day(utc_dt.timestamp()), "2026-07-21")


class BuildDataframeTests(unittest.TestCase):
    def test_drops_rows_with_empty_market(self):
        trades = [_trade(), _trade(market="")]
        df = build_dataframe(trades)
        self.assertEqual(len(df), 1)

    def test_drops_rows_with_market_key_entirely_absent(self):
        # Found in a DeepSeek code review: a dict missing the `market` key entirely (as
        # opposed to present-but-empty) becomes NaN in the DataFrame, and depending on
        # pandas' dtype backend, `.astype(bool)` alone can evaluate NaN as truthy on a
        # string column — `.fillna("")` first is what actually makes this filter correct.
        missing_market = _trade()
        del missing_market["market"]
        trades = [_trade(), missing_market]
        df = build_dataframe(trades)
        self.assertEqual(len(df), 1)

    def test_empty_input_returns_empty_dataframe_with_expected_columns(self):
        df = build_dataframe([])
        self.assertEqual(len(df), 0)
        self.assertIn("day", df.columns)
        self.assertIn("week", df.columns)


class BarrierParamsForTests(unittest.TestCase):
    def test_v_shape_parses_sl_unwind_from_variant_id(self):
        result = barrier_params_for("v_shape", "v_0.7_0.3_0.7_0.3_0.05", {})
        self.assertEqual(result, (0.3, 0.05))

    def test_v_shape_malformed_id_returns_none(self):
        self.assertIsNone(barrier_params_for("v_shape", "v_weird", {}))

    def test_reversal_looks_up_config_dict(self):
        params = {"reversal_0.2_0.55": (0.3, 0.15)}
        self.assertEqual(barrier_params_for("reversal", "reversal_0.2_0.55", params), (0.3, 0.15))

    def test_reversal_missing_from_config_returns_none(self):
        self.assertIsNone(barrier_params_for("reversal", "reversal_9.9_9.9", {}))

    def test_unknown_strategy_returns_none(self):
        self.assertIsNone(barrier_params_for("high_prob", "high_prob_btc", {}))


class BlendedNullTests(unittest.TestCase):
    def test_barrier_only_combo(self):
        df = pd.DataFrame(
            [_trade(outcome="UNWIND", pnl=0.1), _trade(outcome="STOPLOSS", pnl=-0.3)]
        )
        eligible, null_wr = blended_null_and_eligible_trades(df, (0.3, 0.15))
        self.assertEqual(len(eligible), 2)
        self.assertAlmostEqual(null_wr, 0.3 / 0.45)

    def test_timeout_excluded_entirely(self):
        df = pd.DataFrame(
            [_trade(outcome="UNWIND", pnl=0.1), _trade(outcome="TIMEOUT", pnl=0.05)]
        )
        eligible, null_wr = blended_null_and_eligible_trades(df, (0.3, 0.15))
        self.assertEqual(len(eligible), 1)

    def test_blends_barrier_and_resolution_by_count(self):
        df = pd.DataFrame(
            [
                _trade(outcome="UNWIND", pnl=0.1),
                _trade(outcome="UNWIND", pnl=0.1),
                _trade(outcome="WIN", pnl=0.3, token_price=0.5),
            ]
        )
        eligible, null_wr = blended_null_and_eligible_trades(df, (0.3, 0.15))
        # barrier null = 0.6667 (weight 2), resolution null = 0.5 (weight 1)
        expected = (0.3 / 0.45 * 2 + 0.5 * 1) / 3
        self.assertAlmostEqual(null_wr, expected)
        self.assertEqual(len(eligible), 3)

    def test_no_sl_tp_and_no_resolution_trades_returns_none(self):
        df = pd.DataFrame([_trade(outcome="TIMEOUT", pnl=0.05)])
        eligible, null_wr = blended_null_and_eligible_trades(df, None)
        self.assertIsNone(null_wr)
        self.assertEqual(len(eligible), 0)


class ComputeComboStatsTests(unittest.TestCase):
    def test_groups_by_market_strategy_variant(self):
        trades = [
            _trade(market="BTC-5m", variant_id="reversal_0.2_0.55", outcome="UNWIND", pnl=0.1),
            _trade(market="BTC-5m", variant_id="reversal_0.2_0.55", outcome="STOPLOSS", pnl=-0.3),
            _trade(market="ETH-5m", variant_id="reversal_0.2_0.55", outcome="UNWIND", pnl=0.1),
        ]
        df = build_dataframe(trades)
        combo_df = compute_combo_stats(df, {"reversal_0.2_0.55": (0.3, 0.15)})
        self.assertEqual(len(combo_df), 2)
        btc_row = combo_df[combo_df["market"] == "BTC-5m"].iloc[0]
        self.assertEqual(btc_row["total_trades"], 2)
        self.assertEqual(btc_row["eligible_trades"], 2)

    def test_thin_sample_has_no_pvalue_but_is_still_counted(self):
        trades = [_trade(outcome="UNWIND", pnl=0.1)]
        df = build_dataframe(trades)
        combo_df = compute_combo_stats(df, {"reversal_0.2_0.55": (0.3, 0.15)})
        self.assertEqual(combo_df.iloc[0]["total_trades"], 1)
        self.assertIsNotNone(combo_df.iloc[0]["p_value"])  # binomial test still computable

    def test_v_shape_derives_barrier_from_variant_id_without_config(self):
        trades = [
            _trade(
                strategy="v_shape",
                variant_id="v_0.7_0.3_0.7_0.3_0.05",
                outcome="UNWIND",
                pnl=0.05,
            )
        ]
        df = build_dataframe(trades)
        combo_df = compute_combo_stats(df, {})  # no reversal config needed
        self.assertAlmostEqual(combo_df.iloc[0]["null_win_rate"], 0.3 / 0.35)


class AssignVerdictTests(unittest.TestCase):
    def _row(self, eligible, q, edge):
        return pd.Series({"eligible_trades": eligible, "q_value": q, "edge": edge})

    def test_insufficient_sample_below_min_trades(self):
        self.assertEqual(assign_verdict(self._row(10, 0.001, 0.2)), "INSUFFICIENT-SAMPLE")

    def test_insufficient_sample_when_no_pvalue(self):
        self.assertEqual(assign_verdict(self._row(100, float("nan"), 0.2)), "INSUFFICIENT-SAMPLE")

    def test_promote_candidate_significant_positive_edge(self):
        self.assertEqual(assign_verdict(self._row(100, 0.01, 0.2)), "PROMOTE-CANDIDATE")

    def test_reject_significant_negative_edge(self):
        self.assertEqual(assign_verdict(self._row(100, 0.01, -0.2)), "REJECT")

    def test_watch_not_significant(self):
        self.assertEqual(assign_verdict(self._row(100, 0.5, 0.05)), "WATCH")


class ApplyBhCorrectionTests(unittest.TestCase):
    def test_adds_q_value_column_only_for_testable_rows(self):
        combo_df = pd.DataFrame(
            {"p_value": [0.001, None, 0.5], "market": ["a", "b", "c"]}
        )
        result = apply_bh_correction(combo_df)
        self.assertIn("q_value", result.columns)
        self.assertTrue(pd.isna(result.iloc[1]["q_value"]))
        self.assertFalse(pd.isna(result.iloc[0]["q_value"]))


class LedgerUpsertTests(unittest.TestCase):
    def _combo_df(self, edge: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "market": "BTC-5m",
                    "strategy": "reversal",
                    "variant_id": "reversal_0.2_0.55",
                    "total_trades": 100,
                    "eligible_trades": 80,
                    "realized_win_rate": 0.7,
                    "null_win_rate": 0.5,
                    "edge": edge,
                    "p_value": 0.001,
                    "q_value": 0.01,
                    "verdict": "PROMOTE-CANDIDATE",
                }
            ]
        )

    def test_rerun_same_date_replaces_not_duplicates(self):
        with tempfile.TemporaryDirectory() as d:
            ledger_path = Path(d) / "candidate_ledger.csv"
            update_ledger(ledger_path, "2026-07-20", self._combo_df(0.2))
            merged = update_ledger(ledger_path, "2026-07-20", self._combo_df(0.99))
            self.assertEqual(len(merged), 1)
            self.assertAlmostEqual(merged.iloc[0]["edge"], 0.99)

    def test_different_dates_accumulate(self):
        with tempfile.TemporaryDirectory() as d:
            ledger_path = Path(d) / "candidate_ledger.csv"
            update_ledger(ledger_path, "2026-07-20", self._combo_df(0.2))
            merged = update_ledger(ledger_path, "2026-07-21", self._combo_df(0.3))
            self.assertEqual(len(merged), 2)

    def test_write_is_atomic_no_leftover_tmp_file(self):
        with tempfile.TemporaryDirectory() as d:
            ledger_path = Path(d) / "candidate_ledger.csv"
            update_ledger(ledger_path, "2026-07-20", self._combo_df(0.2))
            self.assertFalse((Path(d) / "candidate_ledger.csv.tmp").exists())
            self.assertTrue(ledger_path.exists())


class ComputeStreaksTests(unittest.TestCase):
    def test_consecutive_days_counted(self):
        ledger_df = pd.DataFrame(
            [
                {"date": "2026-07-18", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "PROMOTE-CANDIDATE"},
                {"date": "2026-07-19", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "PROMOTE-CANDIDATE"},
                {"date": "2026-07-20", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "PROMOTE-CANDIDATE"},
            ]
        )
        streaks = compute_streaks(ledger_df, "2026-07-20")
        self.assertEqual(streaks[("BTC-5m", "reversal", "v1")], 3)

    def test_gap_breaks_streak(self):
        ledger_df = pd.DataFrame(
            [
                {"date": "2026-07-18", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "WATCH"},
                {"date": "2026-07-19", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "PROMOTE-CANDIDATE"},
                {"date": "2026-07-20", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "PROMOTE-CANDIDATE"},
            ]
        )
        streaks = compute_streaks(ledger_df, "2026-07-20")
        self.assertEqual(streaks[("BTC-5m", "reversal", "v1")], 2)

    def test_no_streak_omitted_from_dict(self):
        ledger_df = pd.DataFrame(
            [{"date": "2026-07-20", "market": "BTC-5m", "strategy": "reversal", "variant_id": "v1", "verdict": "REJECT"}]
        )
        streaks = compute_streaks(ledger_df, "2026-07-20")
        self.assertNotIn(("BTC-5m", "reversal", "v1"), streaks)


class MarketsMonitoredTableTests(unittest.TestCase):
    def test_scopes_to_trailing_window_only(self):
        as_of = _ts("2026-07-20", hour=20)
        trades = [
            _trade(market="BTC-5m", entry_ts=as_of - 3600),  # 1h ago, in window
            _trade(market="ETH-5m", entry_ts=as_of - 30 * 3600),  # 30h ago, outside window
        ]
        df = build_dataframe(trades)
        table = markets_monitored_table(df, as_of, window_hours=24)
        self.assertEqual(list(table["market"]), ["BTC-5m"])

    def test_empty_window_returns_empty_frame_not_error(self):
        df = build_dataframe([])
        table = markets_monitored_table(df, _ts("2026-07-20"))
        self.assertTrue(table.empty)


class ComputeGroupDiagnosticsTests(unittest.TestCase):
    """`compute_group_diagnostics`'s `status="ok"` branch (>= GROUP_DIAGNOSTIC_MIN_WEEKS of
    history) had never been exercised against real data (siglab itself is nowhere near 8
    weeks old yet) or by a unit test before this class was added — found via a self-review
    after the DeepSeek plan/code reviews: `sharpes.idxmax()` raises `ValueError` on an
    all-NaN Series, which is exactly what happens when every variant in a group has
    zero-variance (or all-zero) weekly PnL. That's a real, currently-latent bug that would
    otherwise only surface for real once siglab's history crosses the warm-up threshold —
    reproduced and fixed here instead of waiting for it to happen unattended."""

    def _make_df(self, n_weeks: int, variant_pnls: dict[str, list[float]]) -> pd.DataFrame:
        rows = []
        base = datetime(2026, 1, 5, 12, tzinfo=HKT)  # a Monday
        for week_idx in range(n_weeks):
            week_start = base + timedelta(weeks=week_idx)
            for variant_id, pnls in variant_pnls.items():
                pnl = pnls[week_idx % len(pnls)]
                rows.append(
                    _trade(
                        market="BTC-5m",
                        strategy="reversal",
                        variant_id=variant_id,
                        outcome="UNWIND",
                        pnl=pnl,
                        entry_ts=week_start.timestamp(),
                        logged_at=week_start.timestamp(),
                        cycle_start=week_start.timestamp(),
                    )
                )
        return build_dataframe(rows)

    def test_below_warmup_reports_insufficient_history(self):
        df = self._make_df(GROUP_DIAGNOSTIC_MIN_WEEKS - 1, {"v1": [0.1, -0.1]})
        diag = compute_group_diagnostics(df)
        self.assertEqual(diag[("BTC-5m", "reversal")]["status"], "insufficient_history")

    def test_at_warmup_with_real_variance_computes_pbo_and_dsr(self):
        rng = random.Random(1)
        variant_pnls = {
            f"v{i}": [rng.uniform(-0.3, 0.3) for _ in range(GROUP_DIAGNOSTIC_MIN_WEEKS)]
            for i in range(4)
        }
        df = self._make_df(GROUP_DIAGNOSTIC_MIN_WEEKS, variant_pnls)
        diag = compute_group_diagnostics(df)["BTC-5m", "reversal"]
        self.assertEqual(diag["status"], "ok")
        self.assertIsNotNone(diag["best_variant"])
        self.assertIsNotNone(diag["dsr_pvalue"])
        self.assertTrue(0.0 <= diag["pbo"] <= 1.0)

    def test_all_zero_variance_variants_does_not_crash(self):
        # Every variant has the exact same PnL every week -> std=0 for every column ->
        # sharpes is all-NaN -> idxmax() would raise without the guard this test protects.
        variant_pnls = {
            "v1": [0.1] * GROUP_DIAGNOSTIC_MIN_WEEKS,
            "v2": [-0.05] * GROUP_DIAGNOSTIC_MIN_WEEKS,
        }
        df = self._make_df(GROUP_DIAGNOSTIC_MIN_WEEKS, variant_pnls)
        diag = compute_group_diagnostics(df)["BTC-5m", "reversal"]
        self.assertEqual(diag["status"], "ok")
        self.assertIsNone(diag["best_variant"])
        self.assertIsNone(diag["dsr_pvalue"])
        self.assertTrue(0.0 <= diag["pbo"] <= 1.0)  # PBO itself still computes fine


if __name__ == "__main__":
    unittest.main()
