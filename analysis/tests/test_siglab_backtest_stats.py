"""Unit tests for siglab_backtest_stats.py.

Run with: python3 -m unittest discover -s analysis/tests -v
(no pytest available in this environment — see analysis/README.md)

Includes a known-ground-truth sanity check for pbo_cscv (pure-noise panel -> PBO ~0.5)
mirroring ../btc_5mins/scripts/backtest_stats_poc.py's own validation approach — this port
must be checked against a case with a known answer before it's trusted to compute a real
verdict, per doc/plan_better_signal_2026-07-24.md's "Revision after DeepSeek review" (§13).
"""

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from siglab_backtest_stats import (  # noqa: E402
    benjamini_hochberg,
    binomial_test_win_rate,
    daily_pnl_panel,
    deflated_sharpe_ratio,
    null_win_rate_barrier,
    null_win_rate_market_implied,
    pbo_cscv,
)


class NullWinRateBarrierTests(unittest.TestCase):
    def test_exact_formula(self):
        # sl=0.3, tp=0.15 -> null = 0.3 / 0.45 = 0.6667
        self.assertAlmostEqual(null_win_rate_barrier(0.3, 0.15), 0.3 / 0.45)

    def test_none_when_not_a_real_barrier(self):
        self.assertIsNone(null_win_rate_barrier(None, 0.15))
        self.assertIsNone(null_win_rate_barrier(0.0, 0.15))
        self.assertIsNone(null_win_rate_barrier(0.3, 0.0))
        self.assertIsNone(null_win_rate_barrier(-0.1, 0.15))


class NullWinRateMarketImpliedTests(unittest.TestCase):
    def test_mean_of_entry_prices(self):
        self.assertAlmostEqual(null_win_rate_market_implied([0.6, 0.7, 0.8]), 0.7)

    def test_none_when_empty(self):
        self.assertIsNone(null_win_rate_market_implied([]))

    def test_drops_nan(self):
        self.assertAlmostEqual(null_win_rate_market_implied([0.5, float("nan"), 0.9]), 0.7)


class BinomialTestWinRateTests(unittest.TestCase):
    def test_exact_match_to_null_is_not_significant(self):
        # 50 wins out of 100 against null=0.5: right at the null, p should be ~1.
        result = binomial_test_win_rate(50, 100, 0.5)
        self.assertGreater(result["p_value"], 0.5)
        self.assertAlmostEqual(result["edge"], 0.0)

    def test_large_deviation_is_significant(self):
        # 80 wins out of 100 against null=0.5 is a large, clearly significant edge.
        result = binomial_test_win_rate(80, 100, 0.5)
        self.assertLess(result["p_value"], 0.01)
        self.assertAlmostEqual(result["realized_win_rate"], 0.8)
        self.assertAlmostEqual(result["edge"], 0.3)

    def test_thin_sample_is_not_significant_even_with_100pct_win_rate(self):
        # 3 wins out of 3 "looks" perfect but is not distinguishable from a 0.6-weighted
        # coin at conventional significance — this is exactly the thin-sample trap the
        # verdict rubric's min-trade-count bar exists to catch.
        result = binomial_test_win_rate(3, 3, 0.6)
        self.assertGreater(result["p_value"], 0.05)

    def test_rejects_invalid_inputs(self):
        with self.assertRaises(ValueError):
            binomial_test_win_rate(1, 0, 0.5)
        with self.assertRaises(ValueError):
            binomial_test_win_rate(1, 10, 0.0)
        with self.assertRaises(ValueError):
            binomial_test_win_rate(1, 10, 1.0)


class BenjaminiHochbergTests(unittest.TestCase):
    def test_empty_input(self):
        result = benjamini_hochberg([])
        self.assertEqual(len(result["q_values"]), 0)
        self.assertEqual(len(result["reject"]), 0)

    def test_all_null_pvalues_mostly_survive_at_low_alpha(self):
        # 100 uniform(0,1) p-values under the null: BH-FDR at alpha=0.05 should reject
        # only a small fraction (not zero, not all) — a basic calibration check.
        rng = np.random.default_rng(42)
        pvals = rng.uniform(0, 1, size=200)
        result = benjamini_hochberg(list(pvals), alpha=0.05)
        reject_fraction = result["reject"].mean()
        self.assertLess(reject_fraction, 0.15, "BH should reject well under 15% of pure noise")

    def test_strong_signal_survives_correction(self):
        pvals = [0.0001, 0.5, 0.6, 0.7, 0.8, 0.9]
        result = benjamini_hochberg(pvals, alpha=0.05)
        self.assertTrue(result["reject"][0])
        self.assertFalse(any(result["reject"][1:]))

    def test_q_values_are_monotone_nondecreasing_in_sorted_pvalue_order(self):
        pvals = [0.2, 0.01, 0.5, 0.001, 0.3]
        result = benjamini_hochberg(pvals, alpha=0.05)
        order = np.argsort(pvals)
        q_sorted = result["q_values"][order]
        self.assertTrue(np.all(np.diff(q_sorted) >= -1e-12))


class DailyPnlPanelTests(unittest.TestCase):
    def test_zero_fills_missing_days_not_nan(self):
        df0 = pd.DataFrame({"day": ["2026-07-13", "2026-07-14"], "pnl": [1.0, 2.0]})
        df1 = pd.DataFrame({"day": ["2026-07-14"], "pnl": [5.0]})
        panel = daily_pnl_panel([df0, df1])
        self.assertEqual(panel.loc["2026-07-13", 1], 0.0)
        self.assertEqual(panel.loc["2026-07-13", 0], 1.0)
        self.assertEqual(panel.loc["2026-07-14", 1], 5.0)

    def test_empty_combo_produces_all_zero_column(self):
        df0 = pd.DataFrame({"day": ["2026-07-13"], "pnl": [1.0]})
        empty = pd.DataFrame({"day": [], "pnl": []})
        panel = daily_pnl_panel([df0, empty])
        self.assertTrue((panel[1] == 0.0).all())

    def test_sums_multiple_trades_same_day(self):
        df0 = pd.DataFrame({"day": ["2026-07-13", "2026-07-13"], "pnl": [1.0, 2.0]})
        panel = daily_pnl_panel([df0])
        self.assertEqual(panel.loc["2026-07-13", 0], 3.0)


class PboCscvGroundTruthTests(unittest.TestCase):
    """Validates the port against a known answer: a panel of pure noise columns must
    produce PBO close to the theoretical 0.5 (picking the best-in-sample combo is worth no
    more than a coin flip out-of-sample, by construction, when there's no real edge
    anywhere in the panel). Averaged over multiple random draws since a single draw is
    itself noisy at these panel sizes — same caveat the source repo's own POC documents."""

    def test_pure_noise_panel_averages_near_half(self):
        rng = np.random.default_rng(7)
        T, N = 120, 30
        pbos = []
        for _ in range(15):
            panel = pd.DataFrame(rng.normal(0, 1, size=(T, N)))
            result = pbo_cscv(panel, n_splits=8)
            pbos.append(result["pbo"])
        mean_pbo = float(np.mean(pbos))
        self.assertAlmostEqual(mean_pbo, 0.5, delta=0.15)

    def test_rejects_odd_n_splits(self):
        panel = pd.DataFrame(np.random.default_rng(0).normal(size=(20, 5)))
        with self.assertRaises(ValueError):
            pbo_cscv(panel, n_splits=7)

    def test_generic_and_fast_paths_agree_on_pbo(self):
        rng = np.random.default_rng(3)
        panel = pd.DataFrame(rng.normal(0, 1, size=(40, 10)))

        def sharpe_metric(col: pd.Series) -> float:
            std = col.std(ddof=1)
            return 0.0 if std == 0 or math.isnan(std) else col.mean() / std

        fast = pbo_cscv(panel, n_splits=8)
        generic = pbo_cscv(panel, n_splits=8, metric=sharpe_metric)
        self.assertAlmostEqual(fast["pbo"], generic["pbo"], delta=0.05)


class DeflatedSharpeRatioTests(unittest.TestCase):
    def test_zero_trial_pool_variance_and_matching_sharpe_is_not_significant(self):
        result = deflated_sharpe_ratio(
            sharpe_hat=0.1, n_trials=1, trial_sharpe_var=0.0, n_obs=30
        )
        self.assertEqual(result["expected_max_sharpe_null"], 0.0)

    def test_more_trials_raises_the_significance_bar(self):
        kwargs = dict(sharpe_hat=1.0, trial_sharpe_var=0.25, n_obs=60)
        few_trials = deflated_sharpe_ratio(n_trials=2, **kwargs)
        many_trials = deflated_sharpe_ratio(n_trials=1000, **kwargs)
        self.assertGreater(
            many_trials["expected_max_sharpe_null"], few_trials["expected_max_sharpe_null"]
        )
        self.assertLess(many_trials["dsr_zscore"], few_trials["dsr_zscore"])


if __name__ == "__main__":
    unittest.main()
