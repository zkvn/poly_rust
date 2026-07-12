#!/usr/bin/env python3
"""Unit tests for the backtest-reconciliation pieces of trade_reconcile.py
(Live vs BT / BT vs Live — trader/doc/feature_bt_recon_2026-07-10.md).

Only the new pure-logic functions are tested directly (CSV parsing, window
filtering, classification, config parsing). The subprocess-calling
orchestration (`run_backtest_reconciliation`) is tested with the Rust
binary and `build_backtest_prices.py` mocked out — these tests never shell
out to `cargo`/the compiled binary and never touch `trader/live_logs` or
the live trading process.

No new dependency beyond what trade_reconcile.py itself already needs
(rich, python-dotenv) — run with the same interpreter as the cron job:
    /home/kev/apps/btc_5mins/venv/bin/python trader/scripts/test_trade_reconcile.py
"""
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_PATH = Path(__file__).resolve().parent / "trade_reconcile.py"


def _load_module():
    """Import trade_reconcile.py by path — standalone script, no package to
    hang an import path off of (mirrors scripts/test_deploy_oracle.py)."""
    spec = importlib.util.spec_from_file_location("trade_reconcile", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


class SlugCycleTsTests(unittest.TestCase):
    def test_extracts_trailing_timestamp(self):
        self.assertEqual(mod.slug_cycle_ts("eth-updown-5m-1783046100"), 1783046100.0)

    def test_malformed_slug_returns_zero(self):
        self.assertEqual(mod.slug_cycle_ts("not-a-slug-"), 0.0)
        self.assertEqual(mod.slug_cycle_ts(""), 0.0)


class ParseBacktestCsvTests(unittest.TestCase):
    def test_parses_rows_and_tags_asset(self):
        csv_text = (
            "slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts\n"
            "eth-updown-5m-1783046100,high_prob,UP,0.930000,1.000000,WIN,0.075300,1783046105.000\n"
        )
        rows = mod.parse_backtest_csv(csv_text, "ETH")
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r["asset"], "ETH")
        self.assertEqual(r["slug"], "eth-updown-5m-1783046100")
        self.assertEqual(r["side"], "UP")
        self.assertEqual(r["outcome"], "WIN")
        self.assertAlmostEqual(r["pnl"], 0.0753)
        self.assertEqual(r["cycle_ts"], 1783046100.0)
        self.assertAlmostEqual(r["entry_ts"], 1783046105.0)
        self.assertAlmostEqual(r["token_price"], 0.93)
        self.assertAlmostEqual(r["exit_price"], 1.0)

    def test_header_only_csv_yields_no_rows(self):
        csv_text = "slug,strategy,side,token_price,exit_price,outcome,pnl,entry_ts\n"
        self.assertEqual(mod.parse_backtest_csv(csv_text, "ETH"), [])

    def test_missing_entry_ts_column_defaults_to_zero(self):
        """Older backtest binaries (pre entry_ts column) must not crash the parse."""
        csv_text = (
            "slug,strategy,side,token_price,exit_price,outcome,pnl\n"
            "eth-updown-5m-1783046100,high_prob,UP,0.930000,1.000000,WIN,0.075300\n"
        )
        rows = mod.parse_backtest_csv(csv_text, "ETH")
        self.assertEqual(rows[0]["entry_ts"], 0.0)


class TMinusStrTests(unittest.TestCase):
    def test_computes_seconds_before_cycle_close(self):
        # 5-min cycle from ts=1000 closes at ts=1300; entry at ts=1291 -> T-9s
        self.assertEqual(mod.t_minus_str("eth-updown-5m-1000", 1291.0), "T-9s")

    def test_missing_or_zero_entry_ts_returns_placeholder(self):
        self.assertEqual(mod.t_minus_str("eth-updown-5m-1000", None), "—")
        self.assertEqual(mod.t_minus_str("eth-updown-5m-1000", 0.0), "—")

    def test_entry_after_cycle_close_shows_t_plus(self):
        self.assertEqual(mod.t_minus_str("eth-updown-5m-1000", 1310.0), "T+10s")


class PctChangeTests(unittest.TestCase):
    def test_computes_percent_change(self):
        self.assertAlmostEqual(mod._pct_change(0.5, 0.6), 20.0)

    def test_none_when_base_missing_or_zero(self):
        self.assertIsNone(mod._pct_change(None, 0.6))
        self.assertIsNone(mod._pct_change(0.0, 0.6))
        self.assertIsNone(mod._pct_change(None, None))


class FmtHelpersTests(unittest.TestCase):
    def test_fmt_pct(self):
        self.assertEqual(mod._fmt_pct(7.53), "+7.5%")
        self.assertEqual(mod._fmt_pct(-3.2), "-3.2%")
        self.assertEqual(mod._fmt_pct(None), "—")

    def test_fmt_price(self):
        self.assertEqual(mod._fmt_price(0.93), "0.9300")
        self.assertEqual(mod._fmt_price(None), "—")


class LoadCycleOpenPricesTests(unittest.TestCase):
    def test_reads_earliest_tick_per_slug(self):
        import pandas as pd
        with tempfile.TemporaryDirectory() as d:
            prices_dir = Path(d)
            df = pd.DataFrame({
                "ts": [1005.0, 1000.0, 1002.0],
                "up": [0.60, 0.50, 0.55],
                "dn": [0.40, 0.50, 0.45],
                "slug": ["eth-updown-5m-1000"] * 3,
            })
            df.to_parquet(prices_dir / "ETH_poly_2026-07-11.parquet", index=False)
            out = mod.load_cycle_open_prices(["ETH"], ["2026-07-11"], prices_dir)
        self.assertEqual(out["eth-updown-5m-1000"], {"UP": 0.50, "DOWN": 0.50})

    def test_missing_file_is_skipped_not_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            out = mod.load_cycle_open_prices(["ETH"], ["2026-07-11"], Path(d))
        self.assertEqual(out, {})

    def test_stale_echo_tick_from_previous_cycle_is_skipped(self):
        """Regression test for the 2026-07-12 +15000% Entry Delta bug:
        doge-updown-5m-1783785600's first recorded tick was an exact
        carry-over of the prior cycle's near-resolution price (dn=0.005),
        relabeled with the new slug 0.2s before the feed caught up to the
        real ~50/50 open (dn=0.405). The echo must be skipped so "cycle
        open" reflects the genuine open, not the echo."""
        import pandas as pd
        with tempfile.TemporaryDirectory() as d:
            prices_dir = Path(d)
            df = pd.DataFrame({
                "ts": [1000.0, 1001.0, 1003.8, 1004.0, 1004.2],
                "up": [0.995, 0.995, 0.995, 0.595, 0.595],
                "dn": [0.005, 0.005, 0.005, 0.405, 0.405],
                "slug": ["doge-updown-5m-700", "doge-updown-5m-700",
                         "doge-updown-5m-1000", "doge-updown-5m-1000", "doge-updown-5m-1000"],
            })
            df.to_parquet(prices_dir / "DOGE_poly_2026-07-12.parquet", index=False)
            out = mod.load_cycle_open_prices(["DOGE"], ["2026-07-12"], prices_dir)
        self.assertEqual(out["doge-updown-5m-1000"], {"UP": 0.595, "DOWN": 0.405})
        # the earlier cycle's own genuine open (not itself an echo) is untouched
        self.assertEqual(out["doge-updown-5m-700"], {"UP": 0.995, "DOWN": 0.005})

    def test_slug_with_only_a_single_echoed_tick_drops_out_cleanly(self):
        """At most one leading row per slug can ever be flagged as an echo
        (only the first row of a group can differ-slug-from-previous), so
        the only way a slug disappears entirely from the output is if it
        has exactly one recorded tick and that tick is an echo. Must
        degrade to "missing from the dict" (→ "—" in the report), not
        crash groupby on an empty group."""
        import pandas as pd
        with tempfile.TemporaryDirectory() as d:
            prices_dir = Path(d)
            df = pd.DataFrame({
                "ts": [1000.0, 1003.0],
                "up": [0.50, 0.50],
                "dn": [0.50, 0.50],
                "slug": ["eth-updown-5m-700", "eth-updown-5m-1000"],
            })
            df.to_parquet(prices_dir / "ETH_poly_2026-07-12.parquet", index=False)
            out = mod.load_cycle_open_prices(["ETH"], ["2026-07-12"], prices_dir)
        self.assertNotIn("eth-updown-5m-1000", out)
        self.assertEqual(out["eth-updown-5m-700"], {"UP": 0.50, "DOWN": 0.50})


class SafeLoadCycleOpenPricesTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        with patch.object(mod, "load_cycle_open_prices", side_effect=RuntimeError("boom")):
            out = mod._safe_load_cycle_open_prices(["ETH"], ["2026-07-11"], Path("/tmp"))
        self.assertEqual(out, {})


class FilterBtRowsToWindowTests(unittest.TestCase):
    def test_keeps_only_rows_in_half_open_window(self):
        rows = [
            {"cycle_ts": 99.0}, {"cycle_ts": 100.0}, {"cycle_ts": 150.0}, {"cycle_ts": 200.0},
        ]
        out = mod.filter_bt_rows_to_window(rows, 100.0, 200.0)
        self.assertEqual([r["cycle_ts"] for r in out], [100.0, 150.0])


class ResolveTradeAssetsTests(unittest.TestCase):
    def test_reads_lexicographically_last_config_file(self):
        with tempfile.TemporaryDirectory() as d:
            config_dir = Path(d)
            (config_dir / "strategy_20260705.toml").write_text('trade_assets = ["BTC"]\n')
            (config_dir / "strategy_20260709.toml").write_text('trade_assets = ["BTC", "ETH", "DOGE"]\n')
            self.assertEqual(mod.resolve_trade_assets(config_dir), ["BTC", "ETH", "DOGE"])

    def test_raises_when_no_config_files(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                mod.resolve_trade_assets(Path(d))


class NormalizeLiveRowsTests(unittest.TestCase):
    def test_coerces_pnl_and_uppercases_side_outcome(self):
        raw = [{
            "logged_at": "1783046100.0", "asset": "ETH", "slug": "eth-updown-5m-1783046100",
            "strategy": "high_prob", "side": "up", "outcome": "win", "pnl": "0.0753",
        }]
        out = mod._normalize_live_rows(raw)
        self.assertEqual(out[0]["side"], "UP")
        self.assertEqual(out[0]["outcome"], "WIN")
        self.assertAlmostEqual(out[0]["pnl"], 0.0753)


class BuildLiveVsBtTests(unittest.TestCase):
    """Classification table: one row per live trade vs. what the backtest
    did at the same cycle."""

    def _live(self, slug="eth-updown-5m-1783046100", side="UP", outcome="WIN", pnl=0.1, asset="ETH"):
        return {"time": "2026-07-09 22:00:00", "asset": asset, "slug": slug,
                "strategy": "high_prob", "side": side, "outcome": outcome, "pnl": pnl}

    def _bt(self, slug="eth-updown-5m-1783046100", side="UP", outcome="WIN", pnl=0.1, asset="ETH"):
        return {"asset": asset, "slug": slug, "strategy": "high_prob", "side": side,
                "outcome": outcome, "pnl": pnl, "cycle_ts": 1783046100.0}

    def test_match_when_same_slug_side_and_outcome(self):
        table, summary = mod.build_live_vs_bt([self._live()], [self._bt()], {"ETH"})
        self.assertEqual(table[0]["status"], "MATCH")
        self.assertEqual(summary["n_match"], 1)
        self.assertEqual(summary["n_outcome_diff"], 0)

    def test_outcome_diff_when_same_slug_side_different_outcome(self):
        live = self._live(outcome="WIN", pnl=0.0753)
        bt = self._bt(outcome="UNWIND", pnl=0.0326)
        table, summary = mod.build_live_vs_bt([live], [bt], {"ETH"})
        self.assertIn("OUTCOME DIFF", table[0]["status"])
        self.assertEqual(summary["n_outcome_diff"], 1)
        self.assertAlmostEqual(table[0]["diff_pnl"], 0.0753 - 0.0326)

    def test_side_diff_when_bt_fired_opposite_side(self):
        live = self._live(side="UP")
        bt = self._bt(side="DOWN")
        table, summary = mod.build_live_vs_bt([live], [bt], {"ETH"})
        self.assertIn("SIDE DIFF", table[0]["status"])
        self.assertEqual(summary["n_side_diff"], 1)

    def test_bt_did_not_fire_when_asset_has_data_but_no_matching_cycle(self):
        table, summary = mod.build_live_vs_bt([self._live()], [], {"ETH"})
        self.assertEqual(table[0]["status"], "BT DID NOT FIRE")
        self.assertEqual(summary["n_not_fired"], 1)
        self.assertEqual(summary["n_no_data"], 0)

    def test_no_price_data_when_asset_missing_from_data_set(self):
        table, summary = mod.build_live_vs_bt([self._live(asset="SOL")], [], {"ETH"})
        self.assertEqual(table[0]["status"], "NO PRICE DATA")
        self.assertEqual(summary["n_no_data"], 1)
        self.assertEqual(summary["n_not_fired"], 0)

    def test_pnl_totals_only_sum_matched_bt_rows(self):
        live = [self._live(slug="a", pnl=0.1), self._live(slug="b", pnl=0.2)]
        bt = [self._bt(slug="a", pnl=0.05)]  # "b" has no bt counterpart
        _, summary = mod.build_live_vs_bt(live, bt, {"ETH"})
        self.assertAlmostEqual(summary["total_live_pnl"], 0.3)
        self.assertAlmostEqual(summary["total_bt_pnl"], 0.05)

    def test_entry_exit_and_delta_columns_computed_from_live_row(self):
        live = self._live(slug="eth-updown-5m-1000")
        live["entry_ts"] = 1291.0  # T-9s
        live["token_price"] = 0.93
        live["exit_price"] = 1.0
        cycle_open_prices = {"eth-updown-5m-1000": {"UP": 0.50, "DOWN": 0.50}}
        table, _ = mod.build_live_vs_bt([live], [self._bt(slug="eth-updown-5m-1000")], {"ETH"},
                                         cycle_open_prices)
        r = table[0]
        self.assertEqual(r["entry_time"], "T-9s")
        self.assertAlmostEqual(r["entry_price"], 0.93)
        self.assertAlmostEqual(r["exit_price"], 1.0)
        self.assertAlmostEqual(r["cycle_delta_pct"], (1.0 - 0.93) / 0.93 * 100)
        self.assertAlmostEqual(r["entry_delta_pct"], (0.93 - 0.50) / 0.50 * 100)

    def test_entry_delta_is_none_without_cycle_open_price_data(self):
        live = self._live()
        live["token_price"] = 0.93
        table, _ = mod.build_live_vs_bt([live], [self._bt()], {"ETH"})
        self.assertIsNone(table[0]["entry_delta_pct"])


class BuildBtVsLiveTests(unittest.TestCase):
    """Cycles the backtest fired but live never traded (either side)."""

    def test_bt_only_cycle_is_reported_as_missed(self):
        bt = [{"asset": "ETH", "slug": "eth-updown-5m-1", "strategy": "high_prob",
               "side": "UP", "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1.0}]
        missed = mod.build_bt_vs_live(bt, live_rows=[])
        self.assertEqual(len(missed), 1)
        self.assertEqual(missed[0]["asset"], "ETH")
        self.assertAlmostEqual(missed[0]["pnl"], 0.3)

    def test_cycle_live_traded_same_side_is_not_missed(self):
        bt = [{"asset": "ETH", "slug": "s1", "strategy": "high_prob", "side": "UP",
               "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1.0}]
        live = [{"slug": "s1", "side": "UP"}]
        self.assertEqual(mod.build_bt_vs_live(bt, live), [])

    def test_cycle_live_traded_opposite_side_is_not_double_counted_as_missed(self):
        """A live trade on the opposite side already shows up as SIDE DIFF in
        the Live vs BT table — it must not also appear here as 'missed',
        or the same discrepancy would be reported twice under two different
        labels."""
        bt = [{"asset": "ETH", "slug": "s1", "strategy": "high_prob", "side": "UP",
               "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1.0}]
        live = [{"slug": "s1", "side": "DOWN"}]
        self.assertEqual(mod.build_bt_vs_live(bt, live), [])

    def test_entry_exit_and_delta_columns_computed_from_bt_row(self):
        bt = [{"asset": "ETH", "slug": "eth-updown-5m-1000", "strategy": "high_prob",
               "side": "UP", "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1000.0,
               "entry_ts": 1291.0, "token_price": 0.93, "exit_price": 1.0}]
        cycle_open_prices = {"eth-updown-5m-1000": {"UP": 0.50, "DOWN": 0.50}}
        missed = mod.build_bt_vs_live(bt, live_rows=[], cycle_open_prices=cycle_open_prices)
        r = missed[0]
        self.assertEqual(r["entry_time"], "T-9s")
        self.assertAlmostEqual(r["entry_price"], 0.93)
        self.assertAlmostEqual(r["exit_price"], 1.0)
        self.assertAlmostEqual(r["cycle_delta_pct"], (1.0 - 0.93) / 0.93 * 100)
        self.assertAlmostEqual(r["entry_delta_pct"], (0.93 - 0.50) / 0.50 * 100)


class SyncPriceFeedFromOracleTests(unittest.TestCase):
    def test_returns_false_when_script_missing(self):
        result = mod.sync_price_feed_from_oracle(Path("/nonexistent/sync_oracle.sh"))
        self.assertFalse(result)

    def test_returns_true_on_success(self):
        with tempfile.NamedTemporaryFile(suffix=".sh") as f:
            script = Path(f.name)
            with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [str(script)], returncode=0, stdout="", stderr="")) as run_mock:
                result = mod.sync_price_feed_from_oracle(script)
            run_mock.assert_called_once()
            self.assertTrue(result)

    def test_returns_false_on_nonzero_exit_without_raising(self):
        with tempfile.NamedTemporaryFile(suffix=".sh") as f:
            script = Path(f.name)
            with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [str(script)], returncode=1, stdout="", stderr="ssh: connect timed out")):
                result = mod.sync_price_feed_from_oracle(script)
            self.assertFalse(result)


class RunBacktestReconciliationTests(unittest.TestCase):
    """Orchestration wrapper — every subprocess/filesystem boundary is
    mocked so these run instantly and never invoke cargo, the compiled
    backtest binary, or anything touching trader/live_logs."""

    def setUp(self):
        self.window_start = mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT)
        self.window_end = mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT)

    def test_returns_none_when_binary_missing(self):
        with patch.object(mod.Path, "exists", return_value=False):
            result = mod.run_backtest_reconciliation(self.window_start, self.window_end, [])
        self.assertIsNone(result)

    def test_skips_one_failing_asset_without_losing_the_others(self):
        def fake_backtest(asset, date, prices_dir, config_dir, binary):
            if asset == "DOGE":
                raise subprocess.CalledProcessError(1, "backtest", stderr="boom")
            return "slug,strategy,side,token_price,exit_price,outcome,pnl\n"

        with patch.object(mod.Path, "exists", return_value=True), \
             patch.object(mod, "resolve_trade_assets", return_value=["ETH", "DOGE"]), \
             patch.object(mod, "sync_price_feed_from_oracle", return_value=True), \
             patch.object(mod, "build_price_data", return_value=True), \
             patch.object(mod, "run_rust_backtest", side_effect=fake_backtest):
            result = mod.run_backtest_reconciliation(self.window_start, self.window_end, [])
        self.assertIsNotNone(result)  # ETH still succeeded even though DOGE raised

    def test_skips_a_date_when_price_build_fails(self):
        with patch.object(mod.Path, "exists", return_value=True), \
             patch.object(mod, "resolve_trade_assets", return_value=["ETH"]), \
             patch.object(mod, "sync_price_feed_from_oracle", return_value=True), \
             patch.object(mod, "build_price_data", return_value=False) as build_mock, \
             patch.object(mod, "run_rust_backtest") as bt_mock:
            result = mod.run_backtest_reconciliation(self.window_start, self.window_end, [])
        build_mock.assert_called()
        bt_mock.assert_not_called()  # never runs the backtest binary for a date with no price data
        self.assertIsNotNone(result)

    def test_sync_failure_does_not_block_the_rest_of_the_pipeline(self):
        """Oracle unreachable (VPN down, SSH failure, etc.) must degrade to
        'use whatever local data already exists', not abort the report."""
        with patch.object(mod.Path, "exists", return_value=True), \
             patch.object(mod, "resolve_trade_assets", return_value=["ETH"]), \
             patch.object(mod, "sync_price_feed_from_oracle", return_value=False) as sync_mock, \
             patch.object(mod, "build_price_data", return_value=True), \
             patch.object(mod, "run_rust_backtest", return_value="slug,strategy,side,token_price,exit_price,outcome,pnl\n"):
            result = mod.run_backtest_reconciliation(self.window_start, self.window_end, [])
        sync_mock.assert_called()
        self.assertIsNotNone(result)

    def test_never_shells_out_to_the_live_binary(self):
        """This feature is backtest-only — it must never invoke the `live`
        binary or anything under trader/live_logs regardless of inputs.
        Exercises both subprocess call sites (build_backtest_prices.py via
        build_price_data, and the backtest binary via run_rust_backtest,
        which is where a stray reference to the live binary would show up)
        without ever actually running cargo or touching real price data."""
        seen_cmds = []

        def spying_run(cmd, *args, **kwargs):
            seen_cmds.append(cmd)
            if kwargs.get("check"):
                # This is the run_rust_backtest call site (uses check=True) —
                # raise so it's caught+skipped like a real backtest failure,
                # without needing real price data on disk.
                raise subprocess.CalledProcessError(1, cmd, stderr="stubbed")
            # This is the build_price_data call site (no check=True) —
            # report success so the flow proceeds to run_rust_backtest too.
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        with patch.object(mod.Path, "exists", return_value=True), \
             patch.object(mod, "resolve_trade_assets", return_value=["ETH"]), \
             patch("subprocess.run", side_effect=spying_run):
            mod.run_backtest_reconciliation(self.window_start, self.window_end, [])

        self.assertTrue(seen_cmds, "expected at least one subprocess.run call to inspect")
        for cmd in seen_cmds:
            joined = " ".join(str(c) for c in cmd)
            self.assertNotIn("live_logs", joined)
            self.assertNotIn("/bin/live", joined)


class SafeRunBacktestReconciliationTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        with patch.object(mod, "run_backtest_reconciliation", side_effect=RuntimeError("boom")):
            result = mod._safe_run_backtest_reconciliation(
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                [],
            )
        self.assertIsNone(result)


class WriteMarkdownSummaryBtSectionTests(unittest.TestCase):
    """The BT Reconciliation section must always render — even on a
    0-live-trade day — since a missed-trade report matters most exactly
    when live traded nothing at all."""

    def test_section_present_on_zero_trade_stub_with_no_bt_result(self):
        with tempfile.TemporaryDirectory() as d:
            path = mod.write_markdown_summary(
                {"direction": {}, "stoploss": {}, "total_rows": 0}, {},
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                Path(d), bt_result=None,
            )
            text = path.read_text()
        self.assertIn("No trades in this window", text)
        self.assertIn("## Backtest Reconciliation", text)

    def test_section_renders_missed_trades_table_when_bt_result_given(self):
        bt_result = (
            [],
            {"n_live": 0, "n_match": 0, "n_outcome_diff": 0, "n_side_diff": 0, "n_not_fired": 0, "n_no_data": 0,
             "total_live_pnl": 0.0, "total_bt_pnl": 0.0},
            [{"time": "2026-07-09 22:00:00", "asset": "ETH", "strategy": "high_prob",
              "side": "UP", "outcome": "WIN", "pnl": 0.3}],
        )
        with tempfile.TemporaryDirectory() as d:
            path = mod.write_markdown_summary(
                {"direction": {}, "stoploss": {}, "total_rows": 0}, {},
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                Path(d), bt_result=bt_result,
            )
            text = path.read_text()
        self.assertIn("1 cycle(s) live missed entirely", text)
        self.assertIn("would-be PnL +0.3000", text)


if __name__ == "__main__":
    unittest.main()
