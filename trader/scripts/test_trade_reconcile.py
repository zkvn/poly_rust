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


class LoadUnderlyingPriceSeriesTests(unittest.TestCase):
    """Regression coverage for incident_delta_pct_2026-07-12.md: Entry Δ%/
    Cycle Δ% must be computed from the underlying (Binance) asset price,
    not the CLOB (Polymarket order-book) probability price."""

    def test_reads_and_sorts_ticks_per_slug(self):
        import pandas as pd
        with tempfile.TemporaryDirectory() as d:
            prices_dir = Path(d)
            df = pd.DataFrame({
                "ts": [1005.0, 1000.0, 1002.0],
                "binance": [1801.0, 1800.0, 1800.5],
                "slug": ["eth-updown-5m-1000"] * 3,
            })
            df.to_parquet(prices_dir / "ETH_binance_2026-07-11.parquet", index=False)
            out = mod.load_underlying_price_series(["ETH"], ["2026-07-11"], prices_dir)
        self.assertEqual(out["eth-updown-5m-1000"],
                          [(1000.0, 1800.0), (1002.0, 1800.5), (1005.0, 1801.0)])

    def test_missing_file_is_skipped_not_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            out = mod.load_underlying_price_series(["ETH"], ["2026-07-11"], Path(d))
        self.assertEqual(out, {})


class SafeLoadUnderlyingPriceSeriesTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        with patch.object(mod, "load_underlying_price_series", side_effect=RuntimeError("boom")):
            out = mod._safe_load_underlying_price_series(["ETH"], ["2026-07-11"], Path("/tmp"))
        self.assertEqual(out, {})


class UnderlyingPriceAtTests(unittest.TestCase):
    def test_returns_nearest_tick_by_absolute_time_delta(self):
        ticks = [(1000.0, 100.0), (1010.0, 110.0), (1020.0, 120.0)]
        self.assertEqual(mod._underlying_price_at(ticks, 1011.0), 110.0)
        self.assertEqual(mod._underlying_price_at(ticks, 1004.0), 100.0)

    def test_none_when_no_ticks_or_no_target_ts(self):
        self.assertIsNone(mod._underlying_price_at([], 1000.0))
        self.assertIsNone(mod._underlying_price_at([(1000.0, 100.0)], None))
        self.assertIsNone(mod._underlying_price_at([(1000.0, 100.0)], 0.0))


class CycleOpenCloseTests(unittest.TestCase):
    def test_returns_first_and_last_tick(self):
        ticks = [(1000.0, 100.0), (1010.0, 110.0), (1020.0, 120.0)]
        self.assertEqual(mod._cycle_open_close(ticks), (100.0, 120.0))

    def test_none_none_when_empty(self):
        self.assertEqual(mod._cycle_open_close([]), (None, None))


class BuildHaltWindowsTests(unittest.TestCase):
    """trader/doc/incident_bt_vs_live_discrepancy_2026-07-12.md's halt-window
    reconstruction, promoted from a one-off investigation script."""

    def _log(self, tmpdir: Path, lines: list) -> Path:
        path = tmpdir / "live.log"
        path.write_text("\n".join(lines) + "\n")
        return path

    def test_balance_drawdown_halt_closed_by_resume(self):
        # slug cycle_ts=1000, cycle_end=1300. T-100 -> real_ts=1200. T-40 -> real_ts=1260.
        lines = [
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1000 T-100s binance=1800 up=0.5 dn=0.5",
            "[live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.",
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1000 T-40s binance=1800 up=0.5 dn=0.5",
            "[telegram] sent: ▶️ Resumed all assets (BTC, ETH, DOGE).",
        ]
        with tempfile.TemporaryDirectory() as d:
            log_path = self._log(Path(d), lines)
            windows = mod.build_halt_windows(log_path, mod.datetime(2026, 1, 1, tzinfo=mod.HKT))
        self.assertEqual(len(windows), 1)
        start, end, reason = windows[0]
        self.assertEqual(start, 1200.0)
        self.assertEqual(end, 1260.0)
        self.assertEqual(reason, "balance drawdown >25% (session)")

    def test_stop_loss_telegram_line_is_not_mistaken_for_a_halt(self):
        """Regression test: a '🛑 <asset> STOP LOSS triggered' trade alert
        shares the same emoji as real halt messages but isn't one — found
        2026-07-12 as a false-positive halt-open 2min before a genuine
        balance-drawdown halt. Only the real halt (which also says "halt")
        should open a window."""
        lines = [
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1000 T-100s binance=1800 up=0.5 dn=0.5",
            "[telegram] sent: \U0001f6d1 <b>ETH</b> STOP LOSS triggered | 14:49:52 | T-7s | UP ↑ | high_prob",
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1000 T-40s binance=1800 up=0.5 dn=0.5",
            "[live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.",
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1300 T-260s binance=1800 up=0.5 dn=0.5",
            "[telegram] sent: ▶️ Resumed all assets (BTC, ETH, DOGE).",
        ]
        with tempfile.TemporaryDirectory() as d:
            log_path = self._log(Path(d), lines)
            windows = mod.build_halt_windows(log_path, mod.datetime(2026, 1, 1, tzinfo=mod.HKT))
        self.assertEqual(len(windows), 1)
        start, end, reason = windows[0]
        self.assertEqual(start, 1260.0, "must open at the real BALANCE DRAWDOWN line, not the stop-loss alert")
        self.assertEqual(reason, "balance drawdown >25% (session)")

    def test_unresumed_halt_stays_open_through_window_end(self):
        lines = [
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1300 T-200s binance=1800 up=0.5 dn=0.5",
            "[telegram] sent: \U0001f6d1 Halted all assets (BTC, ETH, DOGE) — new entries suppressed, open positions still managed.",
        ]
        window_end = mod.datetime(2026, 1, 1, tzinfo=mod.HKT)
        with tempfile.TemporaryDirectory() as d:
            log_path = self._log(Path(d), lines)
            windows = mod.build_halt_windows(log_path, window_end)
        self.assertEqual(len(windows), 1)
        start, end, reason = windows[0]
        self.assertEqual(start, 1400.0)  # 1300 + 300 - 200
        self.assertEqual(end, window_end.timestamp())
        self.assertEqual(reason, "manual /halt")

    def test_halt_line_before_any_heartbeat_is_ignored_not_fatal(self):
        lines = [
            "[live] BALANCE DRAWDOWN >25% from session baseline — halting new entries on all assets.",
            "[live] heartbeat ETH (high_prob) slug=eth-updown-5m-1000 T-100s binance=1800 up=0.5 dn=0.5",
        ]
        with tempfile.TemporaryDirectory() as d:
            log_path = self._log(Path(d), lines)
            windows = mod.build_halt_windows(log_path, mod.datetime(2026, 1, 1, tzinfo=mod.HKT))
        self.assertEqual(windows, [])

    def test_missing_log_file_returns_empty_not_fatal(self):
        windows = mod.build_halt_windows(Path("/nonexistent/live.log"), mod.datetime(2026, 1, 1, tzinfo=mod.HKT))
        self.assertEqual(windows, [])


class SafeBuildHaltWindowsTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        with patch.object(mod, "build_halt_windows", side_effect=RuntimeError("boom")):
            out = mod._safe_build_halt_windows(Path("/tmp"), mod.datetime(2026, 1, 1, tzinfo=mod.HKT))
        self.assertEqual(out, [])


class GetConfigLastChangeTsTests(unittest.TestCase):
    def test_returns_git_commit_ts_of_latest_config_file(self):
        with tempfile.TemporaryDirectory() as d:
            config_dir = Path(d)
            (config_dir / "strategy_20260709.toml").write_text("trade_assets = []\n")
            with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [], returncode=0, stdout="1783900000\n", stderr="")) as run_mock:
                ts = mod.get_config_last_change_ts(config_dir)
            self.assertEqual(ts, 1783900000.0)
            run_mock.assert_called_once()

    def test_none_when_no_config_files(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(mod.get_config_last_change_ts(Path(d)))

    def test_none_when_git_fails(self):
        with tempfile.TemporaryDirectory() as d:
            config_dir = Path(d)
            (config_dir / "strategy_20260709.toml").write_text("trade_assets = []\n")
            with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [], returncode=128, stdout="", stderr="not a git repository")):
                self.assertIsNone(mod.get_config_last_change_ts(config_dir))


class SafeGetConfigLastChangeTsTests(unittest.TestCase):
    def test_never_raises_even_if_the_inner_function_blows_up(self):
        with patch.object(mod, "get_config_last_change_ts", side_effect=RuntimeError("boom")):
            out = mod._safe_get_config_last_change_ts(Path("/tmp"))
        self.assertIsNone(out)


class ClassifyMismatchReasonTests(unittest.TestCase):
    def test_halt_window_match(self):
        windows = [(1000.0, 1300.0, "manual /halt")]
        reason = mod.classify_mismatch_reason(1100.0, windows, None, 2000.0, None)
        self.assertIn("live halted: manual /halt", reason)

    def test_config_change_in_window(self):
        # config changed at ts=1500, which is >= cycle_ts (1100) and <= window_end (2000)
        reason = mod.classify_mismatch_reason(1100.0, [], 1500.0, 2000.0, None)
        self.assertIn("config changed", reason)

    def test_config_change_before_cycle_is_not_flagged(self):
        # config changed at ts=900, before this cycle -> not "same-window" going forward
        reason = mod.classify_mismatch_reason(1100.0, [], 900.0, 2000.0, None)
        self.assertEqual(reason, "unexplained")

    def test_sparse_tick_data(self):
        reason = mod.classify_mismatch_reason(1100.0, [], None, 2000.0, 5)
        self.assertIn("sparse tick data (5 ticks", reason)

    def test_dense_tick_data_is_not_flagged_sparse(self):
        reason = mod.classify_mismatch_reason(1100.0, [], None, 2000.0, 1200)
        self.assertEqual(reason, "unexplained")

    def test_unexplained_fallback(self):
        reason = mod.classify_mismatch_reason(1100.0, [], None, 2000.0, None)
        self.assertEqual(reason, "unexplained")

    def test_priority_halt_beats_config_and_sparse_data(self):
        windows = [(1000.0, 1300.0, "manual /halt")]
        reason = mod.classify_mismatch_reason(1100.0, windows, 1500.0, 2000.0, 5)
        self.assertIn("live halted", reason)

    def test_priority_config_beats_sparse_data(self):
        reason = mod.classify_mismatch_reason(1100.0, [], 1500.0, 2000.0, 5)
        self.assertIn("config changed", reason)


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
        self.assertEqual(table[0]["reason"], "—", "a MATCH needs no explanation")

    def test_outcome_diff_when_same_slug_side_different_outcome(self):
        live = self._live(outcome="WIN", pnl=0.0753)
        bt = self._bt(outcome="UNWIND", pnl=0.0326)
        table, summary = mod.build_live_vs_bt([live], [bt], {"ETH"})
        self.assertIn("OUTCOME DIFF", table[0]["status"])
        self.assertEqual(summary["n_outcome_diff"], 1)
        self.assertAlmostEqual(table[0]["diff_pnl"], 0.0753 - 0.0326)
        self.assertEqual(table[0]["reason"], "unexplained")

    def test_mismatch_reason_uses_halt_window_context(self):
        live = self._live(slug="eth-updown-5m-1000", outcome="WIN")
        bt = self._bt(slug="eth-updown-5m-1000", outcome="UNWIND")
        mismatch_ctx = {
            "halt_windows": [(900.0, 1100.0, "manual /halt")],
            "config_change_ts": None, "window_end_ts": 2000.0,
        }
        table, _ = mod.build_live_vs_bt([live], [bt], {"ETH"}, mismatch_ctx=mismatch_ctx)
        self.assertIn("live halted: manual /halt", table[0]["reason"])

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

    def test_entry_exit_and_delta_columns_computed_from_underlying_price(self):
        """Entry Δ%/Cycle Δ% must come from the underlying (Binance) price
        series, not the trade's own CLOB token_price/exit_price — see
        incident_delta_pct_2026-07-12.md. Entry Px/Exit Px stay CLOB."""
        live = self._live(slug="eth-updown-5m-1000")
        live["entry_ts"] = 1291.0  # T-9s
        live["token_price"] = 0.93  # CLOB — shown as-is, not used for Δ%
        live["exit_price"] = 1.0
        underlying_prices = {"eth-updown-5m-1000": [
            (1000.0, 1800.0),   # cycle open
            (1291.0, 1801.44),  # exact entry_ts tick
            (1300.0, 1802.0),   # cycle close
        ]}
        table, _ = mod.build_live_vs_bt([live], [self._bt(slug="eth-updown-5m-1000")], {"ETH"},
                                         underlying_prices)
        r = table[0]
        self.assertEqual(r["entry_time"], "T-9s")
        self.assertAlmostEqual(r["entry_price"], 0.93)
        self.assertAlmostEqual(r["exit_price"], 1.0)
        self.assertAlmostEqual(r["cycle_delta_pct"], (1802.0 - 1800.0) / 1800.0 * 100)
        self.assertAlmostEqual(r["entry_delta_pct"], (1801.44 - 1800.0) / 1800.0 * 100)

    def test_deltas_are_none_without_underlying_price_data(self):
        live = self._live()
        live["token_price"] = 0.93
        table, _ = mod.build_live_vs_bt([live], [self._bt()], {"ETH"})
        self.assertIsNone(table[0]["entry_delta_pct"])
        self.assertIsNone(table[0]["cycle_delta_pct"])


class BuildBtVsLiveTests(unittest.TestCase):
    """Cycles the backtest fired but live never traded (either side)."""

    def test_bt_only_cycle_is_reported_as_missed(self):
        bt = [{"asset": "ETH", "slug": "eth-updown-5m-1", "strategy": "high_prob",
               "side": "UP", "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1.0}]
        missed = mod.build_bt_vs_live(bt, live_rows=[])
        self.assertEqual(len(missed), 1)
        self.assertEqual(missed[0]["asset"], "ETH")
        self.assertAlmostEqual(missed[0]["pnl"], 0.3)
        self.assertEqual(missed[0]["reason"], "unexplained", "every row here is a mismatch by definition")

    def test_reason_uses_halt_window_context(self):
        bt = [{"asset": "ETH", "slug": "eth-updown-5m-1000", "strategy": "high_prob",
               "side": "UP", "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1000.0}]
        mismatch_ctx = {
            "halt_windows": [(900.0, 1100.0, "balance drawdown >25% (session)")],
            "config_change_ts": None, "window_end_ts": 2000.0,
        }
        missed = mod.build_bt_vs_live(bt, live_rows=[], mismatch_ctx=mismatch_ctx)
        self.assertIn("live halted: balance drawdown", missed[0]["reason"])

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

    def test_entry_exit_and_delta_columns_computed_from_underlying_price(self):
        bt = [{"asset": "ETH", "slug": "eth-updown-5m-1000", "strategy": "high_prob",
               "side": "UP", "outcome": "WIN", "pnl": 0.3, "cycle_ts": 1000.0,
               "entry_ts": 1291.0, "token_price": 0.93, "exit_price": 1.0}]
        underlying_prices = {"eth-updown-5m-1000": [
            (1000.0, 1800.0), (1291.0, 1801.44), (1300.0, 1802.0),
        ]}
        missed = mod.build_bt_vs_live(bt, live_rows=[], underlying_prices=underlying_prices)
        r = missed[0]
        self.assertEqual(r["entry_time"], "T-9s")
        self.assertAlmostEqual(r["entry_price"], 0.93)
        self.assertAlmostEqual(r["exit_price"], 1.0)
        self.assertAlmostEqual(r["cycle_delta_pct"], (1802.0 - 1800.0) / 1800.0 * 100)
        self.assertAlmostEqual(r["entry_delta_pct"], (1801.44 - 1800.0) / 1800.0 * 100)


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


class RenderStrategyConfigTests(unittest.TestCase):
    """Which strategy_*.toml is live and its key params — surfaced at the top
    of the report (see write_markdown_summary's section ordering)."""

    SAMPLE_TOML = """
ts = "2026-07-15T00:00:00+08:00"
source = "test source note"
trade_assets = ["BTC", "BNB"]

[strategies]
default = ["reversal"]

[halt_rev]
default = 1

[halt_prob]
default = 1

[halt_reset_hour_rev]
default = 2

[halt_reset_hour_hp]
default = 8

[reversal]
default = 0.50

[reversal_low_threshold]
default = 0.40
BTC = 0.30

[delta_pct_rev]
default = 0.0003
BNB = 0.0008

[sl_reversal]
default = 0.10

[sl_pnl_rev]
default = 0.30
BNB = 0.50

[unwind_pnl_rev]
default = 0.25

[unwind_time_rev]
default = 20.0
"""

    def _render(self, toml_text: str) -> str:
        with tempfile.TemporaryDirectory() as d:
            config_dir = Path(d)
            (config_dir / "strategy_20260715.toml").write_text(toml_text)
            lines: list = []
            mod.render_strategy_config(lines, config_dir)
            return "\n".join(lines)

    def test_renders_top_level_facts(self):
        text = self._render(self.SAMPLE_TOML)
        self.assertIn("## Strategy Config", text)
        self.assertIn("strategy_20260715.toml", text)
        self.assertIn("BTC, BNB", text)  # trade assets
        self.assertIn("reversal", text)  # strategies
        self.assertIn("| halt_rev / halt_prob | 1 / 1 |", text)
        self.assertIn("| halt_reset_hour (rev / hp) | 2 / 8 HKT |", text)

    def test_per_asset_table_resolves_overrides_and_defaults(self):
        text = self._render(self.SAMPLE_TOML)
        # BTC: reversal_low_threshold overridden (0.30), delta_pct_rev on default (0.0003)
        self.assertIn("| BTC | 0.5000 | 0.3000 | 0.00030 |", text)
        # BNB: delta_pct_rev and sl_pnl_rev both overridden
        self.assertIn("| BNB | 0.5000 | 0.4000 | 0.00080 |", text)
        self.assertIn("0.5000 |", text)  # BNB's overridden sl_pnl_rev

    def test_source_note_is_collapsible(self):
        text = self._render(self.SAMPLE_TOML)
        self.assertIn("<summary>Notes (meta.source)</summary>", text)
        self.assertIn("test source note", text)

    def test_no_config_files_degrades_gracefully(self):
        with tempfile.TemporaryDirectory() as d:
            lines: list = []
            mod.render_strategy_config(lines, Path(d))
            text = "\n".join(lines)
        self.assertIn("## Strategy Config", text)
        self.assertIn("No strategy_*.toml found", text)

    def test_picks_lexicographically_latest_file(self):
        with tempfile.TemporaryDirectory() as d:
            config_dir = Path(d)
            (config_dir / "strategy_20260101.toml").write_text(
                'ts = "old"\ntrade_assets = ["ETH"]\n[strategies]\ndefault = ["high_prob"]\n'
            )
            (config_dir / "strategy_20260715.toml").write_text(self.SAMPLE_TOML)
            lines: list = []
            mod.render_strategy_config(lines, config_dir)
            text = "\n".join(lines)
        self.assertIn("strategy_20260715.toml", text)
        self.assertIn("BTC, BNB", text)
        self.assertNotIn("ETH", text)


class RenderDataQualityTests(unittest.TestCase):
    """price_feed/doc/incident_collector_data_loss_2026-07-12.md's proposed observer."""

    def test_renders_flagged_rows(self):
        lines = []
        result = {
            "hours_checked": 10,
            "flagged": [
                {"asset": "ETH", "kind": "binance", "date": "2026-07-11", "hour": 22,
                 "coverage_pct": 28.3, "status": "GAP"},
                {"asset": "DOGE", "kind": "poly", "date": "2026-07-11", "hour": 23,
                 "coverage_pct": None, "status": "MISSING"},
            ],
        }
        text = "\n".join(_render_data_quality_lines(lines, result))
        self.assertIn("2/10 asset-hours flagged", text)
        self.assertIn("ETH", text)
        self.assertIn("GAP", text)
        self.assertIn("28.3%", text)
        self.assertIn("MISSING", text)

    def test_no_gaps_shows_clean_message(self):
        text = "\n".join(_render_data_quality_lines([], {"hours_checked": 24, "flagged": []}))
        self.assertIn("24 asset-hours checked — no gaps", text)

    def test_zero_hours_checked_shows_placeholder(self):
        text = "\n".join(_render_data_quality_lines([], {"hours_checked": 0, "flagged": []}))
        self.assertIn("No fully-elapsed hours to check yet", text)

    def test_error_is_surfaced_not_swallowed(self):
        text = "\n".join(_render_data_quality_lines([], {"error": "raw dir missing"}))
        self.assertIn("Check failed: raw dir missing", text)


def _render_data_quality_lines(lines: list, result: dict) -> list:
    mod.render_data_quality(lines, result)
    return lines


class WriteMarkdownSummaryDataQualityTests(unittest.TestCase):
    def test_data_quality_section_present_and_summary_line_shown(self):
        result = {
            "hours_checked": 5,
            "flagged": [{"asset": "ETH", "kind": "binance", "date": "2026-07-11", "hour": 22,
                         "coverage_pct": 10.0, "status": "GAP"}],
        }
        with tempfile.TemporaryDirectory() as d:
            path = mod.write_markdown_summary(
                {"direction": {}, "stoploss": {}, "total_rows": 0}, {},
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                Path(d), bt_result=None, data_quality_result=result,
            )
            text = path.read_text()
        self.assertIn("## Data Quality", text)
        self.assertIn("Data quality:** 1/5 asset-hours flagged", text)

    def test_missing_data_quality_result_does_not_crash(self):
        with tempfile.TemporaryDirectory() as d:
            path = mod.write_markdown_summary(
                {"direction": {}, "stoploss": {}, "total_rows": 0}, {},
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                Path(d), bt_result=None, data_quality_result=None,
            )
            text = path.read_text()
        self.assertIn("## Data Quality", text)


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


class MakeSectionsCollapsibleTests(unittest.TestCase):
    def test_wraps_each_top_level_section_in_details(self):
        lines = [
            "# Title", "", "> summary line", "",
            "## Data Quality", "", "row content here", "",
            "## Performance", "", "### Sub Header", "perf content", "",
        ]
        out = mod._make_sections_collapsible(lines)
        text = "\n".join(out)
        self.assertEqual(out.count("<details>"), 2)
        self.assertEqual(out.count("</details>"), 2)
        self.assertIn("<summary><h2>Data Quality</h2></summary>", text)
        self.assertIn("<summary><h2>Performance</h2></summary>", text)
        # preamble before the first '## ' stays outside any <details> block
        self.assertLess(out.index("> summary line"), out.index("<details>"))
        # original header text is preserved inside the block (anchors still work)
        self.assertIn("## Data Quality", text)

    def test_sub_headers_do_not_start_their_own_section(self):
        lines = ["## Performance", "", "### Sub Header", "content", ""]
        out = mod._make_sections_collapsible(lines)
        self.assertEqual(out.count("<details>"), 1)

    def test_no_top_level_headers_is_a_noop_besides_identity(self):
        lines = ["# Title", "", "> just a summary, no trades", ""]
        out = mod._make_sections_collapsible(lines)
        self.assertEqual(out, lines)

    def test_last_section_is_closed_at_end_of_document(self):
        lines = ["## Only Section", "", "content", ""]
        out = mod._make_sections_collapsible(lines)
        self.assertIn("</details>", out)
        self.assertLess(out.index("</details>"), len(out) - 1 if out[-1] == "" else len(out))
        self.assertEqual(out[-1], "")
        self.assertEqual(out[-2], "</details>")

    def test_write_markdown_summary_output_is_collapsible(self):
        result = {
            "hours_checked": 5,
            "flagged": [{"asset": "ETH", "kind": "binance", "date": "2026-07-11", "hour": 22,
                         "coverage_pct": 10.0, "status": "GAP"}],
        }
        with tempfile.TemporaryDirectory() as d:
            path = mod.write_markdown_summary(
                {"direction": {}, "stoploss": {}, "total_rows": 0}, {},
                mod.datetime(2026, 7, 9, 20, 0, tzinfo=mod.HKT),
                mod.datetime(2026, 7, 10, 20, 0, tzinfo=mod.HKT),
                Path(d), bt_result=None, data_quality_result=result,
            )
            text = path.read_text()
        self.assertIn("<details>", text)
        self.assertIn("<summary><h2>Data Quality</h2></summary>", text)
        self.assertIn("## Data Quality", text)
        self.assertIn("<summary><h2>Backtest Reconciliation</h2></summary>", text)
        # Data Quality is rendered last (after Backtest Reconciliation) —
        # Strategy Config, by contrast, is always first.
        self.assertLess(text.index("## Strategy Config"), text.index("## Backtest Reconciliation"))
        self.assertLess(text.index("## Backtest Reconciliation"), text.index("## Data Quality"))


if __name__ == "__main__":
    unittest.main()
