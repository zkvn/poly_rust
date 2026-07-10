#!/usr/bin/env python3
"""Unit tests for trade_reconcile.py's Gamma cross-check fixes (2026-07-10):

  1. TIMEOUT-outcome rows must not be flagged as Gamma "mismatches" — a
     max-holding-time force-close was never a directional prediction, same
     as STOPLOSS/UNWIND, but previously fell through to the WIN/LOSS-only
     comparison branch by omission and was *always* flagged as wrong.
  2. WIN/LOSS rows where Gamma never resolved in time and worker.rs used the
     balance-increase override (trader/src/balance.rs::GammaBalanceTracker,
     2026-07-09) must be reported separately from true correction-logic
     mismatches, not lumped in as "a bug".

Run with the same interpreter as the cron job:
    /home/kev/apps/btc_5mins/venv/bin/python trader/scripts/test_gamma_reconcile.py
"""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_PATH = Path(__file__).resolve().parent / "trade_reconcile.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("trade_reconcile", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


class ParseGammaTimeoutEventsTests(unittest.TestCase):
    def test_parses_continued_and_halted_lines(self):
        log_text = (
            "[live] ETH gave up waiting for Gamma resolution of eth-updown-5m-1 "
            "— halting new entries (high_prob)\n"
            "[live] BTC gave up waiting for Gamma resolution of btc-updown-5m-2 "
            "— balance up since last cycle's checkpoint, continuing (reversal)\n"
        )
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "live.log"
            path.write_text(log_text)
            events = mod.parse_gamma_timeout_events(path)
        self.assertEqual(events, {
            "eth-updown-5m-1": "HALTED",
            "btc-updown-5m-2": "CONTINUED",
        })

    def test_unrelated_lines_are_ignored(self):
        log_text = (
            "[live] eth-updown-5m-9: gave up waiting for Gamma resolution "
            "— no counterfactual verdict\n"
            "[telegram] sent: some other message\n"
        )
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "live.log"
            path.write_text(log_text)
            events = mod.parse_gamma_timeout_events(path)
        self.assertEqual(events, {})

    def test_missing_file_returns_empty_dict(self):
        events = mod.parse_gamma_timeout_events(Path("/nonexistent/live.log"))
        self.assertEqual(events, {})


def _live_row(slug, side, outcome, logged_at=1783606800.0):
    return {
        "logged_at": str(logged_at), "slug": slug, "strategy": "high_prob",
        "side": side, "entry_ts": str(logged_at), "token_price": "0.9",
        "exit_price": "1.0", "outcome": outcome, "pnl": "0.1",
        "exit_attempts": "0", "exit_last_error": "",
    }


class AnnotateRowsTimeoutClassificationTests(unittest.TestCase):
    """Regression test for the TIMEOUT-always-flagged-as-mismatch bug."""

    def test_timeout_row_is_not_counted_as_a_mismatch(self):
        row = _live_row("eth-updown-5m-1", "DOWN", "TIMEOUT")
        with patch.object(mod, "fetch_gamma_outcome", return_value="UP"):  # side lost
            annotated, summary = mod.annotate_rows([row])
        self.assertEqual(summary["direction"]["wrong"], 0)
        self.assertEqual(summary["mismatch_details"], [])
        self.assertEqual(annotated[0]["actual_result"], "LOSS")  # still computed, just not flagged

    def test_win_loss_row_still_flagged_as_mismatch_without_a_timeout_event(self):
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        with patch.object(mod, "fetch_gamma_outcome", return_value="DOWN"):  # actually lost
            annotated, summary = mod.annotate_rows([row])
        self.assertEqual(summary["direction"]["wrong"], 1)
        self.assertEqual(len(summary["mismatch_details"]), 1)

    def test_win_loss_row_still_matches_when_gamma_agrees(self):
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        with patch.object(mod, "fetch_gamma_outcome", return_value="UP"):
            annotated, summary = mod.annotate_rows([row])
        self.assertEqual(summary["direction"]["correct"], 1)
        self.assertEqual(summary["direction"]["wrong"], 0)

    def test_unwind_row_still_advisory_only_no_counters(self):
        row = _live_row("eth-updown-5m-1", "UP", "UNWIND")
        with patch.object(mod, "fetch_gamma_outcome", return_value="DOWN"):
            annotated, summary = mod.annotate_rows([row])
        self.assertEqual(summary["direction"]["resolved"], 0)
        self.assertEqual(summary["mismatch_details"], [])


class AnnotateRowsGammaTimeoutOverrideTests(unittest.TestCase):
    """Rows where the balance-increase Gamma-timeout override fired must be
    reported separately from true ApiResult-correction mismatches."""

    def test_continued_event_excluded_from_mismatches(self):
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        events = {"eth-updown-5m-1": "CONTINUED"}
        with patch.object(mod, "fetch_gamma_outcome", return_value="DOWN"):  # would look "wrong"
            annotated, summary = mod.annotate_rows([row], events)
        self.assertEqual(summary["direction"]["wrong"], 0)
        self.assertEqual(summary["mismatch_details"], [])
        gt = summary["gamma_timeout"]
        self.assertEqual(gt["continued"], 1)
        self.assertEqual(gt["halted"], 0)
        self.assertEqual(len(gt["details"]), 1)
        self.assertEqual(gt["details"][0]["event"], "CONTINUED")
        self.assertFalse(gt["details"][0]["hindsight_match"])  # informational only

    def test_halted_event_excluded_from_mismatches(self):
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        events = {"eth-updown-5m-1": "HALTED"}
        with patch.object(mod, "fetch_gamma_outcome", return_value="UP"):
            annotated, summary = mod.annotate_rows([row], events)
        self.assertEqual(summary["direction"]["correct"], 0)  # not counted as a match either
        self.assertEqual(summary["direction"]["wrong"], 0)
        gt = summary["gamma_timeout"]
        self.assertEqual(gt["halted"], 1)
        self.assertTrue(gt["details"][0]["hindsight_match"])

    def test_pending_gamma_result_takes_priority_over_timeout_event(self):
        """If Gamma still hasn't resolved by the time recon runs, it's PENDING
        regardless of whether a timeout-override event was logged for it."""
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        events = {"eth-updown-5m-1": "CONTINUED"}
        with patch.object(mod, "fetch_gamma_outcome", return_value=None):
            annotated, summary = mod.annotate_rows([row], events)
        self.assertEqual(summary["direction"]["pending"], 1)
        self.assertEqual(summary["gamma_timeout"]["continued"], 0)

    def test_no_timeout_events_is_backward_compatible(self):
        row = _live_row("eth-updown-5m-1", "UP", "WIN")
        with patch.object(mod, "fetch_gamma_outcome", return_value="UP"):
            annotated, summary = mod.annotate_rows([row])  # no second arg at all
        self.assertEqual(summary["direction"]["correct"], 1)
        self.assertEqual(summary["gamma_timeout"], {"continued": 0, "halted": 0, "details": []})


if __name__ == "__main__":
    unittest.main()
