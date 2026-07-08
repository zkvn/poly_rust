#!/usr/bin/env python3
"""Regression tests for scripts/deploy_oracle.py's step wiring.

Specifically: deploying the trader (the default full deploy, or
--trader-only) must always sync the strategy config to Oracle first, not
just on an explicit --config-only run.

Regression test for trader/doc/incident_stale_oracle_config_2026-07-07.md:
Telegram's /status showed sl_pnl=0.8000 for ETH reversal after a
--trader-only deploy that was supposed to set it to 0.25, because
deploy_trader() only rsyncs the binary and regenerates the systemd unit's
--asset flags (computed from *this machine's* trader/config/) — it never
touched the strategy_*.toml file the running binary actually reads from
Oracle's own filesystem on every startup via load_latest(). That file stayed
stale until a separate --config-only deploy, so trade_assets changed (baked
into the unit file directly) while sl_pnl_rev silently didn't (read from the
stale file at runtime).

No new dependency: uses only the stdlib (unittest + unittest.mock). Run with:
    python3 scripts/test_deploy_oracle.py
"""
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

SCRIPT_PATH = Path(__file__).resolve().parent / "deploy_oracle.py"


def _load_module():
    """Import deploy_oracle.py by path — it's a standalone script, not a
    package, and this repo has no other Python test infra to hang an import
    path off of."""
    spec = importlib.util.spec_from_file_location("deploy_oracle", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DeployStepOrderingTests(unittest.TestCase):
    """Every code path that ends up (re)starting trader-live.service must
    sync config first. Mocks every network/subprocess-touching step so these
    run instantly and never touch the real Oracle box or Docker toolchain."""

    def setUp(self):
        self.mod = _load_module()
        self.mod.connect_oracle = MagicMock(return_value=MagicMock())
        self.mod.build = MagicMock(return_value=True)
        self.mod.deploy_price_feed = MagicMock(return_value=True)
        self.mod.deploy_trader = MagicMock(return_value=True)
        self.mod.sync_config = MagicMock(return_value=True)
        self.mod.commit_and_push_config = MagicMock(return_value=True)

    def _run(self, argv, expected_exit_code=0):
        with patch.object(sys, "argv", ["deploy_oracle.py", *argv]):
            with self.assertRaises(SystemExit) as cm:
                self.mod.main()
            self.assertEqual(cm.exception.code, expected_exit_code)

    def test_default_full_deploy_syncs_config_before_deploying_trader(self):
        self._run(["--skip-build"])
        self.mod.sync_config.assert_called_once()
        self.mod.deploy_trader.assert_called_once()
        self.mod.deploy_price_feed.assert_called_once()

    def test_trader_only_syncs_config_before_deploying(self):
        self._run(["--trader-only", "--skip-build"])
        self.mod.sync_config.assert_called_once()
        self.mod.deploy_trader.assert_called_once()
        self.mod.deploy_price_feed.assert_not_called()

    def test_price_feed_only_never_touches_trader_or_config(self):
        self._run(["--price-feed-only", "--skip-build"])
        self.mod.sync_config.assert_not_called()
        self.mod.deploy_trader.assert_not_called()
        self.mod.deploy_price_feed.assert_called_once()

    def test_config_only_syncs_config_and_skips_binary_rsync(self):
        self._run(["--config-only"])
        self.mod.sync_config.assert_called_once()
        client = self.mod.connect_oracle.return_value
        self.mod.deploy_trader.assert_called_once_with(client, False, skip_binary=True)
        self.mod.deploy_price_feed.assert_not_called()
        self.mod.build.assert_not_called()

    def test_trader_deploy_is_skipped_when_config_sync_fails(self):
        """A failed config sync must not proceed to restart the trader
        against a half-synced or unknown config state."""
        self.mod.sync_config.return_value = False
        self._run(["--trader-only", "--skip-build"], expected_exit_code=1)
        self.mod.sync_config.assert_called_once()
        self.mod.deploy_trader.assert_not_called()

    def test_update_config_commits_before_syncing(self):
        self._run(["--update-config"])
        self.mod.commit_and_push_config.assert_called_once()
        self.mod.sync_config.assert_called_once()
        client = self.mod.connect_oracle.return_value
        self.mod.deploy_trader.assert_called_once_with(client, False, skip_binary=True)
        self.mod.deploy_price_feed.assert_not_called()
        self.mod.build.assert_not_called()

    def test_update_config_never_touches_oracle_when_git_push_fails(self):
        """A failed commit/push must not sync a config change to Oracle that
        isn't safely recorded in git — same "don't propagate an unconfirmed
        state" principle as the config-sync-failure case above, one level up."""
        self.mod.commit_and_push_config.return_value = False
        self._run(["--update-config"], expected_exit_code=1)
        self.mod.commit_and_push_config.assert_called_once()
        self.mod.connect_oracle.assert_not_called()
        self.mod.sync_config.assert_not_called()
        self.mod.deploy_trader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
