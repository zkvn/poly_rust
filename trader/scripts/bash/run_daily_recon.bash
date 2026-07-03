#!/usr/bin/env bash
# run_daily_recon.bash — cron wrapper for poly_rust trade reconciliation
# Mirrors btc_5mins/scripts/bash/run_daily_recon.bash: Python computes the
# 8pm-8pm HKT window internally, so re-running mid-window just refreshes the
# same report idempotently.
#
# Syncs trader/live_logs from Oracle first (trade logs are only ever written
# there by the live trader process), then runs the reconciliation.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
TRADER_DIR="$REPO_ROOT/trader"
cd "$REPO_ROOT"

# ── cron-safe environment ────────────────────────────────────────────────
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
export SSH_AUTH_SOCK="$(ls -t "$HOME"/.ssh/agent/s.*.agent.* 2>/dev/null | head -1)"

VENV_PYTHON="/home/kev/apps/btc_5mins/venv/bin/python"
CRON_LOG="$TRADER_DIR/log/recon_cron.log"
ORACLE_HOST="10.8.0.1"
ORACLE_USER="ubuntu"
ORACLE_LOG_DIR="/home/ubuntu/apps/poly_rust/trader/live_logs/"

mkdir -p "$TRADER_DIR/log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Syncing live_logs from Oracle" >> "$CRON_LOG"
rsync -avz "$ORACLE_USER@$ORACLE_HOST:$ORACLE_LOG_DIR" "$TRADER_DIR/live_logs/" >> "$CRON_LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running trade recon (--today)" >> "$CRON_LOG"
"$VENV_PYTHON" "$TRADER_DIR/scripts/trade_reconcile.py" --today >> "$CRON_LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daily recon complete" >> "$CRON_LOG"
