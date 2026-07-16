#!/usr/bin/env bash
set -euo pipefail

# Quick-start for local continuous mode. Mirrors price_feed/scripts/run_collector.sh's
# pattern: builds release, backgrounds it with nohup, writes a PID file so a second run
# doesn't silently start a duplicate process.
#
# Usage: gamma_recorder/scripts/run_local.sh (run from repo root or from gamma_recorder/)

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Ensure cargo is on PATH (needed when launched via nohup without a login shell)
source "$HOME/.cargo/env" 2>/dev/null || true

LOG_DIR="logs"
PID_FILE="gamma_recorder.pid"

mkdir -p "$LOG_DIR" data

if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "gamma_recorder already running (pid $OLD_PID) — kill it first with: kill $OLD_PID"
        exit 1
    fi
fi

cargo build --release

LOG="$LOG_DIR/continuous.log"

nohup ./target/release/gamma_recorder resolve --db data/gamma.db >> "$LOG" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"
echo "started pid=$PID  log=$LOG"
echo "tail -f $LOG"
