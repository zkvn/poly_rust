#!/usr/bin/env bash
set -euo pipefail

ASSETS="${ASSETS:-btc eth sol bnb xrp doge}"
LOG_DIR="log"
PID_FILE="collector.pid"

mkdir -p "$LOG_DIR" raw

if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "collector already running (pid $OLD_PID) — kill it first with: kill $OLD_PID"
        exit 1
    fi
fi

LOG="$LOG_DIR/collector_$(date +%Y%m%d_%H%M%S).log"

nohup cargo run --release -- collect $ASSETS > "$LOG" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"
echo "started pid=$PID  log=$LOG"
echo "tail -f $LOG"
