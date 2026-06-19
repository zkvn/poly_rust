#!/usr/bin/env bash
set -euo pipefail

# Ensure cargo is on PATH (needed when launched via nohup without a login shell)
source "$HOME/.cargo/env" 2>/dev/null || true

LOG_DIR="log"
PID_FILE="collector.pid"

mkdir -p "$LOG_DIR" raw raw_15_mins raw_1hr raw_4hr

if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "collector already running (pid $OLD_PID) — kill it first with: kill $OLD_PID"
        exit 1
    fi
fi

LOG="$LOG_DIR/collector_$(date +%Y%m%d_%H%M%S).log"

nohup cargo run --release -- collect > "$LOG" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"
echo "started pid=$PID  log=$LOG"
echo "tail -f $LOG"
