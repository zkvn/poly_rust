#!/usr/bin/env bash
# Daily Telegram digest of price_feed's Binance @bookTicker observe-only staleness
# logger (plan_binance_ws_quality_2026-07-20.md §4) — a periodic summary, not
# per-event alerting: [OBSERVE-STALE] lines are logged constantly (journal-only,
# no recovery action) and would be alert-fatigue noise sent one-by-one, so this
# rolls the last 24h into one message instead.
#
# Runs on Oracle as `ubuntu` (same user as poly-collector.service), triggered by
# binance-stale-digest.timer. Reads TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID from
# trader/.env (same bot/chat as trader's own notifications) rather than
# duplicating credentials — price_feed has no Telegram config of its own.
#
# Usage: ./binance_stale_digest.sh           # last 24h
#        ./binance_stale_digest.sh "2 hours ago"   # custom window, for testing

set -euo pipefail

WINDOW="${1:-24 hours ago}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../trader/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "no $ENV_FILE — can't send Telegram digest, skipping" >&2
    exit 0
fi

BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d= -f2-)
CHAT_ID=$(grep '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d= -f2-)

if [[ -z "$BOT_TOKEN" || -z "$CHAT_ID" ]]; then
    echo "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing from $ENV_FILE, skipping" >&2
    exit 0
fi

LINES=$(journalctl -u poly-collector --since "$WINDOW" --no-pager 2>/dev/null \
    | grep 'binance bookTicker feed silent' || true)

if [[ -z "$LINES" ]]; then
    TEXT="✅ <b>Binance bookTicker staleness digest</b> (last 24h)
No OBSERVE-STALE events for any asset — @bookTicker stayed continuously fresh."
else
    # Per line: "... [OBSERVE-STALE] <ASSET> binance bookTicker feed silent for
    # >=<bucket>ms (actual <actual>ms) — logging only, no action taken"
    # Aggregate per asset: event count + the worst (max) bucket crossed.
    SUMMARY=$(echo "$LINES" | awk '
        {
            for (i = 1; i <= NF; i++) {
                if ($i == "[OBSERVE-STALE]") { asset = $(i + 1) }
                if ($i ~ /^>=[0-9]+ms$/) {
                    bucket = $i
                    sub(/^>=/, "", bucket)
                    sub(/ms$/, "", bucket)
                    count[asset]++
                    if (bucket + 0 > max[asset] + 0) { max[asset] = bucket }
                }
            }
        }
        END {
            for (a in count) {
                printf "%s|%d|%d\n", a, count[a], max[a]
            }
        }
    ' | sort)

    TOTAL=$(echo "$LINES" | wc -l | tr -d ' ')
    TEXT="⚠️ <b>Binance bookTicker staleness digest</b> (last 24h) — $TOTAL event(s)
"
    while IFS='|' read -r asset count max_ms; do
        max_s=$(awk -v ms="$max_ms" 'BEGIN { printf "%.0f", ms / 1000 }')
        TEXT="$TEXT
$asset: $count event(s), worst ≥${max_s}s"
    done <<< "$SUMMARY"
    TEXT="$TEXT

logging only — no recovery action taken, see price_feed/doc/plan_binance_ws_quality_2026-07-20.md §4"
fi

curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d chat_id="${CHAT_ID}" \
    -d parse_mode="HTML" \
    --data-urlencode text="${TEXT}" \
    > /dev/null

echo "digest sent (window: $WINDOW)"
