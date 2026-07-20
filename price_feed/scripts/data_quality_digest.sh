#!/usr/bin/env bash
# Daily Telegram digest of price_feed's observe-only staleness loggers — CLOB
# bba (plan_bba_feed_staleness_fix_2026-07-10.md) and Binance @bookTicker
# (plan_binance_ws_quality_2026-07-20.md §4). A periodic summary, not
# per-event alerting: [OBSERVE-STALE] lines are logged constantly
# (journal-only, no recovery action) and would be alert-fatigue noise sent
# one-by-one, so this rolls the last 24h of both into one message instead.
#
# Runs on Oracle as `ubuntu` (same user as poly-collector.service), triggered
# by data-quality-digest.timer. Reads TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID from
# trader/.env (same bot/chat as trader's own notifications) rather than
# duplicating credentials — price_feed has no Telegram config of its own.
#
# Usage: ./data_quality_digest.sh           # last 24h
#        ./data_quality_digest.sh "2 hours ago"   # custom window, for testing

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

ALL_LINES=$(journalctl -u poly-collector --since "$WINDOW" --no-pager 2>/dev/null \
    | grep '\[OBSERVE-STALE\]' || true)

# Filters $ALL_LINES to lines matching $1 (a feed-specific fragment, e.g.
# "bba feed silent" or "binance bookTicker feed silent"), aggregates per
# asset (event count + worst/max bucket crossed), and prints either
# "no staleness" or one "⚠️ N event(s)" line + one indented line per asset.
# Every OBSERVE-STALE line (both feeds) shares the same shape from the
# "[OBSERVE-STALE] <ASSET> ... for >=<bucket>ms (actual <actual>ms) —
# logging only, no action taken" point on, so one parser covers both.
digest_section() {
    local feed_pattern="$1"
    local matched
    matched=$(echo "$ALL_LINES" | grep "$feed_pattern" || true)

    if [[ -z "$matched" ]]; then
        echo "no staleness"
        return
    fi

    local total
    total=$(echo "$matched" | wc -l | tr -d ' ')
    local summary
    summary=$(echo "$matched" | awk '
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

    echo "⚠️ $total event(s)"
    while IFS='|' read -r asset count max_ms; do
        max_s=$(awk -v ms="$max_ms" 'BEGIN { printf "%.0f", ms / 1000 }')
        echo "  $asset: $count event(s), worst ≥${max_s}s"
    done <<< "$summary"
}

CLOB_SECTION=$(digest_section 'bba feed silent')
BINANCE_SECTION=$(digest_section 'binance bookTicker feed silent')

ICON="✅"
if [[ "$CLOB_SECTION" == ⚠️* || "$BINANCE_SECTION" == ⚠️* ]]; then
    ICON="⚠️"
fi

TEXT="$ICON <b>Data quality digest</b> (last 24h)

<b>CLOB (bba)</b>
$CLOB_SECTION

<b>Binance (bookTicker)</b>
$BINANCE_SECTION

logging only — no recovery action taken
CLOB: price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md
Binance: price_feed/doc/plan_binance_ws_quality_2026-07-20.md §4"

curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d chat_id="${CHAT_ID}" \
    -d parse_mode="HTML" \
    --data-urlencode text="${TEXT}" \
    > /dev/null

echo "digest sent (window: $WINDOW)"
