#!/usr/bin/env bash
# Daily Telegram digest of price_feed's data-quality signals — CLOB bba
# (plan_bba_feed_staleness_fix_2026-07-10.md), Binance @bookTicker
# (plan_binance_ws_quality_2026-07-20.md §4), and the code's own reconnect
# activity (WS-level retries + reconciliation-triggered process restarts).
#
# Redesigned 2026-07-22 (price_feed/doc/incident_data_quality_2026-07-22.md):
# the original version led with raw [OBSERVE-STALE] silence counts as the
# headline "⚠️" number — but OBSERVE-STALE is a pure silence-duration
# counter with no correctness judgment (a quiet-but-healthy market crosses
# the same buckets a genuinely broken feed would; see staleness.rs's doc
# comment). The real judgment already happens automatically, every 5s, via
# reconcile.rs's REST /midpoint cross-check + a graceful restart on a
# confirmed mismatch — so raw silence counts were mostly false alarms for
# something the process had already checked and cleared itself. This
# version leads with the confirmed-genuine-issue count instead (the [RECONCILE-STALE]
# lines — small, real, and actionable), reports code-driven reconnect
# activity (WS retries + reconciliation restarts — a spike here across an
# overnight window is the actual "something's wrong" signal), and keeps the
# raw silence totals as one informational line, not a per-asset alarm list.
#
# Runs on Oracle as `ubuntu` (same user as poly-collector.service), triggered
# by data-quality-digest.timer. Reads TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID from
# trader/.env (same bot/chat as trader's own notifications) rather than
# duplicating credentials — price_feed has no Telegram config of its own.
#
# Usage: ./data_quality_digest.sh                    # last 24h, sends to Telegram
#        ./data_quality_digest.sh "2 hours ago"       # custom window, for testing
#        DIGEST_DRY_RUN=1 ./data_quality_digest.sh    # build + print the message, don't send
#
# Testing: ./test_data_quality_digest.sh runs the parsing/formatting logic
# (build_digest_text, below) against fixed sample journal lines — no
# journalctl, no network, no Oracle needed. Source this file (it only runs
# main() when executed directly, not when sourced) to reuse the functions.

set -euo pipefail

# ── Single journalctl pass, server-side filtered ─────────────────────────────
# journald's own -g grep (PCRE2, when systemd is built with it — true on
# Ubuntu) filters *inside* the journal daemon, not by piping the full
# unfiltered 24h of routine [live]/heartbeat-style output through an
# external `grep` process. One combined regex covers every pattern this
# digest cares about; every downstream split works off this already-small
# result set. Keeps this oneshot script's footprint on Oracle small even
# over a 24h window — the thing this task explicitly asked to minimize.
JOURNAL_PATTERN='OBSERVE-STALE|RECONCILE-STALE|ws closed, reconnecting|stream closed, reconnecting|connect failed.*retrying|subscribe.*failed.*retrying'

fetch_journal_lines() {
    local window="$1"
    journalctl -u poly-collector --since "$window" --no-pager -o cat \
        -g "$JOURNAL_PATTERN" 2>/dev/null || true
}

# Line count matching `pattern` in `text`, always exactly one integer, never
# empty, never partial output — safe under `set -o pipefail`. `grep -c`
# alone always prints a count line (even "0") but exits 1 on zero matches;
# piping that exit status through further stages (e.g. into awk) under
# pipefail makes the *pipeline's* exit status 1 even though every stage
# produced correct output, so a naive `... | awk ... || echo 0` fallback
# fires *in addition to* the real result and corrupts the captured value
# (two lines instead of one) — found by this script's own local test suite,
# not guessed. The subshell here scopes `|| true` to grep's own exit status
# only, and grep's stdout is always exactly one line either way.
count_matches() {
    local text="$1" pattern="$2"
    printf '%s\n' "$text" | (grep -c -E "$pattern" || true)
}

# ── Genuine issues: [RECONCILE-STALE], REST-ground-truth-confirmed ──────────
# Each episode logs exactly two lines (collect.rs: the detection eprintln,
# then run()'s shutdown-select-arm re-print with " — flushing writers…"
# appended) — filter out the second so each episode counts once.
genuine_issues_section() {
    local all_lines="$1"
    local matched
    matched=$(echo "$all_lines" | grep 'RECONCILE-STALE' | grep -v 'flushing writers' || true)

    if [[ -z "$matched" ]]; then
        echo "0 confirmed issue(s)"
        return
    fi

    local total
    total=$(count_matches "$matched" 'RECONCILE-STALE')
    echo "⚠️ $total confirmed issue(s) (REST-verified, not just silence)"
    echo "$matched" | sed -E 's/^\[RECONCILE-STALE\] ([A-Z]+) rest_mid=([0-9.]+) cached_mid=([0-9.]+) diff=([0-9.]+).*/  \1: cached \3 vs real \2 (off by \4)/'
}

# ── Code-driven reconnects: WS-level retries + reconciliation restarts ──────
# A spike here across an overnight/unattended window is the actual "check
# on this" signal this task asked for — distinct from ordinary silence
# (which the reconciliation check already clears itself) and distinct from
# the genuine-issue count above (one confirmed mismatch = exactly one
# process restart, but a stream can also drop/retry without ever crossing
# reconcile.rs's mismatch threshold, e.g. a clean disconnect that
# reconnects to the same correct price).
reconnects_section() {
    local all_lines="$1"
    local restarts binance_trade binance_book book_ws bba_ws trade_ws

    local reconcile_lines
    reconcile_lines=$(count_matches "$all_lines" 'RECONCILE-STALE')
    restarts=$((reconcile_lines / 2))
    binance_trade=$(count_matches "$all_lines" 'binance trade ws closed, reconnecting|binance trade connect failed.*retrying')
    binance_book=$(count_matches "$all_lines" 'binance bookTicker ws closed, reconnecting|binance bookTicker connect failed.*retrying')
    book_ws=$(count_matches "$all_lines" 'book stream closed, reconnecting|subscribe_orderbook failed.*retrying')
    bba_ws=$(count_matches "$all_lines" 'bba/price stream closed, reconnecting|subscribe best_bid_ask/prices failed.*retrying')
    trade_ws=$(count_matches "$all_lines" 'last-trade stream closed, reconnecting|subscribe_last_trade_price failed.*retrying')

    local total=$((restarts + binance_trade + binance_book + book_ws + bba_ws + trade_ws))
    echo "$total total"
    echo "  process restarts (reconciliation-confirmed): $restarts"
    echo "  Binance trade WS: $binance_trade"
    echo "  Binance bookTicker WS: $binance_book"
    echo "  Polymarket book WS: $book_ws"
    echo "  Polymarket bba/price WS: $bba_ws"
    echo "  Polymarket last-trade WS: $trade_ws"
}

# ── Silence, informational only ──────────────────────────────────────────────
# One compact line per feed instead of the old per-asset alarm list — total
# events, how many assets, the per-asset count range, and the range of
# "worst" bucket crossed. Most of this is ordinary quiet-market silence
# already cross-checked (and cleared, when correct) by reconcile.rs every
# 5s — the genuine-issues section above is what actually needs attention.
silence_summary_line() {
    local all_lines="$1"
    local feed_pattern="$2"
    local matched
    matched=$(echo "$all_lines" | grep "$feed_pattern" || true)

    if [[ -z "$matched" ]]; then
        echo "no silence observed"
        return
    fi

    echo "$matched" | awk -v feed="$feed_pattern" '
        {
            for (i = 1; i <= NF; i++) {
                if ($i == "[OBSERVE-STALE]") { asset = $(i + 1) }
                if ($i ~ /^>=[0-9]+ms$/) {
                    bucket = $i
                    sub(/^>=/, "", bucket)
                    sub(/ms$/, "", bucket)
                    count[asset]++
                    total++
                    if (bucket + 0 > max[asset] + 0) { max[asset] = bucket }
                }
            }
        }
        END {
            n_assets = 0
            min_count = -1; max_count = -1
            min_worst = -1; max_worst = -1
            for (a in count) {
                n_assets++
                if (min_count < 0 || count[a] < min_count) { min_count = count[a] }
                if (count[a] > max_count) { max_count = count[a] }
                w = max[a] / 1000
                if (min_worst < 0 || w < min_worst) { min_worst = w }
                if (w > max_worst) { max_worst = w }
            }
            range = (min_count == max_count) ? min_count : min_count "-" max_count
            worst_range = (min_worst == max_worst) ? "≥" min_worst "s" : "≥" min_worst "s-≥" max_worst "s"
            printf "%d event(s) across %d asset(s) (%s per asset), gaps of %s\n", total, n_assets, range, worst_range
        }
    '
}

# ── Assemble the full message text (pure — no journalctl/curl calls) ────────
# Split out so test_data_quality_digest.sh can exercise it directly against
# fixed sample input instead of needing a live journal or network.
build_digest_text() {
    local all_lines="$1"
    local window_label="$2"

    local genuine reconnects clob_silence binance_silence
    genuine=$(genuine_issues_section "$all_lines")
    reconnects=$(reconnects_section "$all_lines")
    clob_silence=$(silence_summary_line "$all_lines" 'bba feed silent')
    binance_silence=$(silence_summary_line "$all_lines" 'binance bookTicker feed silent')

    local icon="✅"
    if [[ "$genuine" == ⚠️* ]]; then
        icon="⚠️"
    fi

    cat <<EOF
${icon} <b>Data quality digest</b> (${window_label})

<b>Genuine issues</b> (REST-verified against Polymarket's own /midpoint, not just silence)
${genuine}

<b>Code-driven reconnects</b> (WS retries + reconciliation restarts — a spike here overnight is the real "check on this" signal)
${reconnects}

<b>Silence observed</b> (informational — most is ordinary quiet markets already cross-checked above, not a separate alarm)
CLOB (bba): ${clob_silence}
Binance (bookTicker): ${binance_silence}

Genuine issues + reconnects: real signal, code already recovers automatically.
CLOB: price_feed/doc/plan_bba_feed_staleness_fix_2026-07-10.md
Binance: price_feed/doc/plan_binance_ws_quality_2026-07-20.md §4
Digest design: price_feed/doc/incident_data_quality_2026-07-22.md
EOF
}

main() {
    local window="${1:-24 hours ago}"
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local env_file="$script_dir/../../trader/.env"

    if [[ ! -f "$env_file" ]]; then
        echo "no $env_file — can't send Telegram digest, skipping" >&2
        exit 0
    fi

    local bot_token chat_id
    bot_token=$(grep '^TELEGRAM_BOT_TOKEN=' "$env_file" | cut -d= -f2-)
    chat_id=$(grep '^TELEGRAM_CHAT_ID=' "$env_file" | cut -d= -f2-)

    if [[ -z "$bot_token" || -z "$chat_id" ]]; then
        echo "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing from $env_file, skipping" >&2
        exit 0
    fi

    local all_lines text
    all_lines=$(fetch_journal_lines "$window")
    text=$(build_digest_text "$all_lines" "last 24h")

    if [[ "${DIGEST_DRY_RUN:-0}" == "1" ]]; then
        echo "$text"
        echo "--- dry run: not sent (DIGEST_DRY_RUN=1) ---" >&2
        return
    fi

    curl -sS -X POST "https://api.telegram.org/bot${bot_token}/sendMessage" \
        -d chat_id="${chat_id}" \
        -d parse_mode="HTML" \
        --data-urlencode text="${text}" \
        > /dev/null

    echo "digest sent (window: $window)"
}

# Only run when executed directly — test_data_quality_digest.sh sources this
# file to reuse build_digest_text()/genuine_issues_section()/etc. without
# triggering a journalctl call or a Telegram send.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
