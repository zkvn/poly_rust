#!/usr/bin/env bash
# Local, no-network, no-Oracle tests for data_quality_digest.sh's parsing/
# formatting logic — feeds fixed sample journal lines (mirroring the real
# formats confirmed by reading price_feed/src/collect.rs directly, not
# guessed) through build_digest_text() and asserts on the result.
#
# Usage: ./test_data_quality_digest.sh
# Exit 0 = all assertions passed. Exit 1 = at least one failed (printed).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=data_quality_digest.sh
source "$SCRIPT_DIR/data_quality_digest.sh"

FAILURES=0

assert_contains() {
    local haystack="$1" needle="$2" label="$3"
    if [[ "$haystack" != *"$needle"* ]]; then
        echo "FAIL: $label"
        echo "  expected to contain: $needle"
        echo "  --- actual ---"
        echo "$haystack" | sed 's/^/  /'
        FAILURES=$((FAILURES + 1))
    else
        echo "ok: $label"
    fi
}

assert_not_contains() {
    local haystack="$1" needle="$2" label="$3"
    if [[ "$haystack" == *"$needle"* ]]; then
        echo "FAIL: $label"
        echo "  expected NOT to contain: $needle"
        FAILURES=$((FAILURES + 1))
    else
        echo "ok: $label"
    fi
}

# ── Fixture 1: a realistic noisy-but-mostly-healthy day ──────────────────────
# Two RECONCILE-STALE episodes (BNB, HYPE — each logs twice, per collect.rs's
# real shutdown-select-arm re-print), a handful of WS reconnects across every
# stream type, and a pile of OBSERVE-STALE silence across several assets.
FIXTURE_NOISY_DAY='[RECONCILE-STALE] BNB rest_mid=0.5000 cached_mid=0.9750 diff=0.4750 — confirmed via 3 consecutive mismatches, requesting graceful restart
[RECONCILE-STALE] BNB rest_mid=0.5000 cached_mid=0.9750 diff=0.4750 — confirmed via 3 consecutive mismatches, requesting graceful restart — flushing writers…
[RECONCILE-STALE] HYPE rest_mid=0.0050 cached_mid=0.0500 diff=0.0450 — confirmed via 3 consecutive mismatches, requesting graceful restart
[RECONCILE-STALE] HYPE rest_mid=0.0050 cached_mid=0.0500 diff=0.0450 — confirmed via 3 consecutive mismatches, requesting graceful restart — flushing writers…
[BNB] binance trade ws closed, reconnecting…
[DOGE] binance bookTicker ws closed, reconnecting…
[DOGE] binance bookTicker connect failed: timed out, retrying…
book stream closed, reconnecting…
bba/price stream closed, reconnecting…
subscribe best_bid_ask/prices failed: closed, retrying…
last-trade stream closed, reconnecting…
[OBSERVE-STALE] BNB bba feed silent for >=10000ms (actual 10412ms) — logging only, no action taken
[OBSERVE-STALE] BNB bba feed silent for >=30000ms (actual 31000ms) — logging only, no action taken
[OBSERVE-STALE] BTC bba feed silent for >=10000ms (actual 12000ms) — logging only, no action taken
[OBSERVE-STALE] BTC bba feed silent for >=30000ms (actual 35000ms) — logging only, no action taken
[OBSERVE-STALE] BTC bba feed silent for >=60000ms (actual 65000ms) — logging only, no action taken
[OBSERVE-STALE] BTC bba feed silent for >=120000ms (actual 121000ms) — logging only, no action taken
[OBSERVE-STALE] DOGE binance bookTicker feed silent for >=10000ms (actual 15000ms) — logging only, no action taken'

# ── Fixture 2: a clean day — nothing to report ────────────────────────────────
FIXTURE_CLEAN_DAY=''

echo "=== genuine_issues_section ==="
GENUINE=$(genuine_issues_section "$FIXTURE_NOISY_DAY")
assert_contains "$GENUINE" "2 confirmed issue(s)" "counts 2 distinct episodes, not 4 raw lines"
assert_contains "$GENUINE" "BNB: cached 0.9750 vs real 0.5000 (off by 0.4750)" "formats the BNB mismatch"
assert_contains "$GENUINE" "HYPE: cached 0.0500 vs real 0.0050 (off by 0.0450)" "formats the HYPE mismatch"

GENUINE_CLEAN=$(genuine_issues_section "$FIXTURE_CLEAN_DAY")
assert_contains "$GENUINE_CLEAN" "0 confirmed issue(s)" "clean day: zero confirmed issues"
assert_not_contains "$GENUINE_CLEAN" "⚠️" "clean day: no warning icon in the genuine-issues section itself"

echo
echo "=== reconnects_section ==="
RECONNECTS=$(reconnects_section "$FIXTURE_NOISY_DAY")
assert_contains "$RECONNECTS" "process restarts (reconciliation-confirmed): 2" "2 restarts, deduped from 4 raw RECONCILE-STALE lines"
assert_contains "$RECONNECTS" "Binance trade WS: 1" "1 binance trade WS reconnect"
assert_contains "$RECONNECTS" "Binance bookTicker WS: 2" "2 binance bookTicker WS reconnects (closed + connect-failed)"
assert_contains "$RECONNECTS" "Polymarket book WS: 1" "1 book WS reconnect"
assert_contains "$RECONNECTS" "Polymarket bba/price WS: 2" "2 bba/price WS reconnects (closed + subscribe-failed)"
assert_contains "$RECONNECTS" "Polymarket last-trade WS: 1" "1 last-trade WS reconnect"
assert_contains "$RECONNECTS" "9 total" "total reconnects sums correctly (2+1+2+1+2+1)"

RECONNECTS_CLEAN=$(reconnects_section "$FIXTURE_CLEAN_DAY")
assert_contains "$RECONNECTS_CLEAN" "0 total" "clean day: zero reconnects"

echo
echo "=== silence_summary_line ==="
CLOB_SILENCE=$(silence_summary_line "$FIXTURE_NOISY_DAY" 'bba feed silent')
assert_contains "$CLOB_SILENCE" "6 event(s) across 2 asset(s)" "CLOB silence: 6 events (2 BNB + 4 BTC), 2 assets"
assert_contains "$CLOB_SILENCE" "2-4 per asset" "CLOB silence: per-asset range 2-4"
assert_contains "$CLOB_SILENCE" "≥30s-≥120s" "CLOB silence: worst-bucket range 30s (BNB, escalated past 10s) to 120s (BTC)"

BINANCE_SILENCE=$(silence_summary_line "$FIXTURE_NOISY_DAY" 'binance bookTicker feed silent')
assert_contains "$BINANCE_SILENCE" "1 event(s) across 1 asset(s)" "Binance silence: 1 event, 1 asset (DOGE)"

CLOB_SILENCE_CLEAN=$(silence_summary_line "$FIXTURE_CLEAN_DAY" 'bba feed silent')
assert_contains "$CLOB_SILENCE_CLEAN" "no silence observed" "clean day: no CLOB silence"

echo
echo "=== build_digest_text (full message) ==="
FULL_NOISY=$(build_digest_text "$FIXTURE_NOISY_DAY" "last 24h")
assert_contains "$FULL_NOISY" "⚠️ <b>Data quality digest</b>" "noisy day: warning icon on the headline"
assert_contains "$FULL_NOISY" "2 confirmed issue(s)" "noisy day: genuine-issue count in the full message"
assert_contains "$FULL_NOISY" "9 total" "noisy day: reconnect total in the full message"
assert_contains "$FULL_NOISY" "Digest design: price_feed/doc/incident_data_quality_2026-07-22.md" "footer links the design doc"

FULL_CLEAN=$(build_digest_text "$FIXTURE_CLEAN_DAY" "last 24h")
assert_contains "$FULL_CLEAN" "✅ <b>Data quality digest</b>" "clean day: check-mark icon on the headline"
assert_contains "$FULL_CLEAN" "0 confirmed issue(s)" "clean day: zero confirmed issues in the full message"
assert_contains "$FULL_CLEAN" "0 total" "clean day: zero reconnects in the full message"
assert_contains "$FULL_CLEAN" "no silence observed" "clean day: no silence observed"

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES assertion(s) FAILED"
    exit 1
fi
echo "all assertions passed"
