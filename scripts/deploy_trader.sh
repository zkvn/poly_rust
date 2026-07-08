#!/usr/bin/env bash
# Deploy the Rust `live` trader binary to Oracle — trader only.
#
# Safety: always calls deploy_oracle.py with --trader-only, so this NEVER
# touches poly-collector (price_feed) or its systemd restart step. Price
# recording on Oracle is left completely alone regardless of what this
# script does with the trader process.
#
# What it does (see scripts/deploy_oracle.py for the implementation):
#   1. Cross-compiles `live` for aarch64 locally via `cross` (Docker-based) —
#      the binary is built here, not on Oracle.
#   2. rsyncs the binary to Oracle's trader/target/release/.
#   3. Gracefully stops the old trader process (SIGTERM, wait 10s, SIGKILL
#      only if still alive) and kills its tmux session.
#   4. Starts the new binary in a fresh tmux session ('trader'), reading
#      --asset flags from btc_5mins/config's latest strategy_*.toml.
#   Also always syncs trader/config/ (the actual strategy_*.toml, not just the
#   --asset flags) to Oracle before restarting — see
#   trader/doc/incident_stale_oracle_config_2026-07-07.md for why this can't
#   be skipped.
#
# Usage:
#   ./scripts/deploy_trader.sh                 # build + deploy + restart trader
#   ./scripts/deploy_trader.sh --dry-run       # show every step, change nothing
#   ./scripts/deploy_trader.sh --skip-build    # reuse the last local build
#   ./scripts/deploy_trader.sh --config-only   # sync config only, no build/binary rsync
#   ./scripts/deploy_trader.sh --update-config # commit+push config, then sync — no build

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="/home/kev/apps/btc_5mins/venv/bin/python3"

if [[ ! -x "$PYTHON" ]]; then
    echo "error: $PYTHON not found (paramiko/tomllib venv for deploy_oracle.py)" >&2
    exit 1
fi

echo "==> Deploying trader to Oracle (--trader-only: price_feed/poly-collector untouched)"
exec "$PYTHON" "$REPO_ROOT/scripts/deploy_oracle.py" --trader-only "$@"
