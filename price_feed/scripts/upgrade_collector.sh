#!/usr/bin/env bash
# Upgrade the price-feed collector running on the Oracle box to the latest
# code on main: push local commits, pull them down on Oracle, rebuild, and
# restart the systemd service (poly-collector.service).
#
# Usage: ./upgrade_collector.sh

set -euo pipefail

REMOTE_USER="ubuntu"
REMOTE_HOST="10.8.0.1"
REMOTE_DIR="~/apps/poly_rust"
LOCAL_REPO="$(cd "$(dirname "$0")/../.." && pwd)"

# Resolve SSH agent socket for cron environments where SSH_AUTH_SOCK may be
# unset (same pattern as sync_oracle.sh).
if [[ -z "${SSH_AUTH_SOCK:-}" ]]; then
    AGENT_DIR="$HOME/.ssh/agent"
    if [[ -d "$AGENT_DIR" ]]; then
        for sock in "$AGENT_DIR"/s.*; do
            if SSH_AUTH_SOCK="$sock" ssh-add -l &>/dev/null; then
                export SSH_AUTH_SOCK="$sock"
                break
            fi
        done
    fi
fi

echo "==> pushing local main to origin..."
git -C "$LOCAL_REPO" push origin main

echo ""
echo "==> connecting to $REMOTE_USER@$REMOTE_HOST ..."
ssh "$REMOTE_USER@$REMOTE_HOST" bash -s <<REMOTE_SCRIPT
set -euo pipefail
cd $REMOTE_DIR

echo "==> pulling latest main..."
git pull

echo "==> building price_feed (release)..."
source "\$HOME/.cargo/env"
cd price_feed
cargo build --release

echo "==> restarting poly-collector.service..."
# restart_collector.sh tails logs forever when run interactively; cap it here
# so the upgrade completes instead of hanging (|| true absorbs the timeout
# exit code, not a real failure).
timeout 20 bash scripts/restart_collector.sh || true
REMOTE_SCRIPT

echo ""
echo "==> done. Verify with:"
echo "    ssh $REMOTE_USER@$REMOTE_HOST 'sudo systemctl status poly-collector'"
