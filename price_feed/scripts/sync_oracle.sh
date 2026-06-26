#!/usr/bin/env bash
# Sync raw* folders from oracle box (ubuntu@10.8.0.1) to local price_feed/.
# Uses rsync: one SSH connection per directory instead of per-file scp.
# Usage: ./sync_oracle.sh          # sync all raw* folders
#        ./sync_oracle.sh --dry-run # preview only

set -euo pipefail

REMOTE_USER="ubuntu"
REMOTE_HOST="10.8.0.1"
REMOTE_BASE="~/apps/poly_rust/price_feed"
LOCAL_BASE="$(cd "$(dirname "$0")/.." && pwd)"
DRY_RUN=0
RSYNC_OPTS="-avz"

if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    RSYNC_OPTS="$RSYNC_OPTS --dry-run"
    echo "[dry-run] no files will be copied"
fi

# Resolve SSH agent socket for cron environments where SSH_AUTH_SOCK may be unset
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

echo "Connecting to $REMOTE_USER@$REMOTE_HOST ..."

REMOTE_DIRS=$(ssh "$REMOTE_USER@$REMOTE_HOST" \
    "ls -d $REMOTE_BASE/raw*/ 2>/dev/null | xargs -I{} basename {} || true")

if [[ -z "$REMOTE_DIRS" ]]; then
    echo "No raw* folders found on remote."
    exit 0
fi

for dir in $REMOTE_DIRS; do
    echo ""
    echo "==> $dir"

    local_dst="$LOCAL_BASE/$dir"
    mkdir -p "$local_dst"

    rsync $RSYNC_OPTS \
        "$REMOTE_USER@$REMOTE_HOST:$REMOTE_BASE/$dir/" \
        "$local_dst/"
done

echo ""
echo "Done."
