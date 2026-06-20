#!/usr/bin/env bash
# Sync raw* folders from oracle box (ubuntu@10.8.0.1) to local price_feed/.
# Usage: ./sync_oracle.sh          # sync all raw* folders
#        ./sync_oracle.sh --dry-run # preview only

set -euo pipefail

REMOTE_USER="ubuntu"
REMOTE_HOST="10.8.0.1"
REMOTE_BASE="~/apps/poly_rust/price_feed"
LOCAL_BASE="$(cd "$(dirname "$0")/.." && pwd)"
DRY_RUN=0

if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
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

# Get list of remote raw* folder names
REMOTE_DIRS=$(ssh "$REMOTE_USER@$REMOTE_HOST" \
    "ls -d $REMOTE_BASE/raw*/ 2>/dev/null | xargs -I{} basename {} || true")

if [[ -z "$REMOTE_DIRS" ]]; then
    echo "No raw* folders found on remote at $REMOTE_BASE."
    exit 0
fi

echo "Remote raw* folders: $(echo "$REMOTE_DIRS" | tr '\n' ' ')"
echo ""

TOTAL=0
for dir in $REMOTE_DIRS; do
    local_dst="$LOCAL_BASE/$dir"

    if [[ $DRY_RUN -eq 1 ]]; then
        echo "[dry-run] would sync $REMOTE_USER@$REMOTE_HOST:~/apps/poly_rust/price_feed/$dir -> $local_dst/"
        continue
    fi

    mkdir -p "$local_dst"
    echo "Syncing $dir ..."
    ssh "$REMOTE_USER@$REMOTE_HOST" "cd ~/apps/poly_rust/price_feed && tar czf - $dir" | tar xzf - -C "$LOCAL_BASE" || true
    TOTAL=$((TOTAL + 1))
done

if [[ $DRY_RUN -eq 0 ]]; then
    echo ""
    echo "Done. Synced $TOTAL folder(s)."
fi
