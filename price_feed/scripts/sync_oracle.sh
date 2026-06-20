#!/usr/bin/env bash
# Sync raw* folders from oracle box (ubuntu@10.8.0.1) to local price_feed/.
# Skips files already present locally with the same or larger size.
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

REMOTE_DIRS=$(ssh "$REMOTE_USER@$REMOTE_HOST" \
    "ls -d $REMOTE_BASE/raw*/ 2>/dev/null | xargs -I{} basename {} || true")

if [[ -z "$REMOTE_DIRS" ]]; then
    echo "No raw* folders found on remote."
    exit 0
fi

TOTAL_COPIED=0
TOTAL_SKIPPED=0

for dir in $REMOTE_DIRS; do
    local_dst="$LOCAL_BASE/$dir"
    mkdir -p "$local_dst"

    echo ""
    echo "==> $dir"

    # Get remote file list: "size filename" per line (cd first so %n is bare filename)
    remote_files=$(ssh "$REMOTE_USER@$REMOTE_HOST" \
        "cd $REMOTE_BASE/$dir && stat -c '%s %n' * 2>/dev/null" || true)

    if [[ -z "$remote_files" ]]; then
        echo "  (empty)"
        continue
    fi

    while IFS= read -r line; do
        remote_size=$(echo "$line" | awk '{print $1}')
        fname=$(echo "$line" | awk '{print $2}')
        local_file="$local_dst/$fname"

        if [[ -f "$local_file" ]]; then
            local_size=$(stat -c '%s' "$local_file")
            if [[ "$local_size" -ge "$remote_size" ]]; then
                TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
                continue
            fi
            status="outdated"
        else
            status="missing"
        fi

        if [[ $DRY_RUN -eq 1 ]]; then
            echo "  [dry-run] $fname ($status, remote=${remote_size}b)"
        else
            echo "  copying $fname ($status) ..."
            scp -q "$REMOTE_USER@$REMOTE_HOST:$REMOTE_BASE/$dir/$fname" "$local_file"
            TOTAL_COPIED=$((TOTAL_COPIED + 1))
        fi
    done <<< "$remote_files"
done

echo ""
echo "Done. Copied $TOTAL_COPIED file(s), skipped $TOTAL_SKIPPED already up-to-date."
