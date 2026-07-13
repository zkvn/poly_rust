#!/bin/bash
# Installs and enables the systemd --user timer that pushes siglab's hourly report to git.
# No sudo needed (systemctl --user, not a system service) — run once as the normal user.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

cp "$REPO_ROOT/siglab/systemd/siglab-report-push.service" "$UNIT_DIR/"
cp "$REPO_ROOT/siglab/systemd/siglab-report-push.timer" "$UNIT_DIR/"

systemctl --user daemon-reload
systemctl --user enable --now siglab-report-push.timer

echo "Installed. Status:"
systemctl --user status siglab-report-push.timer --no-pager || true

echo
echo "Note: for this timer to keep firing when you are logged out (not just while you have"
echo "an active session), user lingering must be enabled. Check with:"
echo "  loginctl show-user \"\$(whoami)\" --property=Linger"
echo "If it shows 'Linger=no', enable it with:"
echo "  loginctl enable-linger \"\$(whoami)\""
