#!/bin/bash
# Installs and enables both siglab systemd --user timers:
#   - siglab-report-push.timer   (every 15 min: pushes digest_*.md/candidate_ledger.csv to
#                                  git, if either changed — see push_report.sh)
#   - siglab-daily-digest.timer  (08:45 HKT: generates the digest itself — see
#                                  analysis/siglab_daily_digest.py)
# No sudo needed (systemctl --user, not a system service) — run once as the normal user.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR"

# systemd --user services don't inherit this shell's SSH_AUTH_SOCK (they get their own
# default agent, which doesn't have the git-push-authorized key — see the .service file's
# comment and siglab/doc/incident_ws_2026-07-13.md). Bake the *current* shell's socket into
# the installed unit. Re-run this script after a reboot/re-login if push starts failing
# again — the socket path isn't stable across login sessions. Only siglab-report-push
# needs this (it's the only one that touches git); siglab-daily-digest only reads the
# trade log and writes local files.
if [ -z "${SSH_AUTH_SOCK:-}" ]; then
  echo "WARNING: \$SSH_AUTH_SOCK is not set in this shell — the installed push timer's git" >&2
  echo "push will likely fail to authenticate. Run this from a shell where" >&2
  echo "'ssh -T git@github.com' already works, then re-run this script." >&2
fi
sed "s|__SSH_AUTH_SOCK__|${SSH_AUTH_SOCK:-}|" \
  "$REPO_ROOT/siglab/systemd/siglab-report-push.service" > "$UNIT_DIR/siglab-report-push.service"
cp "$REPO_ROOT/siglab/systemd/siglab-report-push.timer" "$UNIT_DIR/"
cp "$REPO_ROOT/siglab/systemd/siglab-daily-digest.service" "$UNIT_DIR/"
cp "$REPO_ROOT/siglab/systemd/siglab-daily-digest.timer" "$UNIT_DIR/"

systemctl --user daemon-reload
systemctl --user enable --now siglab-report-push.timer
systemctl --user enable --now siglab-daily-digest.timer

echo "Installed. Status:"
systemctl --user status siglab-report-push.timer --no-pager || true
systemctl --user status siglab-daily-digest.timer --no-pager || true

echo
echo "Note: for this timer to keep firing when you are logged out (not just while you have"
echo "an active session), user lingering must be enabled. Check with:"
echo "  loginctl show-user \"\$(whoami)\" --property=Linger"
echo "If it shows 'Linger=no', enable it with:"
echo "  loginctl enable-linger \"\$(whoami)\""
