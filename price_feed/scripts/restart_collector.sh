#!/usr/bin/env bash
set -euo pipefail

# Restart the price-feed collector running under systemd.
#
# The collector runs as the systemd service `poly-collector.service`
# (unit at /etc/systemd/system/poly-collector.service). It auto-starts
# on boot and auto-restarts on crash, so you normally don't touch it.
# Run this script after a rebuild (`cargo build --release`) to pick up
# new code.
#
# How to manage it:
#   systemctl status poly-collector        # is it up?
#   journalctl -u poly-collector -f        # live logs
#   sudo systemctl restart poly-collector  # restart (e.g. after rebuild)
#   sudo systemctl stop poly-collector     # graceful stop — flushes parquet via SIGTERM
#   sudo systemctl start poly-collector    # start
#
# This script does the restart and then shows status + tails the logs.

SERVICE="poly-collector.service"

echo "restarting $SERVICE ..."
sudo systemctl restart "$SERVICE"

sudo systemctl status "$SERVICE" --no-pager | head -12

echo
echo "following logs (Ctrl-C to stop) ..."
journalctl -u "$SERVICE" -f -o cat
