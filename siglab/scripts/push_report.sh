#!/bin/bash
# Commits and pushes siglab's per-day signal report(s) to git. Run hourly by a systemd
# --user timer (siglab-report-push.timer, installed by siglab/scripts/install_timer.sh) —
# NOT by the siglab process itself, and NOT requiring any git/SSH credentials inside the
# Docker container. The container only writes report files to a bind-mounted repo path
# (siglab/doc/report/); this script, running on the host as the normal user, is the only
# thing that touches git. This split is what makes the hourly push continue working
# without any AI/session driving it — it's a plain cron-style host job.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# `git add` with a pathspec that matches zero files is a FATAL error (exit 128), not a
# silent no-op — distinct from "matched files but nothing changed" below. Caught in
# production: the very first two hourly timer firings both failed this way because no
# report had been written yet (siglab writes its first report one full interval after
# container start, so a freshly (re)started container has a report-free window).
#
# Matches both `{date}/summary_{date}.md` and `{date}/trades_{date}_{HH}.md` (2026-07-15
# per-day-folder layout, replacing the flat `signal_report_*.md` files).
shopt -s nullglob
report_files=(siglab/doc/report/*/*.md)
if [ ${#report_files[@]} -eq 0 ]; then
  echo "[push_report] no report files exist yet — nothing to push"
  exit 0
fi

git add "${report_files[@]}"

if git diff --cached --quiet; then
  echo "[push_report] no report changes since last run — nothing to push"
  exit 0
fi

git commit -m "siglab: hourly signal report update ($(date -u +%Y-%m-%dT%H:%MZ))

Auto-committed by siglab/scripts/push_report.sh via siglab-report-push.timer — not a
Claude/manual commit. See siglab/doc/report/ for the report(s) and
siglab/scripts/install_timer.sh for how this is scheduled."

git push
echo "[push_report] pushed at $(date -u +%Y-%m-%dT%H:%MZ)"
