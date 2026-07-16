#!/usr/bin/env bash
# Rebuilds and restarts siglab's local Docker Compose service (siglab/docker-compose.yml,
# service "siglab" -> container siglab-siglab-1). Quick redeploy for grid/strategy changes
# (e.g. v_shape.rs / bucket_reversal.rs edits) that need to go live on this box without
# touching Oracle. Config/logs/reports keep their existing bind mounts and volume, so
# in-progress trade logs and the git-tracked doc/report/ tree are untouched by a restart.
#
# Usage:
#   ./siglab/scripts/deploy_local.sh              # cargo test/clippy, then build+restart
#   ./siglab/scripts/deploy_local.sh --skip-checks # build+restart only, no cargo test/clippy
#   ./siglab/scripts/deploy_local.sh --logs        # after restart, follow container logs
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

SKIP_CHECKS=false
FOLLOW_LOGS=false
for arg in "$@"; do
  case "$arg" in
    --skip-checks) SKIP_CHECKS=true ;;
    --logs) FOLLOW_LOGS=true ;;
    *)
      echo "error: unknown argument '$arg'" >&2
      exit 1
      ;;
  esac
done

if [ "$SKIP_CHECKS" = false ]; then
  echo "==> cargo test (siglab)"
  (cd siglab && cargo test)
  echo "==> cargo clippy --all-targets --all-features -- -D warnings (siglab)"
  (cd siglab && cargo clippy --all-targets --all-features -- -D warnings)
  echo "==> cargo fmt --all --check (siglab)"
  (cd siglab && cargo fmt --all --check)
fi

echo "==> docker compose -f siglab/docker-compose.yml up --build -d"
docker compose -f siglab/docker-compose.yml up --build -d

echo "==> status"
docker compose -f siglab/docker-compose.yml ps

if [ "$FOLLOW_LOGS" = true ]; then
  docker compose -f siglab/docker-compose.yml logs -f --tail=50
fi
