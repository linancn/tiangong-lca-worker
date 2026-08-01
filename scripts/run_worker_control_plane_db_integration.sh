#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${WORKER_CONTROL_PLANE_DATABASE_URL:-}" ]]; then
  echo "caller-supplied Worker behavioral database URLs are forbidden" >&2
  exit 2
fi
if [[ -z "${WORKER_CONTROL_PLANE_DATABASE_WORKTREE:-}" ]]; then
  echo "WORKER_CONTROL_PLANE_DATABASE_WORKTREE is required" >&2
  exit 2
fi

exec python3 "$(dirname "${BASH_SOURCE[0]}")/worker_control_plane_db_harness.py"
