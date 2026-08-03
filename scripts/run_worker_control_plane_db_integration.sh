#!/usr/bin/env bash
set -euo pipefail

required=(
  WORKER_CONTROL_PLANE_DATABASE_URL
  WORKER_CONTROL_PLANE_DATABASE_WORKTREE
  WORKER_CONTROL_PLANE_DATABASE_SHA
  WORKER_CONTROL_PLANE_MIGRATION_VERSION
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required isolated-harness variable: ${name}" >&2
    exit 2
  fi
done

database_worktree="$(cd "${WORKER_CONTROL_PLANE_DATABASE_WORKTREE}" && pwd -P)"
database_sha="$(git -C "${database_worktree}" rev-parse HEAD)"
if [[ "${database_sha}" != "${WORKER_CONTROL_PLANE_DATABASE_SHA}" ]]; then
  echo "database worktree HEAD does not match WORKER_CONTROL_PLANE_DATABASE_SHA" >&2
  exit 2
fi
if ! git -C "${database_worktree}" diff --quiet -- . ':(exclude)supabase/config.toml' || \
   ! git -C "${database_worktree}" diff --cached --quiet -- . || \
   [[ -n "$(git -C "${database_worktree}" ls-files --others --exclude-standard)" ]]; then
  echo "database worktree has changes outside the runner-owned Supabase config mutation" >&2
  exit 2
fi
if git -C "${database_worktree}" diff --quiet -- supabase/config.toml; then
  echo "database worktree is missing the expected runner-owned Supabase config mutation" >&2
  exit 2
fi

if [[ "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"localhost"* && \
      "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"127.0.0.1"* && \
      "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"[::1]"* ]]; then
  echo "refusing non-loopback database target" >&2
  exit 2
fi

worker_sha="$(git rev-parse HEAD)"
printf '{"databaseSha":"%s","migrationVersion":"%s","workerSha":"%s","roleProof":"SET ROLE service_role ACL matrix; deployment login external"}\n' \
  "${database_sha}" "${WORKER_CONTROL_PLANE_MIGRATION_VERSION}" "${worker_sha}"

cargo test -p solver-worker \
  --test worker_control_plane_database_contract \
  -- --ignored --exact private_worker_control_plane_preserves_lifecycle_and_compatibility
