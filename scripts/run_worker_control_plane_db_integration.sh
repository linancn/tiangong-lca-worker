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
if [[ -n "$(git -C "${database_worktree}" status --porcelain --untracked-files=no)" ]]; then
  echo "database worktree has tracked changes; exact-SHA evidence would be ambiguous" >&2
  exit 2
fi

if [[ "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"localhost"* && \
      "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"127.0.0.1"* && \
      "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"[::1]"* ]]; then
  preview_ref="${WORKER_CONTROL_PLANE_HOSTED_PREVIEW_REF:-}"
  if [[ -z "${preview_ref}" || \
        "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"${preview_ref}"* || \
        ( "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"sslmode=require"* && \
          "${WORKER_CONTROL_PLANE_DATABASE_URL}" != *"sslmode=verify-full"* ) ]]; then
    echo "refusing non-loopback target without an exact hosted Preview ref and required TLS" >&2
    exit 2
  fi
fi

worker_sha="$(git rev-parse HEAD)"
target_kind="loopback"
if [[ -n "${WORKER_CONTROL_PLANE_HOSTED_PREVIEW_REF:-}" ]]; then
  target_kind="hosted-preview"
fi
printf '{"databaseSha":"%s","migrationVersion":"%s","workerSha":"%s","targetKind":"%s","roleProof":"SET ROLE service_role ACL matrix; deployment login external"}\n' \
  "${database_sha}" "${WORKER_CONTROL_PLANE_MIGRATION_VERSION}" "${worker_sha}" "${target_kind}"

cargo test -p solver-worker \
  --test worker_control_plane_database_contract \
  -- --ignored --exact private_worker_control_plane_preserves_lifecycle_and_compatibility
