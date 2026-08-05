#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec psql "${DATABASE_URL}" \
  --no-psqlrc \
  --quiet \
  --no-align \
  --set ON_ERROR_STOP=1 \
  --tuples-only \
  --file "${script_dir}/audit_source_references.sql"
