#!/usr/bin/env bash
set -euo pipefail

worker_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="$(git -C "$worker_root" rev-parse --show-superproject-working-tree)"
database_root="${DATABASE_ENGINE_ROOT:-$workspace_root/database-engine}"
database_ref="${DATABASE_ENGINE_REF:-origin/dev}"
baseline_ref="${BASELINE_REF:-origin/main}"
candidate_ref="${CANDIDATE_REF:-HEAD}"
project_id="${BENCHMARK_PROJECT_ID:-}"
port_base="${BENCHMARK_PORT_BASE:-56320}"
runs="${BENCHMARK_RUNS:-20}"
keep_environment=0
output_path=""

usage() {
  echo "Usage: scripts/run_review_submit_source_closure_benchmark.sh [options]"
  echo "  --database-engine-root <path>  database-engine Git checkout used only as a schema object source"
  echo "  --database-ref <ref>           exact database schema ref (default: origin/dev)"
  echo "  --baseline-ref <ref>           Worker baseline ref (default: origin/main)"
  echo "  --candidate-ref <ref>          Worker candidate ref (default: HEAD)"
  echo "  --project-id <id>              isolated Supabase project/container prefix (default: unique per run)"
  echo "  --port-base <port>             reserves base+0,+1,+2,+3,+4,+7,+9 (default: 56320)"
  echo "  --runs <count>                 repetitions per variant/cache mode, minimum 20"
  echo "  --output <path>                sanitized JSON evidence path"
  echo "  --keep-environment             retain only this benchmark project after the run"
}

while (($#)); do
  case "$1" in
    --database-engine-root)
      database_root="$2"
      shift 2
      ;;
    --database-ref)
      database_ref="$2"
      shift 2
      ;;
    --baseline-ref)
      baseline_ref="$2"
      shift 2
      ;;
    --candidate-ref)
      candidate_ref="$2"
      shift 2
      ;;
    --project-id)
      project_id="$2"
      shift 2
      ;;
    --port-base)
      port_base="$2"
      shift 2
      ;;
    --runs)
      runs="$2"
      shift 2
      ;;
    --output)
      output_path="$2"
      shift 2
      ;;
    --keep-environment)
      keep_environment=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$project_id" == "database-engine" ]]; then
  echo "benchmark project_id must not target the database-engine stack" >&2
  exit 2
fi
if [[ ! "$port_base" =~ ^[0-9]+$ ]] || ((port_base < 1024 || port_base > 65000)); then
  echo "invalid --port-base: $port_base" >&2
  exit 2
fi
if [[ ! "$runs" =~ ^[0-9]+$ ]] || ((runs < 20)); then
  echo "--runs must be an integer >= 20" >&2
  exit 2
fi
if ! git -C "$database_root" rev-parse --git-dir >/dev/null 2>&1; then
  echo "database-engine Git checkout not found: $database_root" >&2
  exit 2
fi

for command in cargo docker git jq psql python3 supabase tar; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 2
  fi
done
python3 -c "import psutil" >/dev/null

api_port=$((port_base + 1))
db_port=$((port_base + 2))
studio_port=$((port_base + 3))
mail_port=$((port_base + 4))
inspector_port=$((port_base + 6))
analytics_port=$((port_base + 7))
pooler_port=$((port_base + 9))
for port in \
  "$port_base" "$api_port" "$db_port" "$studio_port" "$mail_port" \
  "$inspector_port" "$analytics_port" "$pooler_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "benchmark port is already in use: $port" >&2
    exit 2
  fi
done

baseline_sha="$(git -C "$worker_root" rev-parse "$baseline_ref^{commit}")"
candidate_sha="$(git -C "$worker_root" rev-parse "$candidate_ref^{commit}")"
database_sha="$(git -C "$database_root" rev-parse "$database_ref^{commit}")"
if [[ -z "$project_id" ]]; then
  project_id="w160-${candidate_sha:0:8}-$(date -u +%Y%m%d%H%M%S)-$$"
fi
if [[ ! "$project_id" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "project ID must contain only lowercase letters, digits, and hyphens: $project_id" >&2
  exit 2
fi
if ((${#project_id} > 40)); then
  echo "project ID exceeds the Supabase CLI 40-character limit: $project_id" >&2
  exit 2
fi
run_root="$(mktemp -d "${TMPDIR:-/tmp}/worker-pr161-source-closure.XXXXXX")"
stack_root="$run_root/database"
baseline_root="$run_root/worker-baseline"
candidate_root="$run_root/worker-candidate"
shared_target="$run_root/cargo-target"
binary_dir="$run_root/bin"
mkdir -p "$stack_root" "$binary_dir"

stack_started=0
baseline_worktree_added=0
candidate_worktree_added=0
cleanup() {
  exit_code=$?
  cleanup_project_id="$project_id"
  if [[ -f "$stack_root/supabase/config.toml" ]]; then
    configured_cleanup_project_id="$(
      sed -n 's/^project_id = "\([^"]*\)"$/\1/p' "$stack_root/supabase/config.toml" | head -n1
    )"
    if [[ "$configured_cleanup_project_id" =~ ^[a-z0-9][a-z0-9-]*$ ]] \
      && [[ "$configured_cleanup_project_id" != "database-engine" ]]; then
      cleanup_project_id="$configured_cleanup_project_id"
    fi
  fi
  if ((candidate_worktree_added)); then
    git -C "$worker_root" worktree remove --force "$candidate_root" >/dev/null 2>&1 || true
  fi
  if ((baseline_worktree_added)); then
    git -C "$worker_root" worktree remove --force "$baseline_root" >/dev/null 2>&1 || true
  fi
  if ((stack_started && !keep_environment)); then
    supabase stop --project-id "$cleanup_project_id" --no-backup >/dev/null 2>&1 || true
  fi
  if ((keep_environment)); then
    echo "[benchmark] retained isolated project_id=$project_id workdir=$stack_root" >&2
  else
    rm -rf "$run_root"
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

if docker ps -a --filter "label=com.supabase.cli.project=$project_id" --format '{{.ID}}' | grep -q . \
  || docker volume ls --filter "label=com.supabase.cli.project=$project_id" --format '{{.Name}}' | grep -q . \
  || docker network ls --filter "label=com.supabase.cli.project=$project_id" --format '{{.Name}}' | grep -q .; then
  echo "container, volume, or network already exists for benchmark project; refusing to reuse or delete it: $project_id" >&2
  exit 2
fi

git -C "$database_root" archive "$database_sha" supabase | tar -x -C "$stack_root"
python3 - "$stack_root/supabase/config.toml" \
  "$project_id" "$port_base" "$api_port" "$db_port" "$studio_port" "$mail_port" \
  "$inspector_port" "$analytics_port" "$pooler_port" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
(
    project_id,
    shadow_port,
    api_port,
    db_port,
    studio_port,
    mail_port,
    inspector_port,
    analytics_port,
    pooler_port,
) = sys.argv[2:]
text = path.read_text(encoding="utf-8")
replacements = [
    ('project_id = "database-engine"', f'project_id = "{project_id}"'),
    ("port = 55321", f"port = {api_port}"),
    ("port = 55322", f"port = {db_port}"),
    ("shadow_port = 55320", f"shadow_port = {shadow_port}"),
    ("port = 55329", f"port = {pooler_port}"),
    ("port = 55323", f"port = {studio_port}"),
    ("port = 55324", f"port = {mail_port}"),
    ("inspector_port = 8083", f"inspector_port = {inspector_port}"),
    ("port = 55327", f"port = {analytics_port}"),
]
for old, new in replacements:
    if old not in text:
        raise SystemExit(f"expected config token missing: {old}")
    text = text.replace(old, new, 1)
path.write_text(text, encoding="utf-8")
PY

echo "[benchmark] starting isolated Supabase project_id=$project_id db_port=$db_port api_port=$api_port"
stack_started=1
supabase start \
  --workdir "$stack_root" \
  --exclude gotrue,realtime,imgproxy,mailpit,postgres-meta,studio,edge-runtime,logflare,vector,supavisor \
  --yes >/dev/null
configured_project_id="$(
  sed -n 's/^project_id = "\([^"]*\)"$/\1/p' "$stack_root/supabase/config.toml" | head -n1
)"
if [[ "$configured_project_id" != "$project_id" ]]; then
  echo "Supabase CLI rewrote the benchmark project ID; refusing to continue: requested=$project_id actual=$configured_project_id" >&2
  exit 2
fi

status_env="$run_root/supabase-status.env"
supabase status --workdir "$stack_root" -o env >"$status_env"
set -a
# shellcheck source=/dev/null
source "$status_env"
set +a
export DATABASE_URL="$DB_URL"
export S3_ENDPOINT="$STORAGE_S3_URL"
export S3_REGION="$S3_PROTOCOL_REGION"
export S3_BUCKET="worker-pr161-source-closure-bench"
export S3_ACCESS_KEY_ID="$S3_PROTOCOL_ACCESS_KEY_ID"
export S3_SECRET_ACCESS_KEY="$S3_PROTOCOL_ACCESS_KEY_SECRET"
export S3_PREFIX="worker-pr161-source-closure-bench/$candidate_sha"
export SNAPSHOT_REPORT_MODE="disabled"
export SUPABASE_CLI_VERSION
SUPABASE_CLI_VERSION="$(supabase --version | head -n1)"

git -C "$worker_root" worktree add --detach "$baseline_root" "$baseline_sha" >/dev/null
baseline_worktree_added=1
git -C "$worker_root" worktree add --detach "$candidate_root" "$candidate_sha" >/dev/null
candidate_worktree_added=1
for candidate_path in \
  scripts/run_review_submit_source_closure_benchmark.sh \
  scripts/review_submit_source_closure_benchmark.py \
  crates/solver-worker/tests/scope_closure_package_v2_e2e.rs; do
  if [[ ! -f "$candidate_root/$candidate_path" ]]; then
    echo "candidate exact worktree is missing benchmark harness path: $candidate_path" >&2
    exit 2
  fi
done
if ! grep -q "review_submit_source_closure_benchmark_fixture" \
  "$candidate_root/crates/solver-worker/tests/scope_closure_package_v2_e2e.rs"; then
  echo "candidate exact worktree is missing the benchmark fixture test" >&2
  exit 2
fi
python3 "$candidate_root/scripts/review_submit_source_closure_benchmark.py" --self-test >/dev/null

echo "[benchmark] building exact baseline=$baseline_sha"
CARGO_TARGET_DIR="$shared_target" cargo build \
  --manifest-path "$baseline_root/Cargo.toml" \
  --release -p solver-worker --bin snapshot_builder >/dev/null
cp "$shared_target/release/snapshot_builder" "$binary_dir/snapshot_builder-baseline"

echo "[benchmark] building exact candidate=$candidate_sha"
CARGO_TARGET_DIR="$shared_target" cargo build \
  --manifest-path "$candidate_root/Cargo.toml" \
  --release -p solver-worker --bin snapshot_builder >/dev/null
cp "$shared_target/release/snapshot_builder" "$binary_dir/snapshot_builder-candidate"

echo "[benchmark] seeding fixed Worker-owned fixture"
CARGO_TARGET_DIR="$shared_target" cargo test \
  --manifest-path "$candidate_root/Cargo.toml" \
  -p solver-worker \
  --test scope_closure_package_v2_e2e \
  review_submit_source_closure_benchmark_fixture \
  -- --ignored --exact --nocapture >/dev/null

if [[ -z "$output_path" ]]; then
  output_path="$worker_root/reports/benchmarks/review-submit-source-closure-$candidate_sha.json"
elif [[ "$output_path" != /* ]]; then
  output_path="$worker_root/$output_path"
fi

echo "[benchmark] running $runs cold + $runs hot samples per exact binary"
python3 "$candidate_root/scripts/review_submit_source_closure_benchmark.py" \
  --baseline-bin "$binary_dir/snapshot_builder-baseline" \
  --candidate-bin "$binary_dir/snapshot_builder-candidate" \
  --baseline-ref "$baseline_sha" \
  --candidate-ref "$candidate_sha" \
  --database-schema-ref "$database_sha" \
  --root-process "16000000-0000-4000-8000-000000000011@01.00.000" \
  --fixture-process-id "16000000-0000-4000-8000-000000000010" \
  --project-id "$project_id" \
  --runs "$runs" \
  --output "$output_path"
