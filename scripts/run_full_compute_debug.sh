#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

SNAPSHOT_ID="${SNAPSHOT_ID:-}"
QUEUE_NAME="${PGMQ_QUEUE:-lca_jobs}"
LOG_DIR="${LOG_DIR:-logs/full-run}"
REPORT_DIR="${REPORT_DIR:-reports/full-run}"
TIMEOUT_SEC="${TIMEOUT_SEC:-1800}"
POLL_SEC="${POLL_SEC:-2}"
DEMAND_PROCESS_IDX="${DEMAND_PROCESS_IDX:-0}"
PRINT_LEVEL="${PRINT_LEVEL:-0.0}"
KEEP_WORKER_ALIVE="${KEEP_WORKER_ALIVE:-0}"
BUILD_REPORT_JSON=""
BUILD_TOTAL_SEC=""
BUILD_REUSED_SNAPSHOT=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_full_compute_debug.sh [options]

Options:
  --snapshot-id <uuid>       snapshot id to run (default: latest ready artifact snapshot, then latest snapshot)
  --queue <name>             pgmq queue name (default: lca_jobs)
  --log-dir <dir>            log output directory (default: logs/full-run)
  --report-dir <dir>         report output directory (default: reports/full-run)
  --timeout-sec <sec>        job wait timeout seconds (default: 1800)
  --poll-sec <sec>           job polling interval seconds (default: 2)
  --demand-process-idx <n>   rhs unit demand index (0-based, default: 0)
  --print-level <float>      UMFPACK print level (default: 0.0)
  --keep-worker-alive        do not stop worker when script exits
  -h, --help                 show this help
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --snapshot-id)
      SNAPSHOT_ID="$2"
      shift 2
      ;;
    --queue)
      QUEUE_NAME="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    --report-dir)
      REPORT_DIR="$2"
      shift 2
      ;;
    --timeout-sec)
      TIMEOUT_SEC="$2"
      shift 2
      ;;
    --poll-sec)
      POLL_SEC="$2"
      shift 2
      ;;
    --demand-process-idx)
      DEMAND_PROCESS_IDX="$2"
      shift 2
      ;;
    --print-level)
      PRINT_LEVEL="$2"
      shift 2
      ;;
    --keep-worker-alive)
      KEEP_WORKER_ALIVE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! [[ "$TIMEOUT_SEC" =~ ^[0-9]+$ ]]; then
  echo "TIMEOUT_SEC must be an integer: $TIMEOUT_SEC" >&2
  exit 2
fi

if ! [[ "$POLL_SEC" =~ ^[0-9]+$ ]]; then
  echo "POLL_SEC must be an integer: $POLL_SEC" >&2
  exit 2
fi

if ! [[ "$DEMAND_PROCESS_IDX" =~ ^[0-9]+$ ]]; then
  echo "DEMAND_PROCESS_IDX must be a non-negative integer: $DEMAND_PROCESS_IDX" >&2
  exit 2
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "missing required command: psql" >&2
  exit 1
fi

CARGO_BIN="${CARGO_BIN:-}"
if [ -z "$CARGO_BIN" ]; then
  if command -v cargo >/dev/null 2>&1; then
    CARGO_BIN="$(command -v cargo)"
  elif [ -x "$HOME/.cargo/bin/cargo" ]; then
    CARGO_BIN="$HOME/.cargo/bin/cargo"
  else
    echo "missing required command: cargo" >&2
    exit 1
  fi
fi

DB_URL="${DATABASE_URL:-${CONN:-}}"
if [ -z "$DB_URL" ]; then
  echo "missing DB connection: set DATABASE_URL or CONN" >&2
  exit 1
fi

sql_scalar() {
  psql "$DB_URL" -Atq -v ON_ERROR_STOP=1 -c "$1"
}

sql_scalar_soft() {
  psql "$DB_URL" -Atq -c "$1" 2>/dev/null || true
}

sql_exec() {
  psql "$DB_URL" -v ON_ERROR_STOP=1 -c "$1"
}

is_decimal_number() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

json_number_or_null() {
  local value="$1"
  if is_decimal_number "$value"; then
    printf '%s' "$value"
  else
    printf 'null'
  fi
}

find_latest_snapshot_build_report() {
  find "$ROOT_DIR/reports" -type f -name "${SNAPSHOT_ID}.json" -printf '%T@|%p\n' 2>/dev/null \
    | sort -t'|' -k1,1nr \
    | head -n1 \
    | cut -d'|' -f2-
}

load_build_timing_from_report() {
  local report="$1"
  if [ ! -f "$report" ]; then
    return
  fi

  if command -v jq >/dev/null 2>&1; then
    BUILD_TOTAL_SEC="$(jq -r '.build_timing_sec.total_sec // empty' "$report" 2>/dev/null || true)"
    BUILD_REUSED_SNAPSHOT="$(jq -r '.build_timing_sec.reused_snapshot // empty' "$report" 2>/dev/null || true)"
  else
    BUILD_TOTAL_SEC="$(sed -n 's/.*"total_sec":[[:space:]]*\\([0-9.][0-9.]*\\).*/\\1/p' "$report" | head -n1)"
    BUILD_REUSED_SNAPSHOT="$(sed -n 's/.*"reused_snapshot":[[:space:]]*\\(true\\|false\\).*/\\1/p' "$report" | head -n1)"
  fi

  if ! is_decimal_number "${BUILD_TOTAL_SEC:-}"; then
    BUILD_TOTAL_SEC=""
  fi
  if [ "${BUILD_REUSED_SNAPSHOT:-}" != "true" ] && [ "${BUILD_REUSED_SNAPSHOT:-}" != "false" ]; then
    BUILD_REUSED_SNAPSHOT=""
  fi
}

gen_uuid() {
  cat /proc/sys/kernel/random/uuid
}

if [ -z "$SNAPSHOT_ID" ]; then
  SNAPSHOT_ID="$(sql_scalar_soft "SELECT snapshot_id::text FROM public.lca_snapshot_artifacts WHERE status = 'ready' ORDER BY created_at DESC LIMIT 1;")"
fi

if [ -z "$SNAPSHOT_ID" ]; then
  SNAPSHOT_ID="$(sql_scalar_soft "SELECT id::text FROM public.lca_network_snapshots ORDER BY created_at DESC LIMIT 1;")"
fi

if [ -z "$SNAPSHOT_ID" ]; then
  echo "no snapshot found; run scripts/build_snapshot_from_ilcd.sh first" >&2
  exit 1
fi

BUILD_REPORT_JSON="$(find_latest_snapshot_build_report || true)"
if [ -n "$BUILD_REPORT_JSON" ]; then
  load_build_timing_from_report "$BUILD_REPORT_JSON"
fi

MATRIX_SOURCE=""
ARTIFACT_COUNTS="$(sql_scalar_soft "
SELECT
  process_count::text || '|' ||
  flow_count::text || '|' ||
  a_nnz::text || '|' ||
  b_nnz::text || '|' ||
  c_nnz::text
FROM public.lca_snapshot_artifacts
WHERE snapshot_id = '$SNAPSHOT_ID'::uuid
  AND status = 'ready'
ORDER BY created_at DESC
LIMIT 1;
")"

if [ -n "$ARTIFACT_COUNTS" ]; then
  IFS='|' read -r PROCESS_COUNT FLOW_COUNT A_NNZ B_NNZ C_NNZ <<< "$ARTIFACT_COUNTS"
  MATRIX_SOURCE="artifact_metadata"
else
  PROCESS_COUNT="$(sql_scalar_soft "SELECT COUNT(*)::int FROM public.lca_process_index WHERE snapshot_id = '$SNAPSHOT_ID'::uuid;")"
  FLOW_COUNT="$(sql_scalar_soft "SELECT COUNT(*)::int FROM public.lca_flow_index WHERE snapshot_id = '$SNAPSHOT_ID'::uuid;")"
  A_NNZ="$(sql_scalar_soft "SELECT COUNT(*)::bigint FROM public.lca_technosphere_entries WHERE snapshot_id = '$SNAPSHOT_ID'::uuid;")"
  B_NNZ="$(sql_scalar_soft "SELECT COUNT(*)::bigint FROM public.lca_biosphere_entries WHERE snapshot_id = '$SNAPSHOT_ID'::uuid;")"
  C_NNZ="$(sql_scalar_soft "SELECT COUNT(*)::bigint FROM public.lca_characterization_factors WHERE snapshot_id = '$SNAPSHOT_ID'::uuid;")"
  MATRIX_SOURCE="legacy_tables"
fi

PROCESS_COUNT="${PROCESS_COUNT:-0}"
FLOW_COUNT="${FLOW_COUNT:-0}"
A_NNZ="${A_NNZ:-0}"
B_NNZ="${B_NNZ:-0}"
C_NNZ="${C_NNZ:-0}"

if [ "${PROCESS_COUNT:-0}" -le 0 ]; then
  echo "snapshot $SNAPSHOT_ID has zero processes; cannot run solve" >&2
  exit 1
fi

if [ "${A_NNZ:-0}" -le 0 ]; then
  echo "[warn] snapshot $SNAPSHOT_ID has zero technosphere entries; M defaults to identity (I - 0)" >&2
fi

if [ "$DEMAND_PROCESS_IDX" -ge "$PROCESS_COUNT" ]; then
  echo "demand process index out of range: $DEMAND_PROCESS_IDX (process_count=$PROCESS_COUNT)" >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
mkdir -p "$REPORT_DIR"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_LOG="$LOG_DIR/run-$RUN_TS.log"
WORKER_LOG="$LOG_DIR/worker-$RUN_TS.log"
REPORT_JSON="$REPORT_DIR/run-$RUN_TS.json"
REPORT_MD="$REPORT_DIR/run-$RUN_TS.md"

RUN_START_NS="$(date +%s%N)"
RUN_START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
WORKER_START_NS=""
WORKER_READY_NS=""
PREPARE_ENQUEUE_NS=""
PREPARE_DONE_NS=""
SOLVE_ENQUEUE_NS=""
SOLVE_DONE_NS=""
PREPARE_JOB_ID=""
SOLVE_JOB_ID=""
RESULT_ID=""
RESULT_ARTIFACT_FORMAT=""
RESULT_ARTIFACT_SIZE=""
RESULT_ARTIFACT_URL=""
PREPARE_DB_STATUS=""
PREPARE_DB_QUEUE_WAIT_SEC=""
PREPARE_DB_RUN_SEC=""
PREPARE_DB_END_TO_END_SEC=""
PREPARE_DB_CREATED_UTC=""
PREPARE_DB_STARTED_UTC=""
PREPARE_DB_FINISHED_UTC=""
PREPARE_DB_STATUS_WRITE_RUNNING_SEC=""
PREPARE_DB_STATUS_WRITE_READY_SEC=""
PREPARE_DB_STATUS_WRITE_COMPLETED_SEC=""
PREPARE_DB_STATUS_WRITE_FAILED_SEC=""
PREPARE_DB_STATUS_WRITE_LAST_SEC=""
PREPARE_DB_STATUS_WRITE_LAST_STATUS=""
SOLVE_DB_STATUS=""
SOLVE_DB_QUEUE_WAIT_SEC=""
SOLVE_DB_RUN_SEC=""
SOLVE_DB_END_TO_END_SEC=""
SOLVE_DB_CREATED_UTC=""
SOLVE_DB_STARTED_UTC=""
SOLVE_DB_FINISHED_UTC=""
SOLVE_DB_STATUS_WRITE_RUNNING_SEC=""
SOLVE_DB_STATUS_WRITE_READY_SEC=""
SOLVE_DB_STATUS_WRITE_COMPLETED_SEC=""
SOLVE_DB_STATUS_WRITE_FAILED_SEC=""
SOLVE_DB_STATUS_WRITE_LAST_SEC=""
SOLVE_DB_STATUS_WRITE_LAST_STATUS=""

exec > >(tee -a "$RUN_LOG") 2>&1

echo "[info] run timestamp: $RUN_TS"
echo "[info] snapshot_id: $SNAPSHOT_ID"
echo "[info] queue_name: $QUEUE_NAME"
echo "[info] matrix_source=$MATRIX_SOURCE"
echo "[info] process_count=$PROCESS_COUNT flow_count=$FLOW_COUNT a_nnz=$A_NNZ b_nnz=$B_NNZ c_nnz=$C_NNZ"
if is_decimal_number "${BUILD_TOTAL_SEC:-}"; then
  echo "[info] build_snapshot_sec=$BUILD_TOTAL_SEC"
  if [ -n "$BUILD_REUSED_SNAPSHOT" ]; then
    echo "[info] build_reused_snapshot=$BUILD_REUSED_SNAPSHOT"
  fi
  echo "[info] build_report_json=$BUILD_REPORT_JSON"
else
  echo "[warn] build timing not found for snapshot report: $SNAPSHOT_ID"
fi
echo "[info] logs:"
echo "  run_log=$RUN_LOG"
echo "  worker_log=$WORKER_LOG"
echo "  report_json=$REPORT_JSON"
echo "  report_md=$REPORT_MD"

WORKER_PID=""
WORKER_BIN="$ROOT_DIR/target/release/solver-worker"

worker_needs_build() {
  if [ ! -x "$WORKER_BIN" ]; then
    return 0
  fi

  if [ "$ROOT_DIR/Cargo.lock" -nt "$WORKER_BIN" ] || [ "$ROOT_DIR/Cargo.toml" -nt "$WORKER_BIN" ]; then
    return 0
  fi

  if find "$ROOT_DIR/crates/solver-worker/src" "$ROOT_DIR/crates/solver-core/src" "$ROOT_DIR/crates/suitesparse-ffi/src" \
      -type f -newer "$WORKER_BIN" -print -quit | grep -q .; then
    return 0
  fi

  return 1
}

as_json_string() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

elapsed_or_null() {
  local start="$1"
  local end="$2"
  if [ -n "$start" ] && [ -n "$end" ]; then
    awk -v s="$start" -v e="$end" 'BEGIN{printf "%.6f", (e - s) / 1000000000}'
  else
    printf 'null'
  fi
}

now_ns() {
  date +%s%N
}

empty_to_null() {
  local value="$1"
  if [ -n "$value" ]; then
    printf '%s' "$value"
  else
    printf 'null'
  fi
}

empty_to_json_string_or_null() {
  local value="$1"
  if [ -n "$value" ]; then
    as_json_string "$value"
  else
    printf 'null'
  fi
}

load_job_timing_from_db() {
  local job_id="$1"
  local prefix="$2"
  local line status queue_wait run_sec end_to_end created_utc started_utc finished_utc
  local write_running write_ready write_completed write_failed write_last write_last_status
  line="$(sql_scalar_soft "
  SELECT
    COALESCE(status, ''),
    COALESCE(EXTRACT(EPOCH FROM (started_at - created_at))::text, ''),
    COALESCE(EXTRACT(EPOCH FROM (COALESCE(finished_at, updated_at) - started_at))::text, ''),
    COALESCE(EXTRACT(EPOCH FROM (COALESCE(finished_at, updated_at) - created_at))::text, ''),
    COALESCE(to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), ''),
    COALESCE(to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), ''),
    COALESCE(to_char(COALESCE(finished_at, updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,running_db_write_sec}', ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,ready_db_write_sec}', ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,completed_db_write_sec}', ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,failed_db_write_sec}', ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,last_db_write_sec}', ''),
    COALESCE(diagnostics #>> '{job_status_update_timing_sec,last_status}', '')
  FROM public.lca_jobs
  WHERE id = '$job_id'::uuid
  LIMIT 1;
  ")"

  IFS='|' read -r \
    status queue_wait run_sec end_to_end created_utc started_utc finished_utc \
    write_running write_ready write_completed write_failed write_last write_last_status <<< "$line"
  eval "${prefix}_STATUS=\"\$status\""
  eval "${prefix}_QUEUE_WAIT_SEC=\"\$queue_wait\""
  eval "${prefix}_RUN_SEC=\"\$run_sec\""
  eval "${prefix}_END_TO_END_SEC=\"\$end_to_end\""
  eval "${prefix}_CREATED_UTC=\"\$created_utc\""
  eval "${prefix}_STARTED_UTC=\"\$started_utc\""
  eval "${prefix}_FINISHED_UTC=\"\$finished_utc\""
  eval "${prefix}_STATUS_WRITE_RUNNING_SEC=\"\$write_running\""
  eval "${prefix}_STATUS_WRITE_READY_SEC=\"\$write_ready\""
  eval "${prefix}_STATUS_WRITE_COMPLETED_SEC=\"\$write_completed\""
  eval "${prefix}_STATUS_WRITE_FAILED_SEC=\"\$write_failed\""
  eval "${prefix}_STATUS_WRITE_LAST_SEC=\"\$write_last\""
  eval "${prefix}_STATUS_WRITE_LAST_STATUS=\"\$write_last_status\""
}

write_run_report() {
  local exit_code="$1"
  local run_end_ns run_end_iso run_status total_elapsed
  local worker_start_elapsed prepare_elapsed solve_elapsed
  local build_snapshot_json build_plus_compute_json build_reused_json
  local build_snapshot_md build_plus_compute_md build_reused_md
  local prepare_status solve_status

  run_end_ns="$(now_ns)"
  run_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  total_elapsed="$(elapsed_or_null "$RUN_START_NS" "$run_end_ns")"
  worker_start_elapsed="$(elapsed_or_null "$WORKER_START_NS" "$WORKER_READY_NS")"
  prepare_elapsed="$(elapsed_or_null "$PREPARE_ENQUEUE_NS" "$PREPARE_DONE_NS")"
  solve_elapsed="$(elapsed_or_null "$SOLVE_ENQUEUE_NS" "$SOLVE_DONE_NS")"

  if [ "$exit_code" -eq 0 ]; then
    run_status="completed"
  else
    run_status="failed"
  fi

  prepare_status="not_started"
  solve_status="not_started"
  if [ -n "$PREPARE_JOB_ID" ]; then
    load_job_timing_from_db "$PREPARE_JOB_ID" "PREPARE_DB"
    prepare_status="${PREPARE_DB_STATUS:-unknown}"
  fi
  if [ -n "$SOLVE_JOB_ID" ]; then
    load_job_timing_from_db "$SOLVE_JOB_ID" "SOLVE_DB"
    solve_status="${SOLVE_DB_STATUS:-unknown}"
  fi

  build_snapshot_json="null"
  build_plus_compute_json="null"
  build_reused_json="null"
  build_snapshot_md="n/a"
  build_plus_compute_md="n/a"
  build_reused_md="n/a"
  if is_decimal_number "${BUILD_TOTAL_SEC:-}"; then
    build_snapshot_json="$BUILD_TOTAL_SEC"
    build_plus_compute_json="$(awk -v a="$total_elapsed" -v b="$BUILD_TOTAL_SEC" 'BEGIN{printf "%.6f", a+b}')"
    build_snapshot_md="$BUILD_TOTAL_SEC"
    build_plus_compute_md="$build_plus_compute_json"
  fi
  if [ "$BUILD_REUSED_SNAPSHOT" = "true" ] || [ "$BUILD_REUSED_SNAPSHOT" = "false" ]; then
    build_reused_json="$BUILD_REUSED_SNAPSHOT"
    build_reused_md="$BUILD_REUSED_SNAPSHOT"
  fi

  cat > "$REPORT_JSON" <<JSON
{
  "run_ts": $(as_json_string "$RUN_TS"),
  "run_start_utc": $(as_json_string "$RUN_START_ISO"),
  "run_end_utc": $(as_json_string "$run_end_iso"),
  "status": $(as_json_string "$run_status"),
  "exit_code": $exit_code,
  "snapshot_id": $(as_json_string "$SNAPSHOT_ID"),
  "queue_name": $(as_json_string "$QUEUE_NAME"),
  "demand_process_idx": $DEMAND_PROCESS_IDX,
  "print_level": $PRINT_LEVEL,
  "matrix": {
    "source": $(as_json_string "$MATRIX_SOURCE"),
    "process_count": $PROCESS_COUNT,
    "flow_count": $FLOW_COUNT,
    "a_nnz": $A_NNZ,
    "b_nnz": $B_NNZ,
    "c_nnz": $C_NNZ
  },
  "timing_sec": {
    "total": $total_elapsed,
    "build_snapshot": $build_snapshot_json,
    "build_and_compute_total": $build_plus_compute_json,
    "worker_startup": $worker_start_elapsed,
    "prepare_job": $prepare_elapsed,
    "solve_job": $solve_elapsed
  },
  "job_timing_sec": {
    "prepare": {
      "queue_wait": $(empty_to_null "$PREPARE_DB_QUEUE_WAIT_SEC"),
      "run": $(empty_to_null "$PREPARE_DB_RUN_SEC"),
      "end_to_end": $(empty_to_null "$PREPARE_DB_END_TO_END_SEC")
    },
    "solve": {
      "queue_wait": $(empty_to_null "$SOLVE_DB_QUEUE_WAIT_SEC"),
      "run": $(empty_to_null "$SOLVE_DB_RUN_SEC"),
      "end_to_end": $(empty_to_null "$SOLVE_DB_END_TO_END_SEC")
    }
  },
  "job_db_write_timing_sec": {
    "prepare": {
      "running": $(empty_to_null "$PREPARE_DB_STATUS_WRITE_RUNNING_SEC"),
      "ready": $(empty_to_null "$PREPARE_DB_STATUS_WRITE_READY_SEC"),
      "completed": $(empty_to_null "$PREPARE_DB_STATUS_WRITE_COMPLETED_SEC"),
      "failed": $(empty_to_null "$PREPARE_DB_STATUS_WRITE_FAILED_SEC"),
      "last": $(empty_to_null "$PREPARE_DB_STATUS_WRITE_LAST_SEC"),
      "last_status": $(empty_to_json_string_or_null "$PREPARE_DB_STATUS_WRITE_LAST_STATUS")
    },
    "solve": {
      "running": $(empty_to_null "$SOLVE_DB_STATUS_WRITE_RUNNING_SEC"),
      "ready": $(empty_to_null "$SOLVE_DB_STATUS_WRITE_READY_SEC"),
      "completed": $(empty_to_null "$SOLVE_DB_STATUS_WRITE_COMPLETED_SEC"),
      "failed": $(empty_to_null "$SOLVE_DB_STATUS_WRITE_FAILED_SEC"),
      "last": $(empty_to_null "$SOLVE_DB_STATUS_WRITE_LAST_SEC"),
      "last_status": $(empty_to_json_string_or_null "$SOLVE_DB_STATUS_WRITE_LAST_STATUS")
    }
  },
  "build": {
    "reused_snapshot": $build_reused_json,
    "report_json": $(if [ -n "$BUILD_REPORT_JSON" ]; then as_json_string "$BUILD_REPORT_JSON"; else printf 'null'; fi)
  },
  "jobs": {
    "prepare_job_id": $(as_json_string "$PREPARE_JOB_ID"),
    "prepare_status": $(as_json_string "$prepare_status"),
    "prepare_created_utc": $(empty_to_json_string_or_null "$PREPARE_DB_CREATED_UTC"),
    "prepare_started_utc": $(empty_to_json_string_or_null "$PREPARE_DB_STARTED_UTC"),
    "prepare_finished_utc": $(empty_to_json_string_or_null "$PREPARE_DB_FINISHED_UTC"),
    "solve_job_id": $(as_json_string "$SOLVE_JOB_ID"),
    "solve_status": $(as_json_string "$solve_status"),
    "solve_created_utc": $(empty_to_json_string_or_null "$SOLVE_DB_CREATED_UTC"),
    "solve_started_utc": $(empty_to_json_string_or_null "$SOLVE_DB_STARTED_UTC"),
    "solve_finished_utc": $(empty_to_json_string_or_null "$SOLVE_DB_FINISHED_UTC")
  },
  "result": {
    "id": $(as_json_string "$RESULT_ID"),
    "artifact_format": $(as_json_string "$RESULT_ARTIFACT_FORMAT"),
    "artifact_byte_size": $(if [ -n "$RESULT_ARTIFACT_SIZE" ]; then printf '%s' "$RESULT_ARTIFACT_SIZE"; else printf 'null'; fi),
    "artifact_url": $(if [ -n "$RESULT_ARTIFACT_URL" ]; then as_json_string "$RESULT_ARTIFACT_URL"; else printf 'null'; fi),
    "compute_timing_sec": {
      "comparable_compute_sec": $(json_number_or_null "$RESULT_COMPARABLE_COMPUTE_SEC")
    },
    "persistence_timing_sec": {
      "encode_artifact_sec": $(json_number_or_null "$RESULT_PERSIST_ENCODE_SEC"),
      "upload_artifact_sec": $(json_number_or_null "$RESULT_PERSIST_UPLOAD_SEC"),
      "db_write_sec": $(json_number_or_null "$RESULT_PERSIST_DB_WRITE_SEC"),
      "total_sec": $(json_number_or_null "$RESULT_PERSIST_TOTAL_SEC")
    }
  },
  "paths": {
    "run_log": $(as_json_string "$RUN_LOG"),
    "worker_log": $(as_json_string "$WORKER_LOG")
  }
}
JSON

  cat > "$REPORT_MD" <<MD
# Full Compute Run Report

- run_ts: \`$RUN_TS\`
- status: \`$run_status\` (exit_code=\`$exit_code\`)
- snapshot_id: \`$SNAPSHOT_ID\`
- queue_name: \`$QUEUE_NAME\`
- demand_process_idx: \`$DEMAND_PROCESS_IDX\`

## Timing (sec)

- total: \`$total_elapsed\`
- build_snapshot: \`$build_snapshot_md\`
- build_and_compute_total: \`$build_plus_compute_md\`
- worker_startup: \`$worker_start_elapsed\`
- prepare_job_orchestrator: \`$prepare_elapsed\`
- solve_job_orchestrator: \`$solve_elapsed\`
- prepare_job_queue_wait: \`$PREPARE_DB_QUEUE_WAIT_SEC\`
- prepare_job_run: \`$PREPARE_DB_RUN_SEC\`
- prepare_job_end_to_end: \`$PREPARE_DB_END_TO_END_SEC\`
- solve_job_queue_wait: \`$SOLVE_DB_QUEUE_WAIT_SEC\`
- solve_job_run: \`$SOLVE_DB_RUN_SEC\`
- solve_job_end_to_end: \`$SOLVE_DB_END_TO_END_SEC\`
- prepare_status_write_running_sec: \`$PREPARE_DB_STATUS_WRITE_RUNNING_SEC\`
- prepare_status_write_ready_sec: \`$PREPARE_DB_STATUS_WRITE_READY_SEC\`
- prepare_status_write_completed_sec: \`$PREPARE_DB_STATUS_WRITE_COMPLETED_SEC\`
- prepare_status_write_failed_sec: \`$PREPARE_DB_STATUS_WRITE_FAILED_SEC\`
- prepare_status_write_last_sec: \`$PREPARE_DB_STATUS_WRITE_LAST_SEC\` (\`$PREPARE_DB_STATUS_WRITE_LAST_STATUS\`)
- solve_status_write_running_sec: \`$SOLVE_DB_STATUS_WRITE_RUNNING_SEC\`
- solve_status_write_ready_sec: \`$SOLVE_DB_STATUS_WRITE_READY_SEC\`
- solve_status_write_completed_sec: \`$SOLVE_DB_STATUS_WRITE_COMPLETED_SEC\`
- solve_status_write_failed_sec: \`$SOLVE_DB_STATUS_WRITE_FAILED_SEC\`
- solve_status_write_last_sec: \`$SOLVE_DB_STATUS_WRITE_LAST_SEC\` (\`$SOLVE_DB_STATUS_WRITE_LAST_STATUS\`)

## Build Source

- reused_snapshot: \`$build_reused_md\`
- build_report_json: \`$BUILD_REPORT_JSON\`

## Matrix

- source: \`$MATRIX_SOURCE\`
- process_count: \`$PROCESS_COUNT\`
- flow_count: \`$FLOW_COUNT\`
- a_nnz: \`$A_NNZ\`
- b_nnz: \`$B_NNZ\`
- c_nnz: \`$C_NNZ\`

## Jobs

- prepare_job_id: \`$PREPARE_JOB_ID\`
- prepare_status: \`$prepare_status\`
- prepare_created_utc: \`$PREPARE_DB_CREATED_UTC\`
- prepare_started_utc: \`$PREPARE_DB_STARTED_UTC\`
- prepare_finished_utc: \`$PREPARE_DB_FINISHED_UTC\`
- solve_job_id: \`$SOLVE_JOB_ID\`
- solve_status: \`$solve_status\`
- solve_created_utc: \`$SOLVE_DB_CREATED_UTC\`
- solve_started_utc: \`$SOLVE_DB_STARTED_UTC\`
- solve_finished_utc: \`$SOLVE_DB_FINISHED_UTC\`

## Result

- id: \`$RESULT_ID\`
- artifact_format: \`$RESULT_ARTIFACT_FORMAT\`
- artifact_byte_size: \`$RESULT_ARTIFACT_SIZE\`
- artifact_url: \`$RESULT_ARTIFACT_URL\`
- comparable_compute_sec: \`$RESULT_COMPARABLE_COMPUTE_SEC\`
- persistence_encode_artifact_sec: \`$RESULT_PERSIST_ENCODE_SEC\`
- persistence_upload_artifact_sec: \`$RESULT_PERSIST_UPLOAD_SEC\`
- persistence_db_write_sec: \`$RESULT_PERSIST_DB_WRITE_SEC\`
- persistence_total_sec: \`$RESULT_PERSIST_TOTAL_SEC\`

## Logs

- run_log: \`$RUN_LOG\`
- worker_log: \`$WORKER_LOG\`
MD
}

cleanup() {
  if [ "$KEEP_WORKER_ALIVE" = "1" ]; then
    return
  fi

  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    echo "[info] stopping worker pid=$WORKER_PID"
    kill "$WORKER_PID" >/dev/null 2>&1 || true
    wait "$WORKER_PID" >/dev/null 2>&1 || true
  fi
}

on_exit() {
  local status="$1"
  set +e
  write_run_report "$status"
  cleanup
}
trap 'on_exit $?' EXIT

if worker_needs_build; then
  echo "[info] building release binary: $WORKER_BIN"
  "$CARGO_BIN" build -p solver-worker --release >>"$WORKER_LOG" 2>&1
fi

echo "[info] starting solver-worker in queue mode"
WORKER_START_NS="$(now_ns)"
(
  export DATABASE_URL="$DB_URL"
  export PGMQ_QUEUE="$QUEUE_NAME"
  export SOLVER_MODE=worker
  export RUST_LOG="${RUST_LOG:-info,solver_worker=debug,solver_core=debug}"
  export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
  "$WORKER_BIN"
) >>"$WORKER_LOG" 2>&1 &
WORKER_PID="$!"

sleep 3
if ! kill -0 "$WORKER_PID" >/dev/null 2>&1; then
  echo "[error] worker failed to start, tailing log:" >&2
  tail -n 120 "$WORKER_LOG" >&2 || true
  exit 1
fi
WORKER_READY_NS="$(now_ns)"
echo "[info] worker pid=$WORKER_PID"

enqueue_prepare() {
  local job_id="$1"
  sql_exec "
  WITH payload AS (
    SELECT jsonb_build_object(
      'type', 'prepare_factorization',
      'job_id', '$job_id'::uuid,
      'snapshot_id', '$SNAPSHOT_ID'::uuid,
      'print_level', $PRINT_LEVEL::double precision
    ) AS message
  ),
  ins AS (
    INSERT INTO public.lca_jobs (
      id, job_type, snapshot_id, status, payload, created_at, updated_at
    )
    SELECT
      '$job_id'::uuid,
      'prepare_factorization',
      '$SNAPSHOT_ID'::uuid,
      'queued',
      message,
      NOW(),
      NOW()
    FROM payload
    RETURNING payload
  )
  SELECT pgmq.send('$QUEUE_NAME', payload) AS msg_id
  FROM ins;
  "
}

enqueue_solve() {
  local job_id="$1"
  sql_exec "
  WITH rhs AS (
    SELECT to_jsonb(
      array_agg(
        CASE WHEN idx = $DEMAND_PROCESS_IDX THEN 1.0::double precision ELSE 0.0::double precision END
        ORDER BY idx
      )
    ) AS vec
    FROM generate_series(0, $PROCESS_COUNT - 1) AS idx
  ),
  payload AS (
    SELECT jsonb_build_object(
      'type', 'solve_one',
      'job_id', '$job_id'::uuid,
      'snapshot_id', '$SNAPSHOT_ID'::uuid,
      'rhs', (SELECT vec FROM rhs),
      'solve', jsonb_build_object(
        'return_x', true,
        'return_g', true,
        'return_h', true
      ),
      'print_level', $PRINT_LEVEL::double precision
    ) AS message
  ),
  ins AS (
    INSERT INTO public.lca_jobs (
      id, job_type, snapshot_id, status, payload, created_at, updated_at
    )
    SELECT
      '$job_id'::uuid,
      'solve_one',
      '$SNAPSHOT_ID'::uuid,
      'queued',
      message,
      NOW(),
      NOW()
    FROM payload
    RETURNING payload
  )
  SELECT pgmq.send('$QUEUE_NAME', payload) AS msg_id
  FROM ins;
  "
}

poll_job() {
  local job_id="$1"
  local label="$2"
  local deadline=$(( $(date +%s) + TIMEOUT_SEC ))

  while true; do
    local status
    status="$(sql_scalar "SELECT COALESCE(status, 'missing') FROM public.lca_jobs WHERE id = '$job_id'::uuid;")"
    local updated_at
    updated_at="$(sql_scalar "SELECT COALESCE(to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS'), 'n/a') FROM public.lca_jobs WHERE id = '$job_id'::uuid;")"

    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $label status=$status updated_at=$updated_at"

    case "$status" in
      completed|ready)
        sql_exec "SELECT jsonb_pretty(COALESCE(diagnostics, '{}'::jsonb)) AS diagnostics FROM public.lca_jobs WHERE id = '$job_id'::uuid;"
        return 0
        ;;
      failed)
        sql_exec "SELECT jsonb_pretty(COALESCE(diagnostics, '{}'::jsonb)) AS diagnostics FROM public.lca_jobs WHERE id = '$job_id'::uuid;"
        return 1
        ;;
      missing)
        echo "[error] job not found: $job_id" >&2
        return 1
        ;;
      *)
        ;;
    esac

    if [ "$(date +%s)" -gt "$deadline" ]; then
      echo "[error] timeout waiting for $label (job_id=$job_id, timeout_sec=$TIMEOUT_SEC)" >&2
      return 1
    fi
    sleep "$POLL_SEC"
  done
}

PREPARE_JOB_ID="$(gen_uuid)"
SOLVE_JOB_ID="$(gen_uuid)"

echo "[info] enqueue prepare job: $PREPARE_JOB_ID"
PREPARE_ENQUEUE_NS="$(now_ns)"
enqueue_prepare "$PREPARE_JOB_ID"
poll_job "$PREPARE_JOB_ID" "prepare_factorization"
PREPARE_DONE_NS="$(now_ns)"

echo "[info] enqueue solve job: $SOLVE_JOB_ID (demand_process_idx=$DEMAND_PROCESS_IDX)"
SOLVE_ENQUEUE_NS="$(now_ns)"
enqueue_solve "$SOLVE_JOB_ID"
poll_job "$SOLVE_JOB_ID" "solve_one"
SOLVE_DONE_NS="$(now_ns)"

IFS='|' read -r RESULT_ID RESULT_ARTIFACT_FORMAT RESULT_ARTIFACT_SIZE RESULT_ARTIFACT_URL RESULT_COMPARABLE_COMPUTE_SEC RESULT_PERSIST_ENCODE_SEC RESULT_PERSIST_UPLOAD_SEC RESULT_PERSIST_DB_WRITE_SEC RESULT_PERSIST_TOTAL_SEC < <(
  sql_scalar "
  SELECT
    id::text,
    COALESCE(artifact_format, ''),
    COALESCE(artifact_byte_size::text, ''),
    COALESCE(artifact_url, ''),
    COALESCE(diagnostics #>> '{compute_timing_sec,comparable_compute_sec}', ''),
    COALESCE(diagnostics #>> '{persistence_timing_sec,encode_artifact_sec}', ''),
    COALESCE(diagnostics #>> '{persistence_timing_sec,upload_artifact_sec}', ''),
    COALESCE(diagnostics #>> '{persistence_timing_sec,db_write_sec}', ''),
    COALESCE(diagnostics #>> '{persistence_timing_sec,total_sec}', '')
  FROM public.lca_results
  WHERE job_id = '$SOLVE_JOB_ID'::uuid
  ORDER BY created_at DESC
  LIMIT 1;
  "
)

echo "[info] latest solve result row:"
sql_exec "
SELECT
  id,
  job_id,
  snapshot_id,
  artifact_format,
  artifact_byte_size,
  artifact_url,
  created_at
FROM public.lca_results
WHERE job_id = '$SOLVE_JOB_ID'::uuid
ORDER BY created_at DESC
LIMIT 1;
"

echo "[info] worker log tail:"
tail -n 120 "$WORKER_LOG" || true

if [ "$KEEP_WORKER_ALIVE" = "1" ]; then
  echo "[info] keep-worker-alive enabled; worker pid=$WORKER_PID remains running"
fi

echo "[done] full compute debug run finished"
