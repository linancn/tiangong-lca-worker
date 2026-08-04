from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import bw2calc as bc
import bw_processing as bwp
import h5py
import numpy as np
import psycopg
import requests
from psycopg.rows import dict_row
from scipy import sparse

SNAPSHOT_FORMAT = "snapshot-hdf5:v1"
RESULT_FORMAT = "hdf5:v1"


@dataclass
class TargetResult:
    result_id: str
    job_id: str
    snapshot_id: str
    job_type: str
    result_artifact_url: str | None
    result_artifact_format: str | None
    result_diagnostics: dict[str, Any] | None
    job_payload: dict[str, Any]
    created_at_utc: str


@dataclass
class S3Config:
    endpoint: str | None
    region: str | None
    bucket: str | None
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None

    def is_configured(self) -> bool:
        return all(
            [
                self.endpoint,
                self.region,
                self.access_key_id,
                self.secret_access_key,
            ]
        )


@dataclass
class VectorMetrics:
    size: int
    abs_inf: float
    rel_inf: float
    abs_l2: float
    pass_threshold: bool


@dataclass
class RustJobTiming:
    status: str | None
    queue_wait_sec: float | None
    run_sec: float | None
    end_to_end_sec: float | None


@dataclass
class RustComputeTiming:
    solve_mx_sec: float | None
    bx_sec: float | None
    cg_sec: float | None
    comparable_compute_sec: float | None


@dataclass
class RustPersistenceTiming:
    persist_mode: str | None
    encode_artifact_sec: float | None
    upload_artifact_sec: float | None
    db_write_sec: float | None
    total_sec: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bw25-validate",
        description="Manual Brightway25 cross-validation for solve_one/solve_all_unit artifacts",
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL") or os.getenv("CONN"))
    parser.add_argument("--result-id", default=None)
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument(
        "--job-type",
        choices=["solve_one", "solve_all_unit"],
        default="solve_one",
        help="Used only when selecting latest result by --snapshot-id or default latest lookup",
    )
    parser.add_argument(
        "--report-dir",
        default="reports/bw25-validation",
        help="Output directory for JSON/Markdown reports",
    )
    parser.add_argument("--atol", type=float, default=1e-9)
    parser.add_argument("--rtol", type=float, default=1e-6)
    parser.add_argument("--fail-on-threshold", action="store_true")

    parser.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT"))
    parser.add_argument("--s3-region", default=os.getenv("S3_REGION"))
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--s3-access-key-id", default=os.getenv("S3_ACCESS_KEY_ID"))
    parser.add_argument("--s3-secret-access-key", default=os.getenv("S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3-session-token", default=os.getenv("S3_SESSION_TOKEN"))
    parser.add_argument(
        "--all-unit-max-processes",
        type=int,
        default=None,
        help="Optional cap for solve_all_unit validation (validate first N processes)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("missing DB connection: provide --database-url or DATABASE_URL/CONN")
    if sum(1 for v in [args.result_id, args.job_id] if v) > 1:
        raise SystemExit("use only one of --result-id or --job-id")
    if args.all_unit_max_processes is not None and args.all_unit_max_processes <= 0:
        raise SystemExit("--all-unit-max-processes must be > 0 when provided")

    s3 = S3Config(
        endpoint=args.s3_endpoint,
        region=args.s3_region,
        bucket=args.s3_bucket,
        access_key_id=args.s3_access_key_id,
        secret_access_key=args.s3_secret_access_key,
        session_token=args.s3_session_token,
    )

    total_started = time.perf_counter()
    with psycopg.connect(args.database_url, row_factory=dict_row) as conn:
        resolve_started = time.perf_counter()
        target = resolve_target_result(
            conn, args.result_id, args.job_id, args.snapshot_id, args.job_type
        )
        resolve_sec = time.perf_counter() - resolve_started
        rust_job_timing = fetch_rust_job_timing(conn, target.job_id)
        rust_compute_timing = extract_rust_compute_timing(target.result_diagnostics)
        rust_persistence_timing = extract_rust_persistence_timing(target.result_diagnostics)

        load_started = time.perf_counter()
        result_payload = load_result_payload(target, s3)
        snapshot_payload = load_snapshot_payload(conn, target.snapshot_id, s3)
        load_sec = time.perf_counter() - load_started

    build_started = time.perf_counter()
    n = int(snapshot_payload["process_count"])
    flow_count = int(snapshot_payload["flow_count"])
    impact_count = int(snapshot_payload["impact_count"])
    m, b, c = build_matrices(snapshot_payload)
    build_sec = time.perf_counter() - build_started

    validation: dict[str, Any]
    if target.job_type == "solve_one":
        validation = validate_solve_one(
            m=m,
            b=b,
            c=c,
            n=n,
            result_payload=result_payload,
            job_payload=target.job_payload,
            atol=args.atol,
            rtol=args.rtol,
        )
    elif target.job_type == "solve_all_unit":
        validation = validate_solve_all_unit(
            m=m,
            b=b,
            c=c,
            n=n,
            result_payload=result_payload,
            atol=args.atol,
            rtol=args.rtol,
            max_processes=args.all_unit_max_processes,
        )
    else:
        raise SystemExit(f"unsupported job_type for validation: {target.job_type}")

    x_metrics = validation["x_metrics"]
    g_metrics = validation["g_metrics"]
    h_metrics = validation["h_metrics"]
    lcia_check = validation["lcia_check"]
    bw_residual_rel = validation["bw_residual_rel"]
    rust_residual_rel = validation["rust_residual_rel"]
    solve_sec = float(validation["solve_sec"])
    compare_sec = float(validation["compare_sec"])

    passes = [
        metric["pass_threshold"]
        for metric in [x_metrics, g_metrics, h_metrics]
        if metric is not None
    ]
    if not passes:
        raise SystemExit(
            "no comparable vectors found in result payload (x/g/h all missing)"
        )
    verdict = "pass" if all(passes) else "fail"

    total_sec = time.perf_counter() - total_started
    bw_build_plus_solve_sec = build_sec + solve_sec
    rust_run_vs_bw_solve = safe_ratio(rust_job_timing.run_sec, solve_sec)
    rust_run_vs_bw_build_solve = safe_ratio(rust_job_timing.run_sec, bw_build_plus_solve_sec)
    rust_compute_vs_bw_solve = safe_ratio(rust_compute_timing.comparable_compute_sec, solve_sec)
    rust_compute_vs_bw_build_solve = safe_ratio(
        rust_compute_timing.comparable_compute_sec, bw_build_plus_solve_sec
    )

    primary_ratio = rust_compute_vs_bw_build_solve
    if primary_ratio is None:
        primary_ratio = rust_run_vs_bw_build_solve

    if primary_ratio is not None:
        faster = (
            "rust"
            if primary_ratio < 1.0
            else ("brightway" if primary_ratio > 1.0 else "same")
        )
        faster_factor = (
            (1.0 / primary_ratio)
            if primary_ratio > 0 and primary_ratio < 1.0
            else primary_ratio
        )
        if rust_compute_timing.comparable_compute_sec is not None:
            faster_note = f"{faster} faster (comparable compute)"
        else:
            faster_note = f"{faster} faster (job run)"
        if faster == "same":
            faster_note = "same speed"
    else:
        faster = "unknown"
        faster_factor = None
        faster_note = "unknown"

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    report = {
        "run_utc": now_utc,
        "verdict": verdict,
        "target": {
            "result_id": target.result_id,
            "job_id": target.job_id,
            "snapshot_id": target.snapshot_id,
            "job_type": target.job_type,
            "result_created_at_utc": target.created_at_utc,
        },
        "thresholds": {"atol": args.atol, "rtol": args.rtol},
        "matrix": {
            "process_count": n,
            "flow_count": flow_count,
            "impact_count": impact_count,
            "m_nnz": int(m.nnz),
            "b_nnz": int(b.nnz),
            "c_nnz": int(c.nnz),
        },
        "residual": {
            "bw_rel_inf": bw_residual_rel,
            "rust_rel_inf": rust_residual_rel,
        },
        "comparison": {
            "x": x_metrics,
            "g": g_metrics,
            "h": h_metrics,
            "lcia_score": lcia_check,
        },
        "validation_mode": validation["mode"],
        "validation_scope": validation["scope"],
        "speed_comparison": {
            "rust_job": {
                "status": rust_job_timing.status,
                "queue_wait_sec": rust_job_timing.queue_wait_sec,
                "run_sec": rust_job_timing.run_sec,
                "end_to_end_sec": rust_job_timing.end_to_end_sec,
            },
            "rust_compute": {
                "solve_mx_sec": rust_compute_timing.solve_mx_sec,
                "bx_sec": rust_compute_timing.bx_sec,
                "cg_sec": rust_compute_timing.cg_sec,
                "comparable_compute_sec": rust_compute_timing.comparable_compute_sec,
            },
            "rust_persistence": {
                "persist_mode": rust_persistence_timing.persist_mode,
                "encode_artifact_sec": rust_persistence_timing.encode_artifact_sec,
                "upload_artifact_sec": rust_persistence_timing.upload_artifact_sec,
                "db_write_sec": rust_persistence_timing.db_write_sec,
                "total_sec": rust_persistence_timing.total_sec,
            },
            "brightway": {
                "build_matrices_sec": build_sec,
                "solve_sec": solve_sec,
                "build_plus_solve_sec": bw_build_plus_solve_sec,
            },
            "ratios": {
                "rust_run_over_brightway_solve": rust_run_vs_bw_solve,
                "rust_run_over_brightway_build_plus_solve": rust_run_vs_bw_build_solve,
                "rust_comparable_compute_over_brightway_solve": rust_compute_vs_bw_solve,
                "rust_comparable_compute_over_brightway_build_plus_solve": rust_compute_vs_bw_build_solve,
                "faster_side": faster,
                "faster_factor": faster_factor,
                "note": faster_note,
            },
        },
        "timing_sec": {
            "total": total_sec,
            "resolve_target": resolve_sec,
            "load_artifacts": load_sec,
            "build_matrices": build_sec,
            "brightway_solve": solve_sec,
            "compare": compare_sec,
        },
    }

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{target.result_id}"
    json_path = report_dir / f"{stem}.json"
    md_path = report_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_markdown_report(report), encoding="utf-8")

    print(f"[done] bw25 validation completed: verdict={verdict}")
    print(f"[report] json={json_path}")
    print(f"[report] md={md_path}")
    print(f"[timing] total_sec={total_sec:.6f}")
    print(
        "[speed] rust_job_run_sec={} rust_comparable_compute_sec={} brightway_solve_sec={:.6f} brightway_build_plus_solve_sec={:.6f}".format(
            format_optional_float(rust_job_timing.run_sec),
            format_optional_float(rust_compute_timing.comparable_compute_sec),
            solve_sec,
            bw_build_plus_solve_sec,
        )
    )
    print(
        "[speed] rust_over_bw_solve={} rust_over_bw_build_plus_solve={} rust_compute_over_bw_solve={} rust_compute_over_bw_build_plus_solve={} faster={}".format(
            format_optional_float(rust_run_vs_bw_solve),
            format_optional_float(rust_run_vs_bw_build_solve),
            format_optional_float(rust_compute_vs_bw_solve),
            format_optional_float(rust_compute_vs_bw_build_solve),
            faster_note,
        )
    )

    if args.fail_on_threshold and verdict != "pass":
        raise SystemExit(3)


def resolve_target_result(
    conn: psycopg.Connection[Any],
    result_id: str | None,
    job_id: str | None,
    snapshot_id: str | None,
    selected_job_type: str,
) -> TargetResult:
    if result_id:
        query = """
            SELECT
                r.id::text AS result_id,
                r.job_id::text AS job_id,
                r.snapshot_id::text AS snapshot_id,
                j.job_type AS job_type,
                r.diagnostics AS result_diagnostics,
                r.artifact_url AS result_artifact_url,
                r.artifact_format AS result_artifact_format,
                j.payload AS job_payload,
                to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at_utc
            FROM public.lca_results r
            JOIN public.lca_jobs j ON j.id = r.job_id
            WHERE r.id = %s::uuid
            LIMIT 1
        """
        row = conn.execute(query, (result_id,)).fetchone()
    elif job_id:
        query = """
            SELECT
                r.id::text AS result_id,
                r.job_id::text AS job_id,
                r.snapshot_id::text AS snapshot_id,
                j.job_type AS job_type,
                r.diagnostics AS result_diagnostics,
                r.artifact_url AS result_artifact_url,
                r.artifact_format AS result_artifact_format,
                j.payload AS job_payload,
                to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at_utc
            FROM public.lca_results r
            JOIN public.lca_jobs j ON j.id = r.job_id
            WHERE r.job_id = %s::uuid
            ORDER BY r.created_at DESC
            LIMIT 1
        """
        row = conn.execute(query, (job_id,)).fetchone()
    else:
        params: tuple[Any, ...]
        if snapshot_id:
            query = """
                SELECT
                    r.id::text AS result_id,
                    r.job_id::text AS job_id,
                    r.snapshot_id::text AS snapshot_id,
                    j.job_type AS job_type,
                    r.diagnostics AS result_diagnostics,
                    r.artifact_url AS result_artifact_url,
                    r.artifact_format AS result_artifact_format,
                    j.payload AS job_payload,
                    to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at_utc
                FROM public.lca_results r
                JOIN public.lca_jobs j ON j.id = r.job_id
                WHERE r.snapshot_id = %s::uuid
                  AND j.job_type = %s
                ORDER BY r.created_at DESC
                LIMIT 1
            """
            params = (snapshot_id, selected_job_type)
        else:
            query = """
                SELECT
                    r.id::text AS result_id,
                    r.job_id::text AS job_id,
                    r.snapshot_id::text AS snapshot_id,
                    j.job_type AS job_type,
                    r.diagnostics AS result_diagnostics,
                    r.artifact_url AS result_artifact_url,
                    r.artifact_format AS result_artifact_format,
                    j.payload AS job_payload,
                    to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at_utc
                FROM public.lca_results r
                JOIN public.lca_jobs j ON j.id = r.job_id
                WHERE j.job_type = %s
                ORDER BY r.created_at DESC
                LIMIT 1
            """
            params = (selected_job_type,)
        row = conn.execute(query, params).fetchone()

    if not row:
        raise SystemExit("no result row found for selected target")
    if row["job_type"] not in {"solve_one", "solve_all_unit"}:
        raise SystemExit(
            "unsupported job_type for bw25 validator: "
            f"{row['job_type']} (supported: solve_one, solve_all_unit)"
        )

    return TargetResult(
        result_id=row["result_id"],
        job_id=row["job_id"],
        snapshot_id=row["snapshot_id"],
        job_type=row["job_type"],
        result_diagnostics=as_json_dict(row["result_diagnostics"]),
        result_artifact_url=row["result_artifact_url"],
        result_artifact_format=row["result_artifact_format"],
        job_payload=as_json_dict(row["job_payload"]) or {},
        created_at_utc=row["created_at_utc"] or "",
    )


def fetch_rust_job_timing(conn: psycopg.Connection[Any], job_id: str) -> RustJobTiming:
    row = conn.execute(
        """
        SELECT
            status,
            EXTRACT(EPOCH FROM (started_at - created_at)) AS queue_wait_sec,
            EXTRACT(EPOCH FROM (finished_at - started_at)) AS run_sec,
            EXTRACT(EPOCH FROM (finished_at - created_at)) AS end_to_end_sec
        FROM public.lca_jobs
        WHERE id = %s::uuid
        LIMIT 1
        """,
        (job_id,),
    ).fetchone()
    if not row:
        return RustJobTiming(
            status=None,
            queue_wait_sec=None,
            run_sec=None,
            end_to_end_sec=None,
        )
    return RustJobTiming(
        status=str(row["status"]) if row["status"] is not None else None,
        queue_wait_sec=as_optional_float(row["queue_wait_sec"]),
        run_sec=as_optional_float(row["run_sec"]),
        end_to_end_sec=as_optional_float(row["end_to_end_sec"]),
    )


def extract_rust_compute_timing(
    result_diagnostics: dict[str, Any] | None,
) -> RustComputeTiming:
    if not result_diagnostics:
        return RustComputeTiming(None, None, None, None)

    timing = result_diagnostics.get("compute_timing_sec")
    if not isinstance(timing, dict):
        return RustComputeTiming(None, None, None, None)

    solve_mx_sec = as_optional_float(timing.get("solve_mx_sec"))
    bx_sec = as_optional_float(timing.get("bx_sec"))
    cg_sec = as_optional_float(timing.get("cg_sec"))
    comparable_compute_sec = as_optional_float(timing.get("comparable_compute_sec"))
    if comparable_compute_sec is None and solve_mx_sec is not None:
        comparable_compute_sec = (
            solve_mx_sec + (bx_sec or 0.0) + (cg_sec or 0.0)
        )

    return RustComputeTiming(
        solve_mx_sec=solve_mx_sec,
        bx_sec=bx_sec,
        cg_sec=cg_sec,
        comparable_compute_sec=comparable_compute_sec,
    )


def extract_rust_persistence_timing(
    result_diagnostics: dict[str, Any] | None,
) -> RustPersistenceTiming:
    if not result_diagnostics:
        return RustPersistenceTiming(None, None, None, None, None)

    persist_mode = result_diagnostics.get("persist_mode")
    persist_mode_str = str(persist_mode) if persist_mode is not None else None

    timing = result_diagnostics.get("persistence_timing_sec")
    if not isinstance(timing, dict):
        return RustPersistenceTiming(persist_mode_str, None, None, None, None)

    return RustPersistenceTiming(
        persist_mode=persist_mode_str,
        encode_artifact_sec=as_optional_float(timing.get("encode_artifact_sec")),
        upload_artifact_sec=as_optional_float(timing.get("upload_artifact_sec")),
        db_write_sec=as_optional_float(timing.get("db_write_sec")),
        total_sec=as_optional_float(timing.get("total_sec")),
    )


def load_result_payload(target: TargetResult, s3: S3Config) -> dict[str, Any]:
    if not target.result_artifact_url:
        raise SystemExit("result row has no artifact_url")
    if target.result_artifact_format and target.result_artifact_format != RESULT_FORMAT:
        raise SystemExit(
            "unsupported result artifact format: "
            f"{target.result_artifact_format} (expected {RESULT_FORMAT})"
        )

    bytes_data = download_object_url(target.result_artifact_url, s3)
    envelope = decode_hdf5_envelope(bytes_data)
    if envelope["format"] != RESULT_FORMAT:
        raise SystemExit(
            f"unexpected result artifact format: {envelope['format']} (expected {RESULT_FORMAT})"
        )
    payload = envelope["envelope"].get("payload")
    if not isinstance(payload, dict):
        raise SystemExit("invalid result artifact payload")
    return payload


def extract_result_payload_object(payload: dict[str, Any]) -> dict[str, Any]:
    if "items" in payload:
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise SystemExit("solve_batch payload has empty items")
        first = items[0]
        if not isinstance(first, dict):
            raise SystemExit("invalid solve_batch item in payload")
        return first
    return payload


def load_snapshot_payload(
    conn: psycopg.Connection[Any],
    snapshot_id: str,
    s3: S3Config,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT artifact_url, artifact_format
        FROM public.lca_snapshot_artifacts
        WHERE snapshot_id = %s::uuid
          AND status = 'ready'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (snapshot_id,),
    ).fetchone()

    if not row:
        raise SystemExit(f"no ready snapshot artifact for snapshot_id={snapshot_id}")
    artifact_url = row["artifact_url"]
    artifact_format = row["artifact_format"]
    if artifact_format != SNAPSHOT_FORMAT:
        raise SystemExit(
            f"unsupported snapshot artifact format: {artifact_format} (expected {SNAPSHOT_FORMAT})"
        )
    bytes_data = download_object_url(artifact_url, s3)
    envelope = decode_hdf5_envelope(bytes_data)
    if envelope["format"] != SNAPSHOT_FORMAT:
        raise SystemExit(
            f"unexpected snapshot envelope format: {envelope['format']} (expected {SNAPSHOT_FORMAT})"
        )
    payload = envelope["envelope"].get("payload")
    if not isinstance(payload, dict):
        raise SystemExit("invalid snapshot envelope payload")
    return payload


def extract_rhs(job_payload: dict[str, Any]) -> np.ndarray:
    rhs = job_payload.get("rhs")
    if not isinstance(rhs, list):
        raise SystemExit("job payload has no solve_one rhs array")
    vector = as_float_vector(rhs)
    if vector is None:
        return np.array([], dtype=np.float64)
    return vector


def extract_result_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise SystemExit("solve_all_unit result payload has empty items")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise SystemExit(f"invalid solve_all_unit item at index={idx}")
        out.append(item)
    return out


def validate_solve_one(
    *,
    m: sparse.csc_matrix,
    b: sparse.csc_matrix,
    c: sparse.csc_matrix,
    n: int,
    result_payload: dict[str, Any],
    job_payload: dict[str, Any],
    atol: float,
    rtol: float,
) -> dict[str, Any]:
    result = extract_result_payload_object(result_payload)
    rhs = extract_rhs(job_payload)
    if rhs.shape[0] != n:
        raise SystemExit(f"rhs length mismatch: rhs={rhs.shape[0]} process_count={n}")

    solve_started = time.perf_counter()
    x_bw, bw_score = run_brightway_lca(m, b, c, rhs)
    g_bw = (b @ x_bw).astype(np.float64)
    h_bw = (c @ g_bw).astype(np.float64)
    solve_sec = time.perf_counter() - solve_started

    compare_started = time.perf_counter()
    rust_x = as_float_vector(result.get("x"))
    rust_g = as_float_vector(result.get("g"))
    rust_h = as_float_vector(result.get("h"))
    x_metrics = compare_vector("x", rust_x, x_bw, atol, rtol)
    g_metrics = compare_vector("g", rust_g, g_bw, atol, rtol)
    h_metrics = compare_vector("h", rust_h, h_bw, atol, rtol)

    bw_residual_rel = normalized_residual(m, x_bw, rhs)
    rust_residual_rel = normalized_residual(m, rust_x, rhs) if rust_x is not None else None
    lcia_check = None
    if bw_score is not None and rust_h is not None and rust_h.size > 0:
        abs_delta = float(abs(rust_h[0] - bw_score))
        rel_delta = float(abs_delta / max(abs(rust_h[0]), 1.0))
        lcia_check = {
            "bw_score": float(bw_score),
            "rust_h0": float(rust_h[0]),
            "abs_delta": abs_delta,
            "rel_delta": rel_delta,
            "pass_threshold": bool(abs_delta <= atol or rel_delta <= rtol),
            "considered_for_verdict": False,
        }
    compare_sec = time.perf_counter() - compare_started

    return {
        "mode": "solve_one",
        "scope": {
            "compared_process_count": 1,
            "total_process_count": n,
        },
        "x_metrics": x_metrics,
        "g_metrics": g_metrics,
        "h_metrics": h_metrics,
        "lcia_check": lcia_check,
        "bw_residual_rel": bw_residual_rel,
        "rust_residual_rel": rust_residual_rel,
        "solve_sec": solve_sec,
        "compare_sec": compare_sec,
    }


def validate_solve_all_unit(
    *,
    m: sparse.csc_matrix,
    b: sparse.csc_matrix,
    c: sparse.csc_matrix,
    n: int,
    result_payload: dict[str, Any],
    atol: float,
    rtol: float,
    max_processes: int | None,
) -> dict[str, Any]:
    items = extract_result_items(result_payload)
    if len(items) != n:
        raise SystemExit(
            f"solve_all_unit item count mismatch: items={len(items)} process_count={n}"
        )

    compared_count = min(n, max_processes or n)
    if compared_count <= 0:
        raise SystemExit("solve_all_unit has no process selected for validation")
    indices = list(range(compared_count))
    selected_items = [items[idx] for idx in indices]

    compare_x = any(item.get("x") is not None for item in selected_items)
    compare_g = any(item.get("g") is not None for item in selected_items)
    compare_h = any(item.get("h") is not None for item in selected_items)

    if compare_x and not all(item.get("x") is not None for item in selected_items):
        raise SystemExit("solve_all_unit payload has mixed x presence across items")
    if compare_g and not all(item.get("g") is not None for item in selected_items):
        raise SystemExit("solve_all_unit payload has mixed g presence across items")
    if compare_h and not all(item.get("h") is not None for item in selected_items):
        raise SystemExit("solve_all_unit payload has mixed h presence across items")

    x_agg = init_metric_aggregate()
    g_agg = init_metric_aggregate()
    h_agg = init_metric_aggregate()

    lcia_count = 0
    lcia_abs_max = 0.0
    lcia_rel_max = 0.0
    lcia_pass = True
    lcia_worst_idx: int | None = None
    lcia_worst_bw: float | None = None
    lcia_worst_rust: float | None = None

    bw_residual_rel_max: float | None = None
    rust_residual_rel_max: float | None = None

    dp = bwp.create_datapackage()
    add_sparse_matrix_to_datapackage(dp, "technosphere_matrix", m)
    add_sparse_matrix_to_datapackage(dp, "biosphere_matrix", b)
    if c.nnz > 0:
        add_sparse_matrix_to_datapackage(dp, "characterization_matrix", c)

    lca = bc.LCA(
        demand={indices[0]: 1.0},
        data_objs=[dp],
        use_arrays=False,
        use_distributions=False,
    )

    solve_sec = 0.0
    compare_sec = 0.0

    for pos, process_index in enumerate(indices):
        solve_started = time.perf_counter()
        demand = {process_index: 1.0}
        if pos == 0:
            lca.lci()
        else:
            lca.redo_lci(demand)

        x_bw = np.array(lca.supply_array, dtype=np.float64).reshape(-1)
        g_bw = (b @ x_bw).astype(np.float64)
        h_bw = (c @ g_bw).astype(np.float64)
        solve_sec += time.perf_counter() - solve_started

        compare_started = time.perf_counter()
        rust_item = selected_items[pos]
        rust_x = as_float_vector(rust_item.get("x"))
        rust_g = as_float_vector(rust_item.get("g"))
        rust_h = as_float_vector(rust_item.get("h"))

        x_metric = compare_vector("x", rust_x, x_bw, atol, rtol)
        g_metric = compare_vector("g", rust_g, g_bw, atol, rtol)
        h_metric = compare_vector("h", rust_h, h_bw, atol, rtol)

        update_metric_aggregate(x_agg, x_metric, process_index)
        update_metric_aggregate(g_agg, g_metric, process_index)
        update_metric_aggregate(h_agg, h_metric, process_index)

        if rust_x is not None:
            rhs = np.zeros(n, dtype=np.float64)
            rhs[process_index] = 1.0
            bw_res = normalized_residual(m, x_bw, rhs)
            rust_res = normalized_residual(m, rust_x, rhs)
            if bw_res is not None:
                bw_residual_rel_max = (
                    bw_res
                    if bw_residual_rel_max is None
                    else max(bw_residual_rel_max, bw_res)
                )
            if rust_res is not None:
                rust_residual_rel_max = (
                    rust_res
                    if rust_residual_rel_max is None
                    else max(rust_residual_rel_max, rust_res)
                )

        if h_bw.size > 0 and rust_h is not None and rust_h.size > 0:
            bw_h0 = float(h_bw[0])
            rust_h0 = float(rust_h[0])
            abs_delta = float(abs(rust_h0 - bw_h0))
            rel_delta = float(abs_delta / max(abs(rust_h0), 1.0))
            lcia_count += 1
            lcia_pass = lcia_pass and bool(abs_delta <= atol or rel_delta <= rtol)
            if abs_delta >= lcia_abs_max:
                lcia_abs_max = abs_delta
                lcia_rel_max = rel_delta
                lcia_worst_idx = process_index
                lcia_worst_bw = bw_h0
                lcia_worst_rust = rust_h0

        compare_sec += time.perf_counter() - compare_started

    lcia_check = None
    if lcia_count > 0:
        lcia_check = {
            "bw_score": lcia_worst_bw,
            "rust_h0": lcia_worst_rust,
            "abs_delta": lcia_abs_max,
            "rel_delta": lcia_rel_max,
            "pass_threshold": lcia_pass,
            "considered_for_verdict": False,
            "compared_count": lcia_count,
            "worst_process_index": lcia_worst_idx,
        }

    return {
        "mode": "solve_all_unit",
        "scope": {
            "compared_process_count": compared_count,
            "total_process_count": n,
            "max_processes": max_processes,
        },
        "x_metrics": finalize_metric_aggregate(x_agg),
        "g_metrics": finalize_metric_aggregate(g_agg),
        "h_metrics": finalize_metric_aggregate(h_agg),
        "lcia_check": lcia_check,
        "bw_residual_rel": bw_residual_rel_max,
        "rust_residual_rel": rust_residual_rel_max,
        "solve_sec": solve_sec,
        "compare_sec": compare_sec,
    }


def init_metric_aggregate() -> dict[str, Any]:
    return {
        "count": 0,
        "size": None,
        "abs_inf": 0.0,
        "rel_inf": 0.0,
        "abs_l2": 0.0,
        "pass_threshold": True,
        "worst_process_index": None,
    }


def update_metric_aggregate(
    aggregate: dict[str, Any],
    metric: dict[str, Any] | None,
    process_index: int,
) -> None:
    if metric is None:
        return

    aggregate["count"] = int(aggregate["count"]) + 1
    if aggregate["size"] is None:
        aggregate["size"] = int(metric["size"])

    metric_abs_inf = float(metric["abs_inf"])
    metric_rel_inf = float(metric["rel_inf"])
    metric_abs_l2 = float(metric["abs_l2"])

    if metric_abs_inf >= float(aggregate["abs_inf"]):
        aggregate["abs_inf"] = metric_abs_inf
        aggregate["worst_process_index"] = process_index
    aggregate["rel_inf"] = max(float(aggregate["rel_inf"]), metric_rel_inf)
    aggregate["abs_l2"] = max(float(aggregate["abs_l2"]), metric_abs_l2)
    aggregate["pass_threshold"] = bool(
        aggregate["pass_threshold"] and bool(metric["pass_threshold"])
    )


def finalize_metric_aggregate(
    aggregate: dict[str, Any],
) -> dict[str, Any] | None:
    count = int(aggregate["count"])
    if count <= 0:
        return None
    return {
        "size": int(aggregate["size"]),
        "compared_count": count,
        "abs_inf": float(aggregate["abs_inf"]),
        "rel_inf": float(aggregate["rel_inf"]),
        "abs_l2": float(aggregate["abs_l2"]),
        "pass_threshold": bool(aggregate["pass_threshold"]),
        "worst_process_index": aggregate["worst_process_index"],
    }


def build_matrices(
    snapshot_payload: dict[str, Any],
) -> tuple[sparse.csc_matrix, sparse.csc_matrix, sparse.csc_matrix]:
    process_count = int(snapshot_payload["process_count"])
    flow_count = int(snapshot_payload["flow_count"])
    impact_count = int(snapshot_payload["impact_count"])

    a = triplets_to_sparse(
        snapshot_payload.get("technosphere_entries", []),
        process_count,
        process_count,
    )
    b = triplets_to_sparse(
        snapshot_payload.get("biosphere_entries", []), flow_count, process_count
    )
    c = triplets_to_sparse(
        snapshot_payload.get("characterization_factors", []),
        impact_count,
        flow_count,
    )

    m = sparse.eye(process_count, format="csc", dtype=np.float64) - a
    return m, b, c


def triplets_to_sparse(
    entries: Any,
    nrows: int,
    ncols: int,
) -> sparse.csc_matrix:
    if not isinstance(entries, list) or not entries:
        return sparse.csc_matrix((nrows, ncols), dtype=np.float64)

    rows: list[int] = []
    cols: list[int] = []
    vals: list[float] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        row = int(item.get("row", 0))
        col = int(item.get("col", 0))
        value = float(item.get("value", 0.0))
        rows.append(row)
        cols.append(col)
        vals.append(value)

    coo = sparse.coo_matrix((np.array(vals), (np.array(rows), np.array(cols))), shape=(nrows, ncols))
    return coo.tocsc()


def run_brightway_lca(
    m: sparse.csc_matrix,
    b: sparse.csc_matrix,
    c: sparse.csc_matrix,
    rhs: np.ndarray,
) -> tuple[np.ndarray, float | None]:
    dp = bwp.create_datapackage()
    add_sparse_matrix_to_datapackage(dp, "technosphere_matrix", m)
    add_sparse_matrix_to_datapackage(dp, "biosphere_matrix", b)
    if c.nnz > 0:
        add_sparse_matrix_to_datapackage(dp, "characterization_matrix", c)

    demand = {int(idx): float(value) for idx, value in enumerate(rhs.tolist()) if abs(value) > 0.0}
    if not demand:
        demand = {0: 0.0}

    lca = bc.LCA(demand=demand, data_objs=[dp], use_arrays=False, use_distributions=False)
    lca.lci()
    x_bw = np.array(lca.supply_array, dtype=np.float64).reshape(-1)

    bw_score = None
    if c.nnz > 0:
        lca.lcia()
        bw_score = float(lca.score)

    return x_bw, bw_score


def add_sparse_matrix_to_datapackage(
    dp: bwp.Datapackage,
    matrix_name: str,
    mat: sparse.csc_matrix,
) -> None:
    coo = mat.tocoo()
    indices = np.zeros(coo.nnz, dtype=bwp.INDICES_DTYPE)
    if coo.nnz > 0:
        indices["row"] = coo.row.astype(np.int64)
        indices["col"] = coo.col.astype(np.int64)
    data = coo.data.astype(np.float64, copy=False)
    dp.add_persistent_vector(
        matrix=matrix_name,
        indices_array=indices,
        data_array=data,
        name=f"{matrix_name}-vector",
    )


def compare_vector(
    name: str,
    rust_vec: np.ndarray | None,
    bw_vec: np.ndarray,
    atol: float,
    rtol: float,
) -> dict[str, Any] | None:
    if rust_vec is None:
        return None
    if rust_vec.shape != bw_vec.shape:
        raise SystemExit(f"shape mismatch for {name}: rust={rust_vec.shape} bw={bw_vec.shape}")
    diff = rust_vec - bw_vec
    abs_inf = float(np.linalg.norm(diff, ord=np.inf))
    ref_scale = max(float(np.linalg.norm(rust_vec, ord=np.inf)), 1.0)
    rel_inf = float(abs_inf / ref_scale)
    abs_l2 = float(np.linalg.norm(diff))
    passed = bool(abs_inf <= atol or rel_inf <= rtol)
    return {
        "size": int(rust_vec.size),
        "abs_inf": abs_inf,
        "rel_inf": rel_inf,
        "abs_l2": abs_l2,
        "pass_threshold": passed,
    }


def normalized_residual(
    m: sparse.csc_matrix,
    x: np.ndarray | None,
    rhs: np.ndarray,
) -> float | None:
    if x is None:
        return None
    residual = (m @ x) - rhs
    num = float(np.linalg.norm(residual, ord=np.inf))
    m_norm_inf = float(sparse.linalg.norm(m, ord=np.inf))
    denom = float(m_norm_inf * np.linalg.norm(x, ord=np.inf) + np.linalg.norm(rhs, ord=np.inf))
    if denom <= 0:
        return num
    return num / denom


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator / denominator)


def as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def format_optional_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def as_json_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return None


def as_float_vector(value: Any) -> np.ndarray | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise SystemExit("expected vector list in payload")
    return np.array([float(v) for v in value], dtype=np.float64)


def download_object_url(url: str, s3: S3Config) -> bytes:
    if s3.is_configured():
        parsed = urlparse(url)
        path_parts = [part for part in parsed.path.split("/") if part]
        parsed_bucket: str | None = None
        parsed_key: str | None = None

        # Supabase S3 path-style URL:
        # /storage/v1/s3/<bucket>/<key...>
        if len(path_parts) >= 5 and path_parts[:3] == ["storage", "v1", "s3"]:
            parsed_bucket = path_parts[3]
            parsed_key = "/".join(path_parts[4:])
        elif len(path_parts) >= 2:
            parsed_bucket = path_parts[0]
            parsed_key = "/".join(path_parts[1:])

        if not parsed_bucket or not parsed_key:
            raise SystemExit(f"cannot parse bucket/key from object URL path: {url}")

        bucket = s3.bucket or parsed_bucket
        key = parsed_key
        client = boto3.client(
            "s3",
            endpoint_url=s3.endpoint,
            region_name=s3.region,
            aws_access_key_id=s3.access_key_id,
            aws_secret_access_key=s3.secret_access_key,
            aws_session_token=s3.session_token,
        )
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        return bytes(body)

    response = requests.get(url, timeout=120)
    response.raise_for_status()
    return response.content


def decode_hdf5_envelope(raw: bytes) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(prefix="bw25-", suffix=".h5") as tmp:
        tmp.write(raw)
        tmp.flush()
        with h5py.File(tmp.name, "r") as file:
            fmt_bytes = np.array(file["format"][:], dtype=np.uint8).tobytes()
            envelope_bytes = np.array(file["envelope_json"][:], dtype=np.uint8).tobytes()
    return {
        "format": fmt_bytes.decode("utf-8"),
        "envelope": json.loads(envelope_bytes.decode("utf-8")),
    }


def render_markdown_report(report: dict[str, Any]) -> str:
    target = report["target"]
    threshold = report["thresholds"]
    matrix = report["matrix"]
    timing = report["timing_sec"]
    residual = report["residual"]
    compare = report["comparison"]
    speed = report["speed_comparison"]
    validation_mode = report.get("validation_mode")
    validation_scope = report.get("validation_scope")

    def metric_line(name: str, metric: dict[str, Any] | None) -> str:
        if metric is None:
            return f"- {name}: `n/a`"
        extra = ""
        if metric.get("compared_count") is not None:
            extra += f" compared_count=`{metric['compared_count']}`"
        if metric.get("worst_process_index") is not None:
            extra += f" worst_process_index=`{metric['worst_process_index']}`"
        return (
            f"- {name}: pass=`{metric['pass_threshold']}` "
            f"abs_inf=`{metric['abs_inf']:.6e}` rel_inf=`{metric['rel_inf']:.6e}` abs_l2=`{metric['abs_l2']:.6e}`{extra}"
        )

    lcia = compare.get("lcia_score")
    lcia_line = "- lcia_score: `n/a`"
    if lcia is not None:
        lcia_line = (
            f"- lcia_score: pass=`{lcia['pass_threshold']}` considered_for_verdict=`{lcia['considered_for_verdict']}` "
            f"abs_delta=`{lcia['abs_delta']:.6e}` rel_delta=`{lcia['rel_delta']:.6e}` "
            f"bw_score=`{lcia['bw_score']:.6e}` rust_h0=`{lcia['rust_h0']:.6e}`"
        )

    return "\n".join(
        [
            "# Brightway25 Validation Report",
            "",
            f"- run_utc: `{report['run_utc']}`",
            f"- verdict: `{report['verdict']}`",
            f"- result_id: `{target['result_id']}`",
            f"- job_id: `{target['job_id']}`",
            f"- snapshot_id: `{target['snapshot_id']}`",
            f"- job_type: `{target['job_type']}`",
            f"- validation_mode: `{validation_mode}`",
            f"- validation_scope: `{validation_scope}`",
            "",
            "## Thresholds",
            "",
            f"- atol: `{threshold['atol']}`",
            f"- rtol: `{threshold['rtol']}`",
            "",
            "## Matrix",
            "",
            f"- process_count: `{matrix['process_count']}`",
            f"- flow_count: `{matrix['flow_count']}`",
            f"- impact_count: `{matrix['impact_count']}`",
            f"- m_nnz: `{matrix['m_nnz']}`",
            f"- b_nnz: `{matrix['b_nnz']}`",
            f"- c_nnz: `{matrix['c_nnz']}`",
            "",
            "## Comparison",
            "",
            metric_line("x", compare.get("x")),
            metric_line("g", compare.get("g")),
            metric_line("h", compare.get("h")),
            lcia_line,
            "",
            "## Residual",
            "",
            f"- bw_rel_inf: `{residual['bw_rel_inf']}`",
            f"- rust_rel_inf: `{residual['rust_rel_inf']}`",
            "",
            "## Speed Comparison",
            "",
            f"- rust_job_status: `{speed['rust_job']['status']}`",
            f"- rust_job_queue_wait_sec: `{speed['rust_job']['queue_wait_sec']}`",
            f"- rust_job_run_sec: `{speed['rust_job']['run_sec']}`",
            f"- rust_job_end_to_end_sec: `{speed['rust_job']['end_to_end_sec']}`",
            f"- rust_compute_solve_mx_sec: `{speed['rust_compute']['solve_mx_sec']}`",
            f"- rust_compute_bx_sec: `{speed['rust_compute']['bx_sec']}`",
            f"- rust_compute_cg_sec: `{speed['rust_compute']['cg_sec']}`",
            f"- rust_compute_comparable_sec: `{speed['rust_compute']['comparable_compute_sec']}`",
            f"- rust_persistence_mode: `{speed['rust_persistence']['persist_mode']}`",
            f"- rust_persistence_encode_artifact_sec: `{speed['rust_persistence']['encode_artifact_sec']}`",
            f"- rust_persistence_upload_artifact_sec: `{speed['rust_persistence']['upload_artifact_sec']}`",
            f"- rust_persistence_db_write_sec: `{speed['rust_persistence']['db_write_sec']}`",
            f"- rust_persistence_total_sec: `{speed['rust_persistence']['total_sec']}`",
            f"- brightway_solve_sec: `{speed['brightway']['solve_sec']}`",
            f"- brightway_build_plus_solve_sec: `{speed['brightway']['build_plus_solve_sec']}`",
            f"- rust_run_over_brightway_solve: `{speed['ratios']['rust_run_over_brightway_solve']}`",
            f"- rust_run_over_brightway_build_plus_solve: `{speed['ratios']['rust_run_over_brightway_build_plus_solve']}`",
            f"- rust_comparable_compute_over_brightway_solve: `{speed['ratios']['rust_comparable_compute_over_brightway_solve']}`",
            f"- rust_comparable_compute_over_brightway_build_plus_solve: `{speed['ratios']['rust_comparable_compute_over_brightway_build_plus_solve']}`",
            f"- faster_side: `{speed['ratios']['faster_side']}` (`{speed['ratios']['note']}`)",
            "",
            "## Timing (sec)",
            "",
            f"- total: `{timing['total']}`",
            f"- resolve_target: `{timing['resolve_target']}`",
            f"- load_artifacts: `{timing['load_artifacts']}`",
            f"- build_matrices: `{timing['build_matrices']}`",
            f"- brightway_solve: `{timing['brightway_solve']}`",
            f"- compare: `{timing['compare']}`",
            "",
        ]
    )


if __name__ == "__main__":
    main()
