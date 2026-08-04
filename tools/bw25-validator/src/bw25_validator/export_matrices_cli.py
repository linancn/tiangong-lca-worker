from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .cli import (
    RESULT_FORMAT,
    S3Config,
    decode_hdf5_envelope,
    download_object_url,
    load_snapshot_payload,
)
from .expected_cli import DEFAULT_GWP_IMPACT_ID, derive_snapshot_index_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bw25-export-matrices",
        description=(
            "Export latest snapshot matrices (A/B/M) and climate-only LCIA vectors "
            "(C row + solve_all_unit H column) into TSV files."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("CONN"),
    )
    parser.add_argument("--result-id", default=None)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument(
        "--impact-id",
        default=DEFAULT_GWP_IMPACT_ID,
        help=f"LCIA impact id for C/H export (default: {DEFAULT_GWP_IMPACT_ID})",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/result-matrices",
    )
    parser.add_argument(
        "--base-name",
        default=None,
        help="Output file basename prefix (default: latest-snapshot-matrices-<utc_ts>)",
    )
    parser.add_argument(
        "--no-latest-pointers",
        action="store_true",
        help="Do not write/refresh stable latest pointer files",
    )
    parser.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT"))
    parser.add_argument("--s3-region", default=os.getenv("S3_REGION"))
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET"))
    parser.add_argument("--s3-access-key-id", default=os.getenv("S3_ACCESS_KEY_ID"))
    parser.add_argument("--s3-secret-access-key", default=os.getenv("S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--s3-session-token", default=os.getenv("S3_SESSION_TOKEN"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("missing DB connection: provide --database-url or DATABASE_URL/CONN")

    s3 = S3Config(
        endpoint=args.s3_endpoint,
        region=args.s3_region,
        bucket=args.s3_bucket,
        access_key_id=args.s3_access_key_id,
        secret_access_key=args.s3_secret_access_key,
        session_token=args.s3_session_token,
    )

    with psycopg.connect(args.database_url, row_factory=dict_row) as conn:
        target = resolve_target_result(conn, args.result_id, args.snapshot_id)
        snapshot_artifact_url = fetch_snapshot_artifact_url(conn, target["snapshot_id"])
        snapshot_payload = load_snapshot_payload(conn, target["snapshot_id"], s3)

    snapshot_index = load_snapshot_index(snapshot_artifact_url, target["snapshot_id"], s3)
    process_ids_by_index = build_process_id_vector(snapshot_index, snapshot_payload)
    impact_info = select_impact(snapshot_index, args.impact_id)

    result_payload = load_result_payload(target, s3)
    result_items = extract_result_items(result_payload)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_name = args.base_name or f"latest-snapshot-matrices-{utc_stamp()}"
    paths = build_output_paths(out_dir, base_name)

    tech_entries = must_list(snapshot_payload.get("technosphere_entries"), "technosphere_entries")
    bio_entries = must_list(snapshot_payload.get("biosphere_entries"), "biosphere_entries")
    cf_entries = must_list(
        snapshot_payload.get("characterization_factors"),
        "characterization_factors",
    )

    process_count = int(snapshot_payload.get("process_count", 0))
    flow_count = int(snapshot_payload.get("flow_count", 0))
    impact_count = int(snapshot_payload.get("impact_count", 0))

    if process_count <= 0:
        raise SystemExit("invalid process_count in snapshot payload")
    if flow_count <= 0:
        raise SystemExit("invalid flow_count in snapshot payload")
    if impact_count <= 0:
        raise SystemExit("invalid impact_count in snapshot payload")

    write_a_triplets(paths["a"], tech_entries, process_ids_by_index, process_count)
    write_b_triplets(paths["b"], bio_entries, process_ids_by_index, process_count, flow_count)

    m_entries = derive_m_entries(tech_entries, process_count)
    write_m_triplets(paths["m"], m_entries, process_ids_by_index)

    c_climate_nnz = write_c_impact_triplets(
        paths["c_climate"],
        cf_entries,
        flow_count=flow_count,
        impact_info=impact_info,
    )
    h_rows = write_h_impact_vector(
        paths["h_climate"],
        result_items,
        process_ids_by_index,
        impact_info,
    )

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "result_id": target["result_id"],
            "result_created_at_utc": target["result_created_at_utc"],
            "snapshot_id": target["snapshot_id"],
            "result_artifact_format": target["result_artifact_format"],
        },
        "dimensions": {
            "process_count": process_count,
            "flow_count": flow_count,
            "impact_count": impact_count,
            "a_nnz": len(tech_entries),
            "b_nnz": len(bio_entries),
            "m_nnz": len(m_entries),
            "c_impact_nnz": c_climate_nnz,
            "h_rows": h_rows,
        },
        "impact": impact_info,
        "files": {k: str(v) for k, v in paths.items()},
        "notes": [
            "B/C are exported by flow_index (snapshot index has no flow_map in current runtime)",
            "M is computed from aggregated A via M = I - A",
            "H uses solve_all_unit result artifact items[*].h[impact_index]",
        ],
    }
    paths["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    if not args.no_latest_pointers:
        write_latest_pointer_files(out_dir, paths)

    print("[done] exported latest snapshot matrices")
    print(
        f"[source] result_id={target['result_id']} snapshot_id={target['snapshot_id']}"
    )
    print(
        "[shape] process_count={} flow_count={} impact_count={}".format(
            process_count, flow_count, impact_count
        )
    )
    print(
        "[nnz] a={} b={} m={} c_impact={} h_rows={}".format(
            len(tech_entries),
            len(bio_entries),
            len(m_entries),
            c_climate_nnz,
            h_rows,
        )
    )
    print(f"[impact] id={impact_info['impact_id']} index={impact_info['impact_index']}")
    for key in ["a", "b", "m", "c_climate", "h_climate", "meta"]:
        print(f"[{key}] {paths[key]}")


def resolve_target_result(
    conn: psycopg.Connection[Any],
    result_id: str | None,
    snapshot_id: str | None,
) -> dict[str, str]:
    if result_id:
        row = conn.execute(
            """
            SELECT
              r.id::text AS result_id,
              r.snapshot_id::text AS snapshot_id,
              r.artifact_url AS result_artifact_url,
              r.artifact_format AS result_artifact_format,
              j.job_type AS job_type,
              to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                AS result_created_at_utc
            FROM public.lca_results r
            JOIN public.lca_jobs j ON j.id = r.job_id
            WHERE r.id = %s::uuid
            LIMIT 1
            """,
            (result_id,),
        ).fetchone()
    elif snapshot_id:
        row = conn.execute(
            """
            SELECT
              r.id::text AS result_id,
              r.snapshot_id::text AS snapshot_id,
              r.artifact_url AS result_artifact_url,
              r.artifact_format AS result_artifact_format,
              j.job_type AS job_type,
              to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                AS result_created_at_utc
            FROM public.lca_results r
            JOIN public.lca_jobs j ON j.id = r.job_id
            WHERE r.snapshot_id = %s::uuid
              AND j.job_type = 'solve_all_unit'
            ORDER BY r.created_at DESC
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT
              r.id::text AS result_id,
              r.snapshot_id::text AS snapshot_id,
              r.artifact_url AS result_artifact_url,
              r.artifact_format AS result_artifact_format,
              j.job_type AS job_type,
              to_char(r.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                AS result_created_at_utc
            FROM public.lca_results r
            JOIN public.lca_jobs j ON j.id = r.job_id
            WHERE j.job_type = 'solve_all_unit'
            ORDER BY r.created_at DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        raise SystemExit("no solve_all_unit result found for selected target")
    if row["job_type"] != "solve_all_unit":
        raise SystemExit(
            f"result must come from solve_all_unit, got job_type={row['job_type']}"
        )
    if not row["result_artifact_url"]:
        raise SystemExit("result row has no artifact_url")
    if (
        row["result_artifact_format"]
        and str(row["result_artifact_format"]) != RESULT_FORMAT
    ):
        raise SystemExit(
            "unsupported result artifact format: "
            f"{row['result_artifact_format']} (expected {RESULT_FORMAT})"
        )
    return {
        "result_id": str(row["result_id"]),
        "snapshot_id": str(row["snapshot_id"]),
        "result_artifact_url": str(row["result_artifact_url"]),
        "result_artifact_format": str(row["result_artifact_format"] or ""),
        "result_created_at_utc": str(row["result_created_at_utc"] or ""),
    }


def fetch_snapshot_artifact_url(conn: psycopg.Connection[Any], snapshot_id: str) -> str:
    row = conn.execute(
        """
        SELECT artifact_url
        FROM public.lca_snapshot_artifacts
        WHERE snapshot_id = %s::uuid
          AND status = 'ready'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (snapshot_id,),
    ).fetchone()
    if not row or not row["artifact_url"]:
        raise SystemExit(f"no ready snapshot artifact URL for snapshot_id={snapshot_id}")
    return str(row["artifact_url"])


def load_snapshot_index(
    snapshot_artifact_url: str,
    snapshot_id: str,
    s3: S3Config,
) -> dict[str, Any]:
    index_url = derive_snapshot_index_url(snapshot_artifact_url)
    data = download_object_url(index_url, s3)
    try:
        doc = json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid snapshot index JSON: {exc}") from exc
    if str(doc.get("snapshot_id", "")) != snapshot_id:
        raise SystemExit("snapshot index mismatch with snapshot_id")
    if not isinstance(doc.get("process_map"), list):
        raise SystemExit("snapshot index missing process_map")
    if not isinstance(doc.get("impact_map"), list):
        raise SystemExit("snapshot index missing impact_map")
    return doc


def select_impact(snapshot_index: dict[str, Any], impact_id: str) -> dict[str, Any]:
    for item in snapshot_index.get("impact_map", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("impact_id", "")) != impact_id:
            continue
        return {
            "impact_id": impact_id,
            "impact_index": int(item.get("impact_index")),
            "impact_key": str(item.get("impact_key", "")),
            "impact_name": str(item.get("impact_name", "")),
            "unit": str(item.get("unit", "")),
        }
    raise SystemExit(f"impact_id not found in snapshot impact_map: {impact_id}")


def build_process_id_vector(
    snapshot_index: dict[str, Any],
    snapshot_payload: dict[str, Any],
) -> list[str]:
    process_count = int(snapshot_payload.get("process_count", 0))
    if process_count <= 0:
        raise SystemExit("invalid process_count in snapshot payload")

    out = [""] * process_count
    for item in snapshot_index.get("process_map", []):
        if not isinstance(item, dict):
            continue
        idx = int(item.get("process_index", -1))
        if 0 <= idx < process_count:
            out[idx] = str(item.get("process_id", ""))
    return out


def load_result_payload(target: dict[str, str], s3: S3Config) -> dict[str, Any]:
    data = download_object_url(target["result_artifact_url"], s3)
    envelope = decode_hdf5_envelope(data)
    if envelope["format"] != RESULT_FORMAT:
        raise SystemExit(
            f"unexpected result artifact format: {envelope['format']} (expected {RESULT_FORMAT})"
        )
    payload = envelope.get("envelope", {}).get("payload")
    if not isinstance(payload, dict):
        raise SystemExit("invalid result artifact payload")
    return payload


def extract_result_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise SystemExit("solve_all_unit payload has no items")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise SystemExit(f"invalid item at result payload index={idx}")
        out.append(item)
    return out


def must_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise SystemExit(f"invalid snapshot payload field: {field}")
    return value


def derive_m_entries(
    tech_entries: list[Any],
    process_count: int,
) -> dict[tuple[int, int], float]:
    a_entries: dict[tuple[int, int], float] = defaultdict(float)
    for item in tech_entries:
        if not isinstance(item, dict):
            continue
        row = int(item.get("row", -1))
        col = int(item.get("col", -1))
        value = float(item.get("value", 0.0))
        if 0 <= row < process_count and 0 <= col < process_count and value != 0.0:
            a_entries[(row, col)] += value

    m_entries: dict[tuple[int, int], float] = defaultdict(float)
    for i in range(process_count):
        diag = 1.0 - a_entries.get((i, i), 0.0)
        if diag != 0.0:
            m_entries[(i, i)] = diag
    for (row, col), value in a_entries.items():
        if row != col:
            m_entries[(row, col)] -= value
    return m_entries


def build_output_paths(out_dir: Path, base_name: str) -> dict[str, Path]:
    return {
        "a": out_dir / f"{base_name}-A-triplets.tsv",
        "b": out_dir / f"{base_name}-B-triplets.tsv",
        "m": out_dir / f"{base_name}-M-triplets.tsv",
        "c_climate": out_dir / f"{base_name}-C-impact-triplets.tsv",
        "h_climate": out_dir / f"{base_name}-H-impact.tsv",
        "meta": out_dir / f"{base_name}-meta.json",
    }


def write_a_triplets(
    path: Path,
    tech_entries: list[Any],
    process_ids_by_index: list[str],
    process_count: int,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("row_process_index\trow_process_id\tcol_process_index\tcol_process_id\tvalue\n")
        for item in tech_entries:
            if not isinstance(item, dict):
                continue
            row = int(item.get("row", -1))
            col = int(item.get("col", -1))
            value = float(item.get("value", 0.0))
            if not (0 <= row < process_count and 0 <= col < process_count):
                continue
            f.write(
                "{}\t{}\t{}\t{}\t{}\n".format(
                    row,
                    process_ids_by_index[row],
                    col,
                    process_ids_by_index[col],
                    format_float(value),
                )
            )


def write_b_triplets(
    path: Path,
    bio_entries: list[Any],
    process_ids_by_index: list[str],
    process_count: int,
    flow_count: int,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("flow_index\tprocess_index\tprocess_id\tvalue\n")
        for item in bio_entries:
            if not isinstance(item, dict):
                continue
            row = int(item.get("row", -1))
            col = int(item.get("col", -1))
            value = float(item.get("value", 0.0))
            if not (0 <= row < flow_count and 0 <= col < process_count):
                continue
            f.write(
                "{}\t{}\t{}\t{}\n".format(
                    row,
                    col,
                    process_ids_by_index[col],
                    format_float(value),
                )
            )


def write_m_triplets(
    path: Path,
    m_entries: dict[tuple[int, int], float],
    process_ids_by_index: list[str],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("row_process_index\trow_process_id\tcol_process_index\tcol_process_id\tvalue\n")
        for (row, col), value in sorted(m_entries.items()):
            f.write(
                "{}\t{}\t{}\t{}\t{}\n".format(
                    row,
                    process_ids_by_index[row],
                    col,
                    process_ids_by_index[col],
                    format_float(value),
                )
            )


def write_c_impact_triplets(
    path: Path,
    cf_entries: list[Any],
    *,
    flow_count: int,
    impact_info: dict[str, Any],
) -> int:
    impact_index = int(impact_info["impact_index"])
    count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("impact_index\timpact_id\timpact_name\tunit\tflow_index\tvalue\n")
        for item in cf_entries:
            if not isinstance(item, dict):
                continue
            row = int(item.get("row", -1))
            col = int(item.get("col", -1))
            value = float(item.get("value", 0.0))
            if row != impact_index:
                continue
            if not (0 <= col < flow_count):
                continue
            count += 1
            f.write(
                "{}\t{}\t{}\t{}\t{}\t{}\n".format(
                    impact_index,
                    impact_info["impact_id"],
                    impact_info["impact_name"],
                    impact_info["unit"],
                    col,
                    format_float(value),
                )
            )
    return count


def write_h_impact_vector(
    path: Path,
    result_items: list[dict[str, Any]],
    process_ids_by_index: list[str],
    impact_info: dict[str, Any],
) -> int:
    impact_index = int(impact_info["impact_index"])
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("process_index\tprocess_id\timpact_index\timpact_id\tvalue\n")
        for i, item in enumerate(result_items):
            h = item.get("h")
            value = ""
            if isinstance(h, list) and 0 <= impact_index < len(h):
                value = format_float(float(h[impact_index]))
            process_id = process_ids_by_index[i] if i < len(process_ids_by_index) else ""
            f.write(
                "{}\t{}\t{}\t{}\t{}\n".format(
                    i,
                    process_id,
                    impact_index,
                    impact_info["impact_id"],
                    value,
                )
            )
    return len(result_items)


def write_latest_pointer_files(out_dir: Path, paths: dict[str, Path]) -> None:
    pointers = {
        "latest-snapshot-A-triplets.tsv": paths["a"],
        "latest-snapshot-B-triplets.tsv": paths["b"],
        "latest-snapshot-M-triplets.tsv": paths["m"],
        "latest-snapshot-C-impact-triplets.tsv": paths["c_climate"],
        "latest-solve-all-unit-H-impact.tsv": paths["h_climate"],
    }
    for pointer_name, target_path in pointers.items():
        (out_dir / pointer_name).write_text(str(target_path) + "\n", encoding="utf-8")


def format_float(value: float) -> str:
    return format(float(value), ".17g")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


if __name__ == "__main__":
    main()
