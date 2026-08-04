from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psycopg
from psycopg.rows import dict_row
from scipy import sparse
from scipy.sparse.linalg import splu

from .cli import S3Config, build_matrices, download_object_url, load_snapshot_payload

DEFAULT_GWP_IMPACT_ID = "6209b35f-9447-40b5-b68c-a1099e3674a0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bw25-generate-expected",
        description=(
            "Generate reusable expected LCIA target values from one snapshot artifact "
            "(manual baseline, no Edge function call)."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL") or os.getenv("CONN"),
    )
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--process-ids-file", required=True)
    parser.add_argument("--impact-id", default=DEFAULT_GWP_IMPACT_ID)
    parser.add_argument("--abs-tol", type=float, default=1e-9)
    parser.add_argument("--output", default=None)
    parser.add_argument("--include-process-name", action="store_true")

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

    process_ids = read_process_ids(args.process_ids_file)
    if not process_ids:
        raise SystemExit("process ids file contains no valid process_id")

    s3 = S3Config(
        endpoint=args.s3_endpoint,
        region=args.s3_region,
        bucket=args.s3_bucket,
        access_key_id=args.s3_access_key_id,
        secret_access_key=args.s3_secret_access_key,
        session_token=args.s3_session_token,
    )

    with psycopg.connect(args.database_url, row_factory=dict_row) as conn:
        snapshot_payload = load_snapshot_payload(conn, args.snapshot_id, s3)
        artifact_url = fetch_snapshot_artifact_url(conn, args.snapshot_id)
        snapshot_index = load_snapshot_index(args.snapshot_id, artifact_url, s3)
        process_name_by_id = (
            fetch_process_names(conn, process_ids) if args.include_process_name else {}
        )

    m, b, c = build_matrices(snapshot_payload)
    impact_idx = impact_index_of(snapshot_index, args.impact_id)
    if impact_idx is None:
        raise SystemExit(f"impact_id not found in snapshot: {args.impact_id}")

    process_idx_by_id = {
        str(item["process_id"]): int(item["process_index"])
        for item in snapshot_index.get("process_map", [])
        if isinstance(item, dict)
    }

    missing = [pid for pid in process_ids if pid not in process_idx_by_id]
    if missing:
        raise SystemExit(
            "process ids not found in snapshot (first 5): "
            + ", ".join(missing[:5])
            + ("" if len(missing) <= 5 else f" ... total={len(missing)}")
        )

    n = m.shape[0]
    if n <= 0:
        raise SystemExit("invalid matrix shape")

    solver = splu(m.tocsc())
    d = (c.getrow(impact_idx) @ b).toarray().ravel()

    out_path = resolve_output_path(args.output, args.snapshot_id, args.impact_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "process_id",
        "expected_value",
        "abs_tol",
        "process_index",
        "direct_value",
        "indirect_value",
    ]
    if args.include_process_name:
        header.append("process_name")

    with out_path.open("w", encoding="utf-8", newline="") as f:
        f.write("\t".join(header) + "\n")
        for pid in process_ids:
            process_idx = process_idx_by_id[pid]
            rhs = np.zeros(n, dtype=np.float64)
            rhs[process_idx] = 1.0
            x = solver.solve(rhs)

            expected = float(np.dot(d, x))
            direct = float(d[process_idx])
            indirect = expected - direct

            row = [
                pid,
                format_float(expected),
                format_float(args.abs_tol),
                str(process_idx),
                format_float(direct),
                format_float(indirect),
            ]
            if args.include_process_name:
                row.append(process_name_by_id.get(pid, ""))
            f.write("\t".join(row) + "\n")

    print(f"[done] generated expected targets: {out_path}")
    print(f"[meta] snapshot_id={args.snapshot_id} impact_id={args.impact_id}")
    print(f"[meta] process_count={len(process_ids)} generated_at={now_utc_iso()}")


def read_process_ids(path: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"process ids file not found: {path}")

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lower = line.lower()
        if lower.startswith("process_id"):
            continue
        token = line.split("\t", 1)[0].split(",", 1)[0].strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


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


def derive_snapshot_index_url(snapshot_artifact_url: str) -> str:
    marker = "/snapshot/sparse.h5"
    if marker in snapshot_artifact_url:
        return snapshot_artifact_url.replace(marker, "/snapshot/snapshot-index-v1.json")
    return snapshot_artifact_url.rsplit("/", 1)[0] + "/snapshot-index-v1.json"


def load_snapshot_index(
    snapshot_id: str,
    snapshot_artifact_url: str,
    s3: S3Config,
) -> dict[str, Any]:
    index_url = derive_snapshot_index_url(snapshot_artifact_url)
    bytes_data = download_object_url(index_url, s3)
    try:
        doc = json.loads(bytes_data.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid snapshot index JSON: {exc}") from exc

    if str(doc.get("snapshot_id", "")).strip() != snapshot_id:
        raise SystemExit("snapshot index mismatch with snapshot_id")
    if not isinstance(doc.get("process_map"), list):
        raise SystemExit("snapshot index missing process_map")
    if not isinstance(doc.get("impact_map"), list):
        raise SystemExit("snapshot index missing impact_map")
    return doc


def impact_index_of(snapshot_index: dict[str, Any], impact_id: str) -> int | None:
    for item in snapshot_index.get("impact_map", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("impact_id", "")).strip() == impact_id:
            return int(item["impact_index"])
    return None


def fetch_process_names(
    conn: psycopg.Connection[Any],
    process_ids: list[str],
) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT
          id::text AS process_id,
          COALESCE(
            (json::jsonb -> 'processDataSet' -> 'processInformation' -> 'dataSetInformation'
              -> 'name' -> 'baseName' -> 0 ->> '#text'),
            ''
          ) AS process_name
        FROM public.processes
        WHERE id::text = ANY(%s)
        """,
        (process_ids,),
    ).fetchall()
    return {str(row["process_id"]): str(row["process_name"]) for row in rows}


def resolve_output_path(
    output: str | None,
    snapshot_id: str,
    impact_id: str,
) -> Path:
    if output:
        return Path(output)
    return Path(
        f"reports/lcia-targets/expected-{snapshot_id}-impact-{impact_id}.tsv"
    )


def format_float(value: float) -> str:
    return format(float(value), ".17g")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    main()
