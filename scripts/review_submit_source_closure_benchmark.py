#!/usr/bin/env python3
"""Compare exact baseline/head snapshot-builder binaries in one isolated local stack."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil


TERMINAL_PREFIX = "[snapshot_builder_terminal] "
METRICS_PREFIX = "[source_closure_metrics] "
THRESHOLD_PERCENT = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-bin", type=Path, required=True)
    parser.add_argument("--candidate-bin", type=Path, required=True)
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--database-schema-ref", required=True)
    parser.add_argument("--root-process", required=True)
    parser.add_argument("--fixture-process-id", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sanitized_endpoint(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme and parsed.hostname:
        port = f":{parsed.port}" if parsed.port is not None else ""
        return f"{parsed.scheme}://{parsed.hostname}{port}{parsed.path}"
    return "local"


def validate_revision_update_output(output: str, process_id: str) -> None:
    expected = f"1|{process_id}|t|t"
    if output.strip() != expected:
        raise RuntimeError(
            "benchmark fixture revision advance did not preserve exactly one process identity/version"
        )


def advance_fixture_revision(database_url: str, process_id: str) -> None:
    sql = """
WITH before AS MATERIALIZED (
  SELECT id, version, modified_at
  FROM public.processes
  WHERE id = :'process_id'::uuid
  FOR UPDATE
),
updated AS (
UPDATE public.processes AS process
SET modified_at = clock_timestamp()
FROM before
WHERE process.id = before.id
RETURNING
    process.id::text AS process_id,
    process.version AS version_after,
    before.version AS version_before,
    process.modified_at > before.modified_at AS revision_advanced
)
SELECT
  count(*),
  min(process_id),
  bool_and(version_after = version_before AND version_after <> ''),
  bool_and(revision_advanced)
FROM updated;
"""
    result = subprocess.run(
        [
            "psql",
            database_url,
            "-X",
            "-q",
            "-t",
            "-A",
            "-F",
            "|",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            f"process_id={process_id}",
        ],
        check=False,
        capture_output=True,
        text=True,
        input=sql,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to advance benchmark fixture revision: {result.stderr.strip()}"
        )
    validate_revision_update_output(result.stdout, process_id)


def peak_process_tree_rss(process: psutil.Process) -> int:
    total = 0
    for current in [process, *process.children(recursive=True)]:
        try:
            total += current.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def parse_metrics(stdout: str) -> dict[str, int] | None:
    line = next(
        (line for line in stdout.splitlines() if line.startswith(METRICS_PREFIX)),
        None,
    )
    if line is None:
        return None
    metrics: dict[str, int] = {}
    for token in line.removeprefix(METRICS_PREFIX).split():
        key, separator, value = token.partition("=")
        if separator and value.isdigit():
            metrics[key] = int(value)
    return metrics


def parse_snapshot_output(
    *, variant: str, stdout: str, stderr: str, return_code: int
) -> tuple[dict[str, Any], bool]:
    if return_code != 0:
        raise RuntimeError(
            f"{variant} snapshot_builder exited {return_code}: {stderr[-2000:]}"
        )
    timing_lines = [
        line.removeprefix("[build_timing_sec] ")
        for line in stdout.splitlines()
        if line.startswith("[build_timing_sec] ")
    ]
    if len(timing_lines) != 1:
        raise RuntimeError(
            f"{variant} emitted {len(timing_lines)} legacy timing frames, expected exactly one"
        )
    timing = json.loads(timing_lines[0])
    if not isinstance(timing, dict) or not isinstance(timing.get("total_sec"), int | float):
        raise RuntimeError(f"{variant} emitted invalid build_timing_sec")

    done_lines = [
        line.removeprefix("[done] snapshot ready: ").strip()
        for line in stdout.splitlines()
        if line.startswith("[done] snapshot ready: ")
    ]
    if len(done_lines) != 1:
        raise RuntimeError(
            f"{variant} emitted {len(done_lines)} done frames, expected exactly one"
        )
    try:
        done_snapshot_id = str(uuid.UUID(done_lines[0]))
    except ValueError as error:
        raise RuntimeError(f"{variant} emitted invalid done snapshot ID") from error

    terminal_lines = [
        line.removeprefix(TERMINAL_PREFIX)
        for line in stdout.splitlines()
        if line.startswith(TERMINAL_PREFIX)
    ]
    if variant == "baseline":
        if terminal_lines:
            raise RuntimeError("baseline unexpectedly emitted a terminal protocol frame")
        return timing, False
    if variant != "candidate":
        raise RuntimeError(f"unknown benchmark variant: {variant}")
    if len(terminal_lines) != 1:
        raise RuntimeError(
            f"candidate emitted {len(terminal_lines)} terminal frames, expected exactly one"
        )
    terminal = json.loads(terminal_lines[0])
    if terminal.get("status") != "succeeded":
        raise RuntimeError(f"candidate terminal was not succeeded: {terminal}")
    if terminal.get("build_timing_sec") != timing:
        raise RuntimeError("candidate terminal timing mismatched legacy build_timing_sec")
    if terminal.get("resolved_snapshot_id") != done_snapshot_id:
        raise RuntimeError("candidate terminal snapshot ID mismatched done frame")
    return timing, True


def run_snapshot(
    *,
    variant: str,
    binary: Path,
    root_process: str,
    checksum: str,
    expected_reused: bool,
    namespace: str,
) -> dict[str, Any]:
    command = [
        str(binary),
        "--snapshot-id",
        str(uuid.uuid4()),
        "--root-process",
        root_process,
        "--artifact-purpose",
        "review_submit_overlay",
        "--artifact-expires-in-seconds",
        "86400",
        "--reuse-max-age-seconds",
        "86400",
        "--review-submit-revision-checksum",
        checksum,
        "--no-lcia",
        "--snapshot-report-mode",
        "disabled",
        "--s3-prefix",
        namespace,
    ]
    started = time.perf_counter()
    peak_rss = 0
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        child = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        process = psutil.Process(child.pid)
        while child.poll() is None:
            peak_rss = max(peak_rss, peak_process_tree_rss(process))
            time.sleep(0.005)
        peak_rss = max(peak_rss, peak_process_tree_rss(process))
        return_code = child.wait()
        elapsed = time.perf_counter() - started
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read().decode("utf-8", errors="replace")
        stderr = stderr_file.read().decode("utf-8", errors="replace")
    timing, terminal_protocol = parse_snapshot_output(
        variant=variant,
        stdout=stdout,
        stderr=stderr,
        return_code=return_code,
    )
    reused = bool(timing.get("reused_snapshot"))
    overlay_reused = bool(timing.get("review_submit_overlay_reused"))
    if reused != expected_reused or overlay_reused != expected_reused:
        raise RuntimeError(
            f"{variant} reuse mismatch: expected={expected_reused} "
            f"reused_snapshot={reused} review_submit_overlay_reused={overlay_reused}"
        )
    return {
        "variant": variant,
        "builderTotalSec": float(timing["total_sec"]),
        "wallSec": elapsed,
        "peakRssBytes": peak_rss,
        "timing": timing,
        "terminalProtocol": terminal_protocol,
        "sourceClosureMetrics": parse_metrics(stdout),
    }


def nearest_rank_p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    def metric(name: str) -> dict[str, float]:
        values = [float(sample[name]) for sample in samples]
        mean = statistics.fmean(values)
        variance = statistics.pvariance(values)
        return {
            "median": statistics.median(values),
            "p95": nearest_rank_p95(values),
            "mean": mean,
            "variance": variance,
            "standardDeviation": math.sqrt(variance),
            "coefficientOfVariation": 0.0 if mean == 0 else math.sqrt(variance) / mean,
            "min": min(values),
            "max": max(values),
        }

    return {
        "sampleCount": len(samples),
        "builderTotalSec": metric("builderTotalSec"),
        "wallSec": metric("wallSec"),
        "peakRssBytes": metric("peakRssBytes"),
    }


def compare_group(samples: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_samples = [sample for sample in samples if sample["variant"] == "baseline"]
    candidate_samples = [sample for sample in samples if sample["variant"] == "candidate"]
    baseline = summarize(baseline_samples)
    candidate = summarize(candidate_samples)
    baseline_p95 = baseline["builderTotalSec"]["p95"]
    candidate_p95 = candidate["builderTotalSec"]["p95"]
    regression = (
        math.inf
        if baseline_p95 == 0
        else ((candidate_p95 / baseline_p95) - 1.0) * 100.0
    )
    return {
        "baseline": baseline,
        "candidate": candidate,
        "builderTotalP95RegressionPercent": regression,
        "passesFivePercentGate": regression <= THRESHOLD_PERCENT,
    }


def main() -> int:
    args = parse_args()
    if args.runs < 20:
        raise SystemExit("--runs must be at least 20")
    for binary in (args.baseline_bin, args.candidate_bin):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"snapshot_builder binary is not executable: {binary}")

    database_url = required_env("DATABASE_URL")
    storage_endpoint = required_env("S3_ENDPOINT")
    bucket = required_env("S3_BUCKET")
    namespace = required_env("S3_PREFIX")

    binaries = {
        "baseline": args.baseline_bin,
        "candidate": args.candidate_bin,
    }
    cold_samples: list[dict[str, Any]] = []
    for run_index in range(args.runs):
        nonce = f"cold-{run_index:02d}"
        advance_fixture_revision(database_url, args.fixture_process_id)
        order = ["baseline", "candidate"]
        if run_index % 2:
            order.reverse()
        for variant in order:
            cold_samples.append(
                run_snapshot(
                    variant=variant,
                    binary=binaries[variant],
                    root_process=args.root_process,
                    checksum=f"{nonce}-{variant}",
                    expected_reused=False,
                    namespace=f"{namespace}/{variant}",
                )
            )

    advance_fixture_revision(database_url, args.fixture_process_id)
    warmups = [
        run_snapshot(
            variant=variant,
            binary=binaries[variant],
            root_process=args.root_process,
            checksum=f"hot-stable-{variant}",
            expected_reused=False,
            namespace=f"{namespace}/{variant}",
        )
        for variant in ("baseline", "candidate")
    ]
    hot_samples: list[dict[str, Any]] = []
    for run_index in range(args.runs):
        order = ["baseline", "candidate"]
        if run_index % 2:
            order.reverse()
        for variant in order:
            hot_samples.append(
                run_snapshot(
                    variant=variant,
                    binary=binaries[variant],
                    root_process=args.root_process,
                    checksum=f"hot-stable-{variant}",
                    expected_reused=True,
                    namespace=f"{namespace}/{variant}",
                )
            )

    cold = compare_group(cold_samples)
    hot = compare_group(hot_samples)
    decision = {
        "thresholdPercent": THRESHOLD_PERCENT,
        "coldPass": cold["passesFivePercentGate"],
        "hotPass": hot["passesFivePercentGate"],
        "pass": cold["passesFivePercentGate"] and hot["passesFivePercentGate"],
    }
    report = {
        "schemaVersion": "review_submit_source_closure_benchmark.v1",
        "generatedAt": datetime.now(UTC).isoformat(),
        "environment": {
            "projectId": args.project_id,
            "databaseEndpoint": sanitized_endpoint(database_url),
            "storageEndpoint": sanitized_endpoint(storage_endpoint),
            "bucket": bucket,
            "prefix": namespace,
            "databaseSchemaRef": args.database_schema_ref,
            "supabaseCliVersion": os.environ.get("SUPABASE_CLI_VERSION"),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logicalCpuCount": psutil.cpu_count(logical=True),
            "physicalCpuCount": psutil.cpu_count(logical=False),
            "memoryBytes": psutil.virtual_memory().total,
        },
        "fixture": {
            "rootProcess": args.root_process,
            "fixtureProcessId": args.fixture_process_id,
        },
        "executables": {
            "baseline": {
                "ref": args.baseline_ref,
                "sha256": sha256_file(args.baseline_bin),
            },
            "candidate": {
                "ref": args.candidate_ref,
                "sha256": sha256_file(args.candidate_bin),
            },
        },
        "runsPerVariantAndCacheMode": args.runs,
        "primaryTimingField": "build_timing_sec.total_sec",
        "supplementaryTimingField": "runner wallSec",
        "groups": {
            "cold": {**cold, "samples": cold_samples},
            "hot": {**hot, "warmups": warmups, "samples": hot_samples},
        },
        "decision": decision,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "coldRegressionPercent": cold[
                    "builderTotalP95RegressionPercent"
                ],
                "hotRegressionPercent": hot["builderTotalP95RegressionPercent"],
                "decision": decision,
            },
            sort_keys=True,
        )
    )
    return 0 if decision["pass"] else 3


class ParserSelfTest(unittest.TestCase):
    snapshot_id = "16000000-0000-4000-8000-000000000099"
    timing = {
        "reused_snapshot": False,
        "review_submit_overlay_reused": False,
        "total_sec": 1.25,
    }

    def legacy_stdout(self) -> str:
        return (
            f"[build_timing_sec] {json.dumps(self.timing, separators=(',', ':'))}\n"
            f"[done] snapshot ready: {self.snapshot_id}\n"
        )

    def candidate_stdout(self, timing: dict[str, Any] | None = None) -> str:
        terminal = {
            "status": "succeeded",
            "schema_version": "snapshot_builder_terminal.v1",
            "resolved_snapshot_id": self.snapshot_id,
            "build_timing_sec": self.timing if timing is None else timing,
        }
        return (
            self.legacy_stdout()
            + f"{TERMINAL_PREFIX}{json.dumps(terminal, separators=(',', ':'))}\n"
        )

    def test_accepts_baseline_legacy_output(self) -> None:
        timing, terminal = parse_snapshot_output(
            variant="baseline",
            stdout=self.legacy_stdout(),
            stderr="",
            return_code=0,
        )
        self.assertEqual(timing, self.timing)
        self.assertFalse(terminal)

    def test_accepts_consistent_candidate_terminal(self) -> None:
        timing, terminal = parse_snapshot_output(
            variant="candidate",
            stdout=self.candidate_stdout(),
            stderr="",
            return_code=0,
        )
        self.assertEqual(timing, self.timing)
        self.assertTrue(terminal)

    def test_rejects_duplicate_timing(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "2 legacy timing frames"):
            parse_snapshot_output(
                variant="baseline",
                stdout=self.legacy_stdout() + self.legacy_stdout().splitlines()[0] + "\n",
                stderr="",
                return_code=0,
            )

    def test_rejects_duplicate_candidate_terminal(self) -> None:
        terminal_line = next(
            line
            for line in self.candidate_stdout().splitlines()
            if line.startswith(TERMINAL_PREFIX)
        )
        with self.assertRaisesRegex(RuntimeError, "2 terminal frames"):
            parse_snapshot_output(
                variant="candidate",
                stdout=self.candidate_stdout() + terminal_line + "\n",
                stderr="",
                return_code=0,
            )

    def test_rejects_missing_candidate_terminal(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "0 terminal frames"):
            parse_snapshot_output(
                variant="candidate",
                stdout=self.legacy_stdout(),
                stderr="",
                return_code=0,
            )

    def test_rejects_nonzero_baseline_exit(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "baseline snapshot_builder exited 9"):
            parse_snapshot_output(
                variant="baseline",
                stdout=self.legacy_stdout(),
                stderr="terminated",
                return_code=9,
            )

    def test_accepts_exact_revision_advance_readback(self) -> None:
        validate_revision_update_output(
            "1|16000000-0000-4000-8000-000000000010|t|t\n",
            "16000000-0000-4000-8000-000000000010",
        )

    def test_rejects_zero_row_revision_advance(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "exactly one process"):
            validate_revision_update_output(
                "0|||\n", "16000000-0000-4000-8000-000000000010"
            )

    def test_rejects_revision_identity_or_version_drift(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "identity/version"):
            validate_revision_update_output(
                "1|16000000-0000-4000-8000-000000000010|f|t\n",
                "16000000-0000-4000-8000-000000000010",
            )

    def test_rejects_missing_timing(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "0 legacy timing frames"):
            parse_snapshot_output(
                variant="baseline",
                stdout=f"[done] snapshot ready: {self.snapshot_id}\n",
                stderr="",
                return_code=0,
            )

    def test_rejects_candidate_timing_mismatch(self) -> None:
        mismatch = {**self.timing, "total_sec": 2.5}
        with self.assertRaisesRegex(RuntimeError, "terminal timing mismatched"):
            parse_snapshot_output(
                variant="candidate",
                stdout=self.candidate_stdout(mismatch),
                stderr="",
                return_code=0,
            )


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ParserSelfTest)
        raise SystemExit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
    raise SystemExit(main())
