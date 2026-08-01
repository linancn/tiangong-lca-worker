#!/usr/bin/env python3
"""Fail-closed Worker orchestration for scope-closure qualification evidence."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
from urllib.parse import urlparse
import uuid
import zipfile


EXTERNAL_SCHEMA = "lcia.scope-closure-external-result.v1"
PROVIDER_SCHEMA = "lcia.scope-closure-provider-result.v1"
PROVIDER_OWNER_SCHEMA = "lcia.scope-closure-provider-owned-result.v1"
CAPACITY_SCHEMA = "lcia.scope-closure-capacity-result.v3"
TIDAS_VERSION = "0.1.3"
TIDAS_PROTOCOL = "document-validation-batch.v1"
CACHE_MODES = ("cold", "warm", "mixed", "stale")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SENSITIVE_KEYS = {
    "authorization",
    "credential",
    "credentials",
    "databaseurl",
    "locator",
    "objectpath",
    "password",
    "payload",
    "privatefixture",
    "secret",
    "signedurl",
    "token",
    "url",
}
PRODUCTION_FINGERPRINTS = (
    "qgzvkongdjqiiamzbbts",
    "lca.tiangong.earth",
    "/prod/",
    "-prod-",
    "_prod_",
    ".prod.",
)
PROVIDER_REQUIRED_ENV = (
    "QUALIFICATION_DATABASE_URL",
    "QUALIFICATION_SUPABASE_URL",
    "QUALIFICATION_SUPABASE_SERVICE_ROLE_KEY",
    "QUALIFICATION_S3_ENDPOINT",
    "QUALIFICATION_S3_ACCESS_KEY_ID",
    "QUALIFICATION_S3_SECRET_ACCESS_KEY",
    "QUALIFICATION_S3_BUCKET",
)
PROVIDER_ADAPTERS = (
    ("database", "database", "QUALIFICATION_DATABASE_HARNESS", "database-engine"),
    ("storage", "database", "QUALIFICATION_STORAGE_HARNESS", "database-engine"),
    ("edge", "edge", "QUALIFICATION_EDGE_HARNESS", "tiangong-lca-edge-functions"),
    ("next", "next", "QUALIFICATION_NEXT_HARNESS", "tiangong-lca-next"),
)
PROVIDER_EVIDENCE_FIELDS = {
    "descriptors": {
        "count": int,
        "objects": int,
        "bytes": int,
        "batch596": bool,
        "batch1500OrMore": bool,
        "maximumScaleCase": int,
        "retryIdempotencyPassed": bool,
        "staleFenceRejected": bool,
    },
    "storage": {
        "provider": str,
        "bucketClass": str,
        "objectCount": int,
        "bytes": int,
        "largestObjectBytes": int,
        "multipartBoundaryPassed": bool,
        "overLimitRejectedBeforePut": bool,
    },
    "publication": {
        "noPutBeforeSeal": bool,
        "sealAtomicityPassed": bool,
        "finalizeAtomicityPassed": bool,
        "partialReadyRows": int,
        "retryPassed": bool,
    },
    "download": {
        "signedHeadPassed": bool,
        "signedRangePassed": bool,
        "crossOwnerRejected": bool,
        "locatorRedacted": bool,
        "hashVerified": bool,
    },
    "lifecycle": {
        "expiryRejected": bool,
        "objectGcPassed": bool,
        "detailGcPassed": bool,
        "retryIdempotencyPassed": bool,
        "remainingObjects": int,
        "remainingDetailRows": int,
    },
    "consumers": {
        "edgeContractPassed": bool,
        "nextContractPassed": bool,
        "readyStatePassed": bool,
        "expiredStatePassed": bool,
        "deletedStatePassed": bool,
    },
}


class QualificationError(RuntimeError):
    """A bounded qualification input or evidence failed closed."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"{label} is missing or invalid JSON") from exc
    if not isinstance(value, dict):
        raise QualificationError(f"{label} must be an object")
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(_canonical_bytes(value) + b"\n")
    os.replace(temporary, path)


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repo), *args),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise QualificationError("git identity verification failed")
    return completed.stdout.strip()


def _worker_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_components() -> dict[str, str]:
    raw = os.environ.get("SCOPE_CLOSURE_QUALIFICATION_COMPONENTS")
    if not raw:
        raise QualificationError("SCOPE_CLOSURE_QUALIFICATION_COMPONENTS is required")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise QualificationError("qualification component map is invalid JSON") from exc
    if not isinstance(value, dict) or not value:
        raise QualificationError("qualification component map must be a non-empty object")
    components: dict[str, str] = {}
    for key, sha in value.items():
        if not isinstance(key, str) or not key or not isinstance(sha, str) or not SHA1_RE.fullmatch(sha):
            raise QualificationError("qualification component map contains an invalid exact SHA")
        components[key] = sha
    expected_worker = components.get("worker_harness")
    if expected_worker != _git(_worker_root(), "rev-parse", "HEAD"):
        raise QualificationError("worker_harness SHA does not match the exact checkout")
    return components


def _reject_sensitive(value: Any, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            if normalized in SENSITIVE_KEYS:
                raise QualificationError(f"{path} contains forbidden sensitive field {key!r}")
            _reject_sensitive(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_sensitive(child, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        if "://" in lowered or "-----begin " in lowered or "service_role" in lowered:
            raise QualificationError(f"{path} contains forbidden locator or credential material")


class LinuxSampler:
    """Sample one process tree and its cgroup without retaining command output."""

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self.process = process
        self.process_peak_rss = 0
        self.cgroup_anon = 0
        self.cgroup_file = 0
        self.cgroup_current = 0
        self.cgroup_peak = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "LinuxSampler":
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join()
        self._sample()

    @staticmethod
    def _process_table() -> dict[int, tuple[int, int]]:
        table: dict[int, tuple[int, int]] = {}
        page_size = os.sysconf("SC_PAGE_SIZE")
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                stat_line = (entry / "stat").read_text(encoding="utf-8")
                remainder = stat_line[stat_line.rfind(")") + 2 :].split()
                table[int(entry.name)] = (int(remainder[1]), int(remainder[21]) * page_size)
            except (OSError, ValueError, IndexError):
                continue
        return table

    def _tree_rss(self) -> int:
        table = self._process_table()
        selected = {self.process.pid}
        changed = True
        while changed:
            changed = False
            for pid, (parent, _rss) in table.items():
                if parent in selected and pid not in selected:
                    selected.add(pid)
                    changed = True
        return sum(table.get(pid, (0, 0))[1] for pid in selected)

    @staticmethod
    def _cgroup_root() -> Path | None:
        try:
            for line in Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines():
                if line.startswith("0::"):
                    return Path("/sys/fs/cgroup") / line.partition("0::")[2].lstrip("/")
        except OSError:
            return None
        return None

    def _sample_cgroup(self) -> None:
        root = self._cgroup_root()
        if root is None:
            return
        try:
            stats = {}
            for line in (root / "memory.stat").read_text(encoding="utf-8").splitlines():
                key, raw = line.split(maxsplit=1)
                stats[key] = int(raw)
            current = int((root / "memory.current").read_text(encoding="utf-8").strip())
            peak = int((root / "memory.peak").read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return
        self.cgroup_anon = max(self.cgroup_anon, stats.get("anon", 0))
        self.cgroup_file = max(self.cgroup_file, stats.get("file", 0))
        self.cgroup_current = max(self.cgroup_current, current)
        self.cgroup_peak = max(self.cgroup_peak, peak)

    def _sample(self) -> None:
        if sys.platform.startswith("linux"):
            self.process_peak_rss = max(self.process_peak_rss, self._tree_rss())
            self._sample_cgroup()

    def _run(self) -> None:
        while not self._stop.wait(0.1):
            self._sample()

    def evidence(self, wall_time: float) -> dict[str, int | float]:
        return {
            "wallTimeSeconds": round(wall_time, 3),
            "processPeakRssBytes": self.process_peak_rss,
            "cgroupAnonBytes": self.cgroup_anon,
            "cgroupFileBytes": self.cgroup_file,
            "cgroupCurrentBytes": self.cgroup_current,
            "cgroupPeakBytes": self.cgroup_peak,
        }


def _run_sampled(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    stdout_path: Path,
    stderr_path: Path,
    allowed_codes: set[int] | None = None,
) -> dict[str, int | float]:
    started = time.monotonic()
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(argv, cwd=cwd, env=dict(env), stdout=stdout, stderr=stderr)
        with LinuxSampler(process) as sampler:
            returncode = process.wait()
    if returncode not in (allowed_codes or {0}):
        raise QualificationError("child qualification command failed; bounded local log retained")
    return sampler.evidence(time.monotonic() - started)


def _safe_extract_zip(fixture: Path, destination: Path) -> int:
    max_members = int(os.environ.get("QUALIFICATION_ZIP_MAX_MEMBERS", "1000000"))
    max_member_bytes = int(
        os.environ.get("QUALIFICATION_ZIP_MAX_MEMBER_BYTES", str(512 * 1024 * 1024))
    )
    max_total_bytes = int(
        os.environ.get("QUALIFICATION_ZIP_MAX_TOTAL_BYTES", str(4 * 1024 * 1024 * 1024))
    )
    total = 0
    seen: set[PurePosixPath] = set()
    with zipfile.ZipFile(fixture) as archive:
        members = archive.infolist()
        if len(members) > max_members:
            raise QualificationError("fixture member count exceeds the bounded limit")
        for member in members:
            name = PurePosixPath(member.filename)
            unix_mode = member.external_attr >> 16
            if (
                member.flag_bits & 0x1
                or name.is_absolute()
                or not name.parts
                or any(part in {"", ".", ".."} for part in name.parts)
                or stat.S_ISLNK(unix_mode)
                or name in seen
            ):
                raise QualificationError("fixture contains an unsafe or duplicate member")
            seen.add(name)
            if member.is_dir():
                (destination / Path(*name.parts)).mkdir(parents=True, exist_ok=True)
                continue
            if member.file_size > max_member_bytes:
                raise QualificationError("fixture member exceeds the bounded byte limit")
            total += member.file_size
            if total > max_total_bytes:
                raise QualificationError("fixture extraction exceeds the bounded total byte limit")
            target = destination / Path(*name.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            observed = 0
            with archive.open(member) as source, target.open("xb") as sink:
                while chunk := source.read(1024 * 1024):
                    observed += len(chunk)
                    if observed > member.file_size or observed > max_member_bytes:
                        raise QualificationError("fixture member expanded past its declared limit")
                    sink.write(chunk)
            if observed != member.file_size:
                raise QualificationError("fixture member length differs from its declaration")
    return total


def _tidas_json(binary: Path, args: Sequence[str], work: Path, name: str) -> dict[str, Any]:
    stdout = work / f"{name}.json"
    metrics = _run_sampled(
        (str(binary), *args),
        cwd=_worker_root(),
        env=os.environ,
        stdout_path=stdout,
        stderr_path=work / f"{name}.stderr",
        allowed_codes={0},
    )
    report = _load_json(stdout, f"TIDAS {name} report")
    if report.get("schema_version") != "tidas.operation-report.v1":
        raise QualificationError("TIDAS operation report schema drifted")
    report["_qualificationMetrics"] = metrics
    return report


def _validate_tidas(binary: Path, package: Path, work: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    version = _tidas_json(
        binary,
        ("version", "--format", "json", "--progress", "never"),
        work,
        "version",
    )
    if version.get("summary", {}).get("binary_version") != TIDAS_VERSION:
        raise QualificationError(f"TIDAS_BIN is not exact version {TIDAS_VERSION}")
    describe = _tidas_json(
        binary,
        ("validate", "--describe", "--format", "json", "--progress", "never"),
        work,
        "describe",
    )
    description = describe.get("summary", {}).get("validation_describe", {})
    if (
        description.get("package", {}).get("version") != TIDAS_VERSION
        or TIDAS_PROTOCOL not in description.get("protocols", [])
    ):
        raise QualificationError("TIDAS_BIN does not advertise the exact protocol contract")
    spool = work / "issues.ndjson"
    report_path = work / "validation-report.json"
    metrics = _run_sampled(
        (
            str(binary),
            "validate",
            str(package),
            "--input-format",
            "tidas-json",
            "--issues",
            str(spool),
            "--format",
            "json",
            "--progress",
            "never",
        ),
        cwd=_worker_root(),
        env=os.environ,
        stdout_path=report_path,
        stderr_path=work / "validate.stderr",
        allowed_codes={0, 2},
    )
    report = _load_json(report_path, "TIDAS validation report")
    validation = report.get("summary", {}).get("validation")
    if (
        report.get("schema_version") != "tidas.operation-report.v1"
        or report.get("completeness") != "complete"
        or not isinstance(validation, dict)
    ):
        raise QualificationError("TIDAS validation did not produce complete bounded evidence")
    spool_summary = validation.get("issue_spool")
    if not isinstance(spool_summary, dict):
        raise QualificationError("TIDAS validation omitted issue spool evidence")
    if (
        spool_summary.get("bytes") != spool.stat().st_size
        or spool_summary.get("sha256") != _sha256_file(spool)
        or not isinstance(spool_summary.get("event_count"), int)
        or spool_summary["event_count"] < 1
    ):
        raise QualificationError("TIDAS issue spool identity differs from the operation report")
    evidence = {
        "version": TIDAS_VERSION,
        "protocol": TIDAS_PROTOCOL,
        "events": spool_summary["event_count"],
        "spoolBytes": spool_summary["bytes"],
        "spoolSha256": spool_summary["sha256"],
        "wallTimeSeconds": metrics["wallTimeSeconds"],
        "processPeakRssBytes": metrics["processPeakRssBytes"],
    }
    return evidence, {"spool": spool, "metrics": metrics}


def _capacity_identity(result: Mapping[str, Any]) -> tuple[str, str]:
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise QualificationError("capacity result omitted artifact identities")
    artifact_set = sorted(
        (
            artifact.get("fileName"),
            artifact.get("byteSize"),
            artifact.get("checksumSha256"),
        )
        for artifact in artifacts
        if isinstance(artifact, dict)
    )
    if len(artifact_set) != len(artifacts):
        raise QualificationError("capacity artifact identity is malformed")
    logical = {
        "administrativeRecordSizes": result.get("administrativeRecordSizes"),
        "issueCount": result.get("issueCount"),
        "occurrenceCount": result.get("occurrenceCount"),
        "affectedRootCount": result.get("affectedRootCount"),
        "inputSpoolSha256": result.get("inputSpoolSha256"),
    }
    return (
        hashlib.sha256(_canonical_bytes(artifact_set)).hexdigest(),
        hashlib.sha256(_canonical_bytes(logical)).hexdigest(),
    )


def _capacity_relation_count(result: Mapping[str, Any], relation: str) -> int:
    entries = result.get("administrativeRecordSizes")
    if not isinstance(entries, list):
        raise QualificationError("capacity result omitted administrative relation evidence")
    matches = [
        entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("relation") == relation
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("recordCount"), int):
        raise QualificationError(f"capacity result omitted exact {relation} count")
    return matches[0]["recordCount"]


def _validate_capacity_results(results: Sequence[Mapping[str, Any]]) -> None:
    if len(results) != len(CACHE_MODES):
        raise QualificationError("capacity evidence requires cold, warm, mixed, and stale modes")
    for expected_mode, result in zip(CACHE_MODES, results, strict=True):
        if (
            result.get("schemaVersion") != CAPACITY_SCHEMA
            or result.get("inputMode") != "real-payload"
            or result.get("realPackageEvidence") is not True
            or not isinstance(result.get("cacheScenario"), dict)
            or result["cacheScenario"].get("mode") != expected_mode
        ):
            raise QualificationError(
                f"{expected_mode} capacity result is not exact real-payload evidence"
            )
        collection = result.get("realPackageCollection")
        if (
            not isinstance(collection, dict)
            or collection.get("malformedDocuments") != 0
            or not isinstance(collection.get("includedDocuments"), int)
            or collection["includedDocuments"] < 1
            or not isinstance(collection.get("recordSizeDistribution"), dict)
        ):
            raise QualificationError(f"{expected_mode} capacity result lacks strict source accounting")
    identities = [_capacity_identity(result) for result in results]
    if len(set(identities)) != 1:
        raise QualificationError("cache modes produced semantic or artifact identity drift")
    stable_fields = (
        "documentCount",
        "inputEventCount",
        "inputSpoolBytes",
        "inputSpoolSha256",
        "issueCount",
        "occurrenceCount",
        "affectedRootCount",
    )
    if any(
        result.get(field) != results[0].get(field)
        for result in results[1:]
        for field in stable_fields
    ):
        raise QualificationError("cache modes produced semantic count drift")


def _run_capacity_modes(
    package: Path,
    spool: Path,
    output: Path,
    event_count: int,
    work: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, int | float]]]:
    results = []
    run_metrics = []
    relation_count = event_count * 6 + (event_count + 4) // 5
    for mode in CACHE_MODES:
        mode_output = output / mode
        mode_output.mkdir(parents=True)
        env = os.environ.copy()
        env.update(
            {
                "SCOPE_CLOSURE_CAPACITY_MODE": "real-payload",
                "SCOPE_CLOSURE_CAPACITY_CACHE_MODE": mode,
                "SCOPE_CLOSURE_CAPACITY_OUTPUT": str(mode_output),
                "SCOPE_CLOSURE_REAL_PACKAGE_DIR": str(package),
                "SCOPE_CLOSURE_REAL_ISSUE_SPOOL": str(spool),
                "SCOPE_CLOSURE_PRODUCTION_RAW_EVENTS": str(event_count),
                "SCOPE_CLOSURE_PRODUCTION_RELATIONS": str(relation_count),
            }
        )
        metrics = _run_sampled(
            (
                "cargo",
                "test",
                "--release",
                "-p",
                "solver-worker",
                "--lib",
                "scope_closure::tests::qualified_streaming_issue_merge_report_capacity",
                "--",
                "--exact",
                "--ignored",
                "--nocapture",
            ),
            cwd=_worker_root(),
            env=env,
            stdout_path=work / f"capacity-{mode}.stdout",
            stderr_path=work / f"capacity-{mode}.stderr",
        )
        result_path = mode_output / "capacity-result.json"
        result = _load_json(result_path, f"{mode} capacity result")
        metrics["tempPeakBytes"] = int(result.get("temporaryBytes", 0))
        metrics["tempFinalBytes"] = 0
        result["qualificationRun"] = metrics
        _write_json_atomic(result_path, result)
        results.append(result)
        run_metrics.append(metrics)
    _validate_capacity_results(results)
    return results, run_metrics


def _external_result(
    *,
    components: Mapping[str, str],
    fixture_sha: str,
    capacities: Sequence[Mapping[str, Any]],
    tidas: Mapping[str, Any],
    run_metrics: Sequence[Mapping[str, int | float]],
    extraction_bytes: int,
) -> dict[str, Any]:
    first = capacities[0]
    collection = first["realPackageCollection"]
    distribution = collection["recordSizeDistribution"]
    resources = {
        "wallTimeSeconds": max(float(item["wallTimeSeconds"]) for item in run_metrics),
        "processPeakRssBytes": max(int(item["processPeakRssBytes"]) for item in run_metrics),
        "cgroupAnonBytes": max(int(item["cgroupAnonBytes"]) for item in run_metrics),
        "cgroupFileBytes": max(int(item["cgroupFileBytes"]) for item in run_metrics),
        "cgroupCurrentBytes": max(int(item["cgroupCurrentBytes"]) for item in run_metrics),
        "cgroupPeakBytes": max(int(item["cgroupPeakBytes"]) for item in run_metrics),
        "tempPeakBytes": max(
            extraction_bytes + int(tidas["spoolBytes"]) + int(item["tempPeakBytes"])
            for item in run_metrics
        ),
        "tempFinalBytes": 0,
        "budgetsPassed": True,
    }
    limits = {
        "resources.wallTimeSeconds": (resources["wallTimeSeconds"], 600),
        "resources.processPeakRssBytes": (
            resources["processPeakRssBytes"],
            4 * 1024 * 1024 * 1024,
        ),
        "resources.cgroupPeakBytes": (
            resources["cgroupPeakBytes"],
            16 * 1024 * 1024 * 1024,
        ),
        "resources.tempPeakBytes": (
            resources["tempPeakBytes"],
            32 * 1024 * 1024 * 1024,
        ),
        "tidas.wallTimeSeconds": (float(tidas["wallTimeSeconds"]), 60),
        "tidas.processPeakRssBytes": (
            int(tidas["processPeakRssBytes"]),
            512 * 1024 * 1024,
        ),
    }
    failures = [
        f"{name}={observed}>{limit}"
        for name, (observed, limit) in limits.items()
        if observed > limit
    ]
    resources["budgetsPassed"] = not failures
    if failures:
        raise QualificationError(
            "external qualification exceeded mandatory resource budget(s): "
            + ", ".join(failures)
        )
    result = {
        "schemaVersion": EXTERNAL_SCHEMA,
        "components": dict(components),
        "source": {
            "fixtureSha256": fixture_sha,
            "includedRecords": collection["includedDocuments"],
            "excludedRecords": collection["excludedPackageManifests"]
            + collection["excludedNonJsonFiles"],
            "malformedRecords": collection["malformedDocuments"],
            "recordSizeDistribution": {
                key: distribution[key] for key in ("p50", "p95", "p99", "max")
            },
        },
        "tidas": dict(tidas),
        "resources": resources,
        "traversal": {
            "documents": _capacity_relation_count(first, "documents"),
            "edges": _capacity_relation_count(first, "edges"),
            "roots": _capacity_relation_count(first, "roots"),
        },
    }
    if result["traversal"]["documents"] != first["documentCount"]:
        raise QualificationError("authoritative document traversal count drifted")
    _reject_sensitive(result)
    return result


def run_external(args: argparse.Namespace) -> None:
    if not sys.platform.startswith("linux"):
        raise QualificationError("external qualification requires isolated Linux cgroup evidence")
    fixture = Path(args.fixture).expanduser().resolve()
    output = Path(args.output).expanduser().resolve()
    binary_value = os.environ.get("TIDAS_BIN")
    if not fixture.is_file() or not binary_value:
        raise QualificationError("--fixture and exact TIDAS_BIN are required")
    binary = Path(binary_value).expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise QualificationError("TIDAS_BIN must be an executable file")
    if output.exists() and any(output.iterdir()):
        raise QualificationError("--output must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    components = _load_components()
    fixture_sha = _sha256_file(fixture)
    staging = Path(tempfile.mkdtemp(prefix=".scope-closure-external-", dir=output))
    try:
        package = staging / "package"
        package.mkdir()
        extraction_bytes = _safe_extract_zip(fixture, package)
        tidas, tidas_private = _validate_tidas(binary, package, staging)
        capacities, metrics = _run_capacity_modes(
            package,
            tidas_private["spool"],
            staging,
            int(tidas["events"]),
            staging,
        )
        result = _external_result(
            components=components,
            fixture_sha=fixture_sha,
            capacities=capacities,
            tidas=tidas,
            run_metrics=metrics,
            extraction_bytes=extraction_bytes,
        )
        for mode in CACHE_MODES:
            shutil.move(str(staging / mode), output / mode)
        shutil.rmtree(package)
        for private in staging.iterdir():
            if private.is_file():
                private.unlink()
        if any(staging.iterdir()):
            raise QualificationError("external qualification left cleanup residue")
        staging.rmdir()
        _write_json_atomic(output / "external-result.json", result)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _loopback_target(value: str) -> bool:
    parsed = urlparse(value)
    host = parsed.hostname
    return host in {"127.0.0.1", "::1", "localhost"}


def _validate_provider_environment() -> None:
    missing = [name for name in PROVIDER_REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise QualificationError("isolated provider configuration is incomplete")
    if (
        os.environ.get("QUALIFICATION_NON_PRODUCTION_CONFIRMATION")
        != "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS"
    ):
        raise QualificationError("isolated provider targets require explicit confirmation")
    for name in (
        "QUALIFICATION_DATABASE_URL",
        "QUALIFICATION_SUPABASE_URL",
        "QUALIFICATION_S3_ENDPOINT",
    ):
        if not _loopback_target(os.environ[name]):
            raise QualificationError("provider target fingerprint is not isolated loopback")
    bucket = os.environ["QUALIFICATION_S3_BUCKET"].lower()
    if "prod" in bucket or not any(marker in bucket for marker in ("qualification", "test", "local")):
        raise QualificationError("provider bucket fingerprint is not isolated non-production")
    for name, value in os.environ.items():
        if name.startswith("QUALIFICATION_") and any(
            fingerprint in value.lower() for fingerprint in PRODUCTION_FINGERPRINTS
        ):
            raise QualificationError("qualification configuration contains a production fingerprint")


def _provider_child_environment(run_id: str) -> dict[str, str]:
    """Expose only qualification-scoped configuration plus minimal process runtime."""
    allowed_runtime = ("HOME", "LANG", "LC_ALL", "PATH", "RUST_BACKTRACE", "TMPDIR")
    child = {
        name: value
        for name, value in os.environ.items()
        if name.startswith("QUALIFICATION_") or name in allowed_runtime
    }
    child["QUALIFICATION_RUN_ID"] = run_id
    return child


def _tracked_provider_harness(
    variable: str,
    repo_name: str,
    component: str,
    components: Mapping[str, str],
) -> tuple[Path, Path]:
    configured = os.environ.get(variable)
    if not configured:
        raise QualificationError(f"{variable} is required")
    repo = (_worker_root().parent / repo_name).resolve()
    if not repo.is_dir() or _git(repo, "rev-parse", "HEAD") != components.get(component):
        raise QualificationError(f"{component} checkout does not match the exact component SHA")
    harness = Path(configured).expanduser()
    if not harness.is_absolute():
        harness = repo / harness
    harness = harness.resolve()
    if repo not in harness.parents or not harness.is_file() or not os.access(harness, os.X_OK):
        raise QualificationError(f"{variable} must resolve to an executable inside its owner repo")
    relative = harness.relative_to(repo)
    _git(repo, "ls-files", "--error-unmatch", str(relative))
    return repo, harness


def _validate_owner_result(
    value: Any,
    *,
    owner: str,
    component: str,
    component_sha: str,
    run_id: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QualificationError(f"{owner} provider evidence must be an object")
    expected = {
        "schemaVersion",
        "runId",
        "owner",
        "component",
        "componentSha",
        "targetClass",
        "productionMutation",
        "assertions",
        "evidence",
    }
    if set(value) != expected:
        raise QualificationError(f"{owner} provider evidence fields drifted")
    if (
        value["schemaVersion"] != PROVIDER_OWNER_SCHEMA
        or value["runId"] != run_id
        or value["owner"] != owner
        or value["component"] != component
        or value["componentSha"] != component_sha
        or value["targetClass"] != "isolated-production-equivalent"
        or value["productionMutation"] is not False
        or not isinstance(value["assertions"], int)
        or isinstance(value["assertions"], bool)
        or value["assertions"] < 1
        or not isinstance(value["evidence"], dict)
        or not value["evidence"]
    ):
        raise QualificationError(f"{owner} provider evidence identity or assertions drifted")
    _reject_sensitive(value["evidence"], f"{owner}.evidence")
    return value


def _merge_evidence(target: dict[str, Any], source: Mapping[str, Any], path: str = "evidence") -> None:
    for key, value in source.items():
        if key not in target:
            target[key] = value
        elif isinstance(target[key], dict) and isinstance(value, dict):
            _merge_evidence(target[key], value, f"{path}.{key}")
        else:
            raise QualificationError(f"provider adapters emitted duplicate field {path}.{key}")


def _validate_provider_evidence(evidence: Mapping[str, Any]) -> None:
    if set(evidence) != set(PROVIDER_EVIDENCE_FIELDS):
        raise QualificationError("provider evidence sections are incomplete or unexpected")
    for section, fields in PROVIDER_EVIDENCE_FIELDS.items():
        value = evidence.get(section)
        if not isinstance(value, dict) or set(value) != set(fields):
            raise QualificationError(f"provider evidence.{section} fields drifted")
        for key, expected_type in fields.items():
            observed = value[key]
            if expected_type is bool:
                if observed is not True:
                    raise QualificationError(f"provider evidence.{section}.{key} did not pass")
            elif expected_type is int:
                if not isinstance(observed, int) or isinstance(observed, bool) or observed < 0:
                    raise QualificationError(f"provider evidence.{section}.{key} is invalid")
            elif not isinstance(observed, expected_type) or not observed:
                raise QualificationError(f"provider evidence.{section}.{key} is invalid")
    if evidence["descriptors"]["maximumScaleCase"] < 1500:
        raise QualificationError("provider descriptor scale did not reach 1500")
    if evidence["descriptors"]["count"] < 596:
        raise QualificationError("provider descriptor count did not reach 596")
    if evidence["storage"]["largestObjectBytes"] > 256 * 1024 * 1024:
        raise QualificationError("provider largest object exceeded 256 MiB")
    if evidence["storage"]["bucketClass"] != "non-production-private":
        raise QualificationError("provider bucket class is not isolated")
    for key in ("partialReadyRows",):
        if evidence["publication"][key] != 0:
            raise QualificationError("provider publication left partial ready rows")
    for key in ("remainingObjects", "remainingDetailRows"):
        if evidence["lifecycle"][key] != 0:
            raise QualificationError("provider cleanup left residue")


def run_provider(args: argparse.Namespace) -> None:
    if not sys.platform.startswith("linux"):
        raise QualificationError("provider qualification requires isolated Linux")
    _validate_provider_environment()
    output = Path(args.output).expanduser().resolve()
    if output.exists():
        raise QualificationError("--output must not already exist")
    components = _load_components()
    run_id = str(uuid.uuid4())
    evidence: dict[str, Any] = {}
    assertion_count = 0
    with tempfile.TemporaryDirectory(prefix="scope-closure-provider-") as tempdir:
        temporary = Path(tempdir)
        for owner, component, variable, repo_name in PROVIDER_ADAPTERS:
            repo, harness = _tracked_provider_harness(
                variable, repo_name, component, components
            )
            adapter_output = temporary / f"{owner}.json"
            env = _provider_child_environment(run_id)
            completed = subprocess.run(
                (
                    str(harness),
                    "--output",
                    str(adapter_output),
                    "--run-id",
                    run_id,
                ),
                cwd=repo,
                env=env,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if completed.returncode != 0:
                raise QualificationError(f"{owner} provider-owned adapter failed")
            child = _validate_owner_result(
                _load_json(adapter_output, f"{owner} provider evidence"),
                owner=owner,
                component=component,
                component_sha=components[component],
                run_id=run_id,
            )
            assertion_count += child["assertions"]
            _merge_evidence(evidence, child["evidence"])
        _validate_provider_evidence(evidence)
        _reject_sensitive(evidence)
    if temporary.exists():
        raise QualificationError("provider qualification left temporary cleanup residue")
    result = {
        "schemaVersion": PROVIDER_SCHEMA,
        "components": components,
        "targetClass": "isolated-production-equivalent",
        "productionMutation": False,
        "assertions": {
            "descriptor_write_set": assertion_count,
            "object_storage": assertion_count,
            "seal_and_finalize": assertion_count,
            "signed_head_and_range_download": assertion_count,
            "expiry_and_gc": assertion_count,
            "edge_and_next_contract": assertion_count,
        },
        "evidence": evidence,
    }
    _reject_sensitive(result)
    _write_json_atomic(output, result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    external = subparsers.add_parser("external")
    external.add_argument("--fixture", required=True)
    external.add_argument("--output", required=True)
    external.set_defaults(handler=run_external)
    provider = subparsers.add_parser("provider")
    provider.add_argument("--output", required=True)
    provider.set_defaults(handler=run_provider)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        args.handler(args)
    except QualificationError as exc:
        print(f"scope-closure qualification: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
