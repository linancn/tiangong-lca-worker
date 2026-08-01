#!/usr/bin/env python3
"""Fail when frozen Worker consumers or contract identifiers use public aliases."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_PREFIX = "public" + "."
FORBIDDEN = tuple(
    PUBLIC_PREFIX + name
    for name in (
        "worker_enqueue_job",
        "worker_claim_jobs",
        "worker_heartbeat_job",
        "worker_record_job_result",
        "worker_job_domain_refs",
    )
)
SCANNED_PREFIXES = ("crates/", "docs/", "scripts/")
SCANNED_FILES = {"AGENTS.md", "README.md", ".env.example", "Makefile"}


def tracked_consumer_paths(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    )
    paths = []
    for raw_path in result.stdout.split(b"\0"):
        if not raw_path:
            continue
        relative = raw_path.decode()
        if relative == "scripts/check_worker_control_plane_api_cutover.py":
            continue
        path = root / relative
        if path.exists() and (relative in SCANNED_FILES or relative.startswith(SCANNED_PREFIXES)):
            paths.append(path)
    return paths


def scan(paths: list[Path], root: Path) -> list[str]:
    violations: list[str] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(lines, start=1):
            for forbidden in FORBIDDEN:
                if forbidden in line:
                    violations.append(f"{path.relative_to(root)}:{line_number}: {forbidden}")
    return violations


def main() -> int:
    violations = scan(tracked_consumer_paths(ROOT), ROOT)
    if violations:
        print("frozen Worker public consumer/contract aliases remain:", file=sys.stderr)
        for violation in violations:
            print(violation, file=sys.stderr)
        return 1
    print("PASS frozen Worker control-plane public consumer/contract alias count is zero")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
