#!/usr/bin/env python3
"""Reject every behavioral Worker DB target except a literal loopback URL."""

from __future__ import annotations

import os
import sys
from urllib.parse import parse_qsl, urlsplit

DATABASE_URL_ENV = "WORKER_CONTROL_PLANE_DATABASE_URL"
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
HOST_OVERRIDE_PARAMETERS = frozenset({"host", "hostaddr", "service", "servicefile"})


def is_literal_loopback_database_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        _ = parsed.port
        query_parameters = {name.lower() for name, _ in parse_qsl(parsed.query)}
    except (TypeError, ValueError):
        return False
    return (
        parsed.scheme in {"postgres", "postgresql"}
        and host is not None
        and host.lower() in LOOPBACK_HOSTS
        and not query_parameters.intersection(HOST_OVERRIDE_PARAMETERS)
    )


def main() -> int:
    database_url = os.environ.get(DATABASE_URL_ENV, "")
    if not database_url:
        print(f"missing required isolated-harness variable: {DATABASE_URL_ENV}", file=sys.stderr)
        return 2
    if not is_literal_loopback_database_url(database_url):
        print("refusing Worker behavioral integration target: literal loopback URL required", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
