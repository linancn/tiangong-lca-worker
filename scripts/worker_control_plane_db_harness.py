#!/usr/bin/env python3
"""Own one exact-head local Supabase DB for the Worker behavioral contract test."""

from __future__ import annotations

import json
import os
import re
import secrets
import socket
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

EXPECTED_DATABASE_SHA = "6809528c32bac8163e9a6eec9b985d57370589e1"
EXPECTED_MIGRATION_VERSION = "20260801060304"
CALLER_DATABASE_ENV = "WORKER_CONTROL_PLANE_DATABASE_URL"
PROJECT_LABEL = "com.supabase.cli.project"
COMPOSE_LABEL = "com.docker.compose.project"
EXCLUDED_SERVICES = (
    "gotrue,realtime,storage-api,imgproxy,kong,mailpit,postgrest,postgres-meta,"
    "studio,edge-runtime,logflare,vector,supavisor"
)


class HarnessError(RuntimeError):
    """Fail-closed harness validation error."""


@dataclass(frozen=True)
class Resources:
    containers: tuple[str, ...] = ()
    volumes: tuple[str, ...] = ()
    networks: tuple[str, ...] = ()


@dataclass(frozen=True)
class TrustedDatabase:
    url: str
    container_id: str
    system_identifier: str
    sentinel_schema: str
    sentinel: str


def capture(command: list[str], *, cwd: Path | None = None) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def require_local_docker_context() -> str:
    if os.environ.get("DOCKER_HOST") or os.environ.get("DOCKER_CONTEXT"):
        raise HarnessError("Docker endpoint overrides are forbidden for the local harness")
    context = capture(["docker", "context", "show"])
    raw_endpoint = capture(
        ["docker", "context", "inspect", context, "--format", "{{json .Endpoints.docker.Host}}"]
    )
    try:
        endpoint = json.loads(raw_endpoint)
    except json.JSONDecodeError as exc:
        raise HarnessError("Docker context endpoint was not valid JSON") from exc
    if not isinstance(endpoint, str) or not endpoint.startswith("unix://"):
        raise HarnessError("Worker DB harness requires a local Unix-socket Docker context")
    socket_path = Path(endpoint.removeprefix("unix://"))
    if not socket_path.is_socket():
        raise HarnessError("Docker context Unix socket is not a local socket")
    return context


def docker(context: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "--context", context, *arguments],
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def resources_for_project(context: str, project_id: str) -> Resources:
    label = f"label={PROJECT_LABEL}={project_id}"
    containers = tuple(
        item for item in docker(context, "ps", "-aq", "--no-trunc", "--filter", label).stdout.splitlines()
        if item
    )
    volumes = tuple(
        item for item in docker(context, "volume", "ls", "-q", "--filter", label).stdout.splitlines()
        if item
    )
    networks = tuple(
        item for item in docker(context, "network", "ls", "-q", "--no-trunc", "--filter", label).stdout.splitlines()
        if item
    )
    return Resources(containers, volumes, networks)


def _labels(inspect: dict[str, object]) -> dict[str, str]:
    config = inspect.get("Config")
    if not isinstance(config, dict):
        raise HarnessError("Docker inspect omitted Config")
    labels = config.get("Labels")
    if not isinstance(labels, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
    ):
        raise HarnessError("Docker inspect omitted string labels")
    return labels


def validate_database_container(
    inspect: dict[str, object], *, project_id: str, expected_port: int
) -> str:
    container_id = inspect.get("Id")
    if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise HarnessError("database container lacks an exact immutable ID")
    if inspect.get("Name") != f"/supabase_db_{project_id}":
        raise HarnessError("database container name is not owned by this harness")
    labels = _labels(inspect)
    if labels.get(PROJECT_LABEL) != project_id or labels.get(COMPOSE_LABEL) != project_id:
        raise HarnessError("database container labels are not owned by this harness")
    state = inspect.get("State")
    if not isinstance(state, dict) or state.get("Running") is not True:
        raise HarnessError("database container is not running")
    network = inspect.get("NetworkSettings")
    ports = network.get("Ports") if isinstance(network, dict) else None
    bindings = ports.get("5432/tcp") if isinstance(ports, dict) else None
    if not isinstance(bindings, list) or not bindings:
        raise HarnessError("database container has no published PostgreSQL port")
    observed = {
        (binding.get("HostIp"), binding.get("HostPort"))
        for binding in bindings
        if isinstance(binding, dict)
    }
    allowed_ips = {"0.0.0.0", "127.0.0.1", "::"}
    if not observed or any(ip not in allowed_ips or port != str(expected_port) for ip, port in observed):
        raise HarnessError("database container published-port ownership changed")
    return container_id


def validate_status_database_url(value: str, *, expected_port: int) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise HarnessError("Supabase control-plane DB URL is malformed") from exc
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname != "127.0.0.1"
        or port != expected_port
        or parsed.path != "/postgres"
        or parsed.query
        or parsed.fragment
    ):
        raise HarnessError("Supabase control-plane DB URL does not match the owned container port")
    return value


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def rewrite_local_config(config_path: Path, *, project_id: str, db_port: int, shadow_port: int) -> None:
    text = config_path.read_text(encoding="utf-8")
    replacements = (
        (r'(?m)^project_id = "[^"]+"$', f'project_id = "{project_id}"'),
        (r'(?m)^(\[db\]\n(?:.*\n)*?# Port to use for the local database URL\.\n)port = \d+$', rf"\g<1>port = {db_port}"),
        (r'(?m)^(# Port used by db diff command to initialize the shadow database\.\n)shadow_port = \d+$', rf"\g<1>shadow_port = {shadow_port}"),
    )
    for pattern, replacement in replacements:
        text, count = re.subn(pattern, replacement, text, count=1)
        if count != 1:
            raise HarnessError("exact-head Supabase config does not match reviewed local overrides")
    config_path.write_text(text, encoding="utf-8")


def extract_exact_database_source(source_root: Path, destination: Path) -> None:
    archive_path = destination.parent / "database-engine.tar"
    with archive_path.open("wb") as archive:
        subprocess.run(
            ["git", "archive", "--format=tar", EXPECTED_DATABASE_SHA],
            cwd=source_root,
            check=True,
            stdout=archive,
        )
    destination.mkdir()
    with tarfile.open(archive_path, "r:") as archive:
        archive.extractall(destination, filter="data")
    archive_path.unlink()
    migration_names = sorted(
        path.name for path in (destination / "supabase" / "migrations").glob("*.sql")
    )
    if not migration_names or not migration_names[-1].startswith(EXPECTED_MIGRATION_VERSION + "_"):
        raise HarnessError("database-engine archive is not at the reviewed migration head")


def inspect_one_database_container(
    context: str, resources: Resources, *, project_id: str, expected_port: int
) -> str:
    matches: list[str] = []
    for container_id in resources.containers:
        raw = docker(context, "inspect", container_id).stdout
        payload = json.loads(raw)
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            raise HarnessError("database container inspect result is malformed")
        labels = _labels(payload[0])
        if labels.get(PROJECT_LABEL) != project_id or labels.get(COMPOSE_LABEL) != project_id:
            raise HarnessError("Supabase project contains an unowned container")
        if payload[0].get("Name") == f"/supabase_db_{project_id}":
            matches.append(
                validate_database_container(
                    payload[0], project_id=project_id, expected_port=expected_port
                )
            )
    if len(matches) != 1:
        raise HarnessError("Supabase harness did not create one exact running database container")
    return matches[0]


def create_sentinel(context: str, container_id: str, *, schema: str, sentinel: str) -> str:
    if not re.fullmatch(r"worker_harness_[0-9a-f]{32}", schema):
        raise HarnessError("generated sentinel schema is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", sentinel):
        raise HarnessError("generated sentinel is invalid")
    sql = f'''\
BEGIN;
CREATE SCHEMA "{schema}";
REVOKE ALL ON SCHEMA "{schema}" FROM PUBLIC;
CREATE TABLE "{schema}".instance_identity (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  sentinel text NOT NULL,
  container_id text NOT NULL,
  system_identifier text NOT NULL
);
REVOKE ALL ON "{schema}".instance_identity FROM PUBLIC;
INSERT INTO "{schema}".instance_identity (sentinel, container_id, system_identifier)
SELECT '{sentinel}', '{container_id}', system_identifier::text FROM pg_control_system();
GRANT USAGE ON SCHEMA "{schema}" TO service_role;
GRANT SELECT ON "{schema}".instance_identity TO service_role;
COMMIT;
SELECT system_identifier::text FROM pg_control_system();
'''
    # docker() cannot provide stdin; run this command explicitly without exposing the sentinel.
    completed = subprocess.run(
        [
            "docker", "--context", context, "exec", "-i", container_id,
            "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres",
        ],
        check=True,
        text=True,
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    system_identifier = completed.stdout.strip()
    if not re.fullmatch(r"[0-9]+", system_identifier):
        raise HarnessError("owned database did not return a PostgreSQL system identifier")
    return system_identifier


def cargo_environment(database: TrustedDatabase) -> dict[str, str]:
    environment = {
        key: value for key, value in os.environ.items()
        if key not in {
            "DATABASE_URL", "PGHOST", "PGHOSTADDR", "PGPORT", "PGDATABASE", "PGUSER",
            "PGPASSWORD", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS", CALLER_DATABASE_ENV,
            "WORKER_CONTROL_PLANE_HARNESS_SCHEMA", "WORKER_CONTROL_PLANE_HARNESS_SENTINEL",
            "WORKER_CONTROL_PLANE_SYSTEM_IDENTIFIER", "WORKER_CONTROL_PLANE_CONTAINER_ID",
        }
    }
    environment.update(
        {
            CALLER_DATABASE_ENV: database.url,
            "WORKER_CONTROL_PLANE_MIGRATION_VERSION": EXPECTED_MIGRATION_VERSION,
            "WORKER_CONTROL_PLANE_HARNESS_SCHEMA": database.sentinel_schema,
            "WORKER_CONTROL_PLANE_HARNESS_SENTINEL": database.sentinel,
            "WORKER_CONTROL_PLANE_SYSTEM_IDENTIFIER": database.system_identifier,
            "WORKER_CONTROL_PLANE_CONTAINER_ID": database.container_id,
        }
    )
    return environment


def reject_caller_database_target(environment: dict[str, str]) -> None:
    if environment.get(CALLER_DATABASE_ENV):
        raise HarnessError("caller-supplied Worker behavioral database URLs are forbidden")


def validate_resource_labels(context: str, resources: Resources, project_id: str) -> None:
    for kind, identifiers in (
        ("volume", resources.volumes),
        ("network", resources.networks),
    ):
        for identifier in identifiers:
            payload = json.loads(docker(context, kind, "inspect", identifier).stdout)
            if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                raise HarnessError(f"owned {kind} inspect result is malformed")
            labels = payload[0].get("Labels")
            if (
                not isinstance(labels, dict)
                or labels.get(PROJECT_LABEL) != project_id
                or labels.get(COMPOSE_LABEL) != project_id
            ):
                raise HarnessError(f"refusing cleanup of unowned {kind}")


def cleanup(context: str, resources: Resources, project_id: str) -> None:
    if not any((resources.containers, resources.volumes, resources.networks)):
        return
    validate_resource_labels(context, resources, project_id)
    for container_id in resources.containers:
        inspected = docker(context, "inspect", container_id, check=False)
        if inspected.returncode != 0 and "No such object" in inspected.stderr:
            continue
        if inspected.returncode != 0:
            raise HarnessError("failed to re-inspect an owned container before cleanup")
        payload = json.loads(inspected.stdout)
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            raise HarnessError("refusing cleanup of malformed container identity")
        labels = _labels(payload[0])
        if labels.get(PROJECT_LABEL) != project_id:
            raise HarnessError("refusing cleanup of unowned container")
        docker(context, "rm", "--force", container_id)
    for volume in resources.volumes:
        docker(context, "volume", "rm", volume)
    for network in resources.networks:
        docker(context, "network", "rm", network)
    if resources_for_project(context, project_id) != Resources():
        raise HarnessError("owned Worker DB harness resources remain after cleanup")


def validate_source_root(source_root: Path) -> None:
    if capture(["git", "rev-parse", "--show-toplevel"], cwd=source_root) != str(source_root):
        raise HarnessError("database worktree is not an exact repository root")
    if capture(["git", "rev-parse", "HEAD"], cwd=source_root) != EXPECTED_DATABASE_SHA:
        raise HarnessError("database worktree is not at database-engine PR #365 exact head")
    if capture(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=source_root):
        raise HarnessError("database worktree is not clean")


def main() -> int:
    reject_caller_database_target(os.environ)
    source_value = os.environ.get("WORKER_CONTROL_PLANE_DATABASE_WORKTREE", "")
    if not source_value:
        raise HarnessError("WORKER_CONTROL_PLANE_DATABASE_WORKTREE is required")
    source_root = Path(source_value).resolve(strict=True)
    validate_source_root(source_root)
    context = require_local_docker_context()
    project_id = f"w192-{secrets.token_hex(12)}"
    sentinel_schema = f"worker_harness_{secrets.token_hex(16)}"
    sentinel = secrets.token_hex(32)
    if resources_for_project(context, project_id) != Resources():
        raise HarnessError("random harness project identity already exists")

    resources = Resources()
    with tempfile.TemporaryDirectory(prefix="worker192-db-") as directory:
        root = Path(directory) / "database-engine"
        try:
            extract_exact_database_source(source_root, root)
            db_port = free_port()
            shadow_port = free_port()
            while shadow_port == db_port:
                shadow_port = free_port()
            rewrite_local_config(
                root / "supabase" / "config.toml",
                project_id=project_id,
                db_port=db_port,
                shadow_port=shadow_port,
            )
            subprocess.run(
                [
                    "supabase", "start", "--workdir", str(root), "--exclude", EXCLUDED_SERVICES,
                    "--output", "json",
                ],
                cwd=root,
                check=True,
            )
            resources = resources_for_project(context, project_id)
            container_id = inspect_one_database_container(
                context, resources, project_id=project_id, expected_port=db_port
            )
            status = json.loads(
                capture(["supabase", "status", "--workdir", str(root), "--output", "json"], cwd=root)
            )
            if not isinstance(status, dict) or not isinstance(status.get("DB_URL"), str):
                raise HarnessError("Supabase control plane omitted DB_URL")
            database_url = validate_status_database_url(status["DB_URL"], expected_port=db_port)
            system_identifier = create_sentinel(
                context, container_id, schema=sentinel_schema, sentinel=sentinel
            )
            # Re-inspect immediately before handing the owned endpoint to Rust.
            current = resources_for_project(context, project_id)
            if current.volumes != resources.volumes or current.networks != resources.networks:
                raise HarnessError("owned Supabase volume/network changed before behavioral execution")
            current_container_id = inspect_one_database_container(
                context, current, project_id=project_id, expected_port=db_port
            )
            if current_container_id != container_id:
                raise HarnessError("owned database container ID changed before behavioral execution")
            database = TrustedDatabase(
                database_url, container_id, system_identifier, sentinel_schema, sentinel
            )
            worker_sha = capture(["git", "rev-parse", "HEAD"], cwd=Path.cwd())
            print(json.dumps({
                "databaseSha": EXPECTED_DATABASE_SHA,
                "migrationVersion": EXPECTED_MIGRATION_VERSION,
                "workerSha": worker_sha,
                "targetKind": "runner-owned-local-supabase",
                "containerId": container_id,
                "roleProof": "SET ROLE service_role ACL matrix; deployment login external",
            }, sort_keys=True))
            subprocess.run(
                [
                    "cargo", "test", "-p", "solver-worker", "--test",
                    "worker_control_plane_database_contract", "--", "--ignored",
                    "worker_control_plane_",
                ],
                cwd=Path.cwd(),
                env=cargo_environment(database),
                check=True,
            )
        finally:
            discovered = resources_for_project(context, project_id)
            cleanup(context, discovered, project_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (HarnessError, OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"Worker DB harness failed closed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
