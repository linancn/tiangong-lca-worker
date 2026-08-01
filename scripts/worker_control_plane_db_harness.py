#!/usr/bin/env python3
"""Own one exact-head, loopback-only Supabase DB for the Worker contract test."""

from __future__ import annotations

import json
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator
from urllib.parse import quote, urlsplit

EXPECTED_DATABASE_SHA = "6809528c32bac8163e9a6eec9b985d57370589e1"
EXPECTED_MIGRATION_VERSION = "20260801060304"
EXPECTED_SUPABASE_CLI_VERSION = "2.84.2"
EXPECTED_POSTGRES_IMAGE = (
    "public.ecr.aws/supabase/postgres@"
    "sha256:965e2dfb5a23a0d6541b6106541e777b303656ebabd4e878746b189d550c0a66"
)
LOCAL_POSTGRES_PASSWORD = "postgres"
CALLER_DATABASE_ENV = "WORKER_CONTROL_PLANE_DATABASE_URL"
PROJECT_LABEL = "com.supabase.cli.project"
COMPOSE_LABEL = "com.docker.compose.project"
DOCKER_OVERRIDE_ENV = {
    "DOCKER_API_VERSION",
    "DOCKER_CERT_PATH",
    "DOCKER_CONFIG",
    "DOCKER_CONTEXT",
    "DOCKER_HOST",
    "DOCKER_TLS_VERIFY",
}
CANCELLATION_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class HarnessError(RuntimeError):
    """Fail-closed harness validation error."""


class HarnessCancelled(HarnessError):
    """Cancellation raised only after exact resource identity has been recorded."""


@dataclass
class Resources:
    container_id: str | None = None
    volume_name: str | None = None
    network_id: str | None = None

    def any(self) -> bool:
        return any((self.container_id, self.volume_name, self.network_id))


@dataclass(frozen=True)
class DockerEndpoint:
    socket_url: str
    socket_realpath: Path
    socket_device: int
    socket_inode: int
    daemon_id: str
    daemon_name: str
    config_dir: Path

    def environment(self) -> dict[str, str]:
        environment = {
            key: value for key, value in os.environ.items() if key not in DOCKER_OVERRIDE_ENV
        }
        environment.update(
            {
                "DOCKER_HOST": self.socket_url,
                "DOCKER_CONFIG": str(self.config_dir),
            }
        )
        return environment

    def assert_stable(self) -> None:
        current_realpath = Path(self.socket_url.removeprefix("unix://")).resolve(strict=True)
        current_stat = current_realpath.stat()
        if (
            current_realpath != self.socket_realpath
            or not stat.S_ISSOCK(current_stat.st_mode)
            or current_stat.st_dev != self.socket_device
            or current_stat.st_ino != self.socket_inode
        ):
            raise HarnessError("validated Docker Unix socket identity changed")
        daemon_id, daemon_name = _daemon_identity(self.environment())
        if (daemon_id, daemon_name) != (self.daemon_id, self.daemon_name):
            raise HarnessError("validated Docker daemon identity changed")


@dataclass(frozen=True)
class TrustedDatabase:
    url: str
    container_id: str
    system_identifier: str
    sentinel_schema: str
    sentinel: str


def run(
    command: list[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
    check: bool = True,
    input_text: str | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=check,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )


def capture(
    command: list[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
    timeout: int = 120,
) -> str:
    return run(command, cwd=cwd, environment=environment, timeout=timeout).stdout.strip()


def _daemon_identity(environment: dict[str, str]) -> tuple[str, str]:
    raw = capture(
        ["docker", "info", "--format", "{{json .ID}} {{json .Name}}"],
        environment=environment,
        timeout=30,
    )
    match = re.fullmatch(r'("(?:[^"\\]|\\.)*") ("(?:[^"\\]|\\.)*")', raw)
    if not match:
        raise HarnessError("Docker daemon omitted a stable ID/name identity")
    daemon_id, daemon_name = (json.loads(value) for value in match.groups())
    if not daemon_id or not daemon_name:
        raise HarnessError("Docker daemon ID/name identity is empty")
    return daemon_id, daemon_name


def require_local_docker_endpoint(config_dir: Path) -> DockerEndpoint:
    if any(os.environ.get(name) for name in DOCKER_OVERRIDE_ENV):
        raise HarnessError("Docker endpoint/config overrides are forbidden for the local harness")
    context = capture(["docker", "context", "show"], timeout=30)
    raw_endpoint = capture(
        ["docker", "context", "inspect", context, "--format", "{{json .Endpoints.docker.Host}}"],
        timeout=30,
    )
    try:
        endpoint = json.loads(raw_endpoint)
    except json.JSONDecodeError as exc:
        raise HarnessError("Docker context endpoint was not valid JSON") from exc
    if not isinstance(endpoint, str) or not endpoint.startswith("unix://"):
        raise HarnessError("Worker DB harness requires a local Unix-socket Docker endpoint")
    socket_value = endpoint.removeprefix("unix://")
    socket_path = Path(socket_value)
    if not socket_path.is_absolute():
        raise HarnessError("Docker Unix-socket endpoint must be absolute")
    socket_realpath = socket_path.resolve(strict=True)
    socket_stat = socket_realpath.stat()
    if not stat.S_ISSOCK(socket_stat.st_mode):
        raise HarnessError("Docker context Unix endpoint is not a local socket")
    config_dir.mkdir(mode=0o700)
    pinned_url = f"unix://{socket_realpath}"
    provisional = DockerEndpoint(
        pinned_url,
        socket_realpath,
        socket_stat.st_dev,
        socket_stat.st_ino,
        "pending",
        "pending",
        config_dir,
    )
    daemon_id, daemon_name = _daemon_identity(provisional.environment())
    endpoint_identity = DockerEndpoint(
        pinned_url,
        socket_realpath,
        socket_stat.st_dev,
        socket_stat.st_ino,
        daemon_id,
        daemon_name,
        config_dir,
    )
    endpoint_identity.assert_stable()
    return endpoint_identity


def docker(
    endpoint: DockerEndpoint,
    *arguments: str,
    check: bool = True,
    input_text: str | None = None,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    return run(
        ["docker", *arguments],
        environment=endpoint.environment(),
        check=check,
        input_text=input_text,
        timeout=timeout,
    )


def resources_for_project(endpoint: DockerEndpoint, project_id: str) -> tuple[tuple[str, ...], ...]:
    filters = (
        "--filter", f"label={PROJECT_LABEL}={project_id}",
        "--filter", f"label={COMPOSE_LABEL}={project_id}",
    )
    containers = tuple(
        value for value in docker(endpoint, "ps", "-aq", "--no-trunc", *filters).stdout.splitlines()
        if value
    )
    volumes = tuple(
        value for value in docker(endpoint, "volume", "ls", "-q", *filters).stdout.splitlines()
        if value
    )
    networks = tuple(
        value
        for value in docker(endpoint, "network", "ls", "-q", "--no-trunc", *filters).stdout.splitlines()
        if value
    )
    return containers, volumes, networks


def _container_labels(inspect: dict[str, object]) -> dict[str, str]:
    config = inspect.get("Config")
    if not isinstance(config, dict):
        raise HarnessError("Docker inspect omitted Config")
    labels = config.get("Labels")
    if not isinstance(labels, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
    ):
        raise HarnessError("Docker inspect omitted string labels")
    return labels


def _require_owned_labels(labels: object, project_id: str, kind: str) -> None:
    if (
        not isinstance(labels, dict)
        or labels.get(PROJECT_LABEL) != project_id
        or labels.get(COMPOSE_LABEL) != project_id
    ):
        raise HarnessError(f"refusing {kind} operation without both exact ownership labels")


def validate_database_container(
    inspect: dict[str, object],
    *,
    project_id: str,
    expected_image_id: str,
) -> tuple[str, int]:
    container_id = inspect.get("Id")
    if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise HarnessError("database container lacks an exact immutable ID")
    if inspect.get("Name") != f"/supabase_db_{project_id}":
        raise HarnessError("database container name is not owned by this harness")
    _require_owned_labels(_container_labels(inspect), project_id, "container")
    if inspect.get("Image") != expected_image_id:
        raise HarnessError("database container does not use the exact pinned image ID")
    state = inspect.get("State")
    if (
        not isinstance(state, dict)
        or state.get("Running") is not True
        or not isinstance(state.get("Health"), dict)
        or state["Health"].get("Status") != "healthy"
    ):
        raise HarnessError("database container is not healthy and running")
    network = inspect.get("NetworkSettings")
    ports = network.get("Ports") if isinstance(network, dict) else None
    bindings = ports.get("5432/tcp") if isinstance(ports, dict) else None
    if not isinstance(bindings, list) or len(bindings) != 1 or not isinstance(bindings[0], dict):
        raise HarnessError("database container must have exactly one PostgreSQL publication")
    host_ip = bindings[0].get("HostIp")
    host_port = bindings[0].get("HostPort")
    if host_ip != "127.0.0.1" or not isinstance(host_port, str) or not host_port.isdigit():
        raise HarnessError("database publication is not exactly one 127.0.0.1 binding")
    port = int(host_port)
    if not 0 < port <= 65535:
        raise HarnessError("database publication returned an invalid host port")
    return container_id, port


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
        or parsed.query not in {"", "sslmode=disable"}
        or parsed.fragment
    ):
        raise HarnessError("Supabase control-plane DB URL does not match the owned container port")
    return value


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


def rewrite_local_config(config_path: Path, *, project_id: str, db_port: int) -> None:
    text = config_path.read_text(encoding="utf-8")
    replacements = (
        (r'(?m)^project_id = "[^"]+"$', f'project_id = "{project_id}"'),
        (
            r'(?m)^(\[db\]\n(?:.*\n)*?# Port to use for the local database URL\.\n)port = \d+$',
            rf"\g<1>port = {db_port}",
        ),
    )
    for pattern, replacement in replacements:
        text, count = re.subn(pattern, replacement, text, count=1)
        if count != 1:
            raise HarnessError("exact-head Supabase config does not match reviewed local overrides")
    config_path.write_text(text, encoding="utf-8")


def validate_source_root(source_root: Path) -> None:
    if capture(["git", "rev-parse", "--show-toplevel"], cwd=source_root) != str(source_root):
        raise HarnessError("database worktree is not an exact repository root")
    if capture(["git", "rev-parse", "HEAD"], cwd=source_root) != EXPECTED_DATABASE_SHA:
        raise HarnessError("database worktree is not at database-engine PR #365 exact head")
    if capture(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=source_root):
        raise HarnessError("database worktree is not clean")


def validate_worker_source(worker_root: Path, expected_head: str | None = None) -> str:
    if capture(["git", "rev-parse", "--show-toplevel"], cwd=worker_root) != str(worker_root):
        raise HarnessError("Worker source is not an exact repository root")
    head = capture(["git", "rev-parse", "HEAD"], cwd=worker_root)
    if expected_head is not None and head != expected_head:
        raise HarnessError("Worker source HEAD changed during qualification")
    if capture(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=worker_root):
        raise HarnessError("Worker source is not clean")
    return head


def reject_caller_database_target(environment: dict[str, str]) -> None:
    if environment.get(CALLER_DATABASE_ENV):
        raise HarnessError("caller-supplied Worker behavioral database URLs are forbidden")


@contextmanager
def block_cancellation_signals() -> Iterator[None]:
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def cancellation_signal_handler(signum: int, _frame: object) -> None:
    raise HarnessCancelled(f"received cancellation signal {signum}")


@contextmanager
def installed_cancellation_handlers() -> Iterator[None]:
    previous = {signum: signal.getsignal(signum) for signum in CANCELLATION_SIGNALS}
    for signum in CANCELLATION_SIGNALS:
        signal.signal(signum, cancellation_signal_handler)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def require_pinned_image(endpoint: DockerEndpoint) -> str:
    endpoint.assert_stable()
    docker(endpoint, "pull", EXPECTED_POSTGRES_IMAGE, timeout=600)
    endpoint.assert_stable()
    payload = json.loads(docker(endpoint, "image", "inspect", EXPECTED_POSTGRES_IMAGE).stdout)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise HarnessError("pinned Postgres image inspect result is malformed")
    image = payload[0]
    image_id = image.get("Id")
    repo_digests = image.get("RepoDigests")
    if (
        not isinstance(image_id, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
        or not isinstance(repo_digests, list)
        or EXPECTED_POSTGRES_IMAGE not in repo_digests
        or image.get("Architecture") not in {"amd64", "arm64"}
        or image.get("Os") != "linux"
    ):
        raise HarnessError("resolved Postgres image does not match the pinned multi-platform digest")
    return image_id


def create_network(
    endpoint: DockerEndpoint, resources: Resources, *, project_id: str, name: str
) -> None:
    endpoint.assert_stable()
    with block_cancellation_signals():
        network_id = docker(
            endpoint,
            "network", "create",
            "--label", f"{PROJECT_LABEL}={project_id}",
            "--label", f"{COMPOSE_LABEL}={project_id}",
            name,
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", network_id):
            raise HarnessError("Docker did not return an immutable network ID")
        resources.network_id = network_id
    endpoint.assert_stable()


def create_volume(
    endpoint: DockerEndpoint, resources: Resources, *, project_id: str, name: str
) -> None:
    endpoint.assert_stable()
    with block_cancellation_signals():
        volume_name = docker(
            endpoint,
            "volume", "create",
            "--label", f"{PROJECT_LABEL}={project_id}",
            "--label", f"{COMPOSE_LABEL}={project_id}",
            name,
        ).stdout.strip()
        if volume_name != name:
            raise HarnessError("Docker returned an unexpected volume identity")
        resources.volume_name = volume_name
    endpoint.assert_stable()


def create_container(
    endpoint: DockerEndpoint,
    resources: Resources,
    *,
    project_id: str,
    network_name: str,
    volume_name: str,
    password: str,
    jwt_secret: str,
) -> None:
    endpoint.assert_stable()
    with block_cancellation_signals():
        container_id = docker(
            endpoint,
            "create",
            "--name", f"supabase_db_{project_id}",
            "--label", f"{PROJECT_LABEL}={project_id}",
            "--label", f"{COMPOSE_LABEL}={project_id}",
            "--network", network_name,
            "--network-alias", "db",
            "--mount", f"type=volume,src={volume_name},dst=/var/lib/postgresql/data",
            "--publish", "127.0.0.1::5432",
            "--env", f"POSTGRES_PASSWORD={password}",
            "--env", f"JWT_SECRET={jwt_secret}",
            "--env", "JWT_EXP=3600",
            EXPECTED_POSTGRES_IMAGE,
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise HarnessError("Docker did not return an immutable container ID")
        resources.container_id = container_id
    endpoint.assert_stable()


def wait_for_database(endpoint: DockerEndpoint, container_id: str) -> None:
    deadline = time.monotonic() + 120
    last_status = "unknown"
    while time.monotonic() < deadline:
        payload = inspect_exact(endpoint, "container", container_id)
        state = payload.get("State")
        health = state.get("Health") if isinstance(state, dict) else None
        last_status = str(health.get("Status")) if isinstance(health, dict) else "missing"
        if last_status == "healthy":
            return
        if isinstance(state, dict) and state.get("Running") is not True:
            raise HarnessError("owned Postgres container exited before becoming healthy")
        time.sleep(1)
    raise HarnessError(f"owned Postgres container did not become healthy: {last_status}")


def inspect_exact(endpoint: DockerEndpoint, kind: str, identifier: str) -> dict[str, object]:
    arguments = ("inspect", identifier) if kind == "container" else (kind, "inspect", identifier)
    payload = json.loads(docker(endpoint, *arguments).stdout)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise HarnessError(f"owned {kind} inspect result is malformed")
    return payload[0]


def apply_exact_migrations(endpoint: DockerEndpoint, root: Path) -> None:
    version = capture(
        ["supabase", "--version"], environment=endpoint.environment(), timeout=30
    ).splitlines()[0]
    if version != EXPECTED_SUPABASE_CLI_VERSION:
        raise HarnessError("Supabase CLI version changed from the reviewed harness version")
    endpoint.assert_stable()
    try:
        run(
            [
                "supabase", "migration", "up",
                "--local",
                "--include-all",
                "--workdir", str(root),
                "--yes",
            ],
            cwd=root,
            environment=endpoint.environment(),
            timeout=600,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "Supabase CLI returned no diagnostic").strip()
        raise HarnessError(f"exact migration application failed: {detail}") from None
    endpoint.assert_stable()


def validate_migration_head(endpoint: DockerEndpoint, container_id: str) -> None:
    sql = "select version from supabase_migrations.schema_migrations order by version desc limit 1;"
    observed = docker(
        endpoint,
        "exec", container_id,
        "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres",
        "-c", sql,
    ).stdout.strip()
    if observed != EXPECTED_MIGRATION_VERSION:
        raise HarnessError("owned database migration ledger is not at the exact accepted head")


def create_sentinel(
    endpoint: DockerEndpoint, container_id: str, *, schema: str, sentinel: str
) -> str:
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
    endpoint.assert_stable()
    completed = docker(
        endpoint,
        "exec", "-i", container_id,
        "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres",
        input_text=sql,
    )
    system_identifier = completed.stdout.strip()
    if not re.fullmatch(r"[0-9]+", system_identifier):
        raise HarnessError("owned database did not return a PostgreSQL system identifier")
    endpoint.assert_stable()
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


def _resource_labels(endpoint: DockerEndpoint, kind: str, identifier: str) -> object:
    inspected = inspect_exact(endpoint, kind, identifier)
    if kind == "container":
        return _container_labels(inspected)
    return inspected.get("Labels")


def cleanup(endpoint: DockerEndpoint, resources: Resources, project_id: str) -> None:
    if not resources.any():
        return
    endpoint.assert_stable()
    with block_cancellation_signals():
        exact = (
            ("container", resources.container_id),
            ("volume", resources.volume_name),
            ("network", resources.network_id),
        )
        for kind, identifier in exact:
            if identifier is not None:
                _require_owned_labels(
                    _resource_labels(endpoint, kind, identifier), project_id, kind
                )
        if resources.container_id is not None:
            docker(endpoint, "rm", "--force", resources.container_id)
        if resources.volume_name is not None:
            docker(endpoint, "volume", "rm", resources.volume_name)
        if resources.network_id is not None:
            docker(endpoint, "network", "rm", resources.network_id)
        endpoint.assert_stable()
        for kind, identifier in exact:
            if identifier is None:
                continue
            arguments = (
                ("inspect", identifier)
                if kind == "container"
                else (kind, "inspect", identifier)
            )
            inspected = docker(endpoint, *arguments, check=False)
            if inspected.returncode == 0:
                raise HarnessError(f"exact owned {kind} remains after cleanup")
        unknown = resources_for_project(endpoint, project_id)
        if any(unknown):
            raise HarnessError("unknown project-labeled resources remain; refusing broad cleanup")


def run_owned_action(
    endpoint: DockerEndpoint,
    resources: Resources,
    project_id: str,
    action: Callable[[], None],
) -> None:
    try:
        action()
    finally:
        cleanup(endpoint, resources, project_id)


def main() -> int:
    reject_caller_database_target(os.environ)
    source_value = os.environ.get("WORKER_CONTROL_PLANE_DATABASE_WORKTREE", "")
    if not source_value:
        raise HarnessError("WORKER_CONTROL_PLANE_DATABASE_WORKTREE is required")
    source_root = Path(source_value).resolve(strict=True)
    worker_root = Path.cwd().resolve(strict=True)
    validate_source_root(source_root)
    worker_sha = validate_worker_source(worker_root)
    project_id = f"w192-{secrets.token_hex(12)}"
    sentinel_schema = f"worker_harness_{secrets.token_hex(16)}"
    sentinel = secrets.token_hex(32)
    password = LOCAL_POSTGRES_PASSWORD
    jwt_secret = secrets.token_hex(32)
    resources = Resources()

    with tempfile.TemporaryDirectory(prefix="worker192-db-") as directory, \
            installed_cancellation_handlers():
        temp_root = Path(directory)
        database_root = temp_root / "database-engine"
        endpoint = require_local_docker_endpoint(temp_root / "docker-config")
        if any(resources_for_project(endpoint, project_id)):
            raise HarnessError("random harness project identity already exists")

        def qualify() -> None:
            nonlocal worker_sha
            validate_worker_source(worker_root, worker_sha)
            validate_source_root(source_root)
            extract_exact_database_source(source_root, database_root)
            image_id = require_pinned_image(endpoint)
            network_name = f"supabase_network_{project_id}"
            volume_name = f"supabase_db_{project_id}"
            create_network(
                endpoint, resources, project_id=project_id, name=network_name
            )
            create_volume(
                endpoint, resources, project_id=project_id, name=volume_name
            )
            create_container(
                endpoint,
                resources,
                project_id=project_id,
                network_name=network_name,
                volume_name=volume_name,
                password=password,
                jwt_secret=jwt_secret,
            )
            assert resources.container_id is not None
            endpoint.assert_stable()
            docker(endpoint, "start", resources.container_id)
            endpoint.assert_stable()
            wait_for_database(endpoint, resources.container_id)
            inspected = inspect_exact(endpoint, "container", resources.container_id)
            container_id, db_port = validate_database_container(
                inspected, project_id=project_id, expected_image_id=image_id
            )
            database_url = validate_status_database_url(
                f"postgresql://postgres:{quote(password, safe='')}@127.0.0.1:{db_port}/postgres"
                "?sslmode=disable",
                expected_port=db_port,
            )
            rewrite_local_config(
                database_root / "supabase" / "config.toml",
                project_id=project_id,
                db_port=db_port,
            )
            apply_exact_migrations(endpoint, database_root)
            validate_migration_head(endpoint, container_id)
            system_identifier = create_sentinel(
                endpoint, container_id, schema=sentinel_schema, sentinel=sentinel
            )
            endpoint.assert_stable()
            current = inspect_exact(endpoint, "container", container_id)
            current_id, current_port = validate_database_container(
                current, project_id=project_id, expected_image_id=image_id
            )
            if current_id != container_id or current_port != db_port:
                raise HarnessError("owned database identity/binding changed before behavioral execution")
            validate_worker_source(worker_root, worker_sha)
            database = TrustedDatabase(
                database_url, container_id, system_identifier, sentinel_schema, sentinel
            )
            print(json.dumps({
                "databaseSha": EXPECTED_DATABASE_SHA,
                "migrationVersion": EXPECTED_MIGRATION_VERSION,
                "workerSha": worker_sha,
                "targetKind": "runner-owned-local-supabase",
                "networkExposure": "127.0.0.1-only",
                "dockerDaemonId": endpoint.daemon_id,
                "dockerDaemonName": endpoint.daemon_name,
                "dockerSocketRealpath": str(endpoint.socket_realpath),
                "postgresImage": EXPECTED_POSTGRES_IMAGE,
                "containerId": container_id,
                "roleProof": "SET ROLE service_role ACL matrix; deployment login external",
            }, sort_keys=True))
            endpoint.assert_stable()
            run(
                [
                    "cargo", "test", "-p", "solver-worker", "--test",
                    "worker_control_plane_database_contract", "--", "--ignored",
                    "worker_control_plane_",
                ],
                cwd=worker_root,
                environment=cargo_environment(database),
                check=True,
                timeout=900,
            )
            endpoint.assert_stable()

        run_owned_action(endpoint, resources, project_id, qualify)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        HarnessError,
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as exc:
        print(f"Worker DB harness failed closed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
