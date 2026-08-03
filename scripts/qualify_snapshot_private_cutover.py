#!/usr/bin/env python3
"""Qualify Worker Issue #199 against the exact private snapshot contract.

The static gate is always run. Live qualification accepts only an exact
database-engine source checkout. The runner clones that commit into its own
unique temporary workdir, allocates a unique local Supabase project and ports,
starts the stack, runs the proof, stops it without backup, verifies zero Docker
resource residue, and removes the workdir. No caller-supplied database URL is a
valid destructive target.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from urllib.parse import urlparse

import psycopg
from psycopg import conninfo, sql


ROOT = Path(__file__).resolve().parents[1]
DB_SHA = "c5356d2b0d340f9c5c31a645479be5f3d19a52db"
MIGRATION_HEAD = "20260803090000"
RELATIONS = (
    "lca_active_snapshots",
    "lca_network_snapshots",
    "lca_snapshot_artifacts",
)
CRITICAL_RELATIONS = {
    "auth.sessions",
    "auth.users",
    "private.lca_active_snapshots",
    "private.lca_network_snapshots",
    "private.lca_snapshot_artifacts",
    "public.lca_package_artifacts",
    "public.lca_package_export_items",
    "public.lca_package_request_cache",
    "public.lcia_result_packages",
}
ACTIVE_SOURCE_ROOTS = ("crates", "scripts", "tools", "docs/sql")
SCANNED_SUFFIXES = {".rs", ".py", ".sh", ".sql"}
EXPECTED_STATIC_FILE_COUNT = 99
# Deliberately pinned to the reviewed active-source inventory. Adding/removing a
# Rust, Python, shell, or SQL source requires updating this qualification contract.
EXPECTED_STATIC_INVENTORY_SHA256 = "fd5a15a595b20719c590d1d797440c4c56ddacbee73d2e8c8c0e17ff24fc32ed"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


RELATION_NAME = r"(?:lca_active_snapshots|lca_network_snapshots|lca_snapshot_artifacts)"
RELATION_PATTERN = rf'(?:{RELATION_NAME}|"{RELATION_NAME}")'
IDENTIFIER_PATTERN = r'(?:[a-z_][a-z0-9_]*|"[a-z_][a-z0-9_]*")'
PUBLIC_PATTERN = re.compile(rf'(?:\bpublic\b|"public")\s*\.\s*{RELATION_PATTERN}', re.I)
SQL_RELATION_PATTERN = re.compile(
    rf"\b(?:from|join|update|insert\s+into|delete\s+from)\s+(?:only\s+)?"
    rf"(?:(?P<schema>{IDENTIFIER_PATTERN})\s*\.\s*)?(?P<relation>{RELATION_PATTERN})(?![a-z0-9_])",
    re.I,
)


def line_for_offset(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def mask_sql_comments(text: str) -> str:
    def mask(match: re.Match[str]) -> str:
        return "".join("\n" if character == "\n" else " " for character in match.group(0))

    return re.sub(r"/\*.*?\*/|--[^\n]*", mask, text, flags=re.S)


def scan_consumer_text(text: str, label: str) -> tuple[list[str], list[str]]:
    text = mask_sql_comments(text)
    public_hits = [
        f"{label}:{line_for_offset(text, match.start())}:{match.group(0)!r}"
        for match in PUBLIC_PATTERN.finditer(text)
    ]
    unqualified_hits: list[str] = []
    for match in SQL_RELATION_PATTERN.finditer(text):
        schema = match.group("schema")
        if schema is None:
            unqualified_hits.append(
                f"{label}:{line_for_offset(text, match.start('relation'))}:{match.group(0)!r}"
            )
    return public_hits, unqualified_hits


def active_source_inventory() -> list[Path]:
    inventory: list[Path] = []
    for root_name in ACTIVE_SOURCE_ROOTS:
        for path in (ROOT / root_name).rglob("*"):
            if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
                continue
            if "target" in path.parts or path.name == Path(__file__).name:
                continue
            inventory.append(path.relative_to(ROOT))
    return sorted(inventory)


def static_scanner_self_test() -> None:
    malicious = 'SELECT *\nFROM ONLY "PuBlIc" /* boundary */\n . \n "LCA_NETWORK_SNAPSHOTS"'
    public_hits, unqualified_hits = scan_consumer_text(malicious, "public-newline-fixture")
    require(len(public_hits) == 1, "scanner failed PUBLIC/case/whitespace/newline fixture")
    require(not unqualified_hits, "qualified public fixture was misclassified as unqualified")
    malicious = 'DELETE /* verb boundary */\nFROM -- relation boundary\nONLY "LcA_SnApShOt_ArTiFaCtS" WHERE true'
    public_hits, unqualified_hits = scan_consumer_text(malicious, "unqualified-newline-fixture")
    require(not public_hits, "unqualified fixture was misclassified as public")
    require(len(unqualified_hits) == 1, "scanner failed unqualified/case/whitespace/newline fixture")
    safe = 'INSERT\n INTO "private" /* exact canonical */\n.\n "lca_active_snapshots"(scope) VALUES (\'x\')'
    public_hits, unqualified_hits = scan_consumer_text(safe, "private-newline-fixture")
    require(not public_hits and not unqualified_hits, "scanner rejected exact private fixture")
    commented_out = "-- SELECT * FROM public.lca_network_snapshots\nSELECT 1"
    public_hits, unqualified_hits = scan_consumer_text(commented_out, "comment-only-fixture")
    require(not public_hits and not unqualified_hits, "scanner treated a SQL comment as a consumer")


def partition_s3_keys(keys: list[str], prefix: str) -> tuple[list[str], list[str]]:
    owned = [key for key in keys if key == prefix or key.startswith(prefix + "/")]
    foreign = [key for key in keys if key not in owned]
    return owned, foreign


def safety_fence_self_test() -> None:
    owned, foreign = partition_s3_keys(
        ["worker199/run", "worker199/run/a", "worker199/runaway", "foreign/object"],
        "worker199/run",
    )
    require(owned == ["worker199/run", "worker199/run/a"], "prefix fence admitted foreign keys")
    require(
        foreign == ["worker199/runaway", "foreign/object"],
        "prefix fence did not retain/report foreign keys",
    )


def static_consumer_zero() -> dict[str, object]:
    static_scanner_self_test()
    safety_fence_self_test()
    inventory = active_source_inventory()
    inventory_text = "\n".join(path.as_posix() for path in inventory) + "\n"
    inventory_hash = hashlib.sha256(inventory_text.encode()).hexdigest()
    require(
        len(inventory) == EXPECTED_STATIC_FILE_COUNT,
        f"active-source inventory count drifted: expected {EXPECTED_STATIC_FILE_COUNT}, got {len(inventory)}",
    )
    require(
        inventory_hash == EXPECTED_STATIC_INVENTORY_SHA256,
        "active-source inventory changed; review scope and update the pinned inventory hash: "
        f"expected {EXPECTED_STATIC_INVENTORY_SHA256}, got {inventory_hash}",
    )
    public_hits: list[str] = []
    unqualified_hits: list[str] = []
    for relative_path in inventory:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        found_public, found_unqualified = scan_consumer_text(text, relative_path.as_posix())
        public_hits.extend(found_public)
        unqualified_hits.extend(found_unqualified)
    require(not public_hits, "public compatibility consumers remain:\n" + "\n".join(public_hits))
    require(
        not unqualified_hits,
        "search_path-dependent snapshot consumers remain:\n" + "\n".join(unqualified_hits),
    )
    return {
        "filesScanned": len(inventory),
        "inventorySha256": inventory_hash,
        "publicConsumers": 0,
        "unqualifiedConsumers": 0,
        "scannerFixtures": 4,
        "destructiveSafetyFixtures": 1,
    }


def loopback_only(database_url: str) -> None:
    parsed = urlparse(database_url)
    require(parsed.scheme in {"postgres", "postgresql"}, "database URL must be PostgreSQL")
    require(parsed.hostname in {"127.0.0.1", "localhost", "::1"}, "database URL must be loopback")


def normalized_database_endpoint(database_url: str) -> dict[str, object]:
    parsed = urlparse(database_url)
    loopback_only(database_url)
    require(parsed.port is not None, "database URL must contain an explicit port")
    require(bool(parsed.path and parsed.path != "/"), "database URL must contain a database name")
    return {
        "host": "loopback",
        "port": parsed.port,
        "database": parsed.path.removeprefix("/"),
    }


def normalized_http_endpoint(endpoint: str) -> dict[str, object]:
    parsed = urlparse(endpoint)
    require(parsed.scheme in {"http", "https"}, "endpoint must be HTTP(S)")
    require(parsed.hostname in {"127.0.0.1", "localhost", "::1"}, "endpoint must be loopback")
    require(parsed.port is not None, "endpoint must contain an explicit port")
    return {
        "scheme": parsed.scheme,
        "host": "loopback",
        "port": parsed.port,
        "path": parsed.path.rstrip("/"),
    }


def database_server_identity(database_url: str) -> dict[str, object]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_database(), inet_server_port(),
                       (SELECT system_identifier::text FROM pg_control_system()),
                       current_setting('data_directory')
                """
            )
            database, port, system_identifier, data_directory = cursor.fetchone()
    return {
        "database": database,
        "port": port,
        "systemIdentifier": system_identifier,
        "dataDirectory": data_directory,
    }


def supabase_local_status(database_repo: Path) -> dict[str, object]:
    completed = subprocess.run(
        ["supabase", "status", "--workdir", str(database_repo.resolve()), "-o", "json"],
        text=True,
        capture_output=True,
        check=False,
    )
    require(
        completed.returncode == 0,
        "database workdir does not own a running Supabase local stack:\n"
        + completed.stdout
        + completed.stderr,
    )
    status = json.loads(completed.stdout)
    require(isinstance(status.get("DB_URL"), str), "Supabase status did not return DB_URL")
    return status


def command_output(command: list[str], cwd: Path = ROOT) -> str:
    completed = subprocess.run(command, cwd=cwd, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


def inspect_database_checkout(
    database_repo: Path, expected_sha: str, expected_head: str
) -> dict[str, object]:
    database_repo = database_repo.resolve()
    require((database_repo / ".git").exists(), f"database repo is not a git checkout: {database_repo}")
    actual_sha = command_output(["git", "rev-parse", "HEAD"], database_repo)
    require(
        actual_sha == expected_sha,
        f"database checkout SHA mismatch: expected {expected_sha}, got {actual_sha}",
    )
    migrations = [
        (match.group(1), path)
        for path in (database_repo / "supabase" / "migrations").glob("*.sql")
        if (match := re.match(r"^(\d+)_", path.name))
    ]
    require(migrations, "database checkout has no versioned Supabase migrations")
    duplicates = sorted(
        version for version in {item[0] for item in migrations} if sum(v == version for v, _ in migrations) > 1
    )
    require(not duplicates, f"duplicate migration versions exist: {duplicates}")
    versions = sorted(version for version, _ in migrations)
    require(
        versions[-1] == expected_head,
        f"database checkout migration head mismatch: expected {expected_head}, got {versions[-1]}",
    )
    head_paths = [path for version, path in migrations if version == expected_head]
    require(
        len(head_paths) == 1,
        f"expected exactly one migration at head {expected_head}, got {len(head_paths)}",
    )
    dirty = command_output(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", "supabase/migrations"],
        database_repo,
    )
    require(not dirty, f"database migration paths are dirty/untracked:\n{dirty}")
    migration_path = head_paths[0]
    relative_path = migration_path.relative_to(database_repo).as_posix()
    command_output(["git", "ls-files", "--error-unmatch", relative_path], database_repo)
    blob = subprocess.run(
        ["git", "show", f"HEAD:{relative_path}"],
        cwd=database_repo,
        check=True,
        capture_output=True,
    ).stdout
    current = migration_path.read_bytes()
    require(current == blob, f"migration worktree bytes differ from authoritative HEAD blob: {relative_path}")
    blob_oid = command_output(["git", "rev-parse", f"HEAD:{relative_path}"], database_repo)
    return {
        "path": str(database_repo),
        "headSha": actual_sha,
        "migrationHead": versions[-1],
        "migrationFile": migration_path.name,
        "migrationGitBlobOid": blob_oid,
        "migrationFileSha256": hashlib.sha256(current).hexdigest(),
        "migrationPathsClean": True,
        "uniqueMigrationVersions": True,
    }


def require_checkout_failure(
    database_repo: Path, expected_sha: str, expected_head: str, expected_message: str
) -> None:
    try:
        inspect_database_checkout(database_repo, expected_sha, expected_head)
    except AssertionError as error:
        require(expected_message in str(error), f"unexpected checkout negative result: {error}")
        return
    raise AssertionError(f"checkout negative fixture unexpectedly passed: {expected_message}")


def database_checkout_self_test() -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="worker199-db-checkout-") as directory:
        repo = Path(directory)
        migration_dir = repo / "supabase" / "migrations"
        migration_dir.mkdir(parents=True)
        head = "20990101000000"
        migration = migration_dir / f"{head}_head.sql"
        migration.write_text("select 1;\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Worker Qualification",
                "-c",
                "user.email=worker-qualification@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            cwd=repo,
            check=True,
        )
        fixture_sha = command_output(["git", "rev-parse", "HEAD"], repo)
        migration.write_text("select 2;\n", encoding="utf-8")
        require_checkout_failure(repo, fixture_sha, head, "dirty/untracked")
        migration.write_text("select 1;\n", encoding="utf-8")
        duplicate = migration_dir / f"{head}_duplicate.sql"
        duplicate.write_text("select 3;\n", encoding="utf-8")
        require_checkout_failure(repo, fixture_sha, head, "duplicate migration versions")
    return {"dirtyMigrationRejected": True, "duplicateVersionRejected": True}


def verify_database_checkout(database_repo: Path) -> dict[str, object]:
    return inspect_database_checkout(database_repo, DB_SHA, MIGRATION_HEAD)


def allocate_loopback_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            sockets.append(listener)
        return [int(listener.getsockname()[1]) for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def configure_ephemeral_supabase(config_path: Path, project_id: str, ports: list[int]) -> None:
    require(len(ports) == 7 and len(set(ports)) == 7, "ephemeral port allocation is invalid")
    section_ports = {
        ("api", "port"): ports[0],
        ("db", "port"): ports[1],
        ("db", "shadow_port"): ports[2],
        ("db.pooler", "port"): ports[3],
        ("studio", "port"): ports[4],
        ("inbucket", "port"): ports[5],
        ("analytics", "port"): ports[6],
    }
    lines = config_path.read_text(encoding="utf-8").splitlines()
    section = ""
    replaced: set[tuple[str, str]] = set()
    project_replaced = False
    rendered: list[str] = []
    for line in lines:
        section_match = re.match(r"^\[([^]]+)]\s*$", line)
        if section_match:
            section = section_match.group(1)
        if not section and re.match(r"^project_id\s*=", line):
            rendered.append(f'project_id = "{project_id}"')
            project_replaced = True
            continue
        key_match = re.match(r"^(port|shadow_port)\s*=", line)
        if key_match and (section, key_match.group(1)) in section_ports:
            key = key_match.group(1)
            rendered.append(f"{key} = {section_ports[(section, key)]}")
            replaced.add((section, key))
            continue
        rendered.append(line)
    require(project_replaced, "Supabase config has no top-level project_id")
    require(replaced == set(section_ports), f"Supabase config port surface drifted: {replaced}")
    config_path.write_text("\n".join(rendered) + "\n", encoding="utf-8")


def docker_resources_for_project(project_id: str) -> dict[str, list[str]]:
    label = f"com.supabase.cli.project={project_id}"
    commands = {
        "containers": ["docker", "ps", "-a", "--filter", f"label={label}", "--format", "{{.ID}}"],
        "volumes": ["docker", "volume", "ls", "--filter", f"label={label}", "--format", "{{.Name}}"],
        "networks": ["docker", "network", "ls", "--filter", f"label={label}", "--format", "{{.ID}}"],
    }
    return {
        kind: [line for line in command_output(command).splitlines() if line]
        for kind, command in commands.items()
    }


class RunnerOwnedSupabaseStack:
    """One qualification-owned clone, project identity, ports, and data volumes."""

    def __init__(self, source_repo: Path):
        self.source_repo = source_repo.resolve()
        self.run_id = uuid.uuid4().hex
        self.project_id = f"worker199-ephemeral-{self.run_id[:12]}"
        self.temp_root = Path(tempfile.mkdtemp(prefix="worker199-ephemeral-supabase-"))
        self.workdir = self.temp_root / "database-engine"
        self.database_url = ""
        self.storage_endpoint = ""
        self.storage_environment: dict[str, str] = {}
        self.start_log_sha256 = ""
        self.destruction: dict[str, object] | None = None

    def __enter__(self) -> "RunnerOwnedSupabaseStack":
        try:
            return self._start()
        except BaseException as start_error:
            try:
                self.destruction = self.destroy()
            except BaseException as cleanup_error:
                raise ExceptionGroup(
                    "ephemeral Supabase start and cleanup failed", [start_error, cleanup_error]
                )
            raise

    def _start(self) -> "RunnerOwnedSupabaseStack":
        verify_database_checkout(self.source_repo)
        require(
            not any(docker_resources_for_project(self.project_id).values()),
            "runner-generated Supabase project identity already has Docker resources",
        )
        subprocess.run(
            ["git", "clone", "--quiet", "--shared", "--no-checkout", str(self.source_repo), str(self.workdir)],
            check=True,
        )
        subprocess.run(["git", "checkout", "--quiet", "--detach", DB_SHA], cwd=self.workdir, check=True)
        verify_database_checkout(self.workdir)
        ports = allocate_loopback_ports(7)
        configure_ephemeral_supabase(self.workdir / "supabase" / "config.toml", self.project_id, ports)
        start = subprocess.run(
            [
                "supabase",
                "start",
                "--workdir",
                str(self.workdir),
                "--exclude",
                "realtime,imgproxy,mailpit,studio,edge-runtime,logflare,vector,supavisor",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        start_log = start.stdout + start.stderr
        self.start_log_sha256 = hashlib.sha256(start_log.encode()).hexdigest()
        require(start.returncode == 0, f"ephemeral Supabase start failed:\n{start_log}")
        status = supabase_local_status(self.workdir)
        self.database_url = str(status["DB_URL"])
        self.storage_endpoint = str(status["STORAGE_S3_URL"])
        self.storage_environment = {
            "AWS_ACCESS_KEY_ID": str(status["S3_PROTOCOL_ACCESS_KEY_ID"]),
            "AWS_SECRET_ACCESS_KEY": str(status["S3_PROTOCOL_ACCESS_KEY_SECRET"]),
            "AWS_DEFAULT_REGION": str(status["S3_PROTOCOL_REGION"]),
            "AWS_EC2_METADATA_DISABLED": "true",
            "S3_ENDPOINT": self.storage_endpoint,
            "S3_ACCESS_KEY_ID": str(status["S3_PROTOCOL_ACCESS_KEY_ID"]),
            "S3_SECRET_ACCESS_KEY": str(status["S3_PROTOCOL_ACCESS_KEY_SECRET"]),
            "S3_REGION": str(status["S3_PROTOCOL_REGION"]),
        }
        self.assert_owned_running()
        return self

    def assert_owned_running(self) -> dict[str, object]:
        require(self.workdir.parent == self.temp_root, "ephemeral workdir escaped runner temp root")
        require(self.temp_root.name.startswith("worker199-ephemeral-supabase-"), "temp root identity drift")
        verify_database_checkout(self.workdir)
        status = supabase_local_status(self.workdir)
        require(str(status["DB_URL"]) == self.database_url, "runner-owned DB URL changed")
        require(
            str(status["STORAGE_S3_URL"]) == self.storage_endpoint,
            "runner-owned Storage endpoint changed",
        )
        resources = docker_resources_for_project(self.project_id)
        require(bool(resources["containers"]), "runner-owned project has no running containers")
        return {
            "ownership": "runner-created-and-exclusive",
            "projectId": self.project_id,
            "databaseCheckoutSha": DB_SHA,
            "migrationHead": MIGRATION_HEAD,
            "databaseEndpoint": normalized_database_endpoint(self.database_url),
            "storageEndpoint": normalized_http_endpoint(self.storage_endpoint),
            "server": database_server_identity(self.database_url),
            "startLogSha256": self.start_log_sha256,
        }

    def reset(self) -> None:
        self.assert_owned_running()
        reset = subprocess.run(
            ["supabase", "db", "reset", "--workdir", str(self.workdir), "--yes"],
            text=True,
            capture_output=True,
            check=False,
        )
        require(reset.returncode == 0, f"runner-owned database reset failed:\n{reset.stdout}{reset.stderr}")
        self.assert_owned_running()

    def destroy(self) -> dict[str, object]:
        resources_before = docker_resources_for_project(self.project_id)
        if any(resources_before.values()):
            command = ["supabase", "stop", "--project-id", self.project_id, "--no-backup"]
            if (self.workdir / "supabase" / "config.toml").is_file():
                command.extend(["--workdir", str(self.workdir)])
            stop = subprocess.run(
                command,
                text=True,
                capture_output=True,
                check=False,
            )
            require(
                stop.returncode == 0,
                f"ephemeral Supabase stop failed:\n{stop.stdout}{stop.stderr}",
            )
            stop_status = "succeeded-with-no-backup"
        else:
            stop_status = "not-started-no-resources"
        resources = docker_resources_for_project(self.project_id)
        require(not any(resources.values()), f"ephemeral Docker residue remains: {resources}")
        if self.temp_root.exists():
            shutil.rmtree(self.temp_root)
        require(not self.temp_root.exists(), "ephemeral workdir remains after teardown")
        return {
            "supabaseStop": stop_status,
            "containersRemaining": 0,
            "volumesRemaining": 0,
            "networksRemaining": 0,
            "workdirRemaining": False,
        }

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> bool:
        try:
            self.destruction = self.destroy()
        except BaseException as cleanup_error:
            if exc is not None:
                raise ExceptionGroup("qualification and ephemeral teardown failed", [exc, cleanup_error])
            raise
        return False


def expect_relation_denied(
    admin: psycopg.Connection, denied_role: str, schema: str, relation: str
) -> None:
    try:
        with admin.transaction():
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(denied_role)))
                cursor.execute(
                    sql.SQL("SELECT count(*) FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(relation)
                    )
                )
    except psycopg.errors.InsufficientPrivilege:
        return
    raise AssertionError(f"{denied_role} unexpectedly read {schema}.{relation}")


def relation_parity(
    connection: psycopg.Connection,
    relation: str,
    predicate: sql.SQL,
    parameters: tuple[object, ...],
    order_by: sql.SQL,
) -> dict[str, object]:
    observations: dict[str, dict[str, object]] = {}
    with connection.cursor() as cursor:
        for schema in ("private", "public"):
            cursor.execute(
                sql.SQL(
                    "SELECT count(*), md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY {})::text, '[]')) "
                    "FROM (SELECT * FROM {}.{} WHERE {}) t"
                ).format(
                    order_by,
                    sql.Identifier(schema),
                    sql.Identifier(relation),
                    predicate,
                ),
                parameters,
            )
            count, digest = cursor.fetchone()
            observations[schema] = {"cardinality": count, "fullRowMd5": digest}
        cursor.execute(
            sql.SQL(
                "SELECT count(*) FROM ((SELECT * FROM private.{} WHERE {}) "
                "EXCEPT ALL (SELECT * FROM public.{} WHERE {})) delta"
            ).format(sql.Identifier(relation), predicate, sql.Identifier(relation), predicate),
            parameters + parameters,
        )
        private_minus_public = cursor.fetchone()[0]
        cursor.execute(
            sql.SQL(
                "SELECT count(*) FROM ((SELECT * FROM public.{} WHERE {}) "
                "EXCEPT ALL (SELECT * FROM private.{} WHERE {})) delta"
            ).format(sql.Identifier(relation), predicate, sql.Identifier(relation), predicate),
            parameters + parameters,
        )
        public_minus_private = cursor.fetchone()[0]
    require(
        observations["private"] == observations["public"],
        f"{relation} independent cardinality/full-row hash mismatch: {observations}",
    )
    require(
        private_minus_public == 0 and public_minus_private == 0,
        f"{relation} full-row bidirectional EXCEPT ALL mismatch: "
        f"private-public={private_minus_public}, public-private={public_minus_private}",
    )
    return {
        **observations,
        "privateMinusPublic": private_minus_public,
        "publicMinusPrivate": public_minus_private,
    }


def digest_rows(connection: psycopg.Connection, ids: list[uuid.UUID], scope: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT jsonb_build_object(
              'network', COALESCE((
                SELECT jsonb_agg(to_jsonb(n) ORDER BY n.id)
                FROM private.lca_network_snapshots n WHERE n.id = ANY(%s::uuid[])
              ), '[]'::jsonb),
              'artifact', COALESCE((
                SELECT jsonb_agg(to_jsonb(a) ORDER BY a.snapshot_id, a.artifact_format)
                FROM private.lca_snapshot_artifacts a WHERE a.snapshot_id = ANY(%s::uuid[])
              ), '[]'::jsonb),
              'active', COALESCE((
                SELECT jsonb_agg(to_jsonb(x) ORDER BY x.scope)
                FROM private.lca_active_snapshots x WHERE x.scope = %s
              ), '[]'::jsonb)
            )::text
            """,
            (ids, ids, scope),
        )
        payload = cursor.fetchone()[0]
    return hashlib.sha256(payload.encode()).hexdigest()


def insert_network(cursor: psycopg.Cursor, snapshot_id: uuid.UUID, source_hash: str) -> None:
    cursor.execute(
        """
        INSERT INTO private.lca_network_snapshots
          (id, scope, process_filter, source_hash, status)
        VALUES (%s, 'full_library', '{"issue":199}'::jsonb, %s, 'ready')
        ON CONFLICT (id) DO UPDATE SET
          process_filter = EXCLUDED.process_filter,
          source_hash = EXCLUDED.source_hash,
          status = EXCLUDED.status,
          updated_at = now()
        """,
        (snapshot_id, source_hash),
    )


def insert_artifact(cursor: psycopg.Cursor, snapshot_id: uuid.UUID, suffix: str) -> None:
    cursor.execute(
        """
        INSERT INTO private.lca_snapshot_artifacts
          (snapshot_id, artifact_url, artifact_sha256, artifact_byte_size,
           artifact_format, process_count, flow_count, impact_count,
           a_nnz, b_nnz, c_nnz, coverage, status)
        VALUES (%s, %s, %s, 3, 'snapshot-hdf5:v1', 1, 1, 1, 1, 1, 1,
                '{"issue":199}'::jsonb, 'ready')
        ON CONFLICT (snapshot_id, artifact_format) DO UPDATE SET
          artifact_url = EXCLUDED.artifact_url,
          artifact_sha256 = EXCLUDED.artifact_sha256,
          coverage = EXCLUDED.coverage,
          status = EXCLUDED.status,
          updated_at = now()
        """,
        (snapshot_id, f"s3://issue-199/{suffix}", hashlib.sha256(suffix.encode()).hexdigest()),
    )


def upsert_active(cursor: psycopg.Cursor, scope: str, snapshot_id: uuid.UUID, suffix: str) -> None:
    cursor.execute(
        """
        INSERT INTO private.lca_active_snapshots
          (scope, snapshot_id, source_hash, note)
        VALUES (%s, %s, %s, 'worker issue 199 qualification')
        ON CONFLICT (scope) DO UPDATE SET
          snapshot_id = EXCLUDED.snapshot_id,
          source_hash = EXCLUDED.source_hash,
          activated_at = now(),
          note = EXCLUDED.note
        """,
        (scope, snapshot_id, suffix),
    )


def qualify_database(stack: RunnerOwnedSupabaseStack) -> dict[str, object]:
    stack.assert_owned_running()
    database_url = stack.database_url
    database_repo = stack.workdir
    loopback_only(database_url)
    checkout_negative_fixtures = database_checkout_self_test()
    checkout = verify_database_checkout(database_repo)
    role_name = f"worker_issue_199_{os.getpid()}_{secrets.token_hex(4)}"
    role_password = secrets.token_urlsafe(24)
    worker_url = conninfo.make_conninfo(database_url, user=role_name, password=role_password)
    snapshot_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
    failed_id = uuid.uuid4()
    scope = f"issue_199_{secrets.token_hex(6)}"
    created_role = False
    result: dict[str, object] | None = None

    with psycopg.connect(database_url, autocommit=True) as admin:
        try:
            with admin.cursor() as cursor:
                cursor.execute("SELECT max(version) FROM supabase_migrations.schema_migrations")
                database_head = cursor.fetchone()[0]
                require(
                    database_head == MIGRATION_HEAD,
                    f"database migration max head mismatch: expected {MIGRATION_HEAD}, got {database_head}",
                )
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='private' AND c.relkind='r' AND c.relname=ANY(%s)
                    """,
                    (list(RELATIONS),),
                )
                require(cursor.fetchone()[0] == 3, "private canonical table set is incomplete")
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='public' AND c.relkind='v' AND c.relname=ANY(%s)
                    """,
                    (list(RELATIONS),),
                )
                require(cursor.fetchone()[0] == 3, "Expand compatibility view set is incomplete")
                cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOBYPASSRLS INHERIT"
                    ).format(sql.Identifier(role_name), sql.Literal(role_password))
                )
                created_role = True
                cursor.execute(sql.SQL("GRANT service_role TO {}").format(sql.Identifier(role_name)))

            for denied_role in ("anon", "authenticated"):
                for schema in ("private", "public"):
                    for relation in RELATIONS:
                        expect_relation_denied(admin, denied_role, schema, relation)

            with psycopg.connect(worker_url) as worker:
                with worker.cursor() as cursor:
                    cursor.execute("SET search_path TO pg_temp, public")
                    cursor.execute(
                        """
                        SELECT current_user, r.rolsuper, r.rolbypassrls,
                               has_schema_privilege(current_user, 'private', 'USAGE'),
                               bool_and(has_table_privilege(current_user, 'private.' || name,
                                 'SELECT,INSERT,UPDATE,DELETE'))
                        FROM pg_roles r
                        CROSS JOIN unnest(%s::text[]) name
                        WHERE r.rolname=current_user
                        GROUP BY current_user, r.rolsuper, r.rolbypassrls
                        """,
                        (list(RELATIONS),),
                    )
                    identity = cursor.fetchone()
                    require(identity[0] == role_name, "qualification did not use the dedicated login")
                    require(not identity[1] and not identity[2], "worker login is privileged/BYPASSRLS")
                    require(identity[3] and identity[4], "worker login lacks exact private grants")

                    cursor.execute(
                        "SELECT count(*) FROM private.lca_network_snapshots WHERE id=ANY(%s::uuid[])",
                        (snapshot_ids,),
                    )
                    require(cursor.fetchone()[0] == 0, "test IDs were not blank")

                    for index, snapshot_id in enumerate(snapshot_ids):
                        insert_network(cursor, snapshot_id, f"source-{index}")
                        insert_artifact(cursor, snapshot_id, f"artifact-{index}")
                    upsert_active(cursor, scope, snapshot_ids[0], "active-0")
                worker.commit()

                with worker.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT count(*), count(a.*), count(x.*)
                        FROM private.lca_network_snapshots n
                        JOIN private.lca_snapshot_artifacts a ON a.snapshot_id=n.id
                        LEFT JOIN private.lca_active_snapshots x ON x.snapshot_id=n.id AND x.scope=%s
                        WHERE n.id=ANY(%s::uuid[])
                        """,
                        (scope, snapshot_ids),
                    )
                    require(cursor.fetchone() == (3, 3, 1), "select/join/count parity failed")
                    cursor.execute(
                        "UPDATE private.lca_snapshot_artifacts SET status='stale' WHERE snapshot_id=%s",
                        (snapshot_ids[1],),
                    )
                    cursor.execute(
                        "UPDATE private.lca_network_snapshots SET status='stale' WHERE id=%s",
                        (snapshot_ids[1],),
                    )
                    insert_network(cursor, snapshot_ids[1], "source-1-retry")
                    insert_artifact(cursor, snapshot_ids[1], "artifact-1-retry")
                    upsert_active(cursor, scope, snapshot_ids[0], "active-0-retry")
                worker.commit()

                before_failure = digest_rows(worker, snapshot_ids, scope)
                try:
                    with worker.transaction():
                        with worker.cursor() as cursor:
                            insert_network(cursor, failed_id, "must-rollback")
                            cursor.execute(
                                """
                                INSERT INTO private.lca_snapshot_artifacts
                                  (snapshot_id,artifact_url,artifact_sha256,artifact_byte_size,
                                   artifact_format,process_count,flow_count,impact_count,a_nnz,b_nnz,c_nnz)
                                VALUES (%s,'s3://invalid','bad',-1,'snapshot-hdf5:v1',0,0,0,0,0,0)
                                """,
                                (failed_id,),
                            )
                except psycopg.errors.CheckViolation:
                    pass
                else:
                    raise AssertionError("forced failure did not fail atomically")
                with worker.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM private.lca_network_snapshots WHERE id=%s", (failed_id,)
                    )
                    require(cursor.fetchone()[0] == 0, "failed transaction left a network row")
                require(digest_rows(worker, snapshot_ids, scope) == before_failure, "failure changed rows")

                rollback_digest = digest_rows(worker, snapshot_ids, scope)
                try:
                    with worker.transaction():
                        with worker.cursor() as cursor:
                            cursor.execute(
                                "UPDATE private.lca_network_snapshots SET status='failed' WHERE id=%s",
                                (snapshot_ids[0],),
                            )
                            cursor.execute(
                                """
                                DELETE FROM private.lca_network_snapshots n
                                WHERE n.id=%s AND NOT EXISTS (
                                  SELECT 1 FROM private.lca_active_snapshots x WHERE x.snapshot_id=n.id
                                )
                                """,
                                (snapshot_ids[2],),
                            )
                        raise RuntimeError("intentional rollback")
                except RuntimeError as error:
                    require(str(error) == "intentional rollback", "unexpected rollback error")
                require(digest_rows(worker, snapshot_ids, scope) == rollback_digest, "rollback parity failed")

                parity = {
                    "lca_network_snapshots": relation_parity(
                        worker,
                        "lca_network_snapshots",
                        sql.SQL("id=ANY(%s::uuid[])"),
                        (snapshot_ids,),
                        sql.SQL("id"),
                    ),
                    "lca_snapshot_artifacts": relation_parity(
                        worker,
                        "lca_snapshot_artifacts",
                        sql.SQL("snapshot_id=ANY(%s::uuid[])"),
                        (snapshot_ids,),
                        sql.SQL("snapshot_id, artifact_format, id"),
                    ),
                    "lca_active_snapshots": relation_parity(
                        worker,
                        "lca_active_snapshots",
                        sql.SQL("scope=%s"),
                        (scope,),
                        sql.SQL("scope"),
                    ),
                }

            def concurrent_upsert(index: int) -> None:
                with psycopg.connect(worker_url) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET search_path TO pg_temp, public")
                        upsert_active(cursor, scope, snapshot_ids[index % len(snapshot_ids)], f"race-{index}")
                    connection.commit()

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(concurrent_upsert, range(32)))

            with psycopg.connect(worker_url) as worker:
                with worker.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*), bool_and(snapshot_id=ANY(%s::uuid[])) FROM private.lca_active_snapshots WHERE scope=%s",
                        (snapshot_ids, scope),
                    )
                    require(cursor.fetchone() == (1, True), "concurrent active upsert lost uniqueness")
                    for index, snapshot_id in enumerate(snapshot_ids):
                        insert_network(cursor, snapshot_id, f"source-{index}")
                        insert_artifact(cursor, snapshot_id, f"artifact-{index}")
                    cursor.execute("DELETE FROM private.lca_active_snapshots WHERE scope=%s", (scope,))
                    cursor.execute(
                        "DELETE FROM private.lca_network_snapshots WHERE id=ANY(%s::uuid[])",
                        (snapshot_ids,),
                    )
                worker.commit()

            result = {
                "databaseCheckout": checkout,
                "databaseCheckoutNegativeFixtures": checkout_negative_fixtures,
                "databaseMigrationMaxHead": database_head,
                "identity": "dedicated non-superuser non-BYPASSRLS worker login",
                "aclDenials": {
                    "roles": ["anon", "authenticated"],
                    "relations": [
                        f"{schema}.{relation}"
                        for schema in ("private", "public")
                        for relation in RELATIONS
                    ],
                    "checks": 12,
                },
                "fullRowParity": parity,
                "operations": [
                    "select/join/count",
                    "network/artifact/active upsert",
                    "lifecycle update",
                    "guarded delete rollback",
                    "transaction rollback/failure atomicity",
                    "retry",
                    "three-table independent cardinality/full-row hash and bidirectional EXCEPT ALL parity",
                    "32 concurrent active upserts",
                    "anon/authenticated denial",
                    "search_path poisoning",
                ],
            }
        finally:
            stack.assert_owned_running()
            with admin.cursor() as cursor:
                cursor.execute("DELETE FROM private.lca_active_snapshots WHERE scope=%s", (scope,))
                cursor.execute(
                    "DELETE FROM private.lca_network_snapshots WHERE id=ANY(%s::uuid[])",
                    (snapshot_ids + [failed_id],),
                )
                if created_role:
                    cursor.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=%s AND pid<>pg_backend_pid()",
                        (role_name,),
                    )
                    cursor.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role_name)))
        require(result is not None, "database qualification did not produce a result")
        with admin.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_roles WHERE rolname=%s", (role_name,))
            role_residue = cursor.fetchone()[0]
            cursor.execute(
                "SELECT count(*) FROM private.lca_active_snapshots WHERE scope=%s", (scope,)
            )
            active_residue = cursor.fetchone()[0]
            cursor.execute(
                "SELECT count(*) FROM private.lca_network_snapshots WHERE id=ANY(%s::uuid[])",
                (snapshot_ids + [failed_id],),
            )
            network_residue = cursor.fetchone()[0]
            cursor.execute(
                "SELECT count(*) FROM private.lca_snapshot_artifacts WHERE snapshot_id=ANY(%s::uuid[])",
                (snapshot_ids + [failed_id],),
            )
            artifact_residue = cursor.fetchone()[0]
        residue = {
            "roles": role_residue,
            "activeRows": active_residue,
            "networkRows": network_residue,
            "artifactRows": artifact_residue,
        }
        require(all(value == 0 for value in residue.values()), f"qualification residue remains: {residue}")
        result["residue"] = residue
    return result


def worker_staged_identity() -> dict[str, str]:
    unstaged = subprocess.run(["git", "diff", "--quiet"], cwd=ROOT, check=False)
    require(unstaged.returncode == 0, "live evidence requires a frozen tree with no unstaged changes")
    untracked = command_output(["git", "ls-files", "--others", "--exclude-standard"])
    require(not untracked, f"live evidence rejects untracked files:\n{untracked}")
    staged_patch = subprocess.run(
        ["git", "diff", "--cached", "--binary"], cwd=ROOT, check=True, capture_output=True
    ).stdout
    require(staged_patch, "live evidence requires a non-empty staged Worker change")
    frozen_status = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    return {
        "headSha": command_output(["git", "rev-parse", "HEAD"]),
        "stagedTreeSha": command_output(["git", "write-tree"]),
        "stagedPatchSha256": hashlib.sha256(staged_patch).hexdigest(),
        "frozenStatusSha256": hashlib.sha256(frozen_status).hexdigest(),
        "untrackedFiles": "0",
    }


def parse_marker(log: str, marker: str) -> dict[str, object]:
    prefix = f"[{marker}] "
    matches = [line[len(prefix) :] for line in log.splitlines() if line.startswith(prefix)]
    require(len(matches) == 1, f"expected exactly one {marker} evidence marker, got {len(matches)}")
    parsed = json.loads(matches[0])
    require(parsed.get("status") == "succeeded", f"{marker} did not succeed: {parsed}")
    return parsed


def database_cardinalities(database_url: str) -> dict[str, int]:
    result: dict[str, int] = {}
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=ANY(%s) AND c.relkind IN ('r','p')
                ORDER BY n.nspname, c.relname
                """,
                (["auth", "private", "public", "storage"],),
            )
            relations = cursor.fetchall()
            for schema, relation in relations:
                cursor.execute(
                    sql.SQL("SELECT count(*) FROM {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(relation)
                    )
                )
                result[f"{schema}.{relation}"] = cursor.fetchone()[0]
    return result


def critical_database_state(database_url: str) -> dict[str, dict[str, object]]:
    """Content-level state for Auth sessions/users, packages, and snapshot fixtures."""
    result: dict[str, dict[str, object]] = {}
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE c.relkind IN ('r','p') AND (
                  (n.nspname='auth' AND c.relname IN ('users','sessions')) OR
                  (n.nspname IN ('public','private') AND c.relname LIKE '%%package%%') OR
                  (n.nspname='private' AND c.relname=ANY(%s))
                )
                ORDER BY n.nspname, c.relname
                """,
                (list(RELATIONS),),
            )
            relations = cursor.fetchall()
            for schema, relation in relations:
                cursor.execute(
                    sql.SQL("SELECT to_jsonb(t)::text FROM {}.{} t ORDER BY to_jsonb(t)::text").format(
                        sql.Identifier(schema), sql.Identifier(relation)
                    )
                )
                rows = [row[0] for row in cursor.fetchall()]
                digest = hashlib.sha256(("\n".join(rows) + ("\n" if rows else "")).encode()).hexdigest()
                result[f"{schema}.{relation}"] = {"rows": len(rows), "sha256": digest}
    require(set(result) == CRITICAL_RELATIONS, f"critical relation inventory drifted: {sorted(result)}")
    return result


def s3_environment(storage_environment: dict[str, str]) -> dict[str, str]:
    return {**os.environ, **storage_environment}


def list_s3_keys(
    endpoint: str, bucket: str, storage_environment: dict[str, str]
) -> list[str] | None:
    command = [
        "aws",
        "--endpoint-url",
        endpoint,
        "s3api",
        "list-objects-v2",
        "--bucket",
        bucket,
        "--output",
        "json",
    ]
    completed = subprocess.run(
        command, env=s3_environment(storage_environment), text=True, capture_output=True, check=False
    )
    if completed.returncode != 0:
        if "NoSuchBucket" in completed.stderr or "not exist" in completed.stderr:
            return None
        raise AssertionError(f"S3 list failed: {completed.stderr}")
    report = json.loads(completed.stdout)
    return [item["Key"] for item in report.get("Contents", [])]


def delete_s3_object(
    endpoint: str, bucket: str, key: str, storage_environment: dict[str, str]
) -> None:
    completed = subprocess.run(
        [
            "aws",
            "--endpoint-url",
            endpoint,
            "s3api",
            "delete-object",
            "--bucket",
            bucket,
            "--key",
            key,
        ],
        env=s3_environment(storage_environment),
        text=True,
        capture_output=True,
        check=False,
    )
    require(completed.returncode == 0, f"S3 delete failed for {key}: {completed.stderr}")


def create_s3_bucket(
    endpoint: str, bucket: str, storage_environment: dict[str, str]
) -> None:
    completed = subprocess.run(
        [
            "aws",
            "--endpoint-url",
            endpoint,
            "s3api",
            "create-bucket",
            "--bucket",
            bucket,
        ],
        env=s3_environment(storage_environment),
        text=True,
        capture_output=True,
        check=False,
    )
    require(completed.returncode == 0, f"S3 bucket create failed: {completed.stderr}")


def put_s3_object(
    endpoint: str,
    bucket: str,
    key: str,
    payload: bytes,
    storage_environment: dict[str, str],
) -> None:
    with tempfile.TemporaryDirectory(prefix="worker199-s3-put-") as directory:
        body = Path(directory) / "body.bin"
        body.write_bytes(payload)
        completed = subprocess.run(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--body",
                str(body),
            ],
            env=s3_environment(storage_environment),
            text=True,
            capture_output=True,
            check=False,
        )
    require(completed.returncode == 0, f"S3 put failed for {key}: {completed.stderr}")


def read_s3_object(
    endpoint: str, bucket: str, key: str, storage_environment: dict[str, str]
) -> bytes:
    with tempfile.TemporaryDirectory(prefix="worker199-s3-get-") as directory:
        output = Path(directory) / "body.bin"
        completed = subprocess.run(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3api",
                "get-object",
                "--bucket",
                bucket,
                "--key",
                key,
                str(output),
            ],
            env=s3_environment(storage_environment),
            text=True,
            capture_output=True,
            check=False,
        )
        require(completed.returncode == 0, f"S3 get failed for {key}: {completed.stderr}")
        return output.read_bytes()


def delete_exact_s3_prefix(
    endpoint: str,
    bucket: str,
    prefix: str,
    storage_environment: dict[str, str],
) -> dict[str, object]:
    """Delete only exact-prefix objects; foreign keys are evidence, never targets."""
    keys = list_s3_keys(endpoint, bucket, storage_environment) or []
    owned_keys, foreign_keys = partition_s3_keys(keys, prefix)
    errors: list[str] = []
    for key in owned_keys:
        try:
            delete_s3_object(endpoint, bucket, key, storage_environment)
        except Exception as error:
            errors.append(str(error))
    remaining_keys = list_s3_keys(endpoint, bucket, storage_environment) or []
    remaining_owned_keys, remaining_foreign_keys = partition_s3_keys(remaining_keys, prefix)
    return {
        "ownedKeys": owned_keys,
        "foreignKeys": foreign_keys,
        "remainingOwnedKeys": remaining_owned_keys,
        "remainingForeignKeys": remaining_foreign_keys,
        "errors": errors,
    }


def cleanup_lifecycle_run(
    stack: RunnerOwnedSupabaseStack,
    bucket: str,
    prefix: str,
    baseline: dict[str, int],
    critical_baseline: dict[str, dict[str, object]],
    owned_sentinel_key: str,
    owned_payload: bytes,
    sibling_key: str,
    sibling_payload: bytes,
) -> dict[str, object]:
    cleanup_errors: list[str] = []
    stack.assert_owned_running()
    try:
        storage_cleanup = delete_exact_s3_prefix(
            stack.storage_endpoint, bucket, prefix, stack.storage_environment
        )
    except Exception as error:
        storage_cleanup = {
            "ownedKeys": [],
            "foreignKeys": [],
            "remainingOwnedKeys": [],
            "remainingForeignKeys": [],
            "errors": [f"storage cleanup/readback: {error}"],
        }
    cleanup_errors.extend(str(error) for error in storage_cleanup["errors"])
    if storage_cleanup["foreignKeys"] != [sibling_key]:
        cleanup_errors.append(
            "lifecycle bucket foreign-key set was not the exact runner-owned sibling: "
            f"{storage_cleanup['foreignKeys']}"
        )
    if storage_cleanup["remainingForeignKeys"] != [sibling_key]:
        cleanup_errors.append(
            "exact-prefix cleanup did not retain only the runner sibling object: "
            f"{storage_cleanup['remainingForeignKeys']}"
        )
    if storage_cleanup["remainingOwnedKeys"]:
        cleanup_errors.append(
            f"lifecycle Storage prefix residue remains: {storage_cleanup['remainingOwnedKeys']}"
        )
    if owned_sentinel_key not in storage_cleanup["ownedKeys"]:
        cleanup_errors.append("runner-owned sentinel was absent from exact-prefix deletion input")
    try:
        sibling_readback = read_s3_object(
            stack.storage_endpoint, bucket, sibling_key, stack.storage_environment
        )
        require(sibling_readback == sibling_payload, "sibling object bytes changed during prefix cleanup")
        sibling_sha256 = hashlib.sha256(sibling_readback).hexdigest()
        delete_s3_object(
            stack.storage_endpoint, bucket, sibling_key, stack.storage_environment
        )
        final_keys = list_s3_keys(
            stack.storage_endpoint, bucket, stack.storage_environment
        ) or []
        require(sibling_key not in final_keys, "sibling object remains after exact sibling cleanup")
    except Exception as error:
        sibling_sha256 = ""
        cleanup_errors.append(f"sibling retention/cleanup: {error}")
    stack.reset()
    after = database_cardinalities(stack.database_url)
    critical_after = critical_database_state(stack.database_url)
    changed = {
        key: {"before": baseline.get(key), "after": after.get(key)}
        for key in sorted(set(baseline) | set(after))
        if baseline.get(key) != after.get(key)
    }
    if changed:
        cleanup_errors.append(f"lifecycle cardinality residue remains after reset: {changed}")
    critical_changed = {
        key: {"before": critical_baseline.get(key), "after": critical_after.get(key)}
        for key in sorted(set(critical_baseline) | set(critical_after))
        if critical_baseline.get(key) != critical_after.get(key)
    }
    if critical_changed:
        cleanup_errors.append(
            "critical users/sessions/packages/snapshot content changed after reset: "
            f"{critical_changed}"
        )
    require(not cleanup_errors, "lifecycle cleanup failed:\n" + "\n".join(cleanup_errors))
    return {
        "storageObjectsDeleted": len(storage_cleanup["ownedKeys"]),
        "storagePrefixObjectsRemaining": 0,
        "ownedSentinel": {
            "keyRelation": "inside-owned-prefix",
            "bytesWritten": len(owned_payload),
            "sha256Written": hashlib.sha256(owned_payload).hexdigest(),
            "remainingAfterPrefixCleanup": 0,
        },
        "siblingObject": {
            "keyRelation": "sibling-prefix-outside-owned-prefix",
            "bytesPreserved": len(sibling_payload),
            "sha256Before": hashlib.sha256(sibling_payload).hexdigest(),
            "sha256After": sibling_sha256,
            "remainingAfterExactCleanup": 0,
        },
        "databaseCardinalityDrift": 0,
        "cardinalityEvidence": {
            "comparison": "count-only",
            "schemas": ["auth", "private", "public", "storage"],
            "relationsCompared": len(baseline),
            "driftedRelations": 0,
        },
        "criticalRelationHashes": {
            relation: {
                "beforeSha256": critical_baseline[relation]["sha256"],
                "afterSha256": critical_after[relation]["sha256"],
                "beforeRows": critical_baseline[relation]["rows"],
                "afterRows": critical_after[relation]["rows"],
            }
            for relation in sorted(critical_baseline)
        },
        "usersSessionsPackagesSnapshotContentRestoredToBaseline": True,
    }


def execute_lifecycle_test(
    stack: RunnerOwnedSupabaseStack,
    tidas_bin: Path,
    test_command: list[str],
    inject_failure: bool,
) -> tuple[str, dict[str, object]]:
    run_id = str(uuid.uuid4())
    bucket = f"scope-closure-e2e-{run_id}"
    prefix = f"scope-closure-package-v2-e2e/{run_id}"
    stack.assert_owned_running()
    parsed_run_id = uuid.UUID(run_id)
    require(
        parsed_run_id.version == 4 and run_id == str(parsed_run_id),
        "lifecycle run identity is not a lowercase canonical UUID v4",
    )
    require(
        normalized_database_endpoint(stack.database_url)["host"] == "loopback"
        and normalized_http_endpoint(stack.storage_endpoint)["host"] == "loopback",
        "lifecycle endpoint preflight did not resolve to loopback",
    )
    require(
        prefix == f"scope-closure-package-v2-e2e/{run_id}",
        "lifecycle prefix is not bound to the one-time run identity",
    )
    baseline = database_cardinalities(stack.database_url)
    critical_baseline = critical_database_state(stack.database_url)
    require(
        list_s3_keys(stack.storage_endpoint, bucket, stack.storage_environment) is None,
        f"runner-generated lifecycle bucket already exists: {bucket}",
    )
    owned_sentinel_key = f"{prefix}/runner-owned-sentinel.bin"
    sibling_key = f"{prefix}-sibling/runner-sibling-sentinel.bin"
    owned_payload = f"owned:{run_id}".encode()
    sibling_payload = f"sibling:{run_id}".encode()
    environment = os.environ.copy()
    environment.update(stack.storage_environment)
    environment.update(
        {
            "DATABASE_URL": stack.database_url,
            "TIDAS_BIN": str(tidas_bin),
            "SNAPSHOT_BUILDER_BIN": str(ROOT / "target" / "debug" / "snapshot_builder"),
            "SNAPSHOT_REPORT_MODE": "disabled",
            "S3_BUCKET": bucket,
            "S3_PREFIX": prefix,
            "WORKER199_INJECT_FAILURE_AFTER_HDF_RESTORE": "1" if inject_failure else "0",
        }
    )
    test = subprocess.run(
        test_command, cwd=ROOT, env=environment, text=True, capture_output=True, check=False
    )
    test_log = "$ " + " ".join(test_command) + "\n" + test.stdout + test.stderr
    require(
        list_s3_keys(stack.storage_endpoint, bucket, stack.storage_environment) is not None,
        "lifecycle fixture did not create its one-time bucket",
    )
    put_s3_object(
        stack.storage_endpoint,
        bucket,
        owned_sentinel_key,
        owned_payload,
        stack.storage_environment,
    )
    put_s3_object(
        stack.storage_endpoint,
        bucket,
        sibling_key,
        sibling_payload,
        stack.storage_environment,
    )
    cleanup = cleanup_lifecycle_run(
        stack,
        bucket,
        prefix,
        baseline,
        critical_baseline,
        owned_sentinel_key,
        owned_payload,
        sibling_key,
        sibling_payload,
    )
    require(
        "[hdf_restore_evidence] " in test_log,
        "lifecycle did not reach HDF restoration evidence; "
        f"test exit code was {test.returncode}; bounded log tail follows:\n{test_log[-16000:]}",
    )
    hdf_restore = parse_marker(test_log, "hdf_restore_evidence")
    require(
        hdf_restore.get("objectKey", "").startswith(prefix + "/"),
        f"HDF restore evidence escaped the unique prefix: {hdf_restore}",
    )
    if inject_failure:
        require(test.returncode != 0, "injected lifecycle failure unexpectedly passed")
        require(
            "worker199 injected failure after byte-exact HDF restoration" in test_log,
            "injected lifecycle failure did not reach the post-restoration fence",
        )
    else:
        require(test.returncode == 0, "certified lifecycle failed")
        require("1 passed; 0 failed" in test_log, "lifecycle did not execute one passing test")
    return test_log, {
        "runId": run_id,
        "bucket": bucket,
        "prefix": prefix,
        "expectedOutcome": "injected-failure" if inject_failure else "success",
        "exitCode": test.returncode,
        "hdfRestore": hdf_restore,
        "cleanup": cleanup,
    }


def run_tidas_lifecycle(
    stack: RunnerOwnedSupabaseStack,
    tidas_bin: Path,
    evidence_output: Path,
) -> dict[str, object]:
    tidas_bin = tidas_bin.resolve()
    require(tidas_bin.is_file(), f"TIDAS binary not found: {tidas_bin}")
    stack.assert_owned_running()
    version_command = [str(tidas_bin), "version", "--format", "json", "--progress", "never"]
    version_output = command_output(version_command)
    version_report = json.loads(version_output)
    binary_version = version_report.get("summary", {}).get("binary_version")
    require(binary_version == "0.1.3", f"TIDAS binary must be 0.1.3, got {binary_version}")
    build_command = ["cargo", "build", "-p", "solver-worker", "--bin", "snapshot_builder"]
    test_command = [
        "cargo",
        "test",
        "-p",
        "solver-worker",
        "--test",
        "scope_closure_package_v2_e2e",
        "certified_snapshot_lifecycle_is_frozen_reusable_and_fail_closed",
        "--",
        "--ignored",
        "--exact",
        "--nocapture",
    ]
    build = subprocess.run(
        build_command, cwd=ROOT, text=True, capture_output=True, check=False
    )
    require(build.returncode == 0, f"snapshot_builder build failed:\n{build.stdout}\n{build.stderr}")
    failure_log, failure_evidence = execute_lifecycle_test(
        stack,
        tidas_bin,
        test_command,
        True,
    )
    success_log, success_evidence = execute_lifecycle_test(
        stack,
        tidas_bin,
        test_command,
        False,
    )
    combined_log = (
        "$ "
        + " ".join(build_command)
        + "\n"
        + build.stdout
        + build.stderr
        + "\n# injected post-HDF-restoration failure fixture\n"
        + failure_log
        + "\n# successful certified lifecycle\n"
        + success_log
    )
    log_path = evidence_output.with_suffix(evidence_output.suffix + ".lifecycle.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(combined_log, encoding="utf-8")
    preflight = parse_marker(success_log, "tidas_fixture_preflight")
    lifecycle = parse_marker(success_log, "certified_snapshot_lifecycle_evidence")
    require(
        preflight.get("documentCount") == 34 and preflight.get("issueCount") == 0,
        f"TIDAS fixture evidence mismatch: {preflight}",
    )
    return {
        "tidasBinary": str(tidas_bin),
        "tidasBinarySha256": hashlib.sha256(tidas_bin.read_bytes()).hexdigest(),
        "tidasVersionCommand": version_command,
        "tidasVersion": binary_version,
        "commands": [build_command, test_command],
        "environmentVariableNames": [
            "DATABASE_URL",
            "TIDAS_BIN",
            "SNAPSHOT_BUILDER_BIN",
            "SNAPSHOT_REPORT_MODE",
            "AWS credentials from runner-owned Supabase status",
            "S3_BUCKET (runner-generated)",
            "S3_PREFIX (runner-generated)",
        ],
        "preflight": preflight,
        "lifecycle": lifecycle,
        "failureFixture": failure_evidence,
        "successfulRun": success_evidence,
        "testSummary": "1 passed; 0 failed",
        "logPath": str(log_path),
        "logSha256": hashlib.sha256(combined_log.encode()).hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-repo", type=Path)
    parser.add_argument("--run-lifecycle", action="store_true")
    parser.add_argument("--tidas-bin", type=Path)
    parser.add_argument("--evidence-output", type=Path)
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    result: dict[str, object] = {"static": static_consumer_zero()}
    frozen_worker: dict[str, str] | None = None
    if not args.static_only:
        require(args.database_repo is not None, "live qualification requires --database-repo source")
        frozen_worker = worker_staged_identity()
        result["worker"] = frozen_worker
        stack = RunnerOwnedSupabaseStack(args.database_repo)
        with stack:
            result["ephemeralStack"] = stack.assert_owned_running()
            result["database"] = qualify_database(stack)
            if args.run_lifecycle:
                require(args.tidas_bin is not None, "--run-lifecycle requires --tidas-bin")
                require(args.evidence_output is not None, "--run-lifecycle requires --evidence-output")
                result["tidasLifecycle"] = run_tidas_lifecycle(
                    stack, args.tidas_bin, args.evidence_output
                )
        require(stack.destruction is not None, "ephemeral stack destruction evidence is missing")
        result["ephemeralStack"] = {
            **result["ephemeralStack"],
            "destruction": stack.destruction,
        }
    elif args.run_lifecycle:
        raise AssertionError("--run-lifecycle cannot be combined with --static-only")
    if frozen_worker is not None:
        require(
            worker_staged_identity() == frozen_worker,
            "Worker staged identity changed during qualification",
        )
        result["worker"] = {**frozen_worker, "verifiedAfterQualification": True}
    result["generatedAt"] = datetime.datetime.now(datetime.UTC).isoformat()
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.evidence_output is not None:
        args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"snapshot private cutover qualification failed: {error}", file=sys.stderr)
        raise
