#!/usr/bin/env python3
"""Derive and verify the Worker Supabase consumer manifest.

The manifest is evidence about an immutable git tree.  It is not an input to
the derivation.  Verification derives every occurrence from ``headCommit`` and
then requires exact, bidirectional set equality with the checked-in manifest.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


SCHEMA = "tiangong.supabase-consumer-manifest.v3"
REPOSITORY = "linancn/tiangong-lca-worker"
MANIFEST_PATH = "contracts/supabase-consumer-manifest.v3.json"
SOURCE_PATTERNS = (
    "crates/**/*.rs",
    "scripts/*.py",
    "scripts/*.sh",
    "scripts/**/*.py",
    "scripts/**/*.sh",
    "tools/**/*.py",
)
TRANSPORTS = {
    "direct-postgresql",
    "pgmq",
    "postgrest",
    "supabase-cli",
    "realtime",
    "cron",
    "dynamic-sql",
}
OPERATIONS = {
    "select",
    "insert",
    "update",
    "delete",
    "truncate",
    "call",
    "enqueue",
    "archive",
    "listen",
    "schedule",
    "dynamic",
}

RELATION_RE = re.compile(
    r"(?ix)\b(?P<verb>FROM|JOIN|UPDATE|INSERT\s+INTO|DELETE\s+FROM|"
    r"TRUNCATE(?:\s+TABLE)?|LOCK\s+TABLE)\s+(?:ONLY\s+)?"
    r"(?P<name>(?:public|api|private|util|pgmq|cron|realtime|storage)\."
    r"[A-Za-z_][A-Za-z0-9_]*|(?:lca|lcia|worker|dataset|cmd_dataset|"
    r"processes|flows|contacts|sources|unitgroups|flowproperties|"
    r"lciamethods|lifecyclemodels|ilcd)[A-Za-z0-9_]*)"
)
ROUTINE_RE = re.compile(
    r"(?ix)\b(?P<name>(?:public|api|private|util|pgmq|cron|realtime)\."
    r"[A-Za-z_][A-Za-z0-9_]*)\s*\("
)
REGCLASS_RE = re.compile(
    r"(?ix)\bto_reg(?:class|procedure|proc)\s*\(\s*['\"]"
    r"(?P<name>(?:public|api|private|util|pgmq|cron|realtime)\."
    r"[A-Za-z_][A-Za-z0-9_]*)"
)
POSTGREST_RE = re.compile(
    r"(?ix)\.(?P<method>from|table|rpc)\s*\(\s*['\"]"
    r"(?P<object>[A-Za-z_][A-Za-z0-9_]*)['\"]"
)
SCHEMA_PROFILE_RE = re.compile(
    r"(?ix)\.schema\s*\(\s*['\"](?P<schema>[A-Za-z_][A-Za-z0-9_]*)['\"]\s*\)"
)
SUPABASE_CLI_RE = re.compile(r"(?i)\bsupabase\s+(?P<command>start|stop|status|db\s+reset|migration\s+up)\b")
QUERY_CALL_RE = re.compile(
    r"(?x)(?:(?:::)?sqlx::|pgbouncer_sqlx::)"
    r"(?P<name>query(?:_as|_scalar)?|raw_sql)(?:::\s*<[^;{}]*?>)?\s*\("
)


class ManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceFile:
    path: str
    data: bytes


def run_git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        message = result.stderr.decode("utf-8", "replace").strip()
        raise ManifestError(f"git {' '.join(args)} failed: {message}")
    return result.stdout


def canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_path(path: str) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in SOURCE_PATTERNS)


def read_commit_tree(root: Path, commit: str) -> list[SourceFile]:
    resolved = run_git(root, "rev-parse", "--verify", f"{commit}^{{commit}}").decode().strip()
    if resolved != commit:
        raise ManifestError(f"headCommit must be a full exact commit SHA: expected {commit}, resolved {resolved}")
    raw = run_git(root, "ls-tree", "-r", "-z", commit)
    files: list[SourceFile] = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, encoded_path = record.split(b"\t", 1)
        mode, kind, object_id = metadata.decode().split()
        path = encoded_path.decode("utf-8")
        if not source_path(path):
            continue
        if mode != "100644" and mode != "100755":
            raise ManifestError(f"source path is not a regular git file: {path} mode={mode}")
        if kind != "blob":
            raise ManifestError(f"source path is not a blob: {path} type={kind}")
        files.append(SourceFile(path, run_git(root, "cat-file", "blob", object_id)))
    return sorted(files, key=lambda item: item.path)


def validate_local_artifact(path: Path) -> bytes:
    try:
        info = path.lstat()
    except FileNotFoundError as error:
        raise ManifestError(f"manifest does not exist: {path}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ManifestError(f"manifest must be a no-follow regular file: {path}")
    return path.read_bytes()


def split_name(name: str) -> tuple[str, str]:
    if "." in name:
        return tuple(name.split(".", 1))  # type: ignore[return-value]
    return "public", name


def credential_for(path: str, line: str) -> str:
    if "/tests/" in path or path.startswith("scripts/test_"):
        return "isolated-test-role"
    if path.startswith("scripts/") or path.startswith("tools/"):
        return "operator-database-role"
    if "service_role" in line or path.startswith("crates/solver-worker/"):
        return "service_role"
    return "database-connection-role"


def source_class(path: str) -> str:
    if "/tests/" in path or path.startswith("scripts/test_"):
        return "test"
    if path.startswith("scripts/") or path.startswith("tools/"):
        return "operator-tooling"
    return "runtime"


def occurrence(
    *, path: str, line: int, operation: str, transport: str, credential: str,
    schema: str, object_name: str, signature: str, source_kind: str,
) -> dict[str, object]:
    identity = "\0".join(
        [path, str(line), operation, transport, credential, schema, object_name, signature, source_kind]
    )
    return {
        "id": "occ-" + sha256(identity.encode())[:20],
        "file": path,
        "line": line,
        "operation": operation,
        "transport": transport,
        "credential": credential,
        "schema": schema,
        "object": object_name,
        "signature": signature,
        "sourceClass": source_kind,
    }


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def operation_for_relation(verb: str) -> str:
    normalized = " ".join(verb.lower().split())
    return {
        "from": "select", "join": "select", "update": "update",
        "insert into": "insert", "delete from": "delete",
        "truncate": "truncate", "truncate table": "truncate", "lock table": "select",
    }[normalized]


def balanced_first_argument(text: str, opening: int) -> tuple[str, int] | None:
    depth = 1
    index = opening + 1
    start = index
    quote: str | None = None
    raw_hashes = 0
    while index < len(text):
        char = text[index]
        if quote == "raw":
            terminator = '"' + ('#' * raw_hashes)
            if text.startswith(terminator, index):
                index += len(terminator)
                quote = None
                continue
            index += 1
            continue
        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        raw = re.match(r"r(#+)?\"", text[index:])
        if raw:
            raw_hashes = len(raw.group(1) or "")
            quote = "raw"
            index += len(raw.group(0))
            continue
        if char in "\"'":
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth == 0:
                return text[start:index].strip(), index
        elif char == "," and depth == 1:
            return text[start:index].strip(), index
        index += 1
    return None


def is_direct_literal(expression: str) -> bool:
    value = expression.lstrip("& ")
    return bool(re.match(r'r#*"', value) or value.startswith('"'))


def normalized_expression(expression: str) -> str:
    return re.sub(r"\s+", " ", expression).strip()[:500]


def derive_occurrences(files: Iterable[SourceFile]) -> list[dict[str, object]]:
    found: dict[tuple[object, ...], dict[str, object]] = {}
    for source in files:
        try:
            text = source.data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ManifestError(f"source is not UTF-8: {source.path}") from error
        kind = source_class(source.path)
        lines = text.splitlines()

        def add(item: dict[str, object]) -> None:
            key = tuple(item[key] for key in (
                "file", "line", "operation", "transport", "credential",
                "schema", "object", "signature", "sourceClass",
            ))
            found[key] = item

        for match in RELATION_RE.finditer(text):
            name = match.group("name")
            schema, object_name = split_name(name)
            number = line_number(text, match.start("name"))
            line = lines[number - 1] if number <= len(lines) else ""
            operation = operation_for_relation(match.group("verb"))
            transport = schema if schema in {"pgmq", "cron", "realtime"} else "direct-postgresql"
            add(occurrence(
                path=source.path, line=number, operation=operation,
                transport=transport, credential=credential_for(source.path, line),
                schema=schema, object_name=object_name,
                signature=f"relation:{schema}.{object_name}", source_kind=kind,
            ))
        for match in ROUTINE_RE.finditer(text):
            name = match.group("name")
            if name.lower() in {"public.ecr", "public.aws"}:
                continue
            schema, object_name = split_name(name)
            number = line_number(text, match.start("name"))
            line = lines[number - 1] if number <= len(lines) else ""
            lower = object_name.lower()
            operation = "enqueue" if lower in {"send", "send_batch"} or "enqueue" in lower else "call"
            if lower == "archive":
                operation = "archive"
            elif schema == "cron" and lower == "schedule":
                operation = "schedule"
            elif schema == "realtime":
                operation = "listen"
            transport = "pgmq" if schema == "pgmq" else "direct-postgresql"
            add(occurrence(
                path=source.path, line=number, operation=operation, transport=transport,
                credential=credential_for(source.path, line), schema=schema,
                object_name=object_name, signature=f"routine:{schema}.{object_name}(consumer-arguments)",
                source_kind=kind,
            ))
        for match in REGCLASS_RE.finditer(text):
            schema, object_name = split_name(match.group("name"))
            number = line_number(text, match.start("name"))
            line = lines[number - 1] if number <= len(lines) else ""
            add(occurrence(
                path=source.path, line=number, operation="select", transport="direct-postgresql",
                credential=credential_for(source.path, line), schema=schema,
                object_name=object_name, signature=f"catalog-lookup:{schema}.{object_name}",
                source_kind=kind,
            ))
        for match in POSTGREST_RE.finditer(text):
            number = line_number(text, match.start())
            prefix = text[max(0, match.start() - 300):match.start()]
            schemas = list(SCHEMA_PROFILE_RE.finditer(prefix))
            schema = schemas[-1].group("schema") if schemas else "public"
            method = match.group("method").lower()
            operation = "call" if method == "rpc" else "select"
            object_name = match.group("object")
            add(occurrence(
                path=source.path, line=number, operation=operation, transport="postgrest",
                credential=credential_for(source.path, lines[number - 1]), schema=schema,
                object_name=object_name,
                signature=f"{'routine' if method == 'rpc' else 'relation'}:{schema}.{object_name}",
                source_kind=kind,
            ))
        for match in SUPABASE_CLI_RE.finditer(text):
            number = line_number(text, match.start())
            command = " ".join(match.group("command").lower().split())
            add(occurrence(
                path=source.path, line=number, operation="call", transport="supabase-cli",
                credential="local-supabase-cli", schema="platform", object_name=command,
                signature=f"supabase-cli:{command}", source_kind=kind,
            ))
        if source.path.endswith(".rs"):
            for match in QUERY_CALL_RE.finditer(text):
                parsed = balanced_first_argument(text, match.end() - 1)
                if not parsed:
                    raise ManifestError(f"unbalanced SQL query call: {source.path}:{line_number(text, match.start())}")
                expression, _ = parsed
                if not expression or is_direct_literal(expression):
                    continue
                number = line_number(text, match.start())
                normalized = normalized_expression(expression)
                add(occurrence(
                    path=source.path, line=number, operation="dynamic", transport="dynamic-sql",
                    credential=credential_for(source.path, lines[number - 1]), schema="dynamic",
                    object_name=normalized,
                    signature="dynamic-sql:sha256:" + sha256(normalized.encode()), source_kind=kind,
                ))
    result = list(found.values())
    result.sort(key=lambda item: (
        item["file"], item["line"], item["operation"], item["transport"],
        item["schema"], item["object"], item["signature"],
    ))
    return result


def validate_occurrence(item: object) -> None:
    if not isinstance(item, dict):
        raise ManifestError("each occurrence must be an object")
    required = {
        "id", "file", "line", "operation", "transport", "credential",
        "schema", "object", "signature", "sourceClass",
    }
    if set(item) != required:
        raise ManifestError(f"occurrence fields differ: {sorted(set(item) ^ required)}")
    for field in required - {"line"}:
        if not isinstance(item[field], str) or not item[field]:
            raise ManifestError(f"occurrence {field} must be a non-empty string")
    if not isinstance(item["line"], int) or item["line"] < 1:
        raise ManifestError("occurrence line must be a positive integer")
    path = PurePosixPath(item["file"])
    if path.is_absolute() or ".." in path.parts:
        raise ManifestError(f"unsafe occurrence path: {item['file']}")
    if item["operation"] not in OPERATIONS:
        raise ManifestError(f"unsupported operation: {item['operation']}")
    if item["transport"] not in TRANSPORTS:
        raise ManifestError(f"unsupported transport: {item['transport']}")


def validate_manifest_shape(manifest: object) -> dict[str, object]:
    if not isinstance(manifest, dict):
        raise ManifestError("manifest root must be an object")
    required = {
        "schema", "version", "repository", "baseCommit", "headCommit",
        "authority", "source", "occurrences",
    }
    if set(manifest) != required:
        raise ManifestError(f"manifest fields differ: {sorted(set(manifest) ^ required)}")
    if manifest["schema"] != SCHEMA or manifest["version"] != 3:
        raise ManifestError("manifest schema/version drift")
    if manifest["repository"] != REPOSITORY:
        raise ManifestError("manifest repository drift")
    for field in ("baseCommit", "headCommit"):
        if not isinstance(manifest[field], str) or not re.fullmatch(r"[0-9a-f]{40}", manifest[field]):
            raise ManifestError(f"{field} must be a full lowercase commit SHA")
    if manifest["authority"] != {
        "status": "candidate", "authorizesDatabaseFreeze": False,
        "authorizesHostedMutation": False,
    }:
        raise ManifestError("manifest must remain candidate and non-authorizing")
    expected_source = {
        "derivation": "git-tree-independent-v3",
        "pathPatterns": list(SOURCE_PATTERNS),
        "symlinkPolicy": "reject",
        "nonRegularFilePolicy": "reject",
        "setEquality": "bidirectional-exact",
    }
    if manifest["source"] != expected_source:
        raise ManifestError("source derivation contract drift")
    occurrences = manifest["occurrences"]
    if not isinstance(occurrences, list):
        raise ManifestError("occurrences must be an array")
    for item in occurrences:
        validate_occurrence(item)
    return manifest


def comparable(item: dict[str, object]) -> bytes:
    return canonical_bytes(item)


def verify(root: Path, manifest_path: Path) -> dict[str, object]:
    raw = validate_local_artifact(manifest_path)
    try:
        manifest = validate_manifest_shape(json.loads(raw))
    except json.JSONDecodeError as error:
        raise ManifestError(f"manifest is not valid JSON: {error}") from error
    if raw != canonical_bytes(manifest):
        raise ManifestError("manifest bytes are not canonical JSON")
    base = str(manifest["baseCommit"])
    head = str(manifest["headCommit"])
    ancestor = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", base, head],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
    )
    if ancestor.returncode != 0:
        raise ManifestError("baseCommit is not an ancestor of headCommit")
    derived = derive_occurrences(read_commit_tree(root, head))
    declared = manifest["occurrences"]
    assert isinstance(declared, list)
    derived_set = {comparable(item): item for item in derived}
    declared_set = {comparable(item): item for item in declared}
    if len(declared_set) != len(declared):
        raise ManifestError("manifest contains duplicate occurrences")
    missing = sorted(set(derived_set) - set(declared_set))
    forged = sorted(set(declared_set) - set(derived_set))
    if missing or forged:
        details = {
            "missingFromManifest": [derived_set[key] for key in missing[:10]],
            "notDerivedFromSource": [declared_set[key] for key in forged[:10]],
        }
        raise ManifestError("source/manifest occurrence sets differ: " + json.dumps(details, sort_keys=True))
    return {
        "schema": SCHEMA,
        "repository": REPOSITORY,
        "baseCommit": base,
        "headCommit": head,
        "manifestSha256": sha256(raw),
        "occurrenceCountDerived": len(derived),
        "setEquality": True,
        "authority": manifest["authority"],
    }


def build_manifest(root: Path, base: str, head: str) -> dict[str, object]:
    base_sha = run_git(root, "rev-parse", "--verify", f"{base}^{{commit}}").decode().strip()
    head_sha = run_git(root, "rev-parse", "--verify", f"{head}^{{commit}}").decode().strip()
    return {
        "schema": SCHEMA,
        "version": 3,
        "repository": REPOSITORY,
        "baseCommit": base_sha,
        "headCommit": head_sha,
        "authority": {
            "status": "candidate",
            "authorizesDatabaseFreeze": False,
            "authorizesHostedMutation": False,
        },
        "source": {
            "derivation": "git-tree-independent-v3",
            "pathPatterns": list(SOURCE_PATTERNS),
            "symlinkPolicy": "reject",
            "nonRegularFilePolicy": "reject",
            "setEquality": "bidirectional-exact",
        },
        "occurrences": derive_occurrences(read_commit_tree(root, head_sha)),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--base", default="HEAD")
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args()
    root = args.root.resolve()
    manifest_path = args.manifest or root / MANIFEST_PATH
    try:
        if args.generate:
            value = build_manifest(root, args.base, args.head)
            generated = canonical_bytes(value)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_bytes(generated)
            else:
                sys.stdout.buffer.write(generated)
        else:
            print(json.dumps(verify(root, manifest_path), sort_keys=True))
    except ManifestError as error:
        print(f"consumer manifest check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
