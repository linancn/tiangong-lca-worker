#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from check_supabase_consumer_manifest import (
    CANONICAL_REMOTE_URL,
    SCHEMA_PATH,
    ManifestError,
    SourceFile,
    build_manifest,
    canonical_bytes,
    derive_occurrences,
    validate_manifest_shape,
    verify,
)


def clean_git_environment() -> dict[str, str]:
    """Drop repository-local variables exported to Git hooks."""
    return {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}


def fixture_git(root: Path, *args: str, text: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", f"--git-dir={root / '.git'}", f"--work-tree={root}", *args],
        check=True,
        stdout=subprocess.PIPE,
        text=text,
        env=clean_git_environment(),
    )


class DerivationTests(unittest.TestCase):
    def test_derives_relation_rpc_pgmq_postgrest_and_dynamic_helpers(self) -> None:
        files = [SourceFile("crates/solver-worker/src/example.rs", b'''\
sqlx::query("SELECT * FROM public.worker_jobs");
sqlx::query("SELECT public.worker_claim_jobs($1)");
sqlx::query("SELECT pgmq.send($1, $2)");
client.schema("api").rpc("worker_claim_jobs_v1", body);
sqlx::query(dynamic_sql.as_str());
''')]
        entries = derive_occurrences(files)
        identities = {(item["transport"], item["schema"], item["object"]) for item in entries}
        self.assertIn(("direct-postgresql", "public", "worker_jobs"), identities)
        self.assertIn(("direct-postgresql", "public", "worker_claim_jobs"), identities)
        self.assertIn(("pgmq", "pgmq", "send"), identities)
        self.assertIn(("postgrest", "api", "worker_claim_jobs_v1"), identities)
        self.assertTrue(any(item["operation"] == "dynamic" for item in entries))

    def test_arbitrary_dynamic_query_is_not_a_bypass(self) -> None:
        entries = derive_occurrences([
            SourceFile("crates/solver-worker/src/bypass.rs", b"sqlx::query(user_supplied_sql);\n")
        ])
        self.assertEqual(1, len(entries))
        self.assertEqual("dynamic-sql", entries[0]["transport"])
        self.assertEqual("dynamic", entries[0]["schema"])

    def test_query_with_executor_and_qualified_variants_are_dynamic_findings(self) -> None:
        files = [SourceFile("crates/solver-worker/src/variants.rs", b'''\
::sqlx::query_with(user_sql, args);
sqlx::query_as_with::<_, Row, _>(qualified_sql, args);
Executor::execute(executor_sql);
pool.execute(pool_sql);
''')]
        entries = derive_occurrences(files)
        dynamic = [item for item in entries if item["transport"] == "dynamic-sql"]
        self.assertEqual(4, len(dynamic))
        self.assertTrue(all(item["semantics"] == "dynamic-review-required" for item in dynamic))
        self.assertTrue(all(item["sourceTextSha256"] for item in dynamic))


class ManifestNegativeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        subprocess.run(
            ["git", "init", "-q", "-b", "main", str(self.root)],
            check=True,
            cwd="/",
            env=clean_git_environment(),
        )
        fixture_git(self.root, "config", "core.hooksPath", "/dev/null")
        fixture_git(self.root, "config", "user.email", "test@example.invalid")
        fixture_git(self.root, "config", "user.name", "Manifest Test")
        source = self.root / "crates/solver-worker/src/example.rs"
        source.parent.mkdir(parents=True)
        source.write_text('sqlx::query("SELECT * FROM public.worker_jobs");\n')
        scripts = self.root / "scripts"
        scripts.mkdir()
        (scripts / "existing.py").write_text("# governed Python consumer source\n")
        (scripts / "existing.sh").write_text("# governed shell consumer source\n")
        fixture_git(self.root, "add", ".")
        fixture_git(self.root, "commit", "-qm", "fixture")
        self.head = fixture_git(self.root, "rev-parse", "HEAD", text=True).stdout.strip()
        fixture_git(self.root, "remote", "add", "origin", CANONICAL_REMOTE_URL)
        fixture_git(self.root, "update-ref", "refs/remotes/origin/main", self.head)
        contract = self.root / "contracts"
        contract.mkdir()
        canonical_schema = Path(__file__).resolve().parents[1] / SCHEMA_PATH
        (self.root / SCHEMA_PATH).write_bytes(canonical_schema.read_bytes())
        self.manifest = build_manifest(self.root, self.head, self.head)
        self.path = contract / "supabase-consumer-manifest.v3.json"
        self.write(self.manifest)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write(self, value: object) -> None:
        self.path.write_bytes(canonical_bytes(value))

    def assert_rejected(self, value: object, phrase: str) -> None:
        self.write(value)
        with self.assertRaisesRegex(ManifestError, phrase):
            verify(self.root, self.path)

    def test_missing_occurrence_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["occurrences"] = []
        self.assert_rejected(changed, "sets differ")

    def test_forged_path_line_and_operation_rejected(self) -> None:
        for field, value in (("file", "crates/fake.rs"), ("line", 99), ("operation", "delete")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.manifest)
                changed["occurrences"][0][field] = value
                self.assert_rejected(changed, "canonical source patterns|span.start.line|semantics drift|sets differ")

    def test_commit_sha_and_schema_drift_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["headCommit"] = "0" * 40
        changed["origin"]["sourceTreeCommit"] = "0" * 40
        self.assert_rejected(changed, "ancestor|git rev-parse")
        changed = copy.deepcopy(self.manifest)
        changed["schema"] = "tiangong.supabase-consumer-manifest.v2"
        self.assert_rejected(changed, "schema/version drift")

    def test_credential_and_schema_mismatch_rejected(self) -> None:
        for field, value in (("credential", "anon"), ("schema", "api")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.manifest)
                changed["occurrences"][0][field] = value
                self.assert_rejected(changed, "ACL drift|upstream drift|sets differ")

    def test_span_and_source_text_hash_drift_are_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["occurrences"][0]["sourceTextSha256"] = "0" * 64
        self.assert_rejected(changed, "sets differ")
        changed = copy.deepcopy(self.manifest)
        changed["occurrences"][0]["span"]["start"]["column"] += 1
        self.assert_rejected(changed, "sets differ")

    def test_candidate_cannot_claim_authority(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["authority"]["authorizesDatabaseFreeze"] = True
        self.assert_rejected(changed, "authority flags must remain false")

    def test_canonical_schema_byte_tamper_is_rejected(self) -> None:
        schema_path = self.root / SCHEMA_PATH
        schema_path.write_bytes(schema_path.read_bytes() + b" ")
        with self.assertRaisesRegex(ManifestError, "canonical schema bytes|schema SHA-256"):
            verify(self.root, self.path)

    def test_canonical_schema_draft_drift_is_rejected(self) -> None:
        schema_path = self.root / SCHEMA_PATH
        schema = json.loads(schema_path.read_bytes())
        schema["$schema"] = "http://json-schema.org/draft-07/schema#"
        schema_path.write_bytes(canonical_bytes(schema))
        changed = copy.deepcopy(self.manifest)
        import hashlib
        changed["schemaSha256"] = hashlib.sha256(schema_path.read_bytes()).hexdigest()
        self.write(changed)
        with self.assertRaisesRegex(ManifestError, "Draft 2020-12"):
            verify(self.root, self.path)

    def test_noncanonical_origin_is_rejected(self) -> None:
        fixture_git(self.root, "remote", "set-url", "origin", "https://github.com/example/fork.git")
        with self.assertRaisesRegex(ManifestError, "origin remote is not the canonical repository"):
            verify(self.root, self.path)

    def test_governed_source_tree_digest_tamper_is_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["source"]["governedSourceTreeSha256"] = "0" * 64
        self.assert_rejected(changed, "digest does not match headCommit/sourceTreeCommit")

    def test_absence_proof_distinguishes_zero_findings_from_uncovered(self) -> None:
        proof = {item["surface"]: item for item in self.manifest["absenceProof"]}
        self.assertEqual("covered-no-findings", proof["postgrest"]["scannerStatus"])
        self.assertEqual("not-covered", proof["webhook"]["scannerStatus"])
        self.assertTrue(any(item["kind"] == "scanner-coverage:webhook" for item in self.manifest["pending"]))

    def test_residue_pending_and_absence_proof_tamper_are_rejected(self) -> None:
        for field in ("residue", "pending", "absenceProof"):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.manifest)
                if field == "residue":
                    changed[field] = [{
                        "kind": "dynamic-sql-upstream",
                        "occurrenceId": changed["occurrences"][0]["id"],
                        "disposition": "pending-independent-review",
                    }]
                else:
                    changed[field] = []
                self.assert_rejected(changed, field)

    def test_symlink_manifest_rejected_without_following(self) -> None:
        target = self.root / "target.json"
        target.write_bytes(canonical_bytes(self.manifest))
        self.path.unlink()
        os.symlink(target, self.path)
        with self.assertRaisesRegex(ManifestError, "no-follow regular file"):
            verify(self.root, self.path)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO requires POSIX")
    def test_non_regular_manifest_rejected_without_opening(self) -> None:
        self.path.unlink()
        os.mkfifo(self.path)
        with self.assertRaisesRegex(ManifestError, "no-follow regular file"):
            verify(self.root, self.path)

    def test_symlink_source_in_commit_rejected(self) -> None:
        link = self.root / "scripts/linked.py"
        link.parent.mkdir(exist_ok=True)
        os.symlink("../crates/solver-worker/src/example.rs", link)
        fixture_git(self.root, "add", "scripts/linked.py")
        fixture_git(self.root, "commit", "-qm", "symlink")
        symlink_head = fixture_git(self.root, "rev-parse", "HEAD", text=True).stdout.strip()
        changed = copy.deepcopy(self.manifest)
        changed["headCommit"] = symlink_head
        changed["origin"]["sourceTreeCommit"] = symlink_head
        fixture_git(self.root, "update-ref", "refs/remotes/origin/main", symlink_head)
        self.write(changed)
        with self.assertRaisesRegex(ManifestError, "not a regular git file"):
            verify(self.root, self.path)

    def test_shape_rejects_extra_self_reported_count(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["occurrenceCount"] = 1
        with self.assertRaisesRegex(ManifestError, "manifest fields differ"):
            validate_manifest_shape(changed)

    def test_delivery_guard_allows_only_exact_audit_tool_paths(self) -> None:
        checker = self.root / "scripts/check_supabase_consumer_manifest.py"
        checker.write_text("# verifier delivery change\n")
        test_script = self.root / "scripts/test_supabase_consumer_manifest.py"
        test_script.write_text("# verifier test delivery change\n")
        fixture_git(self.root, "add", checker.relative_to(self.root), test_script.relative_to(self.root))
        fixture_git(self.root, "commit", "-qm", "deliver audit tools")
        result = verify(self.root, self.path)
        self.assertEqual(self.head, result["sourceTreeCommit"])
        self.assertNotEqual(self.head, result["deliveryHead"])

    def test_delivery_guard_rejects_changed_rust_python_and_shell_bytes(self) -> None:
        (self.root / "crates/solver-worker/src/example.rs").write_text(
            'sqlx::query("SELECT * FROM public.worker_jobs");\n// changed\n'
        )
        (self.root / "scripts/existing.py").write_text("# changed Python consumer source\n")
        (self.root / "scripts/existing.sh").write_text("# changed shell consumer source\n")
        fixture_git(self.root, "add", "crates", "scripts")
        fixture_git(self.root, "commit", "-qm", "change governed source bytes")
        with self.assertRaisesRegex(ManifestError, "consumer-governed source bytes drifted"):
            verify(self.root, self.path)

    def test_delivery_guard_rejects_add_delete_and_rename(self) -> None:
        added = self.root / "crates/solver-worker/src/added.rs"
        added.write_text('sqlx::query("SELECT * FROM public.added_consumer");\n')
        (self.root / "scripts/existing.py").unlink()
        (self.root / "scripts/existing.sh").rename(self.root / "scripts/renamed.sh")
        fixture_git(self.root, "add", "-A", "crates", "scripts")
        fixture_git(self.root, "commit", "-qm", "add delete and rename governed source")
        with self.assertRaisesRegex(ManifestError, "consumer-governed source bytes drifted"):
            verify(self.root, self.path)

    def test_delivery_guard_rejects_new_dynamic_call(self) -> None:
        source = self.root / "crates/solver-worker/src/example.rs"
        source.write_text(source.read_text() + "sqlx::query(user_supplied_sql);\n")
        fixture_git(self.root, "add", source.relative_to(self.root))
        fixture_git(self.root, "commit", "-qm", "add dynamic SQL consumer")
        with self.assertRaisesRegex(ManifestError, "consumer-governed source bytes drifted"):
            verify(self.root, self.path)

    def test_delivery_guard_rejects_nonancestor_source_snapshot(self) -> None:
        audit_tool = self.root / "scripts/check_supabase_consumer_manifest.py"
        audit_tool.write_text("# child audit tool\n")
        fixture_git(self.root, "add", audit_tool.relative_to(self.root))
        fixture_git(self.root, "commit", "-qm", "child snapshot")
        child = fixture_git(self.root, "rev-parse", "HEAD", text=True).stdout.strip()
        changed = build_manifest(self.root, self.head, child)
        fixture_git(self.root, "update-ref", "refs/remotes/origin/main", child)
        fixture_git(self.root, "checkout", "-q", "--detach", self.head)
        self.write(changed)
        with self.assertRaisesRegex(ManifestError, "not an ancestor of delivery HEAD"):
            verify(self.root, self.path)

    def test_verify_ignores_outer_hook_repository_environment(self) -> None:
        original = {key: os.environ.get(key) for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR")}
        try:
            os.environ["GIT_DIR"] = "/must/not/be/used.git"
            os.environ["GIT_WORK_TREE"] = "/must/not/be/used"
            os.environ["GIT_COMMON_DIR"] = "/must/not/be/used-common"
            self.assertTrue(verify(self.root, self.path)["setEquality"])
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_fixture_git_ignores_outer_hook_repository_environment(self) -> None:
        original = {key: os.environ.get(key) for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR")}
        try:
            os.environ["GIT_DIR"] = "/must/not/be/used.git"
            os.environ["GIT_WORK_TREE"] = "/must/not/be/used"
            os.environ["GIT_COMMON_DIR"] = "/must/not/be/used-common"
            resolved = fixture_git(self.root, "rev-parse", "--absolute-git-dir", text=True).stdout.strip()
            self.assertEqual((self.root / ".git").resolve(), Path(resolved).resolve())
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
