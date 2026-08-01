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
    ManifestError,
    SourceFile,
    build_manifest,
    canonical_bytes,
    derive_occurrences,
    validate_manifest_shape,
    verify,
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


class ManifestNegativeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", "-b", "main", self.root], check=True)
        subprocess.run(["git", "-C", self.root, "config", "user.email", "test@example.invalid"], check=True)
        subprocess.run(["git", "-C", self.root, "config", "user.name", "Manifest Test"], check=True)
        source = self.root / "crates/solver-worker/src/example.rs"
        source.parent.mkdir(parents=True)
        source.write_text('sqlx::query("SELECT * FROM public.worker_jobs");\n')
        subprocess.run(["git", "-C", self.root, "add", "."], check=True)
        subprocess.run(["git", "-C", self.root, "commit", "-qm", "fixture"], check=True)
        self.head = subprocess.check_output(["git", "-C", self.root, "rev-parse", "HEAD"], text=True).strip()
        self.manifest = build_manifest(self.root, self.head, self.head)
        contract = self.root / "contracts"
        contract.mkdir()
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
                self.assert_rejected(changed, "sets differ")

    def test_commit_sha_and_schema_drift_rejected(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["headCommit"] = "0" * 40
        self.assert_rejected(changed, "ancestor|git rev-parse")
        changed = copy.deepcopy(self.manifest)
        changed["schema"] = "tiangong.supabase-consumer-manifest.v2"
        self.assert_rejected(changed, "schema/version drift")

    def test_credential_and_schema_mismatch_rejected(self) -> None:
        for field, value in (("credential", "anon"), ("schema", "api")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.manifest)
                changed["occurrences"][0][field] = value
                self.assert_rejected(changed, "sets differ")

    def test_candidate_cannot_claim_authority(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["authority"]["authorizesDatabaseFreeze"] = True
        self.assert_rejected(changed, "candidate and non-authorizing")

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
        subprocess.run(["git", "-C", self.root, "add", "scripts/linked.py"], check=True)
        subprocess.run(["git", "-C", self.root, "commit", "-qm", "symlink"], check=True)
        changed = copy.deepcopy(self.manifest)
        changed["headCommit"] = subprocess.check_output(
            ["git", "-C", self.root, "rev-parse", "HEAD"], text=True
        ).strip()
        self.write(changed)
        with self.assertRaisesRegex(ManifestError, "not a regular git file"):
            verify(self.root, self.path)

    def test_shape_rejects_extra_self_reported_count(self) -> None:
        changed = copy.deepcopy(self.manifest)
        changed["occurrenceCount"] = 1
        with self.assertRaisesRegex(ManifestError, "manifest fields differ"):
            validate_manifest_shape(changed)


if __name__ == "__main__":
    unittest.main()
