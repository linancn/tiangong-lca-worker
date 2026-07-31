from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import scope_closure_qualification as qualification


SHA = "1" * 40
SHA256 = "2" * 64


def capacity(mode: str) -> dict:
    return {
        "schemaVersion": qualification.CAPACITY_SCHEMA,
        "inputMode": "real-payload",
        "realPackageEvidence": True,
        "cacheScenario": {"mode": mode},
        "realPackageCollection": {
            "includedDocuments": 2,
            "malformedDocuments": 0,
            "recordSizeDistribution": {
                "p50": 10,
                "p95": 20,
                "p99": 20,
                "max": 20,
                "maxIdentity": "processes:00000000-0000-4000-8000-000000000001:01.00.000",
            },
        },
        "documentCount": 2,
        "inputEventCount": 3,
        "inputSpoolBytes": 40,
        "inputSpoolSha256": SHA256,
        "issueCount": 3,
        "occurrenceCount": 3,
        "affectedRootCount": 1,
        "administrativeRecordSizes": [
            {"relation": "documents", "recordCount": 2},
            {"relation": "edges", "recordCount": 0},
            {"relation": "roots", "recordCount": 1},
        ],
        "artifacts": [
            {
                "fileName": "closure-issues.jsonl.zst",
                "byteSize": 17,
                "checksumSha256": SHA256,
            }
        ],
    }


def provider_evidence() -> dict:
    return {
        "descriptors": {
            "count": 1500,
            "objects": 1500,
            "bytes": 10,
            "batch596": True,
            "batch1500OrMore": True,
            "maximumScaleCase": 1500,
            "retryIdempotencyPassed": True,
            "staleFenceRejected": True,
        },
        "storage": {
            "provider": "local-s3-compatible",
            "bucketClass": "non-production-private",
            "objectCount": 1500,
            "bytes": 10,
            "largestObjectBytes": 10,
            "multipartBoundaryPassed": True,
            "overLimitRejectedBeforePut": True,
        },
        "publication": {
            "noPutBeforeSeal": True,
            "sealAtomicityPassed": True,
            "finalizeAtomicityPassed": True,
            "partialReadyRows": 0,
            "retryPassed": True,
        },
        "download": {
            "signedHeadPassed": True,
            "signedRangePassed": True,
            "crossOwnerRejected": True,
            "locatorRedacted": True,
            "hashVerified": True,
        },
        "lifecycle": {
            "expiryRejected": True,
            "objectGcPassed": True,
            "detailGcPassed": True,
            "retryIdempotencyPassed": True,
            "remainingObjects": 0,
            "remainingDetailRows": 0,
        },
        "consumers": {
            "edgeContractPassed": True,
            "nextContractPassed": True,
            "readyStatePassed": True,
            "expiredStatePassed": True,
            "deletedStatePassed": True,
        },
    }


class QualificationTests(unittest.TestCase):
    def test_capacity_requires_all_exact_cache_modes(self) -> None:
        values = [capacity(mode) for mode in qualification.CACHE_MODES]
        qualification._validate_capacity_results(values)
        values[2]["cacheScenario"]["mode"] = "warm"
        with self.assertRaisesRegex(qualification.QualificationError, "mixed"):
            qualification._validate_capacity_results(values)

    def test_capacity_rejects_synthetic_or_empty_payload_evidence(self) -> None:
        values = [capacity(mode) for mode in qualification.CACHE_MODES]
        values[0]["inputMode"] = "synthetic-cardinality"
        with self.assertRaisesRegex(qualification.QualificationError, "real-payload"):
            qualification._validate_capacity_results(values)

    def test_capacity_rejects_semantic_and_artifact_drift(self) -> None:
        values = [capacity(mode) for mode in qualification.CACHE_MODES]
        values[1]["issueCount"] += 1
        with self.assertRaisesRegex(qualification.QualificationError, "identity drift"):
            qualification._validate_capacity_results(values)
        values = [capacity(mode) for mode in qualification.CACHE_MODES]
        values[3]["artifacts"][0]["checksumSha256"] = "3" * 64
        with self.assertRaisesRegex(qualification.QualificationError, "identity drift"):
            qualification._validate_capacity_results(values)

    def test_external_result_rejects_resource_budget_failure(self) -> None:
        values = [capacity(mode) for mode in qualification.CACHE_MODES]
        values[0]["realPackageCollection"].update(
            {
                "excludedPackageManifests": 1,
                "excludedNonJsonFiles": 0,
            }
        )
        metrics = {
            "wallTimeSeconds": 601.0,
            "processPeakRssBytes": 1,
            "cgroupAnonBytes": 1,
            "cgroupFileBytes": 1,
            "cgroupCurrentBytes": 1,
            "cgroupPeakBytes": 1,
            "tempPeakBytes": 1,
            "tempFinalBytes": 0,
        }
        with self.assertRaisesRegex(qualification.QualificationError, "resource budget"):
            qualification._external_result(
                components={"worker_harness": SHA},
                fixture_sha=SHA256,
                capacities=values,
                tidas={
                    "version": qualification.TIDAS_VERSION,
                    "protocol": qualification.TIDAS_PROTOCOL,
                    "events": 3,
                    "spoolBytes": 40,
                    "spoolSha256": SHA256,
                    "wallTimeSeconds": 1.0,
                    "processPeakRssBytes": 1,
                },
                run_metrics=[metrics],
                extraction_bytes=1,
            )

    def test_safe_zip_preserves_unicode_newlines_and_incompressible_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            archive = root / "fixture.zip"
            random_bytes = os.urandom(1024 * 1024)
            payload = '{"name":"真实 payload\\nline two","report":"' + ("x" * 4096) + '"}'
            with zipfile.ZipFile(archive, "w") as fixture:
                fixture.writestr("processes/真实.json", payload)
                fixture.writestr("sources/incompressible.bin", random_bytes)
            destination = root / "package"
            destination.mkdir()
            observed = qualification._safe_extract_zip(archive, destination)
            self.assertEqual(observed, len(payload.encode()) + len(random_bytes))
            self.assertEqual(
                (destination / "processes/真实.json").read_text(),
                payload,
            )
            self.assertEqual(
                (destination / "sources/incompressible.bin").read_bytes(),
                random_bytes,
            )

    def test_safe_zip_rejects_traversal_and_duplicate_members(self) -> None:
        for members in (
            (("../escape.json", b"{}"),),
            (("same.json", b"{}"), ("same.json", b"{}")),
        ):
            with self.subTest(members=len(members)), tempfile.TemporaryDirectory() as tempdir:
                root = Path(tempdir)
                archive = root / "fixture.zip"
                with zipfile.ZipFile(archive, "w") as fixture:
                    for name, content in members:
                        fixture.writestr(name, content)
                destination = root / "package"
                destination.mkdir()
                with self.assertRaises(qualification.QualificationError):
                    qualification._safe_extract_zip(archive, destination)

    def test_owner_result_rejects_missing_fields_and_wrong_exact_sha(self) -> None:
        value = {
            "schemaVersion": qualification.PROVIDER_OWNER_SCHEMA,
            "runId": "run",
            "owner": "database",
            "component": "database",
            "componentSha": SHA,
            "targetClass": "isolated-production-equivalent",
            "productionMutation": False,
            "assertions": 1,
            "evidence": {"descriptors": {"batch596": True}},
        }
        qualification._validate_owner_result(
            value,
            owner="database",
            component="database",
            component_sha=SHA,
            run_id="run",
        )
        missing = deepcopy(value)
        del missing["assertions"]
        with self.assertRaisesRegex(qualification.QualificationError, "fields drifted"):
            qualification._validate_owner_result(
                missing,
                owner="database",
                component="database",
                component_sha=SHA,
                run_id="run",
            )
        with self.assertRaisesRegex(qualification.QualificationError, "identity"):
            qualification._validate_owner_result(
                value,
                owner="database",
                component="database",
                component_sha="4" * 40,
                run_id="run",
            )

    def test_provider_rejects_missing_fields_failed_assertions_and_cleanup_residue(self) -> None:
        evidence = provider_evidence()
        qualification._validate_provider_evidence(evidence)
        missing = deepcopy(evidence)
        del missing["download"]["signedRangePassed"]
        with self.assertRaisesRegex(qualification.QualificationError, "fields drifted"):
            qualification._validate_provider_evidence(missing)
        failed = deepcopy(evidence)
        failed["descriptors"]["staleFenceRejected"] = False
        with self.assertRaisesRegex(qualification.QualificationError, "did not pass"):
            qualification._validate_provider_evidence(failed)
        residue = deepcopy(evidence)
        residue["lifecycle"]["remainingObjects"] = 1
        with self.assertRaisesRegex(qualification.QualificationError, "cleanup"):
            qualification._validate_provider_evidence(residue)

    def test_provider_rejects_production_fingerprint_without_echoing_value(self) -> None:
        environment = {
            name: "qualification-value" for name in qualification.PROVIDER_REQUIRED_ENV
        }
        environment.update(
            {
                "QUALIFICATION_DATABASE_URL": "postgres://localhost/qualification",
                "QUALIFICATION_SUPABASE_URL": "http://127.0.0.1:54321",
                "QUALIFICATION_S3_ENDPOINT": "http://localhost:9000",
                "QUALIFICATION_S3_BUCKET": "qualification-private",
                "QUALIFICATION_NON_PRODUCTION_CONFIRMATION":
                    "I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS",
                "QUALIFICATION_PRIVATE_MARKER": "https://lca.tiangong.earth/private",
            }
        )
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaises(qualification.QualificationError) as captured:
                qualification._validate_provider_environment()
        self.assertNotIn("lca.tiangong.earth", str(captured.exception))

    def test_provider_child_environment_drops_unscoped_credentials(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PATH": "/usr/bin",
                "DATABASE_URL": "must-not-pass",
                "SUPABASE_SERVICE_ROLE_KEY": "must-not-pass",
                "QUALIFICATION_DATABASE_URL": "postgres://localhost/qualification",
            },
            clear=True,
        ):
            child = qualification._provider_child_environment("run")
        self.assertNotIn("DATABASE_URL", child)
        self.assertNotIn("SUPABASE_SERVICE_ROLE_KEY", child)
        self.assertEqual(child["QUALIFICATION_RUN_ID"], "run")

    def test_result_rejects_secret_locator_and_payload_leakage(self) -> None:
        for value in (
            {"signedUrl": "redacted"},
            {"payload": {}},
            {"safe": "https://localhost/object"},
        ):
            with self.subTest(value=value):
                with self.assertRaises(qualification.QualificationError):
                    qualification._reject_sensitive(value)


if __name__ == "__main__":
    unittest.main()
