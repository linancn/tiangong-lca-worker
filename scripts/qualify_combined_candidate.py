#!/usr/bin/env python3
"""Run the exact-head local qualification for the Worker combined candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

import qualify_snapshot_private_cutover as qualification


ROOT = Path(__file__).resolve().parents[1]
RECEIPT_SCHEMA = "worker.combined-candidate-qualification.v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def frozen_worker_identity() -> dict[str, str]:
    status = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout
    require(not status, "combined qualification requires a clean committed Worker tree")
    return {
        "headSha": qualification.command_output(["git", "rev-parse", "HEAD"]),
        "treeSha": qualification.command_output(["git", "rev-parse", "HEAD^{tree}"]),
    }


def isolated_environment() -> dict[str, str]:
    denied_fragments = ("DATABASE", "SUPABASE", "S3_", "AWS_", "PASSWORD", "SECRET", "TOKEN")
    return {
        key: value
        for key, value in os.environ.items()
        if not any(fragment in key.upper() for fragment in denied_fragments)
    }


def run_test(command: list[str], environment: dict[str, str]) -> dict[str, object]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    combined = completed.stdout + completed.stderr
    require(completed.returncode == 0, f"qualification command failed: {' '.join(command)}\n{combined}")
    require("test result: ok" in combined, f"qualification command emitted no passing test result: {command}")
    return {
        "command": command,
        "passed": True,
    }


def run_control_plane_contract(
    stack: qualification.RunnerOwnedSupabaseStack,
) -> dict[str, object]:
    environment = isolated_environment()
    environment.update(
        {
            "PATH": os.environ["PATH"],
            "WORKER_CONTROL_PLANE_DATABASE_URL": stack.database_url,
            "WORKER_CONTROL_PLANE_DATABASE_WORKTREE": str(stack.workdir),
            "WORKER_CONTROL_PLANE_DATABASE_SHA": qualification.DB_SHA,
            "WORKER_CONTROL_PLANE_MIGRATION_VERSION": qualification.MIGRATION_HEAD,
        }
    )
    result = run_test(["bash", "scripts/run_worker_control_plane_db_integration.sh"], environment)
    return {
        **result,
        "resultGcClaimsEnabled": False,
        "claimExercise": "worker job claim/reclaim lifecycle exercised; result-GC claims not enabled",
        "deploymentLoginClaimed": False,
    }


def run_result_identity_contract(
    stack: qualification.RunnerOwnedSupabaseStack,
) -> dict[str, object]:
    run_id = uuid.uuid4().hex
    bucket = f"worker202-result-{run_id}"
    prefix = f"worker202/result/{run_id}"
    sentinel_key = f"{prefix}/preserve-on-uncertain-outcome.bin"
    sentinel = b"worker202-result-sentinel-v1"
    require(
        qualification.list_s3_keys(
            stack.storage_endpoint, bucket, stack.storage_environment
        )
        is None,
        "runner-generated result bucket already exists",
    )
    qualification.create_s3_bucket(
        stack.storage_endpoint, bucket, stack.storage_environment
    )
    qualification.put_s3_object(
        stack.storage_endpoint,
        bucket,
        sentinel_key,
        sentinel,
        stack.storage_environment,
    )

    environment = isolated_environment()
    environment.update(
        {
            "PATH": os.environ["PATH"],
            "RESULT_IDENTITY_DATABASE_URL": stack.database_url,
        }
    )
    database_result = run_test(
        [
            "cargo",
            "test",
            "-p",
            "solver-worker",
            "--lib",
            "db::tests::result_identity_database_contract_preserves_objects_on_uncertain_outcomes",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
        ],
        environment,
    )
    locator_result = run_test(
        [
            "cargo",
            "test",
            "-p",
            "solver-worker",
            "--lib",
            "storage::tests::result_locator_validation_rejects_target_and_path_drift",
            "--",
            "--exact",
            "--nocapture",
        ],
        environment,
    )
    readback = qualification.read_s3_object(
        stack.storage_endpoint, bucket, sentinel_key, stack.storage_environment
    )
    require(readback == sentinel, "result DB contract changed the runner Storage sentinel")
    qualification.delete_s3_object(
        stack.storage_endpoint, bucket, sentinel_key, stack.storage_environment
    )
    remaining = qualification.list_s3_keys(
        stack.storage_endpoint, bucket, stack.storage_environment
    ) or []
    require(not remaining, f"result qualification Storage residue remains: {remaining}")
    delete_bucket = subprocess.run(
        [
            "aws",
            "--endpoint-url",
            stack.storage_endpoint,
            "s3api",
            "delete-bucket",
            "--bucket",
            bucket,
        ],
        env=qualification.s3_environment(stack.storage_environment),
        text=True,
        capture_output=True,
        check=False,
    )
    require(delete_bucket.returncode == 0, f"owned empty bucket cleanup failed: {delete_bucket.stderr}")
    require(
        qualification.list_s3_keys(
            stack.storage_endpoint, bucket, stack.storage_environment
        )
        is None,
        "runner-owned result bucket remains after exact cleanup",
    )
    return {
        "passed": True,
        "commands": {
            "databaseContract": database_result["command"],
            "locatorBoundary": locator_result["command"],
        },
        "assertions": [
            "preallocated UUID INSERT is exact",
            "lost acknowledgement converges only to an exact visible row",
            "absent readback remains pending and preserves the object",
            "conflicting identity remains an error and preserves the object",
            "readback query failure remains unknown and preserves the object",
            "locator origin/bucket/prefix/result UUID/encoding drift fails closed",
        ],
        "storageCredentialsProvidedToTest": False,
        "sentinelSha256Before": hashlib.sha256(sentinel).hexdigest(),
        "sentinelSha256After": hashlib.sha256(readback).hexdigest(),
        "sentinelChangedByTest": False,
        "localExactKeyCleanup": True,
        "localExactBucketCleanup": True,
        "remainingObjects": 0,
        "remainingBuckets": 0,
    }


def deterministic_receipt(
    worker: dict[str, str],
    checkout: dict[str, object],
    control: dict[str, object],
    result_identity: dict[str, object],
    snapshot: dict[str, object],
    lifecycle: dict[str, object],
    fixture_residue: dict[str, object],
    destruction: dict[str, object],
    tidas_bin: Path,
) -> dict[str, object]:
    failure_cleanup = lifecycle["failureFixture"]["cleanup"]
    success_cleanup = lifecycle["successfulRun"]["cleanup"]
    return {
        "schemaVersion": RECEIPT_SCHEMA,
        "worker": worker,
        "database": {
            "headSha": checkout["headSha"],
            "migrationHead": checkout["migrationHead"],
            "migrationFile": checkout["migrationFile"],
            "migrationGitBlobOid": checkout["migrationGitBlobOid"],
            "migrationFileSha256": checkout["migrationFileSha256"],
            "migrationTreeExact": True,
            "runtimeConfigMutation": "runner-owned supabase/config.toml project identity and seven loopback ports only",
        },
        "tidas": {
            "version": lifecycle["tidasVersion"],
            "binarySha256": hashlib.sha256(tidas_bin.read_bytes()).hexdigest(),
        },
        "tests": {
            "workerControlPlane": control,
            "resultIdentity": result_identity,
            "snapshotPrivateConsumer": {
                "passed": True,
                "residue": snapshot["residue"],
                "dedicatedLoginAclChecked": True,
            },
            "scopeClosureDbStorageLifecycle": {
                "passed": True,
                "tidasDocumentCount": lifecycle["preflight"]["documentCount"],
                "tidasIssueCount": lifecycle["preflight"]["issueCount"],
                "injectedFailureCleanup": {
                    "storagePrefixObjectsRemaining": failure_cleanup[
                        "storagePrefixObjectsRemaining"
                    ],
                    "databaseCardinalityDrift": failure_cleanup["databaseCardinalityDrift"],
                    "siblingShaPreserved": failure_cleanup["siblingObject"]["sha256Before"]
                    == failure_cleanup["siblingObject"]["sha256After"],
                },
                "successfulRunCleanup": {
                    "storagePrefixObjectsRemaining": success_cleanup[
                        "storagePrefixObjectsRemaining"
                    ],
                    "databaseCardinalityDrift": success_cleanup["databaseCardinalityDrift"],
                    "siblingShaPreserved": success_cleanup["siblingObject"]["sha256Before"]
                    == success_cleanup["siblingObject"]["sha256After"],
                },
            },
        },
        "cleanup": {
            "preTeardownFixtureResidue": fixture_residue,
            "stackDestruction": destruction,
        },
        "safety": {
            "targetOwnership": "runner-created-and-exclusive",
            "loopbackOnly": True,
            "callerSuppliedDestructiveTargetAccepted": False,
            "hostedOrProductionWrite": False,
            "hostedOrProductionS3Delete": False,
            "localDeletes": "runner-owned exact prefix/key only",
            "resultGcClaimsEnabled": False,
            "fullResultGcQualificationClaimed": False,
            "resultPhysicalMoveClaimed": False,
            "resultConsumerCutClaimed": False,
            "qualifiedResultSurface": "canonical public.lca_results production insert/reconciliation plus strict locator safety",
            "credentialsIncludedInReceipt": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-repo", type=Path, required=True)
    parser.add_argument("--tidas-bin", type=Path, required=True)
    parser.add_argument("--receipt-output", type=Path, required=True)
    args = parser.parse_args()
    receipt_output = args.receipt_output.resolve()
    require(
        not receipt_output.is_relative_to(ROOT),
        "receipt output must stay outside the Worker checkout",
    )
    worker = frozen_worker_identity()
    checkout = qualification.verify_database_checkout(args.database_repo)
    with tempfile.TemporaryDirectory(prefix="worker202-combined-evidence-") as evidence_dir:
        evidence_output = Path(evidence_dir) / "full-evidence.json"
        stack = qualification.RunnerOwnedSupabaseStack(args.database_repo)
        with stack:
            lifecycle = qualification.run_tidas_lifecycle(
                stack, args.tidas_bin.resolve(), evidence_output
            )
            fixture_baseline = qualification.database_cardinalities(stack.database_url)
            control = run_control_plane_contract(stack)
            result_identity = run_result_identity_contract(stack)
            snapshot = qualification.qualify_database(stack)
            fixture_after = qualification.database_cardinalities(stack.database_url)
            fixture_delta = {
                key: fixture_after.get(key, 0) - fixture_baseline.get(key, 0)
                for key in sorted(set(fixture_baseline) | set(fixture_after))
                if fixture_after.get(key, 0) != fixture_baseline.get(key, 0)
            }
            fixture_residue = {
                "ownership": "runner-owned later control/result/snapshot fixtures",
                "removedBy": "disposable stack destruction",
                "relationCardinalityDelta": fixture_delta,
                "resultStorageObjectsRemaining": result_identity["remainingObjects"],
                "resultStorageBucketsRemaining": result_identity["remainingBuckets"],
            }
        require(stack.destruction is not None, "ephemeral stack destruction evidence missing")
    require(frozen_worker_identity() == worker, "Worker identity changed during qualification")
    receipt = deterministic_receipt(
        worker,
        checkout,
        control,
        result_identity,
        snapshot,
        lifecycle,
        fixture_residue,
        stack.destruction,
        args.tidas_bin.resolve(),
    )
    rendered = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    receipt_output.parent.mkdir(parents=True, exist_ok=True)
    receipt_output.write_text(rendered, encoding="utf-8")
    receipt_sha256 = hashlib.sha256(rendered.encode()).hexdigest()
    receipt_output.with_suffix(receipt_output.suffix + ".sha256").write_text(
        receipt_sha256 + "\n", encoding="utf-8"
    )
    print(rendered, end="")
    print(f"receiptSha256={receipt_sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
