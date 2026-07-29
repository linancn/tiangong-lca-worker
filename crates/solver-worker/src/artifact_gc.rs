//! Retry-safe application-level garbage-collection state machine for Worker artifacts.

use std::future::Future;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const ARTIFACT_GC_JOB_KIND: &str = "worker.artifact_gc";
pub const ARTIFACT_GC_REQUEST_SCHEMA_VERSION: &str = "worker.artifact_gc.request.v1";
pub const ARTIFACT_GC_RESULT_SCHEMA_VERSION: &str = "worker.artifact_gc.result.v1";
pub const ARTIFACT_GC_MAX_BATCH_SIZE: i64 = 500;
pub const ARTIFACT_GC_MAX_BATCHES: i64 = 100;
pub const ARTIFACT_GC_MAX_LEASE_SECONDS: i64 = 3_600;
pub const ARTIFACT_GC_MAX_DETAIL_LIMIT: i64 = 50_000;
pub const ARTIFACT_GC_MAX_COMPLETION_ROUNDS: u64 = 10_000;
pub const ARTIFACT_GC_COMPLETION_CALL_ATTEMPTS: u64 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactGcCandidate {
    pub artifact_id: Uuid,
    #[serde(rename = "bucket")]
    pub storage_bucket: Option<String>,
    #[serde(rename = "objectPath")]
    pub storage_path: Option<String>,
    pub artifact_role: String,
    pub lifecycle_state: String,
    pub gc_phase: ArtifactGcPhase,
    pub object_delete_required: bool,
    pub checksum_sha256: String,
    pub artifact_expires_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactGcPhase {
    ObjectDelete,
    DetailCleanup,
}

impl ArtifactGcPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectDelete => "object_delete",
            Self::DetailCleanup => "detail_cleanup",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactGcClaim {
    pub claim_token: Uuid,
    pub lease_expires_at: String,
    pub items: Vec<ArtifactGcCandidate>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactGcCompletion {
    pub details_remaining: u64,
    #[serde(default)]
    pub deleted_occurrences: u64,
    #[serde(default)]
    pub deleted_affected_roots: u64,
    #[serde(default)]
    pub deleted_issues: u64,
}

impl ArtifactGcCandidate {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self.gc_phase {
            ArtifactGcPhase::ObjectDelete => {
                if !self.object_delete_required || self.lifecycle_state != "expired" {
                    return Err(anyhow::anyhow!(
                        "artifact GC object-delete candidate has inconsistent phase"
                    ));
                }
                let bucket = self.storage_bucket.as_deref().unwrap_or_default().trim();
                if bucket.is_empty() {
                    return Err(anyhow::anyhow!(
                        "artifact GC candidate omitted storage bucket"
                    ));
                }
                validate_storage_path(self.storage_path.as_deref().unwrap_or_default())?;
            }
            ArtifactGcPhase::DetailCleanup => {
                if self.object_delete_required
                    || self.lifecycle_state != "deleted"
                    || self.storage_bucket.is_some()
                    || self.storage_path.is_some()
                {
                    return Err(anyhow::anyhow!(
                        "artifact GC detail-cleanup candidate exposed an object locator"
                    ));
                }
            }
        }
        if self.artifact_role.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "artifact GC candidate omitted artifact role"
            ));
        }
        if self.checksum_sha256.len() != 64
            || !self
                .checksum_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(anyhow::anyhow!(
                "artifact GC candidate has invalid SHA-256 checksum"
            ));
        }
        if self.artifact_expires_at.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "artifact GC candidate omitted artifact expiry"
            ));
        }
        Ok(())
    }
}

fn validate_storage_path(path: &str) -> anyhow::Result<()> {
    let path = path.trim();
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(anyhow::anyhow!(
            "artifact GC candidate has invalid storage path"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactObjectDeleteOutcome {
    Deleted,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactGcProcessOutcome {
    CompletedObject(ArtifactObjectDeleteOutcome),
    CompletedDetailCleanup,
    RetryRecorded,
}

pub fn validated_batch_size(batch_size: i64) -> anyhow::Result<i64> {
    if !(1..=ARTIFACT_GC_MAX_BATCH_SIZE).contains(&batch_size) {
        return Err(anyhow::anyhow!(
            "artifact GC batch size must be between 1 and {ARTIFACT_GC_MAX_BATCH_SIZE}"
        ));
    }
    Ok(batch_size)
}

pub fn validated_max_batches(max_batches: i64) -> anyhow::Result<i64> {
    if !(1..=ARTIFACT_GC_MAX_BATCHES).contains(&max_batches) {
        return Err(anyhow::anyhow!(
            "artifact GC max batches must be between 1 and {ARTIFACT_GC_MAX_BATCHES}"
        ));
    }
    Ok(max_batches)
}

pub fn validated_lease_seconds(lease_seconds: i64) -> anyhow::Result<i64> {
    if !(30..=ARTIFACT_GC_MAX_LEASE_SECONDS).contains(&lease_seconds) {
        return Err(anyhow::anyhow!(
            "artifact GC lease seconds must be between 30 and {ARTIFACT_GC_MAX_LEASE_SECONDS}"
        ));
    }
    Ok(lease_seconds)
}

pub fn validated_detail_limit(detail_limit: i64) -> anyhow::Result<i64> {
    if !(1..=ARTIFACT_GC_MAX_DETAIL_LIMIT).contains(&detail_limit) {
        return Err(anyhow::anyhow!(
            "artifact GC detail limit must be between 1 and {ARTIFACT_GC_MAX_DETAIL_LIMIT}"
        ));
    }
    Ok(detail_limit)
}

pub async fn complete_artifact_details<Complete, CompleteFuture>(
    mut complete: Complete,
) -> anyhow::Result<(u64, ArtifactGcCompletion)>
where
    Complete: FnMut() -> CompleteFuture,
    CompleteFuture: Future<Output = anyhow::Result<ArtifactGcCompletion>>,
{
    let mut total = ArtifactGcCompletion::default();
    for round in 1..=ARTIFACT_GC_MAX_COMPLETION_ROUNDS {
        let mut attempt = 0_u64;
        let completion = loop {
            attempt = attempt.saturating_add(1);
            match complete().await {
                Ok(completion) => break completion,
                Err(_) if attempt < ARTIFACT_GC_COMPLETION_CALL_ATTEMPTS => {}
                Err(error) => {
                    return Err(error.context(format!(
                        "artifact GC completion failed after {attempt} attempts"
                    )));
                }
            }
        };
        total.details_remaining = completion.details_remaining;
        total.deleted_occurrences = total
            .deleted_occurrences
            .saturating_add(completion.deleted_occurrences);
        total.deleted_affected_roots = total
            .deleted_affected_roots
            .saturating_add(completion.deleted_affected_roots);
        total.deleted_issues = total
            .deleted_issues
            .saturating_add(completion.deleted_issues);
        if completion.details_remaining == 0 {
            return Ok((round, total));
        }
    }
    Err(anyhow::anyhow!(
        "artifact GC detail cleanup exceeded {ARTIFACT_GC_MAX_COMPLETION_ROUNDS} bounded rounds"
    ))
}

/// Deletes the object before asking Database to tombstone metadata and purge bounded details.
///
/// A missing object is a successful idempotent delete. Delete failures are recorded for retry and
/// never reach completion. If completion itself fails, the error is returned so the caller can
/// retry with the same claim token. Fresh-process recovery after the first tombstoning completion
/// remains a Database contract requirement.
pub async fn process_artifact_gc_candidate<
    Delete,
    DeleteFuture,
    Complete,
    CompleteFuture,
    Fail,
    FailFuture,
>(
    candidate: &ArtifactGcCandidate,
    expected_bucket: &str,
    delete_object: Delete,
    complete_gc: Complete,
    record_failure: Fail,
) -> anyhow::Result<ArtifactGcProcessOutcome>
where
    Delete: FnOnce(&str) -> DeleteFuture,
    DeleteFuture: Future<Output = anyhow::Result<ArtifactObjectDeleteOutcome>>,
    Complete: FnOnce(ArtifactObjectDeleteOutcome) -> CompleteFuture,
    CompleteFuture: Future<Output = anyhow::Result<()>>,
    Fail: FnOnce(String) -> FailFuture,
    FailFuture: Future<Output = anyhow::Result<()>>,
{
    candidate.validate()?;
    if !candidate.object_delete_required {
        complete_gc(ArtifactObjectDeleteOutcome::Deleted).await?;
        return Ok(ArtifactGcProcessOutcome::CompletedDetailCleanup);
    }
    let candidate_bucket = candidate.storage_bucket.as_deref().unwrap_or_default();
    if candidate_bucket != expected_bucket {
        let message = format!(
            "artifact_gc_bucket_mismatch: candidate_bucket={candidate_bucket}, configured_bucket={expected_bucket}"
        );
        record_failure(message).await?;
        return Ok(ArtifactGcProcessOutcome::RetryRecorded);
    }

    match delete_object(candidate.storage_path.as_deref().unwrap_or_default()).await {
        Ok(outcome) => {
            complete_gc(outcome).await?;
            Ok(ArtifactGcProcessOutcome::CompletedObject(outcome))
        }
        Err(error) => {
            record_failure(error.to_string()).await?;
            Ok(ArtifactGcProcessOutcome::RetryRecorded)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{
        ArtifactGcCandidate, ArtifactGcCompletion, ArtifactGcPhase, ArtifactGcProcessOutcome,
        ArtifactObjectDeleteOutcome, complete_artifact_details, process_artifact_gc_candidate,
        validated_batch_size, validated_detail_limit, validated_lease_seconds,
        validated_max_batches,
    };
    use uuid::Uuid;

    fn candidate() -> ArtifactGcCandidate {
        ArtifactGcCandidate {
            artifact_id: Uuid::nil(),
            storage_bucket: Some("private-results".to_owned()),
            storage_path: Some("lca-results/scope-closure/check/manifest.json".to_owned()),
            artifact_role: "complete_machine_result".to_owned(),
            lifecycle_state: "expired".to_owned(),
            gc_phase: ArtifactGcPhase::ObjectDelete,
            object_delete_required: true,
            checksum_sha256: "a".repeat(64),
            artifact_expires_at: "2026-07-29T00:00:00Z".to_owned(),
        }
    }

    #[tokio::test]
    async fn deletes_object_before_database_completion() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let delete_events = Arc::clone(&events);
        let complete_events = Arc::clone(&events);
        let failure_events = Arc::clone(&events);

        let outcome = process_artifact_gc_candidate(
            &candidate(),
            "private-results",
            move |_| async move {
                delete_events.lock().unwrap().push("delete");
                Ok(ArtifactObjectDeleteOutcome::Deleted)
            },
            move |_| async move {
                complete_events.lock().unwrap().push("complete");
                Ok(())
            },
            move |_| async move {
                failure_events.lock().unwrap().push("failure");
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            ArtifactGcProcessOutcome::CompletedObject(ArtifactObjectDeleteOutcome::Deleted)
        );
        assert_eq!(*events.lock().unwrap(), vec!["delete", "complete"]);
    }

    #[tokio::test]
    async fn missing_object_completes_orphan_recovery_idempotently() {
        let outcome = process_artifact_gc_candidate(
            &candidate(),
            "private-results",
            |_| async { Ok(ArtifactObjectDeleteOutcome::Missing) },
            |outcome| async move {
                assert_eq!(outcome, ArtifactObjectDeleteOutcome::Missing);
                Ok(())
            },
            |_| async { panic!("missing object must not be recorded as a failure") },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            ArtifactGcProcessOutcome::CompletedObject(ArtifactObjectDeleteOutcome::Missing)
        );
    }

    #[tokio::test]
    async fn delete_failure_records_retry_without_tombstone() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let delete_events = Arc::clone(&events);
        let complete_events = Arc::clone(&events);
        let failure_events = Arc::clone(&events);

        let outcome = process_artifact_gc_candidate(
            &candidate(),
            "private-results",
            move |_| async move {
                delete_events.lock().unwrap().push("delete");
                Err(anyhow::anyhow!("temporary object-store outage"))
            },
            move |_| async move {
                complete_events.lock().unwrap().push("complete");
                Ok(())
            },
            move |message| async move {
                assert!(message.contains("temporary object-store outage"));
                failure_events.lock().unwrap().push("failure");
                Ok(())
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome, ArtifactGcProcessOutcome::RetryRecorded);
        assert_eq!(*events.lock().unwrap(), vec!["delete", "failure"]);
    }

    #[tokio::test]
    async fn bucket_mismatch_is_retryable_and_never_deletes() {
        let outcome = process_artifact_gc_candidate(
            &candidate(),
            "different-bucket",
            |_| async { panic!("bucket mismatch must not issue object deletion") },
            |_| async { panic!("bucket mismatch must not tombstone metadata") },
            |message| async move {
                assert!(message.contains("artifact_gc_bucket_mismatch"));
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, ArtifactGcProcessOutcome::RetryRecorded);
    }

    #[test]
    fn batch_size_is_strictly_bounded() {
        assert_eq!(validated_batch_size(1).unwrap(), 1);
        assert_eq!(validated_batch_size(500).unwrap(), 500);
        assert!(validated_batch_size(0).is_err());
        assert!(validated_batch_size(501).is_err());
    }

    #[test]
    fn total_batches_are_strictly_bounded() {
        assert_eq!(validated_max_batches(1).unwrap(), 1);
        assert_eq!(validated_max_batches(100).unwrap(), 100);
        assert!(validated_max_batches(0).is_err());
        assert!(validated_max_batches(101).is_err());
    }

    #[test]
    fn database_claim_bounds_are_enforced() {
        assert_eq!(validated_lease_seconds(30).unwrap(), 30);
        assert_eq!(validated_lease_seconds(3_600).unwrap(), 3_600);
        assert!(validated_lease_seconds(29).is_err());
        assert!(validated_lease_seconds(3_601).is_err());
        assert_eq!(validated_detail_limit(1).unwrap(), 1);
        assert_eq!(validated_detail_limit(50_000).unwrap(), 50_000);
        assert!(validated_detail_limit(0).is_err());
        assert!(validated_detail_limit(50_001).is_err());
    }

    #[tokio::test]
    async fn bounded_completion_retries_purge_all_details_without_redeleting_object() {
        let remaining = Arc::new(Mutex::new(vec![0_u64, 4, 9]));
        let observed = Arc::clone(&remaining);
        let (rounds, completion) = complete_artifact_details(move || {
            let observed = Arc::clone(&observed);
            async move {
                let details_remaining = observed.lock().unwrap().pop().unwrap();
                Ok(ArtifactGcCompletion {
                    details_remaining,
                    deleted_occurrences: 1,
                    deleted_affected_roots: 1,
                    deleted_issues: 1,
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(rounds, 3);
        assert_eq!(completion.details_remaining, 0);
    }

    #[tokio::test]
    async fn completion_retries_a_lost_response_with_the_same_claim() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let (rounds, completion) = complete_artifact_details(move || {
            let call = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 0 {
                    Err(anyhow::anyhow!("completion response was lost"))
                } else {
                    Ok(ArtifactGcCompletion::default())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(rounds, 1);
        assert_eq!(completion.details_remaining, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn object_path_must_be_normalized_and_bucket_relative() {
        for invalid_path in [
            "",
            "/absolute/path",
            "prefix//object",
            "prefix/./object",
            "prefix/../object",
            r"prefix\object",
        ] {
            let mut invalid = candidate();
            invalid.storage_path = Some(invalid_path.to_owned());
            assert!(
                invalid.validate().is_err(),
                "{invalid_path:?} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn fresh_process_detail_cleanup_never_redeletes_the_object() {
        let recovery: ArtifactGcCandidate = serde_json::from_value(serde_json::json!({
            "artifactId": Uuid::nil(),
            "artifactRole": "complete_machine_result",
            "lifecycleState": "deleted",
            "gcPhase": "detail_cleanup",
            "objectDeleteRequired": false,
            "bucket": null,
            "objectPath": null,
            "checksumSha256": "a".repeat(64),
            "artifactExpiresAt": "2026-07-29T00:00:00Z"
        }))
        .unwrap();

        let outcome = process_artifact_gc_candidate(
            &recovery,
            "private-results",
            |_| async { panic!("detail cleanup must not delete an object") },
            |outcome| async move {
                assert_eq!(outcome, ArtifactObjectDeleteOutcome::Deleted);
                Ok(())
            },
            |_| async { panic!("valid detail cleanup must not record failure") },
        )
        .await
        .unwrap();
        assert_eq!(outcome, ArtifactGcProcessOutcome::CompletedDetailCleanup);
    }

    #[test]
    fn database_contract_fixture_matches_pr_309_and_is_bounded() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/artifact_lifecycle_v1/database-contract.json"
        ))
        .unwrap();
        assert_eq!(fixture["sourceIssue"], "tiangong-lca/database-engine#308");
        assert_eq!(
            fixture["status"],
            "reconciled-with-database-engine-pr-309-recovery-contract"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "cc059eef795b0f8a9942f9830945f100a1895638"
        );
        assert_eq!(fixture["publication"]["defaultRetentionSeconds"], 604_800);
        assert_eq!(
            fixture["garbageCollection"]["missingObjectOutcome"],
            "success"
        );
        assert_eq!(
            fixture["garbageCollection"]["ordering"],
            serde_json::json!([
                "claim_bounded_candidates",
                "delete_object",
                "complete_tombstone",
                "repeat_bounded_detail_cleanup_until_detailsRemaining_is_zero"
            ])
        );
        assert_eq!(
            fixture["garbageCollection"]["crossProcessOrphanRecovery"]["phase"],
            "detail_cleanup"
        );
        assert_eq!(
            fixture["garbageCollection"]["crossProcessOrphanRecovery"]["objectDeleteRequired"],
            false
        );
    }
}
