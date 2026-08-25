//! Retry-safe application-level garbage-collection state machine for Worker artifacts.

use std::future::Future;

use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactGcPreview {
    pub items: Vec<ArtifactGcCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactGcRenewal {
    pub claim_token: Uuid,
    pub lease_expires_at: String,
    pub artifact_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactWriteSetReconcileItem {
    pub artifact_id: Uuid,
    #[serde(rename = "bucket")]
    pub storage_bucket: String,
    #[serde(rename = "objectPath")]
    pub storage_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactWriteSetReconcileCandidate {
    pub write_set_id: Uuid,
    pub status: String,
    pub items: Vec<ArtifactWriteSetReconcileItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactWriteSetReconcileClaim {
    pub reconcile_token: Uuid,
    pub lease_expires_at: String,
    pub write_sets: Vec<ArtifactWriteSetReconcileCandidate>,
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

#[derive(Debug, Clone, Copy)]
pub struct ArtifactGcRunOptions {
    pub batch_size: i64,
    pub max_batches: i64,
    pub lease_seconds: i64,
    pub detail_limit: i64,
    pub execute: bool,
}

#[derive(Debug, Default)]
pub struct ArtifactGcRunReport {
    pub preview_items: Vec<ArtifactGcCandidate>,
    pub totals: ArtifactGcTotals,
}

#[derive(Debug, Default)]
pub struct ArtifactGcTotals {
    pub batches: u64,
    pub candidates: u64,
    pub objects_deleted: u64,
    pub objects_missing: u64,
    pub detail_cleanup_candidates: u64,
    pub completed: u64,
    pub retryable_failures: u64,
    pub completion_rounds: u64,
    pub deleted_occurrences: u64,
    pub deleted_affected_roots: u64,
    pub deleted_issues: u64,
    pub lease_handoffs: u64,
    pub staging_write_sets_cleaned: u64,
    pub staging_objects_deleted: u64,
    pub staging_objects_missing: u64,
    pub staging_reconcile_handoffs: u64,
}

#[allow(async_fn_in_trait)]
pub trait ArtifactGcBackend {
    fn expected_bucket(&self) -> &str;

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn preview(&mut self, limit: i64) -> anyhow::Result<ArtifactGcPreview>;

    async fn claim(&mut self, limit: i64, lease_seconds: i64) -> anyhow::Result<ArtifactGcClaim>;

    async fn renew(
        &mut self,
        claim_token: Uuid,
        lease_seconds: i64,
    ) -> anyhow::Result<ArtifactGcRenewal>;

    async fn delete_object(
        &mut self,
        object_path: &str,
    ) -> anyhow::Result<ArtifactObjectDeleteOutcome>;

    async fn complete(
        &mut self,
        claim_token: Uuid,
        candidate: &ArtifactGcCandidate,
        outcome: ArtifactObjectDeleteOutcome,
        detail_limit: i64,
    ) -> anyhow::Result<ArtifactGcCompletion>;

    async fn fail(
        &mut self,
        claim_token: Uuid,
        candidate: &ArtifactGcCandidate,
        message: String,
    ) -> anyhow::Result<()>;

    async fn claim_stale_write_sets(
        &mut self,
        limit: i64,
        lease_seconds: i64,
    ) -> anyhow::Result<ArtifactWriteSetReconcileClaim>;

    async fn complete_stale_write_set(
        &mut self,
        write_set_id: Uuid,
        reconcile_token: Uuid,
    ) -> anyhow::Result<()>;
}

#[derive(Debug)]
struct ArtifactGcLeaseLost(anyhow::Error);

impl std::fmt::Display for ArtifactGcLeaseLost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "artifact GC claim lease lost: {:#}", self.0)
    }
}

impl std::error::Error for ArtifactGcLeaseLost {}

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
    if !(1..=ARTIFACT_GC_MAX_LEASE_SECONDS).contains(&lease_seconds) {
        return Err(anyhow::anyhow!(
            "artifact GC lease seconds must be between 1 and {ARTIFACT_GC_MAX_LEASE_SECONDS}"
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

pub async fn run_artifact_gc<Backend: ArtifactGcBackend>(
    backend: &mut Backend,
    options: ArtifactGcRunOptions,
) -> anyhow::Result<ArtifactGcRunReport> {
    let batch_size = validated_batch_size(options.batch_size)?;
    let max_batches = validated_max_batches(options.max_batches)?;
    let lease_seconds = validated_lease_seconds(options.lease_seconds)?;
    let detail_limit = validated_detail_limit(options.detail_limit)?;
    if !options.execute {
        let preview = backend.preview(batch_size).await?;
        if preview.items.len() > usize::try_from(batch_size)? {
            return Err(anyhow::anyhow!(
                "artifact GC preview exceeded requested batch bound"
            ));
        }
        let candidate_count = u64::try_from(preview.items.len())?;
        return Ok(ArtifactGcRunReport {
            preview_items: preview.items,
            totals: ArtifactGcTotals {
                candidates: candidate_count,
                ..ArtifactGcTotals::default()
            },
        });
    }

    let mut report = ArtifactGcRunReport::default();
    reconcile_stale_write_sets(backend, max_batches, lease_seconds, &mut report.totals).await?;
    if report.totals.staging_reconcile_handoffs > 0 {
        return Ok(report);
    }
    while report.totals.batches < u64::try_from(max_batches)? {
        let mut claim = backend.claim(batch_size, lease_seconds).await?;
        if claim.items.len() > usize::try_from(batch_size)? {
            return Err(anyhow::anyhow!(
                "artifact GC claim exceeded requested batch bound"
            ));
        }
        if claim.items.is_empty() {
            break;
        }
        report.totals.batches = report.totals.batches.saturating_add(1);
        report.totals.candidates = report
            .totals
            .candidates
            .saturating_add(u64::try_from(claim.items.len())?);

        for candidate in claim.items.clone() {
            let result = process_claimed_candidate(
                backend,
                &mut claim,
                &candidate,
                lease_seconds,
                detail_limit,
                &mut report.totals,
            )
            .await;
            match result {
                Ok(()) => {}
                Err(error) if error.downcast_ref::<ArtifactGcLeaseLost>().is_some() => {
                    report.totals.lease_handoffs = report.totals.lease_handoffs.saturating_add(1);
                    return Ok(report);
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(report)
}

async fn reconcile_stale_write_sets<Backend: ArtifactGcBackend>(
    backend: &mut Backend,
    max_batches: i64,
    lease_seconds: i64,
    totals: &mut ArtifactGcTotals,
) -> anyhow::Result<()> {
    for _ in 0..max_batches {
        let claim = backend.claim_stale_write_sets(1, lease_seconds).await?;
        if claim.write_sets.is_empty() {
            return Ok(());
        }
        if claim.write_sets.len() != 1 {
            return Err(anyhow::anyhow!(
                "staging reconciliation exceeded the single-write-set claim bound"
            ));
        }
        let deadline = DateTime::parse_from_rfc3339(&claim.lease_expires_at)
            .map(DateTime::<Utc>::from)
            .map_err(|error| {
                anyhow::anyhow!("invalid staging reconcile leaseExpiresAt: {error}")
            })?;
        let write_set = &claim.write_sets[0];
        if write_set.status != "cleanup_pending" || write_set.items.len() > 500 {
            return Err(anyhow::anyhow!(
                "Database returned an invalid staging reconciliation candidate"
            ));
        }
        for item in &write_set.items {
            if item.storage_bucket != backend.expected_bucket() {
                return Err(anyhow::anyhow!(
                    "staging reconcile bucket mismatch for artifact {}",
                    item.artifact_id
                ));
            }
            let remaining = (deadline - backend.now()).to_std().map_err(|_| {
                anyhow::Error::new(ArtifactGcLeaseLost(anyhow::anyhow!(
                    "staging reconcile claim expired before object deletion"
                )))
            });
            let Ok(remaining) = remaining else {
                totals.staging_reconcile_handoffs =
                    totals.staging_reconcile_handoffs.saturating_add(1);
                return Ok(());
            };
            let deletion =
                tokio::time::timeout(remaining, backend.delete_object(item.storage_path.as_str()))
                    .await;
            match deletion {
                Ok(Ok(ArtifactObjectDeleteOutcome::Deleted)) => {
                    totals.staging_objects_deleted =
                        totals.staging_objects_deleted.saturating_add(1);
                }
                Ok(Ok(ArtifactObjectDeleteOutcome::Missing)) => {
                    totals.staging_objects_missing =
                        totals.staging_objects_missing.saturating_add(1);
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    totals.staging_reconcile_handoffs =
                        totals.staging_reconcile_handoffs.saturating_add(1);
                    return Ok(());
                }
            }
        }
        if deadline <= backend.now() {
            totals.staging_reconcile_handoffs = totals.staging_reconcile_handoffs.saturating_add(1);
            return Ok(());
        }
        backend
            .complete_stale_write_set(write_set.write_set_id, claim.reconcile_token)
            .await?;
        totals.staging_write_sets_cleaned = totals.staging_write_sets_cleaned.saturating_add(1);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_claimed_candidate<Backend: ArtifactGcBackend>(
    backend: &mut Backend,
    claim: &mut ArtifactGcClaim,
    candidate: &ArtifactGcCandidate,
    lease_seconds: i64,
    detail_limit: i64,
    totals: &mut ArtifactGcTotals,
) -> anyhow::Result<()> {
    if let Err(error) = candidate.validate() {
        ensure_current_claim(backend, claim, lease_seconds).await?;
        backend
            .fail(
                claim.claim_token,
                candidate,
                error.to_string().chars().take(1_000).collect(),
            )
            .await?;
        totals.retryable_failures = totals.retryable_failures.saturating_add(1);
        return Ok(());
    }

    if !candidate.object_delete_required {
        totals.detail_cleanup_candidates = totals.detail_cleanup_candidates.saturating_add(1);
        complete_claimed_candidate(
            backend,
            claim,
            candidate,
            ArtifactObjectDeleteOutcome::Deleted,
            lease_seconds,
            detail_limit,
            totals,
        )
        .await?;
        return Ok(());
    }

    if candidate.storage_bucket.as_deref().unwrap_or_default() != backend.expected_bucket() {
        ensure_current_claim(backend, claim, lease_seconds).await?;
        let message = format!(
            "artifact_gc_bucket_mismatch: candidate_bucket={}, configured_bucket={}",
            candidate.storage_bucket.as_deref().unwrap_or_default(),
            backend.expected_bucket(),
        );
        backend.fail(claim.claim_token, candidate, message).await?;
        totals.retryable_failures = totals.retryable_failures.saturating_add(1);
        return Ok(());
    }

    ensure_current_claim(backend, claim, lease_seconds).await?;
    let delete_deadline = claim_deadline(backend, claim)?;
    let delete_timeout = (delete_deadline - backend.now()).to_std().map_err(|_| {
        anyhow::Error::new(ArtifactGcLeaseLost(anyhow::anyhow!(
            "claim expired before object deletion"
        )))
    })?;
    let deletion = tokio::time::timeout(
        delete_timeout,
        backend.delete_object(candidate.storage_path.as_deref().unwrap_or_default()),
    )
    .await
    .map_err(|_| {
        anyhow::Error::new(ArtifactGcLeaseLost(anyhow::anyhow!(
            "object deletion reached claim deadline"
        )))
    })?;
    match deletion {
        Ok(outcome) => {
            match outcome {
                ArtifactObjectDeleteOutcome::Deleted => {
                    totals.objects_deleted = totals.objects_deleted.saturating_add(1);
                }
                ArtifactObjectDeleteOutcome::Missing => {
                    totals.objects_missing = totals.objects_missing.saturating_add(1);
                }
            }
            complete_claimed_candidate(
                backend,
                claim,
                candidate,
                outcome,
                lease_seconds,
                detail_limit,
                totals,
            )
            .await
        }
        Err(error) => {
            ensure_current_claim(backend, claim, lease_seconds).await?;
            backend
                .fail(
                    claim.claim_token,
                    candidate,
                    error.to_string().chars().take(1_000).collect(),
                )
                .await?;
            totals.retryable_failures = totals.retryable_failures.saturating_add(1);
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_claimed_candidate<Backend: ArtifactGcBackend>(
    backend: &mut Backend,
    claim: &mut ArtifactGcClaim,
    candidate: &ArtifactGcCandidate,
    outcome: ArtifactObjectDeleteOutcome,
    lease_seconds: i64,
    detail_limit: i64,
    totals: &mut ArtifactGcTotals,
) -> anyhow::Result<()> {
    let mut aggregate = ArtifactGcCompletion::default();
    for round in 1..=ARTIFACT_GC_MAX_COMPLETION_ROUNDS {
        ensure_current_claim(backend, claim, lease_seconds).await?;
        let mut attempt = 0_u64;
        let completion = loop {
            attempt = attempt.saturating_add(1);
            ensure_current_claim(backend, claim, lease_seconds).await?;
            match backend
                .complete(claim.claim_token, candidate, outcome, detail_limit)
                .await
            {
                Ok(completion) => break completion,
                Err(_) if attempt < ARTIFACT_GC_COMPLETION_CALL_ATTEMPTS => {}
                Err(error) => {
                    return Err(error.context(format!(
                        "artifact GC completion failed after {attempt} attempts"
                    )));
                }
            }
        };
        aggregate.details_remaining = completion.details_remaining;
        aggregate.deleted_occurrences = aggregate
            .deleted_occurrences
            .saturating_add(completion.deleted_occurrences);
        aggregate.deleted_affected_roots = aggregate
            .deleted_affected_roots
            .saturating_add(completion.deleted_affected_roots);
        aggregate.deleted_issues = aggregate
            .deleted_issues
            .saturating_add(completion.deleted_issues);
        if completion.details_remaining == 0 {
            totals.completion_rounds = totals.completion_rounds.saturating_add(round);
            totals.deleted_occurrences = totals
                .deleted_occurrences
                .saturating_add(aggregate.deleted_occurrences);
            totals.deleted_affected_roots = totals
                .deleted_affected_roots
                .saturating_add(aggregate.deleted_affected_roots);
            totals.deleted_issues = totals
                .deleted_issues
                .saturating_add(aggregate.deleted_issues);
            totals.completed = totals.completed.saturating_add(1);
            return Ok(());
        }
    }
    Err(anyhow::anyhow!(
        "artifact GC detail cleanup exceeded {ARTIFACT_GC_MAX_COMPLETION_ROUNDS} bounded rounds"
    ))
}

async fn ensure_current_claim<Backend: ArtifactGcBackend>(
    backend: &mut Backend,
    claim: &mut ArtifactGcClaim,
    lease_seconds: i64,
) -> anyhow::Result<()> {
    let deadline = claim_deadline(backend, claim)?;
    let renew_margin = chrono::Duration::seconds((lease_seconds / 3).max(1));
    if deadline > backend.now() + renew_margin {
        return Ok(());
    }
    let renewal = backend
        .renew(claim.claim_token, lease_seconds)
        .await
        .map_err(|error| anyhow::Error::new(ArtifactGcLeaseLost(error)))?;
    if renewal.claim_token != claim.claim_token {
        return Err(anyhow::Error::new(ArtifactGcLeaseLost(anyhow::anyhow!(
            "Database renewed a different claim token"
        ))));
    }
    claim.lease_expires_at = renewal.lease_expires_at;
    let deadline = claim_deadline(backend, claim)?;
    if deadline <= backend.now() {
        return Err(anyhow::Error::new(ArtifactGcLeaseLost(anyhow::anyhow!(
            "renewed claim is already expired"
        ))));
    }
    Ok(())
}

fn claim_deadline<Backend: ArtifactGcBackend>(
    _backend: &Backend,
    claim: &ArtifactGcClaim,
) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&claim.lease_expires_at)
        .map(DateTime::<Utc>::from)
        .map_err(|error| anyhow::anyhow!("invalid artifact GC leaseExpiresAt: {error}"))
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
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use chrono::{DateTime, TimeZone, Utc};

    use super::{
        ArtifactGcBackend, ArtifactGcCandidate, ArtifactGcClaim, ArtifactGcCompletion,
        ArtifactGcPhase, ArtifactGcPreview, ArtifactGcProcessOutcome, ArtifactGcRenewal,
        ArtifactGcRunOptions, ArtifactObjectDeleteOutcome, ArtifactWriteSetReconcileCandidate,
        ArtifactWriteSetReconcileClaim, ArtifactWriteSetReconcileItem, complete_artifact_details,
        process_artifact_gc_candidate, run_artifact_gc, validated_batch_size,
        validated_detail_limit, validated_lease_seconds, validated_max_batches,
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

    struct FakeRunBackend {
        now: DateTime<Utc>,
        preview: Vec<ArtifactGcCandidate>,
        claim: Option<ArtifactGcClaim>,
        completion_remaining: VecDeque<u64>,
        staging_claim: Option<ArtifactWriteSetReconcileClaim>,
        claim_calls: usize,
        renew_calls: usize,
        delete_calls: usize,
        complete_calls: usize,
        fail_calls: usize,
        staging_complete_calls: usize,
        fail_renewal: bool,
        advance_after_completion_seconds: i64,
    }

    impl FakeRunBackend {
        fn new() -> Self {
            Self {
                now: Utc.with_ymd_and_hms(2026, 7, 29, 0, 0, 0).unwrap(),
                preview: vec![candidate()],
                claim: None,
                completion_remaining: VecDeque::new(),
                staging_claim: None,
                claim_calls: 0,
                renew_calls: 0,
                delete_calls: 0,
                complete_calls: 0,
                fail_calls: 0,
                staging_complete_calls: 0,
                fail_renewal: false,
                advance_after_completion_seconds: 0,
            }
        }
    }

    // The fake preserves the production async backend contract without I/O.
    #[allow(clippy::unused_async_trait_impl)]
    impl ArtifactGcBackend for FakeRunBackend {
        fn expected_bucket(&self) -> &'static str {
            "private-results"
        }

        fn now(&self) -> DateTime<Utc> {
            self.now
        }

        async fn preview(&mut self, _limit: i64) -> anyhow::Result<ArtifactGcPreview> {
            Ok(ArtifactGcPreview {
                items: self.preview.clone(),
            })
        }

        async fn claim(
            &mut self,
            _limit: i64,
            _lease_seconds: i64,
        ) -> anyhow::Result<ArtifactGcClaim> {
            self.claim_calls += 1;
            Ok(self.claim.take().unwrap_or(ArtifactGcClaim {
                claim_token: Uuid::new_v4(),
                lease_expires_at: (self.now + chrono::Duration::seconds(30)).to_rfc3339(),
                items: Vec::new(),
            }))
        }

        async fn renew(
            &mut self,
            claim_token: Uuid,
            lease_seconds: i64,
        ) -> anyhow::Result<ArtifactGcRenewal> {
            self.renew_calls += 1;
            if self.fail_renewal {
                return Err(anyhow::anyhow!("old token rejected"));
            }
            Ok(ArtifactGcRenewal {
                claim_token,
                lease_expires_at: (self.now + chrono::Duration::seconds(lease_seconds))
                    .to_rfc3339(),
                artifact_ids: vec![candidate().artifact_id],
            })
        }

        async fn delete_object(
            &mut self,
            _object_path: &str,
        ) -> anyhow::Result<ArtifactObjectDeleteOutcome> {
            self.delete_calls += 1;
            Ok(ArtifactObjectDeleteOutcome::Deleted)
        }

        async fn complete(
            &mut self,
            _claim_token: Uuid,
            _candidate: &ArtifactGcCandidate,
            _outcome: ArtifactObjectDeleteOutcome,
            _detail_limit: i64,
        ) -> anyhow::Result<ArtifactGcCompletion> {
            self.complete_calls += 1;
            let details_remaining = self.completion_remaining.pop_front().unwrap_or(0);
            self.now += chrono::Duration::seconds(self.advance_after_completion_seconds);
            Ok(ArtifactGcCompletion {
                details_remaining,
                deleted_occurrences: 1,
                deleted_affected_roots: 1,
                deleted_issues: 1,
            })
        }

        async fn fail(
            &mut self,
            _claim_token: Uuid,
            _candidate: &ArtifactGcCandidate,
            _message: String,
        ) -> anyhow::Result<()> {
            self.fail_calls += 1;
            Ok(())
        }

        async fn claim_stale_write_sets(
            &mut self,
            _limit: i64,
            _lease_seconds: i64,
        ) -> anyhow::Result<ArtifactWriteSetReconcileClaim> {
            Ok(self
                .staging_claim
                .take()
                .unwrap_or(ArtifactWriteSetReconcileClaim {
                    reconcile_token: Uuid::new_v4(),
                    lease_expires_at: (self.now + chrono::Duration::seconds(30)).to_rfc3339(),
                    write_sets: Vec::new(),
                }))
        }

        async fn complete_stale_write_set(
            &mut self,
            _write_set_id: Uuid,
            _reconcile_token: Uuid,
        ) -> anyhow::Result<()> {
            self.staging_complete_calls += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn dry_run_uses_preview_without_mutating_claim_state() {
        let mut backend = FakeRunBackend::new();
        let report = run_artifact_gc(
            &mut backend,
            ArtifactGcRunOptions {
                batch_size: 100,
                max_batches: 1,
                lease_seconds: 30,
                detail_limit: 10,
                execute: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.preview_items, vec![candidate()]);
        assert_eq!(backend.claim_calls, 0);
        assert_eq!(backend.renew_calls, 0);
        assert_eq!(backend.delete_calls, 0);
        assert_eq!(backend.complete_calls, 0);
        assert_eq!(backend.fail_calls, 0);
    }

    #[tokio::test]
    async fn slow_multi_round_cleanup_renews_and_never_redeletes() {
        let mut backend = FakeRunBackend::new();
        let claim_token = Uuid::new_v4();
        backend.claim = Some(ArtifactGcClaim {
            claim_token,
            lease_expires_at: (backend.now + chrono::Duration::seconds(5)).to_rfc3339(),
            items: vec![candidate()],
        });
        backend.completion_remaining = VecDeque::from([1, 0]);
        backend.advance_after_completion_seconds = 25;

        let report = run_artifact_gc(
            &mut backend,
            ArtifactGcRunOptions {
                batch_size: 100,
                max_batches: 1,
                lease_seconds: 30,
                detail_limit: 10,
                execute: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(backend.delete_calls, 1);
        assert_eq!(backend.complete_calls, 2);
        assert!(backend.renew_calls >= 2);
        assert_eq!(report.totals.completed, 1);
        assert_eq!(report.totals.completion_rounds, 2);
        assert_eq!(report.totals.lease_handoffs, 0);
    }

    #[tokio::test]
    async fn renewal_rejection_hands_off_without_delete_or_stale_terminal_write() {
        let mut backend = FakeRunBackend::new();
        backend.claim = Some(ArtifactGcClaim {
            claim_token: Uuid::new_v4(),
            lease_expires_at: (backend.now + chrono::Duration::seconds(1)).to_rfc3339(),
            items: vec![candidate()],
        });
        backend.fail_renewal = true;

        let report = run_artifact_gc(
            &mut backend,
            ArtifactGcRunOptions {
                batch_size: 100,
                max_batches: 1,
                lease_seconds: 30,
                detail_limit: 10,
                execute: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.totals.lease_handoffs, 1);
        assert_eq!(backend.delete_calls, 0);
        assert_eq!(backend.complete_calls, 0);
        assert_eq!(backend.fail_calls, 0);
    }

    #[tokio::test]
    async fn stale_db_first_write_set_deletes_every_locator_then_completes() {
        let mut backend = FakeRunBackend::new();
        backend.staging_claim = Some(ArtifactWriteSetReconcileClaim {
            reconcile_token: Uuid::new_v4(),
            lease_expires_at: (backend.now + chrono::Duration::seconds(30)).to_rfc3339(),
            write_sets: vec![ArtifactWriteSetReconcileCandidate {
                write_set_id: Uuid::new_v4(),
                status: "cleanup_pending".to_owned(),
                items: vec![
                    ArtifactWriteSetReconcileItem {
                        artifact_id: Uuid::new_v4(),
                        storage_bucket: "private-results".to_owned(),
                        storage_path: "scope-closure/stale/a".to_owned(),
                    },
                    ArtifactWriteSetReconcileItem {
                        artifact_id: Uuid::new_v4(),
                        storage_bucket: "private-results".to_owned(),
                        storage_path: "scope-closure/stale/b".to_owned(),
                    },
                ],
            }],
        });

        let report = run_artifact_gc(
            &mut backend,
            ArtifactGcRunOptions {
                batch_size: 100,
                max_batches: 2,
                lease_seconds: 30,
                detail_limit: 10,
                execute: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(backend.delete_calls, 2);
        assert_eq!(backend.staging_complete_calls, 1);
        assert_eq!(report.totals.staging_write_sets_cleaned, 1);
        assert_eq!(report.totals.staging_objects_deleted, 2);
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
        assert_eq!(validated_lease_seconds(1).unwrap(), 1);
        assert_eq!(validated_lease_seconds(3_600).unwrap(), 3_600);
        assert!(validated_lease_seconds(0).is_err());
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
}
