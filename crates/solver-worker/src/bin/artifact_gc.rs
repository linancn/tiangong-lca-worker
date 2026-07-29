use clap::Parser;
use serde_json::Value;
use solver_worker::{
    artifact_gc::{
        ArtifactGcCandidate, ArtifactGcClaim, ArtifactGcCompletion, ArtifactObjectDeleteOutcome,
        complete_artifact_details, validated_batch_size, validated_detail_limit,
        validated_lease_seconds, validated_max_batches,
    },
    db_pool::{APP_ARTIFACT_GC, WorkerDbPoolOptions},
    pgbouncer_sqlx::{self as sqlx, Row},
    storage::{ObjectDeleteOutcome, ObjectStoreClient},
};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "artifact-gc")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "S3_REGION")]
    s3_region: Option<String>,
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    s3_access_key_id: Option<String>,
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    s3_secret_access_key: Option<String>,
    #[arg(long, env = "S3_SESSION_TOKEN")]
    s3_session_token: Option<String>,
    #[arg(long, env = "S3_PREFIX", default_value = "lca-results")]
    s3_prefix: String,
    /// Canonical maintenance `worker_jobs` identity used to fence Database RPCs.
    #[arg(long)]
    worker_job_id: Uuid,
    /// Active lease token for the maintenance `worker_jobs` row.
    #[arg(long)]
    worker_lease_token: Uuid,
    #[arg(long, env = "ARTIFACT_GC_BATCH_SIZE", default_value_t = 100_i64)]
    batch_size: i64,
    #[arg(long, env = "ARTIFACT_GC_MAX_BATCHES", default_value = "10")]
    max_batches: Option<i64>,
    #[arg(long, env = "ARTIFACT_GC_LEASE_SECONDS", default_value_t = 300_i64)]
    lease_seconds: i64,
    #[arg(long, env = "ARTIFACT_GC_DETAIL_LIMIT", default_value_t = 10_000_i64)]
    detail_limit: i64,
    /// Execute object deletion and Database completion. Default is bounded preview.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Default)]
struct ArtifactGcTotals {
    batches: u64,
    candidates: u64,
    objects_deleted: u64,
    objects_missing: u64,
    detail_cleanup_candidates: u64,
    completed: u64,
    retryable_failures: u64,
    completion_rounds: u64,
    deleted_occurrences: u64,
    deleted_affected_roots: u64,
    deleted_issues: u64,
}

impl Cli {
    fn database_url(&self) -> anyhow::Result<&str> {
        self.database_url
            .as_deref()
            .or(self.conn.as_deref())
            .ok_or_else(|| anyhow::anyhow!("missing DB connection: set DATABASE_URL or CONN"))
    }

    fn object_store(&self) -> anyhow::Result<ObjectStoreClient> {
        fn required<'a>(value: Option<&'a str>, name: &str) -> anyhow::Result<&'a str> {
            value.ok_or_else(|| anyhow::anyhow!("missing {name}"))
        }
        ObjectStoreClient::new(
            required(self.s3_endpoint.as_deref(), "S3_ENDPOINT")?,
            required(self.s3_region.as_deref(), "S3_REGION")?,
            required(self.s3_bucket.as_deref(), "S3_BUCKET")?,
            &self.s3_prefix,
            required(self.s3_access_key_id.as_deref(), "S3_ACCESS_KEY_ID")?,
            required(self.s3_secret_access_key.as_deref(), "S3_SECRET_ACCESS_KEY")?,
            self.s3_session_token.clone(),
        )
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let batch_size = validated_batch_size(cli.batch_size)?;
    let max_batches = cli
        .max_batches
        .map(validated_max_batches)
        .transpose()?
        .unwrap_or(10);
    let lease_seconds = validated_lease_seconds(cli.lease_seconds)?;
    let detail_limit = validated_detail_limit(cli.detail_limit)?;
    let pool = WorkerDbPoolOptions::new(APP_ARTIFACT_GC)
        .max_connections(2)
        .connect(cli.database_url()?)
        .await?;
    let store = if cli.execute {
        Some(cli.object_store()?)
    } else {
        None
    };
    let mut totals = ArtifactGcTotals::default();

    loop {
        if totals.batches >= u64::try_from(max_batches)? {
            break;
        }
        let claim = claim_candidates(&pool, batch_size, lease_seconds).await?;
        if claim.items.is_empty() {
            break;
        }
        totals.batches = totals.batches.saturating_add(1);
        totals.candidates = totals
            .candidates
            .saturating_add(u64::try_from(claim.items.len())?);

        if !cli.execute {
            for candidate in claim.items {
                println!(
                    "[dry-run] artifact_id={} role={} phase={} object_delete_required={} bucket={} path={} expires_at={}",
                    candidate.artifact_id,
                    candidate.artifact_role,
                    candidate.gc_phase.as_str(),
                    candidate.object_delete_required,
                    candidate.storage_bucket.as_deref().unwrap_or("-"),
                    candidate.storage_path.as_deref().unwrap_or("-"),
                    candidate.artifact_expires_at,
                );
            }
            break;
        }

        let store = store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("object store is required for --execute"))?;
        for candidate in claim.items {
            if let Err(error) = candidate.validate() {
                record_failure(&pool, claim.claim_token, &candidate, error.to_string()).await?;
                totals.retryable_failures = totals.retryable_failures.saturating_add(1);
                continue;
            }
            if !candidate.object_delete_required {
                totals.detail_cleanup_candidates =
                    totals.detail_cleanup_candidates.saturating_add(1);
                complete_and_accumulate(
                    &pool,
                    claim.claim_token,
                    &candidate,
                    ArtifactObjectDeleteOutcome::Deleted,
                    detail_limit,
                    &mut totals,
                )
                .await?;
                continue;
            }
            let candidate_bucket = candidate.storage_bucket.as_deref().unwrap_or_default();
            if candidate_bucket != store.bucket_name() {
                record_failure(
                    &pool,
                    claim.claim_token,
                    &candidate,
                    format!(
                        "artifact_gc_bucket_mismatch: candidate_bucket={}, configured_bucket={}",
                        candidate_bucket,
                        store.bucket_name()
                    ),
                )
                .await?;
                totals.retryable_failures = totals.retryable_failures.saturating_add(1);
                continue;
            }

            match store
                .delete_object_key(candidate.storage_path.as_deref().unwrap_or_default())
                .await
            {
                Ok(outcome) => {
                    let outcome = match outcome {
                        ObjectDeleteOutcome::Deleted => {
                            totals.objects_deleted = totals.objects_deleted.saturating_add(1);
                            ArtifactObjectDeleteOutcome::Deleted
                        }
                        ObjectDeleteOutcome::Missing => {
                            totals.objects_missing = totals.objects_missing.saturating_add(1);
                            ArtifactObjectDeleteOutcome::Missing
                        }
                    };
                    complete_and_accumulate(
                        &pool,
                        claim.claim_token,
                        &candidate,
                        outcome,
                        detail_limit,
                        &mut totals,
                    )
                    .await?;
                }
                Err(error) => {
                    record_failure(&pool, claim.claim_token, &candidate, error.to_string()).await?;
                    totals.retryable_failures = totals.retryable_failures.saturating_add(1);
                }
            }
        }
    }

    println!(
        "[summary] dry_run={} batches={} candidates={} objects_deleted={} objects_missing={} detail_cleanup_candidates={} completed={} retryable_failures={} completion_rounds={} deleted_occurrences={} deleted_affected_roots={} deleted_issues={}",
        !cli.execute,
        totals.batches,
        totals.candidates,
        totals.objects_deleted,
        totals.objects_missing,
        totals.detail_cleanup_candidates,
        totals.completed,
        totals.retryable_failures,
        totals.completion_rounds,
        totals.deleted_occurrences,
        totals.deleted_affected_roots,
        totals.deleted_issues,
    );
    Ok(())
}

async fn complete_and_accumulate(
    pool: &sqlx::PgPool,
    claim_token: Uuid,
    candidate: &ArtifactGcCandidate,
    outcome: ArtifactObjectDeleteOutcome,
    detail_limit: i64,
    totals: &mut ArtifactGcTotals,
) -> anyhow::Result<()> {
    let (rounds, completion) = complete_artifact_details(|| {
        complete_candidate(pool, claim_token, candidate, outcome, detail_limit)
    })
    .await?;
    totals.completion_rounds = totals.completion_rounds.saturating_add(rounds);
    totals.deleted_occurrences = totals
        .deleted_occurrences
        .saturating_add(completion.deleted_occurrences);
    totals.deleted_affected_roots = totals
        .deleted_affected_roots
        .saturating_add(completion.deleted_affected_roots);
    totals.deleted_issues = totals
        .deleted_issues
        .saturating_add(completion.deleted_issues);
    totals.completed = totals.completed.saturating_add(1);
    Ok(())
}

async fn claim_candidates(
    pool: &sqlx::PgPool,
    batch_size: i64,
    lease_seconds: i64,
) -> anyhow::Result<ArtifactGcClaim> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_gc_claim($1, $2) AS result
        FROM _service_role
        ",
    )
    .bind(i32::try_from(batch_size)?)
    .bind(i32::try_from(lease_seconds)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_gc_claim")?;
    let claim = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("artifact GC claim omitted data"))?;
    let claim: ArtifactGcClaim = serde_json::from_value(claim)?;
    if claim.items.len() > usize::try_from(batch_size)? {
        return Err(anyhow::anyhow!(
            "artifact GC claim exceeded requested batch bound"
        ));
    }
    Ok(claim)
}

async fn complete_candidate(
    pool: &sqlx::PgPool,
    claim_token: Uuid,
    candidate: &ArtifactGcCandidate,
    outcome: ArtifactObjectDeleteOutcome,
    detail_limit: i64,
) -> anyhow::Result<ArtifactGcCompletion> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_gc_complete($1, $2, $3, $4) AS result
        FROM _service_role
        ",
    )
    .bind(candidate.artifact_id)
    .bind(claim_token)
    .bind(outcome == ArtifactObjectDeleteOutcome::Missing)
    .bind(i32::try_from(detail_limit)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_gc_complete")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("artifact GC completion omitted data"))?;
    Ok(serde_json::from_value(data)?)
}

async fn record_failure(
    pool: &sqlx::PgPool,
    claim_token: Uuid,
    candidate: &ArtifactGcCandidate,
    message: String,
) -> anyhow::Result<()> {
    let bounded_message = message.chars().take(1_000).collect::<String>();
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_gc_fail($1, $2, $3) AS result
        FROM _service_role
        ",
    )
    .bind(candidate.artifact_id)
    .bind(claim_token)
    .bind(bounded_message)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_gc_fail")
}

fn ensure_rpc_ok(result: &Value, name: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{name} returned non-ok result: {result}"))
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;

    #[test]
    fn cli_defaults_to_non_destructive_preview() {
        let cli = Cli::try_parse_from([
            "artifact-gc",
            "--worker-job-id",
            "11111111-1111-4111-8111-111111111111",
            "--worker-lease-token",
            "22222222-2222-4222-8222-222222222222",
        ])
        .unwrap();
        assert!(!cli.execute);
        assert_eq!(cli.batch_size, 100);
        assert_eq!(cli.max_batches, Some(10));
        assert_eq!(cli.lease_seconds, 300);
        assert_eq!(cli.detail_limit, 10_000);
    }
}
