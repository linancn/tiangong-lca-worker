use clap::Parser;
use serde_json::Value;
use solver_worker::{
    artifact_gc::{
        ArtifactGcBackend, ArtifactGcCandidate, ArtifactGcClaim, ArtifactGcCompletion,
        ArtifactGcPreview, ArtifactGcRenewal, ArtifactGcRunOptions, ArtifactObjectDeleteOutcome,
        ArtifactWriteSetReconcileClaim, run_artifact_gc, validated_batch_size,
        validated_detail_limit, validated_lease_seconds, validated_max_batches,
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
    let mut backend = ProductionArtifactGcBackend { pool, store };
    let report = run_artifact_gc(
        &mut backend,
        ArtifactGcRunOptions {
            batch_size,
            max_batches,
            lease_seconds,
            detail_limit,
            execute: cli.execute,
        },
    )
    .await?;
    for candidate in report.preview_items {
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

    let totals = report.totals;
    println!(
        "[summary] dry_run={} batches={} candidates={} objects_deleted={} objects_missing={} detail_cleanup_candidates={} completed={} retryable_failures={} completion_rounds={} deleted_occurrences={} deleted_affected_roots={} deleted_issues={} lease_handoffs={} staging_write_sets_cleaned={} staging_objects_deleted={} staging_objects_missing={} staging_reconcile_handoffs={}",
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
        totals.lease_handoffs,
        totals.staging_write_sets_cleaned,
        totals.staging_objects_deleted,
        totals.staging_objects_missing,
        totals.staging_reconcile_handoffs,
    );
    Ok(())
}

struct ProductionArtifactGcBackend {
    pool: sqlx::PgPool,
    store: Option<ObjectStoreClient>,
}

impl ArtifactGcBackend for ProductionArtifactGcBackend {
    fn expected_bucket(&self) -> &str {
        self.store
            .as_ref()
            .map_or("", ObjectStoreClient::bucket_name)
    }

    async fn preview(&mut self, limit: i64) -> anyhow::Result<ArtifactGcPreview> {
        preview_candidates(&self.pool, limit).await
    }

    async fn claim(&mut self, limit: i64, lease_seconds: i64) -> anyhow::Result<ArtifactGcClaim> {
        claim_candidates(&self.pool, limit, lease_seconds).await
    }

    async fn renew(
        &mut self,
        claim_token: Uuid,
        lease_seconds: i64,
    ) -> anyhow::Result<ArtifactGcRenewal> {
        renew_claim(&self.pool, claim_token, lease_seconds).await
    }

    async fn delete_object(
        &mut self,
        object_path: &str,
    ) -> anyhow::Result<ArtifactObjectDeleteOutcome> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("object store is required for --execute"))?;
        Ok(match store.delete_object_key(object_path).await? {
            ObjectDeleteOutcome::Deleted => ArtifactObjectDeleteOutcome::Deleted,
            ObjectDeleteOutcome::Missing => ArtifactObjectDeleteOutcome::Missing,
        })
    }

    async fn complete(
        &mut self,
        claim_token: Uuid,
        candidate: &ArtifactGcCandidate,
        outcome: ArtifactObjectDeleteOutcome,
        detail_limit: i64,
    ) -> anyhow::Result<ArtifactGcCompletion> {
        complete_candidate(&self.pool, claim_token, candidate, outcome, detail_limit).await
    }

    async fn fail(
        &mut self,
        claim_token: Uuid,
        candidate: &ArtifactGcCandidate,
        message: String,
    ) -> anyhow::Result<()> {
        record_failure(&self.pool, claim_token, candidate, message).await
    }

    async fn claim_stale_write_sets(
        &mut self,
        limit: i64,
        lease_seconds: i64,
    ) -> anyhow::Result<ArtifactWriteSetReconcileClaim> {
        claim_stale_write_sets(&self.pool, limit, lease_seconds).await
    }

    async fn complete_stale_write_set(
        &mut self,
        write_set_id: Uuid,
        reconcile_token: Uuid,
    ) -> anyhow::Result<()> {
        complete_stale_write_set(&self.pool, write_set_id, reconcile_token).await
    }
}

async fn preview_candidates(
    pool: &sqlx::PgPool,
    batch_size: i64,
) -> anyhow::Result<ArtifactGcPreview> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.svc_lcia_scope_closure_artifact_gc_preview($1) AS result
        FROM _service_role
        ",
    )
    .bind(i32::try_from(batch_size)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_gc_preview")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("artifact GC preview omitted data"))?;
    Ok(serde_json::from_value(data)?)
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
        SELECT private.svc_lcia_scope_closure_artifact_gc_claim($1, $2) AS result
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

async fn renew_claim(
    pool: &sqlx::PgPool,
    claim_token: Uuid,
    lease_seconds: i64,
) -> anyhow::Result<ArtifactGcRenewal> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.svc_lcia_scope_closure_artifact_gc_renew($1, $2) AS result
        FROM _service_role
        ",
    )
    .bind(claim_token)
    .bind(i32::try_from(lease_seconds)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(&result, "svc_lcia_scope_closure_artifact_gc_renew")?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("artifact GC renewal omitted data"))?;
    Ok(serde_json::from_value(data)?)
}

async fn claim_stale_write_sets(
    pool: &sqlx::PgPool,
    limit: i64,
    lease_seconds: i64,
) -> anyhow::Result<ArtifactWriteSetReconcileClaim> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.svc_lcia_scope_closure_artifact_write_set_reconcile($1, $2) AS result
        FROM _service_role
        ",
    )
    .bind(i32::try_from(limit)?)
    .bind(i32::try_from(lease_seconds)?)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_reconcile",
    )?;
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("artifact write-set reconcile omitted data"))?;
    Ok(serde_json::from_value(data)?)
}

async fn complete_stale_write_set(
    pool: &sqlx::PgPool,
    write_set_id: Uuid,
    reconcile_token: Uuid,
) -> anyhow::Result<()> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.svc_lcia_scope_closure_artifact_write_set_reconcile_complete($1, $2) AS result
        FROM _service_role
        ",
    )
    .bind(write_set_id)
    .bind(reconcile_token)
    .fetch_one(pool)
    .await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_rpc_ok(
        &result,
        "svc_lcia_scope_closure_artifact_write_set_reconcile_complete",
    )
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
        SELECT private.svc_lcia_scope_closure_artifact_gc_complete($1, $2, $3, $4) AS result
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
        SELECT private.svc_lcia_scope_closure_artifact_gc_fail($1, $2, $3) AS result
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
        let cli = Cli::try_parse_from(["artifact-gc"]).unwrap();
        assert!(!cli.execute);
        assert_eq!(cli.batch_size, 100);
        assert_eq!(cli.max_batches, Some(10));
        assert_eq!(cli.lease_seconds, 300);
        assert_eq!(cli.detail_limit, 10_000);
    }
}
