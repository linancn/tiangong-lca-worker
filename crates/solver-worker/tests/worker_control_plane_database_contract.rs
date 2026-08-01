use std::env;

use serde_json::{Value, json};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Executor, PgPool, Row},
    worker_control_plane::{
        INSERT_MAINTENANCE_ARTIFACT_SQL, WORKER_JOB_ARTIFACTS_TABLE, WORKER_JOB_DOMAIN_REFS_TABLE,
        WORKER_JOB_EVENTS_TABLE, WORKER_JOB_KINDS_TABLE, WORKER_JOBS_TABLE,
    },
    worker_jobs::{
        WorkerJob, WorkerJobResult, claim_worker_jobs, heartbeat_worker_job,
        record_worker_job_result,
    },
};
use uuid::Uuid;

const DATABASE_URL_ENV: &str = "WORKER_CONTROL_PLANE_DATABASE_URL";
const MIGRATION_VERSION_ENV: &str = "WORKER_CONTROL_PLANE_MIGRATION_VERSION";
const CONTRACT_JOB_KIND: &str = "lca.result_gc";
const CONTRACT_PAYLOAD_SCHEMA_VERSION: &str = "lca.result_gc.request.v1";

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required by the isolated DB harness"))
}

async fn enqueue_unchecked(
    pool: &PgPool,
    job_kind: &str,
    payload_schema_version: &str,
    idempotency_key: &str,
    concurrency_key: &str,
) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT api.worker_enqueue_job_v1(
            p_job_kind => $1,
            p_payload_json => $2::jsonb,
            p_payload_schema_version => $3,
            p_requester_type => 'operator',
            p_idempotency_key => $4,
            p_concurrency_key => $5,
            p_queue_key => 'worker-private-contract-test',
            p_visibility => 'operator',
            p_max_attempts => 3
        ) AS result
        FROM _service_role
        ",
    )
    .bind(job_kind)
    .bind(json!({"dryRun": true, "contractTest": true}))
    .bind(payload_schema_version)
    .bind(idempotency_key)
    .bind(concurrency_key)
    .fetch_one(pool)
    .await?;
    let result: Value = row.try_get("result")?;
    Ok(result)
}

async fn enqueue(
    pool: &PgPool,
    job_kind: &str,
    payload_schema_version: &str,
    idempotency_key: &str,
    concurrency_key: &str,
) -> anyhow::Result<Value> {
    let result = enqueue_unchecked(
        pool,
        job_kind,
        payload_schema_version,
        idempotency_key,
        concurrency_key,
    )
    .await?;
    anyhow::ensure!(
        result.get("ok").and_then(Value::as_bool) == Some(true),
        "worker_enqueue_job_v1 returned non-ok result: {result}"
    );
    Ok(result)
}

fn result_job_id(result: &Value) -> anyhow::Result<Uuid> {
    result
        .pointer("/data/id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("enqueue result omitted logical job id"))?
        .parse()
        .map_err(Into::into)
}

async fn cancel(pool: &PgPool, job_id: Uuid) -> anyhow::Result<Value> {
    let row = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.worker_cancel_job($1, NULL, 'isolated contract test cleanup') AS result
        FROM _service_role
        ",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("result")?)
}

async fn cancel_prior_harness_jobs(pool: &PgPool) -> anyhow::Result<()> {
    let job_ids: Vec<Uuid> = sqlx::query_scalar(
        r"
        SELECT id
        FROM private.worker_jobs
        WHERE idempotency_key LIKE 'worker-private-contract:%'
          AND status NOT IN ('completed', 'failed', 'cancelled')
        ORDER BY created_at, id
        ",
    )
    .fetch_all(pool)
    .await?;
    for job_id in job_ids {
        let result = cancel(pool, job_id).await?;
        anyhow::ensure!(
            result["ok"] == true,
            "prior harness job cleanup failed: {result}"
        );
    }
    Ok(())
}

async fn complete(pool: &PgPool, job: &WorkerJob) -> anyhow::Result<()> {
    record_worker_job_result(
        pool,
        job.id,
        job.lease_token,
        WorkerJobResult::completed(
            json!({"contractTest": true, "jobId": job.id}),
            "worker.private-contract-test.result.v1",
        ),
    )
    .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated exact database-engine#335 head and SET ROLE service_role ACL matrix"]
#[allow(clippy::too_many_lines)]
async fn private_worker_control_plane_preserves_lifecycle_and_compatibility() -> anyhow::Result<()>
{
    let database_url = required_env(DATABASE_URL_ENV);
    let migration_version = required_env(MIGRATION_VERSION_ENV);
    anyhow::ensure!(
        migration_version.chars().all(|ch| ch.is_ascii_digit()),
        "{MIGRATION_VERSION_ENV} must be the exact numeric migration head"
    );

    // Migration provenance is checked before SET ROLE. This owner/admin read is
    // metadata preflight only; all behavioral assertions below run as
    // service_role and must not be reported as deployment-login evidence.
    let preflight_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    let applied_head: Option<String> =
        sqlx::query_scalar("SELECT max(version) FROM supabase_migrations.schema_migrations")
            .fetch_one(&preflight_pool)
            .await?;
    anyhow::ensure!(
        applied_head.as_deref() == Some(migration_version.as_str()),
        "database migration ledger is not at the requested exact head"
    );

    for signature in [
        "api.worker_enqueue_job_v1(text,jsonb,text,text,uuid,text,uuid,text,uuid,text,text,text,integer,text,timestamp with time zone,text,integer,timestamp with time zone,jsonb,uuid,uuid)",
        "api.worker_claim_jobs_v1(text,text,integer,integer)",
        "api.worker_heartbeat_job_v1(uuid,uuid,text,numeric,jsonb,integer)",
        "api.worker_record_job_result_v1(uuid,uuid,text,jsonb,text,jsonb,jsonb,text,text,jsonb,text[],text,boolean)",
    ] {
        let service_can_execute: bool =
            sqlx::query_scalar("SELECT has_function_privilege('service_role', $1, 'EXECUTE')")
                .bind(signature)
                .fetch_one(&preflight_pool)
                .await?;
        let anon_can_execute: bool =
            sqlx::query_scalar("SELECT has_function_privilege('anon', $1, 'EXECUTE')")
                .bind(signature)
                .fetch_one(&preflight_pool)
                .await?;
        let authenticated_can_execute: bool =
            sqlx::query_scalar("SELECT has_function_privilege('authenticated', $1, 'EXECUTE')")
                .bind(signature)
                .fetch_one(&preflight_pool)
                .await?;
        anyhow::ensure!(
            service_can_execute,
            "service_role cannot execute {signature}"
        );
        anyhow::ensure!(!anon_can_execute, "anon can execute {signature}");
        anyhow::ensure!(
            !authenticated_can_execute,
            "authenticated can execute {signature}"
        );
    }
    for role in ["anon", "authenticated"] {
        let can_select: bool = sqlx::query_scalar(
            "SELECT has_table_privilege($1, 'api.worker_job_domain_refs', 'SELECT')",
        )
        .bind(role)
        .fetch_one(&preflight_pool)
        .await?;
        anyhow::ensure!(!can_select, "{role} can select api.worker_job_domain_refs");
    }
    let service_can_select_domain_refs: bool = sqlx::query_scalar(
        "SELECT has_table_privilege('service_role', 'api.worker_job_domain_refs', 'SELECT')",
    )
    .fetch_one(&preflight_pool)
    .await?;
    anyhow::ensure!(
        service_can_select_domain_refs,
        "service_role cannot select api.worker_job_domain_refs"
    );
    preflight_pool.close().await;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                connection.execute("SET ROLE service_role").await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await?;

    let identity = sqlx::query(
        r"
        SELECT current_user::text AS current_user,
               session_user::text AS session_user,
               rolsuper,
               pg_has_role(current_user, 'pg_database_owner', 'member') AS database_owner,
               has_schema_privilege(current_user, 'private', 'USAGE') AS private_usage
        FROM pg_roles
        WHERE rolname = current_user
        ",
    )
    .fetch_one(&pool)
    .await?;
    let current_user: String = identity.try_get("current_user")?;
    anyhow::ensure!(
        current_user == "service_role",
        "SET ROLE service_role did not take effect"
    );
    anyhow::ensure!(
        !identity.try_get::<bool, _>("rolsuper")?,
        "superuser is not valid proof"
    );
    anyhow::ensure!(
        !identity.try_get::<bool, _>("database_owner")?,
        "database owner is not valid proof"
    );
    anyhow::ensure!(identity.try_get::<bool, _>("private_usage")?);
    let session_user: String = identity.try_get("session_user")?;
    eprintln!(
        "ACL matrix only: session_user={session_user}, current_user={current_user}; this is not deployment-login evidence"
    );

    for relation in [
        WORKER_JOBS_TABLE,
        WORKER_JOB_EVENTS_TABLE,
        WORKER_JOB_ARTIFACTS_TABLE,
        WORKER_JOB_KINDS_TABLE,
        WORKER_JOB_DOMAIN_REFS_TABLE,
    ] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation)
            .fetch_one(&pool)
            .await?;
        anyhow::ensure!(exists, "required relation is absent: {relation}");
    }
    let _domain_ref_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM api.worker_job_domain_refs")
            .fetch_one(&pool)
            .await?;
    cancel_prior_harness_jobs(&pool).await?;

    let job_kind = CONTRACT_JOB_KIND;
    let payload_schema_version = CONTRACT_PAYLOAD_SCHEMA_VERSION;
    let run = Uuid::new_v4();

    // Lost enqueue response and duplicate enqueue converge on one logical job.
    let duplicate_key = format!("worker-private-contract:{run}:duplicate");
    let first = enqueue(
        &pool,
        job_kind,
        payload_schema_version,
        &duplicate_key,
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    let replay = enqueue(
        &pool,
        job_kind,
        payload_schema_version,
        &duplicate_key,
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    let duplicate_job_id = result_job_id(&first)?;
    anyhow::ensure!(duplicate_job_id == result_job_id(&replay)?);
    anyhow::ensure!(replay.get("reused").and_then(Value::as_bool) == Some(true));

    // A different logical request cannot occupy the same active concurrency key.
    let conflict = enqueue_unchecked(
        &pool,
        job_kind,
        payload_schema_version,
        &format!("{duplicate_key}:conflict"),
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    anyhow::ensure!(conflict["ok"] == false);
    anyhow::ensure!(
        conflict.get("code") == Some(&json!("WORKER_JOB_CONCURRENCY_CONFLICT")),
        "concurrency conflict changed public error semantics: {conflict}"
    );
    anyhow::ensure!(cancel(&pool, duplicate_job_id).await?["ok"] == true);

    // Two worker processes claim two rows exactly once.
    let mut queued_ids = Vec::new();
    for suffix in ["a", "b"] {
        let key = format!("worker-private-contract:{run}:claim:{suffix}");
        queued_ids.push(result_job_id(
            &enqueue(
                &pool,
                job_kind,
                payload_schema_version,
                &key,
                &format!("{key}:concurrency"),
            )
            .await?,
        )?);
    }
    let (claimed_a, claimed_b) = tokio::join!(
        claim_worker_jobs(&pool, "maintenance", "contract-worker-a", 1, 30),
        claim_worker_jobs(&pool, "maintenance", "contract-worker-b", 1, 30)
    );
    let mut claims = [claimed_a?, claimed_b?]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    claims.sort_by_key(|job| job.id);
    queued_ids.sort_unstable();
    anyhow::ensure!(claims.len() == 2);
    let claimed_ids = claims.iter().map(|job| job.id).collect::<Vec<_>>();
    anyhow::ensure!(
        claimed_ids == queued_ids,
        "concurrent claim mismatch: expected {queued_ids:?}, got {claimed_ids:?}"
    );
    anyhow::ensure!(claims[0].lease_token != claims[1].lease_token);

    complete(&pool, &claims[0]).await?;

    // Restart/retry reclaims an expired lease with a fresh fence. The stale
    // worker cannot heartbeat or commit after the reclaim.
    let stale = claims.remove(1);
    sqlx::query(
        "UPDATE private.worker_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(stale.id)
    .execute(&pool)
    .await?;
    let reclaimed = claim_worker_jobs(&pool, "maintenance", "contract-worker-restart", 1, 30)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expired job was not reclaimed"))?;
    anyhow::ensure!(reclaimed.id == stale.id);
    anyhow::ensure!(reclaimed.lease_token != stale.lease_token);
    anyhow::ensure!(reclaimed.attempt_count == stale.attempt_count + 1);
    anyhow::ensure!(
        heartbeat_worker_job(
            &pool,
            stale.id,
            stale.lease_token,
            "stale-worker",
            0.5,
            None,
            30,
        )
        .await
        .is_err()
    );
    anyhow::ensure!(
        record_worker_job_result(
            &pool,
            stale.id,
            stale.lease_token,
            WorkerJobResult::completed(json!({}), "worker.private-contract-test.result.v1"),
        )
        .await
        .is_err()
    );
    complete(&pool, &reclaimed).await?;

    // Production maintenance workers can write report metadata and read it
    // back through the private Expand relation, without broader artifact DML.
    let artifact_metadata = json!({"contractTest": true, "jobId": reclaimed.id});
    let artifact_id: Uuid = sqlx::query_scalar(INSERT_MAINTENANCE_ARTIFACT_SQL)
        .bind(reclaimed.id)
        .bind(&artifact_metadata)
        .fetch_one(&pool)
        .await?;
    let stored_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM private.worker_job_artifacts WHERE id = $1 AND job_id = $2",
    )
    .bind(artifact_id)
    .bind(reclaimed.id)
    .fetch_one(&pool)
    .await?;
    anyhow::ensure!(stored_metadata == artifact_metadata);

    // A rolled-back enqueue leaves neither a job nor its event behind.
    let rollback_key = format!("worker-private-contract:{run}:rollback");
    let mut transaction = pool.begin().await?;
    let rollback_result: Value = sqlx::query(
        r#"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT api.worker_enqueue_job_v1(
            p_job_kind => $1,
            p_payload_json => '{"contractTest":true}'::jsonb,
            p_payload_schema_version => $2,
            p_requester_type => 'operator',
            p_idempotency_key => $3,
            p_concurrency_key => $4,
            p_queue_key => 'worker-private-contract-test',
            p_visibility => 'operator'
        ) AS result
        FROM _service_role
        "#,
    )
    .bind(job_kind)
    .bind(payload_schema_version)
    .bind(&rollback_key)
    .bind(format!("{rollback_key}:concurrency"))
    .fetch_one(&mut *transaction)
    .await?
    .try_get("result")?;
    let rolled_back_id = result_job_id(&rollback_result)?;
    transaction.rollback().await?;
    let rollback_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM private.worker_jobs WHERE id = $1")
            .bind(rolled_back_id)
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(rollback_count == 0);

    // Expand-window compatibility and private storage expose the same logical
    // row without leaking the physical schema into the public payload.
    let parity_id = reclaimed.id;
    let parity = sqlx::query(
        r"
        SELECT old.id = new.id AS same_id,
               old.status = new.status AS same_status,
               old.job_kind = new.job_kind AS same_kind,
               old.payload_json = new.payload_json AS same_payload
        FROM public.worker_jobs AS old
        JOIN private.worker_jobs AS new ON new.id = old.id
        WHERE old.id = $1
        ",
    )
    .bind(parity_id)
    .fetch_one(&pool)
    .await?;
    for column in ["same_id", "same_status", "same_kind", "same_payload"] {
        anyhow::ensure!(parity.try_get::<bool, _>(column)?);
    }
    let public_payload: Value = sqlx::query(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.worker_read_job($1, true) AS result
        FROM _service_role
        ",
    )
    .bind(parity_id)
    .fetch_one(&pool)
    .await?
    .try_get("result")?;
    anyhow::ensure!(public_payload["ok"] == true);
    anyhow::ensure!(!public_payload.to_string().contains("private."));

    Ok(())
}
