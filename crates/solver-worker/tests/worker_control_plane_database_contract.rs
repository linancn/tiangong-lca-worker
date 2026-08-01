use std::env;

use serde_json::{Value, json};
use solver_worker::{
    pgbouncer_sqlx::{self as sqlx, Executor, PgConnection, PgPool, Postgres, Row, Transaction},
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
const HARNESS_SCHEMA_ENV: &str = "WORKER_CONTROL_PLANE_HARNESS_SCHEMA";
const HARNESS_SENTINEL_ENV: &str = "WORKER_CONTROL_PLANE_HARNESS_SENTINEL";
const SYSTEM_IDENTIFIER_ENV: &str = "WORKER_CONTROL_PLANE_SYSTEM_IDENTIFIER";
const CONTAINER_ID_ENV: &str = "WORKER_CONTROL_PLANE_CONTAINER_ID";
const CONTRACT_JOB_KIND: &str = "lca.result_gc";
const CONTRACT_PAYLOAD_SCHEMA_VERSION: &str = "lca.result_gc.request.v1";

#[derive(Clone)]
struct HarnessIdentity {
    schema: String,
    sentinel: String,
    system_identifier: String,
    container_id: String,
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required by the runner-owned DB harness"))
}

fn harness_identity() -> anyhow::Result<HarnessIdentity> {
    let identity = HarnessIdentity {
        schema: required_env(HARNESS_SCHEMA_ENV),
        sentinel: required_env(HARNESS_SENTINEL_ENV),
        system_identifier: required_env(SYSTEM_IDENTIFIER_ENV),
        container_id: required_env(CONTAINER_ID_ENV),
    };
    anyhow::ensure!(
        identity.schema.strip_prefix("worker_harness_").is_some_and(
            |suffix| suffix.len() == 32 && suffix.chars().all(|ch| ch.is_ascii_hexdigit())
        ),
        "runner-owned sentinel schema is invalid"
    );
    anyhow::ensure!(
        identity.sentinel.len() == 64 && identity.sentinel.chars().all(|ch| ch.is_ascii_hexdigit()),
        "runner-owned sentinel is invalid"
    );
    anyhow::ensure!(
        identity
            .system_identifier
            .chars()
            .all(|ch| ch.is_ascii_digit()),
        "runner-owned PostgreSQL system identifier is invalid"
    );
    anyhow::ensure!(
        identity.container_id.len() == 64
            && identity
                .container_id
                .chars()
                .all(|ch| ch.is_ascii_hexdigit()),
        "runner-owned Docker container ID is invalid"
    );
    Ok(identity)
}

async fn verified_transaction<'a>(
    pool: &'a PgPool,
    identity: &HarnessIdentity,
) -> anyhow::Result<Transaction<'a, Postgres>> {
    let mut transaction = pool.begin().await?;
    let statement = format!(
        r#"
        SELECT current_user = 'service_role'
               AND system_identifier::text = $1
               AND EXISTS (
                   SELECT 1
                   FROM "{}".instance_identity
                   WHERE singleton
                     AND sentinel = $2
                     AND container_id = $3
                     AND system_identifier = $1
               ) AS verified
        FROM pg_control_system()
        "#,
        identity.schema
    );
    let verified: bool = sqlx::query_scalar(&statement)
        .bind(&identity.system_identifier)
        .bind(&identity.sentinel)
        .bind(&identity.container_id)
        .fetch_one(&mut *transaction)
        .await?;
    anyhow::ensure!(
        verified,
        "behavioral write transaction is not bound to the runner-owned database instance"
    );
    Ok(transaction)
}

async fn service_pool(database_url: &str) -> anyhow::Result<PgPool> {
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                connection.execute("SET ROLE service_role").await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await?)
}

async fn enqueue_unchecked_on(
    connection: &mut PgConnection,
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
    .fetch_one(connection)
    .await?;
    Ok(row.try_get("result")?)
}

async fn enqueue_unchecked(
    pool: &PgPool,
    identity: &HarnessIdentity,
    job_kind: &str,
    payload_schema_version: &str,
    idempotency_key: &str,
    concurrency_key: &str,
) -> anyhow::Result<Value> {
    let mut transaction = verified_transaction(pool, identity).await?;
    let result = enqueue_unchecked_on(
        &mut transaction,
        job_kind,
        payload_schema_version,
        idempotency_key,
        concurrency_key,
    )
    .await?;
    transaction.commit().await?;
    Ok(result)
}

async fn enqueue(
    pool: &PgPool,
    identity: &HarnessIdentity,
    job_kind: &str,
    payload_schema_version: &str,
    idempotency_key: &str,
    concurrency_key: &str,
) -> anyhow::Result<Value> {
    let result = enqueue_unchecked(
        pool,
        identity,
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

async fn cancel(pool: &PgPool, identity: &HarnessIdentity, job_id: Uuid) -> anyhow::Result<Value> {
    let mut transaction = verified_transaction(pool, identity).await?;
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
    .fetch_one(&mut *transaction)
    .await?;
    let result = row.try_get("result")?;
    transaction.commit().await?;
    Ok(result)
}

async fn claim(
    pool: &PgPool,
    identity: &HarnessIdentity,
    worker_id: &str,
) -> anyhow::Result<Vec<WorkerJob>> {
    let mut transaction = verified_transaction(pool, identity).await?;
    let jobs = claim_worker_jobs(&mut *transaction, "maintenance", worker_id, 1, 30).await?;
    transaction.commit().await?;
    Ok(jobs)
}

async fn complete(
    pool: &PgPool,
    identity: &HarnessIdentity,
    job: &WorkerJob,
) -> anyhow::Result<()> {
    let mut transaction = verified_transaction(pool, identity).await?;
    record_worker_job_result(
        &mut *transaction,
        job.id,
        job.lease_token,
        WorkerJobResult::completed(
            json!({"contractTest": true, "jobId": job.id}),
            "worker.private-contract-test.result.v1",
        ),
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn preflight(database_url: &str, migration_version: &str) -> anyhow::Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let applied_head: Option<String> =
        sqlx::query_scalar("SELECT max(version) FROM supabase_migrations.schema_migrations")
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(
        applied_head.as_deref() == Some(migration_version),
        "database migration ledger is not at the requested exact head"
    );
    Ok(pool)
}

#[tokio::test]
#[ignore = "requires the runner-owned exact database-engine#365 local instance"]
async fn worker_control_plane_identity_guard_rejects_target_switch_before_mutation()
-> anyhow::Result<()> {
    let database_url = required_env(DATABASE_URL_ENV);
    let identity = harness_identity()?;
    let pool = service_pool(&database_url).await?;
    let run = Uuid::new_v4();
    let key = format!("worker-private-contract:{run}:wrong-target");
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM private.worker_jobs WHERE idempotency_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await?;

    let mut wrong_identity = identity;
    wrong_identity.sentinel = if wrong_identity.sentinel == "0".repeat(64) {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    let attempted = async {
        let mut transaction = verified_transaction(&pool, &wrong_identity).await?;
        enqueue_unchecked_on(
            &mut transaction,
            CONTRACT_JOB_KIND,
            CONTRACT_PAYLOAD_SCHEMA_VERSION,
            &key,
            &format!("{key}:concurrency"),
        )
        .await?;
        transaction.commit().await?;
        anyhow::Ok(())
    }
    .await;
    anyhow::ensure!(
        attempted.is_err(),
        "wrong target sentinel reached the mutation"
    );
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM private.worker_jobs WHERE idempotency_key = $1")
            .bind(&key)
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(
        before == 0 && after == 0,
        "identity failure changed worker jobs"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the runner-owned exact database-engine#365 local instance"]
#[allow(clippy::too_many_lines)]
async fn worker_control_plane_preserves_lifecycle_and_compatibility() -> anyhow::Result<()> {
    let database_url = required_env(DATABASE_URL_ENV);
    let migration_version = required_env(MIGRATION_VERSION_ENV);
    let identity = harness_identity()?;
    anyhow::ensure!(
        migration_version.chars().all(|ch| ch.is_ascii_digit()),
        "{MIGRATION_VERSION_ENV} must be the exact numeric migration head"
    );

    // This owner/admin preflight is metadata only. Every behavioral write below
    // first verifies the runner sentinel and PostgreSQL system identifier in
    // the same service_role transaction that performs the mutation.
    let preflight_pool = preflight(&database_url, &migration_version).await?;
    let observed_system_identifier: String =
        sqlx::query_scalar("SELECT system_identifier::text FROM pg_control_system()")
            .fetch_one(&preflight_pool)
            .await?;
    anyhow::ensure!(observed_system_identifier == identity.system_identifier);

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
    anyhow::ensure!(service_can_select_domain_refs);
    preflight_pool.close().await;

    let pool = service_pool(&database_url).await?;
    let mut identity_transaction = verified_transaction(&pool, &identity).await?;
    let role = sqlx::query(
        r"
        SELECT current_user::text AS current_user,
               session_user::text AS session_user,
               rolsuper,
               pg_has_role(current_user, 'pg_database_owner', 'member') AS database_owner,
               has_schema_privilege(current_user, 'private', 'USAGE') AS private_usage
        FROM pg_roles WHERE rolname = current_user
        ",
    )
    .fetch_one(&mut *identity_transaction)
    .await?;
    anyhow::ensure!(role.try_get::<String, _>("current_user")? == "service_role");
    anyhow::ensure!(!role.try_get::<bool, _>("rolsuper")?);
    anyhow::ensure!(!role.try_get::<bool, _>("database_owner")?);
    anyhow::ensure!(role.try_get::<bool, _>("private_usage")?);
    let session_user: String = role.try_get("session_user")?;
    eprintln!(
        "ACL matrix only: session_user={session_user}, current_user=service_role; this is not deployment-login evidence"
    );
    identity_transaction.commit().await?;

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
    // This is contract-test coverage only; production runtime has no domain-ref view reader.
    let _domain_ref_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM api.worker_job_domain_refs")
            .fetch_one(&pool)
            .await?;

    let job_kind = CONTRACT_JOB_KIND;
    let payload_schema_version = CONTRACT_PAYLOAD_SCHEMA_VERSION;
    let run = Uuid::new_v4();

    let duplicate_key = format!("worker-private-contract:{run}:duplicate");
    let first = enqueue(
        &pool,
        &identity,
        job_kind,
        payload_schema_version,
        &duplicate_key,
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    let replay = enqueue(
        &pool,
        &identity,
        job_kind,
        payload_schema_version,
        &duplicate_key,
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    let duplicate_job_id = result_job_id(&first)?;
    anyhow::ensure!(duplicate_job_id == result_job_id(&replay)?);
    anyhow::ensure!(replay.get("reused").and_then(Value::as_bool) == Some(true));

    let conflict = enqueue_unchecked(
        &pool,
        &identity,
        job_kind,
        payload_schema_version,
        &format!("{duplicate_key}:conflict"),
        &format!("{duplicate_key}:concurrency"),
    )
    .await?;
    anyhow::ensure!(conflict["ok"] == false);
    anyhow::ensure!(conflict.get("code") == Some(&json!("WORKER_JOB_CONCURRENCY_CONFLICT")));
    anyhow::ensure!(cancel(&pool, &identity, duplicate_job_id).await?["ok"] == true);

    let mut queued_ids = Vec::new();
    for suffix in ["a", "b"] {
        let key = format!("worker-private-contract:{run}:claim:{suffix}");
        queued_ids.push(result_job_id(
            &enqueue(
                &pool,
                &identity,
                job_kind,
                payload_schema_version,
                &key,
                &format!("{key}:concurrency"),
            )
            .await?,
        )?);
    }
    let (claimed_a, claimed_b) = tokio::join!(
        claim(&pool, &identity, "contract-worker-a"),
        claim(&pool, &identity, "contract-worker-b")
    );
    let mut claims = [claimed_a?, claimed_b?]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    claims.sort_by_key(|job| job.id);
    queued_ids.sort_unstable();
    anyhow::ensure!(claims.len() == 2);
    anyhow::ensure!(claims.iter().map(|job| job.id).collect::<Vec<_>>() == queued_ids);
    anyhow::ensure!(claims[0].lease_token != claims[1].lease_token);

    complete(&pool, &identity, &claims[0]).await?;
    let stale = claims.remove(1);
    let mut expire_transaction = verified_transaction(&pool, &identity).await?;
    sqlx::query(
        "UPDATE private.worker_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(stale.id)
    .execute(&mut *expire_transaction)
    .await?;
    expire_transaction.commit().await?;

    let reclaimed = claim(&pool, &identity, "contract-worker-restart")
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expired job was not reclaimed"))?;
    anyhow::ensure!(reclaimed.id == stale.id);
    anyhow::ensure!(reclaimed.lease_token != stale.lease_token);
    anyhow::ensure!(reclaimed.attempt_count == stale.attempt_count + 1);

    let mut stale_heartbeat_transaction = verified_transaction(&pool, &identity).await?;
    anyhow::ensure!(
        heartbeat_worker_job(
            &mut *stale_heartbeat_transaction,
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
    stale_heartbeat_transaction.rollback().await?;
    let mut stale_result_transaction = verified_transaction(&pool, &identity).await?;
    anyhow::ensure!(
        record_worker_job_result(
            &mut *stale_result_transaction,
            stale.id,
            stale.lease_token,
            WorkerJobResult::completed(json!({}), "worker.private-contract-test.result.v1"),
        )
        .await
        .is_err()
    );
    stale_result_transaction.rollback().await?;
    complete(&pool, &identity, &reclaimed).await?;

    let artifact_metadata = json!({"contractTest": true, "jobId": reclaimed.id});
    let mut artifact_transaction = verified_transaction(&pool, &identity).await?;
    let artifact_id: Uuid = sqlx::query_scalar(INSERT_MAINTENANCE_ARTIFACT_SQL)
        .bind(reclaimed.id)
        .bind(&artifact_metadata)
        .fetch_one(&mut *artifact_transaction)
        .await?;
    let stored_metadata: Value = sqlx::query_scalar(
        "SELECT metadata FROM private.worker_job_artifacts WHERE id = $1 AND job_id = $2",
    )
    .bind(artifact_id)
    .bind(reclaimed.id)
    .fetch_one(&mut *artifact_transaction)
    .await?;
    anyhow::ensure!(stored_metadata == artifact_metadata);
    artifact_transaction.commit().await?;

    let rollback_key = format!("worker-private-contract:{run}:rollback");
    let mut rollback_transaction = verified_transaction(&pool, &identity).await?;
    let rollback_result = enqueue_unchecked_on(
        &mut rollback_transaction,
        job_kind,
        payload_schema_version,
        &rollback_key,
        &format!("{rollback_key}:concurrency"),
    )
    .await?;
    let rolled_back_id = result_job_id(&rollback_result)?;
    rollback_transaction.rollback().await?;
    let rollback_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM private.worker_jobs WHERE id = $1")
            .bind(rolled_back_id)
            .fetch_one(&pool)
            .await?;
    anyhow::ensure!(rollback_count == 0);

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
    .bind(reclaimed.id)
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
    .bind(reclaimed.id)
    .fetch_one(&pool)
    .await?
    .try_get("result")?;
    anyhow::ensure!(public_payload["ok"] == true);
    anyhow::ensure!(!public_payload.to_string().contains("private."));
    Ok(())
}
