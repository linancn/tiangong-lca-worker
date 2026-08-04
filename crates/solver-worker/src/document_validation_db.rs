use std::{fmt, str::FromStr};

use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    build_metadata::{DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE, SOURCE_COMMIT},
    config::AppConfig,
    db_pool::{APP_DOCUMENT_VALIDATION_EVIDENCE, WorkerDbPoolOptions},
    pgbouncer_sqlx::{self as sqlx, PgPool, Row, postgres::PgConnectOptions},
};

const CONTRACT_SOURCE_MIGRATION: &str = "20260803163000";
const LOOKUP_ROUTINE: &str = "private.svc_lcia_document_validation_evidence_lookup";
const RECORD_ROUTINE: &str = "private.svc_lcia_document_validation_evidence_record";

const PREFLIGHT_SQL: &str = r"
WITH login_role AS (
    SELECT role_row.oid,
           role_row.rolcanlogin
             AND role_row.rolinherit
             AND NOT role_row.rolsuper
             AND NOT role_row.rolcreatedb
             AND NOT role_row.rolcreaterole
             AND NOT role_row.rolbypassrls
             AND NOT role_row.rolreplication
             AND role_row.rolconfig IS NULL AS safe
    FROM pg_catalog.pg_roles role_row
    WHERE role_row.rolname = CURRENT_USER
), runtime_role AS (
    SELECT NOT role_row.rolcanlogin
             AND role_row.rolinherit
             AND NOT role_row.rolsuper
             AND NOT role_row.rolcreatedb
             AND NOT role_row.rolcreaterole
             AND NOT role_row.rolbypassrls
             AND NOT role_row.rolreplication
             AND role_row.rolconfig IS NULL AS safe
    FROM pg_catalog.pg_roles role_row
    WHERE role_row.rolname = 'lca_worker_runtime'
), membership AS (
    SELECT count(*) AS total_count,
           count(*) FILTER (
             WHERE granted_role.rolname = 'lca_worker_runtime'
               AND grantor.rolname = 'postgres'
               AND membership.inherit_option
               AND NOT membership.set_option
               AND NOT membership.admin_option
           ) AS exact_count
    FROM login_role
    LEFT JOIN pg_catalog.pg_auth_members membership
      ON membership.member = login_role.oid
    LEFT JOIN pg_catalog.pg_roles granted_role
      ON granted_role.oid = membership.roleid
    LEFT JOIN pg_catalog.pg_roles grantor
      ON grantor.oid = membership.grantor
    WHERE membership.member IS NOT NULL
), routine_contract AS (
    SELECT count(*) AS exact_count
    FROM pg_catalog.pg_proc procedure
    JOIN pg_catalog.pg_namespace namespace
      ON namespace.oid = procedure.pronamespace
    JOIN pg_catalog.pg_language language
      ON language.oid = procedure.prolang
    WHERE namespace.nspname = 'private'
      AND procedure.proowner = 'postgres'::pg_catalog.regrole
      AND language.lanname = 'plpgsql'
      AND procedure.prokind = 'f'
      AND procedure.prorettype = 'pg_catalog.jsonb'::pg_catalog.regtype
      AND NOT procedure.proretset
      AND procedure.provolatile = 'v'
      AND procedure.proparallel = 'u'
      AND NOT procedure.proisstrict
      AND NOT procedure.proleakproof
      AND procedure.prosecdef
      AND procedure.proconfig = ARRAY['search_path=pg_catalog, pg_temp']::text[]
      AND procedure.proacl::text = '{postgres=X/postgres,lca_worker_runtime=X/postgres}'
      AND (
        (procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
          AND pg_catalog.pg_get_function_arguments(procedure.oid) = 'p_cache_keys jsonb'
          AND pg_catalog.md5(procedure.prosrc) = 'bd277cd343a10462fc536a64390459c5'
          AND pg_catalog.obj_description(procedure.oid, 'pg_proc') =
            'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.')
        OR
        (procedure.proname = 'svc_lcia_document_validation_evidence_record'
          AND pg_catalog.pg_get_function_arguments(procedure.oid) =
              'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid'
          AND pg_catalog.md5(procedure.prosrc) = '2759f5215c8dd4b253db2ed2264cc8ab'
          AND pg_catalog.obj_description(procedure.oid, 'pg_proc') =
            'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.')
      )
), access_contract AS (
    SELECT
      pg_catalog.has_function_privilege(
        CURRENT_USER,
        'private.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
      )
      AND pg_catalog.has_function_privilege(
        CURRENT_USER,
        'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)', 'EXECUTE'
      )
      AND NOT pg_catalog.has_function_privilege(
        CURRENT_USER,
        'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
      )
      AND NOT pg_catalog.has_function_privilege(
        CURRENT_USER,
        'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)', 'EXECUTE'
      )
      AND NOT pg_catalog.has_table_privilege(
        CURRENT_USER, 'public.lcia_document_validation_evidence',
        'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
      ) AS safe
), private_execute_contract AS (
    SELECT COALESCE(
      array_agg(procedure.oid::pg_catalog.regprocedure::text ORDER BY procedure.oid::pg_catalog.regprocedure::text),
      ARRAY[]::text[]
    ) = ARRAY[
      'private.svc_lcia_document_validation_evidence_lookup(jsonb)',
      'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)',
      'private.worker_lca_result_gc_attest_v1(uuid)',
      'private.worker_lca_result_gc_claim_v1(text,integer,integer)',
      'private.worker_lca_result_gc_fail_v1(uuid,uuid,text)',
      'private.worker_lca_result_gc_fence_v1(uuid,uuid)',
      'private.worker_lca_result_gc_finalize_v1(uuid,uuid,text)',
      'private.worker_lca_result_gc_preview_v1(integer)',
      'private.worker_lca_result_gc_renew_v1(uuid,uuid,integer)'
    ]::text[] AS safe
    FROM pg_catalog.pg_proc procedure
    JOIN pg_catalog.pg_namespace namespace
      ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = 'private'
      AND pg_catalog.has_function_privilege(CURRENT_USER, procedure.oid, 'EXECUTE')
), ownership_contract AS (
    SELECT
      NOT pg_catalog.has_database_privilege(
        CURRENT_USER, pg_catalog.current_database(), 'CREATE'
      )
      AND NOT pg_catalog.has_schema_privilege(CURRENT_USER, 'private', 'CREATE')
      AND NOT pg_catalog.has_schema_privilege(CURRENT_USER, 'public', 'CREATE')
      AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace namespace
        WHERE namespace.nspowner = CURRENT_USER::pg_catalog.regrole
          AND namespace.nspname !~ '^pg_(temp|toast_temp)_[0-9]+$'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE relation.relowner = CURRENT_USER::pg_catalog.regrole
          AND namespace.nspname !~ '^pg_(temp|toast_temp)_[0-9]+$'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc procedure
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
        WHERE procedure.proowner = CURRENT_USER::pg_catalog.regrole
          AND namespace.nspname !~ '^pg_(temp|toast_temp)_[0-9]+$'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_type type_row
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = type_row.typnamespace
        WHERE type_row.typowner = CURRENT_USER::pg_catalog.regrole
          AND namespace.nspname !~ '^pg_(temp|toast_temp)_[0-9]+$'
      ) AS safe
)
SELECT SESSION_USER = CURRENT_USER AS session_identity_safe,
       pg_catalog.current_setting('server_version_num')::integer >= 170000
         AND pg_catalog.current_setting('server_version_num')::integer < 180000 AS pg17_safe,
       COALESCE((SELECT safe FROM login_role), false) AS login_safe,
       COALESCE((SELECT safe FROM runtime_role), false) AS runtime_role_safe,
       COALESCE((SELECT total_count = 1 AND exact_count = 1 FROM membership), false)
         AS membership_safe,
       NOT pg_catalog.pg_has_role(SESSION_USER, 'service_role', 'member')
         AS service_role_excluded,
       COALESCE((SELECT exact_count = 2 FROM routine_contract), false)
         AND (
           SELECT count(*) = 2
           FROM pg_catalog.pg_proc procedure
           JOIN pg_catalog.pg_namespace namespace
             ON namespace.oid = procedure.pronamespace
           WHERE namespace.nspname = 'private'
             AND procedure.proname IN (
               'svc_lcia_document_validation_evidence_lookup',
               'svc_lcia_document_validation_evidence_record'
             )
         ) AS routine_contract_safe,
       COALESCE((SELECT safe FROM access_contract), false) AS access_safe,
       COALESCE((SELECT safe FROM private_execute_contract), false)
         AS private_execute_contract_safe,
       COALESCE((SELECT safe FROM ownership_contract), false) AS ownership_safe
";

const LOOKUP_SQL: &str =
    "SELECT private.svc_lcia_document_validation_evidence_lookup($1::jsonb) AS result";
const RECORD_SQL: &str =
    "SELECT private.svc_lcia_document_validation_evidence_record($1::jsonb, $2) AS result";

#[derive(Clone)]
pub struct DocumentValidationDb {
    pool: PgPool,
}

impl fmt::Debug for DocumentValidationDb {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DocumentValidationDb")
            .field("contract", &"document-validation-evidence.private.v1")
            .finish_non_exhaustive()
    }
}

impl DocumentValidationDb {
    pub async fn connect(config: &AppConfig) -> anyhow::Result<Self> {
        let database_url = config.resolved_document_validation_database_url()?;
        validate_database_url(database_url)?;
        let pool = WorkerDbPoolOptions::new(APP_DOCUMENT_VALIDATION_EVIDENCE)
            .max_connections(config.document_validation_db_max_connections())
            .min_connections(config.document_validation_db_min_connections())
            .acquire_timeout(config.document_validation_db_acquire_timeout())
            .connect(database_url)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "document-validation database connection failed (identity and target omitted)"
                )
            })?;

        if let Err(error) = preflight(&pool).await {
            pool.close().await;
            return Err(error);
        }
        tracing::info!(
            contract = "document-validation-evidence.private.v1",
            contract_source_migration = CONTRACT_SOURCE_MIGRATION,
            worker_source_commit = SOURCE_COMMIT,
            pg_major = 17,
            "{}",
            DOCUMENT_VALIDATION_DATABASE_CONTRACT_ACCEPTED_MESSAGE
        );
        Ok(Self { pool })
    }

    pub async fn lookup(&self, keys: &[Value]) -> anyhow::Result<Vec<Value>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let result = call_lookup(&self.pool, keys).await?;
        lookup_success_rows(&result)
    }

    pub async fn record(&self, worker_job_id: Uuid, records: &[Value]) -> anyhow::Result<()> {
        let result = call_record(&self.pool, worker_job_id, records).await?;
        ensure_record_success(&result, records.len())
    }
}

fn validate_database_url(database_url: &str) -> anyhow::Result<()> {
    let valid_scheme =
        database_url.starts_with("postgres://") || database_url.starts_with("postgresql://");
    if !valid_scheme || database_url.chars().any(char::is_control) {
        anyhow::bail!(
            "DOCUMENT_VALIDATION_DATABASE_URL must be an explicit PostgreSQL URL (value omitted)"
        );
    }
    let options = PgConnectOptions::from_str(database_url).map_err(|_| {
        anyhow::anyhow!(
            "DOCUMENT_VALIDATION_DATABASE_URL is not a valid PostgreSQL URL (value omitted)"
        )
    })?;
    if options.get_username().is_empty()
        || options.get_host().is_empty()
        || options.get_database().is_none_or(str::is_empty)
    {
        anyhow::bail!(
            "DOCUMENT_VALIDATION_DATABASE_URL must include login, host, and database (value omitted)"
        );
    }
    Ok(())
}

async fn preflight(pool: &PgPool) -> anyhow::Result<()> {
    let row = sqlx::query(PREFLIGHT_SQL)
        .fetch_one(pool)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "document-validation database preflight query failed (identity and target omitted)"
            )
        })?;
    for field in [
        "session_identity_safe",
        "pg17_safe",
        "login_safe",
        "runtime_role_safe",
        "membership_safe",
        "service_role_excluded",
        "routine_contract_safe",
        "access_safe",
        "private_execute_contract_safe",
        "ownership_safe",
    ] {
        if row.try_get::<bool, _>(field).unwrap_or(false) {
            continue;
        }
        anyhow::bail!(
            "document-validation database preflight rejected contract check {field} (expected Database #409 contract from migration {CONTRACT_SOURCE_MIGRATION}; identity and target omitted)"
        );
    }

    let lookup = call_lookup(pool, &[]).await?;
    ensure_rpc_ok(&lookup, LOOKUP_ROUTINE)?;
    if lookup.get("data") != Some(&json!([])) {
        anyhow::bail!("document-validation empty lookup envelope drifted")
    }
    let record = call_record(pool, Uuid::nil(), &[]).await?;
    ensure_rpc_ok(&record, RECORD_ROUTINE)?;
    if record.pointer("/data/insertedCount") != Some(&json!(0)) {
        anyhow::bail!("document-validation empty record envelope drifted")
    }
    Ok(())
}

async fn call_lookup(pool: &PgPool, keys: &[Value]) -> anyhow::Result<Value> {
    let row = sqlx::query(LOOKUP_SQL)
        .bind(serde_json::to_value(keys)?)
        .fetch_one(pool)
        .await
        .map_err(|_| rpc_transport_error("lookup"))?;
    row.try_get("result")
        .map_err(|_| rpc_transport_error("lookup"))
}

async fn call_record(
    pool: &PgPool,
    worker_job_id: Uuid,
    records: &[Value],
) -> anyhow::Result<Value> {
    let row = sqlx::query(RECORD_SQL)
        .bind(serde_json::to_value(records)?)
        .bind(worker_job_id)
        .fetch_one(pool)
        .await
        .map_err(|_| rpc_transport_error("record"))?;
    row.try_get("result")
        .map_err(|_| rpc_transport_error("record"))
}

fn ensure_rpc_ok(result: &Value, routine: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let code = result
        .pointer("/error/code")
        .and_then(Value::as_str)
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 64
                && code.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'_' | b'-')
                })
        })
        .unwrap_or("unknown");
    let status = result
        .pointer("/error/status")
        .and_then(Value::as_i64)
        .unwrap_or(500);
    anyhow::bail!("{routine} returned non-ok result: code={code} status={status}")
}

fn success_data<'a>(result: &'a Value, routine: &str) -> anyhow::Result<&'a Value> {
    ensure_rpc_ok(result, routine)?;
    let envelope = result.as_object().filter(|value| value.len() == 2);
    envelope
        .filter(|value| value.get("ok") == Some(&Value::Bool(true)))
        .and_then(|value| value.get("data"))
        .ok_or_else(|| {
            anyhow::anyhow!("{routine} returned malformed success envelope (payload omitted)")
        })
}

fn lookup_success_rows(result: &Value) -> anyhow::Result<Vec<Value>> {
    success_data(result, LOOKUP_ROUTINE)?
        .as_array()
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{LOOKUP_ROUTINE} returned malformed success envelope (payload omitted)"
            )
        })
}

fn ensure_record_success(result: &Value, input_count: usize) -> anyhow::Result<()> {
    let data = success_data(result, RECORD_ROUTINE)?;
    let valid = data
        .as_object()
        .filter(|value| value.len() == 1)
        .is_some_and(|value| {
            value
                .get("insertedCount")
                .and_then(Value::as_u64)
                .and_then(|inserted| usize::try_from(inserted).ok())
                .is_some_and(|inserted| inserted <= input_count)
        });
    if !valid {
        anyhow::bail!("{RECORD_ROUTINE} returned malformed success envelope (payload omitted)");
    }
    Ok(())
}

fn rpc_transport_error(operation: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "document-validation evidence {operation} transport failed (identity, target, and payload omitted)"
    )
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use clap::Parser;
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        DocumentValidationDb, LOOKUP_SQL, PREFLIGHT_SQL, RECORD_SQL, ensure_record_success,
        ensure_rpc_ok, lookup_success_rows, success_data, validate_database_url,
    };
    use crate::config::AppConfig;

    #[test]
    fn dedicated_sql_uses_only_database_409_private_routines() {
        assert!(LOOKUP_SQL.contains("private.svc_lcia_document_validation_evidence_lookup"));
        assert!(RECORD_SQL.contains("private.svc_lcia_document_validation_evidence_record"));
        for sql in [LOOKUP_SQL, RECORD_SQL] {
            assert!(!sql.contains("public.svc_lcia_document_validation_evidence"));
            assert!(!sql.contains("request.jwt.claim.role"));
            assert!(!sql.contains("set_config"));
        }
    }

    #[test]
    fn preflight_pins_restricted_identity_and_exact_routine_fingerprints() {
        for expected in [
            "SESSION_USER = CURRENT_USER",
            "server_version_num",
            "NOT role_row.rolsuper",
            "NOT role_row.rolcreatedb",
            "NOT role_row.rolcreaterole",
            "NOT role_row.rolbypassrls",
            "NOT role_row.rolreplication",
            "role_row.rolconfig IS NULL",
            "membership.set_option",
            "membership.admin_option",
            "service_role",
            "has_database_privilege",
            "private_execute_contract",
            "worker_lca_result_gc_claim_v1(text,integer,integer)",
            "has_schema_privilege(CURRENT_USER, 'private', 'CREATE')",
            "namespace.nspowner = CURRENT_USER",
            "relation.relowner = CURRENT_USER",
            "procedure.proowner = CURRENT_USER",
            "type_row.typowner = CURRENT_USER",
            "bd277cd343a10462fc536a64390459c5",
            "2759f5215c8dd4b253db2ed2264cc8ab",
            "SELECT count(*) = 2",
        ] {
            assert!(PREFLIGHT_SQL.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn url_validation_is_explicit_and_secret_free() {
        validate_database_url("postgresql://worker:secret@db.example.test/app").unwrap();
        let secret = "do-not-leak-this-password";
        let error = validate_database_url(&format!("https://worker:{secret}@db.example.test/app"))
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret));
        assert!(!error.contains("db.example.test"));
    }

    #[test]
    fn non_ok_error_preserves_bounded_envelope_without_payload() {
        let result = json!({
            "ok": false,
            "error": {"code": "invalid_document_evidence_keys", "status": 400},
            "payload": "sensitive-document-value"
        });
        let error = ensure_rpc_ok(&result, "private.lookup")
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid_document_evidence_keys"));
        assert!(error.contains("status=400"));
        assert!(!error.contains("sensitive-document-value"));

        let hostile_code = "sensitive-document-value".repeat(8);
        let hostile_result = json!({
            "ok": false,
            "error": {"code": hostile_code, "status": 500}
        });
        let hostile_error = ensure_rpc_ok(&hostile_result, "private.lookup")
            .unwrap_err()
            .to_string();
        assert!(hostile_error.contains("code=unknown"));
        assert!(!hostile_error.contains("sensitive-document-value"));
    }

    #[test]
    fn success_envelope_shape_is_exact_and_payload_free_on_error() {
        assert_eq!(
            success_data(&json!({"ok": true, "data": []}), "private.lookup").unwrap(),
            &json!([])
        );
        for malformed in [
            json!({"ok": true}),
            json!({"ok": true, "data": [], "extra": "sensitive-document-value"}),
            json!({"ok": "true", "data": []}),
        ] {
            let error = success_data(&malformed, "private.lookup")
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("malformed success envelope")
                    || error.contains("returned non-ok result")
            );
            assert!(!error.contains("sensitive-document-value"));
        }

        assert_eq!(
            lookup_success_rows(&json!({"ok": true, "data": [{"id": 1}]})).unwrap(),
            vec![json!({"id": 1})]
        );
        assert!(lookup_success_rows(&json!({"ok": true, "data": {}})).is_err());

        ensure_record_success(&json!({"ok": true, "data": {"insertedCount": 1}}), 1).unwrap();
        for malformed in [
            json!({"ok": true, "data": {}}),
            json!({"ok": true, "data": {"insertedCount": -1}}),
            json!({"ok": true, "data": {"insertedCount": 2}}),
            json!({"ok": true, "data": {"insertedCount": 1, "extra": true}}),
        ] {
            assert!(ensure_record_success(&malformed, 1).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "requires runner-owned loopback PG17 Database #409 URLs in DOCUMENT_VALIDATION_DATABASE_URL and DOCUMENT_VALIDATION_ADMIN_DATABASE_URL"]
    #[allow(clippy::too_many_lines)]
    async fn isolated_pool_concurrency_close_and_reconnect_are_fail_closed() {
        let database_url = std::env::var("DOCUMENT_VALIDATION_DATABASE_URL")
            .expect("isolated DOCUMENT_VALIDATION_DATABASE_URL is required");
        let admin_database_url = std::env::var("DOCUMENT_VALIDATION_ADMIN_DATABASE_URL")
            .expect("isolated DOCUMENT_VALIDATION_ADMIN_DATABASE_URL is required");
        for url in [&database_url, &admin_database_url] {
            let options = crate::pgbouncer_sqlx::postgres::PgConnectOptions::from_str(url)
                .expect("valid isolated PostgreSQL URL");
            assert!(
                matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"),
                "ignored contract test accepts loopback only"
            );
        }
        let config = AppConfig::parse_from([
            "document-validation-db-contract",
            "--database-url",
            "postgresql://unused.invalid/app",
            "--document-validation-database-url",
            database_url.as_str(),
            "--document-validation-db-max-connections",
            "4",
        ]);
        let db = DocumentValidationDb::connect(&config).await.unwrap();
        let admin =
            crate::db_pool::WorkerDbPoolOptions::new("document-validation-evidence-test-admin")
                .max_connections(2)
                .connect(&admin_database_url)
                .await
                .unwrap();
        let worker_job_id = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let key = json!({
            "datasetType": "Process",
            "datasetId": dataset_id,
            "datasetVersion": "01.00.000",
            "canonicalContentHash": "issue-207-content",
            "documentValidatorVersion": "issue-207-validator",
            "documentValidationProfile": "issue-207-profile",
            "validationReportSchemaVersion": "v1",
            "validatorEngineFingerprint": "issue-207-engine",
            "tidasSchemaLockSha256": "issue-207-schema"
        });
        let record = json!({
            "datasetType": "Process",
            "datasetId": dataset_id,
            "datasetVersion": "01.00.000",
            "canonicalContentHash": "issue-207-content",
            "documentValidatorVersion": "issue-207-validator",
            "documentValidationProfile": "issue-207-profile",
            "validationReportSchemaVersion": "v1",
            "validatorEngineFingerprint": "issue-207-engine",
            "tidasSchemaLockSha256": "issue-207-schema",
            "status": "passed",
            "summary": {"issue": 207},
            "issueArtifactRef": {"kind": "runner-owned"},
            "issueArtifactHash": "issue-207-artifact"
        });

        let proof = async {
            let baseline: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM public.lcia_document_validation_evidence WHERE dataset_id=$1",
            )
            .bind(dataset_id)
            .fetch_one(&admin)
            .await?;
            anyhow::ensure!(baseline == 0, "runner-owned fixture identity is not clean");
            sqlx::query(
                "INSERT INTO private.worker_jobs (id,job_kind,worker_queue,requester_type,visibility,payload_schema_version,payload_json) VALUES ($1,'lca.solve_one','solver','system','system','lca.solve_one.request.v1','{\"qualification\":\"issue207\"}'::jsonb)",
            )
            .bind(worker_job_id)
            .execute(&admin)
            .await?;

            sqlx::raw_sql(
                "CREATE OR REPLACE FUNCTION private.issue207_unregistered_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'; REVOKE ALL ON FUNCTION private.issue207_unregistered_probe() FROM PUBLIC",
            )
            .execute(&admin)
            .await?;
            for denied_sql in [
                "SELECT public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)",
                "SELECT count(*) FROM public.lcia_document_validation_evidence",
                "SELECT private.issue207_unregistered_probe()",
                "SET ROLE service_role",
                "CREATE ROLE issue207_forbidden_probe",
            ] {
                anyhow::ensure!(
                    sqlx::raw_sql(denied_sql).execute(&db.pool).await.is_err(),
                    "restricted family login unexpectedly executed a denied statement"
                );
            }

            sqlx::raw_sql(
                "GRANT EXECUTE ON FUNCTION private.issue207_unregistered_probe() TO lca_worker_runtime",
            )
            .execute(&admin)
            .await?;
            anyhow::ensure!(
                sqlx::query_scalar::<_, i32>("SELECT private.issue207_unregistered_probe()")
                    .fetch_one(&db.pool)
                    .await?
                    == 1,
                "granted unregistered private routine was not observable"
            );
            anyhow::ensure!(
                DocumentValidationDb::connect(&config).await.is_err(),
                "unregistered private capability did not fail closed"
            );
            sqlx::raw_sql(
                "REVOKE EXECUTE ON FUNCTION private.issue207_unregistered_probe() FROM lca_worker_runtime; DROP FUNCTION private.issue207_unregistered_probe()",
            )
            .execute(&admin)
            .await?;

            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..20 {
                let db = db.clone();
                let record = record.clone();
                tasks.spawn(async move { db.record(worker_job_id, &[record]).await });
            }
            while let Some(result) = tasks.join_next().await {
                result??;
            }
            let rows = db.lookup(std::slice::from_ref(&key)).await?;
            anyhow::ensure!(rows.len() == 1, "concurrent idempotent record did not converge");

            let mut connection = db.pool.acquire().await?;
            let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *connection)
                .await?;
            let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(backend_pid)
                .fetch_one(&admin)
                .await?;
            anyhow::ensure!(terminated, "failed to terminate the runner-owned backend");
            anyhow::ensure!(
                sqlx::query_scalar::<_, i32>("SELECT 1")
                    .fetch_one(&mut *connection)
                    .await
                    .is_err(),
                "terminated backend connection did not fail"
            );
            drop(connection);
            anyhow::ensure!(
                db.lookup(&[key]).await?.len() == 1,
                "pool did not replace the terminated backend"
            );

            sqlx::raw_sql(
                "CREATE FUNCTION private.svc_lcia_document_validation_evidence_lookup(text) RETURNS jsonb LANGUAGE sql AS 'SELECT jsonb_build_object(''ok'',true,''data'',jsonb_build_array())'; REVOKE ALL ON FUNCTION private.svc_lcia_document_validation_evidence_lookup(text) FROM PUBLIC",
            )
            .execute(&admin)
            .await?;
            anyhow::ensure!(
                DocumentValidationDb::connect(&config).await.is_err(),
                "same-name private overload did not fail closed"
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;

        let cleanup = async {
            sqlx::raw_sql(
                "DROP FUNCTION IF EXISTS private.svc_lcia_document_validation_evidence_lookup(text); DROP FUNCTION IF EXISTS private.issue207_unregistered_probe()",
            )
            .execute(&admin)
            .await?;
            sqlx::query(
                "DELETE FROM public.lcia_document_validation_evidence WHERE dataset_id=$1",
            )
            .bind(dataset_id)
            .execute(&admin)
            .await?;
            sqlx::query("DELETE FROM private.worker_jobs WHERE id=$1")
                .bind(worker_job_id)
                .execute(&admin)
                .await?;
            let residue: i64 = sqlx::query_scalar(
                "SELECT (SELECT count(*) FROM public.lcia_document_validation_evidence WHERE dataset_id=$1) + (SELECT count(*) FROM private.worker_jobs WHERE id=$2)",
            )
            .bind(dataset_id)
            .bind(worker_job_id)
            .fetch_one(&admin)
            .await?;
            anyhow::ensure!(residue == 0, "runner-owned evidence residue remains");
            Ok::<(), anyhow::Error>(())
        }
        .await;
        db.pool.close().await;
        admin.close().await;
        proof.unwrap();
        cleanup.unwrap();
    }
}
