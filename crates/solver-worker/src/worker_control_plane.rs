//! Schema-qualified Expand contract for the Worker control plane.
//!
//! During Expand, `private` may be a security-invoker compatibility view over
//! public physical storage. A later gated Contract migration may swap physical
//! storage without changing these Worker identifiers. Runtime DTOs deliberately
//! use logical names such as `worker_jobs`; callers must not expose the schema
//! choice in payloads, result references, or errors.

macro_rules! worker_jobs_table {
    () => {
        "private.worker_jobs"
    };
}
macro_rules! worker_job_events_table {
    () => {
        "private.worker_job_events"
    };
}
macro_rules! worker_job_artifacts_table {
    () => {
        "private.worker_job_artifacts"
    };
}
macro_rules! worker_job_domain_refs_table {
    () => {
        "api.worker_job_domain_refs"
    };
}
macro_rules! worker_job_kinds_table {
    () => {
        "private.worker_job_kinds"
    };
}

pub(crate) use worker_job_artifacts_table;
pub(crate) use worker_jobs_table;

pub const WORKER_JOBS_TABLE: &str = worker_jobs_table!();
pub const WORKER_JOB_EVENTS_TABLE: &str = worker_job_events_table!();
pub const WORKER_JOB_ARTIFACTS_TABLE: &str = worker_job_artifacts_table!();
pub const WORKER_JOB_DOMAIN_REFS_TABLE: &str = worker_job_domain_refs_table!();
pub const WORKER_JOB_KINDS_TABLE: &str = worker_job_kinds_table!();

pub const CLAIM_JOBS_SQL: &str = r"
    WITH _service_role AS (
        SELECT set_config('request.jwt.claim.role', 'service_role', true)
    )
    SELECT api.worker_claim_jobs_v1($1, $2, $3, $4) AS result
    FROM _service_role
";

pub const HEARTBEAT_JOB_SQL: &str = r"
    WITH _service_role AS (
        SELECT set_config('request.jwt.claim.role', 'service_role', true)
    )
    SELECT api.worker_heartbeat_job_v1(
        $1, $2, $3, $4::double precision::numeric, $5::jsonb, $6
    ) AS result
    FROM _service_role
";

pub const RECORD_JOB_RESULT_SQL: &str = r"
    WITH _service_role AS (
        SELECT set_config('request.jwt.claim.role', 'service_role', true)
    )
    SELECT api.worker_record_job_result_v1(
        $1,
        $2,
        $3,
        $4::jsonb,
        $5,
        $6::jsonb,
        $7::jsonb,
        $8,
        $9,
        $10::jsonb,
        $11::text[],
        $12,
        $13
    ) AS result
    FROM _service_role
";

pub const INSERT_MAINTENANCE_ARTIFACT_SQL: &str = concat!(
    r"
    INSERT INTO ",
    worker_job_artifacts_table!(),
    r" (
        job_id,
        artifact_type,
        content_type,
        metadata,
        visibility
    )
    VALUES ($1, 'maintenance_gc_report', 'application/json', $2::jsonb, 'operator')
    RETURNING id
    "
);

pub const RESULT_GC_CANDIDATE_QUERY: &str = concat!(
    r"
    WITH ranked AS (
      SELECT
        r.id AS result_id,
        r.artifact_url,
        r.created_at,
        r.expires_at,
        r.is_pinned,
        ROW_NUMBER() OVER (
          PARTITION BY
            w.requested_by,
            r.snapshot_id,
            COALESCE(rc.request_key, w.request_hash, r.job_id::text)
          ORDER BY r.created_at DESC, r.id DESC
        ) AS rn,
        rc.result_id AS active_cache_result_id
      FROM public.lca_results AS r
      LEFT JOIN ",
    worker_jobs_table!(),
    r" AS w
        ON w.id = r.worker_job_id
      LEFT JOIN public.lca_result_cache AS rc
        ON rc.result_id = r.id
       AND rc.status IN ('pending', 'running', 'ready')
      WHERE r.worker_job_id IS NOT NULL
    )
    SELECT result_id, artifact_url
    FROM ranked
    WHERE expires_at < now()
      AND is_pinned = false
      AND active_cache_result_id IS NULL
      AND rn > 1
    ORDER BY created_at ASC
    LIMIT $1
    "
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_identifiers_are_private_and_schema_qualified() {
        for identifier in [
            WORKER_JOBS_TABLE,
            WORKER_JOB_EVENTS_TABLE,
            WORKER_JOB_ARTIFACTS_TABLE,
            WORKER_JOB_KINDS_TABLE,
        ] {
            assert!(identifier.starts_with("private."));
            assert_eq!(identifier.matches('.').count(), 1);
        }
        assert_eq!(WORKER_JOB_DOMAIN_REFS_TABLE, "api.worker_job_domain_refs");
    }

    #[test]
    fn frozen_worker_rpcs_use_versioned_api_facades() {
        for sql in [CLAIM_JOBS_SQL, HEARTBEAT_JOB_SQL, RECORD_JOB_RESULT_SQL] {
            assert!(sql.contains("api.worker_"));
            assert!(sql.contains("_v1"));
            assert!(!sql.contains("public.worker_"));
            assert!(!sql.contains("search_path"));
            assert!(sql.contains("WITH _service_role AS"));
            assert!(sql.contains("set_config('request.jwt.claim.role', 'service_role', true)"));
        }
    }
}
