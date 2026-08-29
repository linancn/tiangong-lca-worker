use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    package_types::PackageArtifactKind,
    pgbouncer_sqlx::{self as sqlx, PgPool, Row},
};

pub const DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS: i32 = 30;
pub const DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS: i32 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRetentionSummaryRow {
    pub retention_area: String,
    pub retention_action: String,
    pub is_eligible: bool,
    pub reason: String,
    pub row_count: i64,
    pub total_artifact_bytes: i64,
    pub total_hit_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageArtifactGcCandidate {
    pub artifact_id: Uuid,
    pub job_id: Uuid,
    pub artifact_kind: String,
    pub artifact_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageExportItemDeleteResult {
    pub deleted: u64,
    pub orphan_deleted: u64,
}

#[must_use]
pub const fn default_package_artifact_retention_days(kind: PackageArtifactKind) -> i32 {
    match kind {
        PackageArtifactKind::ExportZip | PackageArtifactKind::ExportReport => {
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        }
        PackageArtifactKind::ImportSource | PackageArtifactKind::ImportReport => {
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        }
    }
}

pub fn validate_retention_days(value: i32, name: &str) -> anyhow::Result<i32> {
    if !(1..=3650).contains(&value) {
        return Err(anyhow::anyhow!("{name} must be between 1 and 3650 days"));
    }
    Ok(value)
}

#[allow(clippy::too_many_lines)]
pub async fn fetch_package_retention_summary(
    pool: &PgPool,
    as_of: DateTime<Utc>,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageRetentionSummaryRow>> {
    let rows = sqlx::query(
        r"
        WITH artifact_classified AS (
          SELECT
            'lca_package_artifacts'::text AS retention_area,
            'delete_object_then_mark_deleted'::text AS retention_action,
            (classified.reason = 'eligible_expired_unpinned_artifact') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            COALESCE(classified.artifact_byte_size, 0)::bigint AS total_artifact_bytes,
            0::bigint AS total_hit_count
          FROM (
            SELECT
              artifacts.*,
              CASE
                WHEN artifacts.is_pinned THEN 'protected_pinned_artifact'
                WHEN artifacts.status = 'deleted' THEN 'protected_already_deleted'
                WHEN artifacts.status <> 'ready' THEN 'protected_artifact_not_ready'
                WHEN artifacts.expires_at IS NULL THEN 'protected_missing_expires_at'
                WHEN artifacts.expires_at > $1 THEN 'protected_expires_at_in_future'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.worker_jobs AS active_job
                  WHERE active_job.status IN ('queued', 'running', 'waiting')
                    AND (
                      (
                        artifacts.worker_job_id IS NOT NULL
                        AND active_job.id = artifacts.worker_job_id
                      )
                      OR active_job.payload_json ->> 'job_id' = artifacts.job_id::text
                    )
                ) THEN 'protected_active_parent_worker_job'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.lca_package_request_cache AS recent_cache
                  WHERE (
                      recent_cache.export_artifact_id = artifacts.id
                      OR recent_cache.report_artifact_id = artifacts.id
                    )
                    AND (
                      recent_cache.status IN ('pending', 'running')
                      OR recent_cache.last_accessed_at >= $1 - make_interval(days => $3::integer)
                    )
                ) THEN 'protected_request_cache_reference'
                ELSE 'eligible_expired_unpinned_artifact'
              END AS reason
            FROM private.lca_package_artifacts AS artifacts
          ) AS classified
        ),
        request_cache_classified AS (
          SELECT
            'lca_package_request_cache'::text AS retention_area,
            'delete_stale_request_cache_row'::text AS retention_action,
            (classified.reason = 'eligible_stale_request_cache') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            0::bigint AS total_artifact_bytes,
            classified.hit_count::bigint AS total_hit_count
          FROM (
            SELECT
              request_cache.*,
              CASE
                WHEN request_cache.status IN ('pending', 'running') THEN 'protected_active_request_cache'
                WHEN request_cache.last_accessed_at >= $1 - make_interval(days => $3::integer) THEN 'protected_recent_request_cache_access'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.worker_jobs AS active_job
                  WHERE active_job.status IN ('queued', 'running', 'waiting')
                    AND (
                      (
                        request_cache.worker_job_id IS NOT NULL
                        AND active_job.id = request_cache.worker_job_id
                      )
                      OR active_job.payload_json ->> 'job_id' = request_cache.job_id::text
                    )
                ) THEN 'protected_active_parent_worker_job'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.lca_package_artifacts AS artifact
                  WHERE artifact.status <> 'deleted'
                    AND artifact.id IN (
                      request_cache.export_artifact_id,
                      request_cache.report_artifact_id
                    )
                ) THEN 'protected_live_artifact_reference'
                ELSE 'eligible_stale_request_cache'
              END AS reason
            FROM private.lca_package_request_cache AS request_cache
          ) AS classified
        ),
        export_item_classified AS (
          SELECT
            'lca_package_export_items'::text AS retention_area,
            'delete_export_item_after_object_gc'::text AS retention_action,
            (classified.reason = 'eligible_export_item_after_object_gc') AS is_eligible,
            classified.reason,
            1::bigint AS row_count,
            0::bigint AS total_artifact_bytes,
            0::bigint AS total_hit_count
          FROM (
            SELECT
              export_item.*,
              canonical_job.finished_at AS canonical_finished_at,
              canonical_job.updated_at AS canonical_updated_at,
              canonical_job.created_at AS canonical_created_at,
              CASE
                WHEN EXISTS (
                  SELECT 1
                  FROM private.worker_jobs AS active_job
                  WHERE active_job.status IN ('queued', 'running', 'waiting')
                    AND (
                      (
                        export_item.worker_job_id IS NOT NULL
                        AND active_job.id = export_item.worker_job_id
                      )
                      OR active_job.payload_json ->> 'job_id' = export_item.job_id::text
                    )
                ) THEN 'protected_active_parent_worker_job'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.lca_package_artifacts AS artifact
                  WHERE artifact.status <> 'deleted'
                    AND (
                      (
                        export_item.worker_job_id IS NOT NULL
                        AND artifact.worker_job_id = export_item.worker_job_id
                      )
                      OR artifact.job_id = export_item.job_id
                    )
                ) THEN 'protected_live_artifact_reference'
                WHEN EXISTS (
                  SELECT 1
                  FROM private.lca_package_request_cache AS request_cache
                  WHERE (
                      request_cache.status IN ('pending', 'running')
                      OR request_cache.last_accessed_at >= $1 - make_interval(days => $3::integer)
                    )
                    AND (
                      (
                        export_item.worker_job_id IS NOT NULL
                        AND request_cache.worker_job_id = export_item.worker_job_id
                      )
                      OR request_cache.job_id = export_item.job_id
                    )
                ) THEN 'protected_request_cache_reference'
                WHEN COALESCE(
                  canonical_job.finished_at,
                  canonical_job.updated_at,
                  canonical_job.created_at,
                  export_item.created_at
                ) >= $1 - make_interval(days => $2::integer)
                  THEN 'protected_recent_export_item'
                ELSE 'eligible_export_item_after_object_gc'
              END AS reason
            FROM private.lca_package_export_items AS export_item
            LEFT JOIN private.worker_jobs AS canonical_job
              ON canonical_job.id = export_item.worker_job_id
          ) AS classified
        ),
        classified AS (
          SELECT * FROM artifact_classified
          UNION ALL
          SELECT * FROM request_cache_classified
          UNION ALL
          SELECT * FROM export_item_classified
        )
        SELECT
          retention_area,
          retention_action,
          is_eligible,
          reason,
          SUM(row_count)::bigint AS row_count,
          COALESCE(SUM(total_artifact_bytes), 0)::bigint AS total_artifact_bytes,
          COALESCE(SUM(total_hit_count), 0)::bigint AS total_hit_count
        FROM classified
        GROUP BY retention_area, retention_action, is_eligible, reason
        ORDER BY retention_area, is_eligible DESC, reason
        ",
    )
    .bind(as_of)
    .bind(job_retention_days)
    .bind(request_cache_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PackageRetentionSummaryRow {
                retention_area: row.try_get("retention_area")?,
                retention_action: row.try_get("retention_action")?,
                is_eligible: row.try_get("is_eligible")?,
                reason: row.try_get("reason")?,
                row_count: row.try_get("row_count")?,
                total_artifact_bytes: row.try_get("total_artifact_bytes")?,
                total_hit_count: row.try_get("total_hit_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn fetch_package_artifact_gc_candidates(
    pool: &PgPool,
    as_of: DateTime<Utc>,
    batch_size: i64,
    request_cache_retention_days: i32,
) -> anyhow::Result<Vec<PackageArtifactGcCandidate>> {
    let rows = sqlx::query(
        r"
        SELECT
          artifacts.id AS artifact_id,
          artifacts.job_id,
          artifacts.artifact_kind,
          artifacts.artifact_url
        FROM private.lca_package_artifacts AS artifacts
        WHERE artifacts.status = 'ready'
          AND artifacts.is_pinned = FALSE
          AND artifacts.expires_at IS NOT NULL
          AND artifacts.expires_at <= $2
          AND NOT EXISTS (
            SELECT 1
            FROM private.worker_jobs AS active_job
            WHERE active_job.status IN ('queued', 'running', 'waiting')
              AND (
                (
                  artifacts.worker_job_id IS NOT NULL
                  AND active_job.id = artifacts.worker_job_id
                )
                OR active_job.payload_json ->> 'job_id' = artifacts.job_id::text
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM private.lca_package_request_cache AS request_cache
            WHERE (
                request_cache.export_artifact_id = artifacts.id
                OR request_cache.report_artifact_id = artifacts.id
              )
              AND (
                request_cache.status IN ('pending', 'running')
                OR request_cache.last_accessed_at >= $2 - make_interval(days => $3::integer)
              )
          )
        ORDER BY artifacts.expires_at ASC, artifacts.created_at ASC, artifacts.id ASC
        LIMIT $1
        FOR UPDATE OF artifacts SKIP LOCKED
        ",
    )
    .bind(batch_size)
    .bind(as_of)
    .bind(request_cache_retention_days)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PackageArtifactGcCandidate {
                artifact_id: row.try_get("artifact_id")?,
                job_id: row.try_get("job_id")?,
                artifact_kind: row.try_get("artifact_kind")?,
                artifact_url: row.try_get("artifact_url")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

pub async fn reconcile_package_artifact_gc_candidate(
    pool: &PgPool,
    artifact_id: Uuid,
    as_of: DateTime<Utc>,
    request_cache_retention_days: i32,
) -> anyhow::Result<Option<PackageArtifactGcCandidate>> {
    let row = sqlx::query(
        r"
        SELECT
          artifacts.id AS artifact_id,
          artifacts.job_id,
          artifacts.artifact_kind,
          artifacts.artifact_url
        FROM private.lca_package_artifacts AS artifacts
        WHERE artifacts.id = $1
          AND artifacts.status = 'ready'
          AND artifacts.is_pinned = FALSE
          AND artifacts.expires_at IS NOT NULL
          AND artifacts.expires_at <= $2
          AND NOT EXISTS (
            SELECT 1
            FROM private.worker_jobs AS active_job
            WHERE active_job.status IN ('queued', 'running', 'waiting')
              AND (
                (
                  artifacts.worker_job_id IS NOT NULL
                  AND active_job.id = artifacts.worker_job_id
                )
                OR active_job.payload_json ->> 'job_id' = artifacts.job_id::text
              )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM private.lca_package_request_cache AS request_cache
            WHERE (
                request_cache.export_artifact_id = artifacts.id
                OR request_cache.report_artifact_id = artifacts.id
              )
              AND (
                request_cache.status IN ('pending', 'running')
                OR request_cache.last_accessed_at >= $2 - make_interval(days => $3::integer)
              )
          )
        FOR UPDATE OF artifacts SKIP LOCKED
        ",
    )
    .bind(artifact_id)
    .bind(as_of)
    .bind(request_cache_retention_days)
    .fetch_optional(pool)
    .await?;

    row.map(|row| -> Result<_, sqlx::Error> {
        Ok(PackageArtifactGcCandidate {
            artifact_id: row.try_get("artifact_id")?,
            job_id: row.try_get("job_id")?,
            artifact_kind: row.try_get("artifact_kind")?,
            artifact_url: row.try_get("artifact_url")?,
        })
    })
    .transpose()
    .map_err(Into::into)
}

pub async fn mark_package_artifact_deleted(
    pool: &PgPool,
    artifact_id: Uuid,
    as_of: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE private.lca_package_artifacts
        SET status = 'deleted',
            metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'gc',
              jsonb_build_object(
                'status', 'object_deleted',
                'deleted_at', NOW(),
                'as_of', $2,
                'reason', 'eligible_expired_unpinned_artifact'
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND status <> 'deleted'
        ",
    )
    .bind(artifact_id)
    .bind(as_of)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn record_package_artifact_gc_error(
    pool: &PgPool,
    artifact_id: Uuid,
    as_of: DateTime<Utc>,
    error_message: &str,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE private.lca_package_artifacts
        SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'gc',
              jsonb_build_object(
                'status', 'object_delete_failed',
                'last_error_at', NOW(),
                'as_of', $2,
                'last_error', $3
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND status <> 'deleted'
        ",
    )
    .bind(artifact_id)
    .bind(as_of)
    .bind(error_message)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn delete_stale_package_request_cache_rows(
    pool: &PgPool,
    as_of: DateTime<Utc>,
    batch_size: i64,
    request_cache_retention_days: i32,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        WITH candidates AS (
          SELECT request_cache.id
          FROM private.lca_package_request_cache AS request_cache
          WHERE request_cache.status NOT IN ('pending', 'running')
            AND request_cache.last_accessed_at < $2 - make_interval(days => $3::integer)
            AND NOT EXISTS (
              SELECT 1
              FROM private.worker_jobs AS active_job
              WHERE active_job.status IN ('queued', 'running', 'waiting')
                AND (
                  (
                    request_cache.worker_job_id IS NOT NULL
                    AND active_job.id = request_cache.worker_job_id
                  )
                  OR active_job.payload_json ->> 'job_id' = request_cache.job_id::text
                )
            )
            AND NOT EXISTS (
              SELECT 1
              FROM private.lca_package_artifacts AS artifact
              WHERE artifact.status <> 'deleted'
                AND artifact.id IN (
                  request_cache.export_artifact_id,
                  request_cache.report_artifact_id
                )
            )
          ORDER BY request_cache.last_accessed_at ASC, request_cache.id ASC
          LIMIT $1
          FOR UPDATE OF request_cache SKIP LOCKED
        )
        DELETE FROM private.lca_package_request_cache AS request_cache
        USING candidates
        WHERE request_cache.id = candidates.id
        ",
    )
    .bind(batch_size)
    .bind(as_of)
    .bind(request_cache_retention_days)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn delete_package_export_items_after_object_gc(
    pool: &PgPool,
    as_of: DateTime<Utc>,
    batch_size: i64,
    job_retention_days: i32,
    request_cache_retention_days: i32,
) -> anyhow::Result<PackageExportItemDeleteResult> {
    let row = sqlx::query(
        r"
        WITH candidates AS (
          SELECT export_item.id, export_item.worker_job_id
          FROM private.lca_package_export_items AS export_item
          LEFT JOIN private.worker_jobs AS canonical_job
            ON canonical_job.id = export_item.worker_job_id
          WHERE COALESCE(
              canonical_job.finished_at,
              canonical_job.updated_at,
              canonical_job.created_at,
              export_item.created_at
            ) < $2 - make_interval(days => $3::integer)
            AND NOT EXISTS (
              SELECT 1
              FROM private.worker_jobs AS active_job
              WHERE active_job.status IN ('queued', 'running', 'waiting')
                AND (
                  (
                    export_item.worker_job_id IS NOT NULL
                    AND active_job.id = export_item.worker_job_id
                  )
                  OR active_job.payload_json ->> 'job_id' = export_item.job_id::text
                )
            )
            AND NOT EXISTS (
              SELECT 1
              FROM private.lca_package_artifacts AS artifact
              WHERE artifact.status <> 'deleted'
                AND (
                  (
                    export_item.worker_job_id IS NOT NULL
                    AND artifact.worker_job_id = export_item.worker_job_id
                  )
                  OR artifact.job_id = export_item.job_id
                )
            )
            AND NOT EXISTS (
              SELECT 1
              FROM private.lca_package_request_cache AS request_cache
              WHERE (
                  request_cache.status IN ('pending', 'running')
                  OR request_cache.last_accessed_at >= $2 - make_interval(days => $4::integer)
                )
                AND (
                  (
                    export_item.worker_job_id IS NOT NULL
                    AND request_cache.worker_job_id = export_item.worker_job_id
                  )
                  OR request_cache.job_id = export_item.job_id
                )
            )
          ORDER BY export_item.created_at ASC, export_item.id ASC
          LIMIT $1
          FOR UPDATE OF export_item SKIP LOCKED
        ),
        deleted AS (
          DELETE FROM private.lca_package_export_items AS export_item
          USING candidates
          WHERE export_item.id = candidates.id
          RETURNING candidates.worker_job_id
        )
        SELECT
          COUNT(*)::bigint AS deleted_count,
          COUNT(*) FILTER (WHERE worker_job_id IS NULL)::bigint AS orphan_deleted_count
        FROM deleted
        ",
    )
    .bind(batch_size)
    .bind(as_of)
    .bind(job_retention_days)
    .bind(request_cache_retention_days)
    .fetch_one(pool)
    .await?;

    let deleted = u64::try_from(row.try_get::<i64, _>("deleted_count")?)
        .map_err(|_| anyhow::anyhow!("deleted export-item count overflow"))?;
    let orphan_deleted = u64::try_from(row.try_get::<i64, _>("orphan_deleted_count")?)
        .map_err(|_| anyhow::anyhow!("orphan export-item count overflow"))?;

    Ok(PackageExportItemDeleteResult {
        deleted,
        orphan_deleted,
    })
}

pub async fn refresh_import_source_retention(
    pool: &PgPool,
    artifact_id: Uuid,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r"
        UPDATE private.lca_package_artifacts
        SET expires_at = NOW() + make_interval(days => $2::integer),
            metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
              'retention',
              jsonb_build_object(
                'policy', 'import_source_terminal',
                'retention_days', $2::integer,
                'refreshed_at', NOW()
              )
            ),
            updated_at = NOW()
        WHERE id = $1
          AND artifact_kind = 'import_source'
          AND status = 'ready'
          AND is_pinned = FALSE
        ",
    )
    .bind(artifact_id)
    .bind(DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS,
        DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS, default_package_artifact_retention_days,
        validate_retention_days,
    };
    use crate::package_types::PackageArtifactKind;

    #[test]
    fn package_artifact_retention_defaults_follow_kind() {
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ExportZip),
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ExportReport),
            DEFAULT_EXPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ImportSource),
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
        assert_eq!(
            default_package_artifact_retention_days(PackageArtifactKind::ImportReport),
            DEFAULT_IMPORT_PACKAGE_ARTIFACT_RETENTION_DAYS
        );
    }

    #[test]
    fn retention_days_must_be_positive_and_bounded() {
        assert!(validate_retention_days(1, "retention").is_ok());
        assert!(validate_retention_days(3650, "retention").is_ok());
        assert!(validate_retention_days(0, "retention").is_err());
        assert!(validate_retention_days(3651, "retention").is_err());
    }
}
