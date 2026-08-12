use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use crate::{
    db_pool::sql_string_literal,
    pgbouncer_sqlx::{self as sqlx, PgPool, Row},
};

pub const REVIEW_SUBMIT_GATE_JOB_KIND: &str = "review_submit.gate";
pub const REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION: &str = "review_submit.gate.request.v1";
pub const REVIEW_SUBMIT_GATE_WORKER_QUEUE: &str = "review_submit_gate";

const RESULT_WRITE_MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerJob {
    pub id: Uuid,
    pub job_kind: String,
    pub worker_queue: String,
    pub payload_schema_version: String,
    pub payload: Value,
    pub requested_by: Option<Uuid>,
    pub lease_token: Uuid,
    pub attempt_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewSubmitGateWorkerRequest {
    pub dataset_table: String,
    pub dataset_id: Uuid,
    pub dataset_version: String,
    pub revision_checksum: Option<String>,
    pub policy_profile: Option<String>,
    pub report_schema_version: Option<String>,
    pub requested_by: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerJobResult {
    pub status: String,
    pub result_json: Option<Value>,
    pub result_schema_version: Option<String>,
    pub result_ref: Option<Value>,
    pub diagnostics: Option<Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub error_details: Option<Value>,
    pub blocker_codes: Vec<String>,
    pub resolution_scope: Option<String>,
    pub retryable: Option<bool>,
}

impl WorkerJob {
    pub fn from_json(value: &Value) -> anyhow::Result<Self> {
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("worker job is missing id"))?
            .parse::<Uuid>()?;
        let lease_token = value
            .get("leaseToken")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("worker job is missing leaseToken"))?
            .parse::<Uuid>()?;
        let requested_by = value
            .get("requestedBy")
            .and_then(Value::as_str)
            .map(str::parse::<Uuid>)
            .transpose()?;

        Ok(Self {
            id,
            job_kind: required_text(value, "jobKind")?,
            worker_queue: required_text(value, "workerQueue")?,
            payload_schema_version: required_text(value, "payloadSchemaVersion")?,
            payload: value.get("payload").cloned().unwrap_or_else(|| json!({})),
            requested_by,
            lease_token,
            attempt_count: value
                .get("attemptCount")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
        })
    }

    pub fn review_submit_gate_request(&self) -> anyhow::Result<ReviewSubmitGateWorkerRequest> {
        if self.job_kind != REVIEW_SUBMIT_GATE_JOB_KIND {
            return Err(anyhow::anyhow!(
                "unsupported worker job kind for review-submit gate: {}",
                self.job_kind
            ));
        }
        if self.worker_queue != REVIEW_SUBMIT_GATE_WORKER_QUEUE {
            return Err(anyhow::anyhow!(
                "unsupported worker queue for review-submit gate: {}",
                self.worker_queue
            ));
        }
        if self.payload_schema_version != REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION {
            return Err(anyhow::anyhow!(
                "unsupported review-submit gate payload schema: {}",
                self.payload_schema_version
            ));
        }

        let payload = serde_json::from_value::<ReviewSubmitGatePayload>(self.payload.clone())?;
        let requested_by = payload.requested_by.or(self.requested_by).ok_or_else(|| {
            anyhow::anyhow!("review-submit gate worker job is missing requestedBy")
        })?;

        Ok(ReviewSubmitGateWorkerRequest {
            dataset_table: payload.dataset_revision.dataset_table,
            dataset_id: payload.dataset_revision.dataset_id,
            dataset_version: payload.dataset_revision.dataset_version,
            revision_checksum: payload.dataset_revision.revision_checksum,
            policy_profile: payload.policy_profile,
            report_schema_version: payload.report_schema_version,
            requested_by,
        })
    }
}

impl WorkerJobResult {
    pub fn completed(result_json: Value, result_schema_version: impl Into<String>) -> Self {
        Self {
            status: "completed".to_owned(),
            result_json: Some(result_json),
            result_schema_version: Some(result_schema_version.into()),
            result_ref: None,
            diagnostics: None,
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: None,
        }
    }

    pub fn blocked(
        result_json: Value,
        result_schema_version: impl Into<String>,
        blocker_codes: Vec<String>,
        resolution_scope: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status: "blocked".to_owned(),
            result_json: Some(result_json),
            result_schema_version: Some(result_schema_version.into()),
            result_ref: None,
            diagnostics: None,
            error_code: None,
            error_message: None,
            error_details: None,
            blocker_codes,
            resolution_scope: Some(resolution_scope.into()),
            retryable: Some(retryable),
        }
    }

    pub fn failed(
        error_code: impl Into<String>,
        error_message: impl Into<String>,
        error_details: Value,
        diagnostics: Option<Value>,
        result_json: Option<Value>,
    ) -> Self {
        Self {
            status: "failed".to_owned(),
            result_json,
            result_schema_version: None,
            result_ref: None,
            diagnostics,
            error_code: Some(error_code.into()),
            error_message: Some(error_message.into()),
            error_details: Some(error_details),
            blocker_codes: Vec::new(),
            resolution_scope: None,
            retryable: Some(true),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerJobProgress<'a> {
    pool: &'a PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
}

impl<'a> WorkerJobProgress<'a> {
    #[must_use]
    pub const fn new(
        pool: &'a PgPool,
        job_id: Uuid,
        lease_token: Uuid,
        lease_seconds: i32,
    ) -> Self {
        Self {
            pool,
            job_id,
            lease_token,
            lease_seconds,
        }
    }

    pub async fn heartbeat(
        &self,
        phase: &str,
        progress: f64,
        diagnostics: Option<Value>,
    ) -> anyhow::Result<()> {
        heartbeat_worker_job(
            self.pool,
            self.job_id,
            self.lease_token,
            phase,
            progress,
            diagnostics,
            self.lease_seconds,
        )
        .await
    }

    #[must_use]
    pub const fn lease_seconds(&self) -> i32 {
        self.lease_seconds
    }

    #[must_use]
    pub const fn lease_token(&self) -> Uuid {
        self.lease_token
    }
}

#[must_use]
pub fn lease_heartbeat_period(lease_seconds: i32) -> std::time::Duration {
    let lease_seconds = u64::try_from(lease_seconds.max(3)).unwrap_or(3);
    std::time::Duration::from_secs((lease_seconds / 3).max(1))
}

pub async fn claim_worker_jobs(
    pool: &PgPool,
    worker_queue: &str,
    worker_id: &str,
    limit: i32,
    lease_seconds: i32,
) -> anyhow::Result<Vec<WorkerJob>> {
    let sql = format!(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.worker_claim_jobs({}, {}, {}, {}) AS result
        FROM _service_role
        ",
        sql_string_literal(worker_queue),
        sql_string_literal(worker_id),
        limit.clamp(1, 50),
        lease_seconds.clamp(1, 86_400),
    );
    let row = sqlx::raw_query(&sql).fetch_one(pool).await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_ok(&result, "worker_claim_jobs")?;

    result.get("data").and_then(Value::as_array).map_or_else(
        || Ok(Vec::new()),
        |items| items.iter().map(WorkerJob::from_json).collect(),
    )
}

pub async fn heartbeat_worker_job(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    phase: &str,
    progress: f64,
    diagnostics: Option<Value>,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let sql = format!(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.worker_heartbeat_job({}::uuid, {}::uuid, {}, {}::double precision::numeric, {}::jsonb, {}) AS result
        FROM _service_role
        ",
        sql_string_literal(&job_id.to_string()),
        sql_string_literal(&lease_token.to_string()),
        sql_string_literal(phase),
        progress,
        json_sql(diagnostics.as_ref()),
        lease_seconds.clamp(1, 86_400),
    );
    let row = sqlx::raw_query(&sql).fetch_one(pool).await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_ok(&result, "worker_heartbeat_job")?;
    Ok(())
}

pub async fn record_worker_job_result(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    result: WorkerJobResult,
) -> anyhow::Result<Value> {
    let sql = format!(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.worker_record_job_result(
            {}::uuid,
            {}::uuid,
            {},
            {}::jsonb,
            {},
            {}::jsonb,
            {}::jsonb,
            {},
            {},
            {}::jsonb,
            {},
            {},
            {}
        ) AS result
        FROM _service_role
        ",
        sql_string_literal(&job_id.to_string()),
        sql_string_literal(&lease_token.to_string()),
        sql_string_literal(&result.status),
        json_sql(result.result_json.as_ref()),
        text_sql(result.result_schema_version.as_deref()),
        json_sql(result.result_ref.as_ref()),
        json_sql(result.diagnostics.as_ref()),
        text_sql(result.error_code.as_deref()),
        text_sql(result.error_message.as_deref()),
        json_sql(result.error_details.as_ref()),
        text_array_sql(&result.blocker_codes),
        text_sql(result.resolution_scope.as_deref()),
        bool_sql(result.retryable),
    );
    let row = sqlx::raw_query(&sql).fetch_one(pool).await?;
    let rpc_result = row.try_get::<Value, _>("result")?;
    ensure_ok(&rpc_result, "worker_record_job_result")?;
    Ok(rpc_result)
}

/// Records a terminal worker result with bounded retries for database/transport failures.
///
/// The database RPC treats an identical terminal write from the same lease token as an
/// idempotent replay, so retrying an ambiguous connection failure cannot strand a completed
/// execution in `running`.
pub async fn record_worker_job_result_reliably(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    result: WorkerJobResult,
) -> anyhow::Result<Value> {
    for attempt in 1..=RESULT_WRITE_MAX_ATTEMPTS {
        match record_worker_job_result(pool, job_id, lease_token, result.clone()).await {
            Ok(value) => return Ok(value),
            Err(error)
                if attempt < RESULT_WRITE_MAX_ATTEMPTS
                    && error
                        .chain()
                        .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some()) =>
            {
                let delay = result_write_retry_delay(attempt);
                warn!(
                    worker_job_id = %job_id,
                    attempt,
                    max_attempts = RESULT_WRITE_MAX_ATTEMPTS,
                    retry_delay_ms = delay.as_millis(),
                    error = %error,
                    "worker job terminal write failed; retrying"
                );
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("terminal result retry loop always returns")
}

fn result_write_retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(100_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(4)))
}

fn text_sql(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_owned(), sql_string_literal)
}

fn bool_sql(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "NULL",
    }
}

fn json_sql(value: Option<&Value>) -> String {
    value.map_or_else(
        || "NULL".to_owned(),
        |value| sql_string_literal(&value.to_string()),
    )
}

fn text_array_sql(values: &[String]) -> String {
    if values.is_empty() {
        "ARRAY[]::text[]".to_owned()
    } else {
        format!(
            "ARRAY[{}]::text[]",
            values
                .iter()
                .map(|value| sql_string_literal(value))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn ensure_ok(result: &Value, rpc_name: &str) -> anyhow::Result<()> {
    if result.get("ok").and_then(Value::as_bool) == Some(true) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{rpc_name} returned non-ok result: {result}"
        ))
    }
}

fn required_text(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("worker job is missing {key}"))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReviewSubmitGatePayload {
    dataset_revision: ReviewSubmitGateDatasetRevision,
    #[serde(default)]
    requested_by: Option<Uuid>,
    #[serde(default)]
    policy_profile: Option<String>,
    #[serde(default)]
    report_schema_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReviewSubmitGateDatasetRevision {
    #[serde(alias = "table", alias = "datasetTable")]
    dataset_table: String,
    #[serde(alias = "id", alias = "datasetId")]
    dataset_id: Uuid,
    #[serde(alias = "version", alias = "datasetVersion")]
    dataset_version: String,
    #[serde(default, alias = "revisionChecksum")]
    revision_checksum: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        REVIEW_SUBMIT_GATE_JOB_KIND, REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
        REVIEW_SUBMIT_GATE_WORKER_QUEUE, WorkerJob, lease_heartbeat_period,
        result_write_retry_delay,
    };

    #[test]
    fn lease_heartbeat_period_refreshes_before_expiry() {
        assert_eq!(lease_heartbeat_period(900).as_secs(), 300);
        assert_eq!(lease_heartbeat_period(2).as_secs(), 1);
        assert_eq!(lease_heartbeat_period(-1).as_secs(), 1);
    }

    #[test]
    fn terminal_result_retry_backoff_is_bounded() {
        assert_eq!(result_write_retry_delay(1).as_millis(), 100);
        assert_eq!(result_write_retry_delay(2).as_millis(), 200);
        assert_eq!(result_write_retry_delay(99).as_millis(), 1_600);
    }

    #[test]
    fn parses_review_submit_gate_worker_job_payload() {
        let job_id = Uuid::new_v4();
        let lease_token = Uuid::new_v4();
        let requested_by = Uuid::new_v4();
        let dataset_id = Uuid::new_v4();
        let job = WorkerJob::from_json(&json!({
            "id": job_id,
            "jobKind": REVIEW_SUBMIT_GATE_JOB_KIND,
            "workerQueue": REVIEW_SUBMIT_GATE_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
            "payload": {
                "datasetRevision": {
                    "table": "processes",
                    "id": dataset_id,
                    "version": "01.00.000",
                    "revisionChecksum": "abc123"
                }
            },
            "requestedBy": requested_by,
            "leaseToken": lease_token,
            "attemptCount": 2
        }))
        .unwrap();

        let request = job.review_submit_gate_request().unwrap();

        assert_eq!(request.dataset_table, "processes");
        assert_eq!(request.dataset_id, dataset_id);
        assert_eq!(request.dataset_version, "01.00.000");
        assert_eq!(request.revision_checksum.as_deref(), Some("abc123"));
        assert_eq!(request.requested_by, requested_by);
        assert_eq!(job.attempt_count, 2);
    }

    #[test]
    fn rejects_wrong_review_submit_worker_job_kind() {
        let job = WorkerJob::from_json(&json!({
            "id": Uuid::new_v4(),
            "jobKind": "lca.solve_one",
            "workerQueue": REVIEW_SUBMIT_GATE_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_SUBMIT_GATE_PAYLOAD_SCHEMA_VERSION,
            "payload": {},
            "requestedBy": Uuid::new_v4(),
            "leaseToken": Uuid::new_v4(),
            "attemptCount": 1
        }))
        .unwrap();

        assert!(job.review_submit_gate_request().is_err());
    }
}
