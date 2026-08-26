use std::{future::Future, time::Duration};

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::oneshot,
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    db_pool::sql_string_literal,
    pgbouncer_sqlx::{self as sqlx, PgPool, Row},
};

pub const REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND: &str = "review.quality_diagnostic";
pub const REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION: &str =
    "review.quality_diagnostic.request.v1";
pub const REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE: &str = "review_quality";

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
pub struct ReviewQualityDiagnosticWorkerRequest {
    pub scope_kind: String,
    pub review_states: Vec<i32>,
    pub requested_at: Option<String>,
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

    pub fn review_quality_diagnostic_request(
        &self,
    ) -> anyhow::Result<ReviewQualityDiagnosticWorkerRequest> {
        if self.job_kind != REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND {
            return Err(anyhow::anyhow!(
                "unsupported worker job kind for review quality diagnostic: {}",
                self.job_kind
            ));
        }
        if self.worker_queue != REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE {
            return Err(anyhow::anyhow!(
                "unsupported worker queue for review quality diagnostic: {}",
                self.worker_queue
            ));
        }
        if self.payload_schema_version != REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION {
            return Err(anyhow::anyhow!(
                "unsupported review quality diagnostic payload schema: {}",
                self.payload_schema_version
            ));
        }

        let payload =
            serde_json::from_value::<ReviewQualityDiagnosticPayload>(self.payload.clone())?;
        if payload.scope.kind != "pending_review" {
            return Err(anyhow::anyhow!(
                "unsupported review quality diagnostic scope: {}",
                payload.scope.kind
            ));
        }
        let mut review_states = payload.scope.review_states;
        review_states.sort_unstable();
        review_states.dedup();
        if review_states.is_empty() {
            return Err(anyhow::anyhow!(
                "review quality diagnostic scope must include review states"
            ));
        }
        if review_states.iter().any(|state| !matches!(state, 0 | 1)) {
            return Err(anyhow::anyhow!(
                "review quality diagnostic scope supports pending review states 0 and 1 only"
            ));
        }
        let requested_by = self.requested_by.ok_or_else(|| {
            anyhow::anyhow!("review quality diagnostic worker job is missing requestedBy")
        })?;

        Ok(ReviewQualityDiagnosticWorkerRequest {
            scope_kind: payload.scope.kind,
            review_states,
            requested_at: payload.requested_at,
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

pub async fn run_with_periodic_lease_renewal<T, Work, Renew, RenewFuture>(
    heartbeat_period: Duration,
    mut renew: Renew,
    work: Work,
) -> anyhow::Result<T>
where
    Work: Future<Output = anyhow::Result<T>>,
    Renew: FnMut() -> RenewFuture + Send + 'static,
    RenewFuture: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let period = heartbeat_period.max(Duration::from_millis(1));
    let mut renewal_task: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let mut heartbeat = interval(period);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The queue path already sent the initial heartbeat before execution.
        heartbeat.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = &mut stop_rx => return Ok(()),
                _ = heartbeat.tick() => {
                    let renewal = timeout(period, renew());
                    tokio::pin!(renewal);
                    tokio::select! {
                        biased;
                        result = &mut renewal => match result {
                            Ok(result) => result?,
                            Err(_) => return Err(anyhow::anyhow!(
                                "worker lease renewal exceeded its heartbeat period"
                            )),
                        },
                        _ = &mut stop_rx => return Ok(()),
                    }
                }
            }
        }
    });

    tokio::pin!(work);
    tokio::select! {
        biased;
        renewal = &mut renewal_task => {
            match renewal {
                Ok(Ok(())) => Err(anyhow::anyhow!(
                    "worker lease renewal stopped before protected work completed"
                )),
                Ok(Err(error)) => Err(error.context(
                    "worker lease renewal failed before protected work completed"
                )),
                Err(error) => Err(anyhow::anyhow!(
                    "worker lease renewal task failed before protected work completed: {error}"
                )),
            }
        }
        result = &mut work => {
            let _ = stop_tx.send(());
            match renewal_task.await {
                Ok(Ok(())) => result,
                Ok(Err(error)) => Err(error.context(
                    "worker lease renewal failed while protected work completed"
                )),
                Err(error) => Err(anyhow::anyhow!(
                    "worker lease renewal task failed while protected work completed: {error}"
                )),
            }
        }
    }
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

fn worker_job_lease_renewal_sql(job_id: Uuid, lease_token: Uuid, lease_seconds: i32) -> String {
    format!(
        r"
        WITH _service_role AS (
            SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT private.worker_heartbeat_job(
            {}::uuid,
            {}::uuid,
            NULL::text,
            NULL::numeric,
            NULL::jsonb,
            {}
        ) AS result
        FROM _service_role
        ",
        sql_string_literal(&job_id.to_string()),
        sql_string_literal(&lease_token.to_string()),
        lease_seconds.clamp(1, 86_400),
    )
}

pub async fn renew_worker_job_lease(
    pool: &PgPool,
    job_id: Uuid,
    lease_token: Uuid,
    lease_seconds: i32,
) -> anyhow::Result<()> {
    let sql = worker_job_lease_renewal_sql(job_id, lease_token, lease_seconds);
    let row = sqlx::raw_query(&sql).fetch_one(pool).await?;
    let result = row.try_get::<Value, _>("result")?;
    ensure_ok(&result, "worker_heartbeat_job lease renewal")?;
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
struct ReviewQualityDiagnosticPayload {
    scope: ReviewQualityDiagnosticScope,
    #[serde(default)]
    requested_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReviewQualityDiagnosticScope {
    kind: String,
    review_states: Vec<i32>,
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::json;
    use tokio::{
        sync::{mpsc, oneshot},
        time::timeout,
    };
    use uuid::Uuid;

    use super::{
        REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND, REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION,
        REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE, WorkerJob, lease_heartbeat_period,
        result_write_retry_delay, run_with_periodic_lease_renewal, worker_job_lease_renewal_sql,
    };

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn initial_interval_tick_does_not_duplicate_the_queue_heartbeat() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let renewal_counter = Arc::clone(&renewals);
        run_with_periodic_lease_renewal(
            Duration::from_secs(1),
            move || {
                renewal_counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            },
            async { Ok(()) },
        )
        .await
        .expect("immediate protected work completes");

        assert_eq!(renewals.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_lease_renewal_repeats_and_stops_cleanly() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let renewal_counter = Arc::clone(&renewals);
        let renewal_task_dropped = Arc::new(AtomicBool::new(false));
        let renewal_drop = DropFlag(Arc::clone(&renewal_task_dropped));
        let (renewal_tx, mut renewal_rx) = mpsc::unbounded_channel();
        let result = timeout(
            Duration::from_secs(1),
            run_with_periodic_lease_renewal(
                Duration::from_millis(5),
                move || {
                    let _renewal_task_lifetime = &renewal_drop;
                    renewal_counter.fetch_add(1, Ordering::SeqCst);
                    renewal_tx
                        .send(())
                        .expect("protected work receives renewal");
                    async { Ok(()) }
                },
                async move {
                    for _ in 0..3 {
                        renewal_rx
                            .recv()
                            .await
                            .expect("renewal task remains active");
                    }
                    Ok(42_u8)
                },
            ),
        )
        .await
        .expect("renewal orchestration completes before test deadline")
        .expect("protected work completes");

        assert_eq!(result, 42);
        assert_eq!(renewals.load(Ordering::SeqCst), 3);
        assert!(renewal_task_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewal_task_is_not_starved_by_cpu_heavy_work_poll() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let renewal_counter = Arc::clone(&renewals);
        let (renewal_tx, mut renewal_rx) = mpsc::unbounded_channel();
        let renewals_during_block = timeout(
            Duration::from_secs(1),
            run_with_periodic_lease_renewal(
                Duration::from_millis(5),
                move || {
                    let renewal_counter = Arc::clone(&renewal_counter);
                    let renewal_tx = renewal_tx.clone();
                    async move {
                        let count = renewal_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        renewal_tx
                            .send(count)
                            .expect("protected work receives renewal count");
                        Ok(())
                    }
                },
                async move {
                    let before = renewal_rx.recv().await.expect("first renewal");
                    std::thread::sleep(Duration::from_millis(35));
                    Ok(renewals.load(Ordering::SeqCst).saturating_sub(before))
                },
            ),
        )
        .await
        .expect("CPU-heavy orchestration completes before test deadline")
        .expect("CPU-heavy protected work completes");

        assert!(renewals_during_block >= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renewal_failure_cancels_unfinished_work_and_joins_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let work_drop = Arc::clone(&dropped);
        let renewal_task_dropped = Arc::new(AtomicBool::new(false));
        let renewal_drop = DropFlag(Arc::clone(&renewal_task_dropped));
        let error = timeout(
            Duration::from_secs(1),
            run_with_periodic_lease_renewal(
                Duration::from_millis(5),
                move || {
                    let _renewal_task_lifetime = &renewal_drop;
                    async { Err(anyhow::anyhow!("lease lost")) }
                },
                async move {
                    let _drop_flag = DropFlag(work_drop);
                    pending::<()>().await;
                    Ok(())
                },
            ),
        )
        .await
        .expect("lease-loss orchestration completes before test deadline")
        .expect_err("renewal failure must cancel protected work");

        assert!(error.to_string().contains("lease renewal failed"));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(renewal_task_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn work_completion_does_not_mask_an_in_flight_renewal_failure() {
        let (renewal_started_tx, mut renewal_started_rx) = mpsc::unbounded_channel();
        let (release_renewal_tx, release_renewal_rx) = oneshot::channel();
        let mut release_renewal_rx = Some(release_renewal_rx);
        let error = timeout(
            Duration::from_secs(1),
            run_with_periodic_lease_renewal(
                Duration::from_millis(5),
                move || {
                    renewal_started_tx
                        .send(())
                        .expect("protected work observes in-flight renewal");
                    let release_renewal_rx = release_renewal_rx
                        .take()
                        .expect("test expects exactly one renewal");
                    async move {
                        release_renewal_rx.await.expect("test releases renewal");
                        Err(anyhow::anyhow!("lease lost at work completion"))
                    }
                },
                async move {
                    renewal_started_rx
                        .recv()
                        .await
                        .expect("renewal starts before work completes");
                    release_renewal_tx
                        .send(())
                        .expect("in-flight renewal remains attached");
                    Ok(())
                },
            ),
        )
        .await
        .expect("renewal race resolves before test deadline")
        .expect_err("completed work must not mask lease loss");

        assert!(error.to_string().contains("lease renewal failed"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_renewal_fails_closed_before_the_lease_can_expire() {
        let work_dropped = Arc::new(AtomicBool::new(false));
        let work_drop = Arc::clone(&work_dropped);
        let error = timeout(
            Duration::from_secs(1),
            run_with_periodic_lease_renewal(
                Duration::from_millis(5),
                pending::<anyhow::Result<()>>,
                async move {
                    let _drop_flag = DropFlag(work_drop);
                    pending::<()>().await;
                    Ok(())
                },
            ),
        )
        .await
        .expect("stalled-renewal orchestration completes before test deadline")
        .expect_err("a stalled renewal must cancel protected work");

        assert!(
            error
                .chain()
                .any(|cause| cause.to_string().contains("exceeded its heartbeat period"))
        );
        assert!(work_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn lease_renewal_preserves_phase_progress_and_diagnostics() {
        let sql = worker_job_lease_renewal_sql(Uuid::new_v4(), Uuid::new_v4(), 900);
        assert!(sql.contains("NULL::text"));
        assert!(sql.contains("NULL::numeric"));
        assert!(sql.contains("NULL::jsonb"));
        assert!(!sql.contains("0.05"));
        assert!(!sql.contains("0.70"));
    }
    #[test]
    fn parses_review_quality_diagnostic_worker_job_payload() {
        let requested_by = Uuid::new_v4();
        let job = WorkerJob::from_json(&json!({
            "id": Uuid::new_v4(),
            "jobKind": REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND,
            "workerQueue": REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION,
            "payload": {
                "scope": {
                    "kind": "pending_review",
                    "reviewStates": [0, 1]
                },
                "requestedAt": "2026-08-13T09:00:00Z"
            },
            "requestedBy": requested_by,
            "leaseToken": Uuid::new_v4(),
            "attemptCount": 1
        }))
        .unwrap();

        let request = job.review_quality_diagnostic_request().unwrap();

        assert_eq!(request.scope_kind, "pending_review");
        assert_eq!(request.review_states, vec![0, 1]);
        assert_eq!(
            request.requested_at.as_deref(),
            Some("2026-08-13T09:00:00Z")
        );
        assert_eq!(request.requested_by, requested_by);
    }

    #[test]
    fn rejects_review_quality_diagnostic_without_pending_review_scope() {
        let job = WorkerJob::from_json(&json!({
            "id": Uuid::new_v4(),
            "jobKind": REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND,
            "workerQueue": REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION,
            "payload": {
                "scope": {
                    "kind": "single_dataset",
                    "reviewStates": [0, 1]
                }
            },
            "requestedBy": Uuid::new_v4(),
            "leaseToken": Uuid::new_v4(),
            "attemptCount": 1
        }))
        .unwrap();

        assert!(job.review_quality_diagnostic_request().is_err());
    }

    #[test]
    fn rejects_review_quality_diagnostic_with_non_pending_state() {
        let job = WorkerJob::from_json(&json!({
            "id": Uuid::new_v4(),
            "jobKind": REVIEW_QUALITY_DIAGNOSTIC_JOB_KIND,
            "workerQueue": REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE,
            "payloadSchemaVersion": REVIEW_QUALITY_DIAGNOSTIC_PAYLOAD_SCHEMA_VERSION,
            "payload": {
                "scope": {
                    "kind": "pending_review",
                    "reviewStates": [0, 100]
                }
            },
            "requestedBy": Uuid::new_v4(),
            "leaseToken": Uuid::new_v4(),
            "attemptCount": 1
        }))
        .unwrap();

        assert!(job.review_quality_diagnostic_request().is_err());
    }
}
