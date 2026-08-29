use std::{sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{info, warn};

use crate::{
    pgbouncer_sqlx::PgPool,
    worker_jobs::{
        FailureDisposition, WorkerJob, WorkerJobProgress, WorkerJobResult, claim_worker_jobs,
        lease_heartbeat_period, record_worker_job_result_reliably,
    },
};

use super::tidas_suggestion::{
    AI_TIDAS_SUGGESTION_JOB_KIND, AI_TIDAS_SUGGESTION_REQUEST_SCHEMA_VERSION,
    AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION, AiSuggestionStatus, AiTidasSuggestionRequest,
    AiTidasSuggestionRuntime,
};

pub const AI_WORKER_QUEUE: &str = "ai";

#[derive(Debug, Clone)]
pub struct AiWorkerOptions {
    pub poll_interval: Duration,
    pub max_runs: Option<usize>,
    pub exit_when_idle: bool,
    pub worker_id: String,
    pub claim_limit: i32,
    pub lease_seconds: i32,
}

impl Default for AiWorkerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_runs: None,
            exit_when_idle: false,
            worker_id: format!("ai-worker-{}", std::process::id()),
            claim_limit: 1,
            lease_seconds: 900,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AiWorkerSummary {
    pub claimed: usize,
    pub complete: usize,
    pub partial: usize,
    pub failed: usize,
    pub invalid: usize,
    pub idle_polls: usize,
}

pub async fn run_ai_worker(
    pool: &PgPool,
    runtime: Arc<AiTidasSuggestionRuntime>,
    options: AiWorkerOptions,
) -> anyhow::Result<AiWorkerSummary> {
    let mut summary = AiWorkerSummary::default();
    loop {
        if options
            .max_runs
            .is_some_and(|max_runs| summary.claimed >= max_runs)
        {
            break;
        }

        let remaining = options
            .max_runs
            .map_or(options.claim_limit, |max_runs| {
                i32::try_from(max_runs.saturating_sub(summary.claimed))
                    .unwrap_or(i32::MAX)
                    .min(options.claim_limit)
            })
            .clamp(1, 50);
        let jobs = claim_worker_jobs(
            pool,
            AI_WORKER_QUEUE,
            &options.worker_id,
            remaining,
            options.lease_seconds,
        )
        .await?;

        if jobs.is_empty() {
            summary.idle_polls += 1;
            if options.exit_when_idle {
                break;
            }
            sleep(options.poll_interval).await;
            continue;
        }

        for job in jobs {
            summary.claimed += 1;
            match process_claimed_job(pool, runtime.as_ref(), &job, options.lease_seconds).await? {
                AiJobOutcome::Complete => summary.complete += 1,
                AiJobOutcome::Partial => summary.partial += 1,
                AiJobOutcome::Failed => summary.failed += 1,
                AiJobOutcome::Invalid => summary.invalid += 1,
            }
        }
    }
    Ok(summary)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AiJobOutcome {
    Complete,
    Partial,
    Failed,
    Invalid,
}

async fn process_claimed_job(
    pool: &PgPool,
    runtime: &AiTidasSuggestionRuntime,
    job: &WorkerJob,
    lease_seconds: i32,
) -> anyhow::Result<AiJobOutcome> {
    let request = match parse_tidas_suggestion_request(job) {
        Ok(request) => request,
        Err(error) => {
            let result = WorkerJobResult::failed(
                "invalid_ai_worker_job",
                "AI worker job payload or contract is invalid",
                json!({ "reason": error.to_string() }),
                Some(job_diagnostics(job, "rejected")),
                None,
                FailureDisposition::NonRetryable,
            );
            record_worker_job_result_reliably(pool, job.id, job.lease_token, result).await?;
            return Ok(AiJobOutcome::Invalid);
        }
    };

    let progress = WorkerJobProgress::new(pool, job.id, job.lease_token, lease_seconds);
    progress
        .heartbeat(
            "ai_tidas_suggestion",
            0.05,
            Some(job_diagnostics(job, "started")),
        )
        .await?;

    let execution = runtime.execute(request);
    tokio::pin!(execution);
    let mut heartbeat = interval(lease_heartbeat_period(progress.lease_seconds()));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let result = loop {
        tokio::select! {
            execution_result = &mut execution => break execution_result,
            _ = heartbeat.tick() => {
                progress
                    .heartbeat(
                        "ai_tidas_suggestion",
                        0.5,
                        Some(job_diagnostics(job, "model_requests_in_progress")),
                    )
                    .await?;
            }
        }
    };

    let suggestion = match result {
        Ok(suggestion) => suggestion,
        Err(error) => {
            warn!(worker_job_id = %job.id, error = %error, "AI job handler failed");
            let result = WorkerJobResult::failed(
                "ai_tidas_suggestion_runtime_error",
                "AI TIDAS suggestion handler failed before producing a result",
                json!({ "reason": safe_runtime_error(&error) }),
                Some(job_diagnostics(job, "runtime_error")),
                None,
                FailureDisposition::from_retryable(is_retryable_runtime_error(&error)),
            );
            record_worker_job_result_reliably(pool, job.id, job.lease_token, result).await?;
            return Ok(AiJobOutcome::Failed);
        }
    };

    let status = suggestion.status;
    let result_json = serde_json::to_value(&suggestion)?;
    let terminal = match status {
        AiSuggestionStatus::Complete | AiSuggestionStatus::Partial => {
            let mut result =
                WorkerJobResult::completed(result_json, AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION);
            result.diagnostics = Some(job_diagnostics(job, "completed"));
            result
        }
        AiSuggestionStatus::Failed => {
            let retryable = suggestion.failures.iter().any(|failure| failure.retryable);
            let mut result = WorkerJobResult::failed(
                "ai_tidas_suggestion_failed",
                "AI TIDAS suggestion handler could not process any matched field",
                json!({
                    "failedPathCount": suggestion.summary.failed_path_count,
                    "retryable": retryable
                }),
                Some(job_diagnostics(job, "failed")),
                Some(result_json),
                FailureDisposition::from_retryable(retryable),
            );
            result.result_schema_version =
                Some(AI_TIDAS_SUGGESTION_RESULT_SCHEMA_VERSION.to_owned());
            result
        }
    };
    record_worker_job_result_reliably(pool, job.id, job.lease_token, terminal).await?;
    info!(worker_job_id = %job.id, ?status, "AI worker job recorded");
    Ok(match status {
        AiSuggestionStatus::Complete => AiJobOutcome::Complete,
        AiSuggestionStatus::Partial => AiJobOutcome::Partial,
        AiSuggestionStatus::Failed => AiJobOutcome::Failed,
    })
}

fn parse_tidas_suggestion_request(job: &WorkerJob) -> anyhow::Result<AiTidasSuggestionRequest> {
    if job.worker_queue != AI_WORKER_QUEUE {
        anyhow::bail!("unsupported AI worker queue: {}", job.worker_queue);
    }
    if job.job_kind != AI_TIDAS_SUGGESTION_JOB_KIND {
        anyhow::bail!("unsupported AI worker job kind: {}", job.job_kind);
    }
    if job.payload_schema_version != AI_TIDAS_SUGGESTION_REQUEST_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported AI TIDAS suggestion payload schema: {}",
            job.payload_schema_version
        );
    }
    serde_json::from_value(job.payload.clone()).map_err(Into::into)
}

fn job_diagnostics(job: &WorkerJob, phase: &str) -> Value {
    json!({
        "runner": "ai-worker",
        "handler": AI_TIDAS_SUGGESTION_JOB_KIND,
        "workerJobId": job.id,
        "attemptCount": job.attempt_count,
        "phase": phase
    })
}

fn safe_runtime_error(error: &anyhow::Error) -> &'static str {
    let text = error.to_string();
    if text.starts_with("ai_tidas_input_too_large") {
        "input_too_large"
    } else if text.starts_with("ai_tidas_input_invalid") {
        "input_invalid"
    } else if text.starts_with("ai_ruleset_missing") {
        "ruleset_missing"
    } else {
        "runtime_error"
    }
}

fn is_retryable_runtime_error(error: &anyhow::Error) -> bool {
    let text = error.to_string();
    !(text.starts_with("ai_tidas_input_") || text.starts_with("ai_ruleset_missing"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{AI_WORKER_QUEUE, parse_tidas_suggestion_request};
    use crate::{
        ai::tidas_suggestion::{
            AI_TIDAS_SUGGESTION_JOB_KIND, AI_TIDAS_SUGGESTION_REQUEST_SCHEMA_VERSION,
            TidasDatasetType,
        },
        worker_jobs::WorkerJob,
    };

    fn job() -> WorkerJob {
        WorkerJob {
            id: Uuid::new_v4(),
            job_kind: AI_TIDAS_SUGGESTION_JOB_KIND.to_owned(),
            worker_queue: AI_WORKER_QUEUE.to_owned(),
            payload_schema_version: AI_TIDAS_SUGGESTION_REQUEST_SCHEMA_VERSION.to_owned(),
            payload: json!({
                "dataType": "process",
                "data": {"processDataSet": {"name": "test"}}
            }),
            requested_by: Some(Uuid::new_v4()),
            lease_token: Uuid::new_v4(),
            attempt_count: 1,
        }
    }

    #[test]
    fn accepts_versioned_tidas_suggestion_job() {
        let request = parse_tidas_suggestion_request(&job()).unwrap();
        assert_eq!(request.data_type, TidasDatasetType::Process);
    }

    #[test]
    fn rejects_future_schema_version() {
        let mut job = job();
        job.payload_schema_version = "ai.tidas_suggestion.request.v2".to_owned();
        assert!(parse_tidas_suggestion_request(&job).is_err());
    }
}
