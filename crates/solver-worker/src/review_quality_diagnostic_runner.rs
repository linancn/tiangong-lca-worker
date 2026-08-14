#![allow(
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    db::{self, AppState},
    graph_types::RequestRootProcess,
    pgbouncer_sqlx::{self as sqlx, Row},
    readiness::{
        FindingSeverity, MatrixReadinessInput, MatrixReadinessPolicy, MatrixReadinessReport,
        ReadinessFinding, verify_matrix_readiness,
    },
    worker_jobs::{
        REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE, ReviewQualityDiagnosticWorkerRequest, WorkerJob,
        WorkerJobProgress, WorkerJobResult, claim_worker_jobs, record_worker_job_result_reliably,
    },
};

pub const REVIEW_QUALITY_DIAGNOSTIC_REPORT_SCHEMA_VERSION: &str =
    "review.quality_diagnostic.report.v1";

const RUNNER_NAME: &str = "review_quality_diagnostic_runner";
const PROCESS_SAMPLE_LIMIT: usize = 50;

#[derive(Debug, Clone)]
pub struct ReviewQualityDiagnosticRunnerOptions {
    pub poll_interval: Duration,
    pub max_runs: Option<usize>,
    pub exit_when_idle: bool,
    pub worker_id: String,
    pub lease_seconds: i32,
}

impl Default for ReviewQualityDiagnosticRunnerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_runs: None,
            exit_when_idle: false,
            worker_id: RUNNER_NAME.to_owned(),
            lease_seconds: 900,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReviewQualityDiagnosticRunnerSummary {
    pub claimed: usize,
    pub clear: usize,
    pub findings: usize,
    pub not_evaluable: usize,
    pub errors: usize,
    pub idle_polls: usize,
}

impl ReviewQualityDiagnosticRunnerSummary {
    fn record(&mut self, outcome: RecordedDiagnosticOutcome) {
        self.claimed += 1;
        match outcome {
            RecordedDiagnosticOutcome::Clear => self.clear += 1,
            RecordedDiagnosticOutcome::Findings => self.findings += 1,
            RecordedDiagnosticOutcome::NotEvaluable => self.not_evaluable += 1,
            RecordedDiagnosticOutcome::Error => self.errors += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedDiagnosticOutcome {
    Clear,
    Findings,
    NotEvaluable,
    Error,
}

impl RecordedDiagnosticOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Clear => "clear",
            Self::Findings => "findings",
            Self::NotEvaluable => "not_evaluable",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingReviewTarget {
    target_table: Option<String>,
    data_id: Option<Uuid>,
    data_version: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingReviewScope {
    review_count: usize,
    dataset_count: usize,
    dataset_counts: BTreeMap<String, usize>,
    process_roots: Vec<RequestRootProcess>,
}

#[derive(Debug, Clone)]
struct DiagnosticExecution {
    outcome: RecordedDiagnosticOutcome,
    report: Value,
    diagnostics: Value,
}

pub async fn run_review_quality_diagnostic_runner(
    state: &AppState,
    options: ReviewQualityDiagnosticRunnerOptions,
) -> anyhow::Result<ReviewQualityDiagnosticRunnerSummary> {
    let mut summary = ReviewQualityDiagnosticRunnerSummary::default();

    loop {
        if options
            .max_runs
            .is_some_and(|max_runs| summary.claimed >= max_runs)
        {
            break;
        }

        if let Some(outcome) = run_one_review_quality_diagnostic(state, &options).await? {
            summary.record(outcome);
        } else {
            summary.idle_polls += 1;
            if options.exit_when_idle {
                break;
            }
            sleep(options.poll_interval).await;
        }
    }

    Ok(summary)
}

pub async fn run_one_review_quality_diagnostic(
    state: &AppState,
    options: &ReviewQualityDiagnosticRunnerOptions,
) -> anyhow::Result<Option<RecordedDiagnosticOutcome>> {
    let jobs = claim_worker_jobs(
        &state.queue_pool,
        REVIEW_QUALITY_DIAGNOSTIC_WORKER_QUEUE,
        &options.worker_id,
        1,
        options.lease_seconds,
    )
    .await?;

    let Some(job) = jobs.into_iter().next() else {
        return Ok(None);
    };

    let outcome = process_claimed_review_quality_diagnostic(state, &job, options).await?;
    Ok(Some(outcome))
}

async fn process_claimed_review_quality_diagnostic(
    state: &AppState,
    job: &WorkerJob,
    options: &ReviewQualityDiagnosticRunnerOptions,
) -> anyhow::Result<RecordedDiagnosticOutcome> {
    let request = match job.review_quality_diagnostic_request() {
        Ok(request) => request,
        Err(error) => {
            record_worker_job_result_reliably(
                &state.queue_pool,
                job.id,
                job.lease_token,
                WorkerJobResult::failed(
                    "invalid_review_quality_diagnostic_job",
                    "review quality diagnostic worker job payload is invalid",
                    json!({ "error": error.to_string() }),
                    Some(json!({ "runner": RUNNER_NAME, "workerJobId": job.id })),
                    None,
                ),
            )
            .await?;
            return Ok(RecordedDiagnosticOutcome::Error);
        }
    };
    let progress = WorkerJobProgress::new(
        &state.queue_pool,
        job.id,
        job.lease_token,
        options.lease_seconds,
    );

    let execution = match execute_diagnostic(state, job, &request, &progress).await {
        Ok(execution) => execution,
        Err(error) => {
            warn!(
                worker_job_id = %job.id,
                error = %error,
                "review quality diagnostic failed before a report could be produced"
            );
            record_worker_job_result_reliably(
                &state.queue_pool,
                job.id,
                job.lease_token,
                WorkerJobResult::failed(
                    "review_quality_diagnostic_runtime_error",
                    "review quality diagnostic worker failed before producing a report",
                    json!({ "error": error.to_string() }),
                    Some(json!({ "runner": RUNNER_NAME, "workerJobId": job.id })),
                    None,
                ),
            )
            .await?;
            return Ok(RecordedDiagnosticOutcome::Error);
        }
    };

    let mut result = WorkerJobResult::completed(
        execution.report,
        REVIEW_QUALITY_DIAGNOSTIC_REPORT_SCHEMA_VERSION,
    );
    result.diagnostics = Some(execution.diagnostics);
    record_worker_job_result_reliably(&state.queue_pool, job.id, job.lease_token, result).await?;

    info!(
        worker_job_id = %job.id,
        outcome = execution.outcome.as_str(),
        "recorded informational Review Admin quality diagnostic"
    );
    Ok(execution.outcome)
}

async fn execute_diagnostic(
    state: &AppState,
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    progress: &WorkerJobProgress<'_>,
) -> anyhow::Result<DiagnosticExecution> {
    progress
        .heartbeat("collecting_pending_review_scope", 0.05, None)
        .await?;
    let scope = fetch_pending_review_scope(&state.pool, &request.review_states).await?;

    if scope.process_roots.is_empty() {
        let report = empty_process_scope_report(job, request, &scope);
        return Ok(DiagnosticExecution {
            outcome: RecordedDiagnosticOutcome::Clear,
            report,
            diagnostics: json!({
                "runner": RUNNER_NAME,
                "matrixBuildInvoked": false,
                "reason": "no_pending_process_reviews"
            }),
        });
    }

    progress
        .heartbeat(
            "building_pending_review_matrix",
            0.15,
            Some(json!({
                "pendingReviewCount": scope.review_count,
                "pendingProcessCount": scope.process_roots.len()
            })),
        )
        .await?;
    let snapshot = match run_snapshot_with_heartbeat(state, job.id, &scope, progress).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(execution) = snapshot_not_evaluable_execution(job, request, &scope, &error)
            {
                return Ok(execution);
            }
            return Err(error);
        }
    };

    progress
        .heartbeat(
            "evaluating_matrix_quality",
            0.75,
            Some(json!({ "snapshotId": snapshot.resolved_snapshot_id })),
        )
        .await?;
    let artifact =
        db::fetch_decoded_snapshot_artifact(state, snapshot.resolved_snapshot_id).await?;
    let compiled_graph = artifact
        .review_gate_evidence
        .map(crate::snapshot_artifacts::SnapshotReviewGateEvidence::into_compiled_graph)
        .or(artifact.compiled_graph);
    let readiness = verify_matrix_readiness(&MatrixReadinessInput {
        schema_version: "matrix_readiness_input.v2".to_owned(),
        snapshot_id: Some(snapshot.resolved_snapshot_id),
        config: Some(artifact.config),
        coverage: artifact.coverage,
        payload: artifact.payload,
        compiled_graph,
        policy: MatrixReadinessPolicy {
            require_lcia_factors: false,
            ..MatrixReadinessPolicy::default()
        },
    });
    let (outcome, report) = readiness_report(job, request, &scope, &readiness);

    progress
        .heartbeat(
            "recording_report",
            0.95,
            Some(json!({ "outcome": outcome.as_str() })),
        )
        .await?;
    Ok(DiagnosticExecution {
        outcome,
        report,
        diagnostics: json!({
            "runner": RUNNER_NAME,
            "requestedSnapshotId": snapshot.requested_snapshot_id,
            "resolvedSnapshotId": snapshot.resolved_snapshot_id,
            "snapshotBuilder": {
                "exitCode": snapshot.exit_code,
                "command": snapshot.command,
                "buildTimingSec": snapshot.build_timing_sec,
                "stdoutTail": snapshot.stdout_tail,
                "stderrTail": snapshot.stderr_tail
            }
        }),
    })
}

async fn run_snapshot_with_heartbeat(
    state: &AppState,
    snapshot_id: Uuid,
    scope: &PendingReviewScope,
    progress: &WorkerJobProgress<'_>,
) -> anyhow::Result<db::SnapshotBuilderExecution> {
    let build = db::run_review_quality_diagnostic_snapshot_builder(
        state,
        snapshot_id,
        scope.process_roots.as_slice(),
    );
    let mut build = Box::pin(build);
    loop {
        tokio::select! {
            result = &mut build => return result,
            () = sleep(Duration::from_secs(5)) => {
                progress.heartbeat(
                    "building_pending_review_matrix",
                    0.40,
                    Some(json!({
                        "pendingProcessCount": scope.process_roots.len(),
                        "snapshotBuilder": { "running": true }
                    })),
                ).await?;
            }
        }
    }
}

async fn fetch_pending_review_scope(
    pool: &sqlx::PgPool,
    review_states: &[i32],
) -> anyhow::Result<PendingReviewScope> {
    let rows = sqlx::query(
        r"
        SELECT target_table, data_id, btrim(data_version::text) AS data_version
        FROM private.reviews
        WHERE review_kind IN ('root', 'reference')
          AND state_code = ANY($1)
        ORDER BY target_table, data_id, btrim(data_version::text), id
        ",
    )
    .bind(review_states)
    .fetch_all(pool)
    .await?;
    let targets = rows
        .iter()
        .map(|row| {
            Ok(PendingReviewTarget {
                target_table: row.try_get::<Option<String>, _>("target_table")?,
                data_id: row.try_get::<Option<Uuid>, _>("data_id")?,
                data_version: row.try_get::<Option<String>, _>("data_version")?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(summarize_pending_review_targets(targets.as_slice()))
}

fn summarize_pending_review_targets(targets: &[PendingReviewTarget]) -> PendingReviewScope {
    let mut datasets = BTreeSet::<(String, Uuid, String)>::new();
    for target in targets {
        if let (Some(table), Some(id), Some(version)) = (
            target.target_table.as_deref(),
            target.data_id,
            target.data_version.as_deref(),
        ) {
            datasets.insert((table.to_owned(), id, version.trim().to_owned()));
        }
    }
    let mut dataset_counts = BTreeMap::<String, usize>::new();
    let mut process_roots = BTreeSet::<RequestRootProcess>::new();
    for (table, id, version) in &datasets {
        *dataset_counts.entry(table.clone()).or_default() += 1;
        if table == "processes" {
            process_roots.insert(RequestRootProcess::new(*id, version));
        }
    }

    PendingReviewScope {
        review_count: targets.len(),
        dataset_count: datasets.len(),
        dataset_counts,
        process_roots: process_roots.into_iter().collect(),
    }
}

fn readiness_report(
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    scope: &PendingReviewScope,
    readiness: &MatrixReadinessReport,
) -> (RecordedDiagnosticOutcome, Value) {
    let mut completeness = Vec::new();
    let mut numerical = Vec::new();
    for finding in readiness.findings.iter().chain(readiness.blockers.iter()) {
        let normalized = normalize_readiness_finding(finding);
        if finding_category(&finding.code) == "numerical_stability" {
            numerical.push(normalized);
        } else {
            completeness.push(normalized);
        }
    }
    let outcome = if completeness.is_empty() && numerical.is_empty() {
        RecordedDiagnosticOutcome::Clear
    } else {
        RecordedDiagnosticOutcome::Findings
    };
    let all_findings = completeness
        .iter()
        .chain(numerical.iter())
        .cloned()
        .collect::<Vec<_>>();
    let report = report_envelope(
        job,
        request,
        scope,
        outcome,
        &json!({
            "processCount": readiness.metrics.graph_readiness.process_count,
            "flowCount": readiness.metrics.graph_readiness.flow_count,
            "inputEdgesTotal": readiness.metrics.provider_closure.input_edges_total,
            "unmatchedNoProvider": readiness.metrics.provider_closure.unmatched_no_provider,
            "factorizationReady": readiness.metrics.compute_stability.factorization_ready,
            "sampledUnitSolves": readiness.metrics.compute_stability.sampled_unit_solves,
            "findingCount": all_findings.len()
        }),
        &json!([
            {
                "key": "completeness",
                "status": section_status(completeness.as_slice()),
                "metrics": {
                    "providerClosure": readiness.metrics.provider_closure,
                    "graphReadiness": readiness.metrics.graph_readiness
                },
                "findings": completeness
            },
            {
                "key": "numerical_stability",
                "status": section_status(numerical.as_slice()),
                "metrics": {
                    "singularRiskLevel": readiness.metrics.graph_readiness.singular_risk_level,
                    "zeroDiagonalCount": readiness.metrics.graph_readiness.m_zero_diagonal_count,
                    "minAbsoluteDiagonal": readiness.metrics.graph_readiness.m_min_abs_diagonal,
                    "computeStability": readiness.metrics.compute_stability
                },
                "findings": numerical
            }
        ]),
        all_findings.as_slice(),
        readiness.snapshot_id,
    );
    (outcome, report)
}

fn empty_process_scope_report(
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    scope: &PendingReviewScope,
) -> Value {
    report_envelope(
        job,
        request,
        scope,
        RecordedDiagnosticOutcome::Clear,
        &json!({
            "pendingProcessCount": 0,
            "findingCount": 0,
            "message": "No pending Process reviews require matrix evaluation."
        }),
        &json!([
            {
                "key": "completeness",
                "status": "not_applicable",
                "metrics": {},
                "findings": []
            },
            {
                "key": "numerical_stability",
                "status": "not_applicable",
                "metrics": {},
                "findings": []
            }
        ]),
        &[],
        None,
    )
}

fn snapshot_not_evaluable_execution(
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    scope: &PendingReviewScope,
    error: &anyhow::Error,
) -> Option<DiagnosticExecution> {
    match error.downcast_ref::<db::SnapshotBuilderProcessFailure>()? {
        db::SnapshotBuilderProcessFailure::Blocked {
            code,
            blocking_reasons,
            blocking_reason_count,
            blocking_reasons_sha256,
            blocking_reasons_truncated,
            ..
        } => {
            let findings = blocking_reasons
                .iter()
                .map(normalize_snapshot_finding)
                .collect::<Vec<_>>();
            let report = not_evaluable_report(
                job,
                request,
                scope,
                *blocking_reason_count,
                findings.as_slice(),
                *blocking_reasons_truncated,
            );
            Some(DiagnosticExecution {
                outcome: RecordedDiagnosticOutcome::NotEvaluable,
                report,
                diagnostics: json!({
                    "runner": RUNNER_NAME,
                    "matrixBuildInvoked": true,
                    "snapshotBuilderBlocked": {
                        "code": code,
                        "findingCount": blocking_reason_count,
                        "findingsSha256": blocking_reasons_sha256,
                        "sampleTruncated": blocking_reasons_truncated
                    }
                }),
            })
        }
        db::SnapshotBuilderProcessFailure::Exit {
            exit_code,
            stdout_tail,
            stderr_tail,
            ..
        } if snapshot_exit_is_data_quality(stdout_tail, stderr_tail) => {
            let finding = json!({
                "code": "pending_review_matrix_build_failed",
                "category": "completeness",
                "level": "error",
                "message": "Pending-review data could not be compiled into a complete matrix.",
                "details": {
                    "exitCode": exit_code,
                    "builderMessage": stderr_tail
                },
                "workflowBlocking": false
            });
            let findings = vec![finding];
            Some(DiagnosticExecution {
                outcome: RecordedDiagnosticOutcome::NotEvaluable,
                report: not_evaluable_report(job, request, scope, 1, findings.as_slice(), false),
                diagnostics: json!({
                    "runner": RUNNER_NAME,
                    "matrixBuildInvoked": true,
                    "snapshotBuilderExit": {
                        "exitCode": exit_code,
                        "stdoutTail": stdout_tail,
                        "stderrTail": stderr_tail
                    }
                }),
            })
        }
        db::SnapshotBuilderProcessFailure::Launch { .. }
        | db::SnapshotBuilderProcessFailure::Timeout { .. }
        | db::SnapshotBuilderProcessFailure::Signal { .. }
        | db::SnapshotBuilderProcessFailure::Protocol { .. }
        | db::SnapshotBuilderProcessFailure::Exit { .. } => None,
    }
}

fn snapshot_exit_is_data_quality(stdout_tail: &str, stderr_tail: &str) -> bool {
    let text = format!("{stdout_tail}\n{stderr_tail}").to_ascii_lowercase();
    [
        "request root not found in candidate scope",
        "invalid allocation",
        "quantitative reference",
        "process exchange references flow",
        "required exact numerical-axis dependency",
        "source_dependency_unavailable",
        "source_reference_invalid",
        "source closure",
        "flow reference could not resolve",
        "no processes matched filter",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

fn not_evaluable_report(
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    scope: &PendingReviewScope,
    finding_count: u64,
    findings: &[Value],
    findings_truncated: bool,
) -> Value {
    report_envelope(
        job,
        request,
        scope,
        RecordedDiagnosticOutcome::NotEvaluable,
        &json!({
            "findingCount": finding_count,
            "matrixBuilt": false,
            "message": "Pending-review matrix could not be fully constructed; numerical stability was not evaluated."
        }),
        &json!([
            {
                "key": "completeness",
                "status": "findings",
                "metrics": {
                    "findingCount": finding_count,
                    "sampleCount": findings.len(),
                    "sampleTruncated": findings_truncated
                },
                "findings": findings
            },
            {
                "key": "numerical_stability",
                "status": "not_evaluable",
                "metrics": {},
                "findings": []
            }
        ]),
        findings,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn report_envelope(
    job: &WorkerJob,
    request: &ReviewQualityDiagnosticWorkerRequest,
    scope: &PendingReviewScope,
    outcome: RecordedDiagnosticOutcome,
    summary: &Value,
    sections: &Value,
    findings: &[Value],
    snapshot_id: Option<Uuid>,
) -> Value {
    let process_sample = scope
        .process_roots
        .iter()
        .take(PROCESS_SAMPLE_LIMIT)
        .map(|root| {
            json!({
                "id": root.process_id,
                "version": root.process_version
            })
        })
        .collect::<Vec<_>>();
    json!({
        "schemaVersion": REVIEW_QUALITY_DIAGNOSTIC_REPORT_SCHEMA_VERSION,
        "runId": job.id,
        "generatedAt": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "requestedAt": request.requested_at,
        "requestedBy": request.requested_by,
        "outcome": outcome.as_str(),
        "informationalOnly": true,
        "affectsReviewState": false,
        "scope": {
            "kind": request.scope_kind,
            "reviewStates": request.review_states,
            "reviewCount": scope.review_count,
            "datasetCount": scope.dataset_count,
            "datasetCounts": scope.dataset_counts,
            "pendingProcessCount": scope.process_roots.len(),
            "pendingProcessSample": process_sample,
            "pendingProcessSampleTruncated": scope.process_roots.len() > PROCESS_SAMPLE_LIMIT,
            "snapshotId": snapshot_id
        },
        "summary": summary,
        "sections": sections,
        "findings": findings
    })
}

fn section_status(findings: &[Value]) -> &'static str {
    if findings.is_empty() {
        "clear"
    } else {
        "findings"
    }
}

fn finding_category(code: &str) -> &'static str {
    if code.contains("singular")
        || code.contains("factorization")
        || code.contains("solve")
        || code.contains("non_finite")
        || code.contains("negative_lcia")
        || code.contains("matrix_validation")
    {
        "numerical_stability"
    } else {
        "completeness"
    }
}

fn normalize_readiness_finding(finding: &ReadinessFinding) -> Value {
    let level = match finding.severity {
        FindingSeverity::Info => "info",
        FindingSeverity::Warning => "warning",
        FindingSeverity::Blocker => "error",
    };
    json!({
        "code": finding.code,
        "category": finding_category(&finding.code),
        "level": level,
        "message": finding.message,
        "details": finding.details,
        "workflowBlocking": false
    })
}

fn normalize_snapshot_finding(finding: &Value) -> Value {
    let code = finding
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("pending_review_matrix_incomplete");
    json!({
        "code": code,
        "category": "completeness",
        "level": "error",
        "message": finding
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Pending-review matrix source data is incomplete."),
        "details": finding.get("details").cloned().unwrap_or_else(|| finding.clone()),
        "workflowBlocking": false
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        PendingReviewScope, PendingReviewTarget, RecordedDiagnosticOutcome,
        ReviewQualityDiagnosticWorkerRequest, empty_process_scope_report, finding_category,
        snapshot_exit_is_data_quality, snapshot_not_evaluable_execution,
        summarize_pending_review_targets,
    };
    use crate::{db::SnapshotBuilderProcessFailure, worker_jobs::WorkerJob};

    fn job() -> WorkerJob {
        WorkerJob {
            id: Uuid::new_v4(),
            job_kind: "review.quality_diagnostic".to_owned(),
            worker_queue: "review_quality".to_owned(),
            payload_schema_version: "review.quality_diagnostic.request.v1".to_owned(),
            payload: json!({}),
            requested_by: Some(Uuid::new_v4()),
            lease_token: Uuid::new_v4(),
            attempt_count: 1,
        }
    }

    fn request() -> ReviewQualityDiagnosticWorkerRequest {
        ReviewQualityDiagnosticWorkerRequest {
            scope_kind: "pending_review".to_owned(),
            review_states: vec![0, 1],
            requested_at: None,
            requested_by: Uuid::new_v4(),
        }
    }

    #[test]
    fn pending_review_scope_deduplicates_process_targets() {
        let process_id = Uuid::new_v4();
        let targets = vec![
            PendingReviewTarget {
                target_table: Some("processes".to_owned()),
                data_id: Some(process_id),
                data_version: Some("01.00.000".to_owned()),
            },
            PendingReviewTarget {
                target_table: Some("processes".to_owned()),
                data_id: Some(process_id),
                data_version: Some("01.00.000".to_owned()),
            },
            PendingReviewTarget {
                target_table: Some("flows".to_owned()),
                data_id: Some(Uuid::new_v4()),
                data_version: Some("01.00.000".to_owned()),
            },
        ];

        let scope = summarize_pending_review_targets(&targets);

        assert_eq!(scope.review_count, 3);
        assert_eq!(scope.dataset_count, 2);
        assert_eq!(scope.process_roots.len(), 1);
        assert_eq!(scope.dataset_counts.get("processes"), Some(&1));
        assert_eq!(scope.dataset_counts.get("flows"), Some(&1));
    }

    #[test]
    fn empty_process_scope_is_clear_and_explicitly_informational() {
        let report = empty_process_scope_report(
            &job(),
            &request(),
            &PendingReviewScope {
                review_count: 1,
                dataset_count: 1,
                dataset_counts: [("flows".to_owned(), 1)].into_iter().collect(),
                process_roots: Vec::new(),
            },
        );

        assert_eq!(report["outcome"], "clear");
        assert_eq!(report["informationalOnly"], true);
        assert_eq!(report["affectsReviewState"], false);
        assert_eq!(report["sections"][1]["status"], "not_applicable");
    }

    #[test]
    fn source_incompleteness_is_completed_as_not_evaluable_not_blocked() {
        let error = anyhow::Error::from(SnapshotBuilderProcessFailure::Blocked {
            code: "source_dependency_unavailable".to_owned(),
            blocking_reasons: vec![json!({
                "code": "source_dependency_unavailable",
                "message": "missing flow"
            })],
            blocking_reason_count: 1,
            blocking_reasons_sha256: "abc".to_owned(),
            blocking_reasons_truncated: false,
            blocking_reasons_spool: None,
        });
        let scope = PendingReviewScope {
            review_count: 1,
            dataset_count: 1,
            dataset_counts: [("processes".to_owned(), 1)].into_iter().collect(),
            process_roots: vec![crate::graph_types::RequestRootProcess::new(
                Uuid::new_v4(),
                "01.00.000",
            )],
        };

        let execution = snapshot_not_evaluable_execution(&job(), &request(), &scope, &error)
            .expect("structured not-evaluable report");

        assert_eq!(execution.outcome, RecordedDiagnosticOutcome::NotEvaluable);
        assert_eq!(execution.report["outcome"], "not_evaluable");
        assert_eq!(execution.report["findings"][0]["workflowBlocking"], false);
    }

    #[test]
    fn finding_categories_separate_matrix_stability_from_completeness() {
        assert_eq!(
            finding_category("factorization_not_ready"),
            "numerical_stability"
        );
        assert_eq!(
            finding_category("provider_closure_unmatched"),
            "completeness"
        );
    }

    #[test]
    fn only_data_quality_snapshot_exits_become_not_evaluable() {
        assert!(snapshot_exit_is_data_quality(
            "",
            "request root not found in candidate scope"
        ));
        assert!(!snapshot_exit_is_data_quality(
            "",
            "S3 upload failed: connection reset"
        ));
    }
}
