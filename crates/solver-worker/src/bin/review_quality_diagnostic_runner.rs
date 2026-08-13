use std::time::Duration;

use clap::Parser;
use solver_worker::{
    config::AppConfig,
    db::AppState,
    db_pool::{APP_REVIEW_QUALITY_DIAGNOSTIC_RUNNER, APP_REVIEW_QUALITY_DIAGNOSTIC_RUNNER_QUEUE},
    review_quality_diagnostic_runner::{
        ReviewQualityDiagnosticRunnerOptions, run_review_quality_diagnostic_runner,
    },
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "review-quality-diagnostic-runner")]
#[command(about = "Run manual, informational Review Admin diagnostics for pending reviews.")]
struct Cli {
    #[command(flatten)]
    config: AppConfig,
    #[arg(long, env = "REVIEW_QUALITY_POLL_MS", default_value_t = 1_000_u64)]
    poll_ms: u64,
    #[arg(long, env = "REVIEW_QUALITY_MAX_RUNS")]
    max_runs: Option<usize>,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(
        long = "review-quality-worker-id",
        env = "REVIEW_QUALITY_WORKER_ID",
        default_value = "review_quality_diagnostic_runner"
    )]
    worker_id: String,
    #[arg(
        long = "review-quality-worker-lease-seconds",
        env = "REVIEW_QUALITY_WORKER_LEASE_SECONDS",
        default_value_t = 900_i32
    )]
    lease_seconds: i32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let state = AppState::new_with_application_names(
        &cli.config,
        APP_REVIEW_QUALITY_DIAGNOSTIC_RUNNER,
        APP_REVIEW_QUALITY_DIAGNOSTIC_RUNNER_QUEUE,
    )
    .await?;
    let summary = run_review_quality_diagnostic_runner(
        &state,
        ReviewQualityDiagnosticRunnerOptions {
            poll_interval: Duration::from_millis(cli.poll_ms.max(100)),
            max_runs: cli.max_runs.or_else(|| cli.once.then_some(1)),
            exit_when_idle: cli.once,
            worker_id: cli.worker_id,
            lease_seconds: cli.lease_seconds.max(60),
        },
    )
    .await?;
    info!(?summary, "review quality diagnostic runner stopped");
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
