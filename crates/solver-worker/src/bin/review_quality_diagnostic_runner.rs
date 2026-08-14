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
    review_quality_worker_id: String,
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
            worker_id: cli.review_quality_worker_id,
            lease_seconds: cli.lease_seconds.max(60),
        },
    )
    .await?;
    info!(?summary, "review quality diagnostic runner stopped");
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        sync::{Mutex, MutexGuard},
    };

    use super::*;

    static WORKER_ID_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct WorkerIdEnvGuard {
        _lock: MutexGuard<'static, ()>,
        previous_worker_id: Option<OsString>,
        previous_review_quality_worker_id: Option<OsString>,
    }

    impl WorkerIdEnvGuard {
        fn set(worker_id: Option<&str>, review_quality_worker_id: Option<&str>) -> Self {
            let lock = WORKER_ID_ENV_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let previous_worker_id = std::env::var_os("WORKER_ID");
            let previous_review_quality_worker_id = std::env::var_os("REVIEW_QUALITY_WORKER_ID");
            // SAFETY: these tests serialize every mutation through WORKER_ID_ENV_LOCK.
            unsafe {
                set_or_remove_env("WORKER_ID", worker_id);
                set_or_remove_env("REVIEW_QUALITY_WORKER_ID", review_quality_worker_id);
            }
            Self {
                _lock: lock,
                previous_worker_id,
                previous_review_quality_worker_id,
            }
        }
    }

    impl Drop for WorkerIdEnvGuard {
        fn drop(&mut self) {
            // SAFETY: this guard still owns WORKER_ID_ENV_LOCK.
            unsafe {
                restore_env("WORKER_ID", self.previous_worker_id.take());
                restore_env(
                    "REVIEW_QUALITY_WORKER_ID",
                    self.previous_review_quality_worker_id.take(),
                );
            }
        }
    }

    unsafe fn set_or_remove_env(key: &str, value: Option<&str>) {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    unsafe fn restore_env(key: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[test]
    fn parses_default_worker_ids_without_argument_collision() {
        let _env = WorkerIdEnvGuard::set(None, None);

        let cli = Cli::try_parse_from(["review-quality-diagnostic-runner"]).unwrap();

        assert!(cli.config.worker_id().starts_with("solver-worker-"));
        assert_eq!(
            cli.review_quality_worker_id,
            "review_quality_diagnostic_runner"
        );
    }

    #[test]
    fn parses_distinct_worker_ids_from_environment() {
        let _env = WorkerIdEnvGuard::set(Some("shared-worker"), Some("review-quality-worker"));

        let cli = Cli::try_parse_from(["review-quality-diagnostic-runner"]).unwrap();

        assert_eq!(cli.config.worker_id(), "shared-worker");
        assert_eq!(cli.review_quality_worker_id, "review-quality-worker");
    }

    #[test]
    fn parses_distinct_worker_ids_from_cli() {
        let _env = WorkerIdEnvGuard::set(None, None);

        let cli = Cli::try_parse_from([
            "review-quality-diagnostic-runner",
            "--worker-id",
            "shared-worker",
            "--review-quality-worker-id",
            "review-quality-worker",
        ])
        .unwrap();

        assert_eq!(cli.config.worker_id(), "shared-worker");
        assert_eq!(cli.review_quality_worker_id, "review-quality-worker");
    }
}
