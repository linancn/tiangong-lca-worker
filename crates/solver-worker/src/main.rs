use std::{env::VarError, num::NonZeroUsize, sync::Arc};

use axum::serve;
use clap::Parser;
use solver_worker::{
    config::{AppConfig, QueueBackend, RunMode},
    db::AppState,
    http, queue,
};
use tokio::net::TcpListener;
use tracing::info;

const MINIMUM_SOLVER_RUNTIME_WORKER_THREADS: usize = 2;

fn resolve_solver_runtime_worker_threads(
    configured: Option<&str>,
    available_parallelism: usize,
) -> anyhow::Result<usize> {
    let available_parallelism = available_parallelism.max(1);
    let Some(configured) = configured else {
        return Ok(available_parallelism.max(MINIMUM_SOLVER_RUNTIME_WORKER_THREADS));
    };
    let worker_threads = configured.parse::<usize>().map_err(|_| {
        anyhow::anyhow!("TOKIO_WORKER_THREADS must be an integer greater than or equal to 2")
    })?;
    if worker_threads < MINIMUM_SOLVER_RUNTIME_WORKER_THREADS {
        anyhow::bail!("TOKIO_WORKER_THREADS must be greater than or equal to 2");
    }
    Ok(worker_threads)
}

fn solver_runtime_worker_threads() -> anyhow::Result<usize> {
    let configured = match std::env::var("TOKIO_WORKER_THREADS") {
        Ok(value) => Some(value),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => {
            anyhow::bail!("TOKIO_WORKER_THREADS must contain valid Unicode")
        }
    };
    let available_parallelism = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    resolve_solver_runtime_worker_threads(configured.as_deref(), available_parallelism)
}

fn build_solver_runtime(worker_threads: usize) -> anyhow::Result<tokio::runtime::Runtime> {
    if worker_threads < MINIMUM_SOLVER_RUNTIME_WORKER_THREADS {
        anyhow::bail!("solver runtime requires at least two worker threads");
    }
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?)
}

fn main() -> anyhow::Result<()> {
    let worker_threads = solver_runtime_worker_threads()?;
    build_solver_runtime(worker_threads)?.block_on(run(worker_threads))
}

async fn run(worker_threads: usize) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    info!(worker_threads, "configured solver Tokio runtime");

    let config = AppConfig::parse();
    let state = Arc::new(AppState::new(&config).await?);

    match config.mode {
        RunMode::Worker => {
            info!("starting queue worker mode");
            run_worker(state, &config).await?;
        }
        RunMode::Http => {
            info!("starting internal HTTP mode");
            run_http(state, config.http_socket_addr()?).await?;
        }
        RunMode::Both => {
            info!("starting worker + internal HTTP mode");
            let worker_state = Arc::clone(&state);
            let worker_config = config.clone();
            let worker_handle =
                tokio::spawn(async move { run_worker(worker_state, &worker_config).await });

            let http_handle =
                tokio::spawn(run_http(Arc::clone(&state), config.http_socket_addr()?));

            tokio::select! {
                worker_result = worker_handle => {
                    worker_result??;
                }
                http_result = http_handle => {
                    http_result??;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("ctrl-c received, exiting");
                }
            }
        }
    }

    Ok(())
}

async fn run_worker(state: Arc<AppState>, config: &AppConfig) -> anyhow::Result<()> {
    match config.queue_backend {
        QueueBackend::Pgmq => {
            config.require_legacy_job_table_backend_allowed("solver pgmq backend")?;
            anyhow::bail!(
                "SOLVER_QUEUE_BACKEND=pgmq is retired because the lca_jobs lifecycle no longer exists; use SOLVER_QUEUE_BACKEND=worker-jobs"
            )
        }
        QueueBackend::WorkerJobs => {
            queue::run_solver_worker_jobs_loop(
                state,
                config.worker_id(),
                config.worker_jobs_claim_limit(),
                config.worker_jobs_lease_seconds(),
                config.poll_interval(),
            )
            .await
        }
    }
}

async fn run_http(state: Arc<AppState>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let app = http::router(state);
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "internal HTTP listening");
    serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use solver_worker::worker_jobs::run_with_periodic_lease_renewal;
    use tokio::{sync::mpsc, time::timeout};

    use super::{build_solver_runtime, resolve_solver_runtime_worker_threads};

    #[test]
    fn solver_runtime_preserves_host_parallelism_and_rejects_unsafe_overrides() {
        assert_eq!(resolve_solver_runtime_worker_threads(None, 1).unwrap(), 2);
        assert_eq!(resolve_solver_runtime_worker_threads(None, 8).unwrap(), 8);
        assert_eq!(
            resolve_solver_runtime_worker_threads(Some("4"), 8).unwrap(),
            4
        );
        assert!(resolve_solver_runtime_worker_threads(Some("1"), 8).is_err());
        assert!(resolve_solver_runtime_worker_threads(Some("invalid"), 8).is_err());
    }

    #[test]
    fn solver_entry_uses_the_guarded_multi_thread_runtime_builder() {
        let source = include_str!("main.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("solver entry test module")
            .0;
        assert!(!production.contains(&["#[tokio", "::main]"].concat()));
        assert!(production.contains("let worker_threads = solver_runtime_worker_threads()?;"));
        assert!(
            production
                .contains("build_solver_runtime(worker_threads)?.block_on(run(worker_threads))")
        );
        assert!(production.contains("Builder::new_multi_thread()"));
        assert!(production.contains(".worker_threads(worker_threads)"));
    }

    #[test]
    fn one_parallelism_host_runtime_keeps_renewal_live_during_a_cpu_block() {
        let worker_threads = resolve_solver_runtime_worker_threads(None, 1).unwrap();
        let runtime = build_solver_runtime(worker_threads).unwrap();
        runtime.block_on(async {
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
                            let _ = renewal_tx.send(count);
                            Ok(())
                        }
                    },
                    async move {
                        let before = renewal_rx.recv().await.expect("first renewal");
                        let blocked_until = Instant::now() + Duration::from_millis(50);
                        while Instant::now() < blocked_until {
                            std::hint::spin_loop();
                        }
                        Ok(renewals.load(Ordering::SeqCst).saturating_sub(before))
                    },
                ),
            )
            .await
            .expect("orchestration completes before test deadline")
            .expect("protected work completes");

            assert!(renewals_during_block >= 2);
        });
    }
}
