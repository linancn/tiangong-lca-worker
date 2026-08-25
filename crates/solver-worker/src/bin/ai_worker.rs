use std::{sync::Arc, time::Duration};

use clap::Parser;
use solver_worker::{
    ai::{
        client::{AiClientConfig, AiModelClient, OpenAiCompatibleClient},
        rules::AiRulesets,
        runner::{AiWorkerOptions, run_ai_worker},
        tidas_suggestion::AiTidasSuggestionRuntime,
    },
    db_pool::{APP_AI_WORKER_QUEUE, WorkerDbPoolOptions},
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "ai-worker")]
#[command(about = "Run versioned AI jobs from the dedicated worker queue.")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "CONN")]
    conn: Option<String>,
    #[arg(long, env = "QUEUE_DATABASE_URL")]
    queue_database_url: Option<String>,
    #[arg(long, env = "QUEUE_CONN")]
    queue_conn: Option<String>,
    #[arg(long, env = "QUEUE_DB_MAX_CONNECTIONS", default_value_t = 2_u32)]
    queue_db_max_connections: u32,
    #[arg(long, env = "QUEUE_DB_MIN_CONNECTIONS", default_value_t = 0_u32)]
    queue_db_min_connections: u32,
    #[arg(
        long,
        env = "QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 30_u64
    )]
    queue_db_acquire_timeout_seconds: u64,
    #[arg(long, env = "AI_WORKER_POLL_MS", default_value_t = 1_000_u64)]
    poll_ms: u64,
    #[arg(long, env = "AI_WORKER_MAX_RUNS")]
    max_runs: Option<usize>,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(
        long = "ai-worker-id",
        env = "AI_WORKER_ID",
        default_value = "ai-worker"
    )]
    ai_worker_id: String,
    #[arg(
        long = "ai-worker-claim-limit",
        env = "AI_WORKER_CLAIM_LIMIT",
        default_value_t = 1_i32
    )]
    claim_limit: i32,
    #[arg(
        long = "ai-worker-lease-seconds",
        env = "AI_WORKER_LEASE_SECONDS",
        default_value_t = 900_i32
    )]
    lease_seconds: i32,
    #[arg(long, env = "AI_MAX_CONCURRENCY", default_value_t = 4_usize)]
    max_concurrency: usize,
    #[arg(long, env = "AI_MAX_INPUT_BYTES", default_value_t = 2_097_152_usize)]
    max_input_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let queue_url = resolved_queue_database_url(&cli)?;
    let queue_db_max_connections = cli.queue_db_max_connections.max(1);
    let pool = WorkerDbPoolOptions::new(APP_AI_WORKER_QUEUE)
        .max_connections(queue_db_max_connections)
        .min_connections(cli.queue_db_min_connections.min(queue_db_max_connections))
        .acquire_timeout(Duration::from_secs(
            cli.queue_db_acquire_timeout_seconds.max(1),
        ))
        .connect(queue_url)
        .await?;

    let rulesets = tokio::task::spawn_blocking(AiRulesets::load_from_tidas).await??;
    let client: Arc<dyn AiModelClient> =
        Arc::new(OpenAiCompatibleClient::new(AiClientConfig::from_env()?)?);
    let runtime = Arc::new(AiTidasSuggestionRuntime::new(
        client,
        rulesets,
        cli.max_concurrency,
        cli.max_input_bytes,
    )?);
    let summary = run_ai_worker(
        &pool,
        runtime,
        AiWorkerOptions {
            poll_interval: Duration::from_millis(cli.poll_ms.max(100)),
            max_runs: cli.max_runs.or_else(|| cli.once.then_some(1)),
            exit_when_idle: cli.once,
            worker_id: cli.ai_worker_id,
            claim_limit: cli.claim_limit.clamp(1, 50),
            lease_seconds: cli.lease_seconds.clamp(60, 86_400),
        },
    )
    .await?;
    info!(?summary, "AI worker stopped");
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn resolved_queue_database_url(cli: &Cli) -> anyhow::Result<&str> {
    cli.queue_database_url
        .as_deref()
        .or(cli.queue_conn.as_deref())
        .or(cli.database_url.as_deref())
        .or(cli.conn.as_deref())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing queue database URL: set QUEUE_DATABASE_URL, QUEUE_CONN, DATABASE_URL, or CONN"
            )
        })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, resolved_queue_database_url};

    #[test]
    fn parses_focused_ai_worker_options_without_solver_argument_collisions() {
        let cli = Cli::try_parse_from([
            "ai-worker",
            "--queue-database-url",
            "postgresql://queue.example/test",
            "--ai-worker-id",
            "ai-worker-test",
        ])
        .unwrap();

        assert_eq!(
            resolved_queue_database_url(&cli).unwrap(),
            "postgresql://queue.example/test"
        );
        assert_eq!(cli.ai_worker_id, "ai-worker-test");
        assert_eq!(cli.claim_limit, 1);
    }
}
