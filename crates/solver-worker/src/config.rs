use std::{net::SocketAddr, str::FromStr, time::Duration};

use clap::{Parser, ValueEnum};

use crate::pgbouncer_sqlx::postgres::PgConnectOptions;

/// Solver worker launch mode.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RunMode {
    /// Only queue worker.
    Worker,
    /// Only internal HTTP server.
    Http,
    /// Run both components in one process.
    Both,
}

/// Queue backend used by worker mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum QueueBackend {
    /// Legacy pgmq queue payloads.
    Pgmq,
    /// Unified `private.worker_jobs` queue payloads.
    WorkerJobs,
}

/// CLI + env config.
#[derive(Debug, Clone, Parser)]
#[command(name = "solver-worker")]
pub struct AppConfig {
    /// Launch mode.
    #[arg(long, env = "SOLVER_MODE", default_value = "both")]
    pub mode: RunMode,
    /// `PostgreSQL` URL (preferred env: `DATABASE_URL`, fallback: `CONN`).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,
    /// `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "CONN")]
    pub conn: Option<String>,
    /// Optional queue-only `PostgreSQL` URL. Use this for transaction poolers
    /// such as Supabase 6543 while keeping main compute queries on `DATABASE_URL`.
    #[arg(long, env = "QUEUE_DATABASE_URL")]
    pub queue_database_url: Option<String>,
    /// Queue-only `PostgreSQL` URL fallback used by this project in local `.env`.
    #[arg(long, env = "QUEUE_CONN")]
    pub queue_conn: Option<String>,
    /// Worker queue backend.
    #[arg(long, env = "SOLVER_QUEUE_BACKEND", default_value = "worker-jobs")]
    pub queue_backend: QueueBackend,
    /// Queue name in pgmq.
    #[arg(long, env = "PGMQ_QUEUE", default_value = "lca_jobs")]
    pub pgmq_queue: String,
    /// Acknowledges an explicit request for a retired legacy backend.
    ///
    /// The process still fails closed because the legacy lifecycle tables were
    /// retired; this guard only makes accidental backend selection explicit.
    #[arg(long, env = "ALLOW_LEGACY_JOB_TABLE_BACKEND", default_value_t = false)]
    pub allow_legacy_job_table_backend: bool,
    /// Stable worker id recorded on claimed `worker_jobs` rows.
    #[arg(long, env = "WORKER_ID")]
    pub worker_id: Option<String>,
    /// Number of `worker_jobs` rows to claim per poll.
    #[arg(long, env = "WORKER_JOBS_CLAIM_LIMIT", default_value_t = 1_i32)]
    pub worker_jobs_claim_limit: i32,
    /// Lease seconds used when claiming or heartbeating `worker_jobs` rows.
    #[arg(long, env = "WORKER_JOBS_LEASE_SECONDS", default_value_t = 900_i32)]
    pub worker_jobs_lease_seconds: i32,
    /// Poll interval for queue worker (ms).
    #[arg(long, env = "WORKER_POLL_MS", default_value_t = 1_000_u64)]
    pub worker_poll_ms: u64,
    /// Message visibility timeout for pgmq.read.
    #[arg(long, env = "WORKER_VT_SECONDS", default_value_t = 30_i32)]
    pub worker_vt_seconds: i32,
    /// Maximum number of DB connections held by the worker process.
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value_t = 8_u32)]
    pub db_max_connections: u32,
    /// Minimum number of DB connections kept by the worker process.
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value_t = 1_u32)]
    pub db_min_connections: u32,
    /// DB connection acquire timeout for the worker process.
    #[arg(long, env = "DB_ACQUIRE_TIMEOUT_SECONDS", default_value_t = 30_u64)]
    pub db_acquire_timeout_seconds: u64,
    /// Maximum number of DB connections held by the queue polling pool.
    #[arg(long, env = "QUEUE_DB_MAX_CONNECTIONS", default_value_t = 2_u32)]
    pub queue_db_max_connections: u32,
    /// Minimum number of DB connections kept by the queue polling pool.
    #[arg(long, env = "QUEUE_DB_MIN_CONNECTIONS", default_value_t = 0_u32)]
    pub queue_db_min_connections: u32,
    /// DB connection acquire timeout for the queue polling pool.
    #[arg(
        long,
        env = "QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS",
        default_value_t = 30_u64
    )]
    pub queue_db_acquire_timeout_seconds: u64,
    /// Maximum number of concurrent `build_snapshot` jobs across worker instances.
    #[arg(long, env = "BUILD_SNAPSHOT_MAX_CONCURRENCY", default_value_t = 1_u32)]
    pub build_snapshot_max_concurrency: u32,
    /// Poll interval while waiting for a `build_snapshot` concurrency slot.
    #[arg(long, env = "BUILD_SNAPSHOT_LOCK_POLL_MS", default_value_t = 5_000_u64)]
    pub build_snapshot_lock_poll_ms: u64,
    /// Internal HTTP bind address.
    #[arg(long, env = "HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: String,
    /// S3-compatible endpoint for large result artifacts.
    #[arg(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "S3_REGION")]
    pub s3_region: Option<String>,
    /// S3 bucket.
    #[arg(long, env = "S3_BUCKET")]
    pub s3_bucket: Option<String>,
    /// S3 access key id for `SigV4` authenticated uploads.
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    pub s3_access_key_id: Option<String>,
    /// S3 secret access key for `SigV4` authenticated uploads.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,
    /// Optional S3 session token for temporary credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    pub s3_session_token: Option<String>,
    /// Object key prefix under the bucket.
    #[arg(long, env = "S3_PREFIX", default_value = "lca-results")]
    pub s3_prefix: String,
    /// Optional local preflight upload limit matching the storage max-file-limit.
    #[arg(long, env = "S3_MAX_UPLOAD_BYTES")]
    pub s3_max_upload_bytes: Option<u64>,
    /// Hard retained-byte capacity for prepared sparse factorizations.
    #[arg(
        long,
        env = "FACTORIZATION_CACHE_MAX_BYTES",
        default_value_t = 1_073_741_824_usize
    )]
    pub factorization_cache_max_bytes: usize,
    /// Workload admission multiplier applied to factorized M storage before UMFPACK runs.
    ///
    /// This is a deployment/workload policy for fill-in headroom, not a
    /// matrix-size-independent memory guarantee.
    #[arg(
        long,
        env = "FACTORIZATION_ADMISSION_FILL_IN_MULTIPLIER",
        default_value_t = 8.0_f64
    )]
    pub factorization_admission_fill_in_multiplier: f64,
}

impl AppConfig {
    /// Returns resolved database URL from `DATABASE_URL` or `CONN`.
    pub fn resolved_database_url(&self) -> anyhow::Result<&str> {
        self.database_url
            .as_deref()
            .or(self.conn.as_deref())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing database URL: set DATABASE_URL or CONN environment variable"
                )
            })
    }

    /// Returns queue-only database URL from `QUEUE_DATABASE_URL` / `QUEUE_CONN`,
    /// falling back to the main runtime database URL.
    pub fn resolved_queue_database_url(&self) -> anyhow::Result<&str> {
        self.queue_database_url
            .as_deref()
            .or(self.queue_conn.as_deref())
            .map_or_else(|| self.resolved_database_url(), Ok)
    }

    /// Rejects the known Supabase transaction-pooler endpoint for the main pool.
    ///
    /// Main solver/package/snapshot paths use bound `SQLx` queries and therefore
    /// require a direct or session-mode connection. The queue-only pool may use
    /// Supabase's 6543 transaction endpoint because its hot RPCs use the simple
    /// query protocol.
    pub fn validate_database_pool_modes(&self) -> anyhow::Result<()> {
        let database_url = self.resolved_database_url()?;
        let options = PgConnectOptions::from_str(database_url)
            .map_err(|err| anyhow::anyhow!("invalid DATABASE_URL/CONN: {err}"))?;

        if options.get_port() == 6543
            && options
                .get_host()
                .to_ascii_lowercase()
                .ends_with(".pooler.supabase.com")
        {
            anyhow::bail!(
                "DATABASE_URL/CONN points to Supabase's 6543 transaction pooler, but main Worker queries require a direct or session-mode connection; use the same Supabase pooler host on port 5432 for DATABASE_URL and keep port 6543 only in QUEUE_DATABASE_URL"
            );
        }

        Ok(())
    }

    /// Returns whether queue DB URL was explicitly configured.
    #[must_use]
    pub fn has_explicit_queue_database_url(&self) -> bool {
        self.queue_database_url.is_some() || self.queue_conn.is_some()
    }

    /// Poll interval as Duration.
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.worker_poll_ms)
    }

    /// Stable worker id for `worker_jobs` claim diagnostics.
    #[must_use]
    pub fn worker_id(&self) -> String {
        self.worker_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map_or_else(
                || format!("solver-worker-{}", std::process::id()),
                str::to_owned,
            )
    }

    /// Ensures legacy job-table + pgmq backends were explicitly enabled.
    pub fn require_legacy_job_table_backend_allowed(
        &self,
        backend_name: &str,
    ) -> anyhow::Result<()> {
        if self.allow_legacy_job_table_backend {
            return Ok(());
        }

        anyhow::bail!(
            "{backend_name} uses retained legacy job tables and is disabled by default; set ALLOW_LEGACY_JOB_TABLE_BACKEND=true only for explicit compatibility/debug runs"
        )
    }

    /// Sanitized `worker_jobs` claim limit.
    #[must_use]
    pub fn worker_jobs_claim_limit(&self) -> i32 {
        self.worker_jobs_claim_limit.clamp(1, 50)
    }

    /// Sanitized `worker_jobs` lease seconds.
    #[must_use]
    pub fn worker_jobs_lease_seconds(&self) -> i32 {
        self.worker_jobs_lease_seconds.clamp(1, 86_400)
    }

    /// Sanitized maximum DB connections for the worker process.
    #[must_use]
    pub fn db_max_connections(&self) -> u32 {
        self.db_max_connections.max(1)
    }

    /// Sanitized minimum DB connections for the worker process.
    #[must_use]
    pub fn db_min_connections(&self) -> u32 {
        self.db_min_connections.min(self.db_max_connections())
    }

    /// DB connection acquire timeout.
    #[must_use]
    pub fn db_acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.db_acquire_timeout_seconds.max(1))
    }

    /// Sanitized maximum DB connections for the queue polling pool.
    #[must_use]
    pub fn queue_db_max_connections(&self) -> u32 {
        self.queue_db_max_connections.max(1)
    }

    /// Sanitized minimum DB connections for the queue polling pool.
    #[must_use]
    pub fn queue_db_min_connections(&self) -> u32 {
        self.queue_db_min_connections
            .min(self.queue_db_max_connections())
    }

    /// Queue DB connection acquire timeout.
    #[must_use]
    pub fn queue_db_acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.queue_db_acquire_timeout_seconds.max(1))
    }

    /// Sanitized maximum `build_snapshot` concurrency.
    #[must_use]
    pub fn build_snapshot_max_concurrency(&self) -> u32 {
        self.build_snapshot_max_concurrency.max(1)
    }

    /// Poll interval used when all `build_snapshot` concurrency slots are busy.
    #[must_use]
    pub fn build_snapshot_lock_poll_interval(&self) -> Duration {
        Duration::from_millis(self.build_snapshot_lock_poll_ms.max(100))
    }

    /// Optional local upload-size guard for object-storage writes.
    #[must_use]
    pub fn s3_max_upload_bytes(&self) -> Option<u64> {
        self.s3_max_upload_bytes
    }

    /// Hard factorization-cache capacity.
    #[must_use]
    pub fn factorization_cache_max_bytes(&self) -> usize {
        self.factorization_cache_max_bytes.max(1)
    }

    /// Sanitized workload-derived fill-in admission multiplier.
    #[must_use]
    pub fn factorization_admission_fill_in_multiplier(&self) -> f64 {
        if self.factorization_admission_fill_in_multiplier.is_finite() {
            self.factorization_admission_fill_in_multiplier.max(1.0)
        } else {
            8.0
        }
    }

    /// Parsed http socket addr.
    pub fn http_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        SocketAddr::from_str(&self.http_addr)
            .map_err(|err| anyhow::anyhow!("invalid HTTP_ADDR {}: {err}", self.http_addr))
    }
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, QueueBackend};
    use clap::Parser;
    use std::time::Duration;

    #[test]
    fn db_and_build_snapshot_config_defaults_match_previous_limits() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
        ]);

        assert_eq!(config.db_max_connections(), 8);
        assert_eq!(config.db_min_connections(), 1);
        assert_eq!(config.db_acquire_timeout(), Duration::from_secs(30));
        assert_eq!(
            config.resolved_queue_database_url().unwrap(),
            "postgres://example.local/app"
        );
        assert!(!config.has_explicit_queue_database_url());
        assert_eq!(config.queue_db_max_connections(), 2);
        assert_eq!(config.queue_db_min_connections(), 0);
        assert_eq!(config.queue_db_acquire_timeout(), Duration::from_secs(30));
        assert_eq!(config.queue_backend, QueueBackend::WorkerJobs);
        assert!(!config.allow_legacy_job_table_backend);
        assert!(config.worker_id().starts_with("solver-worker-"));
        assert_eq!(config.worker_jobs_claim_limit(), 1);
        assert_eq!(config.worker_jobs_lease_seconds(), 900);
        assert_eq!(config.build_snapshot_max_concurrency(), 1);
        assert_eq!(
            config.build_snapshot_lock_poll_interval(),
            Duration::from_secs(5)
        );
        assert_eq!(config.factorization_cache_max_bytes(), 1_073_741_824);
        assert!((config.factorization_admission_fill_in_multiplier() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn db_and_build_snapshot_config_clamps_invalid_low_values() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--db-max-connections",
            "0",
            "--db-min-connections",
            "4",
            "--db-acquire-timeout-seconds",
            "0",
            "--queue-db-max-connections",
            "0",
            "--queue-db-min-connections",
            "4",
            "--queue-db-acquire-timeout-seconds",
            "0",
            "--worker-jobs-claim-limit",
            "0",
            "--worker-jobs-lease-seconds",
            "0",
            "--build-snapshot-max-concurrency",
            "0",
            "--build-snapshot-lock-poll-ms",
            "1",
        ]);

        assert_eq!(config.db_max_connections(), 1);
        assert_eq!(config.db_min_connections(), 1);
        assert_eq!(config.db_acquire_timeout(), Duration::from_secs(1));
        assert_eq!(config.queue_db_max_connections(), 1);
        assert_eq!(config.queue_db_min_connections(), 1);
        assert_eq!(config.queue_db_acquire_timeout(), Duration::from_secs(1));
        assert_eq!(config.worker_jobs_claim_limit(), 1);
        assert_eq!(config.worker_jobs_lease_seconds(), 1);
        assert_eq!(config.build_snapshot_max_concurrency(), 1);
        assert_eq!(
            config.build_snapshot_lock_poll_interval(),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn queue_database_url_overrides_main_database_url() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--queue-database-url",
            "postgres://pooler.example.local/app",
        ]);

        assert_eq!(
            config.resolved_queue_database_url().unwrap(),
            "postgres://pooler.example.local/app"
        );
        assert!(config.has_explicit_queue_database_url());
    }

    #[test]
    fn rejects_supabase_transaction_pooler_for_main_database_url() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://postgres.example:secret@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
            "--queue-database-url",
            "postgres://postgres.example:secret@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        ]);

        let err = config.validate_database_pool_modes().unwrap_err();
        assert!(err.to_string().contains("main Worker queries require"));
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn accepts_supabase_session_pooler_for_main_and_transaction_pooler_for_queue() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://postgres.example:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
            "--queue-database-url",
            "postgres://postgres.example:secret@aws-0-us-east-1.pooler.supabase.com:6543/postgres",
        ]);

        config.validate_database_pool_modes().unwrap();
    }

    #[test]
    fn parses_optional_s3_max_upload_bytes() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--s3-max-upload-bytes",
            "209715200",
        ]);

        assert_eq!(config.s3_max_upload_bytes(), Some(209_715_200));
    }

    #[test]
    fn worker_jobs_backend_and_worker_id_parse_from_cli() {
        let config = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--queue-backend",
            "worker-jobs",
            "--worker-id",
            " solver-a ",
            "--worker-jobs-claim-limit",
            "100",
            "--worker-jobs-lease-seconds",
            "90000",
        ]);

        assert_eq!(config.queue_backend, QueueBackend::WorkerJobs);
        assert_eq!(config.worker_id(), "solver-a");
        assert_eq!(config.worker_jobs_claim_limit(), 50);
        assert_eq!(config.worker_jobs_lease_seconds(), 86_400);
    }

    #[test]
    fn legacy_job_table_backend_requires_explicit_opt_in() {
        let blocked = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--queue-backend",
            "pgmq",
        ]);

        assert_eq!(blocked.queue_backend, QueueBackend::Pgmq);
        assert!(
            blocked
                .require_legacy_job_table_backend_allowed("solver pgmq backend")
                .unwrap_err()
                .to_string()
                .contains("ALLOW_LEGACY_JOB_TABLE_BACKEND=true")
        );

        let allowed = AppConfig::parse_from([
            "solver-worker",
            "--database-url",
            "postgres://example.local/app",
            "--queue-backend",
            "pgmq",
            "--allow-legacy-job-table-backend",
        ]);

        assert!(allowed.allow_legacy_job_table_backend);
        allowed
            .require_legacy_job_table_backend_allowed("solver pgmq backend")
            .expect("legacy backend opt-in should allow pgmq");
    }
}
