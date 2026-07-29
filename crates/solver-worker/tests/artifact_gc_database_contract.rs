use std::{
    collections::BTreeSet,
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use solver_worker::pgbouncer_sqlx::{self as sqlx, Row};
use solver_worker::storage::{ObjectStoreClient, ObjectTransferOptions};
use uuid::Uuid;

const DATABASE_CONTRACT_COMMIT: &str = "7d40ab62fcf26de42f2c9b4aeb33acd47caa5f20";
const DATABASE_CONTRACT_FIXTURE: &str =
    "supabase/tests/fixtures/20260729_scope_closure_public_artifact_contract.json";

fn authoritative_database_fixture() -> serde_json::Value {
    let database_root = std::env::var("DATABASE_ENGINE_ROOT")
        .expect("DATABASE_ENGINE_ROOT points to Database #309");
    let revision = format!("{DATABASE_CONTRACT_COMMIT}:{DATABASE_CONTRACT_FIXTURE}");
    let output = Command::new("git")
        .args(["-C", &database_root, "show", &revision])
        .output()
        .expect("read authoritative Database #309 fixture");
    assert!(
        output.status.success(),
        "cannot read Database #309 fixture at {DATABASE_CONTRACT_COMMIT}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("parse authoritative Database #309 fixture")
}

fn fake_s3_delete_server() -> (String, thread::JoinHandle<Option<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake S3");
    listener
        .set_nonblocking(true)
        .expect("make fake S3 listener bounded");
    let address = listener.local_addr().expect("fake S3 address");
    let task = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept S3 delete: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound fake S3 request read");
        let mut request = [0_u8; 8192];
        let read = stream.read(&mut request).expect("read S3 delete");
        let request = String::from_utf8_lossy(&request[..read]).into_owned();
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .expect("write S3 delete response");
        Some(request.lines().next().unwrap_or_default().to_owned())
    });
    (format!("http://{address}"), task)
}

fn run_bounded(command: &mut Command, timeout: Duration) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn production artifact_gc CLI");
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll production artifact_gc CLI") {
            Some(_) => {
                return child
                    .wait_with_output()
                    .expect("collect artifact_gc output");
            }
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            None => {
                child.kill().expect("terminate timed-out artifact_gc CLI");
                let output = child
                    .wait_with_output()
                    .expect("collect timed-out artifact_gc output");
                panic!(
                    "artifact_gc timed out after {timeout:?}: stdout={} stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
            }
        }
    }
}

struct FakeObjectStore {
    endpoint: String,
    objects: Arc<Mutex<BTreeSet<String>>>,
    stop: Arc<AtomicBool>,
    task: Option<thread::JoinHandle<()>>,
}

impl FakeObjectStore {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake object store");
        listener
            .set_nonblocking(true)
            .expect("make fake object store nonblocking");
        let address = listener.local_addr().expect("fake object store address");
        let objects = Arc::new(Mutex::new(BTreeSet::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let server_objects = Arc::clone(&objects);
        let server_stop = Arc::clone(&stop);
        let task = thread::spawn(move || {
            while !server_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept fake object-store request: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("bound fake object-store read");
                let mut request = [0_u8; 16 * 1024];
                let read = stream
                    .read(&mut request)
                    .expect("read object-store request");
                let request = String::from_utf8_lossy(&request[..read]);
                let first_line = request.lines().next().unwrap_or_default();
                let mut fields = first_line.split_whitespace();
                let method = fields.next().unwrap_or_default();
                let path = fields.next().unwrap_or_default().to_owned();
                let existed = match method {
                    "PUT" => server_objects.lock().unwrap().insert(path),
                    "DELETE" => server_objects.lock().unwrap().remove(&path),
                    _ => false,
                };
                let response = match method {
                    "PUT" => {
                        "HTTP/1.1 200 OK\r\nETag: \"test-etag\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    }
                    "DELETE" if existed => {
                        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    }
                    "DELETE" => {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    }
                    _ => {
                        "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    }
                };
                stream
                    .write_all(response.as_bytes())
                    .expect("write fake object-store response");
            }
        });
        Self {
            endpoint: format!("http://{address}"),
            objects,
            stop,
            task: Some(task),
        }
    }

    fn paths(&self) -> BTreeSet<String> {
        self.objects.lock().unwrap().clone()
    }
}

impl Drop for FakeObjectStore {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            task.join().expect("join fake object store");
        }
    }
}

async fn seed_crash_closure(
    pool: &sqlx::PgPool,
    owner_id: Uuid,
    worker_job_id: Uuid,
    closure_check_id: Uuid,
) {
    sqlx::query(
        r"
        INSERT INTO auth.users (
          instance_id, id, aud, role, email, encrypted_password,
          email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
          created_at, updated_at, is_sso_user, is_anonymous
        ) VALUES (
          '00000000-0000-0000-0000-000000000000',
          $1, 'authenticated', 'authenticated', $2, 'x', now(),
          '{}', '{}', now(), now(), false, false
        )
        ",
    )
    .bind(owner_id)
    .bind(format!("{owner_id}@example.test"))
    .execute(pool)
    .await
    .expect("seed crash-test auth user");
    sqlx::query(
        "INSERT INTO public.users (id, raw_user_meta_data, contact) VALUES ($1, '{}', null)",
    )
    .bind(owner_id)
    .execute(pool)
    .await
    .expect("seed crash-test public user");
    sqlx::query(
        r"
        INSERT INTO public.worker_jobs (
          id, job_kind, worker_runtime, worker_queue, requester_type,
          requested_by, visibility, payload_schema_version, payload_json, status
        ) VALUES (
          $1, 'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
          $2, 'operator', 'lcia.scope_closure_check.request.v1', '{}', 'running'
        )
        ",
    )
    .bind(worker_job_id)
    .bind(owner_id)
    .execute(pool)
    .await
    .expect("seed crash-test worker job");
    sqlx::query(
        r"
        INSERT INTO public.lcia_scope_closure_checks (
          id, worker_job_id, requested_by, request_idempotency_token, request_key,
          request_fingerprint, requested_scope_hash, policy_fingerprint,
          data_snapshot_token, expected_validator_scanner_fingerprint, status,
          certificate_status
        ) VALUES (
          $1, $2, $3, $4, $5, repeat('a', 64), repeat('b', 64),
          repeat('c', 64), $6, 'scope-closure-validator-scanner.v1',
          'running', 'pending'
        )
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(owner_id)
    .bind(format!("crash-{closure_check_id}"))
    .bind(format!("crash-key-{closure_check_id}"))
    .bind(format!("crash-snapshot-{closure_check_id}"))
    .execute(pool)
    .await
    .expect("seed crash-test closure check");
}

#[test]
#[ignore = "requires DATABASE_ENGINE_ROOT with Database #309 exact commit"]
fn consumes_authoritative_database_fixture_at_pinned_commit() {
    let fixture = authoritative_database_fixture();
    assert_eq!(
        fixture["download"]["descriptorFields"],
        serde_json::json!([
            "artifactId",
            "artifactRole",
            "artifactState",
            "filename",
            "format",
            "mediaType",
            "size",
            "checksumSha256",
            "artifactExpiresAt",
            "bucket",
            "objectPath"
        ])
    );
    assert_eq!(
        fixture["workerGc"]["claimItemFields"],
        serde_json::json!([
            "artifactId",
            "artifactRole",
            "lifecycleState",
            "gcPhase",
            "objectDeleteRequired",
            "bucket",
            "objectPath",
            "checksumSha256",
            "artifactExpiresAt"
        ])
    );
    assert_eq!(
        fixture["workerGc"]["freshProcessDetailCleanup"],
        serde_json::json!({
            "objectDeleteRequired": false,
            "bucket": null,
            "objectPath": null,
            "requiresNewFencedToken": true
        })
    );
    assert_eq!(
        fixture["publicationStaging"]["createSignature"],
        "svc_lcia_scope_closure_artifact_write_set_create(uuid,text,jsonb,integer,uuid)"
    );
    assert_eq!(
        fixture["publicationStaging"]["bundleMetadata"]["completeMachineResultClientKey"],
        "manifest.json"
    );
    assert_eq!(
        fixture["publicationStaging"]["publicationModes"]["reused"]["shape"],
        "exactly one closure_report_xlsx"
    );
    assert_eq!(
        fixture["publicationStaging"]["reconcileSignature"],
        "svc_lcia_scope_closure_artifact_write_set_reconcile(integer,integer)"
    );
    assert_eq!(
        fixture["workerGc"]["previewIsNonMutating"],
        serde_json::Value::Bool(true)
    );
}

#[tokio::test]
#[ignore = "subprocess helper for db_first_crash_is_fully_reconciled_after_restart"]
#[allow(clippy::too_many_lines)]
async fn db_first_crash_child_aborts_after_n_uploads() {
    if std::env::var("WORKER_ARTIFACT_CRASH_CHILD").as_deref() != Ok("1") {
        return;
    }
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let closure_check_id: Uuid = std::env::var("CLOSURE_CHECK_ID")
        .expect("CLOSURE_CHECK_ID")
        .parse()
        .expect("valid closure check ID");
    let endpoint = std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT");
    let object_prefix = std::env::var("OBJECT_PREFIX").expect("OBJECT_PREFIX");
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("connect child to Database #309");
    let payloads = [
        (
            "report.xlsx",
            "closure_report_xlsx",
            "closure_report",
            "report",
        ),
        (
            "manifest.json",
            "closure_complete_machine_result",
            "complete_machine_result",
            "manifest",
        ),
        ("bundle.json", "closure_bundle", "closure_bundle", "bundle"),
    ];
    let items = payloads
        .iter()
        .enumerate()
        .map(
            |(index, (name, artifact_type, artifact_role, client_key))| {
                let bytes = format!("crash-fixture-{index}");
                let metadata = if *artifact_role == "closure_bundle" {
                    serde_json::json!({"completeMachineResultClientKey": "manifest"})
                } else {
                    serde_json::json!({})
                };
                serde_json::json!({
                    "clientKey": client_key,
                    "artifactType": artifact_type,
                    "artifactRole": artifact_role,
                    "bucket": "closure-private",
                    "objectPath": format!("{object_prefix}/{name}"),
                    "mediaType": if *name == "report.xlsx" {
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    } else if *name == "manifest.json" {
                        "application/vnd.tiangong.scope-closure-manifest+json"
                    } else {
                        "application/json"
                    },
                    "size": bytes.len(),
                    "checksumSha256": format!("{:x}", Sha256::digest(bytes.as_bytes())),
                    "metadata": metadata
                })
            },
        )
        .collect::<Vec<_>>();
    let created = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_create(
          $1, 'subprocess-crash-after-two-uploads', $2::jsonb, 1, null
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(serde_json::Value::Array(items))
    .fetch_one(&pool)
    .await
    .expect("register complete DB-first write set")
    .try_get::<serde_json::Value, _>("result")
    .expect("read write-set create result");
    assert_eq!(created["ok"], true);
    assert_eq!(
        created["data"]["items"].as_array().map(Vec::len),
        Some(payloads.len())
    );

    let object_store = ObjectStoreClient::new(
        &endpoint,
        "test",
        "closure-private",
        "",
        "test-access",
        "test-secret",
        None,
    )
    .expect("construct child object store");
    let temp = tempfile::tempdir().expect("create child artifact directory");
    for (index, (name, _, _, _)) in payloads.iter().enumerate().take(2) {
        let bytes = format!("crash-fixture-{index}");
        let path = temp.path().join(name);
        std::fs::write(&path, &bytes).expect("write child artifact");
        object_store
            .upload_object_key_file_bounded(
                &format!("{object_prefix}/{name}"),
                "application/octet-stream",
                &path,
                ObjectTransferOptions::new(1024)
                    .with_expected_sha256(format!("{:x}", Sha256::digest(bytes.as_bytes())))
                    .with_request_timeout(Duration::from_secs(5)),
            )
            .await
            .expect("upload child artifact before crash");
    }
    std::process::abort();
}

#[tokio::test]
#[ignore = "requires a local Database #309 schema at DATABASE_URL"]
#[allow(clippy::too_many_lines)]
async fn db_first_crash_is_fully_reconciled_after_restart() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("connect local Database #309");
    let owner_id = Uuid::new_v4();
    let worker_job_id = Uuid::new_v4();
    let closure_check_id = Uuid::new_v4();
    seed_crash_closure(&pool, owner_id, worker_job_id, closure_check_id).await;
    let object_prefix = format!("gc-crash/{closure_check_id}");
    let object_store = FakeObjectStore::start();

    let current_test = std::env::current_exe().expect("current integration-test executable");
    let crash = run_bounded(
        Command::new(current_test)
            .env("WORKER_ARTIFACT_CRASH_CHILD", "1")
            .env("DATABASE_URL", &database_url)
            .env("CLOSURE_CHECK_ID", closure_check_id.to_string())
            .env("S3_ENDPOINT", &object_store.endpoint)
            .env("OBJECT_PREFIX", &object_prefix)
            .args([
                "--exact",
                "db_first_crash_child_aborts_after_n_uploads",
                "--ignored",
                "--nocapture",
            ]),
        Duration::from_secs(20),
    );
    assert!(
        !crash.status.success(),
        "crash child unexpectedly exited successfully"
    );
    let uploaded_paths = object_store.paths();
    assert_eq!(
        uploaded_paths,
        BTreeSet::from([
            format!("/closure-private/{object_prefix}/manifest.json"),
            format!("/closure-private/{object_prefix}/report.xlsx"),
        ]),
        "failpoint must abort after exactly two registered uploads"
    );
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let mut gc_command = Command::new(env!("CARGO_BIN_EXE_artifact_gc"));
    gc_command
        .env("DATABASE_URL", &database_url)
        .env("S3_ENDPOINT", &object_store.endpoint)
        .env("S3_REGION", "test")
        .env("S3_BUCKET", "closure-private")
        .env("S3_ACCESS_KEY_ID", "test-access")
        .env("S3_SECRET_ACCESS_KEY", "test-secret")
        .env("S3_PREFIX", "")
        .args([
            "--execute",
            "--batch-size",
            "1",
            "--max-batches",
            "1",
            "--lease-seconds",
            "5",
            "--detail-limit",
            "1",
        ]);
    let gc = run_bounded(&mut gc_command, Duration::from_secs(20));
    assert!(
        gc.status.success(),
        "restart reconciler failed: stdout={} stderr={}",
        String::from_utf8_lossy(&gc.stdout),
        String::from_utf8_lossy(&gc.stderr),
    );
    assert!(
        object_store.paths().is_empty(),
        "restart reconciliation left tracked objects in fake S3"
    );

    let write_set = sqlx::query(
        r"
        SELECT id, status
        FROM public.lcia_scope_closure_artifact_write_sets
        WHERE closure_check_id = $1
        ",
    )
    .bind(closure_check_id)
    .fetch_one(&pool)
    .await
    .expect("read reconciled write set");
    let write_set_id = write_set.try_get::<Uuid, _>("id").unwrap();
    assert_eq!(write_set.try_get::<String, _>("status").unwrap(), "cleaned");
    let ready_count = sqlx::query(
        r"
        SELECT count(*) AS count
        FROM public.worker_job_artifacts artifact
        JOIN public.lcia_scope_closure_artifact_write_set_items item
          ON item.id = artifact.id
        WHERE item.write_set_id = $1
        ",
    )
    .bind(write_set_id)
    .fetch_one(&pool)
    .await
    .expect("count partial ready artifacts")
    .try_get::<i64, _>("count")
    .unwrap();
    assert_eq!(ready_count, 0, "crash exposed a partial ready write set");

    sqlx::query(
        "DELETE FROM public.lcia_scope_closure_artifact_write_set_items WHERE write_set_id = $1",
    )
    .bind(write_set_id)
    .execute(&pool)
    .await
    .expect("clean crash-test write-set items");
    sqlx::query("DELETE FROM public.lcia_scope_closure_artifact_write_sets WHERE id = $1")
        .bind(write_set_id)
        .execute(&pool)
        .await
        .expect("clean crash-test write set");
    sqlx::query("DELETE FROM public.lcia_scope_closure_checks WHERE id = $1")
        .bind(closure_check_id)
        .execute(&pool)
        .await
        .expect("clean crash-test closure check");
    sqlx::query("DELETE FROM public.worker_jobs WHERE id = $1")
        .bind(worker_job_id)
        .execute(&pool)
        .await
        .expect("clean crash-test worker job");
    sqlx::query("DELETE FROM public.users WHERE id = $1")
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("clean crash-test public user");
    sqlx::query("DELETE FROM auth.users WHERE id = $1")
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("clean crash-test auth user");
}

#[tokio::test]
#[ignore = "requires a local Database #309 schema at DATABASE_URL"]
#[allow(clippy::too_many_lines)]
async fn production_cli_uses_exact_database_gc_contract_and_fake_s3() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("connect local Database #309");
    let signatures = sqlx::query(
        r"
        SELECT
          to_regprocedure(
            'public.svc_lcia_scope_closure_artifact_gc_preview(integer)'
          ) IS NOT NULL AS preview_exists,
          to_regprocedure(
            'public.svc_lcia_scope_closure_artifact_gc_renew(uuid,integer)'
          ) IS NOT NULL AS renew_exists,
          to_regprocedure(
            'public.svc_lcia_scope_closure_artifact_write_set_reconcile(integer,integer)'
          ) IS NOT NULL AS reconcile_exists
        ",
    )
    .fetch_one(&pool)
    .await
    .expect("read exact RPC signatures");
    assert!(signatures.try_get::<bool, _>("preview_exists").unwrap());
    assert!(signatures.try_get::<bool, _>("renew_exists").unwrap());
    assert!(signatures.try_get::<bool, _>("reconcile_exists").unwrap());

    let owner_id = Uuid::new_v4();
    let worker_job_id = Uuid::new_v4();
    let artifact_id = Uuid::new_v4();
    sqlx::query(
        r"
        INSERT INTO auth.users (
          instance_id, id, aud, role, email, encrypted_password,
          email_confirmed_at, raw_app_meta_data, raw_user_meta_data,
          created_at, updated_at, is_sso_user, is_anonymous
        ) VALUES (
          '00000000-0000-0000-0000-000000000000',
          $1, 'authenticated', 'authenticated', $2, 'x', now(),
          '{}', '{}', now(), now(), false, false
        )
        ",
    )
    .bind(owner_id)
    .bind(format!("{owner_id}@example.test"))
    .execute(&pool)
    .await
    .expect("seed auth user");
    sqlx::query(
        "INSERT INTO public.users (id, raw_user_meta_data, contact) VALUES ($1, '{}', null)",
    )
    .bind(owner_id)
    .execute(&pool)
    .await
    .expect("seed public user");
    sqlx::query(
        r"
        INSERT INTO public.worker_jobs (
          id, job_kind, worker_runtime, worker_queue, requester_type,
          requested_by, visibility, payload_schema_version, payload_json, status
        ) VALUES (
          $1, 'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
          $2, 'operator', 'lcia.scope_closure_check.request.v1', '{}', 'running'
        )
        ",
    )
    .bind(worker_job_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .expect("seed worker job");
    sqlx::query(
        r"
        INSERT INTO public.worker_job_artifacts (
          id, job_id, artifact_type, storage_bucket, storage_path, content_type,
          byte_size, checksum_sha256, metadata, created_at
        ) VALUES (
          $1, $2, 'closure_report_xlsx', 'closure-private',
          'gc/production-cli-expired.xlsx',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          7, repeat('a', 64), '{}', timestamptz '2000-01-01 00:00:00+00'
        )
        ",
    )
    .bind(artifact_id)
    .bind(worker_job_id)
    .execute(&pool)
    .await
    .expect("seed expired artifact");

    let (endpoint, s3_task) = fake_s3_delete_server();
    let mut command = Command::new(env!("CARGO_BIN_EXE_artifact_gc"));
    command
        .env("DATABASE_URL", &database_url)
        .env("S3_ENDPOINT", endpoint)
        .env("S3_REGION", "test")
        .env("S3_BUCKET", "closure-private")
        .env("S3_ACCESS_KEY_ID", "test-access")
        .env("S3_SECRET_ACCESS_KEY", "test-secret")
        .env("S3_PREFIX", "")
        .args([
            "--execute",
            "--batch-size",
            "1",
            "--max-batches",
            "1",
            "--lease-seconds",
            "2",
            "--detail-limit",
            "1",
        ]);
    let output = run_bounded(&mut command, Duration::from_secs(20));
    let request_line = s3_task.join().expect("fake S3 task");

    let artifact = sqlx::query(
        r"
        SELECT lifecycle_state, storage_bucket, storage_path
        FROM public.worker_job_artifacts
        WHERE id = $1
        ",
    )
    .bind(artifact_id)
    .fetch_one(&pool)
    .await
    .expect("read tombstoned artifact");
    assert_eq!(
        artifact.try_get::<String, _>("lifecycle_state").unwrap(),
        "deleted"
    );
    assert!(
        artifact
            .try_get::<Option<String>, _>("storage_bucket")
            .unwrap()
            .is_none()
    );
    assert!(
        artifact
            .try_get::<Option<String>, _>("storage_path")
            .unwrap()
            .is_none()
    );

    sqlx::query("DELETE FROM public.worker_job_artifacts WHERE id = $1")
        .bind(artifact_id)
        .execute(&pool)
        .await
        .expect("clean artifact");
    sqlx::query("DELETE FROM public.worker_jobs WHERE id = $1")
        .bind(worker_job_id)
        .execute(&pool)
        .await
        .expect("clean worker job");
    sqlx::query("DELETE FROM public.users WHERE id = $1")
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("clean public user");
    sqlx::query("DELETE FROM auth.users WHERE id = $1")
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("clean auth user");

    assert!(
        output.status.success(),
        "artifact_gc failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        request_line
            .as_deref()
            .is_some_and(|line| line.starts_with("DELETE /closure-private/gc/")),
        "production artifact_gc did not issue the expected S3 DELETE: {request_line:?}"
    );
}
