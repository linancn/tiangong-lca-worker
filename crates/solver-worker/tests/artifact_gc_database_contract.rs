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

const DATABASE_CONTRACT_COMMIT: &str = "bf1add3dc9f78bbc1aaf2750a1fca9c45d788c4d";
const DATABASE_CONTRACT_FIXTURE: &str =
    "supabase/tests/fixtures/20260730_scope_closure_staged_write_set_v2_contract.json";
const DATABASE_CONTRACT_FIXTURE_SHA256: &str =
    "89d0c82a6f6a3a487be5ba77a450e5a474e68b59fad3ce752444d8894eb166be";

fn authoritative_database_fixture() -> (serde_json::Value, Vec<u8>) {
    let database_root = std::env::var("DATABASE_ENGINE_ROOT")
        .expect("DATABASE_ENGINE_ROOT points to Database #316");
    let revision = format!("{DATABASE_CONTRACT_COMMIT}:{DATABASE_CONTRACT_FIXTURE}");
    let output = Command::new("git")
        .args(["-C", &database_root, "show", &revision])
        .output()
        .expect("read authoritative Database #309 fixture");
    assert!(
        output.status.success(),
        "cannot read Database #316 fixture at {DATABASE_CONTRACT_COMMIT}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let fixture =
        serde_json::from_slice(&output.stdout).expect("parse authoritative Database #316 fixture");
    (fixture, output.stdout)
}

fn canonical_json_sha256(value: &serde_json::Value) -> String {
    fn write(value: &serde_json::Value, output: &mut Vec<u8>) {
        match value {
            serde_json::Value::Object(object) => {
                output.push(b'{');
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_by_key(|(key, _)| *key);
                for (index, (key, item)) in entries.into_iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    serde_json::to_writer(&mut *output, key).expect("serialize canonical key");
                    output.push(b':');
                    write(item, output);
                }
                output.push(b'}');
            }
            serde_json::Value::Array(items) => {
                output.push(b'[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        output.push(b',');
                    }
                    write(item, output);
                }
                output.push(b']');
            }
            _ => serde_json::to_writer(output, value).expect("serialize canonical scalar"),
        }
    }

    let mut bytes = Vec::new();
    write(value, &mut bytes);
    format!("{:x}", Sha256::digest(bytes))
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
    worker_lease_token: Uuid,
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
          requested_by, visibility, payload_schema_version, payload_json, status,
          lease_token, lease_expires_at
        ) VALUES (
          $1, 'lcia.scope_closure_check', 'calculator', 'solver', 'operator',
          $2, 'operator', 'lcia.scope_closure_check.request.v1', '{}', 'running',
          $3, now() + interval '15 minutes'
        )
        ",
    )
    .bind(worker_job_id)
    .bind(owner_id)
    .bind(worker_lease_token)
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
#[ignore = "requires DATABASE_ENGINE_ROOT with Database #316 exact commit"]
fn consumes_authoritative_database_fixture_at_pinned_commit() {
    let (fixture, fixture_bytes) = authoritative_database_fixture();
    assert_eq!(
        format!("{:x}", Sha256::digest(&fixture_bytes)),
        DATABASE_CONTRACT_FIXTURE_SHA256
    );
    assert_eq!(
        fixture["schemaVersion"],
        "lcia.scope-closure-staged-write-set-fixture.v2"
    );
    assert_eq!(
        fixture["contractVersion"],
        "lcia.scope-closure-artifact-write-set.v2"
    );
    assert_eq!(fixture["limits"]["ordinalBase"], 1);
    assert_eq!(fixture["limits"]["maximumBatchDescriptorCount"], 500);
    assert_eq!(
        fixture["canonicalization"]["descriptorSetSha256"],
        "11723d5becbb3c1c3a9a3c6d7d23f021044f260857558c4520c40614fd14e27f"
    );
    assert_eq!(
        fixture["canonicalization"]["descriptorFields"],
        serde_json::json!([
            "ordinal",
            "clientKey",
            "artifactType",
            "artifactRole",
            "bucket",
            "objectPath",
            "mediaType",
            "size",
            "checksumSha256",
            "metadata"
        ])
    );
    assert_eq!(
        fixture["rpc"]["create"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_create_v2(uuid,uuid,uuid,uuid,text,integer,text,jsonb,integer,uuid)"
    );
    assert_eq!(
        fixture["rpc"]["registerBatch"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_register_batch_v2(uuid,uuid,uuid,uuid,uuid,jsonb)"
    );
    assert_eq!(
        fixture["rpc"]["status"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_status_v2(uuid,uuid,uuid,uuid)"
    );
    assert_eq!(
        fixture["rpc"]["seal"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_seal_v2(uuid,uuid,uuid,uuid)"
    );
    assert_eq!(
        fixture["rpc"]["finalize"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_finalize_v2(uuid,uuid,uuid,uuid)"
    );
    assert_eq!(
        fixture["rpc"]["fail"]["signature"],
        "svc_lcia_scope_closure_artifact_write_set_fail_v2(uuid,uuid,uuid,uuid,text)"
    );
    assert_eq!(fixture["rpc"]["create"]["uploadEligible"], false);
    assert_eq!(
        fixture["rpc"]["seal"]["success"],
        "uploadEligible=true and artifactMap becomes visible"
    );
    assert_eq!(
        fixture["states"]["registration_open"]["readyArtifactRows"],
        0
    );
    assert_eq!(fixture["states"]["staging"]["readyArtifactRows"], 0);
    assert_eq!(
        fixture["states"]["ready"]["readyArtifactRows"],
        "expectedDescriptorCount"
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
    let worker_job_id: Uuid = std::env::var("WORKER_JOB_ID")
        .expect("WORKER_JOB_ID")
        .parse()
        .expect("valid Worker job ID");
    let worker_lease_token: Uuid = std::env::var("WORKER_LEASE_TOKEN")
        .expect("WORKER_LEASE_TOKEN")
        .parse()
        .expect("valid Worker lease token");
    let request_id: Uuid = std::env::var("REQUEST_ID")
        .expect("REQUEST_ID")
        .parse()
        .expect("valid request ID");
    let endpoint = std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT");
    let object_prefix = std::env::var("OBJECT_PREFIX").expect("OBJECT_PREFIX");
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("connect child to Database #316");
    let payloads = [
        (
            "closure-bundle-v3.json",
            "closure_bundle",
            "closure_bundle",
            "application/json",
        ),
        (
            "closure-report-v3.xlsx",
            "closure_report_xlsx",
            "closure_report",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        (
            "manifest.json",
            "closure_complete_machine_result",
            "complete_machine_result",
            "application/vnd.tiangong.scope-closure-manifest+json",
        ),
    ];
    let items = payloads
        .iter()
        .enumerate()
        .map(
            |(index, (name, artifact_type, artifact_role, media_type))| {
                let bytes = format!("crash-fixture-{index}");
                let metadata = if *artifact_role == "closure_bundle" {
                    serde_json::json!({
                        "schemaVersion": "lcia.scope-closure-artifact.v2",
                        "closureCheckId": closure_check_id,
                        "fileName": name,
                        "artifactRole": artifact_role,
                        "retentionSeconds": 604_800,
                        "contentArtifactManifestHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "completeMachineResultClientKey": "manifest.json",
                    })
                } else {
                    serde_json::json!({
                        "schemaVersion": "lcia.scope-closure-artifact.v2",
                        "closureCheckId": closure_check_id,
                        "fileName": name,
                        "artifactRole": artifact_role,
                        "retentionSeconds": 604_800,
                        "contentArtifactManifestHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    })
                };
                serde_json::json!({
                    "ordinal": index + 1,
                    "clientKey": name,
                    "artifactType": artifact_type,
                    "artifactRole": artifact_role,
                    "bucket": "closure-private",
                    "objectPath": format!("{object_prefix}/{name}"),
                    "mediaType": media_type,
                    "size": bytes.len(),
                    "checksumSha256": format!("{:x}", Sha256::digest(bytes.as_bytes())),
                    "metadata": metadata
                })
            },
        )
        .collect::<Vec<_>>();
    let descriptor_set_sha256 = canonical_json_sha256(&serde_json::json!({
        "contractVersion": "lcia.scope-closure-artifact-write-set.v2",
        "descriptors": &items,
    }));
    let required_primary_roles = serde_json::json!([
        {
            "artifactRole": "closure_report",
            "artifactType": "closure_report_xlsx",
            "mediaType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "exactCount": 1
        },
        {
            "artifactRole": "complete_machine_result",
            "artifactType": "closure_complete_machine_result",
            "mediaType": "application/vnd.tiangong.scope-closure-manifest+json",
            "exactCount": 1
        },
        {
            "artifactRole": "closure_bundle",
            "artifactType": "closure_bundle",
            "mediaType": "application/json",
            "exactCount": 1
        }
    ]);
    let created = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_create_v2(
          $1, $2, $3, $4, 'lcia.scope-closure-artifact-write-set.v2',
          $5, $6, $7::jsonb, 2, null
        ) AS result
        FROM _service_role
        ",
    )
    .bind(closure_check_id)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .bind(request_id)
    .bind(i32::try_from(items.len()).unwrap())
    .bind(&descriptor_set_sha256)
    .bind(&required_primary_roles)
    .fetch_one(&pool)
    .await
    .expect("create DB-first staged write set")
    .try_get::<serde_json::Value, _>("result")
    .expect("read write-set create result");
    assert_eq!(created["ok"], true);
    assert_eq!(created["data"]["status"], "registration_open");
    assert_eq!(created["data"]["uploadEligible"], false);
    let write_set_id: Uuid = created["data"]["writeSetId"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let write_token: Uuid = created["data"]["writeToken"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    for (batch_index, batch) in items.chunks(2).enumerate() {
        let batch_id = Uuid::from_u128(177_000 + u128::try_from(batch_index).unwrap());
        let registered = sqlx::query(
            r"
            WITH _service_role AS (
              SELECT set_config('request.jwt.claim.role', 'service_role', true)
            )
            SELECT public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
              $1, $2, $3, $4, $5, $6::jsonb
            ) AS result
            FROM _service_role
            ",
        )
        .bind(write_set_id)
        .bind(write_token)
        .bind(worker_job_id)
        .bind(worker_lease_token)
        .bind(batch_id)
        .bind(serde_json::Value::Array(batch.to_vec()))
        .fetch_one(&pool)
        .await
        .expect("register bounded descriptor batch")
        .try_get::<serde_json::Value, _>("result")
        .expect("read batch result");
        assert_eq!(registered["ok"], true);
        assert_eq!(registered["data"]["uploadEligible"], false);
    }
    let sealed = sqlx::query(
        r"
        WITH _service_role AS (
          SELECT set_config('request.jwt.claim.role', 'service_role', true)
        )
        SELECT public.svc_lcia_scope_closure_artifact_write_set_seal_v2(
          $1, $2, $3, $4
        ) AS result
        FROM _service_role
        ",
    )
    .bind(write_set_id)
    .bind(write_token)
    .bind(worker_job_id)
    .bind(worker_lease_token)
    .fetch_one(&pool)
    .await
    .expect("atomically seal complete descriptor set")
    .try_get::<serde_json::Value, _>("result")
    .expect("read seal result");
    assert_eq!(sealed["ok"], true);
    assert_eq!(sealed["data"]["status"], "staging");
    assert_eq!(sealed["data"]["uploadEligible"], true);
    assert_eq!(
        sealed["data"]["artifactMap"]
            .as_object()
            .map(serde_json::Map::len),
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
#[ignore = "requires a disposable local Database #316 schema at DATABASE_URL"]
#[allow(clippy::too_many_lines)]
async fn db_first_crash_is_fully_reconciled_after_restart() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .expect("connect local Database #316");
    let owner_id = Uuid::new_v4();
    let worker_job_id = Uuid::new_v4();
    let worker_lease_token = Uuid::new_v4();
    let closure_check_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    seed_crash_closure(
        &pool,
        owner_id,
        worker_job_id,
        worker_lease_token,
        closure_check_id,
    )
    .await;
    let object_prefix = format!("scope-closure/{closure_check_id}/{request_id}");
    let object_store = FakeObjectStore::start();

    let current_test = std::env::current_exe().expect("current integration-test executable");
    let crash = run_bounded(
        Command::new(current_test)
            .env("WORKER_ARTIFACT_CRASH_CHILD", "1")
            .env("DATABASE_URL", &database_url)
            .env("CLOSURE_CHECK_ID", closure_check_id.to_string())
            .env("WORKER_JOB_ID", worker_job_id.to_string())
            .env("WORKER_LEASE_TOKEN", worker_lease_token.to_string())
            .env("REQUEST_ID", request_id.to_string())
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
            format!("/closure-private/{object_prefix}/closure-bundle-v3.json"),
            format!("/closure-private/{object_prefix}/closure-report-v3.xlsx"),
        ]),
        "failpoint must abort after exactly two registered uploads"
    );
    tokio::time::sleep(Duration::from_millis(2_200)).await;

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
    let retained_item_count = sqlx::query(
        r"
        SELECT count(*) AS count
        FROM public.lcia_scope_closure_artifact_write_set_items
        WHERE write_set_id = $1
        ",
    )
    .bind(write_set_id)
    .fetch_one(&pool)
    .await
    .expect("count retained immutable write-set items")
    .try_get::<i64, _>("count")
    .unwrap();
    assert_eq!(
        retained_item_count, 3,
        "one bounded detail-cleanup slot must be reclaimed without touching other audit items"
    );
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
