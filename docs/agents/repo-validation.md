---
title: worker Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: worker
language: en
whenToUse:
  - when a tiangong-lca-worker change is ready for local validation
  - when deciding the minimum proof required for solver, worker, script, runtime-contract, or docpact changes
  - when writing PR validation notes for tiangong-lca-worker work
whenToUpdate:
  - when the repo gains new canonical validation wrappers
  - when change categories require different proof
  - when runtime SQL or parity-validation expectations change
checkPaths:
  - docs/agents/repo-validation.md
  - .docpact/config.yaml
  - .env.example
  - Cargo.toml
  - Makefile
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/lca-api-contract.md
  - docs/scope-closure-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
  - .github/workflows/**
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedCommit: cfc356d96f7fe47e4f128ce1551c5ce3f3f47326
lastReviewedAt: 2026-08-04
lastReviewedNote: "Reviewed for Worker Issues #205 and #207: proof retains file-backed large-manifest permissions/cleanup and requires restricted-login, exact Database #409, concurrency/reconnect, no-fallback, secret-free evidence, and exact compile-time source-commit attestation."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ../../docs/lca-api-contract.md
  - ../../docs/scope-closure-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-submit-fast-gate-contract.md
  - ../../docs/tidas-package-contract.md
  - ./contracts/scope-closure-memory-and-result-contract.md
---

## Default Baseline

Unless the change is doc-only repo-maintenance work, the default baseline is:

```bash
make check
cargo clippy -p solver-worker --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Treat the last two commands as non-negotiable hard gates after code changes.

The snapshot private-cutover static gate currently pins 103 active Rust,
Python, shell, and SQL sources with inventory SHA-256
`fe645b0ca9f44ec12b763ca90e967cf88788fcb5fe598a7247789aa9c09d8d9d`.
Issue #207 adds the document-validation database module plus its source-commit
build metadata and qualification test. The reviewed inventory change does not
alter the three private snapshot relation identities scanned by that gate.

The local `pre-push` hook runs the docpact gate first and then runs `make check`. The GitHub `ci` workflow is manual-dispatch only, so ordinary branch pushes do not spend Actions minutes on standalone tests.

## Validation Matrix

For scope-closure transport changes, include `cargo test -p solver-worker scope_closure_snapshot_transport_keeps_large_manifest_out_of_argv_and_cleans_up` and `cargo test -p solver-worker --bin snapshot_builder snapshot_builder_reads_scope_closure_snapshot_from_file`. These tests must prove that a manifest larger than ordinary argv capacity is file-backed, permission-restricted, available for parsing, absent from argv/diagnostics, and removed after the owning handle is dropped.

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `crates/**` solver or worker code | `make check`; hard Clippy gate; hard format gate | run the narrow manual script that matches the touched area, such as snapshot build, full compute debug, or BW25 validation | Record which job family or worker path was exercised. |
| private LCA snapshot persistence consumer | `python3 scripts/qualify_snapshot_private_cutover.py --static-only`; `cargo test -p solver-worker --bin snapshot_builder`; snapshot retention and scope-closure lifecycle tests; baseline hard gates | pass only an exact source checkout: `python3 scripts/qualify_combined_candidate.py --database-repo <database-engine-at-c5356d2b0d340f9c5c31a645479be5f3d19a52db> --tidas-bin <exact-tidas-0.1.3-binary> --receipt-output <outside-git.json>` | Static scanning is a scoped diagnostic, not final consumer-zero proof. Live qualification rejects caller-supplied database URLs: the runner locally clones the exact Database commit into its own temporary workdir, allocates a unique project identity and seven loopback ports, starts that stack, and permits reset only through the runner-owned object. Teardown uses `supabase stop --no-backup`, verifies zero project-labeled containers/volumes/networks, and removes the temporary workdir. Each failure/success lifecycle creates a unique bucket/prefix and writes both an owned sentinel and a sibling-prefix sentinel. Exact-prefix cleanup must leave zero owned keys while the sibling bytes/SHA remain identical; only then is the sibling deleted by exact key. No bucket-wide deletion is used. Evidence labels the 99 discovered auth/private/public/storage relations as count-only cardinality comparison and lists the nine critical auth users/sessions, package, and private snapshot relations with before/after full-content hashes and row counts. It also binds the frozen Worker status/tree/patch, exact Database/TIDAS identities, lifecycle objects, cleanup readbacks, full log, and stack destruction. |
| document-validation evidence dedicated DB | `cargo test -p solver-worker document_validation`; `cargo test -p solver-worker config::tests`; `cargo test -p solver-worker --test source_commit_attestation`; baseline hard gates | against a runner-owned disposable loopback PG17 stack at exact Database #409, run ignored `isolated_pool_concurrency_close_and_reconnect_are_fail_closed`; prove restricted LOGIN flags, no object ownership/DDL, exact membership, private success, public/table/unregistered-private denial, exact success/error envelopes, non-empty concurrent idempotency, forced backend reconnect, and zero residue | `DOCUMENT_VALIDATION_DATABASE_URL` is mandatory only for solver-worker and never falls back. The exact migration ledger head is verified by the privileged local operator/harness; the restricted runtime instead pins the two routine bodies/comments/ACLs and rejects same-name overloads. Existing registered result-GC capabilities remain under their own disabled/token gates and are never referenced by this family module. The build must fail closed unless the complete source worktree is clean, including untracked files; it must discard hook-provided repository-local `GIT_*` context and resolve the crate checkout itself. Only then may the successful preflight journal marker contain the exact 40-character source commit while omitting identity/target data. No hosted/production mutation is valid proof. Receipts and errors must omit URL, database/login identity, credentials, payloads, and connection-string hashes. |
| shared resource admission or object file I/O | `cargo test -p solver-worker --lib resource::tests`; `cargo test -p solver-worker --lib storage::tests`; `cargo test -p solver-worker --lib result_insert_`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates | exercise a local no-`Content-Length` response, cancellation during streaming, hash mismatch, preflight oversize rejection, multipart abort, result PUT/delete redirect rejection, generic delete redirect following, lost INSERT acknowledgement, absent readback, identity conflict, and unknown DB outcome when isolated test infrastructure is available | Assert stable resource error codes, fixed buffers, and absence of destination partial files. New result locators must bind the preallocated UUID in one exact `/results/<result_uuid>/` segment; reject origin, scheme/port, bucket, prefix, case, UUID, query/fragment, duplicate/empty/dot, encoded, and redirect drift before result-aware PUT or deletion. Exact persisted identity is the only lost-ack success. A single absent readback remains indeterminate, preserves exact `result_id`/object-key/error evidence, and must never trigger deletion; safe automatic cleanup requires a DB-side staged identity/fence. Keep generic package/snapshot/artifact delete redirect behavior and legacy byte-returning APIs compatible, but use explicit-cap file APIs for newly migrated heavy paths. |
| Calculation Bundle / all-unit directional LCI | `cargo test -p solver-worker calculation_bundle`; `cargo test -p solver-worker artifacts`; `cargo test -p solver-core cache`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | with safe DB/S3 env, rebuild one snapshot, run `solve_all_unit`, verify manifest-last upload, all compressed/uncompressed hashes, exact 256-process boundaries, query-v2 chunk ranges/hashes without `h_matrix`, reviewed 25-method identities, recursively complete TIDAS source closure, directional LCI parity, and retry byte determinism. Exercise multiple admitted snapshots and prove resident cache bytes never exceed capacity, LRU eviction is deterministic, invalidation releases bytes, oversized workloads reject before factorization, and actual UMFPACK peak/retained estimates are reported. Include an LCIA-factor-only Elementary Flow and prove it appears as `support` with transitive Flow Property/Unit Group/Source/Contact documents while compiled Flow count, B/C axes, and provider decisions remain inventory-derived; also prove a non-Elementary factor target fails closed. | Old snapshots without `compiled_graph.release_evidence.source_datasets`, or with the legacy exchange-only source-closure policy, must fail closed and be rebuilt. Never infer exchange IDs, versions, units, directions, provider output IDs, or source documents from matrix indices or mutable solve-time database state. Sparse factorization fill-in is workload-dependent; tune admission from observed workloads and do not claim an input-independent constant bound. |
| solver `worker_jobs` queue backend | `cargo test -p solver-worker worker_jobs`; `cargo test -p solver-worker maps_worker_jobs`; `cargo check -p solver-worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=solver` job and run `solver-worker --queue-backend worker-jobs --mode worker` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_jobs` is absent or ignored | Keep `docs/lca-api-contract.md` and `docs/edge-function-integration.md` aligned with job kind, payload schema, worker_jobs result_ref, and optional legacy `lca_jobs` compatibility expectations. |
| certificate-grade scope closure / package binding | `cargo test -p solver-worker scope_closure`; `cargo test -p solver-worker maps_scope_closure_payload_from_database_envelope`; `cargo test -p solver-worker package_closure_binding_is_all_or_none_and_result_ref_preserves_check_id`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | Run the release qualification harness against the external open-data package twice and require byte-identical non-summary artifacts, manifest, ordering, and logical hashes. Native TIDAS must complete within 60 seconds and 512 MiB peak RSS; the production-shaped complete closure must finish within 10 minutes and 4 GiB process-tree peak RSS. Run the same production issue/root distribution at 1×, 2×, 5×, and 10×, recording wall time, process-tree RSS, TIDAS RSS, Linux cgroup anon/file/current/peak when available, temporary bytes, partition/artifact bytes and counts, descriptor count, maximum object bytes, and cache reclaim. Prove the unified TIDAS/graph/frozen-source/provider/matrix/factorization/LCIA-readiness issue set, blocker/verdict/certificate inputs, exact counts, root membership, and on-demand witnesses are semantically equivalent while `expandedAffectedRootRecordCount=0`. Reproduce an old-layout logical bundle above the storage object limit with deterministic generated/replayed evidence, then prove the v4 binding manifest stays small, ordinary administrative evidence partitions close at 25,000 records or 32 MiB uncompressed, and every physical object is at most 256 MiB. Exercise logical-record sizes at 32 MiB−1, exactly 32 MiB, 32 MiB+1, 36,105,476 bytes, 64 MiB, and above 256 MiB with generated high-entropy and Unicode/newline payloads. Require the oversized-record index and fixed chunks to preserve canonical record-plus-newline relation hashes across two runs, and inject missing, duplicate, reordered, and corrupted chunks. Exercise cancellation or crash/retry at coalesce, administrative/issue partition write, pre-write-set object admission, bounded batch registration, atomic seal, upload, and finalize, with no visible partial write set, orphan, or local temporary leak. Run the exact Database #316 fixture/digest/ordinal contract against matching canonical migrations in isolated local DB/storage. | Keep the package and generated output outside git. Use local fixtures and isolated non-production DB/storage only; never deploy, restart a server, enqueue production work, mutate production state, or update the root pointer for validation. A mock-only traversal is insufficient. Keep `docs/agents/contracts/scope-closure-memory-and-result-contract.md`, `docs/scope-closure-contract.md`, `docs/lca-api-contract.md`, and `docs/tidas-package-contract.md` aligned with Database #316 and the public TIDAS protocol. |
| versioned public-plus-owner-draft snapshot / LCIA evidence | `cargo test -p solver-worker calculation_evidence`; `cargo test -p solver-worker static_lcia_cache`; `cargo test -p solver-worker maps_exact_public_owner_draft_build_v2`; `cargo test -p solver-worker rejects_summary_only_lcia_manifest_before_build_execution`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; baseline hard gates | run ignored `verifies_reviewed_release_bundle_bytes` with `LCIA_STATIC_CACHE_RELEASE_DIR=<next-public-root>` whenever the reviewed static bundle changes; in a non-production environment with DB/S3 available, enqueue one v2 build and verify public `100`, owner `0`, foreign/nonzero/collaboration rejection, snapshot-index source/identity hashes, per-method JSONL gap count, worker-only build result projection, and solve binding drift rejection | Never use a production data mutation as validation. Keep the complete reviewed manifest plus Edge/Next/Worker v2 source, scope, matrix, and release hashes byte-for-byte aligned; reject summary-only manifests during queue payload validation, and reject v1 source/evidence/solve downgrade. |
| snapshot-builder signed-flow linking or routing | `cargo test -p solver-worker --lib signed_flow`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates; `./scripts/build_snapshot_from_ilcd.sh` when safe | exercise Product and Waste reference Input/Output with positive/negative amounts, opposite/same-sign candidates, multi-reference rejection, self-link exclusion, request-root flow-space closure, multi-candidate weights, and closed/open/cutoff evidence. For Flow identity changes, include one UUID with two referenced exact revisions plus one unreferenced historical revision; prove exact provider isolation, two compact flow-axis rows, omitted-version freezing, and pruning of the unreferenced revision | Keep `docs/provider-linking.md` and both implicit regional supply mix docs aligned. Assert non-negative activity requirements and signed closure, not direction/type-based provider roles. Explicit Flow versions must never be silently replaced by the latest revision. |
| matrix-readiness / signed-balance closure gate | `cargo test -p solver-worker readiness`; `cargo check -p solver-worker --bin matrix_readiness`; hard Clippy gate for the touched binary/module | run `snapshot_builder` or `matrix_readiness --input <fixture> --out <report>` against the closest available target artifact; verify `balance_evidence`, `unresolved_balances`, and explicit boundary-policy behavior | Keep `docs/matrix-readiness-report-contract.md` aligned with schema, blocker/finding code, policy, and next_action changes. Use `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` on Homebrew setups. |
| review-submit fast gate / source-reference closure / snapshot subprocess | `cargo test -p solver-worker review_submit_gate`; `cargo test -p solver-worker source_reference_policy`; `cargo test -p solver-worker snapshot_source_closure`; `cargo test -p solver-worker snapshot_builder_protocol`; `cargo test -p solver-worker --test source_reference_characterization`; `cargo check -p solver-worker --bin review_submit_gate`; for DB runner or `worker_jobs` changes also run `cargo test -p solver-worker worker_jobs`, `cargo test -p solver-worker review_submit_gate_runner`, and `cargo check -p solver-worker --bin review_submit_gate_runner`; hard Clippy/format gates | run the two sanitized lineage/model-composition fixtures plus one successful review-submit control; compare Process/Flow axes, sparse payload, numeric result, evidence/hash/status, redaction and lease behavior. In isolated non-production DB/S3, collect at least 20 valid hot and cold repeats for source document/reference counts, frontier rounds, support query count, classification/support/total elapsed and peak RSS; total snapshot p95 may not regress more than 5%. Exercise timeout, signal, missing/duplicate/unknown terminal, strict `0+succeeded` / `42+blocked` exit pairing and both mismatches, blocked truncation metadata, and lease loss; verify child cleanup and no stale writeback. | Preserve only existing `passed|blocked|error`, `blockingReasons`, and `calculatorReport`; do not add DB status enums/migrations. Lineage/model-composition must not probe targets or expand axes; exact exchange/provider dependencies still fail closed. Never use production enqueue/write for validation. |
| maintenance worker_jobs / GC orchestration | `cargo check -p solver-worker --bin maintenance_worker`; `cargo check -p solver-worker --bin maintenance_enqueue`; run touched binaries such as `cargo check -p solver-worker --bin snapshot_gc --bin result_gc --bin artifact_gc --bin package_gc --bin process_flow_graph_cache_builder`; `cargo test -p solver-worker --bin maintenance_worker`; `cargo test -p solver-worker --bin maintenance_enqueue`; run the touched GC/filter/cache tests such as `cargo test -p solver-worker artifact_gc`, `cargo test -p solver-worker snapshot_gc`, `cargo test -p solver-worker result_gc`, `cargo test -p solver-worker package_gc`, or `cargo test -p solver-worker --bin process_flow_graph_cache_builder`; hard Clippy gate for all targets | run only a safe dry-run `lca.snapshot_gc`, `lca.result_gc`, `worker.artifact_gc`, `tidas.package_artifact_gc`, or `national_carbon.process_flow_graph_cache_build` worker job in dev when DB and storage env are available; result GC must prove exact result-locator rejection before network I/O, while generic artifact GC must prove object-delete-before-complete, missing-object idempotency, bucket/path rejection, bounded claim batches, and retry without tombstoning | Keep `docs/agents/repo-architecture.md`, `README.md`, deployment units, and the package/LCA retention docs aligned with job kind, payload, summary, destructive-execute safety semantics, and fixed stdout/stderr capture limits. Never enable result-GC claims or perform a real object delete as part of local validation. |
| package worker import or export flows | baseline gates; real release-binary handshake against the active governed `0.1.3` default; active-source audit proving no Python validator or validator-command fallback remains | validate the largest available package locally with the release `tidas` binary before any server execution; verify `tidas.operation-report.v1`, exact version, `asset_fingerprint`, issue-spool SHA-256/bytes/event count, bounded memory/queue settings, and stable Worker error codes; run the closest safe package-flow helper when isolated DB/S3 is available | The large package fixture remains outside git. Package-job semantics are runtime-sensitive and may depend on storage or DB state; never make a production mutation for validation. |
| package `worker_jobs` queue backend | `cargo test -p solver-worker --bin package_worker`; `cargo test -p solver-worker package_worker`; `cargo check -p solver-worker --bin package_worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=package` job and run `package_worker --package-queue-backend worker-jobs` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_package_jobs` is absent or ignored | Keep `docs/tidas-package-contract.md` aligned with job kind, payload schema, continuation behavior, artifact projection, worker_jobs result_ref, and optional legacy `lca_package_jobs` compatibility expectations. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| repo docs, `.env.example`, or docpact config only | `scripts/docpact validate-config --root . --strict`; `scripts/docpact lint --root . --worktree --mode enforce` | perform route checks for affected intent surfaces such as `solver-runtime`, `package-worker`, or `runtime-sql-boundary` | Refresh review metadata even when prose-only docs change. Keep `.env.example` secret-free. |

## Isolated Worker Control-Plane Database Contract

Run `scripts/run_worker_control_plane_db_integration.sh` against a fresh,
loopback-only Supabase/Postgres stack built from the exact Database Engine #335
worktree and migration head. The wrapper verifies the clean database worktree
HEAD before emitting a locator-free SHA manifest. The Rust harness performs
migration metadata preflight before switching every behavioral connection with
`SET ROLE service_role`; this proves the local ACL matrix only and must never be
reported as proof of the deployment login role. Production `DATABASE_URL`
`current_user`, pooler mode, and effective grants remain an external evidence
gate.

The harness covers duplicate enqueue/lost response, two-worker concurrent
claim, stale-lease reclaim, lease-token fencing, restart/retry, cancellation,
terminal result, transaction rollback, and old-public/new-private parity. It
refuses non-loopback targets and does not create or reuse containers or volumes.
The caller owns creation and teardown of the isolated exact-head stack.

## Combined Candidate Exact-Head Qualification

Run `scripts/qualify_combined_candidate.py` only from a clean committed Worker
tree. The runner accepts the exact Database Engine
`c5356d2b0d340f9c5c31a645479be5f3d19a52db` source checkout with migration head
`20260803090000`; it never accepts a database URL. It owns the temporary clone,
project identity, loopback ports, database reset authority, Storage
buckets/prefixes, and teardown. Receipt output must stay outside the repository.
The deterministic receipt binds the Worker commit/tree, Database commit,
migration file/blob/hash, exact TIDAS 0.1.3 binary hash, passing contract names,
cleanup facts, and the explicit claim that result-GC claiming remained disabled.
It contains no endpoint, credential, token, random object name, or hosted target.
The exact migration tree remains clean; the only permitted tracked checkout
change is the runner-owned `supabase/config.toml` project identity and seven
loopback port substitutions, which the receipt records separately.
The scope-closure DB/Storage lifecycle runs first against the fresh stack so its
reset comparison is not contaminated by later control-plane contract fixtures.
The receipt reports any later runner-owned fixture cardinality delta before
teardown separately from the final zero-container/volume/network/workdir
destruction proof. The result sentinel uses exact-key deletion followed by
deletion of only its unique empty runner-owned bucket.

The real result-identity contract inserts an explicitly preallocated UUID,
reconciles only an exact visible row after a simulated lost acknowledgement,
keeps absence/conflict/query-error outcomes non-destructive, and checks strict
locator boundaries. The combined runner separately executes and records the
exact narrow `result_locator_validation_rejects_target_and_path_drift` unit
test command, so origin, bucket, prefix, result UUID, and encoded-path claims are
bound to that run rather than inferred from the broader `make check`. The DB
fixture receives no Storage credentials; a runner-owned local sentinel must
remain byte-identical until the runner deletes that exact key.
The exact-head production insert path remains explicitly
`public.lca_results`; this qualification claims neither a result-family physical
move nor a result-consumer cut. Snapshot/control/hash-family consumer-zero proof
does not extend that claim to the result family.

### Scope-closure lifecycle fixture

The ignored `scope_closure_package_v2_e2e` lifecycle fixture is a self-contained TIDAS 0.1.3 mini graph. Before it mutates the isolated database, it must validate exactly 34 documents with zero issues using the release `tidas` binary. Schema evolution must be handled by updating the fixture documents and their closed references; never bypass or weaken this preflight. The revoked-certificate case proves fail-closed admission by requiring the database claim boundary to reject the queued job before runtime execution and by verifying that no result package exists.

### Scope-closure capacity input modes

Select `real-payload` or `synthetic-cardinality` explicitly. Generated cardinality is scaling evidence only and must never be cited as real-package evidence. Real-payload qualification bounded-reads and preserves every actual package document JSON value, accounts for every package member, and fails rather than silently skipping, replacing, or truncating a document.

Run the tracked qualification layers in this order:

```bash
make qualification-test
# Then run the generated Rust boundary tests named by the root #518 plan.
TIDAS_BIN=/exact/linux/tidas \
SCOPE_CLOSURE_QUALIFICATION_COMPONENTS='<exact component SHA JSON>' \
  ./scripts/run_scope_closure_external_qualification.sh \
  --fixture /absolute/path/to/local-open-data.zip \
  --output /outside-git/external-result

QUALIFICATION_NON_PRODUCTION_CONFIRMATION=I_CONFIRM_ISOLATED_NON_PRODUCTION_TARGETS \
SCOPE_CLOSURE_QUALIFICATION_COMPONENTS='<exact component SHA JSON>' \
  ./scripts/run_scope_closure_provider_qualification.sh \
  --output /outside-git/provider-result.json
```

The external executable requires Linux cgroup v2 evidence and exact TIDAS
`0.1.3`; it emits `cold`, `warm`, `mixed`, and `stale` real-payload capacity
results plus `external-result.json`. The provider executable requires isolated
loopback Database/Supabase/S3 targets and four git-tracked owning-repository
adapters selected by `QUALIFICATION_DATABASE_HARNESS`,
`QUALIFICATION_STORAGE_HARNESS`, `QUALIFICATION_EDGE_HARNESS`, and
`QUALIFICATION_NEXT_HARNESS`. A missing owner adapter or non-production
credential is an external blocker, never a skipped or synthetic pass.

`make qualification-test` covers missing child fields, wrong exact SHAs, wrong
cache modes, semantic/artifact drift, production fingerprints, secret/locator
or payload leakage, unsafe archives, and cleanup residue. Harness stdout/stderr
and private temporary files are not evidence and are removed before the final
aggregate is written.

For every administrative relation, report record count and p50/p95/p99/max logical and standalone-zstd record bytes with the maximum exact identity; represent empty relations explicitly with zero count. Exercise cold, warm, mixed, and stale cache replay without changing exact event bytes. The generated non-sensitive boundary set covers 32 MiB minus/equal/plus one byte, exactly 36,105,476 bytes, 64 MiB, incompressible content, Unicode/newlines, and oversized human-report fields through the segmented-record contract.

## Isolated Review-Submit Source-Closure Benchmark

Use `scripts/run_review_submit_source_closure_benchmark.sh` for the review-submit source-reference performance gate. Commit the harness first, then pass that exact candidate ref. The runner builds exact baseline and candidate commits in detached temporary worktrees, verifies the candidate contains and self-tests the committed runner/parser/fixture, archives an exact `database-engine` schema commit without using its running project or data volume, and starts a fresh Supabase project with a unique candidate-derived project ID, distinct ports, Docker volumes, Storage S3 credentials, bucket, and object prefix. It refuses to reuse or delete any pre-existing container, volume, or network for the selected project ID.

The Worker-owned fixture uses stable synthetic identities. Before every paired cold run, the runner advances only the dependency Process `modified_at` outside the timed process, then asserts exactly one returned row, the exact Process ID, the unchanged non-empty version, and a strictly newer revision timestamp. This avoids changing fixture documents or bypassing writer triggers while making the source summary cold for both binaries. Variant-specific review checksums and object prefixes independently prevent one exact binary from reusing the other's overlay artifacts. Hot measurements require a separate explicit successful warmup for each variant followed by exact snapshot reuse. Variant order alternates to reduce order bias. Each cache mode requires at least 20 valid samples per exact binary, records the shared legacy `[build_timing_sec].total_sec` field as the primary metric, wall time as supplementary evidence, process-tree peak RSS, raw candidate closure counters, median, nearest-rank p95, population variance, standard deviation, and coefficient of variation, and exits nonzero when either candidate primary p95 regresses by more than 5%.

Never point this runner at `project_id=database-engine`, an existing Database worktree volume, shared ports, hosted Supabase, or production S3. Its JSON evidence contains only local endpoints, fixed fixture identities, exact commits, binary hashes, aggregate statistics, and raw timing/resource samples; it must not contain database passwords or Storage credentials.

## Replay A Previously Successful Calculation

Use this acceptance flow when a matrix-build or allocation-semantics change needs proof against a previously successful calculation:

1. Select a completed calculation whose resolved process closure contains at least one process with co-product allocation data affected by the change. Keep one or more unaffected processes from the same scope as controls.
2. Capture the baseline before replay: the canonical original request and its hash; worker job ID; solve job ID; snapshot ID; result ID; result artifact URL, format, byte size, and SHA-256; and the snapshot process/flow/impact counts plus A/B/C nonzero counts.
3. Replay the same business request with new build and solve job IDs, a new requested snapshot ID, a new result ID, and fresh idempotency/request keys. Do not mutate or reuse the completed task records.
4. Verify that `allocation_semantics_version`, `link_semantics_version`, `technosphere_boundary_policy`, `flow_identity_policy`, `source_closure_policy`, and `source_reference_policy` participate in snapshot and review identity/reuse decisions. Confirm that changed semantics produce a newly built snapshot rather than reusing the baseline; legacy source-reference policy artifacts remain readable but are cache misses.
5. Export and independently validate both results, keeping separate output paths:

   ```bash
   ./scripts/export_latest_matrices.sh --result-id <old-result-id> --base-name before --no-latest-pointers
   ./scripts/export_latest_matrices.sh --result-id <new-result-id> --base-name after --no-latest-pointers
   ./scripts/run_bw25_validation.sh --result-id <old-result-id>
   ./scripts/run_bw25_validation.sh --result-id <new-result-id>
   ```

6. Compare by process and flow UUID rather than matrix index. Record the old/new A and B entries for affected processes, the old/new target LCIA values with absolute and relative deltas, and tolerance results for the unaffected control processes.
7. Perform every replay write only in staging or a local environment with isolated database and object-storage state. Production reads may be used to select or copy an authorized baseline, but production mutation, enqueue, snapshot creation, cache invalidation, or active-pointer changes are prohibited as validation steps.

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which baseline gates ran
2. which job family, script, or manual parity helper was exercised
3. whether any required database-engine or edge-functions proof lives elsewhere

## Docpact Governance Notes

The repo's machine-readable governance source is `.docpact/config.yaml`.

That means:

- governed-doc rules, routing intents, ownership boundaries, and freshness live in `.docpact/config.yaml`
- `.github/workflows/ai-doc-lint.yml` is manual-dispatch fallback and should delegate to the same local docpact gate
- retained explanatory docs stay in `AGENTS.md`, this file, `repo-architecture.md`, `README.md`, and the narrow runtime-facing contract docs under `docs/*.md`

Do not recreate deleted `ai/*` files under a new name. Keep deterministic facts in config and explanatory material in retained source docs.

## Local Docpact Push Gate

Install the versioned local hook once per checkout:

```bash
./scripts/install-git-hooks.sh
```

The `pre-push` hook runs `scripts/docpact-gate.sh`, which delegates CLI lookup to `scripts/docpact` and performs strict config validation plus enforced lint before the push leaves the machine. It then runs `make check` as the local test gate. The wrapper checks `DOCPACT_BIN`, Cargo install locations, Homebrew install locations, and then `PATH`, so local agent shells should not fail only because bare `docpact` is unavailable. The default comparison base is `origin/main`. Override it for unusual stacks with `DOCPACT_BASE_REF=<ref>` or `scripts/docpact-gate.sh --base <ref>`. The gate writes its detailed report to a temporary file so normal pushes do not create `.docpact/runs/` artifacts.
