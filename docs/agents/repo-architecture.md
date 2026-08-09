---
title: worker Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: worker
language: en
whenToUse:
  - when you need a compact mental model of the solver stack before editing crates, workers, or runtime SQL expectations
  - when deciding which crate or binary owns a behavior change
  - when snapshot build, package flow, or contribution-path analysis is mentioned without exact paths
whenToUpdate:
  - when major crate boundaries or job families change
  - when result persistence or runtime SQL boundaries move
  - when the current map becomes misleading
checkPaths:
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - Cargo.toml
  - crates/**
  - scripts/**
  - tools/bw25-validator/**
  - supabase/migrations/**
  - docs/lca-api-contract.md
  - docs/scope-closure-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/provider-linking.md
  - docs/implicit-regional-supply-mix-modeling.md
  - docs/implicit-regional-supply-mix-modeling.en.md
  - docs/tidas-package-contract.md
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-08-09
lastReviewedCommit: 63dac07d858a14427f663a03c69f17db9ed26419
lastReviewedNote: "Reviewed for Worker PR #225: private/api/util SQL boundaries and compact discovery paths remain aligned with Worker runtime ownership."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ../../docs/lca-api-contract.md
  - ../../docs/scope-closure-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-submit-fast-gate-contract.md
  - ./contracts/scope-closure-memory-and-result-contract.md
---

## Repo Shape

This repo is a Rust workspace with three core layers:

- `crates/suitesparse-ffi`
- `crates/solver-core`
- `crates/solver-worker`

The runtime solves sparse systems asynchronously and keeps heavy compute out of the API layer.

## Core Solver Invariants

Keep these constraints in mind before editing `crates/solver-core/**` or worker solve flows:

- The runtime solves the sparse system `Mx = b` with `M = I - A`; preserve that modeling contract when reshaping matrix-build code.
- Do not introduce explicit matrix inversion for solve paths. Reuse factorization or sparse-solve flows instead.
- Heavy recomputation belongs in async worker jobs, not inline request handlers or API-edge adapters.
- If a change affects factorization reuse, provider matching, or snapshot payload shape, review worker and persistence paths together.
- `solve_all_unit` treats Calculation Bundle partitions plus manifest as the complete result. Its query artifact is a bounded deterministic chunk index, while the retained HDF5 result row is only a compatibility descriptor; neither path may reconstruct a process-by-impact `h_matrix`.
- The factorization cache accounts retained CSC capacities plus UMFPACK's workload-reported symbolic/numeric object sizes, enforces a hard byte capacity with deterministic LRU eviction, and releases invalidated entries immediately. Pre-factorization admission uses deployment-tuned fill-in headroom and must not be described as input-independent constant memory.

## Stable Path Map

| Path group | Role |
| --- | --- |
| `crates/suitesparse-ffi/**` | CSC matrix representation and SuiteSparse bindings |
| `crates/solver-core/**` | matrix build, factorization cache, solve orchestration, provider matching |
| `crates/solver-worker/src/**` | queue workers, package worker, snapshot builder, matrix-readiness verification, result persistence |
| `crates/solver-worker/src/resource.rs` | shared `worker.resource-profile.v1` admission, cancellation, Linux RSS/cgroup telemetry, and owned/temp/object/cache counters |
| `crates/solver-worker/src/storage.rs` | S3-compatible object operations plus byte-capped, hash-verified, cancellable file download/upload primitives |
| `crates/solver-worker/src/snapshot_artifacts.rs` | numerical HDF5 envelope, Review projections, release-metadata/source-closure descriptor chain, and legacy graph/v1 readers |
| `crates/solver-worker/src/artifact_gc.rs` | generic artifact lifecycle candidate validation and object-first retry-safe GC state machine |
| `crates/solver-worker/src/scope_closure.rs` | frozen-release closure traversal, TIDAS validation, canonical v3 issue partitions, compact root-impact/witness evidence, staged artifact publication, scan reuse, and package certificate verification |
| `scripts/scope_closure_qualification.py` and `scripts/run_scope_closure_*_qualification.sh` | fail-closed Linux orchestration for the real external package and isolated non-production provider child-result contracts consumed by the root qualification adapter |
| `docs/agents/contracts/scope-closure-*-result.v1.schema.json` | compatibility snapshots for the root child-result envelopes and the owning-repository provider fragment boundary |
| `crates/solver-worker/src/tidas_cli.rs` | single-binary Rust tidas version/protocol handshake, bounded command execution, report validation, and spool hash/count verification |
| `crates/solver-worker/src/signed_flow.rs` | direction-neutral signed coefficient, reference pivot, boundary policy, and balance-closure primitives |
| `scripts/**` | manual validation, debug, diagnostics, and snapshot helpers |
| `tools/bw25-validator/**` | manual Brightway comparison tooling |
| `supabase/migrations/**` | local runtime-facing SQL expectations referenced by the worker runtime |
| `docs/lca-api-contract.md` | shared jobs/results/payload/status contract for edge and frontend consumers |
| `docs/scope-closure-contract.md` | closure traversal, immutable source, validation, artifact, reuse, and build-binding contract |
| `docs/agents/contracts/scope-closure-memory-and-result-contract.md` | canonical v4 bounded artifact shape with v3 issue semantics, compact root-impact/witness representation, memory/cancellation invariants, and Database #316 staged-publication handshake |
| `docs/matrix-readiness-report-contract.md` | worker-owned matrix-readiness report schema, blocker/finding codes, and next-action contract |
| `docs/review-submit-fast-gate-contract.md` | worker-owned review-submit fast gate schema, blocker codes, and targeted probe contract |
| `docs/edge-function-integration.md` | edge-facing enqueue, polling, and service-role integration contract |
| `docs/frontend-integration.md` | frontend-side solve/result interaction contract |
| `docs/provider-linking.md` | current provider-link runtime decision order, default rule, candidate eligibility, and diagnostics contract |
| `docs/implicit-regional-supply-mix-modeling.md` / `docs/implicit-regional-supply-mix-modeling.en.md` | Chinese and English modeling notes for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider share semantics |
| `docs/tidas-package-contract.md` | package-worker async import/export contract |

## Current Runtime Families

### Solve and queue jobs

The worker currently covers families such as:

- `prepare_factorization`
- `solve_one`
- `solve_batch`
- `solve_all_unit`
- `invalidate_factorization`
- `rebuild_factorization`
- `analyze_contribution_path`
- `build_snapshot`
- `lcia_result_package_build`
- `lcia.scope_closure_check`

These flows belong to the worker runtime, not to the API repo.

The main solver worker uses `SOLVER_QUEUE_BACKEND=worker-jobs`. It claims `private.worker_jobs` rows from `worker_queue=solver`, maps `job_kind=lca.*`, `job_kind=lcia_result.package_build`, and `job_kind=lcia.scope_closure_check` payloads back to internal `JobPayload` variants, heartbeats `phase/progress`, and records lease-fenced terminal results. Ordinary solve jobs link LCA domain rows and use `private.worker_record_job_result`; scope closure uses its V2 result or reuse-finalizer RPC because that same transaction persists issue provenance, evidence, certificate state, and the terminal Worker result. LCIA result package builds use `private.lca_results` plus `private.lca_latest_all_unit_results` as Worker-produced artifacts and then mark `private.lcia_result_packages` preview-ready through the database service-role command. The retired `lca_jobs` lifecycle and matrix-table fallback are not compatibility surfaces: selecting `SOLVER_QUEUE_BACKEND=pgmq` or encountering a snapshot without a ready artifact fails closed. The independent `pgmq` extension remains available to unrelated consumers.

### Scope closure and certificate-bound build

`crates/solver-worker/src/scope_closure.rs` owns deterministic union traversal and report production for `lcia.scope_closure_check`. It reads only exact identities from `lcia.scope-closure-data-snapshot.v2`, which is populated from the current public release manifest. Every fetched document is rehashed; an allowlisted missing row, a hash-drifted row, or a live-only row makes the scan incomplete. Bounded breadth-first traversal remains cycle-safe and non-fail-fast, while accepted transitive process providers become part of the effective scope. Successful snapshot discovery crosses the child-process boundary through a parent-owned, size/SHA-256-verified temporary JSON file containing the process axis and compact readiness projection; captured stdout carries only its bounded terminal descriptor.

Qualification preserves the same ownership boundary. Worker owns real payload
collection, TIDAS/spool verification, four-mode capacity replay, resource
sampling, and child-envelope validation. Database, Storage, Edge, and Next own
their provider-facing assertions and expose only aggregate, locator-free
fragments through git-tracked exact-checkout adapters. The Worker aggregator
cannot turn a missing owner fragment into passed evidence and cannot infer a
foreign business assertion from its own mocks.

The same module invokes TIDAS `document-validation-batch.v1` through `tidas_cli.rs`. The adapter accepts one `TIDAS_BIN`, requires the exact expected Rust release version, verifies `validate --describe`, hashes issue events from their original NDJSON lines before parsing, and rejects any SHA-256/byte/count or asset-fingerprint drift. Validation evidence lookup is capped at 256 keys, uncached execution at 64 documents, and cache writes at 8 MiB. Traversal writes canonical document payloads and full reference evidence to temporary random-access/sorted spools, retains only document metadata plus a numeric-ID graph, and reloads payloads only for uncached validation windows. Canonical v3 issue coalescing uses bounded issue/source/occurrence external sort, one source reachability state, one current-issue root union, issue-level compact root ordinals, and one frozen reverse graph; it never emits issue×root×full-witness partitions. Fresh v4 publication preserves those semantics, compresses ordinary administrative scan/resolution records into deterministic 25,000-record/32 MiB partitions, and replaces the monolithic closure bundle with a small relation-hash binding manifest. A single record above that window is represented by one canonical index and contiguous fixed 8 MiB canonical-byte chunks; the layout stream interleaves that logical record with ordinary partitions, so reconstruction hashes the original record bytes plus newline without redefining relation identity. The exact TIDAS event stream is compressed once, coalesced issue records are globally ordered once into final zstd partitions, and historical readers remain version-dispatched. Every object is file-backed and admitted below a 256 MiB ceiling before Database #316 write-set creation; bounded XLSX, temporary-space reserve, automatic cleanup, RSS/cgroup telemetry, and lease-driven cancellation remain in force. Database #316 pre-registers descriptors in bounded batches and atomically seals an exact artifact map before Worker uploads any object; finalization is the only transition to `ready`. The snapshot builder still runs first for non-persisting signed-flow discovery and persists a bound HDF5 numerical snapshot only after the discovered Process axis is rescanned and blocker-free. It takes the exact LCIA method axis from the frozen Scope Closure request and uses one active C-factor selection for both matrix construction and numerical source closure. Blocked or incomplete checks produce no numerical snapshot or certificate. Reuse preserves immutable evidence but creates a current-run XLSX, summary, report binding, and certificate. Before `lcia_result.package_build`, the queue validates the full certificate/scope/HDF5/index/build-contract/bundle/report binding against the database and reads bundle binding fields from a bounded local file. See `docs/scope-closure-contract.md` and the narrow memory/result contract for exact behavior.

Versioned `public_plus_owner_draft` snapshot builds keep actor visibility limited to process/flow rows and load LCIA methods from the reviewed, release-pinned static cache through `crates/solver-worker/src/static_lcia_cache.rs`. That module owns trusted-base retrieval, byte/decompression limits, raw and canonical hash verification, method/locator alias validation, and streaming factor normalization. `calculation_evidence.rs` owns the v2 source/bundle/25-method coverage binding. Gap evidence is deterministically spooled as JSONL rather than retained as an exchange-by-method object graph. Build-snapshot terminal projection comes from canonical `private.worker_jobs` diagnostics, including reuse-resolved snapshot ID and evidence. Singular/factorization diagnostics use only the exact process/version pairs in the snapshot index.

### Snapshot builder and provider matching

The snapshot builder path owns sparse payload generation, provider matching, and snapshot artifact metadata.
`CompiledGraph` is compiler IR, not a durable snapshot schema. A fresh ordinary snapshot persists only numerical payload/config/coverage in `snapshot-hdf5:v1`. Its descriptor binds `snapshot-release-evidence-json-zstd:v2`, which contains Calculation Bundle metadata and a second descriptor for the content-addressed `snapshot-source-closure-json-zstd:v1`; neither metadata sidecar contains source documents twice. Both encoders borrow compiler data and stream to zstd-backed temporary files before bounded multipart upload. Ordinary solve loads only the numerical artifact; Calculation Bundle materialization explicitly hydrates and verifies the two-level descriptor chain. Historical HDF5 numerical payloads remain readable even when an embedded compiler schema has drifted: graph decoding is isolated, incompatible metadata is ignored for ordinary solve, and a purpose-specific consumer reports rebuild guidance. Compatible embedded graphs and v1 full-evidence sidecars remain readable. Review-submit baseline/overlay now persist consumer-owned `SnapshotReviewBaseline` / `SnapshotReviewGateEvidence` projections instead of compiler IR.
The current provider-link runtime contract lives in `docs/provider-linking.md`. The modeling basis for implicit regional supply mix, exchange-location supply-region anchors, and annual-volume provider shares lives in `docs/implicit-regional-supply-mix-modeling.md` and `docs/implicit-regional-supply-mix-modeling.en.md`.

The process-column contract is one complete TIDAS Process revision per snapshot matrix column. `quantitativeReference.referenceToReferenceFlow` selects that column's signed normalization pivot; it does not require Product, Output, or a positive amount. Non-reference exchanges do not create derived matrix columns. When another exchange needs an independent activity pivot, upstream must publish another complete Process revision.

`crates/solver-worker/src/signed_flow.rs` owns the direction-neutral math: `coefficient = direction_sign * amount`, signed unit reference pivots, opposite-sign weighted balance, non-negative activity requirements, closure checks, and explicit `closed/open/cutoff` boundary identifiers. `snapshot_builder` maps Product/Waste to technosphere, Elementary to biosphere, and Other to reporting; full snapshots, request-root closure, and review-submit overlays share the same technosphere balance compiler. Candidate eligibility is exact same-flow, different-process, quantitative-reference, and opposite-sign—not Product/Waste or Input/Output semantics.

`crates/solver-worker/src/tidas_process_semantics.rs` owns target-aware allocation. Targets may identify any known exchange internal ID; the reference pivot is never multiplied by allocation, while non-reference residuals are. A scalar `{}` remains the bounded undeclared fallback, and one full targetless entry is inferred only when the reference exchange/ID is unique. Multiple quantitative references are explicitly unsupported. Snapshot compilation keys version-sensitive metadata, reference ports, flow axes, and diagnostics by `(Flow UUID, resolved version)`, while pruning unreferenced historical revisions before compact indexing. Selected LCIA Method factor references are resolved separately in bounded batches: factor-only Elementary Flow revisions enter frozen source closure as support and recursively close their supporting documents, but never enter the inventory-derived matrix/provider Flow universe. Build identity records `tidas-reference-allocation-v3`, `signed-flow-balance-v1`, boundary policy, `exact-flow-version-reference-unit-v2`, and `selected-lcia-factor-flow-support-v1`; coverage is `snapshot_coverage.v3`.

`crates/solver-worker/src/readiness.rs` owns the worker-side verification gate for automated data production. It turns coverage, sparse payloads, and optional reference-port/balance evidence into `matrix_readiness_report.v2`. `closed` boundaries block unresolved technosphere coefficients; explicit `open/cutoff` boundaries retain them as auditable warnings. Callers must not reimplement balance/routing, singular-risk, LCIA, or factorization checks outside the worker.

`crates/solver-worker/src/review_submit_gate.rs` owns the worker-side fast gate for dataset revision review submission. It layers revision freshness, process/exchange scans, provider evidence, sparse structural checks, and targeted RHS probes into a binary `passed` / `blocked` report without full matrix inversion or full `solve_all_unit`.

`crates/solver-worker/src/review_submit_gate_runner.rs`, `crates/solver-worker/src/worker_jobs.rs`, and `crates/solver-worker/src/bin/review_submit_gate_runner.rs` are the DB runtime bridge for that gate. The legacy mode claims persisted `dataset_review_submit_gate_runs`; the `--worker-jobs` mode claims child `review_submit.gate` jobs from `private.worker_jobs`. Both modes build a no-LCIA review-submit baseline plus draft overlay snapshot for the submitted process revision, compute the `json_ordered` checksum, execute `review_submit_gate`, and record the result through the database RPC. The root `review_submit.submit` job is created and advanced by the DB/Edge coordinator contract; worker only executes the numeric gate child job.

### Maintenance worker

`crates/solver-worker/src/bin/maintenance_enqueue.rs` is the operator/timer entrypoint that enqueues worker maintenance jobs through `private.worker_enqueue_job`. `crates/solver-worker/src/bin/maintenance_worker.rs` is the `worker_jobs` consumer for maintenance work that should be observable through the shared job lifecycle. It claims `worker_queue=maintenance` and dispatches these job kinds:

- `lca.snapshot_gc`
- `lca.result_gc`
- `worker.artifact_gc`
- `tidas.package_artifact_gc`
- `national_carbon.process_flow_graph_cache_build`

The maintenance worker is intentionally a thin orchestrator over the existing `snapshot_gc`, `result_gc`, `package_gc`, and `process_flow_graph_cache_builder` binaries. Those binaries keep their own safety rules, object-first behavior, active snapshot/package protections, cache-prefix contracts, and PostgreSQL advisory locks where applicable. The process-flow graph builder emits the national-carbon global non-elementary process/flow graph, binary adjacency/edge payloads, worker-computed layouts, geo-map views, and browser lookup indexes; its `expanded2d` layout is grouped by level-3 classification before being fitted to a relation-first topology and uniform overview silhouette so the frontend does not derive layout coordinates at runtime. Cache v2 nodes and metadata expose separate level-1 and level-3 cluster ids/labels, while geo-map views include worker-derived process links, scoped graph indexes, and world/china projected layouts. The `worker_jobs` layer records dry-run/execute intent, phase/heartbeat, exit status, stdout/stderr tails, parsed `[summary]` metrics, and an operator-only `maintenance_gc_report` artifact metadata row for operator visibility. Child stdout and stderr are drained concurrently into independent fixed 1 MiB tail buffers; diagnostics record total observed bytes and truncation flags, so a verbose maintenance binary cannot make orchestrator memory grow with total log volume.

Generic `artifact_gc` consumes only bounded Database Engine #309 scope-closure candidates under the current maintenance-job lease and database claim token. It validates and deletes the configured bucket-relative object only for `object_delete` claims, treats object absence as idempotent success, and repeats bounded detail cleanup while the token remains valid. After process loss following a partial tombstone, Database reclaims the pending row under a fresh token as `detail_cleanup` with no locator and `objectDeleteRequired=false`, so Worker completes details without deleting again. Object-delete failures are reported for retry without premature tombstoning.

### Shared resource and object-I/O primitives

`crates/solver-worker/src/resource.rs` defines the reusable `worker.resource-profile.v1` contract. Heavy job families can admit owned-memory estimates, temporary bytes, object download/upload bytes, cache bytes, stage-window bytes, and concurrency before work begins. Rejections expose stable `resource_admission_rejected`, `artifact_limit_exceeded`, and `operation_cancelled` classes. Phase measurements separate owned estimates from process RSS and Linux cgroup v2 `anon`, `file`, `memory.current`, and `memory.peak`.

`crates/solver-worker/src/storage.rs` retains the existing byte-returning and file-upload methods for compatibility. New or migrated heavy paths must prefer `download_object_url_to_file` and `upload_object_key_file_bounded`, pass an explicit task-specific byte cap, and optionally pass an expected SHA-256 and cancellation token. Downloads stream into an adjacent temporary file and publish the destination only after limit/hash/cancellation checks pass. Multipart uploads hash and admit the source before network work, use a fixed part buffer, and abort the multipart upload on a detected cancellation or failure. Calculation Bundle release evidence now uses this file path; the smaller numerical HDF5, package, graph-cache, and solve paths retain their existing transport until separately migrated.

### Package worker

The package worker handles:

- `export_package`
- `import_package`

It also owns package-job artifacts and diagnostics. Import validation uses the same `tidas_cli.rs` adapter and a streaming hash/count-verified issue spool before conflict checks or inserts; the report retains a deterministic bounded issue sample plus complete counts, never the entire large spool in memory. There is no Python validator or command fallback. `PACKAGE_QUEUE_BACKEND=worker-jobs` claims `private.worker_jobs` rows from `worker_queue=package`, maps `job_kind=tidas.export_package|tidas.import_package` into the same `PackageJobPayload` variants, heartbeats package progress, records terminal `worker_jobs` results, and links package artifacts / export items / request-cache rows back to the canonical `worker_jobs` id. The retired `lca_package_jobs` lifecycle is not optional compatibility: selecting `PACKAGE_QUEUE_BACKEND=pgmq` fails closed before consuming a message.

### Result persistence

Result artifacts are persisted through the worker and supporting runtime storage flows instead of inlining heavy compute payloads into the API layer.

`crates/solver-worker/src/calculation_bundle.rs` owns canonical `tiangong.calculation-bundle.v2` generation. Its technosphere edge schema uses dependent/residual/balancing/reference/routing/activity fields rather than assuming consumer Input and provider Output. Solver evidence records allocation and link semantics versions, boundary policy, and exact flow identity policy. The frozen source-document closure and directional LCI release guarantees remain unchanged; older snapshots without exact signed-flow release evidence must be rebuilt.

Calculation Bundle reads exact release evidence from the current integrity-bound v2 metadata + v1 source-closure chain, the transitional v1 full-evidence sidecar, or a schema-compatible legacy embedded `compiled_graph.release_evidence`. Incompatible embedded evidence fails with explicit rebuild guidance without making the numerical payload unreadable. Compatibility is read-only: new snapshots never serialize the complete graph. Publication uploads source closure, release metadata, numerical HDF5 and index before the database ready record becomes visible; failed pre-publication uploads remain unreferenced objects under the same snapshot directory and are removed by directory-level snapshot/orphan GC.

## Operational Baseline

- Solve result persistence is S3-only; treat `lca_results` as artifact metadata plus diagnostics, not as an inline result store.
- The worker uses a main DB pool plus an optional queue-only DB pool. The main pool is configured through `DATABASE_URL` / `CONN`, `DB_MAX_CONNECTIONS`, `DB_MIN_CONNECTIONS`, and `DB_ACQUIRE_TIMEOUT_SECONDS`; it must remain on a session/direct connection or session pooler when compute paths use SQLx bound queries. Supabase's known `:6543` transaction endpoint is rejected for the main pool at startup. The queue-only pool is configured through `QUEUE_DATABASE_URL` / `QUEUE_CONN`, `QUEUE_DB_MAX_CONNECTIONS`, `QUEUE_DB_MIN_CONNECTIONS`, and `QUEUE_DB_ACQUIRE_TIMEOUT_SECONDS`; if no queue URL is set it reuses the main pool.
- `WORKER_ID`, `WORKER_JOBS_CLAIM_LIMIT`, and `WORKER_JOBS_LEASE_SECONDS` control solver `worker_jobs` claim diagnostics, batch size, and lease renewal. Keep the lease longer than a normal solve/snapshot heartbeat interval and use `BUILD_SNAPSHOT_MAX_CONCURRENCY` for actual snapshot build throttling.
- `build_snapshot` is globally throttled with a PostgreSQL transaction-level advisory lock (`BUILD_SNAPSHOT_MAX_CONCURRENCY`, default `1`) across worker instances; keep `WORKER_VT_SECONDS` larger than the worst-case lock wait plus build time.
- Runtime SQLx queries use non-persistent prepared statements so the worker does not reuse named prepared statements across PostgreSQL session reuse boundaries. High-frequency pgmq polling and archive operations use the queue-only pool plus `raw_sql` with validated queue-name literals so they can run through the simple query protocol on Supabase's 6543 transaction pooler without moving compute/package/snapshot queries onto that pooler.
- Snapshot-builder local reports under `reports/snapshot-coverage` are guarded optional diagnostics, not durable artifacts. `SNAPSHOT_REPORT_MODE`, `SNAPSHOT_REPORT_RETENTION_DAYS`, `SNAPSHOT_REPORT_MAX_FILES`, and `SNAPSHOT_REPORT_MIN_FREE_BYTES` control local report writes, retention, and low-disk skipping; object-store snapshot artifacts remain the durable compute payload.
- Queue enqueue and protected writes stay on service-side runtime paths guarded by existing RLS and `service_role` boundaries.
- Worker and snapshot paths require DB connectivity plus the required S3 env set before runtime validation is meaningful.
- Worker-owned DB pools set explicit PostgreSQL `application_name` values for observability. `snapshot_builder` also applies `SNAPSHOT_DB_STATEMENT_TIMEOUT_SECONDS` as a bounded statement timeout; `0` is reserved for targeted manual recovery, not normal production operation.
- TIDAS validation semantics belong to the published Rust tool. Worker retains lease/heartbeat/cancellation, timeout, deterministic evidence, and terminal error projection; it never reimplements those operations in deployment or API repositories.

## Runtime SQL Boundary

This repo still documents and depends on runtime SQL expectations, but durable schema governance belongs in `database-engine`.

Use this rule:

- runtime compute truth here
- durable schema, migration, RPC, and policy truth there

## Cross-Repo Boundaries

- `edge-functions` owns request normalization, auth, enqueue, and polling API behavior
- `database-engine` owns durable schema governance
- `lca-workspace` owns root delivery completion after a child PR merges

## Common Misreads

- API behavior does not belong in the solver repo
- local migrations here are not the workspace-wide schema source of truth
- a merged child PR does not finish workspace delivery

## Local Docpact Push Gate

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh` and then runs `make check`. The gate resolves the CLI through `scripts/docpact`, so local agent shells do not need bare `docpact` on `PATH`. The hook is the local guard for docpact config validation, enforced doc-governance linting, and worker runtime tests; the GitHub `ci` workflow is manual-dispatch only.
