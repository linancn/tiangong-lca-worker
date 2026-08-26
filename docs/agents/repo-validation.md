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
  - docs/review-quality-diagnostic-contract.md
  - docs/ai-worker-contract.md
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
lastReviewedAt: 2026-08-26
lastReviewedCommit: cb7467aabae4072d5e2c22d10503ff9921c4971f
lastReviewedNote: "Validation now covers strict V3 package-ready receipts and a single fetch-only ambiguous-response retry in addition to the existing Portal LCIA proof."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ../../docs/lca-api-contract.md
  - ../../docs/scope-closure-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-quality-diagnostic-contract.md
  - ../../docs/ai-worker-contract.md
  - ../../docs/tidas-package-contract.md
  - ./contracts/scope-closure-memory-and-result-contract.md
  - ./contracts/portal-lcia-projection-contract.md
---

## Default Baseline

Unless the change is doc-only repo-maintenance work, the default baseline is:

```bash
make check
cargo clippy -p solver-worker --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Treat the last two commands as non-negotiable hard gates after code changes.

The local `pre-push` hook runs the docpact gate first and then runs `make check`. The GitHub `ci` workflow is manual-dispatch only, so ordinary branch pushes do not spend Actions minutes on standalone tests.

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `crates/**` solver or worker code | `make check`; hard Clippy gate; hard format gate | run the narrow manual script that matches the touched area, such as snapshot build, full compute debug, or BW25 validation | Record which job family or worker path was exercised. |
| shared resource admission or object file I/O | `cargo test -p solver-worker --lib resource::tests`; `cargo test -p solver-worker --lib storage::tests`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates | exercise a local no-`Content-Length` response, cancellation during streaming, hash mismatch, preflight oversize rejection, and multipart abort behavior when the object-store test environment is available | Assert stable resource error codes, fixed buffers, and absence of destination partial files. Keep legacy byte-returning APIs compatible, but use explicit-cap file APIs for newly migrated heavy paths. |
| snapshot artifact boundaries / purpose projections | `cargo test -p solver-worker snapshot_artifacts`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo test -p solver-worker snapshot_retention`; `cargo test -p solver-worker storage`; `cargo test -p solver-worker db`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates | in isolated DB/S3, build a production-equivalent full-library snapshot and record numerical HDF5, release metadata, and source closure sizes separately; prove `sparse.h5` has no `compiled_graph` or source documents, Review baseline/overlay contain only their declared projections, ordinary solve requests no business sidecar, files above 8 MiB use multipart, Calculation Bundle output is unchanged, every SHA/size/format/count mismatch fails closed, and snapshot GC removes all sibling objects plus pre-publication orphans | Keep legacy graph-bearing HDF5 numerical payloads readable even when compiler metadata has drifted; keep schema-compatible graph evidence and v1 full-evidence sidecars bundle-readable, while incompatible evidence fails with rebuild guidance. Deploy readers on all Worker nodes before enabling v2 writers, or drain/restart the fleet as one coordinated rollout; older binaries cannot consume newly written v2 artifacts without duplicating the removed data. Do not cite a synthetic size check as production-equivalent capacity proof. |
| Calculation Bundle / all-unit directional LCI | `cargo test -p solver-worker calculation_bundle`; `cargo test -p solver-worker artifacts`; `cargo test -p solver-core cache`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | with safe DB/S3 env, rebuild one snapshot, run `solve_all_unit`, verify manifest-last upload, all compressed/uncompressed hashes, exact 256-process boundaries, query-v2 chunk ranges/hashes without `h_matrix`, reviewed method identities, recursively complete TIDAS source closure, directional LCI parity, and retry byte determinism. Prove a one-method Scope Closure request loads only that frozen method. Include active and off-axis LCIA factors: only factors intersecting the inventory-derived biosphere Flow/direction axis may enter C, source support traversal, or blockers; active Elementary support must include transitive Flow Property/Unit Group/Source/Contact documents while compiled Flow count and provider decisions remain inventory-derived, and active non-Elementary targets fail closed. Exercise multiple admitted snapshots and prove resident cache bytes never exceed capacity, LRU eviction is deterministic, invalidation releases bytes, oversized workloads reject before factorization, and actual UMFPACK peak/retained estimates are reported. | Current snapshots must provide exact source datasets through the integrity-bound v2 release-metadata → v1 source-closure chain; legacy v1 full-evidence sidecars and embedded `compiled_graph.release_evidence.source_datasets` remain readable. Snapshots without an exact evidence form, or with the legacy exchange-only source-closure policy, must fail closed and be rebuilt. Never infer exchange IDs, versions, units, directions, provider output IDs, or source documents from matrix indices or mutable solve-time database state. Sparse factorization fill-in is workload-dependent; tune admission from observed workloads and do not claim an input-independent constant bound. |
| Portal LCIA projection / package build V3 | `cargo test -p solver-worker --lib portal_lcia_projection`; `cargo test -p solver-worker --lib portal_package_ready`; `cargo test -p solver-worker --lib calculation_bundle`; `cargo test -p solver-worker --lib queue`; `cargo check -p solver-worker --all-targets --all-features`; hard Clippy/format gates; validate all three JSON Schemas | against an isolated matching Database migration, execute authoritative V3 input → begin → batch → status → seal → V3 package ready; verify the fixed UTF-8/null framing vector, exact Worker/Database record/relation/content hashes, 500-record and 1-MiB boundaries, response-loss replay, lease loss, package `portalProjectionId`/`portalProjectionContentHash`, and zero visible partial projection. Inject first-fetch response loss and require one exact reused package-ready receipt; prove two fetch failures stop after the retry and a Database non-ok result is not retried. After commit, simulate process death plus a newly claimed job lease and require pre-build readback to return the same receipt without calculation, upload, staging, seal, or mutation. Then run Release package-publish/finalize/readback and anonymous public readback without any artifact locator | V1/V2 payloads must reject projection markers and retain byte-compatible calculation/package behavior. Unit/mock proof is not the cross-repository E2E gate. Keep `docs/agents/contracts/portal-lcia-projection-contract.md`, Database RPC tests, and Release receipts aligned. |
| solver `worker_jobs` queue backend | `cargo test -p solver-worker worker_jobs`; `cargo test -p solver-worker maps_worker_jobs`; `cargo check -p solver-worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=solver` job and run `solver-worker --queue-backend worker-jobs --mode worker` to verify claim/heartbeat/result projection; run against a schema where retired `public.lca_jobs` is absent; verify explicit `pgmq` selection and missing snapshot artifacts fail closed | Keep `docs/lca-api-contract.md` and `docs/edge-function-integration.md` aligned with job kind, payload schema, Worker result projection, and retired lifecycle behavior. |
| certificate-grade scope closure / package binding | `cargo test -p solver-worker scope_closure`; `cargo test -p solver-worker snapshot_builder_protocol`; `cargo test -p solver-worker maps_scope_closure_payload_from_database_envelope`; `cargo test -p solver-worker package_closure_binding_is_all_or_none_and_result_ref_preserves_check_id`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | Run the release qualification harness against the external open-data package twice and require byte-identical non-summary artifacts, manifest, ordering, and logical hashes. Exercise successful discovery with at least 5,000 process-axis entries and require the terminal to remain below its capture ceiling while the verified temporary JSON preserves the complete axis and compact readiness fields; inject missing, truncated, malformed, size-mismatched, and SHA-256-mismatched result files and require protocol failure plus cleanup. Native TIDAS must complete within 60 seconds and 512 MiB peak RSS; the production-shaped complete closure must finish within 10 minutes and 4 GiB process-tree peak RSS. Run the same production issue/root distribution at 1×, 2×, 5×, and 10×, recording wall time, process-tree RSS, TIDAS RSS, Linux cgroup anon/file/current/peak when available, temporary bytes, partition/artifact bytes and counts, descriptor count, maximum object bytes, and cache reclaim. Prove the unified TIDAS/graph/frozen-source/provider/matrix/factorization/LCIA-readiness issue set, blocker/verdict/certificate inputs, exact counts, root membership, and on-demand witnesses are semantically equivalent while `expandedAffectedRootRecordCount=0`. Reproduce an old-layout logical bundle above the storage object limit with deterministic generated/replayed evidence, then prove the v4 binding manifest stays small, ordinary administrative evidence partitions close at 25,000 records or 32 MiB uncompressed, and every physical object is at most 256 MiB. Exercise logical-record sizes at 32 MiB−1, exactly 32 MiB, 32 MiB+1, 36,105,476 bytes, 64 MiB, and above 256 MiB with generated high-entropy and Unicode/newline payloads. Require the oversized-record index and fixed chunks to preserve canonical record-plus-newline relation hashes across two runs, and inject missing, duplicate, reordered, and corrupted chunks. Exercise cancellation or crash/retry at coalesce, administrative/issue partition write, pre-write-set object admission, bounded batch registration, atomic seal, upload, and finalize, with no visible partial write set, orphan, or local temporary leak. Run the exact Database #316 fixture/digest/ordinal contract against matching canonical migrations in isolated local DB/storage. | Keep the package and generated output outside git. Use local fixtures and isolated non-production DB/storage only; never deploy, restart a server, enqueue production work, mutate production state, or update the root pointer for validation. A mock-only traversal is insufficient. Keep `docs/agents/contracts/scope-closure-memory-and-result-contract.md`, `docs/scope-closure-contract.md`, `docs/lca-api-contract.md`, and `docs/tidas-package-contract.md` aligned with Database #316 and the public TIDAS protocol. |
| versioned public-plus-owner-draft snapshot / LCIA evidence | `cargo test -p solver-worker calculation_evidence`; `cargo test -p solver-worker static_lcia_cache`; `cargo test -p solver-worker maps_exact_public_owner_draft_build_v2`; `cargo test -p solver-worker rejects_summary_only_lcia_manifest_before_build_execution`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; baseline hard gates | run ignored `verifies_reviewed_release_bundle_bytes` with `LCIA_STATIC_CACHE_RELEASE_DIR=<next-public-root>` whenever the reviewed static bundle changes; in a non-production environment with DB/S3 available, enqueue one v2 build and verify public `100`, actor-owned `0` with null and non-null team/review metadata, foreign/nonzero rejection, legacy v1 guarded-scope readability, snapshot-index source/identity hashes, per-method JSONL gap count, worker-only build result projection, and solve binding drift rejection | Never use a production data mutation as validation. Keep the complete reviewed manifest plus Edge/Next/Worker source, scope, matrix, and release hashes byte-for-byte aligned; reject summary-only manifests during queue payload validation, preserve old manifest semantics by hash, and reject v1 source/evidence/solve downgrade. |
| snapshot-builder signed-flow linking or routing | `cargo test -p solver-worker --lib signed_flow`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates; `./scripts/build_snapshot_from_ilcd.sh` when safe | exercise Product and Waste reference Input/Output with positive/negative amounts, opposite/same-sign candidates, multi-reference rejection, same-Process opposite-sign candidate inclusion, retained `A[i,i]` below/at/above the diagnostic cutoff, request-root flow-space closure, multi-candidate weights, and closed/open/cutoff evidence. For Flow identity changes, include one UUID with two referenced exact revisions plus one unreferenced historical revision; prove exact provider isolation, two compact flow-axis rows, omitted-version freezing, and pruning of the unreferenced revision | Keep `docs/provider-linking.md` and both implicit regional supply mix docs aligned. Assert non-negative activity requirements and signed closure, not direction/type-based provider roles. Explicit Flow versions must never be silently replaced by the latest revision. |
| matrix-readiness / signed-balance closure gate | `cargo test -p solver-worker readiness`; `cargo check -p solver-worker --bin matrix_readiness`; hard Clippy gate for the touched binary/module | run `snapshot_builder` or `matrix_readiness --input <fixture> --out <report>` against the closest available target artifact; verify `balance_evidence`, `unresolved_balances`, and explicit boundary-policy behavior | Keep `docs/matrix-readiness-report-contract.md` aligned with schema, blocker/finding code, policy, and next_action changes. Use `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` on Homebrew setups. |
| Review Admin quality diagnostic / pending-review matrix | `cargo test -p solver-worker worker_jobs`; `cargo test -p solver-worker review_quality_diagnostic_runner`; `cargo check -p solver-worker --bin review_quality_diagnostic_runner`; `cargo test -p solver-worker snapshot_builder_protocol`; hard Clippy/format gates | in isolated non-production DB/S3, enqueue one safe `review.quality_diagnostic` job with multiple pending Process targets and prove they enter one request-root snapshot. Exercise complete, source-incomplete, factorization-failed, no-pending-Process, timeout, signal, terminal-protocol and lease-loss cases. | Require `completed + clear/findings/not_evaluable` for every data-quality conclusion, empty `blocker_codes`, null `resolution_scope`, `workflowBlocking=false`, and unchanged Review states. Launch/timeout/signal/protocol faults use `failed`. Never use production enqueue/write for validation. Keep `docs/review-quality-diagnostic-contract.md` aligned. |
| generic AI worker or handler | `cargo test -p solver-worker ai::`; `cargo check -p solver-worker --bin ai_worker`; `cargo clippy -p solver-worker --bin ai_worker --tests -- -D warnings`; hard format gate; run both strict-authoring `tidas ruleset` commands against the governed binary | with an isolated non-production DB and mock/provider test endpoint, enqueue one Process and one Flow `ai.tidas_suggestion.request.v1`; prove claim/heartbeat/result, bounded concurrency, complete/partial/failed outcomes, original-data preservation on all-path failure, unknown schema rejection, provider timeout/429/5xx mapping, and no domain-row mutation | Keep `docs/ai-worker-contract.md`, `docs/lca-api-contract.md`, `.env.example`, ruleset/catalog binding, and model config version aligned. Never put provider keys, raw error bodies, or dataset content in validation logs. |
| maintenance worker_jobs / GC orchestration | `cargo check -p solver-worker --bin maintenance_worker`; `cargo check -p solver-worker --bin maintenance_enqueue`; run touched binaries such as `cargo check -p solver-worker --bin snapshot_gc --bin result_gc --bin artifact_gc --bin package_gc --bin process_flow_graph_cache_builder`; `cargo test -p solver-worker --bin maintenance_worker`; `cargo test -p solver-worker --bin maintenance_enqueue`; run the touched GC/filter/cache tests such as `cargo test -p solver-worker artifact_gc`, `cargo test -p solver-worker snapshot_gc`, `cargo test -p solver-worker result_gc`, `cargo test -p solver-worker package_gc`, or `cargo test -p solver-worker --bin process_flow_graph_cache_builder`; hard Clippy gate for all targets | run a safe dry-run `lca.snapshot_gc`, `lca.result_gc`, `worker.artifact_gc`, `tidas.package_artifact_gc`, or `national_carbon.process_flow_graph_cache_build` worker job in dev when DB and storage env are available; generic artifact GC must prove object-delete-before-complete, missing-object idempotency, bucket/path rejection, bounded claim batches, and retry without tombstoning | Keep `docs/agents/repo-architecture.md`, `README.md`, deployment units, and the package/LCA retention docs aligned with job kind, payload, summary, destructive-execute safety semantics, and fixed stdout/stderr capture limits. |
| package worker import or export flows | baseline gates; real release-binary handshake against the active governed `0.2.0` default; active-source audit proving no Python validator or validator-command fallback remains | validate the largest available package locally with the release `tidas` binary before any server execution; verify `tidas.operation-report.v1`, exact version, `asset_fingerprint`, issue-spool SHA-256/bytes/event count, bounded memory/queue settings, and stable Worker error codes; run the closest safe package-flow helper when isolated DB/S3 is available | The large package fixture remains outside git. Package-job semantics are runtime-sensitive and may depend on storage or DB state; never make a production mutation for validation. |
| package `worker_jobs` queue backend | `cargo test -p solver-worker --bin package_worker`; `cargo test -p solver-worker package_worker`; `cargo check -p solver-worker --bin package_worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=package` job and run `package_worker --package-queue-backend worker-jobs` to verify claim/heartbeat/result projection; verify explicit `pgmq` selection fails closed before consumption | Keep `docs/tidas-package-contract.md` aligned with job kind, payload schema, continuation behavior, artifact projection, Worker result projection, and retired lifecycle behavior. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| repo docs, `.env.example`, or docpact config only | `scripts/docpact validate-config --root . --strict`; `scripts/docpact lint --root . --worktree --mode enforce` | perform route checks for affected intent surfaces such as `solver-runtime`, `package-worker`, or `runtime-sql-boundary` | Refresh review metadata even when prose-only docs change. Keep `.env.example` secret-free. |

Certificate-grade Scope Closure validation must additionally prove that the frozen boundary is exactly `cutoff`, `closed/open` inputs fail with `scope_closure_boundary_policy_must_be_cutoff`, and unresolved provider balances remain warnings with complete evidence. Generic snapshot-builder and matrix-readiness diagnostics keep their explicit three-policy compatibility tests.

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
`0.2.0`; it emits `cold`, `warm`, `mixed`, and `stale` real-payload capacity
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
