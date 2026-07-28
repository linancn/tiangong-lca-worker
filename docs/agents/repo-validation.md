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
  - .github/workflows/**
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-07-28
lastReviewedCommit: 98ca40c
lastReviewedNote: "Issue #160 subprocess tests passed five consecutive default-parallel focused runs after deterministic child-start ordering and mutex-poison recovery."
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-architecture.md
  - ../../docs/lca-api-contract.md
  - ../../docs/scope-closure-contract.md
  - ../../docs/matrix-readiness-report-contract.md
  - ../../docs/review-submit-fast-gate-contract.md
  - ../../docs/tidas-package-contract.md
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
| Calculation Bundle / all-unit directional LCI | `cargo test -p solver-worker calculation_bundle`; `cargo test -p solver-worker artifacts`; `cargo test -p solver-core cache`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | with safe DB/S3 env, rebuild one snapshot, run `solve_all_unit`, verify manifest-last upload, all compressed/uncompressed hashes, exact 256-process boundaries, query-v2 chunk ranges/hashes without `h_matrix`, reviewed 25-method identities, recursively complete TIDAS source closure, directional LCI parity, and retry byte determinism. Exercise multiple admitted snapshots and prove resident cache bytes never exceed capacity, LRU eviction is deterministic, invalidation releases bytes, oversized workloads reject before factorization, and actual UMFPACK peak/retained estimates are reported. Include an LCIA-factor-only Elementary Flow and prove it appears as `support` with transitive Flow Property/Unit Group/Source/Contact documents while compiled Flow count, B/C axes, and provider decisions remain inventory-derived; also prove a non-Elementary factor target fails closed. | Old snapshots without `compiled_graph.release_evidence.source_datasets`, or with the legacy exchange-only source-closure policy, must fail closed and be rebuilt. Never infer exchange IDs, versions, units, directions, provider output IDs, or source documents from matrix indices or mutable solve-time database state. Sparse factorization fill-in is workload-dependent; tune admission from observed workloads and do not claim an input-independent constant bound. |
| solver `worker_jobs` queue backend | `cargo test -p solver-worker worker_jobs`; `cargo test -p solver-worker maps_worker_jobs`; `cargo check -p solver-worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=solver` job and run `solver-worker --queue-backend worker-jobs --mode worker` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_jobs` is absent or ignored | Keep `docs/lca-api-contract.md` and `docs/edge-function-integration.md` aligned with job kind, payload schema, worker_jobs result_ref, and optional legacy `lca_jobs` compatibility expectations. |
| certificate-grade scope closure / package binding | `cargo test -p solver-worker scope_closure`; `cargo test -p solver-worker maps_scope_closure_payload_from_database_envelope`; `cargo test -p solver-worker package_closure_binding_is_all_or_none_and_result_ref_preserves_check_id`; `cargo check -p solver-worker --all-targets`; hard Clippy and format gates | run `cargo test --release -p solver-worker qualified_reference_graph_completes_within_five_minutes -- --ignored --exact` and `cargo test --release -p solver-worker qualified_million_event_spool_stays_within_fixed_runs -- --ignored --exact` under an external process-tree RSS recorder; run `TIDAS_BIN=<release-bin> TIDAS_EXPECTED_VERSION=<version> cargo test -p solver-worker tidas_cli::tests::release_binary_completes_version_and_protocol_handshake -- --ignored`; with isolated non-production DB/S3 and matching database-engine migrations, run one fresh and one concurrently deduplicated closure; verify current-release snapshot hashes, live-drift/live-only rejection, exact-version union/cycle traversal, byte-exact non-empty TIDAS issue hashing, TIDAS cache keys and final event, JSONL/XLSX artifacts, cancellation/lease fencing, target-specific reuse report, certificate binding, and unchanged package numerical artifacts | Before server execution, use the local capacity fixtures and confirm document/reference spools plus compact graph stay within the process-tree RSS budget, 16 MiB sort runs, 8 MiB cache writes, spool caps, temporary-space guard, cleanup, deterministic V1 bytes, and live lease heartbeats. Never use production mutation. Keep `docs/scope-closure-contract.md`, `docs/lca-api-contract.md`, and `docs/tidas-package-contract.md` aligned with the database RPC signatures and TIDAS public protocol. A mock-only traversal is not sufficient integration proof. |
| versioned public-plus-owner-draft snapshot / LCIA evidence | `cargo test -p solver-worker calculation_evidence`; `cargo test -p solver-worker static_lcia_cache`; `cargo test -p solver-worker maps_exact_public_owner_draft_build_v2`; `cargo test -p solver-worker rejects_summary_only_lcia_manifest_before_build_execution`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; baseline hard gates | run ignored `verifies_reviewed_release_bundle_bytes` with `LCIA_STATIC_CACHE_RELEASE_DIR=<next-public-root>` whenever the reviewed static bundle changes; in a non-production environment with DB/S3 available, enqueue one v2 build and verify public `100`, owner `0`, foreign/nonzero/collaboration rejection, snapshot-index source/identity hashes, per-method JSONL gap count, worker-only build result projection, and solve binding drift rejection | Never use a production data mutation as validation. Keep the complete reviewed manifest plus Edge/Next/Worker v2 source, scope, matrix, and release hashes byte-for-byte aligned; reject summary-only manifests during queue payload validation, and reject v1 source/evidence/solve downgrade. |
| snapshot-builder signed-flow linking or routing | `cargo test -p solver-worker --lib signed_flow`; `cargo test -p solver-worker --bin snapshot_builder`; `cargo check -p solver-worker --all-targets`; hard Clippy/format gates; `./scripts/build_snapshot_from_ilcd.sh` when safe | exercise Product and Waste reference Input/Output with positive/negative amounts, opposite/same-sign candidates, multi-reference rejection, self-link exclusion, request-root flow-space closure, multi-candidate weights, and closed/open/cutoff evidence. For Flow identity changes, include one UUID with two referenced exact revisions plus one unreferenced historical revision; prove exact provider isolation, two compact flow-axis rows, omitted-version freezing, and pruning of the unreferenced revision | Keep `docs/provider-linking.md` and both implicit regional supply mix docs aligned. Assert non-negative activity requirements and signed closure, not direction/type-based provider roles. Explicit Flow versions must never be silently replaced by the latest revision. |
| matrix-readiness / signed-balance closure gate | `cargo test -p solver-worker readiness`; `cargo check -p solver-worker --bin matrix_readiness`; hard Clippy gate for the touched binary/module | run `snapshot_builder` or `matrix_readiness --input <fixture> --out <report>` against the closest available target artifact; verify `balance_evidence`, `unresolved_balances`, and explicit boundary-policy behavior | Keep `docs/matrix-readiness-report-contract.md` aligned with schema, blocker/finding code, policy, and next_action changes. Use `PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig` on Homebrew setups. |
| review-submit fast gate / source-reference closure / snapshot subprocess | `cargo test -p solver-worker review_submit_gate`; `cargo test -p solver-worker source_reference_policy`; `cargo test -p solver-worker snapshot_source_closure`; `cargo test -p solver-worker snapshot_builder_protocol`; `cargo test -p solver-worker --test source_reference_characterization`; `cargo check -p solver-worker --bin review_submit_gate`; for DB runner or `worker_jobs` changes also run `cargo test -p solver-worker worker_jobs`, `cargo test -p solver-worker review_submit_gate_runner`, and `cargo check -p solver-worker --bin review_submit_gate_runner`; hard Clippy/format gates | run the two sanitized lineage/model-composition fixtures plus one successful review-submit control; compare Process/Flow axes, sparse payload, numeric result, evidence/hash/status, redaction and lease behavior. In isolated non-production DB/S3, collect at least 20 valid hot and cold repeats for source document/reference counts, frontier rounds, support query count, classification/support/total elapsed and peak RSS; total snapshot p95 may not regress more than 5%. Exercise timeout, signal, missing/duplicate/unknown terminal, strict `0+succeeded` / `42+blocked` exit pairing and both mismatches, blocked truncation metadata, and lease loss; verify child cleanup and no stale writeback. | Preserve only existing `passed|blocked|error`, `blockingReasons`, and `calculatorReport`; do not add DB status enums/migrations. Lineage/model-composition must not probe targets or expand axes; exact exchange/provider dependencies still fail closed. Never use production enqueue/write for validation. |
| maintenance worker_jobs / GC orchestration | `cargo check -p solver-worker --bin maintenance_worker`; `cargo check -p solver-worker --bin maintenance_enqueue`; run touched binaries such as `cargo check -p solver-worker --bin snapshot_gc --bin result_gc --bin package_gc --bin process_flow_graph_cache_builder`; `cargo test -p solver-worker --bin maintenance_worker`; `cargo test -p solver-worker --bin maintenance_enqueue`; run the touched GC/filter/cache binary tests such as `cargo test -p solver-worker snapshot_gc`, `cargo test -p solver-worker result_gc`, `cargo test -p solver-worker package_gc`, or `cargo test -p solver-worker --bin process_flow_graph_cache_builder`; hard Clippy gate for all targets | run a safe dry-run `lca.snapshot_gc`, `lca.result_gc`, `tidas.package_artifact_gc`, or `national_carbon.process_flow_graph_cache_build` worker job in dev when DB and storage env are available; legacy-table retirement should verify `result_gc` does not join `lca_jobs` and package GC can run without `lca_package_jobs` | Keep `docs/agents/repo-architecture.md`, `README.md`, deployment units, and the package/LCA retention docs aligned with job kind, payload, summary, and destructive-execute safety semantics. |
| package worker import or export flows | baseline gates; real release-binary handshake; active-source audit proving no Python validator or validator-command fallback remains | validate the largest available package locally with the release `tidas` binary before any server execution; verify `tidas.operation-report.v1`, exact version, `asset_fingerprint`, issue-spool SHA-256/bytes/event count, bounded memory/queue settings, and stable Worker error codes; run the closest safe package-flow helper when isolated DB/S3 is available | The large package fixture remains outside git. Package-job semantics are runtime-sensitive and may depend on storage or DB state; never make a production mutation for validation. |
| package `worker_jobs` queue backend | `cargo test -p solver-worker --bin package_worker`; `cargo test -p solver-worker package_worker`; `cargo check -p solver-worker --bin package_worker`; hard Clippy gate; hard format gate | when DB/S3 env is available, enqueue one safe `worker_queue=package` job and run `package_worker --package-queue-backend worker-jobs` to verify claim/heartbeat/result projection; for legacy-table retirement, run against a schema where `public.lca_package_jobs` is absent or ignored | Keep `docs/tidas-package-contract.md` aligned with job kind, payload schema, continuation behavior, artifact projection, worker_jobs result_ref, and optional legacy `lca_package_jobs` compatibility expectations. |
| runtime SQL expectation docs or local migration helpers | baseline gates plus `./scripts/validate_additive_migration.sh` when the task touches migration expectations | record separately when durable schema proof is required in `database-engine` | Local migration files here are not the workspace-wide source of truth. |
| manual debug, parity, or target-validation scripts | run the touched script with safe args or `--help` when available, plus baseline gates if code changed nearby | `./scripts/run_full_compute_debug.sh`, `./scripts/run_bw25_validation.sh`, or `./scripts/validate_lcia_targets.sh` as applicable | `bw25-validator` is manual-only and out-of-band. |
| repo docs, `.env.example`, or docpact config only | `scripts/docpact validate-config --root . --strict`; `scripts/docpact lint --root . --worktree --mode enforce` | perform route checks for affected intent surfaces such as `solver-runtime`, `package-worker`, or `runtime-sql-boundary` | Refresh review metadata even when prose-only docs change. Keep `.env.example` secret-free. |

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
