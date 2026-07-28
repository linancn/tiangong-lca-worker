---
title: LCIA Scope Closure Worker Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: en
whenToUse:
  - when changing lcia.scope_closure_check worker execution
  - when changing closure evidence, scan reuse, or package-build certificate binding
  - when coordinating closure contracts with database-engine, edge-functions, or tidas-tools
whenToUpdate:
  - when closure job payloads, release snapshots, traversal rules, validation protocols, artifacts, or certificate bindings change
checkPaths:
  - docs/scope-closure-contract.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/solver-worker/src/scope_closure.rs
  - crates/solver-worker/src/queue.rs
  - crates/solver-worker/src/types.rs
  - crates/solver-worker/src/db.rs
  - docs/lca-api-contract.md
  - docs/tidas-package-contract.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
lastReviewedAt: 2026-07-28
lastReviewedCommit: 833808c
lastReviewedNote: "Issue #158 sets the Linux scope-closure fail-closed RSS guard to 2048 MiB after production-shape validation."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/tidas-package-contract.md
  - docs/agents/repo-validation.md
---

# LCIA Scope Closure Worker Contract

## Ownership

The Worker owns execution of `lcia.scope_closure_check`: immutable-source verification, deterministic reference traversal, document validation, issue aggregation, artifact production, and lease-fenced terminal projection.

`database-engine` owns durable tables, policies, RPC signatures, request normalization, current-release snapshot creation, certificate minting, and atomic build binding. `tidas-tools` owns reference extraction and document-validation protocol semantics. Edge and Next consume those contracts; they do not recreate closure truth.

## Job and immutable input

The canonical job kind is `lcia.scope_closure_check` with payload schema `lcia.scope_closure_check.request.v1`. The claimed database envelope carries:

- `closure_check_id`
- `scan_execution_id`
- `data_snapshot_token`
- `request_fingerprint`

The Worker loads the full service input through `svc_lcia_scope_closure_check_get_worker_input`. It requires the normalized requested scope, scope/policy/request hashes, expected validator-scanner fingerprint, publication epoch, and `lcia.scope-closure-data-snapshot.v2`.

The V2 data snapshot is the only certificate-grade source boundary. It contains:

- the exact normalized requested scope;
- the current public publication and release-run identity;
- the release manifest hash;
- the complete `lca_release_dataset_versions` allowlist with exact dataset identity, role, source-process provenance, version-significant hash, semantic hash, and canonical content hash.

The Worker recomputes the PostgreSQL JSONB scope and snapshot hashes with the database's authoritative hash helper. A blank or inconsistent binding fails closed. Requested process roots that are present in the release must have role `unit_process`; a root absent from the release is reported as an incomplete source-boundary blocker.

## Frozen-source traversal

Traversal is a union traversal over all exact process and LCIA-method roots. It is:

- breadth-first, cycle-safe, and deterministic;
- deduplicated by `(dataset type, UUID, exact version)`;
- fetched in bounded batches of 96 identities;
- non-fail-fast for domain findings;
- checkpointed through the active Worker lease between batches.

CPU-heavy graph finalization runs through Tokio's blocking pool so sorting, coalescing, and affected-root analysis cannot occupy a lease-heartbeat runtime thread. Canonical ordering materializes each serialized sort key once per collection, and the Worker emits phase durations and collection counts for capacity diagnosis without changing the evidence payload.

Fetched document payloads are canonicalized and appended to a temporary random-access spool during traversal, then released after reference extraction. The retained document index contains only exact identity, canonical content hash, file offset, and byte size. Reference-edge and resolved-reference evidence is likewise file-backed; affected-root analysis uses numeric identity IDs and compact adjacency lists rather than repeated full identity objects per graph edge. Cache hits never reload document payloads, while each uncached TIDAS execution reloads at most its fixed 64-document window.

Every database fetch is constrained to an identity in the frozen release manifest. The Worker canonicalizes each fetched JSON document and compares it with the release's `canonicalContentHash`. These conditions make the scan incomplete and block certificate issuance:

- an allowlisted identity is unreadable from the live source table;
- live content for an allowlisted exact identity has drifted from the release hash;
- a referenced identity exists live but is absent from the frozen release manifest.

An exact reference never falls back to another version. A missing exact identity is a complete negative finding when it is absent from both the release allowlist and the observed closure. The legacy omitted-version policy is normally `reject`. If a tracked future scope explicitly uses `latest_eligible`, candidates and the deterministic winner must come only from the frozen release manifest, and the resolution map records the policy, candidate universe, candidates, and selected identity.

`linkPolicy.providerUniversePolicy=scope_only` rejects a process provider outside the requested roots. `eligible_transitive_expansion-v1` may add a referenced process only when that exact identity is in the frozen release. Every accepted transitive process is part of the effective scope and evidence; the Worker never searches a mutable live provider universe.

## TIDAS validation

Reference extraction in `scope_closure.rs` mirrors the public `tidas.reference-extraction-result.v1` contract and is locked by the shared golden fixture under `crates/solver-worker/tests/fixtures/reference_extraction_v1/`.

Document validation uses only the published unified Rust `tidas` CLI selected by `TIDAS_BIN` (default `tidas`). No Python entrypoint, legacy binary name, or ordered command-candidate fallback is permitted:

1. `version --format json --progress never` must equal `TIDAS_EXPECTED_VERSION` (default `0.1.1`).
2. `validate --describe --format json --progress never` must advertise `document-validation-batch.v1`, `tidas-document-conformance.v1`, the validation report schema, and an immutable asset fingerprint.
3. Uncached documents are spooled as canonical JSON plus an exact JSONL input manifest.
4. The Worker invokes profile `tidas-document-conformance.v1` with bounded memory/queue configuration inherited by the binary.
5. Validation events are written to a file spool and the bounded operation report is captured as JSON. For the published v0.1 protocol field `logical_issue_stream_sha256`, both producer and consumer hash the exact issue-event NDJSON bytes, including line framing and excluding the terminal final event; Worker computes that digest before JSON parsing. Worker also verifies spool SHA-256/byte size, issue count, final-event equality, report completeness, and asset fingerprint before accepting evidence.
6. While traversal/validation is active, the leased Worker executor refreshes its lease every one-third lease interval. Lease loss or cancellation rejects the operation future and no validation evidence may reach certificate projection.
7. A command timeout, unsupported version/protocol, malformed report/event, spool mismatch, or missing final event is a system failure; document issues remain domain blockers.

Document-validation evidence is cached only under the full immutable key: exact dataset identity, canonical content hash, validator package version, validation profile, report schema, engine/ruleset fingerprint, and full published asset fingerprint. Cached issue events are replayed into the current scan; cache identity never depends on a mutable row alone.

The Worker consumes validation evidence under fixed resource windows: at most 256 cache keys per lookup, 64 uncached documents per `tidas` execution, and 8 MiB of encoded evidence per cache-record RPC. Issue events are stream-verified into disk spools capped at 2 GiB and 5,000,000 events, then deterministically ordered through 16 MiB external-sort runs. Resolution-map ordering uses the same bounded mechanism. The Worker records document/cache/issue/spool counts and hashes, retains at least 512 MiB of temporary-volume headroom beyond planned sort space, and fails closed when a spool, cache record, or temporary-space limit is exceeded.

On the Linux runtime, `SCOPE_CLOSURE_MEMORY_BUDGET_MIB` defaults to 2048 MiB and applies to Worker RSS across traversal, graph finalization, validation/cache windows, and issue merging. Crossing the limit fails the run without certificate projection. The TIDAS child retains its own `TIDAS_MEMORY_BUDGET_MIB` enforcement.

## Issues and affected roots

The Worker coalesces deterministic issue keys while retaining occurrence counts and orders the final set by stable `issue_key`. Each issue records the primary source identity, JSON path, reference role, requested target identity, message, action, and blocker status. Graph analysis records every affected root and a deterministic witness path specific to that root. Result projection stores primary issues, occurrences, and affected-root rows through the database's V2 result RPC.

The scan never short-circuits after the first broken reference or invalid document. This gives the operator one stable issue set for the entire requested union.

## Evidence and artifacts

Closure production runs in this fail-closed order:

1. complete the administrative exact-version closure against the frozen release manifest;
2. run signed-flow provider discovery against that same manifest without persisting a snapshot;
3. freeze the discovered exact Process axis and administratively scan the added provider processes;
4. evaluate the discovered matrix, provider-link, factorization, and LCIA readiness evidence;

Administrative and final closure bundles, document/edge/reference evidence, issue JSONL, XLSX worksheets, and object-store uploads are file-backed. Canonical V1 arrays are emitted incrementally from indexed or sorted spools; the Worker does not reconstruct the complete document, reference-edge, TIDAS issue, or resolution-map collection as `Vec<Value>`. Temporary directories own every intermediate spool/run/artifact and remove them on success, failure, cancellation, or lease loss after the bounded blocking task exits. Lease heartbeats remain active during both administrative and final artifact preparation.
5. only when every scan is complete and no blocker remains, run the frozen snapshot builder in persisted build mode.

Administrative closure and numerical Flow selection remain distinct. During the persisted snapshot build, every Elementary Flow referenced by a selected LCIA Method factor is additionally frozen as source-closure `support`, with exact/once-resolved version and recursive support-document closure. A factor-only Flow does not enter the inventory-derived B/C axes, compiled graph, provider discovery, or provider universe. Product, Waste, or Other factor targets are semantic failures; they never cause technosphere expansion.

The numerical snapshot source walk is `path-aware-bounded-frontier-v2`. It consumes the same raw
reference edges produced by `scope_closure.rs`, but applies a separate role × artifact-purpose
policy. Each exact document identity/hash is processed once; exact and omitted-version indexes make
satisfaction checks deterministic; support reads use fixed 512-identity batches with a 64 MiB
returned-byte ceiling, and the build enforces cumulative document/reference/edge/depth limits.
Identity/hash drift and limit overflow are operator errors. Metrics expose source document count,
classified reference count, frontier rounds, support query count, and decoded document bytes.

This numerical frontier does not replace certificate-grade administrative traversal. For
review-submit and ordinary Calculation Bundles, lineage and model-composition edges are evidence
only and never probe their target. Certificate closure continues its full, non-fail-fast union
traversal and issue aggregation under this document's frozen-release rules.

Each fresh scan produces deterministic administrative artifacts:

- `closure-bundle-v1.json`: requested bindings, TIDAS validation evidence, scan, and resolution map;
- `closure-issues-v1.jsonl`: one canonical issue per line;
- `closure-report-v1.xlsx`: a valid workbook tagged with the current `closureCheckId`.

`closure-snapshot-v1.json` is not a numerical snapshot and must not be produced. A blocked or incomplete run persists only the administrative artifacts above; its snapshot identity, snapshot hashes, snapshot artifact reference, numerical `evidenceHash`, and certificate are absent.

For a complete blocker-free run, the existing frozen `snapshot_builder` persists the real `snapshot-hdf5:v1` artifact and snapshot-index sidecar through `lca_network_snapshots` and `lca_snapshot_artifacts`. Passed evidence comes back from those persisted records and binds `snapshotId`, the HDF5 artifact SHA-256 as `snapshotHash`, `snapshotArtifactId`, `snapshotIndexSha256`, and `snapshotBuildContractHash`. The embedded HDF5 binding uses `lcia.scope-closure-snapshot-binding.v1` and binds `effectiveScopeHash`, `dataSnapshotToken`, and `closureBundleHash`; its exact compiled Process axis must match the frozen discovered axis. Generic live-snapshot reuse cannot substitute an artifact that lacks this binding.

Administrative artifacts are uploaded before terminal projection. The report artifact manifest hash is recomputed from persisted database metadata. `evidenceHash` is `lcia.scope-closure-evidence.v2` and binds the immutable scan hashes plus the persisted numerical snapshot identity and hashes, while intentionally excluding the run-specific report artifact manifest. A certificate additionally binds the current closure check and its current report artifact manifest, so copied or stale reports cannot be substituted.

A certificate is available only for `status=passed` and `scanCompleteness=complete`. Domain blockers produce a complete blocked result. Cancellation, lease loss, validator failure, source drift, or another system failure cannot produce a valid certificate.

## Shared scans and retry behavior

`scan_execution_id` coordinates identical immutable work. The Worker claims it with the active job lease:

- acquired executions run normally;
- busy executions wait with lease heartbeats and bounded exponential backoff;
- completed executions may reuse immutable scan evidence only when the database verifies all request, policy, snapshot, and scan bindings.

Reuse does not copy the source run's report or result summary. The current run rebuilds and uploads a new XLSX tagged with its own `closureCheckId`, supplies a new result summary to the six-argument reuse finalizer, and receives a new target-scoped certificate bound to the new report manifest. Source `evidenceHash` remains immutable.

The early-failure RPC is safe before or after scan claim. It fails only the current run and releases a scan execution only when this job holds its lease; a waiter cannot destroy another run's reusable work.

## Package build binding

The database Build V2 command atomically enqueues `lcia_result.package_build` with a full closure binding:

- `closure_check_id`
- `closure_certificate_hash`
- `effective_scope_hash`
- `data_snapshot_token`
- `snapshot_id`
- `snapshot_hash`
- `snapshot_artifact_id`
- `snapshot_index_sha256`
- `snapshot_build_contract_hash`
- `closure_bundle_artifact_id`
- `closure_bundle_hash`

The Worker accepts this authoritative eleven-field binding only all-or-none and validates every field against a currently valid, complete, passed closure check before package execution. It downloads the exact closure-bundle artifact and numerical snapshot artifact by their certified IDs, recomputes their hashes, and requires the HDF5 compiled graph plus snapshot-index sidecar to preserve the exact ordered effective Process axis. `report_artifact_manifest_hash` remains certificate/audit evidence in the job payload, but it is not a substitute for the exact closure-bundle artifact identity. The Worker consumes the certificate and frozen snapshot; it does not rerun administrative closure.

Closure binding changes provenance and eligibility, not numerical computation. The existing package snapshot build, all-unit solve, result artifact, and ready-marking path remains unchanged. Result JSON, result refs, persisted package metadata, and audit context preserve `closureCheckId` so downstream consumers can prove which certificate authorized the unchanged numerical output.

## Required proof

For changes to this contract, run the repo baseline plus focused closure tests. At minimum, preserve proof for:

- TIDAS/Worker golden extraction parity;
- union traversal, shared dependencies, cycles, exact versions, and non-fail-fast aggregation;
- frozen-release live drift and live-only substitution rejection;
- omitted-version frozen candidates and winner provenance;
- bounded batches, deterministic hashes, cancellation, and valid check-scoped XLSX output;
- byte-exact non-empty TIDAS issue-stream hashing across JSON field orders;
- file-backed document/reference retention and compact-graph RSS qualification;
- all-or-none package binding and database certificate mismatch rejection;
- shared-scan target-specific report/finalizer behavior.

Live integration proof, when available, must use isolated non-production database and object-storage state. Before any server execution, validate the largest available package locally with the exact published binary and bounded memory/queue settings. Do not deploy or mutate production data as validation.
