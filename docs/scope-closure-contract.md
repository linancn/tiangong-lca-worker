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
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
  - docs/agents/contracts/scope-closure-external-result.v1.schema.json
  - docs/agents/contracts/scope-closure-provider-result.v1.schema.json
  - docs/agents/contracts/scope-closure-provider-owned-result.v1.schema.json
  - scripts/scope_closure_qualification.py
  - scripts/run_scope_closure_external_qualification.sh
  - scripts/run_scope_closure_provider_qualification.sh
lastReviewedAt: 2026-08-05
lastReviewedCommit: 7f6240a9e5e81797a16c5e948edc07c2423d1d05
lastReviewedNote: "Updated for Worker Issue #221: numerical artifacts distinguish optional administrative support from fail-closed numerical dependencies."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/tidas-package-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
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

1. `version --format json --progress never` must equal `TIDAS_EXPECTED_VERSION` (active governed default `0.1.3`).
2. `validate --describe --format json --progress never` must advertise `document-validation-batch.v1`, `tidas-document-conformance.v1`, the validation report schema, and an immutable asset fingerprint.
3. Uncached documents are spooled as canonical JSON plus an exact JSONL input manifest.
4. The Worker invokes profile `tidas-document-conformance.v1` with bounded memory/queue configuration inherited by the binary.
5. Validation events are written to a file spool and the bounded operation report is captured as JSON. For the published v0.1 protocol field `logical_issue_stream_sha256`, both producer and consumer hash the exact issue-event NDJSON bytes, including line framing and excluding the terminal final event; Worker computes that digest before JSON parsing. Worker also verifies spool SHA-256/byte size, issue count, final-event equality, report completeness, and asset fingerprint before accepting evidence.
6. While traversal/validation is active, the leased Worker executor refreshes its lease every one-third lease interval. Lease loss or cancellation rejects the operation future and no validation evidence may reach certificate projection.
7. A command timeout, unsupported version/protocol, malformed report/event, spool mismatch, or missing final event is a system failure; document issues remain domain blockers.

Document-validation evidence is cached only under the full immutable key: exact dataset identity, canonical content hash, validator package version, validation profile, report schema, engine/ruleset fingerprint, and full published asset fingerprint. Cached issue events are replayed into the current scan; cache identity never depends on a mutable row alone.

The Worker consumes validation evidence under fixed resource windows: at most 256 cache keys per lookup, 64 uncached documents per `tidas` execution, and 8 MiB of encoded evidence per cache-record RPC. Raw validator issue events are stream-verified into a spool capped at 2 GiB and 5,000,000 events. The exact verified NDJSON is compressed once into the final `tidas/issues.ndjson.zst` evidence member; its logical bytes, SHA-256, and event count remain authoritative. Unified issue coalescing uses bounded external-sort runs keyed by source and issue, keeps only one coalesced issue plus one source reachability state, and feeds globally issue-key-ordered final partitions. It does not create occurrence or affected-root/witness expansion runs.

Before coalescing begins, temporary-disk admission projects only stages derivable from observed raw bytes and bounded active windows. It adds a 25% safety margin plus the fixed 512 MiB reserve. The global requested-root count is never multiplied by issue count for admission. Source impact is encoded once as compact ordinals, and witness evidence is stored once as the frozen reference graph, so physical temporary and artifact bytes do not scale with `issue × root × witness length`. Initial or incremental shortage returns the stable `scope_closure_relation_temp_space_low` error with its stage, available, planned, required, and reserve bytes. Run creation, merge consumption, and partition completion use best-effort sequential-access/cache-release hints, while Linux phase telemetry records process RSS and cgroup v2 `anon`, `file`, `memory.current`, and `memory.peak`. Cancellation, lease loss, or any admission failure cleans every owned temporary directory.

Local capacity qualification has two non-interchangeable modes. `real-payload` bounded-reads and preserves each actual package document JSON value, validates and accounts for every package member, and fails closed on malformed, oversized, or unexpectedly shaped input. `synthetic-cardinality` generates deterministic scale and distribution evidence only; it cannot substantiate real-package behavior. A real-payload result reports per-administrative-relation p50/p95/p99/max logical and standalone-zstd record bytes plus the maximum exact identity, and labels any synthetic topology scaffold separately from package payload evidence.

The git-tracked external qualification entrypoint is
`scripts/run_scope_closure_external_qualification.sh --fixture <zip> --output <dir>`.
It runs only on Linux, requires an exact executable `TIDAS_BIN=0.1.3`, streams a
bounded safe extraction without logging payloads, validates the native TIDAS
protocol and spool identity, and runs the same real package/spool through exact
`cold`, `warm`, `mixed`, and `stale` capacity modes. The four logical and artifact
identities and stable counts must agree before
`lcia.scope-closure-external-result.v1` is written atomically. The child result
contains only aggregate source accounting/distribution, authoritative
document/edge/root counts, TIDAS version/protocol/spool evidence, process RSS,
cgroup v2 memory, temporary-space, and wall-time evidence. Package paths,
payloads, private fixture content, logs, credentials, and locators are never
result fields.

The git-tracked provider entrypoint is
`scripts/run_scope_closure_provider_qualification.sh --output <file>`. It accepts
only explicitly confirmed loopback `QUALIFICATION_*` targets and invokes
git-tracked, exact-SHA adapters in Database, Edge, and Next. Those owning
repositories produce `lcia.scope-closure-provider-owned-result.v1` fragments for
their own database/storage/publication/download/lifecycle/consumer assertions.
Worker only verifies identity, positive evidence, scale and cleanup invariants,
merges disjoint fragments, and emits
`lcia.scope-closure-provider-result.v1`. It does not reproduce another
repository's business assertion. Missing adapters or credentials, a production
fingerprint, descriptor-scale/batch drift, retry/fence/seal/finalize/download/GC
failure, secret or locator material, or cleanup residue fails before output.

On the Linux runtime, `SCOPE_CLOSURE_MEMORY_BUDGET_MIB` defaults to 2048 MiB and applies to Worker RSS across traversal, graph finalization, validation/cache windows, and issue merging. Crossing the limit fails the run without certificate projection. The TIDAS child retains its own `TIDAS_MEMORY_BUDGET_MIB` enforcement.

## Issues and affected roots

The Worker coalesces deterministic issue keys while retaining exact deduplicated occurrence counts and orders the final set by stable `issueKey`. The canonical set is unified across TIDAS, exact-reference and graph findings, frozen-release/source-drift findings, provider findings, matrix/factorization findings, and LCIA-readiness blockers. Each `lcia.scope-closure-issue.v3` main record retains the stable source, code, path, message, severity, blocker flag, exact `occurrenceCount`, exact `affectedRootCount`, and explicitly bounded occurrence/root samples. Only the bounded RPC/XLSX projection is retained in `scan.issues`; the manifest artifacts are the completeness authority.

Graph analysis groups the sorted merge stream by source identity and keeps only one source's reverse-reachability state at a time. A compact source-level root-impact record uses stable ordinals with explicit `none`, `allRoots`, `includedOrdinals`, or `excludedOrdinals` mode; every issue from that source references it. The frozen exact reference graph stores compact reverse predecessors once, allowing a reader to reconstruct any requested root witness deterministically. Production emits zero expanded affected-root rows and no repeated full witness paths. The V3 result RPC receives exact counts, at most 5,000 issue summaries, and at most 100 occurrences/affected roots per projected issue; truncation flags make sampling explicit.

The scan never short-circuits after the first broken reference or invalid document. This gives the operator one stable issue set for the entire requested union.

## Evidence and artifacts

Closure production runs in this fail-closed order:

1. complete the administrative exact-version closure against the frozen release manifest;
2. run signed-flow provider discovery against that same manifest without persisting a snapshot;
3. freeze the discovered exact Process axis and administratively scan the added provider processes;
4. evaluate the discovered matrix, provider-link, factorization, and LCIA readiness evidence;

Administrative and final closure bundles, document/edge/reference evidence, canonical issue partitions, compact graph/impact evidence, XLSX worksheets, and object-store uploads are file-backed. Canonical V1 arrays are emitted incrementally from indexed or sorted spools; the Worker does not reconstruct complete document, reference-edge, TIDAS issue-event, resolution-map, root-impact, or witness collections as `Vec<Value>`. Temporary directories own every intermediate spool/run/artifact and remove them on success, failure, cancellation, or lease loss after the bounded blocking task exits. Cancellation is checked during merge, coalescing, reachability, partition writing, frozen-graph writing, TIDAS compression, and between report stages. Lease heartbeats remain active during artifact preparation and each upload.
5. only when every scan is complete and no blocker remains, run the frozen snapshot builder in persisted build mode.

Administrative closure and numerical Flow selection remain distinct. During the persisted snapshot build, every Elementary Flow referenced by a selected LCIA Method factor is additionally frozen as source-closure `support`, with exact/once-resolved version and recursive support-document closure. A factor-only Flow does not enter the inventory-derived B/C axes, compiled graph, provider discovery, or provider universe. Product, Waste, or Other factor targets are semantic failures; they never cause technosphere expansion.

The numerical snapshot source walk is `path-aware-bounded-frontier-v2`. It consumes the same raw
reference edges produced by `scope_closure.rs`, but applies a separate role × artifact-purpose
policy. Each exact document identity/hash is processed once; exact and omitted-version indexes make
satisfaction checks deterministic. Support reads first obtain bounded identity/version/byte-size
metadata, resolve the exact versions needed by the frontier, and deterministically pack exact-row
queries to both a 512-identity and 64 MiB returned-byte ceiling. A single support document above
64 MiB fails closed instead of weakening the query admission limit. The cumulative canonical
source-document ceiling is configured by
`SOURCE_CLOSURE_TOTAL_DOCUMENT_BYTES` and defaults to 1 GiB; zero, malformed, and overflow
values fail back to that bounded default. The build separately enforces cumulative
document/reference/edge/depth limits.
Identity/hash drift and limit overflow are operator errors. Metrics expose source document count,
classified reference count, frontier rounds, support query count, and decoded document bytes.

This numerical frontier does not replace certificate-grade administrative traversal. For
review-submit and ordinary Calculation Bundles, lineage and model-composition edges are evidence
only and never probe their target. Certificate closure continues its full, non-fail-fast union
traversal and issue aggregation under this document's frozen-release rules.
Administrative-support references such as data-entry contacts, ownership, dataset formats,
compliance systems, logos, and provenance sources are optional for numerical artifacts: valid
targets are fetched when available, while malformed placeholders and unavailable targets are
retained only in bounded provenance evidence. Numerical exchange/provider references and required
Flow Property, Unit Group, and LCIA support remain fail-closed. This distinction is identified by
`source-reference-policy.v4` and participates in snapshot/review fingerprints.
Schema-defined `referenceToDigitalFile` URI values are external attachment locators rather than
dataset references. The numerical source walk ignores their raw extraction findings for
review-submit and ordinary Calculation Bundles; certificate-grade administrative traversal keeps
its existing strict extraction and issue-aggregation behavior.

Each fresh scan produces deterministic administrative artifacts:

- `closure-bundle-v4.json`: a small certificate/snapshot/package binding manifest containing requested bindings, scan counts, relation-level logical hashes/counts, and the logical hash/count/path reference to the single compressed TIDAS stream; it contains no growing scan arrays, and the package verifier reads it through bounded file I/O while historical v1/v3 bundles remain readable;
- `manifest.json`: `lcia.scope-closure-issue-manifest.v4`, preserving canonical v3 unified issue/root-impact/witness semantics while additionally binding bounded administrative evidence partitions, oversized-record indexes/chunks, and their relation-local logical layout;
- `issues/part-NNNNNN.ndjson.zst`: globally `issueKey`-ordered `lcia.scope-closure-issue.v3` main records, each streamed through one active zstd writer and sealed at the first of 25,000 records or 32 MiB canonical uncompressed NDJSON;
- `tidas/issues.ndjson.zst`: the exact verified TIDAS logical event stream, compressed once;
- `evidence/root-impact-index-v1.bin.zst` and `evidence/frozen-reference-graph-v1.bin.zst`: compact root membership and deterministic witness-reconstruction evidence;
- `administrative/<relation>/part-NNNNNN.ndjson.zst`: documents, edges, resolved references, resolution map, roots, frontier, provider universe, and omitted-version resolutions, each sealed at the first of 25,000 records or 32 MiB canonical uncompressed NDJSON;
- `administrative/<relation>/oversized/record-<logical-ordinal>/index.json` plus `chunk-NNNNNN.bin`: the representation for one administrative record whose canonical record plus newline exceeds 32 MiB; the small index binds the relation, logical ordinal/key, total canonical byte length/hash, and every fixed 8 MiB canonical-byte chunk ordinal/path/length/hash;
- `closure-report-v1.xlsx`: a valid workbook tagged with the current `closureCheckId`, containing exact summary counts, a complete-artifact index, at most 5,000 issue rows, 10,000 occurrence rows, and 10,000 affected-root rows.

Partition ordering, binary encoding, and zstd level are deterministic for identical inputs. Ordinary records keep the historical NDJSON representation. For an individually oversized administrative record, Worker first closes the active ordinary partition, writes the canonical JSON bytes without their framing newline into contiguous fixed-size chunks, and records one layout entry for the index. A reader must stream chunks in layout order, verify every binding, and append exactly one logical newline while updating the relation hash/count; missing, extra, duplicate, reordered, truncated, or corrupted index/chunk material fails closed. The logical issue hash remains the exact TIDAS issue-event NDJSON bytes, and every administrative relation hash remains the original canonical record-plus-newline stream; physical segmentation does not redefine either identity. Historical v4 manifests without oversized-record fields remain readable as partition-only layouts.

Every prepared scope-closure object is checked against a 256 MiB hard ceiling before a write-set header is created, so an oversized physical object cannot be registered, sealed, or uploaded. A logical administrative record may exceed that ceiling only because its physical representation is bounded to one small index and 8 MiB chunks. Before opening the XLSX ZIP, Worker rejects any worksheet over 1,048,576 rows or 64 MiB estimated uncompressed XML and any workbook over 128 MiB estimated worksheet XML. It also rejects the finished archive above 64 MiB.

The manifest plus its listed members is the only complete machine-result representation. A bounded version-dispatch reader retains v2/v3 migration support and verifies v4 exact membership, ordering, compressed/uncompressed hashes and sizes, administrative relation hashes/counts, unified issue counts, root-impact references, graph boundaries, and reconstructed sample witnesses. Publication follows Database #316's `lcia.scope-closure-artifact-write-set.v2`: deterministic one-based descriptors and digest, header creation, batches of at most 500, status readback, atomic seal into `staging`, exact `clientKey -> artifactId` readback, and only then object upload. Finalization atomically exposes `ready`. Worker uploads no object before successful seal/readback and preserves Database #309's role, trusted expiry, download, and GC semantics.

Expired report artifacts are reclaimed through the generic `worker.artifact_gc` maintenance job and Database Engine #309's exact `svc_lcia_scope_closure_artifact_gc_*` contract. The claim RPC returns at most 500 items under one token. An `object_delete` item has `lifecycleState=expired`, `objectDeleteRequired=true`, and an exact bucket/path; Worker validates that identity, deletes once, treats an already-missing object as idempotent success, and begins at most 50,000-row completion batches. If details remain after tombstoning, Database persists `gc_cleanup_state=pending`; a fresh process reclaims the row with a new fenced token as `gcPhase=detail_cleanup`, `objectDeleteRequired=false`, and null bucket/path, then completes bounded batches without a second object deletion. Object deletion failure records `gc_failure_count` and releases the claim without premature tombstoning.

The actor-bound download projection is `get_lcia_scope_closure_report_download(uuid,text)`. It accepts only `closure_report_xlsx` and `closure_issue_manifest` and returns the exact 11-field public descriptor: artifact ID/role/state, semantic filename, format, media type, size, checksum, expiry, bucket, and object path. Database Engine #309 retains the one-argument compatibility overload, fixed-order locator-free owner summaries, trusted expiry, and fenced GC preview/renew/reconcile RPCs; its old one-shot write operation is now only a compatibility adapter. Database #316 owns the staged v2 registration/seal/finalize contract used by fresh Worker publication. Database maps public selectors to linked coarse-role rows; Worker does not expose or synthesize a separate download API. This delivery exposes only XLSX and the manifest. The manifest identifies the complete member set, but direct client retrieval of subordinate members requires a separately authorized cross-repository contract; Worker #177 does not invent an archive or broaden selectors.

`closure-snapshot-v1.json` is not a numerical snapshot and must not be produced. A blocked or incomplete run persists only the administrative artifacts above; its snapshot identity, snapshot hashes, snapshot artifact reference, numerical `evidenceHash`, and certificate are absent.

For a complete blocker-free run, the existing frozen `snapshot_builder` persists the real numerical `snapshot-hdf5:v1` artifact and snapshot-index sidecar through `lca_network_snapshots` and `lca_snapshot_artifacts`. Passed evidence comes back from those persisted records and binds `snapshotId`, the HDF5 artifact SHA-256 as `snapshotHash`, `snapshotArtifactId`, `snapshotIndexSha256`, and `snapshotBuildContractHash`. The embedded HDF5 binding uses `lcia.scope-closure-snapshot-binding.v1` and binds `effectiveScopeHash`, `dataSnapshotToken`, and `closureBundleHash`. The snapshot-index sidecar is the authoritative exact ordered Process axis; its count must also match the numerical payload. Calculation Bundle release evidence is a separate integrity-bound zstd sidecar referenced from the HDF5 envelope, not a persisted full compiler graph. Generic live-snapshot reuse cannot substitute an artifact that lacks these bindings.

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

The Worker accepts this authoritative eleven-field binding only all-or-none and validates every field against a currently valid, complete, passed closure check before package execution. It downloads the exact closure-bundle artifact and numerical snapshot artifact by their certified IDs, recomputes their hashes, and requires the snapshot-index sidecar to preserve the exact ordered effective Process axis while the numerical payload preserves the same count. When Calculation Bundle materialization needs release evidence, Worker follows the integrity-bound sidecar descriptor in the verified HDF5 rather than requiring a persisted compiled graph. `report_artifact_manifest_hash` remains certificate/audit evidence in the job payload, but it is not a substitute for the exact closure-bundle artifact identity. The Worker consumes the certificate and frozen snapshot; it does not rerun administrative closure.

Closure binding changes provenance and eligibility, not numerical computation. The existing package snapshot build, all-unit solve, result artifact, and ready-marking path remains unchanged. Result JSON, result refs, persisted package metadata, and audit context preserve `closureCheckId` so downstream consumers can prove which certificate authorized the unchanged numerical output.

## Required proof

For changes to this contract, run the repo baseline plus focused closure tests. At minimum, preserve proof for:

- TIDAS/Worker golden extraction parity;
- union traversal, shared dependencies, cycles, exact versions, and non-fail-fast aggregation;
- frozen-release live drift and live-only substitution rejection;
- omitted-version frozen candidates and winner provenance;
- bounded batches, deterministic hashes, cancellation, and valid check-scoped XLSX output;
- role-tagged publication metadata, trusted seven-day expiry, bounded compressed partitions, and no duplicate monolithic machine result;
- object-delete-before-complete GC, missing-object idempotency, invalid bucket/path rejection, and retry without premature tombstoning;
- byte-exact non-empty TIDAS issue-stream hashing across JSON field orders;
- file-backed document/reference retention and compact-graph RSS qualification;
- all-or-none package binding and database certificate mismatch rejection;
- shared-scan target-specific report/finalizer behavior.

Live integration proof, when available, must use isolated non-production database and object-storage state. Before any server execution, validate the largest available package locally with the exact published binary and bounded memory/queue settings. Do not deploy or mutate production data as validation.
