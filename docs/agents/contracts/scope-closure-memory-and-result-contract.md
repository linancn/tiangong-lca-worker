---
title: Scope Closure Memory and Result Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: en
whenToUse:
  - when changing scope-closure issue aggregation, affected-root impact, witness evidence, or artifact layout
  - when changing scope-closure memory, temporary-space, cancellation, publication, or determinism behavior
  - when reviewing compatibility between canonical v3 artifacts and legacy v2 readers
whenToUpdate:
  - when the canonical issue schema, compact evidence format, manifest, publication handshake, or qualification gates change
checkPaths:
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
  - docs/scope-closure-contract.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - crates/solver-worker/src/scope_closure.rs
  - crates/solver-worker/src/worker_jobs.rs
  - crates/solver-worker/tests/artifact_gc_database_contract.rs
  - scripts/scope_closure_qualification.py
  - scripts/run_scope_closure_external_qualification.sh
  - scripts/run_scope_closure_provider_qualification.sh
  - docs/agents/contracts/scope-closure-external-result.v1.schema.json
  - docs/agents/contracts/scope-closure-provider-result.v1.schema.json
  - docs/agents/contracts/scope-closure-provider-owned-result.v1.schema.json
lastReviewedAt: 2026-08-12
lastReviewedCommit: 30c8e0216028116556769291481822353266f65b
lastReviewedNote: "Reviewed for Worker PR #225: schema cutover preserves the current bounded, file-backed scope-closure result and memory contract."
related:
  - ../../../AGENTS.md
  - ../../../.docpact/config.yaml
  - ../../scope-closure-contract.md
  - ../repo-validation.md
  - ../repo-architecture.md
---

# Scope Closure Memory and Result Contract

## Canonical semantic result

`lcia.scope-closure-issue-manifest.v4` is the canonical complete machine-result contract for a fresh scope-closure scan. V4 preserves the unified v3 issue/root-impact/witness semantics and adds bounded administrative evidence membership. It represents one unified issue set, not a TIDAS-only validation result. The set includes:

- TIDAS document-conformance issues;
- exact-reference, missing-reference, frozen-release, and source-drift issues;
- process-provider and provider-universe issues;
- requested-scope and reference-graph issues;
- matrix construction, signed-flow/provider, factorization, and LCIA-readiness blockers;
- typed snapshot source-preflight blockers from discovery or the final numerical build preflight.

Every distinct issue has one `lcia.scope-closure-issue.v3` main record. Snapshot source blockers coalesce by error code plus requested target identity (or raw malformed-target fingerprint plus extraction code); source/path/role remain occurrence evidence. A multi-source grouped issue omits a misleading single top-level source/path. Stable semantic fields include `issueKey`, `code`, `message`, `severity`, `blocker`, `occurrenceCount`, `affectedRootCount`, and bounded occurrence/root samples; source/path remain present for single-source issues. Reference role, requested target, suggested action, and truncation flags remain present when applicable. The complete blocker count, blocker-code set, verdict, certificate inputs, occurrence count, and affected-root count are derived from this unified set. Inline RPC and general XLSX views are bounded projections and are never completeness authorities. The exception is the dedicated snapshot-blocker worksheets: they stream every record from the verified canonical NDJSON sidecar and split instead of truncating.

Issue identity and order are deterministic:

- `issueKey` is the coalescing identity and partitions are globally ordered by UTF-8 ascending `issueKey`;
- exact dataset identities and root ordinals are ordered by dataset category, UUID, and exact version;
- object keys are canonicalized recursively, arrays preserve semantic order, NDJSON has one canonical JSON record plus `\n`, and zstd uses a fixed level;
- manifest logical and physical hashes bind the exact bytes and counts required to reconstruct the result.

The raw TIDAS issue-event NDJSON is preserved exactly once as `tidas/issues.ndjson.zst`. Its logical SHA-256, logical byte size, and event count are those of the original verified NDJSON stream, including line framing and excluding the terminal final event. It is not expanded or copied into occurrence partitions. Non-TIDAS issue occurrences are represented by their exact coalesced count and bounded samples; their source graph, reference, provider, matrix, and readiness evidence remains in the frozen closure inputs and bundle.

## Compact root impact and witnesses

Production must not materialize or publish the Cartesian physical relation `issue × affected root × full witness path`.

Root impact uses stable zero-based root ordinals and `lcia.scope-closure-root-impact-index.v1`. Records are issue-level and globally ordered by issue key. For a grouped issue, Worker walks source occurrences in stable order, computes one source reachability window at a time, and ORs those results into one current-issue root bitset. Each impact has one explicit mode:

- `none`;
- `allRoots`;
- `includedOrdinals`;
- `excludedOrdinals`.

`allRoots` is never inferred from a missing list. For partial sets, the writer chooses the smaller deterministic included or excluded delta-varint ordinal set. `affectedRootCount` remains exact even when the inline sample is truncated.

`evidence/frozen-reference-graph-v1.bin.zst` stores the exact sorted identity table, stable root-to-node ordinals, and compact reverse predecessor adjacency once per result. Given a source node ordinal and affected root ordinal, a reader reconstructs the witness deterministically by breadth-first traversal over predecessors in stable identity order. The v3 compatibility reader verifies every impact reference, cardinality, root membership, sample witness, evidence hash, and graph boundary before accepting the result.

## Files and manifest

A fresh v4 result contains:

- `closure-bundle-v4.json`, a small certificate/snapshot/package binding manifest containing stable request, policy, validator, TIDAS, scan-count, and administrative relation logical hashes/counts; it never copies growing scan arrays, issue rows, or raw TIDAS events, and historical v1/v3 bundles remain readable through a bounded file reader;
- `closure-report-v1.xlsx`, retained for the existing public operator transport;
- `manifest.json` with schema `lcia.scope-closure-issue-manifest.v4`;
- `issues/part-NNNNNN.ndjson.zst`, containing the globally ordered coalesced v3 issue records;
- `tidas/issues.ndjson.zst`, containing the byte-exact logical TIDAS event stream once;
- `evidence/root-impact-index-v1.bin.zst`;
- `evidence/frozen-reference-graph-v1.bin.zst`;
- `administrative/<relation>/part-NNNNNN.ndjson.zst` for documents, edges, resolved references, resolution map, roots, frontier, provider universe, and omitted-version resolutions.

There are no production `occurrences/*` or `affected-roots/*` partitions in v4, and `expandedAffectedRootRecordCount` is exactly zero. Issue partitions and ordinary administrative records close at the first of 25,000 records or 32 MiB canonical uncompressed NDJSON. An administrative record whose canonical record plus newline exceeds 32 MiB is not rejected and does not change the ordinary representation: Worker flushes the active ordinary partition, writes `administrative/<relation>/oversized/record-<logical-ordinal>/index.json`, and writes contiguous raw canonical-byte chunks named `chunk-NNNNNN.bin`. Every non-final chunk is exactly 8 MiB; the final chunk is non-empty and at most 8 MiB.

The oversized-record index uses `lcia.scope-closure-administrative-oversized-record.v1`. It binds the relation, logical record ordinal and stable record key, canonical record byte length and SHA-256 excluding NDJSON framing, fixed chunk size/count, and every chunk ordinal/path/length/SHA-256. The top-level manifest repeats the record/index identity and carries a relation-local layout whose contiguous sequence ordinals interleave ordinary partition paths and oversized-record index paths. A reader streams that layout in order, streams every chunk, verifies exact membership/order/length/hash, appends the one logical newline only to relation hashing/counting, and therefore reconstructs the same record count, logical byte size, and relation SHA-256 as the unsegmented canonical record plus newline. Missing, extra, duplicate, reordered, truncated, or corrupted index/chunk material fails closed.

Every physical scope-closure object must remain at most 256 MiB; an oversized physical object still fails before write-set creation, registration, seal, or upload. The manifest binds every artifact path, media type, compressed and uncompressed byte size and SHA-256, record count, first/last key, global relation hashes, root count, graph node/edge count, partition/chunk limits, sample limits, and ordering rules.

The version-dispatch reader accepts v2, v3, and v4 manifests. V2 remains readable through its original issue/occurrence/affected-root partitions. V3 and V4 can project the legacy affected-root view on demand from issue records, the compact root-impact index, and the frozen graph. V4 additionally verifies every administrative partition, oversized-record index/chunk set, layout sequence, and relation-level logical hash/count. Historical v4 manifests without oversized-record fields remain readable by deriving their partition-only relation layout. The reader rejects missing, extra, duplicate, reordered, truncated, hash-mismatched, boundary-inconsistent, oversized, or cardinality-inconsistent files. Writers always emit the current v4 shape; they never silently downgrade.

Next and Edge continue to expose the existing XLSX and manifest download selectors and descriptors. V4 does not change their public DTO. Access to subordinate manifest members remains governed by the existing authorized artifact boundary; Worker does not create a new cross-repo public API.

## Bounded execution and cleanup

The implementation is window-bounded rather than relation-cardinality-bounded:

- raw TIDAS events retain their existing 2 GiB and 5,000,000-event validation-input caps;
- snapshot builder stdout retains at most 16 blocker samples; the full set is written to a parent-owned canonical NDJSON sidecar, bound by count/size/SHA-256/completeness metadata under the unchanged terminal V1 schema, verified before conversion, streamed into sorted issue runs and XLSX, and removed with its owning temporary file on every exit path;
- successful snapshot discovery writes the complete `processAxis` and only the readiness fields consumed by Scope Closure (`schema_version`, `status`, `next_action`, and `blockers`) to one parent-owned bounded JSON file; captured stdout contains only a small terminal V1 size/SHA-256 descriptor, and the parent verifies the file before parsing and removes it on every exit path;
- issue coalescing uses bounded external sort runs ordered by issue/source/occurrence and one current coalesced issue;
- reverse reachability keeps one source's compact visited/parent/ordinal state plus one current-issue root bitset; it never retains the issue-by-source or issue-by-root Cartesian relation;
- one active issue partition writer, root-impact writer, frozen-graph writer, and TIDAS compression buffer is retained per stage;
- residual sort records contain the compact issue key and coalesced record, not repeated affected-root identities or JSON witness paths;
- temporary-space admission uses observed input and measured intermediate bytes plus the configured reserve, never `issue count × global root count`.

The later numerical snapshot publication does not copy this administrative artifact graph into HDF5. `CompiledGraph` remains transient compiler IR. Calculation Bundle metadata and source documents are encoded as separate zstd temporary files; both encoders borrow the already-frozen compiler slices and do not clone the source-document vector during serialization. Ordinary solve reads neither file. Calculation Bundle materialization downloads them through explicit byte/SHA bounds and only then hydrates the source vector required by the existing bundle validator/writer. Review Submit persists bounded baseline/gate projections and never embeds source documents.

Cancellation is checked during raw merge, issue coalescing, graph reachability, partition writing, frozen-graph writing, TIDAS compression, and between bundle/report stages. A lease-heartbeat failure cancels the blocking task and waits for it to exit before returning. Temporary directories own all runs and artifacts, so success, cancellation, admission failure, crash recovery, and retry do not leave committed partial files. Object upload uses cancellable bounded file transfer; multipart cancellation aborts the remote upload.

Progress and qualification evidence distinguish records, logical bytes, compressed/artifact bytes, partition/artifact counts, and publication state. Linux evidence additionally records process-tree RSS and cgroup v2 `anon`, `file`, `memory.current`, and `memory.peak`; qualification records temporary bytes, descriptor count, and cache-reclaim evidence.

## Database #316 staged publication

Database owns durable registration and atomic visibility. Worker implements the shared `lcia.scope-closure-artifact-write-set.v2` fixture and digest contract:

1. Sort descriptors by UTF-8 `clientKey` and assign contiguous one-based ordinals.
2. Hash canonical compact JSON for `{"contractVersion":"lcia.scope-closure-artifact-write-set.v2","descriptors":[...]}`.
3. Create an idempotent header with the closure ID, Worker job and lease token, deterministic request ID, descriptor count/digest, required primary roles, staging lease, and optional reuse source.
4. Register deterministic batches of at most 500 descriptors. Re-read status after every batch.
5. Atomically seal only when the exact descriptor set is complete. Re-read `status=staging`, `uploadEligible=true`, and the exact bounded `clientKey -> artifactId` map.
6. Upload no object before the successful seal/readback fence.
7. Upload the locally staged files under deterministic request-scoped keys, then finalize atomically and require `status=ready` with the same artifact map.

Registration, seal, upload, and finalize failures call the fenced fail transition when possible. Uploaded objects are deleted best-effort after an observed failure; deterministic request IDs, keys, batch IDs, and database idempotency make retries converge. `ready` is the only publicly visible completed set. The legacy one-shot database operation is a compatibility adapter, not the v3 production path.

The shared fixture, digest, ordinals, states, and stable v2 error codes are recorded identically on Worker #177 and Database #316. Database's canonical migration fixture is the cross-repository source of truth for RPC signatures and result fields.

## Mandatory local qualification

Before merge, the v3 implementation must pass the Worker baseline gates and the scope-closure qualification in `docs/agents/repo-validation.md`. At minimum:

- the external executable emits the exact
  `lcia.scope-closure-external-result.v1` envelope and four real-payload
  `lcia.scope-closure-capacity-result.v3` mode directories; component SHAs,
  TIDAS/spool identity, source accounting, traversal counts, logical identity,
  artifact identity, and stable counts fail closed on any mismatch;
- the isolated-provider executable emits the exact
  `lcia.scope-closure-provider-result.v1` envelope only after receiving
  disjoint, positive `lcia.scope-closure-provider-owned-result.v1` fragments
  from exact git-tracked Database/Storage/Edge/Next adapters;
- no credential, signed URL, storage/database locator, package payload, private
  fixture content, or production fingerprint may enter a child result or
  retained log, and temporary/provider residue must be zero before output;

- qualification selects `real-payload` or `synthetic-cardinality` explicitly; generated cardinality is never real-package evidence;
- real-payload qualification bounded-reads every package document as its actual JSON value, accounts for every package file, and fails rather than silently skipping, replacing, or truncating a document;
- each administrative relation reports record count and p50/p95/p99/max logical and standalone-zstd record bytes, including the maximum exact identity; empty relations are explicit zero-count entries;
- generated, non-sensitive fixtures cover 32 MiB minus/equal/plus one byte, exactly 36,105,476 bytes, 64 MiB, incompressible content, Unicode/newlines, and oversized human-report fields;
- two complete runs over the external open-data package produce byte-identical non-summary artifacts, manifests, order, and logical hashes;
- native TIDAS completes within 60 seconds and 512 MiB peak RSS;
- the production-shaped closure completes within 10 minutes and 4 GiB process-tree peak RSS;
- 1×, 2×, 5×, and 10× production-distribution runs record wall time, RSS/cgroup breakdown, temporary bytes, artifact/partition bytes and counts, descriptor count, and cache reclaim;
- the unified issue set, blockers, verdict/certificate inputs, counts, root membership, and reconstructed witnesses are semantically equivalent to the pre-v3 result while physical expanded relations remain zero;
- cancellation, crash, and retry are exercised at coalesce, partition write, batch registration, seal, upload, and finalization boundaries, with no visible partial set, orphan, or local temporary leak.

The boundary qualification consumes the segmented-record representation owned by Worker #181 once that contract is available. The capacity harness must not duplicate or privately redefine that core chunk contract.

The package fixture and generated outputs stay outside git. Qualification never deploys, restarts a server, enqueues a production task, mutates production state, or updates the root workspace submodule pointer.
