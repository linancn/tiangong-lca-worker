---
title: Portal LCIA Projection Worker Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: en
whenToUse:
  - when changing lcia_result.package_build request.v3
  - when changing Portal LCIA Process, Impact, or Value records
  - when changing projection hashing, batching, lease fencing, or package binding
whenToUpdate:
  - when the V3 opt-in marker, record schemas, hash framing, staging protocol, or evidence binding changes
checkPaths:
  - docs/agents/contracts/portal-lcia-projection-contract.md
  - docs/agents/contracts/portal-lcia-projection-process.schema.json
  - docs/agents/contracts/portal-lcia-projection-impact.schema.json
  - docs/agents/contracts/portal-lcia-projection-value.schema.json
  - crates/solver-worker/src/portal_lcia_projection.rs
  - crates/solver-worker/src/calculation_bundle.rs
  - crates/solver-worker/src/db.rs
  - crates/solver-worker/src/queue.rs
  - crates/solver-worker/src/types.rs
  - docs/lca-api-contract.md
  - docs/agents/repo-validation.md
lastReviewedAt: 2026-08-26
lastReviewedCommit: 0093406327807bc62d9fe431aa1d33f6b049def6
lastReviewedNote: "Established the additive V3-only Portal LCIA materialization, cross-language hash, staging, and package-binding contract for Worker Issue #275."
related:
  - ../../../AGENTS.md
  - ../../../.docpact/config.yaml
  - ../repo-validation.md
  - ../repo-architecture.md
  - ../../lca-api-contract.md
---

# Portal LCIA Projection Worker Contract

## Boundary

Portal LCIA materialization is an additive branch of the certificate-bound LCIA result package job. Only a job with both of these exact fields opts in:

- `payload_schema_version = lcia_result.package_build.request.v3`
- `portalProjectionContractVersion = portal.lcia-projection.v1`

The matching hash marker is `portalProjectionHashContractVersion = portal.lcia-projection.int32be-frame-sha256.v1`. Request V1 and V2 reject either marker and continue through their unchanged calculation, artifact, and package-ready paths. Portal does not call Worker directly; Database owns durable projection rows and Release owns publication approval/finalization.

## Source evidence

The Worker derives the projection only while materializing the same verified Calculation Bundle used by the package:

- Process order and identity come from the certificate-bound `input_manifest.processes` and frozen Calculation Bundle Process axis.
- Method order and identity come from the frozen snapshot impact axis and exact reviewed LCIA Method source documents.
- Values are streamed from the locally produced, compressed LCIA shards after compressed and uncompressed byte-size, SHA-256, record-count, range, and Cartesian-order verification.
- Process and Method document SHA-256 values come from the immutable snapshot source closure, never mutable solve-time database reads.
- Artifact binding includes input manifest, closure certificate, numerical snapshot, closure bundle, snapshot index/build contract, Calculation Bundle content/manifest, LCIA chunk set, result artifact, and query artifact hashes.

A missing functional unit, reference Flow, geography, reference year, Method name, unit, source document, grid cell, or exact identity fails the V3 package build. Missing numerical values are never converted to zero; an explicit finite zero remains the canonical string `"0"`.

## Typed records

The three governed Draft 2020-12 schemas are:

- `portal-lcia-projection-process.schema.json`
- `portal-lcia-projection-impact.schema.json`
- `portal-lcia-projection-value.schema.json`

Indices are zero-based on Process and Impact axes. Dense Value ordinals are one-based and satisfy:

```text
ordinal = processIndex * impactCount + impactIndex + 1
```

Decimal fields are finite binary64 values converted to shortest round-tripping fixed notation, with no exponent, plus sign, trailing fractional zero, negative zero, or more than 38 ASCII digits. Localized arrays contain 1–64 unique, lowercase, sorted language tags; each trimmed value contains 1–4096 Unicode scalar values. Plain or untagged legacy source text becomes language `und` before normalization.

Projection records and Database RPC payloads contain no object-store locator, credential, actor, team, or review field. Private Calculation Bundle and result locators remain in their existing artifact contracts and are never copied into the typed projection.

## Hash contract

Every scalar hash field uses UTF-8 bytes framed by a signed network-order int32 length. SQL/Rust `NULL` is length `-1`; an empty string is length `0`. Domain markers and field order are part of every record, relation, grid, content, and publication hash. The fixed cross-language vector is:

```text
["A", "é", NULL, ""]
=> 5a01047a86055adc7954e7411667d0ef91c64f0c9ff4550dce738aa4d2f4a6ea
```

Process, Impact, and Value relation hashes frame their declared record count followed by `(one-based relation ordinal, record hash)` pairs. The grid relation binds all three relation hashes and the ordinal formula. The final content hash also binds every source/artifact hash and exact axis/value count. JSON serialization, object key order, locale, platform endianness, and storage path do not participate.

## Database staging

Worker uses the Database-owned service RPC sequence under the active V3 job lease:

1. fetch and compare the authoritative V3 Worker input;
2. begin one projection stage with the exact job/lease, counts, and source hashes;
3. register typed batches containing at most 500 records and at most 1 MiB of serialized UTF-8 JSON;
4. heartbeat between batches;
5. read back accumulated counts;
6. seal only after Database independently validates positional Process/Method identities, dense grid completeness, every record hash, relation hashes, and content hash;
7. compare Database seal/status hashes byte-for-byte with the Worker spool;
8. call the V3-only package-ready RPC.

The same batch is safe to replay after response loss. A reused ordinal/identity with different content conflicts. Lease loss, status drift, missing rows, hash drift, or an unavailable RPC fails closed. Worker makes one bounded response-loss retry after status readback and records a locator-free best-effort stage failure when the stage is still writable.

## Package binding

Before V3 package ready-marking, Worker adds these private artifact-manifest fields:

- `bundleContentHash`
- `bundleManifestSha256`
- `lciaChunkSetSha256`
- `portalProjectionId`
- `portalProjectionContentHash`

Database rechecks them against the sealed typed rows and exact package evidence. This persistent binding disambiguates lease retries and prevents Release from selecting another prepared projection with equal counts but different content. The fields are private evidence, not a public locator surface.

## Required proof

Minimum local proof is:

```bash
cargo test -p solver-worker --lib
cargo check -p solver-worker --all-targets --all-features
cargo fmt --all -- --check
```

Focused proof must cover the fixed framing vector, decimal limits, explicit zero, source context, missing/reordered/tampered grids, 500-record/1-MiB batches, exact V3 schema gates, authoritative Worker input, response-loss replay, status/seal hash comparison, and V1/V2 non-opt-in behavior. Cross-repository completion additionally requires the matching Database migration/pgTAP contract, Release publication workflow, and an isolated non-production Worker↔Database integration run; mock-only RPC tests are not sufficient for that final gate.
