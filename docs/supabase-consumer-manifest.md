---
title: Worker Supabase Consumer Manifest
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: en
whenToUse:
  - when database-engine needs exact Worker consumer evidence before a schema freeze
  - when Worker SQL, PostgREST, PGMQ, Cron, Realtime, or dynamic SQL helpers change
whenToUpdate:
  - when the audited Worker source commit changes
  - when the consumer derivation or manifest schema changes
checkPaths:
  - contracts/supabase-consumer-manifest.v3.json
  - contracts/supabase-consumer-manifest.v3.schema.json
  - scripts/check_supabase_consumer_manifest.py
  - scripts/test_supabase_consumer_manifest.py
  - scripts/requirements-supabase-consumer-manifest.txt
  - crates/**
  - scripts/**
  - tools/**
  - .github/workflows/supabase-consumer-manifest.yml
lastReviewedAt: 2026-08-02
lastReviewedCommit: cabb2518a69272c20abe61692eadb292b95596f2
lastReviewedNote: "Established the Issue #192 v3 candidate manifest and database-owned exact-bytes acceptance boundary."
related:
  - ../AGENTS.md
  - agents/repo-validation.md
---

# Worker Supabase Consumer Manifest

`contracts/supabase-consumer-manifest.v3.json` is a candidate snapshot of every
Supabase consumer occurrence derived from the exact Git tree named by
`headCommit`. It is deliberately non-authorizing: database-engine must read the
exact manifest bytes from an immutable Worker commit, independently run the
checker, bind the accepted bytes and commit into its freeze receipt, and run the
real joint Supabase contract suite before any schema freeze or DDL is authorized.

The manifest never reports a trusted count. The checker derives occurrences
from source, compares the full occurrence sets in both directions, and only then
prints a derived count. Each occurrence records file, line, operation,
transport, credential profile, schema, object, signature, and source class.
Dynamic SQL entrypoints are first-class occurrences even when their object name
is constructed by a helper; a variable or arbitrary string passed to SQLx is
therefore not an inventory bypass.

Each occurrence also binds an exact start/end span, the matched source-text
SHA-256, normalized semantics and upstream, required capability, and applicable
credential/ACL privilege. The Rust parser treats `query_with` variants,
qualified SQLx calls, and direct `Executor::execute` or pool/connection execute
calls as SQL entrypoints. Any non-literal first SQL argument becomes an explicit
`dynamic-sql` residue with pending independent review; it cannot disappear as
zero findings.

The manifest binds the canonical JSON Schema path and exact schema SHA-256. The
checker opens that no-follow regular file, requires canonical bytes, validates
the schema itself as Draft 2020-12, and validates the manifest through it.
Canonical origin fields are fixed to this repository and `origin/main`; the
source commit must be reachable from that canonical ref.

`residue`, `pending`, and `absenceProof` are independently reconstructed from
the occurrence set. A covered scanner with zero findings is recorded as
`covered-no-findings`; an unimplemented scanner is recorded as `not-covered`
and remains pending. This candidate reports webhook scanning as not covered
rather than misrepresenting it as zero.

The source reader uses the immutable Git object database. Included symlinks,
gitlinks, or other non-regular entries fail closed. The checked-in manifest is
also opened only after `lstat` confirms it is a regular non-symlink file.

`headCommit` is the immutable `sourceTreeCommit`, not the delivery commit. On
every verification run, the checker resolves the current exact Git `HEAD` as
`deliveryHead`, requires `sourceTreeCommit` to be its ancestor, and compares
the complete path/mode/type/blob identity for every source path at both ends.
Only `scripts/check_supabase_consumer_manifest.py` and
`scripts/test_supabase_consumer_manifest.py` are exempt so that the audit guard
can be delivered without self-reference. The exact allowlist and comparison
policy are part of the manifest `source` contract and JSON Schema. No broad
`scripts/**` exemption exists: any other matching Rust, Python, or shell source
addition, deletion, rename, mode change, or byte change fails closed.
The `source.governedSourceTreeSha256` binding is the SHA-256 of the canonical
filtered projection of every governed path, mode, Git object type, and blob OID.
The checker independently recomputes it at both `sourceTreeCommit` and
`deliveryHead`; both ends must equal the embedded digest.

All authority fields are booleans and all remain `false`: the artifact cannot
authorize a database freeze, merge, deployment, hosted mutation, or production
use.

Verify the checked-in candidate:

```bash
python3 -m pip install -r scripts/requirements-supabase-consumer-manifest.txt
python3 scripts/check_supabase_consumer_manifest.py
python3 -m unittest scripts/test_supabase_consumer_manifest.py
```

Generate candidate bytes for an explicitly chosen base/head without trusting a
previous manifest:

```bash
python3 scripts/check_supabase_consumer_manifest.py \
  --generate --base <full-commit-sha> --head <full-commit-sha>
```

Generation writes canonical JSON to stdout. Review and place those exact bytes
at the manifest path; do not add self-reported counts or authorization flags.
