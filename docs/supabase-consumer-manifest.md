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

The source reader uses the immutable Git object database. Included symlinks,
gitlinks, or other non-regular entries fail closed. The checked-in manifest is
also opened only after `lstat` confirms it is a regular non-symlink file.

Verify the checked-in candidate:

```bash
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
