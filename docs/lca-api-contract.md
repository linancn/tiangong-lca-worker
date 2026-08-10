---
title: LCA API Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要共享的 jobs/results/payload/status 契约时
  - 当 edge-functions 或前端的集成行为依赖 worker runtime 输出语义时
whenToUpdate:
  - 当 job payload、状态机、结果 artifact、幂等规则或服务端权限边界变化时
checkPaths:
  - docs/lca-api-contract.md
  - docs/scope-closure-contract.md
  - docs/provider-linking.md
  - AGENTS.md
  - .docpact/config.yaml
  - crates/**
  - supabase/migrations/**
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
lastReviewedAt: 2026-08-10
lastReviewedCommit: 1de9c777b57b034c2b703ceedabd692526bb4fd0
lastReviewedNote: "Updated for Worker Issue #247: result packages publish ordered canonical impact IDs and a normalized default from the frozen snapshot axis."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/provider-linking.md
  - docs/scope-closure-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/review-submit-fast-gate-contract.md
  - docs/edge-function-integration.md
  - docs/frontend-integration.md
  - docs/agents/repo-validation.md
  - docs/agents/contracts/scope-closure-memory-and-result-contract.md
---

# LCA API Contract (Snapshot-First)

本文档定义本项目当前可用的作业/结果契约，供 Edge Function 与前端共用。

## 1. 范围与原则

- 数值核心固定为 `M = I - A`，只解 `M x = y`。
- `snapshot_builder` 对 elementary flow 的 `B` 采用 `gross` 口径（`Input/Output` 均按原始 `amount` 入模，不做方向符号翻转）。
- 计算入口是异步任务；默认统一队列路径使用 `worker_jobs(worker_queue=solver)`，legacy `lca_jobs` + `pgmq` 仅保留为显式兼容/debug 路径。前端不直连队列。
- worker 连接池可通过 `DB_MAX_CONNECTIONS`、`DB_MIN_CONNECTIONS` 和 `DB_ACQUIRE_TIMEOUT_SECONDS` 调整；默认采用 `max_connections = 8`、`min_connections = 1`、`acquire_timeout = 30s`、`idle_timeout = 5min` 与 `max_lifetime = 30min`，以保证长时求解与 artifact 落盘阶段有稳定连接窗口。
- 主路径读取 `lca_snapshot_artifacts`（artifact-first），旧 `lca_*_entries` 仅兼容回退。
- 所有写操作由服务端（Edge Function / worker，`service_role`）执行。

## 2. 关键表与职责

- `lca_network_snapshots`: snapshot 元信息（含 `source_hash`）。
- `lca_snapshot_artifacts`: snapshot 矩阵 artifact 元信息（`snapshot-hdf5:v1`）。
- `worker_jobs`: canonical worker 生命周期表；solver 队列任务使用 `worker_queue=solver`，用于服务端任务中心、operator 查询、lease fencing、状态、错误、进度和 result projection。
- `lca_jobs`: optional retained LCA domain/history 兼容表，用于历史诊断和 legacy pgmq/debug 路径；统一 `worker_jobs` 路径不得要求该表存在。
- `lca_results`: 作业结果主表（仅 artifact 元数据 + diagnostics）。
- `lca_active_snapshots`: 各 scope 的当前生效 snapshot 指针。
- `lca_result_cache`: 请求级缓存/去重状态。
- `lca_factorization_registry`: 分解状态注册表（当前 schema 已就绪，运行时待接入）。

## 3. 作业类型与 payload

legacy `lca_jobs.job_type` 与 worker payload `type` 必须一致。`worker_jobs` 路径使用 `job_kind` 表达统一队列类型，并在 worker runtime 内部映射回同一组 legacy payload `type`，从而复用既有求解和 artifact 持久化逻辑。

支持类型：

- `prepare_factorization`
- `solve_one`
- `solve_batch`
- `solve_all_unit`
- `invalidate_factorization`
- `rebuild_factorization`

### 3.1 `prepare_factorization`

```json
{
  "type": "prepare_factorization",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "print_level": 0.0
}
```

### 3.2 `solve_one`

```json
{
  "type": "solve_one",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "rhs": [0.0, 1.0, 0.0],
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  },
  "print_level": 0.0
}
```

### 3.3 `solve_batch`

```json
{
  "type": "solve_batch",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "rhs_batch": [
    [1.0, 0.0, 0.0],
    [0.0, 1.0, 0.0]
  ],
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  },
  "print_level": 0.0
}
```

### 3.4 `solve_all_unit`

```json
{
  "type": "solve_all_unit",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "solve": {
    "return_x": false,
    "return_g": false,
    "return_h": true
  },
  "unit_batch_size": 128,
  "print_level": 0.0
}
```

说明：

- worker 会按 `unit_batch_size` 分块构造单位需求向量（每个 process 一条 `amount=1`）。
- 为控制结果体积，`solve_all_unit` 仅支持 `return_h=true` 且 `return_x/return_g=false`。
- 对调用者仍只返回/保留 H；worker 内部会在固定 256-process artifact chunk 内临时请求 `x+h`，用 snapshot 的 exact directional biosphere evidence 计算 LCI，写完该 chunk 后立即释放 `x`。完整 `x`、`G` 或 directional LCI 矩阵不会在内存中跨 chunk 聚合。
- 每个成功的新 `solve_all_unit` 同时生成不可变 `tiangong.calculation-bundle.v2`。旧 snapshot 若没有 exact signed-flow release evidence，任务以明确错误要求重建 snapshot；worker 不从 A/B 或数据库当前态猜测 exchange identity。

### 3.5 兼容字段

worker 反序列化时 `model_version` 仍可作为 `snapshot_id` 的别名（兼容旧请求）。新实现应只发 `snapshot_id`。

### 3.6 `worker_jobs` solver 队列映射

solver worker 默认使用 `SOLVER_QUEUE_BACKEND=worker-jobs` / `--queue-backend worker-jobs` 的 `public.worker_jobs` claim 模式。该模式只领取 `worker_queue=solver` 的 LCA solve jobs。`SOLVER_QUEUE_BACKEND=pgmq` / `--queue-backend pgmq` 仅用于 legacy 兼容/debug，且必须显式设置 `ALLOW_LEGACY_JOB_TABLE_BACKEND=true` 或传入 `--allow-legacy-job-table-backend`；生产 worker 应保持关闭。

| `worker_jobs.job_kind` | `payload_schema_version` | legacy payload `type` | result schema |
| --- | --- | --- | --- |
| `lca.solve_one` | `lca.solve_one.request.v1` / `lca.solve_one.request.v2` | `solve_one` | `lca.solve.result.v1` |
| `lca.solve_batch` | `lca.solve_batch.request.v1` | `solve_batch` | `lca.solve.result.v1` |
| `lca.solve_all_unit` | `lca.solve_all_unit.request.v1` / `lca.solve_all_unit.request.v2` | `solve_all_unit` | `lca.solve.result.v1` |
| `lca.build_snapshot` | `lca.build_snapshot.request.v1` / `lca.build_snapshot.request.v2` | `build_snapshot` | `lca.snapshot.result.v2` |
| `lca.contribution_path` | `lca.contribution_path.request.v1` / `lca.contribution_path.request.v2` | `analyze_contribution_path` | `lca.contribution_path.result.v1` |
| `lca.factorization_prepare` | `lca.factorization_prepare.request.v1` | `prepare_factorization` | `lca.factorization_prepare.result.v1` |
| `lcia_result.package_build` | `lcia_result.package_build.request.v1` | `lcia_result_package_build` | `lcia_result.package_build.result.v1` |
| `lcia.scope_closure_check` | `lcia.scope_closure_check.request.v1` | `scope_closure_check` | `lcia.scope_closure_check.result.v1` |

`worker_jobs.payload_json` may use the legacy snake_case fields above, or Edge-friendly camelCase aliases such as `lcaJobId`, `snapshotId`, `rhsBatch`, `unitBatchSize`, `processId`, `impactId`, `requestRoots`, `noLcia`, `buildId`, `requestedBy`, `inputManifest`, `inputManifestHash`, `lciaMethodSet`, and `defaultImpactCategory`. Payloads must still carry a valid `lcaJobId` / `job_id` compatibility UUID when the task writes `lca_results`、`lca_result_cache`、`lca_latest_all_unit_results` 或 `lca_factorization_registry` rows keyed by historical `job_id` columns. 这些 columns 不再要求 `public.lca_jobs` FK 或 parent row。

### 3.7 `public_plus_owner_draft` versioned calculation contract

`lca.build_snapshot.request.v2` is reserved for the private-incubation scope `public_plus_owner_draft`. It must carry the complete Edge-produced contract; the worker does not infer or default omitted fields:

- `all_states=false`, `process_states="100"`;
- `include_user_id=<authenticated actor>`, `include_user_state_codes="0"`;
- `include_user_unassigned_only=true`, `include_user_review_free_only=true`;
- `scope_manifest` using `lca.data_scope.manifest.v1` and its canonical `scope_manifest_sha256`;
- `lcia_method_factor_source` using `lca.method_factor_source.request.v2`, the exact relative path `lciamethods/cache_manifest.json`, the reviewed raw-manifest SHA-256, the complete identical embedded `lcia.static_cache_bundle.v1` manifest, `base_url_binding=worker_trusted_configuration`, and snapshot evidence schema `lca.method_factor_source.snapshot.v2`;
- `lcia_factor_coverage_contract` using `lcia.method_factor_coverage.contract.v2`, `count_unit=exchange_method_pair`, match dimensions `(method_id, method_version, flow_uuid, direction)`, and `missing_factor_semantics=incomplete_coverage_not_zero`;
- `no_lcia=false`.

The worker independently enforces the frozen predicate after queue decoding and again in the snapshot builder. The data scope applies only to processes and flows: they are eligible only when `state_code=100`, or when they are actor-owned `state_code=0` rows with both `team_id` and `review_id` null. Public states `101..199`, foreign drafts, owner nonzero rows, team drafts, and review drafts are rejected. LCIA methods are intentionally outside this actor-specific DB predicate.

LCIA methods and factors come from the reviewed 25-method static cache bundle shared with Next, not from `public.lciamethods`. Before a v2 job is accepted, the worker canonicalizes the embedded manifest and requires the pinned digest of the complete reviewed file; summary-only projections and any file, alias, provenance, count, or metadata drift fail during payload validation rather than after execution starts. The worker reads assets only from `LCIA_STATIC_CACHE_BASE_URL` (HTTPS, except loopback test HTTP) or `LCIA_STATIC_CACHE_DIR`; request payloads cannot override the base. It then verifies the fetched raw manifest against the request hash and embedded object, verifies raw/compressed/decompressed/canonical hashes and declared byte sizes under hard limits, validates the one known method/locator alias, rejects non-finite factors, and streams the 62.6 MB decompressed factor map instead of materializing a full JSON tree. Versioned directional builds prune only exact zero for static characterization factors and biosphere exchanges, so finite sub-epsilon values remain calculable; derived non-finite exchange amounts remain invalid coverage gaps and never enter A/B/C, while finite B aggregation overflow fails closed. `lca.method_factor_source.snapshot.v2` binds the bundle manifest, source/method/factor/identity hashes, bundle version, and exact method count. The source fingerprint includes this actor-independent proof, so bundle drift cannot reuse a prior snapshot.

Factor coverage is counted for every selected method/exchange pair. Counts are `matched`, `unmatched`, `invalid`, and `unsupported_direction`, both overall and in `by_method`. A flow characterized by one method is still unmatched for methods that lack that flow/direction factor; numeric C-matrix behavior is unchanged, but missing factors can no longer masquerade as complete zero impact. An incomplete build writes deterministic `lcia-uncharacterized-jsonl:v2` records containing `method_id`, `method_version`, `artifact_locator_id`, `flow_uuid`, `flow_version`, `direction`, `exchange_id`, `amount`, and `reason`. The worker spools records to a local file in method/flow/exchange order, hashes and counts while writing, and uploads by file/multipart so evidence size is not proportional to in-memory objects. It fails closed instead of truncating at 25,000,000 records or 8 GiB, before publishing a snapshot index with incomplete evidence.

`snapshot-index-v1.json` carries top-level `calculation_evidence` (`lca.calculation_evidence.v2`) with the exact scope hash, `lca.method_factor_source.snapshot.v2`, and the single coverage truth source `lcia.method_factor_coverage.matrix.v1`. The matrix repeats the source hashes, count unit and key dimensions, binds `by_method` identities to the reviewed 25-method identity-manifest hash, requires every method row to have the same nonzero exchange-pair cardinality, requires global totals to equal per-method cardinality × 25, and rejects counts above JavaScript's safe-integer maximum. Complete coverage requires zero gap counts and a null evidence artifact. Incomplete coverage requires `coverage_status=incomplete_coverage`, a verified artifact, and `record_count = unmatched + invalid + unsupported_direction`.

For this scope, `lca.solve_one.request.v2`, `lca.solve_all_unit.request.v2`, and `lca.contribution_path.request.v2` must carry `calculation_evidence_binding` equal to the snapshot-index evidence. The worker rejects missing, malformed, or drifted bindings before factorization/solve. A v1 solve against a bound snapshot is rejected, so the contract cannot silently downgrade. Successful scoped results repeat `calculation_evidence` in `lca_results.diagnostics` and job diagnostics; numeric trial results with gaps remain explicitly marked `incomplete_coverage`.

`lcia_result.package_build` 不是普通求解 API 的用户请求类型，而是 data product manager command 创建的后台构建任务。payload 必须来自数据库/Edge 的 service-role command 边界，包含 `buildId`、`requestedBy`、published-only `inputManifest`、`inputManifestHash`、`coverageMode`、`eligibleInputCount`、`includedInputCount`、`lciaMethodSet` 和可选 `defaultImpactCategory`。worker 只接受 `inputManifest.processes` 中 `stateCode/state_code` 为 `100..199` 的已发布过程；不会纳入 draft data。

### 3.8 `lcia.scope_closure_check` 与 Build V2 证书绑定

`lcia.scope_closure_check` 只能由数据库命令边界创建。其 claim payload 是最小 envelope：`closure_check_id`、`scan_execution_id`、`data_snapshot_token`、`request_fingerprint`；完整 scope 和 `lcia.scope-closure-data-snapshot.v2` 必须通过 service-role worker-input RPC 读取。Worker 按当前 public release 的完整 exact dataset/hash allowlist 执行 union closure，不从 live state code 推断可用 universe。live-only identity、同 identity 内容漂移或 allowlisted row 不可读都会使 `scanCompleteness=incomplete`，且不能得到有效证书。

新建 certificate-grade Scope Closure 的 `linkPolicy.technosphereBoundaryPolicy` 固定冻结为 `cutoff`。数据库对兼容输入中的省略值、`closed`、`open` 和 `cutoff` 统一规范化后再计算 scope/policy/request hash；Worker 只接受冻结后的 `cutoff`，不会在运行时静默覆盖不同策略。非 canonical 输入在 scan/publication 前以 `scope_closure_boundary_policy_must_be_cutoff` 失败。`cutoff` 下 unmatched provider、A-write coverage 和 unresolved balance 继续进入 metrics、findings 与逐边 evidence，但不成为 certificate blocker；generic snapshot/readiness CLI 的三策略兼容面不变。

terminal result 使用 `lcia.scope_closure_check.result.v1`，并保留 `closureCheckId`、`status`、`scanCompleteness`、`certificateStatus/certificateHash`、report artifact reference 和 blocker codes。closure domain V3 RPC 原子落库精确 summary/count 与有界兼容明细：最多投影 5,000 个 issue summary，每个 issue 的 inline occurrences 和 affected roots 最多各 100 条，`issueDetailsTruncated`、逐 issue 截断状态与真实 count 必须同时保留。完整机器结果以 `lcia.scope-closure-issue-manifest.v4` 为权威：它保留 v3 的统一 issue、compact root-impact 与 frozen-graph witness 语义，并增加 documents、edges、resolution map、resolved references、roots、frontier、provider universe 与 omitted-version resolutions 的确定性有界分区。每个统一 issue 仍只有一条按 `issueKey` 排序的 `lcia.scope-closure-issue.v3` 主记录，覆盖 TIDAS、graph、frozen-source drift、snapshot source preflight、provider、matrix、factorization 与 LCIA-readiness 语义；原始 TIDAS NDJSON 只压缩保留一次；`expandedAffectedRootRecordCount=0`。version-dispatch reader 仍可读取历史 v2/v3。`closure-bundle-v4.json` 只保存证书/快照/package 所需的稳定 binding 与 relation logical hash/count，不再复制可增长数组；所有 scope-closure 对象在 write-set 创建前受 256 MiB 单对象硬上限约束。XLSX 的一般关系视图只保留摘要、artifact index 和有界样本，不能替代完整机器结果；但 snapshot source blocker 会从已校验 sidecar 完整写入专用拆分工作表。bundle、manifest、members 与 XLSX administrative evidence 都进入当前 check 的 report artifact manifest，并分别带 `closure_bundle`、`complete_machine_result` 或 `closure_report` role，以及实际 bucket/path、SHA-256、byte size、content type、`ready` lifecycle state 和可信数据库时间加七天的 expiry；queue 层不得随后重复调用普通 `worker_record_job_result`。`closure-snapshot-v1.json` 不再生成：blocked/incomplete 结果没有 numerical snapshot、numerical evidence hash 或 certificate。只有 complete 且零 blocker 的检查才把真实 `snapshot-hdf5:v1` 与 snapshot index 落入 `lca_network_snapshots` / `lca_snapshot_artifacts`，并从持久化 metadata 记录 `snapshotId`、HDF5 `snapshotHash`、`snapshotArtifactId`、`snapshotIndexSha256` 与 `snapshotBuildContractHash`。共享 scan reuse 只复用 immutable bound evidence，并为当前 closure check 新建 summary、XLSX、report manifest binding 和 certificate。

下载 descriptor 由 Database 的 actor-bound `get_lcia_scope_closure_report_download(uuid,text)` 投影；selector 只能是 `closure_report_xlsx` 或 `closure_issue_manifest`。成功响应固定为 11 个字段：`artifactId`、public `artifactRole`、`artifactState`、semantic filename、`format`、`mediaType`、`size`、`checksumSha256`、`artifactExpiresAt`、`bucket`、`objectPath`。Database #309 保留 owner summaries、临时单参数 overload、可信 expiry 与 fenced GC；Database #316 权威定义 `lcia.scope-closure-artifact-write-set.v2` staged publication：one-based descriptor ordinals、canonical descriptor digest、最多 500 条的 bounded registration、status readback、atomic seal、精确 `clientKey -> artifactId` map 与 finalize。Worker 在 seal 后读到 `staging/uploadEligible=true` 之前不得上传对象，只有 finalize 后的 `ready` write set 才可见。本批 UI/API 仍只直接下载 XLSX 和 manifest；manifest 列出的 members 构成完整可恢复结果，但逐 member 的客户端下载授权需要另一个 tracked cross-repo contract。Worker 不新增 archive、selector、actor ACL 或 public DTO 映射来绕过该边界。

数据库 Build V2 command 原子创建 `lcia_result.package_build` job，并返回权威十一字段 closure binding：`closure_check_id`、`closure_certificate_hash`、`effective_scope_hash`、`data_snapshot_token`、`snapshot_id`、`snapshot_hash`、`snapshot_artifact_id`、`snapshot_index_sha256`、`snapshot_build_contract_hash`、`closure_bundle_artifact_id`、`closure_bundle_hash`。`report_artifact_manifest_hash` 仍保留在 job payload 和 certificate audit evidence 中，但不能替代 closure bundle 的精确 artifact identity。Worker 对权威 binding 和请求 manifest 执行全量相等校验，并调用 Database-owned bundle-binding predicate 统一验证 fresh 与单层 direct reuse：复用 artifact 的 `metadata.closureCheckId` 继续指向直接来源检查，最终 Calculation Bundle、result JSON、result_ref、package metadata 与 audit 则绑定当前目标检查；Worker 不得把二者强制为同一 ID。Worker 在运行数值构建前按精确 artifact ID 通过 bounded file API 下载、重算哈希，以流式 JSON binding reader 支持历史 v1/v3 和当前 v4，逐项核对当前 passed/complete/valid certificate、HDF5 embedded binding、exact ordered Process axis、snapshot index 与 build-contract hash；在最终 ready RPC 前再次验证对象内容，DB 再执行 lease、revocation、metadata freshness 的最终检查。它直接消费已签名 snapshot/evidence，不重复运行 administrative closure。数值 snapshot/all-unit solve/artifact 路径保持原样。

完整 traversal、artifact、reuse 和 failure 契约见 `docs/scope-closure-contract.md`。

TIDAS system failures 在 Worker diagnostic / request-cache boundary 使用稳定 code：`tidas_binary_unavailable`、`tidas_version_mismatch`、`tidas_protocol_mismatch`、`tidas_handshake_failed`、`tidas_timeout`、`tidas_report_invalid`、`tidas_spool_invalid`、`tidas_execution_failed`。这些 code 表示 Worker 无法获得完整可信的 validator evidence，不得转换为 domain blocker、成功 certificate 或部分 import。大 package import report 只保留确定性有界 issue sample，同时保存完整 issue/severity counts；spool 自身按流式 SHA-256/bytes/event count 验证，不进入 API payload。具体 report/spool 契约分别见 scope-closure 与 TIDAS package contract。

该 package build 的 legacy database-backed LCIA 路径在读取 factors 前必须把每个方法归一化为 `(canonical method UUID, exact version, artifact locator UUID)`：文档内 `common:UUID` 是 matrix axis、calculation evidence、result key 和 source-closure identity，`public.lciamethods.id` 只作为精确读取该文档的 artifact locator。Locator 与文档 UUID 相同时可直接使用；不相同时必须与 reviewed `RELEASE_METHOD_IDENTITIES` 中的完整三元组精确匹配，否则在矩阵构建前 fail closed。单方法选择若使用 canonical UUID，worker 可通过同一 reviewed mapping 定位 locator，但写入 snapshot/result 的仍是 canonical UUID。

On success, the worker records a terminal `worker_jobs` result with:

- `result_json.lcaJobId`
- `result_json.workerJobId`
- `result_json.snapshotId`
- `result_json.resultId` when a `lca_results` row was produced
- `result_ref = {"domainSource":"worker_jobs","workerJobId":"<uuid>","lcaJobId":"<uuid>","result":{"table":"lca_results","id":"<uuid>"}}` for solve/result-producing jobs
- `diagnostics.lcaJob = {"id":"<uuid>","projectionSkipped":true}` and `result_json.lcaJobStatus = null` are non-querying compatibility placeholders; canonical result diagnostics come from `worker_jobs` and domain result tables, and the `worker_jobs` runtime does not query optional `lca_jobs`

`build_snapshot` is projected independently from `worker_jobs.diagnostics.build_snapshot_result`: it always returns the resolved snapshot ID (including reuse) and scoped calculation evidence without reading optional `lca_jobs`. Its `result_ref.snapshot` points at the resolved `lca_network_snapshots` row.

Singular/factorization failure diagnostics load the exact `(process_id, process_version)` pairs from `snapshot-index.process_map`. Duplicate-exchange and service-loop scans join only those pairs; they do not reconstruct scope from broad owner/state filters.

On success or failure, the worker links `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, and `lca_factorization_registry` rows back to the canonical `worker_jobs.id` where those rows exist. The canonical path never probes or backfills optional `lca_jobs`. On failure, the worker records `worker_jobs.status=failed` with `error_code=solver_worker_job_failed` and updates `lca_result_cache` failed state where a cache row exists. Retained `lca_jobs.status/diagnostics` writes are limited to the explicitly enabled legacy pgmq/debug backend.

For `lcia_result.package_build`, worker builds a published-only snapshot using the package `buildId` as the requested snapshot/result compatibility key, computes and persists the all-unit LCIA result artifact plus query artifact, then calls service-role RPC `public.cmd_lcia_result_package_mark_ready(...)`. The ready projection persists `availableImpactCategories` from the frozen snapshot impact axis as ordered canonical impact UUIDs; `defaultImpactCategory` is normalized from either a requested canonical UUID or frozen impact key to the matching canonical UUID, and defaults to the first frozen impact when omitted. An empty impact axis or a requested default outside that axis fails closed instead of publishing an empty category list. Success `result_ref` uses `{"domainSource":"worker_jobs","workerJobId":"<uuid>","buildId":"<uuid>","package":{"table":"lcia_result_packages","id":"<uuid>"}}`; failures use package-specific error codes and do not update `lca_result_cache` or optional legacy `lca_jobs`.

## 4. 作业状态机

legacy `lca_jobs.status` 允许值：

- `queued`
- `running`
- `ready`
- `completed`
- `failed`
- `stale`

legacy pgmq/debug 路径语义：

- `prepare_factorization`: `queued -> running -> ready`。
- `solve_one` / `solve_batch` / `solve_all_unit`: `queued -> running -> completed`。
- `invalidate_factorization`: 通常直接 `completed`。
- 失败路径统一落 `failed`，错误详情在 `lca_jobs.diagnostics`。

`worker_jobs` 路径的外层生命周期是 `queued/stale -> running -> completed|failed|cancelled`。`phase` 使用 `solve_one`、`solve_batch`、`solve_all_unit`、`build_snapshot`、`analyze_contribution_path`、`prepare_factorization` 或 `lcia_result_package_build`，`progress` 仅作为任务中心提示，不替代 domain artifact 状态。

## 5. 结果契约

`lca_results` 一行对应一次完成的求解任务（通常 `solve_one`/`solve_batch`/`solve_all_unit`），当前为 **S3-only**：

- 不再存 inline `payload`
- 必须写入 `artifact_url` / `artifact_sha256` / `artifact_byte_size` / `artifact_format`
- 当前 `artifact_format = hdf5:v1`
- 附加 retention 字段：`expires_at` / `is_pinned`
- `diagnostics.calculation_evidence`：versioned scoped snapshots 必须为非空，并与 snapshot-index binding 完全一致；legacy snapshots 为 `null`

`snapshot` artifact 当前格式：`snapshot-hdf5:v1`。

`solve_one` / `solve_batch` 继续把数值 payload 写入 `hdf5:v1`。`solve_all_unit`
例外：完整结果只存在于 Calculation Bundle manifest/partitions；`lca_results` 的
`hdf5:v1` 是一个 bounded compatibility descriptor，只包含 canonical bundle 与
query-index references，不包含 `SolveBatchResult.items` 或完整 `h_matrix`。

### 5.1 Calculation Bundle v2

`solve_all_unit` 的 canonical release evidence 是 manifest + deterministic gzip NDJSON sidecars：

```text
calculation-bundle.json
source/source-closure.ndjson.gz
axes/processes-000000.ndjson.gz
axes/inventory-000000.ndjson.gz
graph/technosphere-000000.ndjson.gz
graph/biosphere-000000.ndjson.gz
results/lci-000000.ndjson.gz
results/lcia-000000.ndjson.gz
evidence/coverage.json
```

- 每个 canonical chunk 固定覆盖最多 256 个连续 process index；NDJSON record 使用 canonical JSON 和单个换行，gzip 固定 level 6、mtime 0、无文件名/comment。
- manifest 的 `artifacts[]` 按 path 排序，记录 compressed/uncompressed SHA-256、byte size、record count 与 process-index boundary；`bundleContentHash` 不包含生成时间、对象存储 URL 或自身 hash。
- `all-unit-query:v2` 是确定性 metadata/index view：记录 Calculation Bundle manifest identity，并按 `firstProcessIndex` 排序列出 LCIA chunk path/schema/compression/hash/bytes/record count/range。调用方按所需 range 读取 partitions；该 artifact 不包含 `h_matrix`。
- `source/source-closure.ndjson.gz` 使用 `tiangong.source-closure.bundle.v1`，每条 `tiangong.source-closure.dataset.v1` record 固化 dataset type、role、TIDAS UUID/version、目标包内 path、canonical document SHA-256 与完整 TIDAS JSON。Process role 固定为 `unit_process`，其余为 `support`；它是 Calculation Bundle 的原始输入证据，不是由结果反推的派生视图。
- process axis 固化 Process UUID/version 和唯一 quantitative reference 的 exchange internal ID、Flow UUID/version、reference unit、raw direction/amount/coefficient 与 signed normalized pivot；inventory axis逐 exchange 保存 raw/signed/normalized coefficient、allocation target 与 selected fraction。Snapshot flow axis 使用 `(Flow UUID, resolved version)`；同一 UUID 的多个实际引用 revision 可共存并获得独立连续 `flow_idx`，未被最终 process closure exchange 引用的 revision 不进入矩阵。LCIA factor 只有在其 Flow 与 inventory-derived biosphere axis 相交且实际进入 C 时才成为数值 source-closure 依赖；off-axis factor 不创建额外 Flow axis 或 support root。
- fresh snapshot 不持久化完整 `CompiledGraph`。普通 `snapshot-hdf5:v1` 只保存 numerical payload/config/coverage，并通过 URL、SHA-256、byte size、format 与 content type descriptor 绑定 `snapshot-release-evidence-json-zstd:v2`；该元数据再以同样的 integrity descriptor 和 dataset count 绑定内容寻址的 `snapshot-source-closure-json-zstd:v1`。release metadata 只保存 Calculation Bundle 所需 process/inventory/edge/provenance 字段，完整 TIDAS `source_datasets` 只出现于 source closure，两个编码器均借用 compiler data 流式写入文件并通过 bounded multipart path 上传。常规 solve 只读取 HDF5；只有 Calculation Bundle materialization 才沿两级 descriptor chain 下载并 fail-closed 校验。历史 HDF5 的 numerical payload 与 compiler metadata 分开解码：即使旧 graph schema 已漂移，普通 solve 仍可读取矩阵；schema-compatible 的内嵌 `compiled_graph.release_evidence` 和 transitional v1 full-evidence sidecar 继续兼容，incompatible release evidence 则明确要求重建 snapshot。review-submit baseline/overlay 按 review contract 分别保存 baseline projection 与最小 gate projection，也不持久化 compiler IR。Scope Closure 的 LCIA method axis 只来自冻结请求 manifest，不能退化为加载完整方法目录。source closure 从本次 snapshot 精确选择的 Process/inventory Flow revision、请求方法 identity 和 active C-factor selection 出发；只对实际进入 C 的 factor Flow reference 做 exact/once-resolved 校验与递归 support closure。off-axis、zero 或方向不适用的 factor 仍保留在已哈希方法文档中作为证据，但不 probe target、不生成 blocker。显式版本只允许 exact match，省略的 active support version 只确定一次并冻结；active target 的缺失、歧义、无效 UUID/version、非 Elementary 类型或同 identity/version 内容漂移均 fail closed。support documents 不在 solve 时重新查询。
- numerical source closure 复用 `scope_closure.rs` raw extractor，并通过
  `source-reference-policy.v4` 决定 artifact-purpose action。lineage / model-composition 只进入
  additive `release_evidence.source_reference_provenance` 的 count/hash/bounded sample，不 probe
  target，也不改变 Process/Flow axis、A/B/C、sparse payload 或 Calculation Bundle 的 unit-process
  source validator。administrative-support reference（例如 data-entry contact、ownership、dataset
  format、compliance、logo 与 provenance source）在数值 artifact 中尽力读取；缺失或空占位只进入
  bounded provenance evidence，不阻断计算。exchange/provider/Flow Property/Unit Group/LCIA 等数值
  或必需支持引用仍 fail closed。Certificate Closure 保持严格遍历。unknown Flow/Process path 是
  operator error。
- source preflight blocker 继续使用现有 `passed|blocked|error` 与
  `blockingReasons` / `calculatorReport` JSON。此 Worker contract 不新增数据库 status enum、
  migration 或 Edge/Next schema dependency；DB/S3/protocol/timeout/lease-loss 仍为 operator
  `error`。Canonical `worker_jobs` failure diagnostics 在 `snapshot_builder_blocked` 下保留 terminal
  已有的 bounded `blocking_reasons` sample、总数、SHA-256、truncated 标记与 sample count；不得把
  未截断的完整 blocker 集合写入任务结果。Scope Closure 调用会额外传入父进程拥有的 canonical
  NDJSON sidecar 路径；terminal V1 只追加 `recordCount`、`byteSize`、`sha256`、
  `collectionComplete` 描述符。Worker 完整校验后流式转成 Closure Issue，并在 blocked 结果中生成
  manifest、bundle 与包含完整 snapshot blocker 明细（超单表限制时拆表）的 XLSX；sidecar 缺失、
  损坏或不完整仍按 protocol `error` 失败。
- 若 exchange 的 Flow 引用省略 `@version`，release evidence 使用本次 snapshot 实际选择并冻结的 Flow metadata version；若引用显式给出版本，则 worker 精确查询并保持该版本绑定，且 unit metadata 只允许来自同一版本，不能静默回退到另一版本。同一 UUID 的不同显式 revision 作为不同 identity 参与 provider lookup 与 flow axis；任何缺失或不可见的 exact revision 都在 snapshot compilation 时 fail closed。
- 已审 LCIA 方法中 UUID 与 artifact locator 不同的已知 alias，无论来自 static cache 还是 legacy database-backed package build，都只允许 locator 用于精确读取源文档；source closure、LCIA axis 与最终发布始终使用文档内的 canonical method UUID/version。Calculation Bundle 接受证书 snapshot 冻结的任意非空已审方法子集，并保持 snapshot impact index 的精确顺序；子集中的任一 identity/version/locator/document UUID 不符合 25-method reviewed catalog mapping、重复或 index 不连续都 fail closed。
- technosphere evidence 使用中性字段固化 `dependent_process_idx`、`residual_exchange_internal_id`、`balancing_process_idx`、`balancing_reference_exchange_internal_id`、residual/reference coefficient、routing weight、activity requirement、Flow UUID/version 和 location；每个最终 balancing reference port 一条 edge。
- directional LCI key 固定为 Flow UUID/version + Input/Output + reference unit + optional location；LCIA 绑定已审查 static-cache bundle 1.2.4 中由证书 snapshot 精确选择的非空 method UUID/version 子集，而不是强制发布全部 25 个方法。
- object path 使用 `calculation-bundles/<calculation-id>/<bundle-content-hash>/...`，先上传 sidecars，最后上传 manifest。job diagnostics 的 `calculation_bundle`（package build 中为 `artifactManifest.calculationBundle`）保存 manifest URL/hash/byte size 和 bundle content hash。
- bounded `hdf5:v1` descriptor 与 `all-unit-query:v2` index 是兼容/查询视图；它们不是 canonical release evidence，也不得回退为完整矩阵驻留。旧 `all-unit-query:v1` artifacts remain readable only as historical artifacts; new solves never produce them。旧 snapshot 缺少 frozen `source_datasets` 时必须重建，禁止在 solve 或 release 阶段从数据库当前态补齐。

Factorization cache uses a configured hard retained-byte capacity. Entry estimates include CSC
vector capacities and UMFPACK-reported symbolic/numeric object sizes after workload-dependent
fill-in; prepare diagnostics expose admitted entry bytes, UMFPACK peak bytes, resident/capacity
bytes, hits/misses, evictions, invalidations, and admission rejections. Pre-factorization
admission applies deployment-tuned fill-in headroom to the concrete M/B/C workload. This is an
admission policy, not a promise of constant memory independent of sparse fill-in.

Snapshot 的 Process 身份契约是：一个完整 TIDAS Process revision 只对应一个 process index / 矩阵列，其 `quantitativeReference.referenceToReferenceFlow` 选择 signed normalization pivot。Reference 可以是 Input/Output、正/负 amount、任一 source flow type；有效性不由 Product/Waste 或方向决定。Non-reference exchange 不生成派生 Process；需要独立 activity pivot 时，上游必须提供另一个完整 Process revision。

Snapshot build 对 exchange allocation 使用 target-aware 语义：object/array 都按 `@internalReferenceToCoProduct` 匹配当前 quantitative reference，TIDAS `Perc` 统一除以 `100`；闭合 allocation vector 中缺少当前 target 表示稀疏零，完全未声明 allocation 表示 fraction `1`。Legacy compatibility 只允许两个有界 fallback：scalar `{}` 按 undeclared 处理；单个 targetless full allocation 仅在 reference exchange 和 internal ID 唯一时推断。方向不参与 target validity；multiple-entry targetless、多个 quantitative reference、坏 ID 或其他无效声明均 fail closed。Reference pivot 本身不乘 allocation fraction。

Build config 记录 `allocation_semantics_version = tidas-reference-allocation-v3`、`link_semantics_version = signed-flow-balance-v1`、`technosphere_boundary_policy`、`flow_identity_policy = exact-flow-version-reference-unit-v2`、`source_closure_policy = path-aware-bounded-frontier-v2` 与 `source_reference_policy = source-reference-policy.v4`；所有字段进入 source/review fingerprint，solver contract 也显式记录两项 source policy。Flow identity v2 只查询和编译最终 process closure exchange 实际引用的 exact Flow identities；source-closure v2 只为 selected LCIA-factor-only Elementary Flow 增加 support evidence，不扩大稀疏矩阵轴。`referenceToDigitalFile` 只是外部附件 URI，在 review-submit 和普通 Calculation Bundle 中不作为数据集引用校验。legacy artifact 仍可反序列化，但非 v4 policy artifact 不可作为 v4 reuse candidate；相同 v4 输入的 hash/reuse 必须稳定。

显式零或稀疏零 allocation 得到的 Input 不展开 provider closure、不产生 provider-gap diagnostics，也不写入 `A`；零 attributed elementary exchange 也不写入 `B`，不参与 LCIA direction 与 factor-coverage evidence。它只表示该 exchange 对当前 quantitative reference 没有 attributed burden。

Snapshot coverage diagnostics 暴露 signed-flow balance 与矩阵写入质量。当前 coverage schema 为 `snapshot_coverage.v3`，使用 `residual_edges_total` / `a_balance_edges_written` 作为中性计数，同时保留 provider/input compatibility fields。

Allocation summary 的兼容计数为：

- `allocation.legacy_empty_allocation_as_undeclared_count`：按 scalar `{}` legacy fallback 视为 undeclared 的 exchange 数。
- `allocation.legacy_single_reference_target_inferred_count`：在 reference exchange/internal ID 唯一时安全推断 full targetless allocation 的 exchange 数；旧 output 命名字段只作为 default-zero 兼容投影。

这两个字段用于审计 compatibility normalization，不产生新的通用 fallback，也不把其他 invalid allocation 降级为 warning。

Provider-link 的运行时决策顺序、默认 provider rule、candidate eligibility 和 provider diagnostics 维护在 `docs/provider-linking.md`。本文档只定义 worker/API 消费这些 coverage 与 artifact 字段的契约边界。

- `candidate_summary`：same-flow reference-port candidate 数量分布。只有 exact-flow、different-process、opposite-sign reference port eligible；same-sign reference 和 non-reference exchange 作为 rejected evidence。
- `resolution_summary`：resolved strategy 与 unresolved reason 分布。
- `geography_summary`：地理层级、strategy × geography tier、supply-region anchor 来源、exchange location 覆盖情况和 location 粒度分布。
- `volume_weight_summary`：基于 `annualSupplyOrProductionVolume` 的权重数据可用性与 fallback-to-one 情况。
- `gap_summary`：no-provider gap 的 top flows 与 top processes。

`provider_decision_diagnostics` 中与 reference-output eligibility 相关的字段包括：

- `candidate_eligibility_counts`：provider output evidence 按 `accepted_reference_output`、`rejected_non_reference_output`、`unknown` 统计。
- `rejected_non_reference_output_count`：同 flow 但未进入自动 provider linking 的 non-reference output 数量。
- `unresolved_reason_counts.rejected_non_reference_only`：某 input flow 只有 non-reference same-flow outputs、没有 eligible reference-output provider。

`geography_summary` 中的 canonical 字段包括：

- `tier_counts`：所有 resolved provider decision 的地理匹配层级总计。
- `tier_counts_by_strategy`：按 resolved strategy 拆分的地理匹配层级，用于判断 `unique_provider` 或 `split_by_process_volume` 各自的本地匹配与地理 fallback 分布。
- `supply_region_source_counts`：供应区域 anchor 来源总计，典型 key 为 `exchange_location`、`consumer_process_location`、`unspecified`。
- `supply_region_source_counts_by_strategy`：按 resolved strategy 拆分的供应区域 anchor 来源，用于判断某个 link 策略实际使用 exchange-level location 还是 consumer process location。
- `exchange_location_present_count`：input exchange 中存在 exchange-level `location` 的总数。
- `exchange_location_present_count_by_strategy`：按 resolved strategy 拆分的 exchange-level `location` 覆盖数。
- `requested_location_granularity_counts`：目标供应区域粒度总计，例如 `subnational`、`country`、`region`、`global`、`unspecified`。
- `requested_location_granularity_counts_by_strategy`：按 resolved strategy 拆分的目标供应区域粒度。

`build_snapshot` job 运行和完成时，worker 会在 `worker_jobs.diagnostics/result_json` 中记录全局构建并发锁与构建耗时信息。Canonical `worker_jobs` execution never mirrors these diagnostics into optional `lca_jobs`; only the explicitly enabled legacy pgmq/debug backend writes legacy diagnostics. 这些字段属于诊断信息，不改变 job payload、状态机或 result artifact 主契约。

### 5.1 Matrix-readiness verification report

自动化 LCA 数据研制使用 worker 侧的 matrix-readiness gate 判断写入后的数据是否可被行业级计算链路接受。该 gate 不决定是否创建 process/flow，也不替代 CLI schema/ruleset gate；它只验证 provider closure、snapshot graph readiness 和 solver/LCIA compute stability。

可调用入口：

```bash
cargo run -p solver-worker --bin matrix_readiness -- \
  --input matrix-readiness-input.json \
  --out matrix-readiness-report.json
```

fresh `snapshot_builder` run 也会在 `report_dir` 下尝试写出 `matrix-readiness-<snapshot_id>.json`；该本地文件受 `SNAPSHOT_REPORT_*` retention 和低磁盘 guard 保护，跳过本地写入不改变 snapshot artifact 或 report schema。输入 `matrix_readiness_input.v2` 包含：

- `coverage`: snapshot coverage report。
- `payload`: `ModelSparseData` sparse payload。
- `compiled_graph`（可选）：fresh build 时包含 reference ports、resolved/unresolved signed balances 和兼容 provider evidence。没有该字段时仍可验证 coverage/compute，但逐边 balance evidence 会为空。
- `policy`: provider write percentage、unmatched / unresolved provider 容忍度、singular risk、LCIA factor、factorization 和 negative LCIA anomaly 策略。

输出 `matrix_readiness_report.v2` 包含：

- `status`: `passed` 或 `failed`。
- `next_action`: 例如 `publish_ready`、`repair_provider_closure_then_recheck`、`repair_compute_stability_then_recheck`。
- `metrics.provider_closure`: residual/written balance、unmatched opposite-sign reference、multi-candidate unresolved 和 equal-fallback 统计。
- `metrics.graph_readiness`: process/flow/impact scale、A/B/C/M nnz、reference/allocation closure 和 singular risk。
- `metrics.compute_stability`: factorization readiness、matrix validation report、sample unit solves、non-finite count 和 negative LCIA count。
- `balance_evidence` / `unresolved_balances`: signed coefficient、routing weight、activity requirement、closure residual 和未闭合原因；`provider_evidence` 作为兼容投影保留。
- `findings` / `blockers`: machine-readable issue codes、severity、message 和 detail payload。

当前 matrix-readiness 只通过 worker CLI 与 `snapshot_builder` report artifact 暴露；本节不表示 Edge/API 已提供 HTTP 调用入口。稳定 code、`blockers` / `findings` / `next_action` 规则、policy 默认值和调用方消费约束由 `docs/matrix-readiness-report-contract.md` 维护。

Foundry、CLI 或 Edge adapter 只能消费该 report 的 `status`、`next_action`、`blockers`、`metrics`、`balance_evidence` 和 `unresolved_balances`；不应在外部复制 worker runtime 的 balance、routing、singular-risk 或 UMFPACK readiness 规则。

### 5.2 Review-submit fast gate report

dataset revision 提交审核前使用 worker 侧 `review_submit_gate` 判断当前 revision 是否可进入审核流程。该 gate 输出二元结果：`passed` 或 `blocked`，不产生 `manual_review_required` 状态。

文件输入/输出入口：

```bash
cargo run -p solver-worker --bin review_submit_gate -- \
  --input review-submit-gate-input.json \
  --out review-submit-gate-report.json
```

数据库运行时入口：

```bash
cargo run -p solver-worker --bin review_submit_gate_runner -- --once
```

worker_jobs 运行时入口：

```bash
cargo run -p solver-worker --bin review_submit_gate_runner -- \
  --worker-jobs \
  --once
```

Edge/API 不直接运行数值 gate。legacy 路径中，Edge 通过数据库 RPC 创建、读取或 rerun `dataset_review_submit_gate_runs`；worker runner 领取 queued gate run，默认构造 no-LCIA review-submit baseline + draft overlay snapshot，执行 `review_submit_gate`，再通过 `cmd_dataset_review_submit_gate_record_result` 写回 `passed`、`blocked` 或 `error`。

新 `worker_jobs` 路径中，Edge 只 enqueue `job_kind=review_submit.gate`，worker 使用 `worker_claim_jobs` 领取、按阶段 heartbeat、执行同一 gate，然后用 `worker_record_job_result` 写回：

- `completed`：gate passed，result 中包含 `calculatorReport` 与权威 `datasetRevision.revisionChecksum`。
- `blocked`：gate blocked，`blocker_codes` 来自 report blockers，`resolution_scope=user`，`retryable=true`。
- `failed`：runner、S3、DB 或暂不支持的数据集类型错误，写入 operator diagnostics。

`worker_jobs` 模式不调用 final submit，也不修改 review-submit domain 状态；gate passed 后的 durable coordinator 属于 Edge / database 层。

输入 `review_submit_gate_input.v1` 复用 snapshot coverage、`ModelSparseData` sparse payload、compiled provider graph，并可附加 dataset revision checksum、target process indices 和 process/exchange scan records。输出 `review_submit_gate_report.v1` 包含：

- `status`: `passed` 或 `blocked`。
- `policy`: 默认 profile 为 `review_submit_fast.v1`。
- `metrics`: revision、process_scan、provider_scan、sparse_scan 和 targeted probe 统计。
- `blockers`: 提交审核硬失败 code、message 和 detail payload。

该 gate 先执行 revision/process/provider/flow/sparse 结构检查；只有没有结构 blocker 时才执行 sparse factorization readiness 与 targeted RHS solve。默认 targeted probe 只验证 `x/g` 稳定性，不计算 LCIA `h`。它不 materialize inverse，也不要求 full `solve_all_unit`。

稳定 blocker code、policy 默认值、快速验证顺序和 caller consumption 约束由 `docs/review-submit-fast-gate-contract.md` 维护。Edge 或 Next 在提交审核链路中应消费 DB gate result 里的 status、blockingReasons 和 calculatorReport，不应直接把 `matrix_readiness_report.v2` 的 blocker 当成提交审核结论。

### 5.3 Worker resource 与 object file I/O 内部契约

Worker 重任务可使用共享 `worker.resource-profile.v1` primitive 声明并在阶段开始前检查：

- owned memory estimate 与 soft/hard memory limit；
- temporary、object download/upload、cache 与 stage-window bytes；
- maximum concurrency。

稳定资源错误类为 `resource_admission_rejected`、`artifact_limit_exceeded` 与 `operation_cancelled`。阶段 telemetry 分开记录 owned estimate、process RSS、Linux cgroup v2 anon/file/current/peak、temp/object/cache bytes、rows 与 nnz；RSS/cgroup 是最终安全边界，不替代 owned admission。

对象存储的新重任务迁移面使用 file API：

- `download_object_url_to_file`：必须传显式 byte cap；即使响应没有 `Content-Length` 也在每个 chunk 后执行累计上限检查；可校验 SHA-256 和 cooperative cancellation；只有完整成功后才原子发布目标文件。
- `upload_object_key_file_bounded`：在网络传输前按文件 metadata 拒绝超限、流式计算/校验 SHA-256，并在每个 multipart boundary 检查 cancellation；失败或取消会 abort 已创建的 multipart upload。

旧 `download_object_url -> Vec<u8>`、`download_object_key -> Vec<u8>` 与现有 file-upload 方法保留作兼容面；新迁移的 snapshot、package、graph-cache 或 solve 路径不得继续采用完整对象内存物化。具体算法迁移由 #162 的后续独立交付完成，不改变本节现有 jobs/results consumer schema。

`lcia.scope_closure_check` 的公开 XLSX/manifest selector 与 result DTO 不因单条超大 administrative record 改变。canonical v4 内部保持普通 NDJSON partition 原样；若一条逻辑记录连同 NDJSON 换行超过 32 MiB，则以小型 index 和固定 8 MiB canonical-byte chunks 表示。index、top-level manifest 与 relation-local layout 共同绑定逻辑长度、完整 record SHA-256 以及 chunk 顺序/长度/hash；读端流式重建并以原 canonical record 加单个换行计算 relation hash，对缺失、重复、乱序或篡改 fail closed。该内部物理分段不扩大公开下载 selector，也不改变 Database #316 的 descriptor ordinal/digest、seal 或 finalize 契约。

## 6. 幂等与请求缓存（建议约束）

- `worker_jobs.idempotency_key` / `worker_jobs.request_hash`：同一业务请求重试时复用，避免重复创建 canonical worker job。
- `lca_results.job_id` / `lca_result_cache.job_id`：仅为历史 compatibility UUID，不要求 `lca_jobs` parent row。
- `lca_result_cache`：
  - 唯一键 `(scope, snapshot_id, request_key)`
  - 状态 `pending/running/ready/failed/stale`
  - 命中时直接返回已有 `result_id` 或进行中的 `job_id`
  - 当前实现中：
    - Edge 入队时写 `pending`
    - worker 开始求解时写 `running`
    - worker 成功落结果后写 `ready + result_id`
    - worker 失败时写 `failed + error_code/error_message`

## 7. 安全与权限边界

- `lca_*` 表已启用 RLS。
- `anon` 无权限。
- `authenticated` 仅可读“自己的 `lca_jobs` + 关联 `lca_results`”。
- 任何 enqueue / insert / update 必须经服务端 `service_role`。

## 8. 最小 SQL 约定（服务端）

legacy pgmq 路径：

1. 插入 job 行（`status=queued`，带 payload）。
2. 调 `public.lca_enqueue_job(text, jsonb)` RPC 投递消息（函数内部调用 `pgmq.send`）。
3. 返回 `job_id` 给调用方。
4. worker 消费后更新 `lca_jobs` 并写 `lca_results`。

统一 `worker_jobs` 路径：

1. 先创建或复用 `lca_result_cache` domain row，并生成 `lcaJobId` compatibility UUID；不要求创建 `lca_jobs` row。
2. 调 `public.worker_enqueue_job(...)` 创建 `job_kind=lca.*`、`worker_queue=solver` 的 job，payload 带 `lcaJobId` 和标准化求解参数。
3. 返回 `workerJobId` 与 `lcaJobId` 给 Edge / Next projection。
4. worker 使用 `worker_claim_jobs('solver', ...)` claim、heartbeat，并通过 `worker_record_job_result(...)` 写 canonical 终态；同时维护 `lca_results`、`lca_result_cache` domain/cache metadata，并回填 `worker_job_id`。Canonical path does not query or update optional `lca_jobs`; legacy table access is restricted to the explicitly enabled pgmq/debug backend。

不建议前端直接调用 `pgmq.send`、直接写 `lca_jobs` 或直接写 `worker_jobs`。
