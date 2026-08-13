---
title: Edge Function Integration Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要把 edge-functions 请求稳定映射到 worker 异步求解链路时
  - 当 enqueue、polling、service-role、request_key 或 snapshot 选择规则变化时
  - 当 Edge 需要接入 Review Admin quality diagnostic 的 start/read/status contract 时
whenToUpdate:
  - 当 edge-facing solve API、入队流程、worker 边界或错误处理约定变化时
  - 当 Review Admin quality diagnostic 的 Edge RPC 边界或 worker runner 结果回写边界变化时
checkPaths:
  - docs/edge-function-integration.md
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/review-quality-diagnostic-contract.md
  - crates/solver-worker/src/review_quality_diagnostic_runner.rs
  - crates/solver-worker/src/worker_jobs.rs
  - crates/solver-worker/src/bin/review_quality_diagnostic_runner.rs
  - crates/**
  - supabase/migrations/**
lastReviewedAt: 2026-08-13
lastReviewedCommit: 223892ac89d08e5266b41c7d697ecb121d20d508
lastReviewedNote: "Updated for Issue #249: Edge exposes Review Admin-only manual diagnostic start/read behavior and never turns quality findings into Review blockers."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/frontend-integration.md
---

# Edge Function Integration Guide

本文档给 Supabase Edge Functions 项目使用，目标是把前端请求稳定地映射到 worker 异步链路。统一任务路径是 `private.worker_jobs(worker_queue=solver)`，result/cache domain truth 由 `private.worker_jobs`、`private.lca_results` 与 `private.lca_result_cache` 承载；旧 `lca_jobs + pgmq` 生命周期已退役。

## 1. 为什么必须走 Edge Function

- 前端不应持有 `service_role`。
- `worker_enqueue_job` / `pgmq.send`、缓存去重、历史 compatibility id 生成都属于受控写操作。
- RLS 已收紧，前端只适合读取自己的 `jobs/results`，不适合写任务。

## 2. 推荐的 Edge API

建议提供以下 API（函数路由名可调整）：

- `POST /lca/solve`
- `GET /lca/jobs/:jobId`
- `GET /lca/results/:resultId`
- `POST /lca/prepare`（管理员/运维）
- `POST /lca/invalidate`（管理员/运维）
- `POST /data-products/lcia-result-packages/build`（data product manager）
- `POST /data-products/lcia-result-packages/:packageId/publish`（data product manager）

## 3. `POST /lca/solve` 输入/输出

### 3.1 请求体（建议）

```json
{
  "scope": "prod",
  "snapshot_id": "optional-uuid",
  "demand_mode": "single",
  "demand": {
    "process_index": 123,
    "amount": 1.0
  },
  "solve": {
    "return_x": true,
    "return_g": true,
    "return_h": true
  }
}
```

全量单位需求模式（不传 `process_index/amount`）：

```json
{
  "scope": "prod",
  "snapshot_id": "optional-uuid",
  "demand_mode": "all_unit",
  "solve": {
    "return_x": false,
    "return_g": false,
    "return_h": true
  },
  "unit_batch_size": 128
}
```

Header 建议：

- `X-Idempotency-Key: <uuid-or-hash>`

### 3.2 响应（建议）

首次入队：

```json
{
  "mode": "queued",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

命中缓存：

```json
{
  "mode": "cache_hit",
  "result_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

命中运行中任务：

```json
{
  "mode": "in_progress",
  "job_id": "<uuid>",
  "snapshot_id": "<uuid>",
  "cache_key": "<request_key>"
}
```

## 4. Edge 端处理流程（强约束）

1. 验证用户 JWT，拿到 `user_id`。
2. 解析请求并标准化（默认 `amount=1.0`，补 `solve` 默认值）。
3. 选择 `snapshot_id`：
   - 若请求显式给出，校验存在且可用。
   - 否则读 `lca_active_snapshots(scope='prod')`。
   - `data_scope=public_plus_owner_draft` 时，Edge 必须使用 `lca.build_snapshot.request.v2` 创建独立 snapshot family：只传 public `state_code=100` 与当前 actor `state_code=0`，并携带 frozen scope manifest、method/factor source contract 和 factor-coverage contract。不能复用 legacy `100..199 + all owner states` snapshot。
4. 构造求解负载：
   - `demand_mode=single`：构造 `rhs`（长度 = `process_count`，只在目标 index 赋值 `amount`）。
   - `demand_mode=all_unit`：构造 `solve_all_unit` payload（不在 Edge 侧生成整块 `rhs_batch`）。
   - 对 `public_plus_owner_draft`，先从 `snapshot-index-v1.json.calculation_evidence` 读取并校验 `lca.calculation_evidence.v2`，再原样写入 `calculation_evidence_binding`；分别使用 `lca.solve_one.request.v2`、`lca.solve_all_unit.request.v2` 或 `lca.contribution_path.request.v2`。证据缺失、scope hash 漂移、static-bundle source/identity hash 非法、25-method matrix 成员或计数不一致、coverage 状态与 gap/artifact 不一致时返回冲突，不入队 v1 fallback。
5. 生成：
   - `request_key`（标准化请求哈希）
   - `idempotency_key`（优先 header，否则退化为 `user_id + request_key`）
6. 在事务中操作：
   - upsert/读取 `lca_result_cache(scope,snapshot_id,request_key)`
   - 若 `ready` 且有 `result_id`，直接返回 `cache_hit`
   - 若 `pending/running` 且有 `job_id`，返回 `in_progress`
   - 否则生成 `lcaJobId` compatibility UUID，并创建或更新 `lca_result_cache(status='pending', job_id=lcaJobId)`
   - 调用 `private.worker_enqueue_job(...)`，使用 `job_kind=lca.solve_one|lca.solve_batch|lca.solve_all_unit|lca.build_snapshot|lca.contribution_path`、`worker_queue=solver`，并在 payload 中携带 `lcaJobId`
   - 回写 `lca_result_cache.worker_job_id`
7. 返回 `queued`。`worker_jobs` 路径应额外返回 `workerJobId`，供任务中心和 operator 查询使用。

worker 侧以 `private.worker_jobs` 为任务生命周期事实，并继续推进 domain/cache 表：`private.lca_result_cache` 从 `pending -> running -> ready`（或失败时 `failed`）。终态写回时会把 `private.lca_results`、`private.lca_result_cache`、`private.lca_latest_all_unit_results`、`private.lca_factorization_registry` 中可关联的 rows 回填到同一个 `worker_job_id`。

`public_plus_owner_draft` 是 fail-closed 协议：Edge 负责生产和预校验证据，worker 仍会独立验证 payload、process/flow 数据库行可见性、reviewed static LCIA bundle、snapshot-index evidence 与 solve binding。scope manifest 只覆盖 processes/flows；LCIA 来源是 actor-independent、hash-bound 的 25-method cache bundle。Edge 只能发送固定相对清单路径、最终 raw SHA 和完全相同的 embedded manifest，不能发送 URL。worker 从可信配置的 HTTPS base（或本地验证目录）取文件，验证大小、全部哈希、alias、方法成员和 factor 数值后参与计算。coverage 按 method/exchange pair 统计；任一方法缺 factor 都保持 `incomplete_coverage_not_zero` 和外置 JSONL 证据，不能被 UI 当成“完整的零影响”。

LCIA result package 构建走同一个 `worker_jobs(worker_queue=solver)` 生命周期，但不是普通 `/lca/solve` 请求。Edge 的 data product manager command 应先通过数据库 command 解析权限、published-only eligibility 和默认 impact category，再 enqueue `job_kind=lcia_result.package_build` / `payload_schema_version=lcia_result.package_build.request.v1`。payload 使用数据库返回的 `buildId`、`requestedBy`、`coverageMode`、`inputManifest`、`inputManifestHash`、`eligibleInputCount`、`includedInputCount`、`lciaMethodSet` 和可选 `defaultImpactCategory`；worker 只消费已发布 `stateCode/state_code=100..199` 的 manifest 输入。worker 完成后用 service-role DB 连接调用 `private.cmd_lcia_result_package_mark_ready(...)` 固化 `lcia_result_packages` preview package；发布仍由 Edge manager command 调用数据库 publish RPC 完成。

## 5. 与 worker 的职责边界

Edge：

- 鉴权
- 快速参数校验
- 缓存去重与入队
- 结果读取聚合（可选）

worker：

- 取快照数据
- 分解/求解
- heartbeat `worker_jobs.phase/progress`
- 用 `worker_record_job_result` 写统一任务终态、错误、`result_json` 和 `result_ref`
- 写 domain/cache metadata（如 `private.lca_results` artifact、`private.lca_result_cache`）；这些都不替代 `private.worker_jobs` 任务生命周期事实
- 对 `lcia_result.package_build`，构建 published-only snapshot、持久化 all-unit result/query artifacts，并通过 service-role `cmd_lcia_result_package_mark_ready` 标记 package preview ready；失败只写 `worker_jobs` package-specific result，不更新 `lca_result_cache`
- 对 `build_snapshot`，从同一 `worker_jobs` heartbeat diagnostics 投影 resolved snapshot ID 与 calculation evidence；snapshot reuse 也必须返回真实 resolved ID

不要让 Edge 自己更新 worker lease/result 字段。

## 6. 失败与重试建议

- Edge 入队失败：返回 `5xx`，前端可用同 `X-Idempotency-Key` 重试。
- worker 失败：以 `private.worker_jobs.status=failed` 和 `error_*` 字段为任务事实。
- 前端轮询到 `failed` 时，提示用户重试并保留 `job_id` 便于追踪。

## 7. 最小实现清单

- 使用 service role client（仅服务端）。
- 封装 `resolve_snapshot(scope)`。
- 封装 `build_rhs(process_count, process_index, amount)`。
- 封装 `build_solve_all_unit_payload(snapshot_id, solve, unit_batch_size)`。
- 封装 `make_request_key(normalized_input)`。
- 封装 `enqueue_job_and_update_cache(...)` 事务函数；统一任务路径必须创建/复用 `lca_result_cache`，生成 compatibility `lcaJobId`，enqueue `worker_jobs`，并回写 `worker_job_id`。
- 输出统一错误码（如 `BAD_INPUT` / `SNAPSHOT_NOT_READY` / `QUEUE_ERROR`）。

## 8. 不要做的事

- 不要让前端直接写 `lca_jobs`。
- 不要让前端直接调用 `pgmq.send`。
- 不要让前端直接写 `worker_jobs`；统一任务也必须由 Edge/database service-role 边界 enqueue。
- 不要在 Edge Function 同步等待完整求解结果。
- 不要在 Edge Function 中进行重数值计算。

## 9. Review Admin 质量诊断集成边界

提交审核不再创建或等待 worker 数值 Gate。服务端提交接口只处理鉴权、目标状态、所有权、并发和事务不变量；可由当前提交者直接修复的字段规则由 Next 在提交前即时展示。

Review Admin 质量诊断的职责分层：

- Database 负责 Review Admin 鉴权、人工 start/read RPC、`worker_jobs` 生命周期和结果投影。
- Edge 只把 start/read RPC 暴露给 Review Admin，转发稳定 envelope，不解释矩阵规则。
- Worker 领取 `worker_queue=review_quality` 的 `review.quality_diagnostic` job，对全部待审核 Process 构建一张联合矩阵并写回报告。
- Next 只在 Review Admin 页面提供“运行质量诊断”、进度和报告展示；Review Member 与提交者不显示入口或报告。

Edge 不执行 snapshot builder、provider closure、factorization 或 unit solve，也不创建 Batch、revision/checksum、waiver 或 risk-acceptance 对象。

payload：

```json
{
  "scope": {
    "kind": "pending_review",
    "reviewStates": [0, 1]
  },
  "requestedAt": "2026-08-13T09:00:00Z"
}
```

状态语义：

- `queued` / `running`：显示诊断进度，但不禁用任何 Review 操作。
- `completed + clear`：已执行检查未发现问题或没有待审核 Process。
- `completed + findings`：显示完整性/数值稳定性发现；不阻止分配、批准或拒绝。
- `completed + not_evaluable`：显示导致矩阵无法构建的数据发现；不阻止 Review 操作。
- `failed`：显示运行故障和重试入口；不阻止 Review 操作。

Edge/Next 必须以 report 中 `informationalOnly=true`、`affectsReviewState=false` 和 finding `workflowBlocking=false` 为准。finding `level=error` 不是工作流 `blocked`。

部署上，`review_quality_diagnostic_runner` 需要与 solver worker 相同的 DB 和 S3 环境。常驻运行时使用 `REVIEW_QUALITY_POLL_MS`、`REVIEW_QUALITY_WORKER_ID` 与 `REVIEW_QUALITY_WORKER_LEASE_SECONDS`；`--once` 仅处理一条或空转退出。
