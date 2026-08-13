---
title: Frontend Integration Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当你需要给前端消费方说明 solve/result 交互契约时
  - 当 job 状态展示、轮询策略、artifact 读取或幂等键策略变化时
  - 当 Review Admin quality diagnostic 的前端状态消费规则变化时
whenToUpdate:
  - 当前端侧交互流程、结果读取方式或错误处理约定变化时
  - 当 Review Admin quality diagnostic 的前端轮询、状态或报告展示规则变化时
checkPaths:
  - docs/frontend-integration.md
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
  - docs/review-quality-diagnostic-contract.md
lastReviewedAt: 2026-08-13
lastReviewedCommit: 223892ac89d08e5266b41c7d697ecb121d20d508
lastReviewedNote: "Updated for Issue #249: submit no longer waits for worker numerical validation; Review Admin manually runs and views an informational report."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
---

# Frontend Integration Guide

本文档给前端项目使用，目标是稳定触发 LCA 异步计算并正确展示结果。

## 1. 前端只做两件事

- 调 Edge API 发起计算。
- 轮询或订阅 job/result 状态并展示。

前端不要直接写 `lca_jobs`、`pgmq` 或 `worker_jobs`。

## 2. 推荐交互流程

1. 用户提交求解模式：
   - `single`：目标 process + amount + solve 选项
   - `all_unit`：全量 process 的单位需求（`amount=1`）+ solve 选项（建议仅 `h`）
2. 前端 `POST /lca/solve`（带 `X-Idempotency-Key`）。
3. 根据返回模式处理：
   - `cache_hit`: 直接拉结果并渲染。
   - `queued` / `in_progress`: 进入任务中心或 job 进度页，由 Edge 返回的服务端 projection 驱动展示。
4. 轮询 `GET /lca/jobs/:jobId`，直到：
   - `completed` / `ready`：查询结果
   - `failed`：展示失败原因
5. `GET /lca/results/:resultId` 后渲染 `x/g/h`（按 UI 需求）。

## 3. 轮询策略

- 初始间隔：`1s`
- 递增到：`2s -> 3s -> 5s`（上限 5s）
- 最长等待：建议 `60-120s`（超时后允许用户继续后台等待）

建议显示：

- 当前状态（queued/running/completed/failed）
- 任务创建时间
- 最近更新时间
- 失败时 diagnostics 摘要

## 4. 结果读取注意点

`lca_results` 当前是 S3-only：

- DB 只返回 `artifact_*` 元数据与 diagnostics
- 结果实体在对象存储（`hdf5:v1`）

前端建议不直接下载 `artifact_url`，而是经 Edge Function 读取/代理，避免暴露存储细节和权限问题。

## 5. 幂等键建议

每次用户点击“计算”前，生成稳定键：

- 同一请求重复提交（网络重试、刷新）使用同一个 key。
- 请求参数变化（mode/process/amount/solve）必须生成新 key。

可用方式：

- `sha256(user_id + normalized_request_json)`。

## 6. 状态与文案建议

- `queued`: 排队中
- `running`: 计算中
- `ready`: 分解已就绪（prepare 场景）
- `completed`: 计算完成
- `failed`: 计算失败
- `stale`: 快照或分解已过期，需要重建

## 7. 错误处理建议

- `400`：参数问题（提示用户修改输入）
- `401/403`：登录或权限问题
- `404`：任务/结果不存在或不属于当前用户
- `409`：并发冲突（可重试）
- `5xx`：系统异常（带幂等键重试）

## 8. 最小前端接口模型（示例）

```ts
export type SolveSubmitResponse =
  | { mode: 'queued'; job_id: string; snapshot_id: string; cache_key: string }
  | { mode: 'in_progress'; job_id: string; snapshot_id: string; cache_key: string }
  | { mode: 'cache_hit'; result_id: string; snapshot_id: string; cache_key: string };

export type JobStatus =
  | 'queued'
  | 'running'
  | 'ready'
  | 'completed'
  | 'failed'
  | 'stale';
```

## 9. Review Admin Quality Diagnostic 状态消费

普通提交审核不创建、轮询或等待 worker 数值任务。前端保留当前数据本身的即时可修复字段校验；服务端提交成功后直接进入 Review 工作流。

Review Admin 页面提供单一“运行质量诊断”入口，并通过 Edge 消费服务端 projection：

- `queued` / `running`：显示进度，但不得禁用分配、批准或拒绝。
- `completed + clear`：显示本次检查范围和未发现问题。
- `completed + findings`：按 `completeness` 与 `numerical_stability` 分区显示 findings。
- `completed + not_evaluable`：说明完整性问题使矩阵无法构建，数值稳定性未执行。
- `failed`：显示运行故障和重试入口。

Review Member 和提交者不显示入口或报告。前端不得复制 provider、factorization 或 solve 规则，不得直接读写 `private.worker_jobs`，不得把 finding `level=error` 当成工作流阻断。report 的 `informationalOnly=true`、`affectsReviewState=false` 和 finding `workflowBlocking=false` 是强制消费语义。

`worker_jobs` 的 `progress`、`phase`、`heartbeatAt` 可用于该页面的运行状态。浏览器本地状态只能作为 UI cache，不是诊断运行事实来源。

LCA solver/snapshot 使用 `worker_jobs(worker_queue=solver)` 后，前端仍然不读表；Edge 应把 `workerJobId`、`lcaJobId`、`phase`、`progress`、`resultId` 和失败摘要投影给任务中心。worker result ref 使用 `{"domainSource":"worker_jobs","workerJobId":"<uuid>","lcaJobId":"<uuid>"}` 这类服务端诊断结构；前端不直接解释 raw `result_ref`，而是消费 Edge projection。`lcaJobId` 是读取 `lca_results` / `lca_result_cache` 历史 `job_id` columns 的 compatibility domain key，不能被浏览器伪造，也不表示必须存在 `lca_jobs` parent row。

## 10. 前端验收清单

- 同一请求重复提交不会生成重复 job。
- 页面刷新后可恢复轮询状态。
- `failed` 能显示可读错误并支持重试。
- 结果读取按 artifact 元数据路径工作（不依赖 inline payload）。
- 用户无法读取他人的 job/result。
- 普通提交审核不会进入 worker 数值任务，也不会因上游完整性或矩阵稳定性被卡住。
- 只有 Review Admin 能运行和查看质量诊断；`findings` / `not_evaluable` / `failed` 均不禁用 Review 操作。
