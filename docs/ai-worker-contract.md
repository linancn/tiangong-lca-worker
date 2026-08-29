---
title: AI Worker Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当新增或修改 AI worker、AI job handler、模型 provider 或 TIDAS AI 校验结果时
  - 当 Edge 或 Database 需要 enqueue、查询或解释 AI worker job 时
whenToUpdate:
  - 当 AI queue、job kind、payload/result schema、规则绑定、模型配置或失败语义变化时
checkPaths:
  - docs/ai-worker-contract.md
  - AGENTS.md
  - .env.example
  - .docpact/config.yaml
  - crates/solver-worker/src/ai/**
  - crates/solver-worker/src/bin/ai_worker.rs
  - crates/solver-worker/src/worker_jobs.rs
  - crates/solver-worker/src/tidas_cli.rs
  - docs/lca-api-contract.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
lastReviewedAt: 2026-08-29
lastReviewedCommit: c7f362e7a50eb003104851dcc1112fece81038bc
lastReviewedNote: "Documented Worker Issue #277 explicit AI terminal failure disposition and unchanged one-third lease behavior."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/agents/repo-architecture.md
  - docs/agents/repo-validation.md
---

# AI Worker Contract

本文档定义 Worker 仓库拥有的通用 AI 任务运行时。进程和队列命名为
`ai-worker` / `ai`；任何具体能力都必须作为版本化 job handler 接入，不能把进程固化为
某一个 suggestion 功能。

## 1. Ownership 与信任边界

- Next 只负责触发、轮询和展示差异，不直连模型或 `private.worker_jobs`。
- Edge 负责用户鉴权、请求归一化、enqueue 与结果查询 API。
- Database 拥有 durable job schema、RPC、lease fencing、RLS 与幂等约束。
- Worker 的 `ai-worker` 负责 claim、heartbeat、handler dispatch、模型请求、结果生成和
  lease-fenced terminal write。
- LangGraph 不属于这条运行路径。模型调用由 Rust `reqwest + serde + tokio` 边界完成。

`ai-worker` 只连接 queue database；它不要求 solver 的 S3/AppState，也不暴露新的 HTTP
入口。

## 2. Queue 与 handler registry

当前固定 queue：

- `worker_queue = ai`
- process/binary name：`ai-worker` / `ai_worker`

首个 handler：

| job kind | request schema | result schema |
| --- | --- | --- |
| `ai.tidas_suggestion` | `ai.tidas_suggestion.request.v1` | `ai.tidas_suggestion.result.v1` |

未知 queue、job kind 或 schema version 必须 fail closed，记录
`invalid_ai_worker_job` 且 `retryable=false`。未来能力使用新的 job kind 和独立 handler；不得
通过扩大 v1 payload 的自由字段绕过版本升级。

## 3. `ai.tidas_suggestion.request.v1`

```json
{
  "dataType": "process",
  "data": {
    "processDataSet": {}
  }
}
```

- `dataType` 只能是 `process` 或 `flow`。
- `data` 必须是 JSON object，并包含与 `dataType` 对应的 `processDataSet` 或
  `flowDataSet` root。
- v1 不接受未知顶层字段。
- encoded `data` 默认最大 2 MiB，可用 `AI_MAX_INPUT_BYTES` 在 1 KiB 到 16 MiB
  范围内配置。

## 4. Authoritative ruleset binding

Worker 不复制方法学规则。进程启动时通过统一 Rust `tidas` CLI 加载：

- Process：`tidas ruleset --id process-authoring/strict --format json`
- Flow：`tidas ruleset --id flow-authoring/strict --format json`

两个 ruleset 必须来自同一个 `ruleset_version` 与 `catalog_sha256`，并绑定实际
`tidas` binary version；不匹配或不完整时进程在 claim job 前 fail closed。结果必须保留
ruleset id/version/catalog SHA-256/TIDAS version，保证建议可以回溯到确切方法学输入。

handler 只处理规则命中的现有字段；它不因规则路径缺失而凭空创建字段。`[*]` 路径展开到
输入中实际存在的数组成员。

## 5. Model boundary

v1 使用 OpenAI-compatible `POST /chat/completions`：

- `AI_PROVIDER_BASE_URL`
- `AI_PROVIDER_API_KEY`
- `AI_PROVIDER_MODEL`
- `AI_MODEL_CONFIG_VERSION`

请求固定 `temperature=0`。`AI_MODEL_CONFIG_VERSION` 是部署方维护的 prompt/provider
配置标识，必须随结果返回；模型名不能替代配置版本。

每个规则路径使用独立请求，并受 `AI_MAX_CONCURRENCY` 限制。HTTP timeout、最大响应字节和
重试分别由 `AI_REQUEST_TIMEOUT_SECONDS`、`AI_MAX_RESPONSE_BYTES` 和
`AI_PROVIDER_MAX_ATTEMPTS` 控制。只重试 transport/timeout、HTTP 429 与 5xx；格式、shape
和配置错误不重试。

API key、provider 原始错误 body 和 dataset payload 不得写入 Worker diagnostics/error
message。

## 6. Output 与部分失败

模型必须只返回目标字段的有效 JSON value，且 JSON type、object keys、array 长度和嵌套
shape 与原值完全一致。shape 不一致、非 JSON 或额外解释文本只使该路径失败，不得应用。

`ai.tidas_suggestion.result.v1` 返回完整 dataset，而不是 patch：

```json
{
  "schemaVersion": "ai.tidas_suggestion.result.v1",
  "status": "complete",
  "dataType": "process",
  "data": {},
  "inputSha256": "...",
  "ruleset": {
    "id": "process-authoring/strict",
    "version": "...",
    "catalogSha256": "...",
    "tidasVersion": "..."
  },
  "model": {
    "model": "...",
    "configVersion": "..."
  },
  "summary": {
    "matchedPathCount": 0,
    "processedPathCount": 0,
    "changedPathCount": 0,
    "failedPathCount": 0
  },
  "failures": []
}
```

- `complete`：所有命中路径完成，Worker terminal status 为 `completed`。
- `partial`：至少一个路径完成且至少一个路径失败；保留成功修改，terminal status 仍为
  `completed`，路径失败记录稳定 code 和 `retryable`。
- `failed`：有命中路径但没有任何路径完成；返回原始完整 dataset，Worker terminal status
  为 `failed`，并保留 versioned result JSON。
- 没有命中任何现有路径是合法的 `complete` no-op。

terminal failure 的 `retryable` 不能依赖 constructor 默认值：无效 queue/job/schema/payload、输入超限/非法或 ruleset 缺失明确为 `false`；provider transport/timeout、429/5xx 等运行时瞬态明确为 `true`；所有路径失败时，只要任一稳定 failure 明确可重试，terminal 结果即为 `true`，否则为 `false`。

AI 输出始终是 advisory suggestion。Worker 不写 Process/Flow domain row，不改变 Review 或
发布状态；用户接受哪些差异仍由产品层决定。

## 7. Lease 与运行语义

`ai-worker` 使用 `private.worker_claim_jobs`、`private.worker_heartbeat_job` 和
`private.worker_record_job_result`。运行期间按 lease 的约三分之一周期 heartbeat；lease 或
heartbeat 丢失必须中止当前 handler future，不能在失去所有权后继续写 terminal result。
terminal write 使用现有同 lease token 幂等重试边界。

队列级 claim 数量与 handler 内模型并发是两个不同限制。部署时应从小的
`AI_WORKER_CLAIM_LIMIT` 和 `AI_MAX_CONCURRENCY` 开始，根据 provider rate limit 与 DB
连接预算分别调整。
