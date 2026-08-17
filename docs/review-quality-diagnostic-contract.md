---
title: Review Admin Quality Diagnostic Contract
docType: contract
scope: repo
status: active
authoritative: true
owner: worker
language: zh-CN
whenToUse:
  - 当 Review Admin 手动检查全部待审核数据的完整性和数值稳定性时
  - 当 Edge 或 Next 需要消费 review quality diagnostic report 时
  - 当 review.quality_diagnostic 的 worker payload、report 或运行语义变化时
whenToUpdate:
  - 当 crates/solver-worker/src/review_quality_diagnostic_runner.rs 变化时
  - 当 crates/solver-worker/src/bin/review_quality_diagnostic_runner.rs 变化时
  - 当 review.quality_diagnostic 的 job/result schema 变化时
  - 当提交审核与 Review Admin 质量诊断的边界变化时
checkPaths:
  - docs/review-quality-diagnostic-contract.md
  - crates/solver-worker/src/review_quality_diagnostic_runner.rs
  - crates/solver-worker/src/bin/review_quality_diagnostic_runner.rs
  - crates/solver-worker/src/worker_jobs.rs
  - crates/solver-worker/src/db.rs
  - crates/solver-worker/src/bin/snapshot_builder.rs
  - crates/solver-worker/src/readiness.rs
  - crates/solver-worker/src/snapshot_artifacts.rs
  - docs/lca-api-contract.md
  - docs/edge-function-integration.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
lastReviewedAt: 2026-08-17
lastReviewedCommit: 4c9f23335c10b01bd48466650ac9f0323b5ff9c4
lastReviewedNote: "Reviewed after versioned calculation snapshots made legacy team/review guard fields optional; Review Admin diagnostics use their separate pending-review scope and this contract remains unchanged."
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/lca-api-contract.md
  - docs/matrix-readiness-report-contract.md
  - docs/edge-function-integration.md
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
---

# Review Admin Quality Diagnostic Contract

`review.quality_diagnostic` 是 Review Admin 手动发起的待审核数据质量诊断。它回答两个问题：

1. 当前全部待审核 Process 及其计算依赖是否足以形成完整矩阵。
2. 该联合矩阵能否完成 sparse factorization，并通过有界 unit-solve 稳定性探测。

该功能不是提交审核 Gate，也不是审核决策 Gate。诊断结果、无法计算和运行失败都不得改变 Review 状态，不得阻止分配、批准或拒绝操作。

## 产品边界

- 仅 Review Admin 可以通过 Database / Edge 发起和读取诊断。
- 诊断只由人工点击启动，不在提交审核、分配审核或审核决策时自动运行。
- 提交者不消费该报告；提交审核只处理当前数据可立即修复的输入规则和服务端事务不变量。
- 报告是观察信息，不提供 waiver、risk acceptance 或“带问题通过”动作。
- 不创建 Batch、revision 或 checksum 产品实体。worker job ID 只是已有异步运行事实；内部 snapshot ID 只是计算 artifact 定位信息。

## Worker job contract

runner 只领取：

- `job_kind = review.quality_diagnostic`
- `worker_queue = review_quality`
- `payload_schema_version = review.quality_diagnostic.request.v1`

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

`requestedBy` 来自 worker job 顶层事实，不在 payload 中重复。runner 使用通用 `worker_claim_jobs`、heartbeat 和 lease-fenced result write；旧 lease 不得覆盖新 lease 的结果。

运行入口：

```bash
cargo run -p solver-worker --bin review_quality_diagnostic_runner -- --once
```

常驻部署使用：

- `REVIEW_QUALITY_POLL_MS`，默认 `1000`
- `REVIEW_QUALITY_WORKER_ID`，默认 `review_quality_diagnostic_runner`
- `REVIEW_QUALITY_WORKER_LEASE_SECONDS`，默认 `900`
- `REVIEW_QUALITY_MAX_RUNS`，可选

## 联合待审核矩阵

runner 在一次运行开始时读取 `private.reviews` 中 `review_kind in (root, reference)` 且 `state_code in payload.scope.reviewStates` 的当前记录，并按 `(target_table, data_id, data_version)` 去重。

所有待审核 `processes` target 作为同一次 snapshot build 的 request roots。候选 Process 范围是：

- `state_code = 20` 的审核中 Process；
- `state_code = 100..199` 的公共 Process。

snapshot builder 对全部 roots 一起解析 provider closure，因此报告反映联合矩阵，而不是把旧的单条提交 Gate 批量循环执行。Flow 等依赖按 Process 中的 exact reference 解析；缺失或结构非法的依赖必须成为完整性发现。

snapshot 使用 `review_quality_diagnostic` artifact purpose、no-LCIA 模式和短期 artifact 生命周期。报告不依赖 LCIA factor 完整性；数值部分检查 `M = I - A` 的可计算性。

如果当前没有待审核 Process，runner 不构建矩阵，返回 `outcome = clear`，两个 section 均为 `not_applicable`。这不代表其他类型待审核数据已通过独立业务质量认证。

## 报告 contract

所有有业务意义的诊断结论都以 worker job `status = completed` 写回，且：

- `result_schema_version = review.quality_diagnostic.report.v1`
- `blocker_codes = []`
- `resolution_scope = null`
- report 顶层 `informationalOnly = true`
- report 顶层 `affectsReviewState = false`

顶层 `outcome`：

| outcome | 含义 |
| --- | --- |
| `clear` | 已执行的检查未发现问题，或当前没有待审核 Process 需要矩阵检查 |
| `findings` | 矩阵可构建，完整性或数值稳定性检查发现需要关注的信息 |
| `not_evaluable` | 数据/依赖不足或结构非法，联合矩阵无法完整构建，因此数值稳定性未执行 |

报告稳定 envelope：

- `schemaVersion`、`runId`、`generatedAt`、`requestedAt`、`requestedBy`
- `outcome`、`informationalOnly`、`affectsReviewState`
- `scope`：review states、Review/Dataset/Process 数量、按表数量、Process 有界 sample、可选内部 snapshot ID
- `summary`：面向 UI 的关键统计
- `sections[]`：固定 `completeness` 与 `numerical_stability`
- `findings[]`：两个 section 的扁平汇总

每条 finding 包含：

- `code`
- `category = completeness | numerical_stability`
- `level = info | warning | error`
- `message`
- `details`
- `workflowBlocking = false`

`level = error` 表示质量严重程度，不是工作流阻断状态。调用方不得把它翻译成 Review 操作禁用条件。

## 完整性与数值稳定性

完整性 section 复用 matrix-readiness 的 provider closure、reference normalization、allocation 和 graph coverage 事实。数值稳定性 section 复用 singular-risk、factorization、matrix validation 与有界 unit-solve 事实。

构建阶段的结构化 source-closure findings 会直接形成 `not_evaluable` 报告。可归因于数据内容的 snapshot builder 非零退出也形成通用完整性 finding；launch、timeout、signal 或 terminal protocol 错误属于 worker 运行失败，写 `status = failed`，仍不影响 Review 状态。

该诊断使用 `matrix_readiness_report.v2` 的当前计算规则，但不复用它的 `passed / failed` Gate 语义。worker 将原 `blocker` 严重度映射为 finding `level = error`，同时固定 `workflowBlocking = false`。

## 兼容边界

`review_submit_gate` 与 `review_submit_gate_runner` 暂时保留为离线 fixture / 历史运行兼容入口。当前产品提交审核流程不得 enqueue `review_submit.gate`，也不得等待其 `passed / blocked` 结果；新功能不得继续扩展 legacy Gate contract。

## 最小验证

```bash
cargo test -p solver-worker worker_jobs
cargo test -p solver-worker review_quality_diagnostic_runner
cargo check -p solver-worker --bin review_quality_diagnostic_runner
cargo clippy -p solver-worker --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

在隔离的非生产 DB/S3 smoke 中至少验证：

- 多个待审核 Process 被放进同一个 request-root snapshot；
- 完整数据返回 `completed + clear`；
- provider/reference/source 缺失返回 `completed + findings/not_evaluable`，且 `blocker_codes` 为空；
- factorization 失败返回 `completed + findings`；
- DB/S3/进程启动故障返回 `failed`；
- 所有结果均不修改 Review 状态。
