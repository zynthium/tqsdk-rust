# AI 工作流与架构守则

本文是 Codex、Claude Code 和其他代码代理的新 session 入口。它只给出不可违反的边界、最小阅读路由和变更同步规则；不得复制完整架构说明或验证矩阵。

## 权威与最小阅读

当前代码、[`docs/architecture/`](./) 和受影响 crate 的 README 优先于 review、历史计划和归档。`docs/reviews/` 是决策输入，`docs/superpowers/` 是执行记录，`docs/archive/` 是历史记录，均不能覆盖当前架构。

| 任务 | 必读 | 按需加读 |
| --- | --- | --- |
| 文档或 AI 工作流 | 根 [`README.md`](../../README.md)、[`docs/README.md`](../README.md) | 本文、目标文档 |
| 修改具体 crate | 根 README、`docs/README.md`、目标 crate README | 相关 API / 专题文档 |
| crate 边界或 public API | [`README.md`](README.md)、[`crate-boundaries.md`](crate-boundaries.md) | 对应 `api-*.md`、contract example |
| runtime / 状态 / cursor / command | [`runtime-core/overview.md`](runtime-core/overview.md) | `runtime-core/*.md`、[`validation.md`](validation.md) |
| feature、用户入口或验证 | 根 README、目标 crate README | contract example、`validation.md` |
| Universe DSL、snapshot/timeline 或入口能力 | [`universe-language.md`](universe-language.md) | 受影响 crate README、contract example、`validation.md` |
| 历史 universe proof、artifact、retry receipt 或 plan 持久化 | [`historical-universe-catalog.md`](historical-universe-catalog.md)、[`universe-language.md`](universe-language.md) | `tqsdk-data`/`tqsdk-cache` README、`validation.md` |
| WebSocket DIFF / 状态字段 | [`diff_protocol_spec.md`](../diff_protocol_spec.md) 的目标章节 | 不全文读取协议 |
| relay history / snapshot | [`history-relay.md`](history-relay.md)、[`history-relay-http.md`](history-relay-http.md)、[`history-snapshot-manifest.md`](history-snapshot-manifest.md) | relay README、`validation.md` |

修改前先检查工作树，不覆盖已有改动。可用时先用 CodeGraph 定位代码；修改 Rust 符号前按 `AGENTS.md` 做 impact analysis。HIGH、CRITICAL 或 unresolved UNKNOWN 风险先向用户报告。真实账号、行情、交易、发布和不可逆操作始终需要显式授权及环境变量门控。

## 不可违反的架构边界

- `tqsdk-core` 拥有 protocol runtime、状态、commit/revision、cursor、adapter 与底层 contract；不拥有 facade、direct query、task 或 data 语义。
- `tqsdk` 仅是默认入口、prelude、轻量 wrapper 和 curated re-export；不拥有第二棵状态树或第二套 runtime。
- `tqsdk-session` 拥有 shared session、one-shot request/response、direct query、GraphQL、metadata、calendar、ranking、EDB、auth refresh 与 replay control。
- `tqsdk-wait` 仅做 single-owner continuous consumption；`tqsdk-task` 是执行层，`tqsdk-data` 是 research/offline data 层。
- `tqsdk-relay` 是可选 CacheOnly market relay；不进入默认 SDK 依赖路径，不扩展为通用代理或 provider 聚合。
- 可见状态变化只能走 `RuntimeHandle -> StateStore -> CommitResult -> RuntimeReader/UpdateCursor`；禁止旁路通知、私有 revision 或第二棵状态树。
- domain 写入经过 `MutationSource`；command/order 遵守 runtime 状态机，禁止 adapter 本地字符串判断绕过转换校验。
- core public surface 保持克制。完整定义见 [`crate-boundaries.md`](crate-boundaries.md) 与 [`runtime-core/`](runtime-core/)。

## Codex 多代理路由

`.codex/config.toml` 与 `.codex/agents/*.toml` 定义模型、推理深度、并发与 sandbox；本文定义委派门槛。

- 默认不委派。主代理负责全部探索、实现、验证、失败归因、风险升级与结果整合。
- 只有已确认 HIGH/CRITICAL 风险，或明确需要 public API、并发、持久化、迁移、安全的独立审查时，才使用 `architecture_reviewer`；每任务最多一个 reviewer，只允许一层委派。
- 委派显式使用 `fork_turns="none"`，任务消息自包含审查目标、必要证据、交付物和停止条件。只有 reviewer 必须解释主线程对话时才传最近必要轮次；只有必须解释完整对话历程时才使用 `all`，并写明原因。
- reviewer 只返回按严重度排序的发现、`file:line` 证据和验收缺口；不得编辑、验证或继续委派。最终判断与验证由主代理负责。
- 每次委派最终报告实际代理数、耗时、返工和质量结果；系统提供实际 token usage 时一并记录，否则标记 unavailable，不做估算。相对同类单代理基线无质量提升且总 token 未下降的路由应移除。

## 实施与同步

1. 分类：docs-only、局部实现、public API/facade、runtime contract 或架构边界变更。
2. 探索：只读取本任务路由所需上下文；用图谱工具和定向搜索，不无目的扫仓库。
3. 实现：遵守既有 crate 边界、error 类型、async 风格和测试组织；不顺手重构。
4. 验证：选择最小充分的本地、非 live 检查；完整矩阵见 [`validation.md`](validation.md)。
5. 收尾：确认改动归属。提交前执行图谱 detect changes；已闭环的 spec / plan / review 移入 `docs/archive/superpowers/`，当前架构文档不自动归档。

以下属于架构更新：新增/删除/重命名 crate；移动能力归属；改变 runtime、状态树、commit/revision/cursor、mutation 或 command lifecycle；改变 public surface、feature/依赖裁剪或 session/cache/aggregation 模型。

架构更新同轮更新本文、[`README.md`](README.md)、受影响专题文档和 crate README；用户入口变化更新根 README；AI 工作流变化更新 `AGENTS.md` / `CLAUDE.md`；验收变化更新 [`validation.md`](validation.md)。最终说明明确是否属于架构更新、更新哪些权威文档及验证结果。

## 验证入口

文档或工作流改动运行：

```bash
git diff --check
```

Rust、feature、public API、relay 与 release 检查按 [`validation.md`](validation.md) 的任务分类执行。public API、crate 拆分、feature 或 facade/runtime 消费方式变化时，相关 `crates/*/examples/api_contract_sXX_*.rs` 必须继续清晰且可编译。
