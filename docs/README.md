# 文档索引

本文档是仓库级文档入口。它负责说明各类文档的职责边界，避免后续人工或 AI 代码助手把历史审查记录、执行计划和当前架构权威混用。superpowers 里的 spec / plan 属于执行记录，完成代码修改后应默认进入 archive，不要长期留在活跃目录。

## 权威层级

1. [`architecture/ai-workflow.md`](architecture/ai-workflow.md)：AI 代码助手的新 session 入口和硬性架构守则。
2. [`architecture/README.md`](architecture/README.md)：当前分层架构、crate 职责、runtime contract 与 API 归属总览。
3. `architecture/api-*.md`、`architecture/runtime-core/*.md`、[`architecture/validation.md`](architecture/validation.md)：专题设计和验证矩阵。
4. `crates/*/README.md` 与 `crates/*/examples/api_contract_sXX_*.rs`：crate 级用户入口和可编译 public API 契约。
5. [`scenarios/`](scenarios/) 与 [`reviews/`](reviews/)：场景审查、API gap、public API 决策记录。它们用于指导迭代，但不能覆盖 `architecture/` 的权威边界。
6. [`archive/`](archive/)：历史审查输入、已闭环 spec 与 plan。[`superpowers/`](superpowers/) 只保留仍在执行或需要继续跟踪的 agentic 记录。两者都不是当前架构权威。

## 目录分工

| 目录 | 职责 | 使用规则 |
| --- | --- | --- |
| [`architecture/`](architecture/) | 当前架构权威、crate 边界、runtime contract、API 归属和验证矩阵 | 改动 crate 边界、public API、runtime 状态模型或 facade 归属时必须同步检查 |
| [`scenarios/`](scenarios/) | 用户场景、API gap sketch、使用者分层迭代顺序 | gap 修复后要提升为 `crates/*/examples/api_contract_sXX_*.rs` 并更新 review |
| [`reviews/`](reviews/) | 当前仍有决策价值的审查和 public API disposition 记录 | 作为计划输入和决策证据；与架构文档冲突时以 `architecture/` 为准 |
| [`archive/`](archive/) | 已闭环或已转化为计划的历史审查输入、已归档 gap sketch、旧 spec 与闭环 plan | 只作追溯，不直接驱动代码改动 |
| [`superpowers/`](superpowers/) | 当前仍在执行或需要继续跟踪的 agentic specs / plans | 闭环后迁入 `archive/superpowers/`；计划中的旧判断不能覆盖当前代码和架构文档 |

## 操作指南

- [回测 Tick 持久缓存预热与验收](architecture/backtest-tick-cache-operations.md)：按已完成交易日
  增量填充共享 TQBN cache，或用显式当前结束日期自动保存 provisional 快照，并用 CacheOnly 和
  实际回放验证 final coverage。
- [回测 Tick Cache CLI](architecture/backtest-tick-cache-cli.md)：可选 `tqsdk-cache` binary 的
  默认文本摘要 / 按需 versioned JSON、calendar-aware closed-day fill、显式当前结束日期自动
  provisional fill、`--require-final` 严格保护、selectable stderr progress、report-bound verify
  与 TQBN doctor 合同。
- [回测历史查询与缓存来源](architecture/api-data.md#回测历史查询与缓存来源)：`BacktestHistoryClient`
  的 request/chunk/terminal contract、Tick/15s/60s/整数分钟 K 的 durable-source matrix、metadata
  sidecar 和 CacheOnly 语义。

## AI 助手读取顺序

新 session 开始代码改动前必须先读：

1. [`architecture/ai-workflow.md`](architecture/ai-workflow.md)
2. 根 [`README.md`](../README.md)
3. 本文档
4. [`architecture/README.md`](architecture/README.md)
5. 受影响 crate 的 `README.md`
6. 受影响专题文档、scenario review 或 superpowers plan

如果审查记录、plan 或 archived report 与当前代码或 `architecture/` 不一致，应先核对代码和架构文档，再把审查建议转化为新的计划；不要直接按历史报告改代码。
