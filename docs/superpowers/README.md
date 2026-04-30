# Superpowers Specs And Plans

本目录保存 agentic 工作流产生的 spec 和 implementation plan。

## 职责边界

- [`specs/`](specs/)：设计阶段产物，用于记录已确认的需求、方案和约束。
- [`plans/`](plans/)：执行阶段产物，用于拆分任务、记录验证命令和阶段性决策。

这些文档是过程记录，不是当前架构权威。AI 助手可以用它们理解为什么做过某个改动，但不能用历史 plan 覆盖以下来源：

1. 当前代码。
2. [`../architecture/ai-workflow.md`](../architecture/ai-workflow.md)。
3. [`../architecture/README.md`](../architecture/README.md) 和相关专题架构文档。
4. crate 级 `README.md` 与可编译 `api_contract_sXX_*.rs` examples。

## 使用规则

- 执行计划前先确认计划仍然符合当前代码和架构文档。
- 如果计划引用的路径已经移动，以 [`../README.md`](../README.md) 和 [`../reviews/README.md`](../reviews/README.md) 为准。
- 如果计划要求修改 public API，必须同步检查 [`../reviews/public-api-scenario-review.md`](../reviews/public-api-scenario-review.md)、[`../reviews/public-api-disposition-matrix.md`](../reviews/public-api-disposition-matrix.md)、相关架构专题文档和 crate README。
- 如果计划中的判断与当前代码不一致，应更新计划或创建新计划，不要盲目执行旧步骤。
