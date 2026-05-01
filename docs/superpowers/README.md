# Superpowers Specs And Plans

本目录现在只保留历史入口说明；活跃 spec / plan 已移到 `docs/archive/superpowers/`。

## 职责边界

- [`../archive/superpowers/`](../archive/superpowers/)：历史 spec 与 implementation plan 的归档入口。
- `docs/architecture/*`：当前架构权威。

不要把已经闭环的 spec/plan 再放回本目录；如果需要追溯，请直接看归档区。

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
