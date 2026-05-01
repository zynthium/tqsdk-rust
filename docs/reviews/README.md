# 审查与 Public API 决策记录

本目录保存仍然会影响后续迭代判断的审查记录。它们不是架构权威；当这里的建议与 `docs/architecture/*` 或当前代码冲突时，必须先核对架构文档和代码，再决定是否创建新的计划。

## 当前记录

| 文档 | 职责 |
| --- | --- |
| [`public-api-scenario-review.md`](public-api-scenario-review.md) | 场景驱动 public API 表达能力审查，记录哪些场景自然、勉强或暂缓 |
| [`public-api-disposition-matrix.md`](public-api-disposition-matrix.md) | public API 符号级 disposition gate，区分 keep、internalize、needs-arch-change 和 split-plan |

## 历史输入

2026-04-29 的 Claude Code 原始审查输入已经归档到 [`../archive/reviews/2026-04-29/`](../archive/reviews/2026-04-29/)：

- [`public-api-overdesign-audit.md`](../archive/reviews/2026-04-29/public-api-overdesign-audit.md)
- [`review-2026-04-29-pending.md`](../archive/reviews/2026-04-29/review-2026-04-29-pending.md)

2026-04-30 的全面审查已闭环并归档到 [`../archive/reviews/2026-04-30/comprehensive-review-2026-04-30.md`](../archive/reviews/2026-04-30/comprehensive-review-2026-04-30.md)。

这些文件保留原始问题背景和闭环状态，但不再作为直接改代码的入口。
