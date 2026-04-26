# `tqsdk-api-wait` 专题设计

## 文档定位
本文档描述的是建立在 V1 runtime contract 之上的 `wait_update` consumption adapter。

它不是 V1 的基础契约，而是 V2+ 的用户态适配层，目标是：

- 用同一个 `RuntimeReader` / `UpdateCursor` 构建 Python 风格的 `wait_update()` 语义
- 提供 `TqApi`、typed views、`snapshot()`、`is_changing()` 等 facade
- 不回改 runtime core 的提交模型

相关文档：

- [总架构入口](README.md)
- [Python / Rust facade 范式对比](facade-paradigms.md)
- [runtime-core 总览](runtime-core/overview.md)
- [协议交互](runtime-core/protocol-flow.md)
- [数据契约](runtime-core/data-contracts.md)
- [类型约束](runtime-core/type-system.md)
- [验收与测试矩阵](validation.md)

## 对 runtime contract 的依赖
`tqsdk-api-wait` 必须只依赖这些基础能力：

- `RuntimeHandle`
- `RuntimeReader`
- `SnapshotReadGuard` / `StateReadView`
- `CommitResult`
- `ChangeSet`
- `UpdateCursor`
- `StatePath` / `ObjectKey`
- `StateSnapshot`（仅在 facade 明确需要 detached owned snapshot 时）

如果 wait adapter 需要额外内核能力，说明 V1 contract 设计仍然不完整。

## 核心目标
- 兼容 Python TqSdk 最重要的策略语义
- 保住两次 `wait_update()` 之间的稳定状态截面
- 让 `is_changing()` 成为对最近一次已消费 commit 的查询

## 非目标
- 不定义 runtime 的提交边界
- 不改变 revision 推进规则
- 不在 adapter 层重新维护另一份状态树

## 未来 facade 形状
wait adapter 层未来可以提供：

- `TqApi`
- `QuoteView` / `QuoteSnapshot`
- `AccountView` / `OrderView` / `PositionView`
- `wait_update()`
- `is_changing()`

但这些都只是对 runtime contract 的消费包装，不属于 V1。

## 关键约束
- `wait_update()` 只消费 `RuntimeReader::next()` / `UpdateCursor`，不生成 commit
- facade 如提供 `snapshot()`，默认应建立在某个已提交 revision 的借用读视图之上
- 只有明确需要 detached ownership 时，才应退回 `StateSnapshot` clone 路径
- `is_changing()` 只解释最近一次成功消费到的 commit
- 所有 timeout / 初始 ready / 重连行为都必须建立在同一 commit 模型上
- 一次性 `quote_snapshot(symbol, deadline)` 这类 helper 可以作为 wait facade
  的薄便利层存在，但必须仍通过同一个 session 提交订阅、通过同一个
  `RuntimeReader` 等待 ready snapshot，并且不得偷改用户随后看到的
  `last_commit()` / `is_changing()` 语义。

## 为什么单独保留这份文档
因为 Python 兼容性的大部分难点都在 wait facade 层，而不在 V1 contract 层。

把它单独拆出来，可以避免为追求 Python 用法而过早污染基础内核。
