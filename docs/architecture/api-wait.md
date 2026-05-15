# `tqsdk-api-wait` 专题设计

## 文档定位
本文档描述的是建立在 V1 runtime contract 之上的 wait consumption adapter。

它不是 V1 的基础契约，而是 V2+ 的用户态适配层，目标是：

- 用同一个 `RuntimeReader` / `UpdateCursor` 构建 Python 风格的单 owner 推进语义
- 提供 `TqApi`、typed handles、`snapshot()`、`WaitStep::is_changing()` 等 facade
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
- `SharedCommitResult`
- `ChangeSet`
- `UpdateCursor`
- `StatePath` / `ObjectKey`
- `StateSnapshot`（仅在 facade 明确需要 detached owned snapshot 时）

如果 wait adapter 需要额外内核能力，说明 V1 contract 设计仍然不完整。

## 核心目标
- 兼容 Python TqSdk 最重要的策略语义
- 保住两次 `step()` 之间的稳定状态截面
- 让 `WaitStep::is_changing()` 成为对当前已消费 commit 的查询

## 非目标
- 不定义 runtime 的提交边界
- 不改变 revision 推进规则
- 不在 adapter 层重新维护另一份状态树

## 未来 facade 形状
wait adapter 层未来可以提供：

- `TqApi`
- `QuoteView` / `QuoteSnapshot`
- `AccountView` / `OrderView` / `PositionView`
- `step()` / `step_until(...)`
- `WaitStep::is_changing()`

但这些都只是对 runtime contract 的消费包装，不属于 V1。

## 关键约束
- `wait_update()` 只消费 `RuntimeReader::next()` / `UpdateCursor`，不生成 commit
- facade 如提供 `snapshot()`，默认应建立在某个已提交 revision 的借用读视图之上
- 只有明确需要 detached ownership 时，才应退回 `StateSnapshot` clone 路径
- `WaitStep::is_changing()` 只解释当前 `step()` / `step_until(...)` 成功消费到的 commit
- 所有 timeout / 初始 ready / 重连行为都必须建立在同一 commit 模型上
- 一次性行情快照 helper 如果存在于示例或用户代码中，应只是薄封装：
  通过同一个 session 创建 `quote` handle，通过同一个 `RuntimeReader` 和
  `step_until(...)` 等待 ready snapshot，并且不得绕过 `WaitStep`
  的 commit 边界。
- `insert_order(..., OrderPrice)` / `insert_limit_order(...)` 这类 typed trade
  helper 可以作为 wait facade 的薄便利层存在，用来从用户路径移除
  `serde_json::Value` 价格参数和 `"BEST"` / `"FIVELEVEL"` 这类魔法字符串；
  它仍必须只提交到底层 command contract，不做本地伪造订单状态或第二棵
  trade state。
- `limit_order(...).client_intent(...).send_once()` 这类订单 intent helper
  可以作为 wait facade 的薄便利层存在，用来把用户稳定 intent id 映射到
  runtime `order_id`，并通过 `tqsdk-session` 的 session-scoped intent ledger
  防止同一 session 内相同 intent 重复提交；`OrderTicket::status()` /
  `wait_reconnect_safe_terminal*()` 可以把 runtime command ledger 和 order
  lifecycle 合并成 typed `OrderTicketState`；`OrderTicket::wait_partially_filled*()`
  和 `cancel_remaining()` 只是委托内部 `OrderRef`，让 reconnect-safe intent 路径
  也能自然完成部分成交撤单流程。它不得声明已经完成跨进程持久恢复，也不得聚合
  成交明细或执行组状态；这些属于后续 execution/task 层能力。
- `login_trade_account(...)` 这类 typed login helper 可以作为 wait facade
  的薄便利层存在，用来从用户路径移除 `TradeLoginCommand` 构造；builder
  仍负责 trade route 配置，helper 只提交 runtime trade login command 并等待
  同一 trade state 分区里的账户对象 ready。
- `startup_recovery()` 这类启动屏障 helper 可以作为 wait facade 的薄便利层
  存在，用来把 quote 订阅和 trade 初始同步合并成用户级 ready barrier；ready
  判定复用 `tqsdk-session::StartupRecoverySpec`，不得绕过 runtime state tree，
  也不得把 provider 私有恢复状态暴露给业务代码。
- `OrderRef::cancel_remaining()`、`wait_partially_filled()`、
  `wait_terminal()` 这类订单 helper 可以作为 wait facade 的薄便利层存在；
  它们必须只读取 `read_trade_state()` 暴露的 typed order 状态并复用
  `TqApi::wait_update()` 推进，不得绕过 runtime order lifecycle 校验，也不得
  新增本地订单 overlay。
- `tqsdk_wait::testing::WaitTestDriver` 只属于 deterministic fixture 支持面；
  它可以刻画 wait guard 和 deferred commit 行为，但不得演变成普通用户的运行时
  控制入口，也不得成为绕过 `TqApi::wait_update()` 的第二套状态推进模型。

## 为什么单独保留这份文档
因为 Python 兼容性的大部分难点都在 wait facade 层，而不在 V1 contract 层。

把它单独拆出来，可以避免为追求 Python 用法而过早污染基础内核。
