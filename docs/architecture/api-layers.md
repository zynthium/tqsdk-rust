# Runtime Contract 与后续 Adapter 分层

## 设计原则
future API 层的职责是消费 runtime contract 的提交结果，而不是重新定义状态机。

共同点：
- 都建立在同一份不可变 `CommitResult` payload 上；commit 发布和 cursor 消费用
  `SharedCommitResult = Arc<CommitResult>` 共享所有权
- 都读取同一棵 runtime state tree
- 都通过 `RuntimeReader::read()` / `SnapshotReadGuard` 获得 revision-bound 读视图
- 都遵守同一套 `Revision / ChangeSet` 语义
- 都通过 `RuntimeReader::cursor()` / `RuntimeReader::next()` 消费提交结果

兼容层仍可直接使用 `StateSnapshot` / `CommitLog`，但这不是 V1 的主叙事。

## V1 只交付什么
V1 只交付两类稳定 public contract：

1. runtime contract
   - `RuntimeHandle`
   - `RuntimeReader`
   - `SnapshotReadGuard`
   - `StateReadView`
   - `RuntimeCommand`
   - `RuntimeInput`
   - `CommitResult`
   - `SharedCommitResult`
   - `UpdateCursor`
   - `StateSnapshot` / `CommitLog`（兼容与底层原语）
2. protocol adapter contract
   - `ProtocolAdapter`
   - `ProtocolDomain`
   - `NormalizedMutation`
   - `OutboundRequest`
   - `OutboundDispatch`

Raw runtime outbox envelopes and multi-source aggregation helpers are not part of the V1 public contract; low-level route consumers should use `OutboundDispatch` and reader/cursor primitives.

官方 schema type 是 runtime contract 的一部分，而不是 facade 层 overlay。期货
`Order` / `Trade` 的方向、开平字段和 `Order.price_type` 应在 core 中解码为
`TradeDirection`、`TradeOffset`、`TradePriceType` 的可选枚举；缺失值保留为
`None`，但已知协议字段不再暴露为裸 `String`。

V1 不交付任何用户态 facade。

## V2+ 才出现的 adapter 层
### `tqsdk-api-wait`
建立在：

- `RuntimeHandle::reader()`
- `RuntimeReader::cursor()`
- `RuntimeReader::next()`
- `SnapshotReadGuard` / `StateReadView`
- `ChangeSet`

目标：
- 构建 Python 风格的单 owner 推进语义
- 提供 `TqApi`、typed handles、snapshots、`WaitStep::is_changing()` 等 facade

约束：
- 不得回改 commit 生成逻辑

### `tqsdk-api-stream`
```rust
pub trait CommitStreamApi {
    type CommitStream: Stream<Item = SharedCommitResult>;
    fn commit_stream(&self) -> Self::CommitStream;
}
```

适合：
- tokio/Stream 生态
- 数据管道
- 异步 fan-out

约束：
- 只是 cursor/log 的连续消费包装
- 不得绕开 `Revision / ChangeSet`

### `tqsdk-api-callback`
```rust
pub trait CallbackApi {
    async fn on_commit<F>(&self, f: F)
    where
        F: Fn(SharedCommitResult) + Send + Sync + 'static;
}
```

适合：
- UI
- 监控
- 通知与落盘

约束：
- callback 只是 commit 的旁路消费形式
- 不得提前成为 runtime 的主驱动接口

## 关键判断
- `wait` / stream / callback 是并列的消费 adapter
- 它们都晚于 runtime contract
- 它们的实现差异只能体现在 cursor 消费与读视图包装，不能体现在状态提交模型
