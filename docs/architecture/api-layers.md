# Runtime Contract 与后续 Adapter 分层

## 设计原则
future API 层的职责是消费 runtime contract 的提交结果，而不是重新定义状态机。

共同点：
- 都建立在同一份 `CommitResult`
- 都读取同一份 `StateSnapshot`
- 都遵守同一套 `Revision / ChangeSet` 语义
- 都通过 `UpdateCursor` / `CommitLog` 消费提交结果

## V1 只交付什么
V1 只交付两类稳定 public contract：

1. runtime contract
   - `RuntimeHandle`
   - `RuntimeCommand`
   - `RuntimeInput`
   - `StateSnapshot`
   - `CommitResult`
   - `UpdateCursor`
2. protocol adapter contract
   - `ProtocolAdapter`
   - `ProtocolDomain`
   - `NormalizedMutation`
   - `OutboundRequest`

V1 不交付任何用户态 facade。

## V2+ 才出现的 adapter 层
### `tqsdk-api-wait`
建立在：

- `RuntimeHandle::cursor()`
- `CommitLog`
- `StateSnapshot`
- `ChangeSet`

目标：
- 构建 Python 风格的 `wait_update()` 语义
- 提供 `TqApi`、views、snapshots、`is_changing()` 等 facade

约束：
- 不得回改 commit 生成逻辑

### `tqsdk-api-stream`
```rust
pub trait CommitStreamApi {
    type CommitStream: Stream<Item = CommitResult>;
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
        F: Fn(CommitResult) + Send + Sync + 'static;
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
- 它们的实现差异只能体现在消费模型，不能体现在状态提交模型
