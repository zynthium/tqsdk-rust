# tqsdk-api-* 扩展层

## 设计原则
`api-*` 的定位是消费内核结果，而不是重新定义状态机。

共同点：
- 都建立在同一份 `CommitResult`
- 都读取同一份 `StateSnapshot`
- 都遵守同一套 `Revision / ChangeSet` 语义

## `tqsdk-api-wait`
wait 范式的完整专题设计见：

- [api-wait.md](api-wait.md)

适合：
- 多对象联合决策
- 策略主循环
- 回测与实盘统一
- 强一致性读取

## `tqsdk-api-stream`
```rust
pub trait CommitStreamApi {
    type CommitStream: Stream<Item = CommitResult>;
    fn commit_stream(&self) -> Self::CommitStream;
}
```

适合：
- tokio/Stream 生态集成
- 函数式数据处理
- 异步管道

## `tqsdk-api-callback`
```rust
pub trait CallbackApi {
    async fn on_commit<F>(&self, f: F)
    where
        F: Fn(CommitResult) + Send + Sync + 'static;
}
```

适合：
- UI / 监控
- 告警与通知
- 数据落盘
- 旁路型消费任务
