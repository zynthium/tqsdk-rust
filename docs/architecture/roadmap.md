# 分层成长路线

## 为什么 `wait_update` 仍应是主范式
- 更接近真实量化主循环
- 更容易保证一致性
- 更适合作为“最严格消费者”

## 性能与适用场景
### `wait_update` 更适合
- 多对象联合决策
- 高频 burst 更新下的批量合并
- 多策略共享状态
- 回测与实盘一致性验证

### stream/callback 更适合
- UI、告警、日志、落盘
- 与异步管道集成
- 单对象单事件的外围派发

## 演进路线
### V1：Runtime Kernel
包含：
- `Transport`
- `AuthProvider`
- `SessionLifecycle`
- `SessionBootstrap`
- `SubscriptionRegistry`
- `RuntimeInput`
- `StateStore`
- `Revision`
- `ChangeSet`
- `StateSnapshot`
- `CommitResult`
- `wait_next_commit()`

### V2：`tqsdk-api-wait`
完整专题设计见：

- [api-wait.md](api-wait.md)
- [validation.md](validation.md)

- `TqApi`
- `wait_update()`
- `QuoteView`
- `snapshot()`
- `is_changing()`

### V3：`tqsdk-api-stream`
- `commit_stream()`
- 对象级 stream 投影

### V4：`tqsdk-api-callback`
- `on_commit`
- `on_quote`
- `on_order`
- `on_new_bar`

### V5：更高层工具层
- `TargetPosTask`
- `TradeSession`
- `SeriesApi`
- 多账户会话管理
- 回测 facade

## 实现建议
1. 先做 `tqsdk-diff-core`
2. 再做 `tqsdk-runtime-core`
3. 优先做 `tqsdk-api-wait`
4. 最后补 stream/callback
