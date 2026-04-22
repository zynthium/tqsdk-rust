# `tqsdk-stream`

`tqsdk-stream` 是建立在 `tqsdk-core + tqsdk-session` 之上的 Rust async-native stream facade。

它当前的最小职责很窄：

- 提供共享 session 驱动的 `TqStream`
- 提供多消费者 raw commit fan-out
- 提供基于 path / scope / domain / object / field 的轻量 commit 过滤
- 提供建立在 commit 过滤之上的 typed path、ready-window、账户级 trade object 事件流，以及 market / system / trade / security 对象 stream 薄包装
- 保留 `RuntimeReader` 与 `SessionClient` 作为高性能读面和 direct-query 逃生舱

它明确不负责：

- GraphQL / HTTP direct query
- schema / metadata direct facade
- downloader / `TargetPosTask` / callback
- 第二棵状态树或本地对象 cache

## 当前公开面

当前最小 surface 包含：

- `TqStreamBuilder`
- `TqStream`
- `CommitStream`
- `PathCommitStream`
- `ScopeCommitStream`
- `DomainCommitStream`
- `ObjectCommitStream`
- `FieldCommitStream`
- `PathValueStream<T>`
- `KlineWindow`
- `TickWindow`
- `KlineWindowStream`
- `TickWindowStream`
- `PositionEventStream`
- `PreInsertOrderEventStream`
- `OrderEventStream`
- `TradeEventStream`
- `RiskManagementRuleEventStream`
- `RiskManagementDataEventStream`
- `SettlementInfoEventStream`
- `SecurityPositionEventStream`
- `SecurityOrderEventStream`
- `SecurityTradeEventStream`
- `ValueUpdate<T>`
- `commit_stream()`
- `CommitStream::filter_path(s)`
- `CommitStream::filter_scope(s)`
- `CommitStream::filter_domain(s)`
- `CommitStream::filter_object(s)`
- `CommitStream::filter_fields(...)`
- `path_stream::<T>(...)`
- `quote_stream(...)`
- `trading_status_stream(...)`
- `kline_stream(...)`
- `tick_stream(...)`
- `KlineWindowStream::close()`
- `TickWindowStream::close()`
- `notification_stream(...)`
- `account_stream(...)`
- `position_stream(...)`
- `pre_insert_order_stream(...)`
- `position_event_stream(...)`
- `pre_insert_order_event_stream(...)`
- `order_stream(...)`
- `trade_stream(...)`
- `order_event_stream(...)`
- `trade_event_stream(...)`
- `risk_management_rule_stream(...)`
- `risk_management_data_stream(...)`
- `settlement_info_stream(...)`
- `risk_management_rule_event_stream(...)`
- `risk_management_data_event_stream(...)`
- `settlement_info_event_stream(...)`
- `security_account_stream(...)`
- `security_position_stream(...)`
- `security_order_stream(...)`
- `security_trade_stream(...)`
- `security_position_event_stream(...)`
- `security_order_event_stream(...)`
- `security_trade_event_stream(...)`
- `reader()`
- `session()`
- `into_session()`

## 设计边界

- 第一版只提供 raw commit stream，不预先冻结对象级 stream 形状
- 第二版增量先补 commit 级 path / scope / domain / object / field 过滤，不直接跳到对象级 stream
- 当前第三步已经补到 typed path、ready-window、账户级 trade object 事件流，以及 market/system/trade/security 单对象 stream；统一 trade session 事件流与更高层 family API 仍未冻结
- `kline/tick` 的远端 chart 生命周期当前采用显式 `close()`，不做隐式 async drop
- commit fan-out 的语义必须直接来自 `RuntimeReader::next()`
- 背压通过 bounded broadcast ring 显式暴露为 `Lagged`
- one-shot query / schema / metadata 始终留在 `tqsdk-session`

## 示例

```rust
use futures::StreamExt;
use tqsdk_stream::TqStreamBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let stream = TqStreamBuilder::new(user, pass).build().await?;
    let mut commits = stream.commit_stream()?;

    while let Some(update) = commits.next().await {
        let commit = update?;
        let snapshot = stream.reader().read();
        println!("revision={} scope={:?}", commit.revision, commit.scope);
        println!("head={}", snapshot.revision());
    }

    Ok(())
}
```

更完整的架构说明见 [../../docs/architecture/api-stream.md](../../docs/architecture/api-stream.md)。
