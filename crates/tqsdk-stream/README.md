# `tqsdk-stream`

`tqsdk-stream` 是建立在 `tqsdk-core + tqsdk-session` 之上的 Rust async-native stream facade。

它当前的最小职责很窄：

- 提供共享 session 驱动的 `TqStream`
- 提供多消费者 raw commit fan-out
- 提供基于 path / scope / domain / object / field 的轻量 commit 过滤
- 提供建立在 commit 过滤之上的 typed path、ready-window、账户级 trade object / trade session 事件流，以及 market / system / trade / security 对象 stream 薄包装
- 保留 `RuntimeReader` 与 `SessionClient` 作为高性能读面和 direct-query 逃生舱

它明确不负责：

- GraphQL / HTTP direct query
- schema / metadata direct facade
- downloader / `TargetPosTask` / callback
- 第二棵状态树或本地对象 cache

## 依赖方式

Cargo 包名是 `tqsdk-stream`，代码里的 crate 路径是 `tqsdk_stream`。

正式发布到 crates.io 前，workspace 外项目可以先使用 Git dependency：

```toml
[dependencies]
tqsdk-stream = { git = "https://github.com/zynthium/tqsdk-rust" }
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在本仓库内做 crate 间开发时使用 `path = "../tqsdk-stream"`；正式发布后把 Git
dependency 换成版本号即可。默认 feature 包含 live session 与 service query 支持。

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
- `QuoteSubscription`
- `MarketEvent`
- `MarketEventBuilder`
- `MarketEventStream`
- `StreamHealthSnapshot`
- `StreamHealthStatus`
- `StreamSessionPhase`
- `StreamReconnectMonitor`
- `StreamReconnectOutcome`
- `StreamReconnectReport`
- `StreamErrorDiagnostic`
- `StreamErrorKind`
- `StreamRetryPolicy`
- `StreamRetryDecision`
- `StreamRetryGiveUpReason`
- `StreamRetryReport`
- `CommitSink`
- `StreamCommitJournal`
- `StreamCommitJournalRecord`
- `StreamCommitJournalReplayReport`
- `StreamCommitJournalScope`
- `StreamCommitJournalDomain`
- `StreamSinkHandle`
- `StreamSinkStats`
- `StreamSinkShutdownReport`
- `StreamSinkOptions`
- `StreamSinkRetryPolicy`
- `StreamSinkStatus`
- `StreamSinkWalCompaction`
- `StreamSinkWalCompactionReport`
- `StreamSinkWalFsyncPolicy`
- `StreamSinkWalRecord`
- `StreamSinkWalRecordKind`
- `StreamSinkWalRecovery`
- `StreamSinkWalRecoveryReport`
- `StreamGracefulShutdown`
- `StreamGracefulShutdownReport`
- `StreamShutdownError`
- `testing::StreamTestDriver`
- `SessionReconnectEvent`
- `TradeObjectEvent`
- `TradeObjectEventStream`
- `TradeSessionEvent`
- `TradeSessionEventUpdate`
- `TradeSessionEventStream`
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
- `subscribe_quotes(...)`
- `unsubscribe_quotes(...)`
- `quotes(...).await`
- `market_events()`
- `health()`
- `StreamHealthSnapshot::status()`
- `StreamHealthSnapshot::should_restart()`
- `reconnect_monitor()`
- `StreamFacadeError::diagnostic()`
- `StreamFacadeError::is_retryable()`
- `spawn_commit_sink(...)`
- `spawn_commit_sink_with_options(...)`
- `graceful_shutdown()`
- `recover_state()`
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
- `trade_object_event_stream(...)`
- `trade_session_event_stream(...)`
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

`testing::StreamTestDriver` 只用于 deterministic fixture：注入合成 session error、
closed event，或关闭 stream driver 来刻画消费者行为。普通用户代码不应把它当成运行时
控制 API。

`CommitStream` 和 managed `CommitSink` 传递的是
`tqsdk_core::SharedCommitResult = Arc<CommitResult>`。这保持 commit payload
不可变，同时让 fan-out、过滤和 retry 复用同一份提交元数据，而不是深拷贝
`ChangeSet`。

## 设计边界

- 第一版只提供 raw commit stream，不预先冻结对象级 stream 形状
- 第二版增量先补 commit 级 path / scope / domain / object / field 过滤，不直接跳到对象级 stream
- 当前第三步已经补到 typed path、ready-window、统一 market event、账户级 trade object / trade session 事件流，以及 market/system/trade/security 单对象 stream；更高层 family API 仍未冻结
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

更多 live 示例：

- `examples/quote_stream.rs`
- `examples/quote_stream_with_session_query.rs`
- `examples/kline_stream.rs`
- `examples/trade_session_events.rs`
- `examples/api_contract_s21_slow_consumer_isolation.rs`

更完整的架构说明见 [../../docs/architecture/api-stream.md](../../docs/architecture/api-stream.md)。

## Builder 边界

`TqStreamBuilder` 只补一层和 stream facade 直接相关的便利配置，例如：

- `legacy compatibility: market_target(...)`
- `stock_market()`
- `futures_market()`
- `stock_backtest_market()`
- `futures_backtest_market()`
- `trade_target(...)`
- `trade_target_tqkq()`
- `trade_target_tqkq_numbered(<1..99>)`
- `trade_target_tqkq_stock()`
- `trade_target_tqkq_stock_numbered(<1..99>)`
- `trade_target_with_url(...)`
- `replay_url(...)`
- `commit_channel_capacity(...)`

优先使用这些命名方法，而不是直接写 `market_target(bool, bool)` 这种裸布尔组合。
`market_target(...)` 仅保留兼容用途，不应作为新的推荐入口。

如果需要更细的 session 级配置，例如 direct query、schema 或其他未来扩展项，应先配置 `tqsdk_session::SessionClientBuilder`，再通过 `TqStreamBuilder::from_session_builder(...)` 包装成 stream facade。
如果调用方已经持有 `SessionClient`，可以直接使用 `TqStream::new(session)`；
需要显式设置 root fan-out 容量时使用
`TqStream::with_commit_channel_capacity(session, capacity)`。

如果要证明 stream facade 可以复用同一个底层 session 做一次性 metadata/direct query，而不需要额外建第二个 client，可参考 `examples/quote_stream_with_session_query.rs`。

quote stream 的订阅意图可以通过 `subscribe_quotes(...)` /
`unsubscribe_quotes(...)` 表达，普通用户不需要直接提交
`RuntimeCommand::Market(MarketCommand::SubscribeQuotes { .. })`。

多合约动态 quote 订阅优先使用 `quotes(...).await` 返回的
`QuoteSubscription`。它持有当前 symbol 集合，提供 `add(...)` /
`remove(...)` / `symbols()` / `close()`，并作为 typed quote stream 使用。
底层仍复用 market adapter 的全量订阅集合和同一条 commit fan-out；session
reconnect/resync 后，runtime 会根据 adapter 保留的订阅意图重新排队发送恢复命令，
用户不需要手写重连后的重订阅逻辑。

如果同一个用户循环需要同时处理 quote、tick window 和 kline window，优先使用
`market_events()` 构造统一 `MarketEventStream`。它仍然只是一层 facade：
内部提交 quote/chart 命令，并从同一条 commit fan-out 中投影 typed event；不维护
第二棵状态树，也不复制 direct-query 能力。

如果 async 系统在启动阶段需要等待行情订阅和交易初始同步完成，可以使用
`TqStream::recover_state()`。它从同一条 commit fan-out 等待 readiness，并复用
`tqsdk-session` 的 `StartupRecoverySpec` 判断状态，不要求用户手写 channel 或
provider 级恢复 flag。

生产守护进程如果只需要 typed health snapshot，可以调用 `TqStream::health()`。
返回的 `StreamHealthSnapshot` 包含 runtime revision、session phase、最近一次
reconnect diagnostics 和 stream driver closed 状态，并提供
`status()` / `should_restart()` 作为生产指标和日志的最小判定。需要在生产守护
进程中等待现有 session 重连恢复结果时，可以使用 `TqStream::reconnect_monitor()`
得到 `StreamReconnectReport`，区分 already healthy、recovered、exhausted、
timed out 和 closed；它只消费同一条 commit fan-out 与 health snapshot，不接管
底层 reconnect 执行。需要显式关闭 stream driver 并 flush managed sink 时，可以使用
`TqStream::graceful_shutdown()`，把 `StreamSinkHandle` 交给 shutdown coordinator 后得到
`StreamGracefulShutdownReport`。daemon-level ctrl-c signal 位于 `tqsdk-task`
的 strategy supervisor；跨进程 daemon 管理仍属于后续 daemon/tooling 能力。Rust
SDK 不规划 GUI、web helper 或内置 HTTP health/metrics endpoint。

慢消费者隔离的底层配置通过 `TqStreamBuilder::commit_channel_capacity(...)`
或已有 session 场景下的 `TqStream::with_commit_channel_capacity(...)`
表达。每个 `commit_stream()` consumer 仍持有独立 receiver；落后时通过
`StreamFacadeError::Lagged` 和 `StreamFacadeError::diagnostic()` 暴露 typed lag
信息。写库 / 日志这类非核心消费者可以通过 `TqStream::spawn_commit_sink(...)`
交给 SDK 托管，sink 在独立 consumer task 中消费 commit，并通过
`StreamSinkStats` / `StreamSinkShutdownReport` 暴露 processed / lagged / errors /
retry_attempts / wal_records / journal_records 和 flush 结果。需要有限重试或本地 JSONL WAL 时，
使用 `TqStream::spawn_commit_sink_with_options(...)` 传入 `StreamSinkOptions`、
`StreamSinkRetryPolicy` 和 `jsonl_wal(...)`。如果本地 WAL 需要更强落盘语义，可配置
`StreamSinkWalFsyncPolicy::EveryRecord`；如果需要裁剪本地 JSONL WAL，可使用
`StreamSinkWalCompaction` 按 revision 保留记录并返回 typed compaction report。
如果新进程需要审计旧 WAL，可使用 `StreamSinkWalRecovery` 扫描 delivered /
pending / failed revisions、lagged records 和 flush failures；它只基于 WAL
元数据生成 report。需要让后续进程按 revision checkpoint 重放 sink commit
元数据时，可用 `StreamSinkOptions::jsonl_commit_journal(...)` 写入本地 commit
journal，并用 `StreamCommitJournal::replay_jsonl(...)` 重放到 `CommitSink`。这个
foundation 不是完整 durable daemon queue，也不恢复 runtime state snapshot。

错误诊断的低层 contract 通过 `StreamFacadeError::diagnostic()` 与
`tqsdk-session` / `tqsdk-core` 的 `RetryHint` 贯通。`StreamRetryPolicy` 可以把
typed diagnostic 转换成 `StreamRetryDecision`，并为 stream-facing fallible
operation 提供一个最小 async retry runner。它不接管底层 reconnect executor，
也不解释业务拒单；订单 intent 幂等、风控拒单和交易拒单仍应走 wait/task 的
typed order/risk surface。

如果 trade session 走官方内置模拟账户，登录命令也可以直接从共享 session 里派生：

- `stream.session().tqkq_login_command().await`
- `stream.session().tqkq_login_command_numbered(<1..99>).await`
- `stream.session().tqkq_stock_login_command().await`
- `stream.session().tqkq_stock_login_command_numbered(<1..99>).await`
