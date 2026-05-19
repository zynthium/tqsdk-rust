# `tqsdk-stream` 最小 API 草图

## 文档定位
本文档描述的是建立在 `tqsdk-core + tqsdk-session` 之上的 Rust async-native continuous-consumption facade。

它的目标不是复制 `tqsdk-rs` 现有宽 public surface，也不是把 callback、task、direct query 重新揉进一个大而全的 `TqApi`。

当前这份文档只回答三个问题：

- `tqsdk-stream` 的最小 canonical API 应该长什么样
- 第一版最小实现应该先交付什么，不该交付什么
- 它如何在不污染 `tqsdk-core` 提交模型的前提下，提供多消费者异步消费能力

相关文档：

- [总架构入口](README.md)
- [crate 边界审计](crate-boundaries.md)
- [未来 crate 蓝图](crate-blueprint.md)
- [Python / Rust facade 范式对比](facade-paradigms.md)
- [wait facade 设计](api-wait.md)

## 设计目标

- 提供 Rust async-native 的连续 commit 消费形状
- 保持和 `tqsdk-wait` 相同的底层语义来源：`RuntimeReader + UpdateCursor + SessionClient`
- 允许多消费者各自独立推进，不强制单 owner `wait_update()`
- 保留高性能用户直接读取共享状态树的能力
- 不复制第二棵状态树
- 不把 direct query / schema / metadata 搬进来

## 非目标

第一版 `tqsdk-stream` 明确不负责：

- GraphQL / HTTP query
- schema refresh / metadata / calendar / settlement / ranking 这些 one-shot query
- callback facade
- `TargetPosTask`
- downloader / DataFrame / polars
- 自己维护 object cache / watcher registry / 第二棵状态树

## 先给结论

推荐的最小设计不是“先做很多对象级 stream”，而是：

1. 先提供一个共享 session 驱动的 `CommitStream`
2. 再暴露共享 `RuntimeReader` / `SessionClient` 作为读面与逃生舱
3. 让后续的对象级 stream、路径过滤、trade 可靠事件流都建立在同一条 commit fan-out 之上

换句话说，第一版 `tqsdk-stream` 的最小稳定内核应当是：

- 一个 `TqStreamBuilder`
- 一个 `TqStream`
- 一个 `CommitStream`
- 一个显式的 lag / closed error surface
- 一个仅用于 deterministic fixture 的 `testing::StreamTestDriver`

而不是一开始就铺开：

- `QuoteStream`
- `KlineStream`
- `OrderStream`
- `TradeEventStream`
- path watcher
- callback bridge

这些能力都应该建立在最小 commit stream 先稳定之后再往上叠。

`testing::StreamTestDriver` 只用于测试中注入合成 driver close/session error，
不得作为普通用户的运行时控制 API，也不得暴露私有 channel handle 或第二棵状态树。

## 为什么不从对象级 stream 起步

### 方案 A：commit-first

形状：

```rust
let stream = TqStreamBuilder::new(user, pass).build().await?;
let mut commits = stream.commit_stream()?;

while let Some(update) = commits.next().await {
    let commit = update?;
    let snapshot = stream.reader().read();
    // 用户自己决定读哪些对象
}
```

优点：

- public surface 最小
- 和 `tqsdk-core` 的 commit/revision 语义完全一致
- 后续对象级 facade、过滤器、事件流都能建立其上
- 不需要一开始就决定“对象级 stream 到底返回 commit、返回 snapshot、还是返回 typed value”

缺点：

- 初期对终端用户不够便利
- 需要调用方自己根据 commit 和 state tree 解释变化

### 方案 B：对象级 stream-first

形状：

```rust
let quote = stream.quote_stream("SHFE.au2602")?;
let order = stream.order_stream("sim", "order-1")?;
```

优点：

- 用户更直观
- 更接近现有 `tqsdk-rs` 的某些使用形状

缺点：

- 一开始就必须冻结大量 API 形状
- 容易把对象缓存、订阅生命周期、过滤语义、背压策略一起绑死
- 容易过早把 crate 做宽

### 方案 C：可靠事件流-first

形状：

```rust
let mut trades = stream.trade_events("sim")?;
```

优点：

- 对交易场景很有吸引力

缺点：

- 会过早把“状态流”和“事件流”的分层绑死
- 对 market/query/schema/replay 不形成统一消费主线

### 推荐

第一版应选择方案 A：`commit-first`。

原因不是它最方便，而是它最稳，且最符合你当前对底座的要求：

- 精简
- 稳定
- 高性能
- 先锁定真正的公共抽象，再叠加便利层

## 最小 canonical API

### builder

```rust
pub struct TqStreamBuilder {
    inner: tqsdk_session::SessionClientBuilder,
}

impl TqStreamBuilder {
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self;
    pub fn from_session_builder(inner: SessionClientBuilder) -> Self;

    // legacy compatibility only; prefer the named market selectors below
    pub fn market_target(self, stock: bool, backtest: bool) -> Self;
    pub fn stock_market(self) -> Self;
    pub fn futures_market(self) -> Self;
    pub fn stock_backtest_market(self) -> Self;
    pub fn futures_backtest_market(self) -> Self;
    pub fn trade_target(
        self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self;
    pub fn trade_target_with_url(
        self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self;
    pub fn replay_url(self, replay_url: impl Into<String>) -> Self;
    pub fn commit_channel_capacity(self, capacity: usize) -> tqsdk_stream::Result<Self>;
    pub fn expected_commit_consumers(
        self,
        expected_consumers: usize,
    ) -> tqsdk_stream::Result<Self>;

    pub async fn build(self) -> tqsdk_stream::Result<TqStream>;
}
```

设计意图：

- 和 `tqsdk-wait::TqApiBuilder` 保持相似建造路径
- 继续复用 `SessionClientBuilder`
- 优先暴露命名清楚的 market-target shortcut，避免 façade 层继续传播裸布尔 market 选择
- `market_target(bool, bool)` 只作为兼容入口存在，不是推荐 surface
- 只暴露 stream 自身的连续消费配置，例如 root fan-out capacity
- 高频多 consumer 场景可以用 `expected_commit_consumers(...)` 按
  `max(1024, expected_consumers * 8)` 估算 root fan-out capacity；需要精确控制时仍使用
  `commit_channel_capacity(...)`
- 不在 stream builder 重新定义 direct query 选项

### root facade

```rust
pub struct TqStream { /* private */ }

impl TqStream {
    pub fn new(session: SessionClient) -> Self;
    pub fn with_commit_channel_capacity(
        session: SessionClient,
        capacity: usize,
    ) -> tqsdk_stream::Result<Self>;
    pub fn with_expected_commit_consumers(
        session: SessionClient,
        expected_consumers: usize,
    ) -> tqsdk_stream::Result<Self>;

    pub fn session(&self) -> &SessionClient;
    pub fn into_session(self) -> SessionClient;
    pub fn reader(&self) -> &RuntimeReader;
    pub fn health(&self) -> tqsdk_stream::Result<StreamHealthSnapshot>;
    pub fn reconnect_monitor(&self) -> StreamReconnectMonitor<'_>;

    pub fn commit_stream(&self) -> tqsdk_stream::Result<CommitStream>;
    pub fn path_stream<T, I, S>(&self, path: I) -> tqsdk_stream::Result<PathValueStream<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: Into<String>;
    pub async fn subscribe_quotes<I, S>(&self, symbols: I) -> tqsdk_stream::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
    pub async fn unsubscribe_quotes<I, S>(&self, symbols: I) -> tqsdk_stream::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
    pub async fn quotes<I, S>(&self, symbols: I) -> tqsdk_stream::Result<QuoteSubscription>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
    pub async fn quote_batches<I, S>(
        &self,
        symbols: I,
    ) -> tqsdk_stream::Result<QuoteBatchSubscription>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
    pub fn market_events(&self) -> MarketEventBuilder<'_>;
    pub fn recover_state(&self) -> StreamStartupRecovery<'_>;
    pub async fn kline_stream(
        &self,
        symbol: impl AsRef<str>,
        duration: Duration,
        data_length: usize,
    ) -> tqsdk_stream::Result<KlineRowStream>;
    pub async fn tick_stream(
        &self,
        symbol: impl AsRef<str>,
        data_length: usize,
    ) -> tqsdk_stream::Result<TickRowStream>;
    pub fn quote_stream(&self, symbol: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<Quote>>;
    pub fn trading_status_stream(&self, symbol: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<TradingStatus>>;
    pub fn notification_stream(&self, notification_id: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<Notification>>;
    pub fn account_stream(&self, account_id: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<Account>>;
    pub fn position_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<Position>>;
    pub fn pre_insert_order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<PreInsertOrder>>;
    pub fn order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<Order>>;
    pub fn trade_stream(
        &self,
        account_id: impl AsRef<str>,
        trade_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<Trade>>;
    pub fn order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<OrderEventStream>;
    pub fn position_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PositionEventStream>;
    pub fn pre_insert_order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PreInsertOrderEventStream>;
    pub fn trade_object_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<TradeObjectEventStream>;
    pub fn trade_session_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<TradeSessionEventStream>;
    pub fn trade_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<TradeEventStream>;
    pub fn risk_management_rule_stream(
        &self,
        account_id: impl AsRef<str>,
        exchange_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<RiskManagementRule>>;
    pub fn risk_management_data_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<RiskManagementData>>;
    pub fn settlement_info_stream(
        &self,
        account_id: impl AsRef<str>,
        trading_day: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<SettlementInfo>>;
    pub fn risk_management_rule_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<RiskManagementRuleEventStream>;
    pub fn risk_management_data_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<RiskManagementDataEventStream>;
    pub fn settlement_info_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<SettlementInfoEventStream>;
    pub fn security_account_stream(&self, account_id: impl AsRef<str>)
        -> tqsdk_stream::Result<PathValueStream<SecurityAccount>>;
    pub fn security_position_stream(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<SecurityPosition>>;
    pub fn security_order_stream(
        &self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<SecurityOrder>>;
    pub fn security_trade_stream(
        &self,
        account_id: impl AsRef<str>,
        trade_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<PathValueStream<SecurityTrade>>;
    pub fn security_position_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<SecurityPositionEventStream>;
    pub fn security_order_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<SecurityOrderEventStream>;
    pub fn security_trade_event_stream(
        &self,
        account_id: impl AsRef<str>,
    ) -> tqsdk_stream::Result<SecurityTradeEventStream>;
}
```

`recover_state()` 是 stream facade 的启动恢复屏障。它可以提交 quote 订阅意图，
再通过同一条 commit fan-out 等待 `tqsdk-session::StartupRecoverySpec` 判定为
ready。它不维护第二棵状态树，也不暴露 provider 私有 reconnect/resync 类型。

设计意图：

- `session()` 是 one-shot query / raw command / direct-query 的 escape hatch
- `reader()` 保留高性能用户直接读共享状态树的权利
- `commit_stream()` 是第一版唯一必须稳定的 continuous-consumption 入口
- `path_stream()` 是最薄的 typed decode 便利层
- `subscribe_quotes()` / `unsubscribe_quotes()` 是 quote 订阅命令的薄包装，
  用来避免普通 stream 用户直接构造 `RuntimeCommand::Market`；它们不是
  subscription handle，也不改变订阅恢复语义。
- `quote_batches()` 返回用户级 `QuoteBatchSubscription` handle，用来表达动态
  add/remove/current symbols，并按 commit 产出 changed quote batch；它通过
  `tqsdk-session` 的 session-scoped market interest registry 表达 quote interest，
  避免同一 session 内多个 stream/facade 对重叠 symbol 重复提交或互相取消。
- `quotes()` 保留为兼容的逐 quote item stream，内部可复用 batch collector flatten
  成 `ValueUpdate<Quote>`。
  session reconnect/resync 后，runtime 会从底层 market adapter 当前订阅意图生成
  recovery commands 并重新排队发送，用户不需要在业务代码中维护第二份 symbol 集合。
- `market_events()` 是 quote / tick rows / kline rows 的统一事件循环包装；
  它内部仍然只提交 quote/chart 命令并消费同一条 commit fan-out，不维护第二棵状态树。
  quote 事件读 `read_market_state()` 分区；tick/kline row batch 沿用 chart bounds
  与 commit touch 投影逻辑。
- `kline_stream()/tick_stream()` 是最薄的 row-batch stream 包装：内部仍然只是提交
  `set_chart`，然后基于同一条 commit fan-out 读取共享状态树。初次 ready
  产出 `InitialSnapshot`，后续 commit 只产出显式变化 row id 的 `Delta`；
  chart reset 或 bounds regression 产出 `ResyncSnapshot`。
- 账户级 trade object 事件流包装也都只是按 commit 的 `object_hits` 解释匹配对象更新，不额外维护事件日志
- `trade_object_event_stream()` 是这些账户级 object 事件流的统一枚举包装，不增加新的底层语义
- `trade_session_event_stream()` 继续坚持薄包装，但它直接消费 raw driver 事件，把 trade object、notification、reconnect 与 session error 聚合为一个账户级统一事件面
- `health()` 是生产部署的 typed snapshot 读面，只从 runtime `system/session`
  状态和 stream driver closed flag 组装 `StreamHealthSnapshot`，并提供
  `status()` / `should_restart()` 作为最小状态判定；它不是 metrics exporter、
  supervisor、GUI/web helper 或 graceful shutdown 框架。
- `reconnect_monitor()` 是生产守护进程的 typed reconnect wait/report 工具；
  它只消费同一条 commit fan-out 与 `StreamHealthSnapshot`，返回 already healthy /
  recovered / exhausted / timed out / closed 等结果，不驱动或替代底层 session
  reconnect。
- `quote_stream()` 只是 `path_stream()` 在行情对象上的第一个包装
- `notification_stream()` 对齐 core 的 canonical `system/notify/{id}` 路径
- `trading_status/account/position/pre_insert_order/order/trade/risk/settlement/security` 这些 wrapper
  也都只是固定 path 的薄包装，不引入新的 driver 或 cache

### commit stream

```rust
pub struct CommitStream { /* private */ }

impl futures::Stream for CommitStream {
    type Item = tqsdk_stream::Result<tqsdk_core::SharedCommitResult>;
}

impl CommitStream {
    pub fn filter_path<I, S>(self, path: I) -> PathCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;

    pub fn filter_paths(self, paths: impl IntoIterator<Item = StatePath>)
        -> PathCommitStream;

    pub fn filter_scope(self, scope: CommitScope) -> ScopeCommitStream;
    pub fn filter_scopes(
        self,
        scopes: impl IntoIterator<Item = CommitScope>,
    ) -> ScopeCommitStream;

    pub fn filter_domain(self, domain: ProtocolDomain) -> DomainCommitStream;
    pub fn filter_domains(
        self,
        domains: impl IntoIterator<Item = ProtocolDomain>,
    ) -> DomainCommitStream;

    pub fn filter_object(self, object: ObjectKey) -> ObjectCommitStream;
    pub fn filter_objects(
        self,
        objects: impl IntoIterator<Item = ObjectKey>,
    ) -> ObjectCommitStream;

    pub fn filter_fields<I, S>(self, object: ObjectKey, fields: I)
        -> FieldCommitStream
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;
}
```

其中 `Result` 的 error surface 在第一版应显式覆盖：

- session 驱动错误
- stream receiver lagged
- stream closed
- 非 Tokio runtime 中启动 driver

建议对应一个小而硬的错误枚举：

```rust
pub enum StreamFacadeError {
    Session(tqsdk_session::SessionFacadeError),
    Lagged { skipped: u64 },
    Closed,
    InvalidState(&'static str),
}
```

注：

- `Lagged` 是 stream facade 自己的 fan-out lag，不是 `tqsdk-core` cursor lag
- 这两者必须区分开

### typed path stream

```rust
pub struct ValueUpdate<T> {
    pub commit: SharedCommitResult,
    pub value: T,
}

pub struct PathValueStream<T> { /* private */ }

impl<T> futures::Stream for PathValueStream<T>
where
    T: DeserializeOwned,
{
    type Item = tqsdk_stream::Result<ValueUpdate<T>>;
}
```

设计意图：

- 不引入第二棵状态树
- 不把 typed stream 的推进点从 commit fan-out 分叉出去
- typed stream 只是“收到匹配 commit 后，用同一个 `RuntimeReader` 立即 decode”
- 若调用方需要更低开销或更细粒度控制，仍然可以直接使用 `CommitStream + reader()`

### ready row-batch stream

```rust
pub enum RowBatchKind {
    InitialSnapshot,
    Delta,
    ResyncSnapshot,
}

pub struct KlineRowBatch { /* metadata + owned rows */ }
pub struct TickRowBatch { /* metadata + owned rows */ }
pub struct KlineRowStream { /* private */ }
pub struct TickRowStream { /* private */ }

impl futures::Stream for KlineRowStream {
    type Item = tqsdk_stream::Result<ValueUpdate<KlineRowBatch>>;
}

impl futures::Stream for TickRowStream {
    type Item = tqsdk_stream::Result<ValueUpdate<TickRowBatch>>;
}
```

设计意图：

- 不复刻 `tqsdk-wait` 的同步 `wait_until_ready`
- stream 侧的 `kline/tick` 仍然通过 `MarketCommand::SetChart` 建立远端订阅
- 只有当 `charts/{chart_id}` 进入 ready 且 `more_data == false` 时才产出 row batch
- row batch 本身是基于共享状态树现读现投影的 owned rows，不额外维护本地 serial cache
- 每个 stream/spec 只保留轻量 cursor：是否已经发过初始 ready snapshot，以及上一轮
  chart bounds；不维护第二棵状态树或 row cache
- 当前 chart 生命周期采用显式 `close()` 提交 `cancel_chart`，不在 `Drop` 中做隐式 async 清理

### account-scoped trade event streams

```rust
pub struct PositionEventStream { /* private */ }
pub struct PreInsertOrderEventStream { /* private */ }
pub struct OrderEventStream { /* private */ }
pub struct TradeEventStream { /* private */ }
pub struct RiskManagementRuleEventStream { /* private */ }
pub struct RiskManagementDataEventStream { /* private */ }
pub struct SettlementInfoEventStream { /* private */ }
pub struct SecurityPositionEventStream { /* private */ }
pub struct SecurityOrderEventStream { /* private */ }
pub struct SecurityTradeEventStream { /* private */ }

impl futures::Stream for PositionEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<Position>>;
}

impl futures::Stream for PreInsertOrderEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<PreInsertOrder>>;
}

impl futures::Stream for OrderEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<Order>>;
}

impl futures::Stream for TradeEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<Trade>>;
}

impl futures::Stream for RiskManagementRuleEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<RiskManagementRule>>;
}

impl futures::Stream for RiskManagementDataEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<RiskManagementData>>;
}

impl futures::Stream for SettlementInfoEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<SettlementInfo>>;
}

impl futures::Stream for SecurityPositionEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<SecurityPosition>>;
}

impl futures::Stream for SecurityOrderEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<SecurityOrder>>;
}

impl futures::Stream for SecurityTradeEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<SecurityTrade>>;
}
```

设计意图：

- 先不照搬 `tqsdk-rs` 的独立 event journal
- 事件语义仍然直接来源于 trade 域 commit 的 `object_hits`
- `position/pre_insert/order/trade/risk/settlement/security` 这些事件流都只是账户级 object-hit 投影
- futures / security 共用同一批 `ObjectKey`，差异只体现在 decode 的目标类型
- 如果同一个 commit 命中多个同类对象，就顺序产出多个 `ValueUpdate<T>`

### unified trade object event stream

```rust
pub enum TradeObjectEvent {
    Account(Account),
    SecurityAccount(SecurityAccount),
    Position(Position),
    SecurityPosition(SecurityPosition),
    PreInsertOrder(PreInsertOrder),
    Order(Order),
    SecurityOrder(SecurityOrder),
    Trade(Trade),
    SecurityTrade(SecurityTrade),
    RiskManagementRule(RiskManagementRule),
    RiskManagementData(RiskManagementData),
    SettlementInfo(SettlementInfo),
}

pub struct TradeObjectEventStream { /* private */ }

impl futures::Stream for TradeObjectEventStream {
    type Item = tqsdk_stream::Result<ValueUpdate<TradeObjectEvent>>;
}
```

设计意图：

- 为账户级 trade object 更新提供单一消费入口
- 仍然只依赖同一个 trade 域 commit 流与共享状态树
- futures / security 的歧义对象通过原始字段做轻量判别，再选择 decode 目标类型
- 这是 commit-backed 的纯对象更新面，不负责通知和 session 级事件

### unified trade session event stream

```rust
pub enum StreamSessionPhase {
    Idle,
    Authenticating,
    Connecting,
    Bootstrapping,
    Running,
    Reconnecting,
    Resyncing,
    Closed,
}

pub struct StreamHealthSnapshot {
    pub revision: Revision,
    pub session_phase: Option<StreamSessionPhase>,
    pub reconnect: Option<SessionReconnectEvent>,
    pub driver_closed: bool,
}

pub enum StreamHealthStatus {
    Starting,
    Healthy,
    Recovering,
    Degraded,
    Closed,
}

impl StreamHealthSnapshot {
    pub fn status(&self) -> StreamHealthStatus;
    pub fn should_restart(&self) -> bool;
}

pub struct SessionReconnectEvent {
    pub attempt: u32,
    pub scheduled_backoff_ms: u64,
    pub max_attempts: Option<u32>,
    pub exhausted: bool,
    pub detail: serde_json::Value,
}

pub enum TradeSessionEvent {
    TradeObject(TradeObjectEvent),
    Notification(Notification),
    Reconnect(SessionReconnectEvent),
    SessionError(SessionFacadeError),
}

pub struct TradeSessionEventUpdate {
    pub commit: Option<SharedCommitResult>,
    pub event: TradeSessionEvent,
}

pub struct TradeSessionEventStream { /* private */ }

impl futures::Stream for TradeSessionEventStream {
    type Item = tqsdk_stream::Result<TradeSessionEventUpdate>;
}
```

`SessionReconnectEvent::max_attempts = None` 对应底层默认无限重试策略；在
runtime snapshot 中该字段来自 `system.session.reconnect.max_attempts = null`。
stream 只把它 typed 化为 `Option<u32>`，不另行解释或执行 reconnect。

设计意图：

- 统一账户级 trade session 消费入口，同时覆盖 trade object、system notification、session reconnect 与底层 session error
- health snapshot 是同一 session 状态的当前截面读面，服务生产指标/日志读取；
  它不启动额外 task，也不拥有独立健康状态树
- 对 commit-backed 事件保留 `Option<SharedCommitResult>` 中的 `Some(commit)`，不伪造 driver error 的 commit
- 实现层直接订阅 raw driver 事件，而不是建立在 `CommitStream` 之上，以免把 `DriverEvent::Error` 提前折叠成 facade error
- `Closed` / `Lagged` 仍保留为 stream error，因为这两个语义属于消费通道自身，而不是业务事件
- `StreamFacadeError::diagnostic()` 将 contract/session/lag/closed/missing-value
  错误统一成 typed kind + retry hint
- `StreamRetryPolicy` 只把 stream-facing error diagnostic 转换成 typed retry
  decision，并可运行最小 async backoff loop；它不执行 reconnect，不解释业务拒单，
  也不替代 order intent 幂等和审计

## 第一版实现边界

### 必须先实现

- `TqStreamBuilder`
- `TqStream`
- 单个共享 driver task
- 基于 commit fan-out 的 `CommitStream`
- 显式 lag / closed 错误
- `session()` / `reader()` 逃生舱

### 这一版先不实现

- callback bridge
- trade command thin wrappers

其中：

- path / scope / domain / object / field 过滤已经作为 commit stream 的薄组合层落地
- typed path、基础对象 stream、row-batch `kline/tick` stream、账户级 trade object 事件流、统一 `trade_object_event_stream` 与统一 `trade_session_event_stream` 已落地

## 内部驱动模型

`TqStream` 的内部驱动应复用 `tqsdk-wait` 已验证过的 session 推进顺序：

1. 先尝试从 `RuntimeReader::next()` 读取已有 commit
2. 若没有，再 `flush_outbound()`
3. 再尝试读取 commit
4. 再 `drive_pending_once()`
5. 再尝试读取 commit
6. 最后 `drive_route_once(None)`，等待远端事件

区别只在于：

- `tqsdk-wait` 把 commit 交给单 owner `wait_update()`
- `tqsdk-stream` 把 commit 发到内部 fan-out channel，再让每个消费者独立接收

也就是说：

- commit 生成逻辑不变
- state tree 不变
- revision 推进不变
- 只是消费形状从“pull by wait loop”变成“push into stream channel”

## 背压模型

第一版推荐使用：

- 单个 driver task
- 单个 bounded broadcast ring
- 每个 `commit_stream()` 调用者持有自己的 receiver

最小语义应当是：

- 慢消费者落后时，返回 `Lagged`
- root fan-out buffer 可通过 `TqStreamBuilder::commit_channel_capacity(...)`
  或已有 session 场景下的 `TqStream::with_commit_channel_capacity(...)` 显式配置
- 生产进程关闭时可通过 `TqStream::graceful_shutdown()` 显式 flush outbound、
  关闭 stream driver，并返回 outbound/driver typed report
- 生产进程可通过 `TqStream::reconnect_monitor()` 等待并报告既有 session
  reconnect 的恢复、耗尽、超时或关闭结果；它是 typed supervision helper，不是
  新的 reconnect executor
- 不为慢消费者阻塞整个 session 驱动
- 不为每个订阅者维护独立 cursor + 独立 route 驱动

这个配置只控制 stream facade 内部 bounded broadcast ring。写库、日志、重试、WAL、
journal、compaction、跨进程锁、调度或 daemon queue 都属于调用方 sidecar 或后续
daemon/tooling 层；stream crate 不再托管 durable sink。
`graceful_shutdown()` 是
stream facade 的显式关闭工具；`reconnect_monitor()` 只做 typed wait/report。
两者都不接管底层 reconnect 执行，也不应下沉到 `tqsdk-core` 或
`tqsdk-session`。

为什么第一版不做更复杂的 path/object fan-out：

- 因为对象级过滤在第一版还不是稳定边界
- 先用 commit-level fan-out 锁住主数据流，再决定更细粒度投影

## 与 `tqsdk-wait` 的关系

`tqsdk-stream` 和 `tqsdk-wait` 是并列 facade，不是上下层关系。

两者共享：

- `SessionClient`
- `RuntimeReader`
- `UpdateCursor`
- 同一棵状态树
- 同一套 `CommitResult` payload / `SharedCommitResult` 所有权模型

两者不同：

- `tqsdk-wait` 是单 owner、单推进点、稳定截面优先
- `tqsdk-stream` 是多消费者、异步 fan-out、组合性优先

因此第一版不应该为了复用而直接依赖 `tqsdk-wait`。

如果后续发现两边确实有稳定共享的 `Ref` / filter / projection 抽象，再单独抽公共层；在那之前，不要过早提炼一个“wait+stream 通用 facade core”。

## 与 `tqsdk-session` 的关系

`tqsdk-stream` 继续把 `tqsdk-session` 视为共享 session substrate。

边界保持不变：

- direct query / schema / metadata 继续留在 `tqsdk-session`
- `tqsdk-stream` 只负责 diff-backed continuous consumption

所以 `TqStream::session()` 的意义只是复用同一个底层 session，而不是把 direct query API 重新归属到 stream crate。

## 第一版建议的代码布局

推荐最小文件布局：

```text
crates/tqsdk-stream/
  src/
    lib.rs
    builder.rs
    api.rs
    driver.rs
    error.rs
    event.rs
    window.rs
  tests/
    stream_surface.rs
    stream_commit_flow.rs
    support/
```

各文件职责：

- `builder.rs`
  - `TqStreamBuilder`
- `api.rs`
  - `TqStream`
  - `CommitStream`
- `driver.rs`
  - 后台 pump task
  - 启动/关闭/单实例保护
- `error.rs`
  - facade 级错误类型
- `event.rs`
  - commit-backed trade object event 投影
- `window.rs`
  - ready row-batch `kline/tick` 投影
- `tests/*`
  - surface / driver / lag 语义

## 第一版验收标准

如果第一版最小实现完成，至少应能验证：

1. 同一个 `TqStream` 可以创建多个 `commit_stream()` receiver
2. 一个 receiver 消费到的 commit revision 顺序与 `RuntimeReader` 一致
3. receiver 落后时会显式报 `Lagged`
4. `reader()` 可以在收到 commit 后读到对应 revision 的状态
5. `session()` 仍可用于 direct query / raw submit 复用同一 session
6. 整个实现不需要回改 `tqsdk-core` 的 commit 生成逻辑

## 后续增量方向

在最小 commit stream 稳定之后，下一批最自然的增量是：

### 第二批

- `CommitStream` 的 path / scope / domain / object / field 过滤已经落地

### 第三批

- `path_stream<T>()`、基础对象 stream、`notification`、security trade object、row-batch `kline/tick`、账户级 trade object 事件流、统一 `trade_object_event_stream` 与统一 `trade_session_event_stream` 已落地
- futures / securities 对象级投影仍保持“固定 path 或固定 chart row batch”的薄包装原则

### 第四批

- callback bridge

这个顺序的核心原则是：

- 先锁主数据流
- 再锁过滤语义
- 最后再锁高层对象形状
