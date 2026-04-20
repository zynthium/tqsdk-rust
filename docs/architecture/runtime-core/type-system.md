# 类型级约束

## 原则
V1 应优先锁定“runtime contract 会长期稳定使用的标识符和路径体系”，而不是提前锁定 facade 类型。

同时，V1 可以锁定一层纯 schema/type contract，用来承载官方对象定义，但这层不能反向演化成 typed state view 或用户 facade。

## 必锁定的标识符类型
```rust
pub struct Revision(u64);
pub struct CommandId(u64);
pub struct CursorId(u64);

pub struct Symbol(String);
pub struct AccountId(String);
pub struct OrderId(String);
pub struct TradeId(String);
pub struct QueryId(String);
pub struct SchemaId(String);
pub struct ReplaySessionId(String);
pub struct AuthId(String);
```

## `ProtocolDomain`
```rust
pub enum ProtocolDomain {
    System,
    Market,
    Trade,
    Replay,
    Query,
    Schema,
}
```

## `StatePath`
```rust
pub struct StatePath(Vec<PathSegment>);
```

用途：
- 保留协议原生结构
- 保证任何 mutation 都有稳定落点

## `ObjectKey`
```rust
pub enum ObjectKey {
    Quote { symbol: Symbol },
    Kline { series: SeriesKey, bar_id: i64 },
    Tick { symbol: Symbol, tick_id: i64 },
    Account { account_id: AccountId },
    Position { account_id: AccountId, symbol: Symbol },
    Order { account_id: AccountId, order_id: OrderId },
    Trade { account_id: AccountId, trade_id: TradeId },
    QueryResult { query_id: QueryId },
    SchemaNode { schema_id: SchemaId },
    ReplayCursor { session_id: ReplaySessionId },
}
```

用途：
- 给未来 facade 一个稳定逻辑对象身份
- 支撑 path/object/field 三级变更命中

## `RuntimeCommand`
```rust
pub enum RuntimeCommand {
    System(SystemCommand),
    Market(MarketCommand),
    Trade(TradeCommand),
    Replay(ReplayCommand),
    Query(QueryCommand),
    Schema(SchemaCommand),
}
```

## `ChangeSet`
```rust
pub struct ChangeSet {
    pub path_hits: Vec<StatePath>,
    pub object_hits: Vec<ObjectKey>,
    pub field_hits: Vec<ChangeHit>,
}
```

## `SessionConfig`
```rust
pub struct SessionConfig {
    pub endpoints: EndpointConfig,
    pub heartbeat: HeartbeatPolicy,
    pub reconnect: ReconnectPolicy,
    pub enabled_domains: Vec<ProtocolDomain>,
}
```

## 为什么 V1 要先锁这些类型
- 它们直接决定 runtime contract 的稳定性
- 它们会被所有未来 facade 间接复用
- 一旦这些类型漂移，后续 `wait_update` 和 stream/callback 都会被迫重构

## 可提前锁定的纯 schema 类型
- `types::Quote` / `types::Kline` / `types::Tick`
- `types::Account` / `types::Position` / `types::Order` / `types::Trade`
- `types::RiskManagementRule` / `types::RiskManagementData`
- `types::SecurityAccount` / `types::SecurityPosition` / `types::SecurityOrder` / `types::SecurityTrade`

这些类型只表达远端状态树对象的字段契约、默认值和稀疏 payload 兼容语义，不表达 reader 绑定、增量更新语义或 facade 行为。

它们进入 core 的方式应该是：
- 通过 `RuntimeReader::read()` / `RuntimeReader::next_view()` 拿到 revision-bound 视图
- 通过 `SnapshotReadGuard` / `CommitReadGuard` / `StateReadView::decode<T>()` 在需要时按路径解码为 typed schema
- 不能把 runtime state 本身提前锁成 typed object graph

## 明确后置到后续阶段的类型
- `TqApi`
- user-facing `ChangeTarget`
- typed `QuoteView` / `QuoteSnapshot`
- typed `KlineSerialView`
- facade 级任务与 helper 类型

原因：
- 这些类型属于消费层，不属于 V1 contract
