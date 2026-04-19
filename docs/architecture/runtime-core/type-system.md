# 类型级约束

## 原则
`V1 Runtime Kernel` 建议从一开始就采用强类型 Newtype 风格，避免关键标识符和键体系在后续 API 层扩展中漂移。

## 标识符类型
```rust
pub struct Symbol(String);
pub struct AccountId(String);
pub struct OrderId(String);
pub struct TradeId(String);
pub struct AuthId(String);
```

## `SeriesKey`
```rust
pub struct SeriesKey {
    pub primary: Symbol,
    pub secondary: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub right_id: Option<i64>,
}
```

## `PatchTarget`
```rust
pub enum PatchTarget {
    Quote { symbol: Symbol },
    Tick { symbol: Symbol },
    Kline { series: SeriesKey },
    Account { account_id: AccountId },
    Position { account_id: AccountId, symbol: Symbol },
    Order { account_id: AccountId, order_id: OrderId },
    Trade { account_id: AccountId, trade_id: TradeId },
}
```

## `SubscriptionIntent`
```rust
pub enum SubscriptionIntent {
    Quote(QuoteIntent),
    Series(SeriesIntent),
    Trade(TradeIntent),
}

pub struct QuoteIntent {
    pub symbols: Vec<Symbol>,
}

pub struct SeriesIntent {
    pub key: SeriesKey,
    pub kind: SeriesKind,
}

pub enum SeriesKind {
    Kline,
    Tick,
}

pub struct TradeIntent {
    pub account_id: AccountId,
    pub scope: TradeScope,
}
```

## `SessionConfig`
```rust
pub struct SessionConfig {
    pub endpoint: String,
    pub heartbeat: HeartbeatPolicy,
    pub reconnect: ReconnectPolicy,
    pub initial_subscriptions: Vec<SubscriptionIntent>,
}
```

## `BootstrapResult`
```rust
pub struct BootstrapResult {
    pub auth: AuthContext,
    pub initial_revision: Revision,
    pub ready_scope: CommitScope,
}
```

## 为什么内核层必须优先强类型
- 避免 `symbol`、`account_id`、`auth_id`、`order_id` 混用
- 稳定 `PatchTarget`、`SubscriptionIntent`、`ChangeSet` 的映射关系
- 让 reconnect/resubscribe 过程复用同一套键体系

## V1 先锁定的类型
- `Symbol`
- `AccountId`
- `OrderId`
- `TradeId`
- `AuthId`
- `SeriesKey`
- `PatchTarget`
- `SubscriptionIntent`
- `QuoteIntent`
- `SeriesIntent`
- `TradeIntent`
- `SessionConfig`
- `BootstrapResult`
