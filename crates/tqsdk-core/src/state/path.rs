use crate::ids::{
    AccountId, ChartId, CommandId, NotificationId, OrderId, QueryId, ReplaySessionId, SchemaId,
    Symbol, TradeId,
};

pub type PathSegment = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatePath(Vec<PathSegment>);

impl StatePath {
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(segments.into_iter().map(Into::into).collect())
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub primary: Symbol,
    pub secondary: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub right_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    SessionAuth,
    SessionLifecycle,
    SessionTopology,
    SessionReconnect,
    Quote {
        symbol: Symbol,
    },
    Kline {
        series: SeriesKey,
        bar_id: i64,
    },
    Tick {
        symbol: Symbol,
        tick_id: i64,
    },
    TradingStatus {
        symbol: Symbol,
    },
    Chart {
        chart_id: ChartId,
    },
    Command {
        command_id: CommandId,
    },
    Account {
        account_id: AccountId,
    },
    TradeSession {
        account_id: AccountId,
    },
    RiskManagementRule {
        account_id: AccountId,
        exchange_id: String,
    },
    RiskManagementData {
        account_id: AccountId,
        symbol: Symbol,
    },
    Position {
        account_id: AccountId,
        symbol: Symbol,
    },
    PreInsertOrder {
        account_id: AccountId,
        order_id: OrderId,
    },
    Order {
        account_id: AccountId,
        order_id: OrderId,
    },
    Trade {
        account_id: AccountId,
        trade_id: TradeId,
    },
    Settlement {
        account_id: AccountId,
        trading_day: String,
    },
    QueryResult {
        query_id: QueryId,
    },
    SchemaNode {
        schema_id: SchemaId,
    },
    ReplayCursor {
        session_id: ReplaySessionId,
    },
    Notification {
        notification_id: NotificationId,
    },
}
