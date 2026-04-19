use serde_json::Value;

use crate::ids::{AccountId, CommandId, OrderId, ProtocolDomain, QueryId, SchemaId, Symbol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeCommand {
    System(SystemCommand),
    Market(MarketCommand),
    Trade(TradeCommand),
    Replay(ReplayCommand),
    Query(QueryCommand),
    Schema(SchemaCommand),
}

impl RuntimeCommand {
    pub fn domain(&self) -> ProtocolDomain {
        match self {
            Self::System(_) => ProtocolDomain::System,
            Self::Market(_) => ProtocolDomain::Market,
            Self::Trade(_) => ProtocolDomain::Trade,
            Self::Replay(_) => ProtocolDomain::Replay,
            Self::Query(_) => ProtocolDomain::Query,
            Self::Schema(_) => ProtocolDomain::Schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemCommand {
    Shutdown,
    RefreshAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketCommand {
    SubscribeQuotes { symbols: Vec<Symbol> },
    UnsubscribeQuotes { symbols: Vec<Symbol> },
    SetChart(MarketChartCommand),
    CancelChart { chart_id: String },
    SubscribeTradingStatus { symbols: Vec<Symbol> },
    UnsubscribeTradingStatus { symbols: Vec<Symbol> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketChartCommand {
    pub chart_id: String,
    pub symbols: Vec<Symbol>,
    pub duration_ns: i64,
    pub view_width: usize,
    pub left_kline_id: Option<i64>,
    pub focus_datetime_ns: Option<i64>,
    pub focus_position: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeCommand {
    Login(TradeLoginCommand),
    ConfirmSettlement {
        account_id: AccountId,
    },
    QueryAccountInfo {
        account_id: AccountId,
    },
    QueryAccountRegister {
        account_id: AccountId,
    },
    QuerySettlementInfo {
        account_id: AccountId,
        trading_day: u32,
    },
    InsertOrder(TradeInsertOrderCommand),
    CancelOrder {
        account_id: AccountId,
        order_id: OrderId,
    },
    Transfer {
        account_id: AccountId,
        bank_id: String,
        bank_password: String,
        future_account: String,
        future_password: String,
        currency: String,
        amount: Value,
    },
    SetRiskManagementRule {
        account_id: AccountId,
        rule: Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeAccountType {
    Future,
    Spot,
}

impl TradeAccountType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Future => "future",
            Self::Spot => "spot",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeLoginCommand {
    pub account_id: AccountId,
    pub broker_id: String,
    pub password: String,
    pub account_type: TradeAccountType,
    pub front_broker: Option<String>,
    pub front_url: Option<String>,
    pub client_app_id: Option<String>,
    pub client_system_info: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeDirection {
    Buy,
    Sell,
}

impl TradeDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeOffset {
    Open,
    Close,
    CloseToday,
}

impl TradeOffset {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Close => "CLOSE",
            Self::CloseToday => "CLOSETODAY",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradePriceType {
    Any,
    Limit,
    Best,
    FiveLevel,
}

impl TradePriceType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::Limit => "LIMIT",
            Self::Best => "BEST",
            Self::FiveLevel => "FIVELEVEL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeTimeCondition {
    Ioc,
    Gfd,
}

impl TradeTimeCondition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ioc => "IOC",
            Self::Gfd => "GFD",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeVolumeCondition {
    Any,
    All,
}

impl TradeVolumeCondition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::All => "ALL",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeInsertOrderCommand {
    pub account_id: AccountId,
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub volume: i64,
    pub price_type: TradePriceType,
    pub limit_price: Option<Value>,
    pub time_condition: TradeTimeCondition,
    pub volume_condition: TradeVolumeCondition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCommand {
    Step,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryCommand {
    Fetch {
        query_id: QueryId,
        query: String,
        variables: Option<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaCommand {
    Refresh { schema_id: SchemaId, path: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CausationMeta {
    pub parent: Option<CommandId>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandEnvelope {
    pub id: CommandId,
    pub command: RuntimeCommand,
    pub causation: CausationMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    Queued,
    Sent,
    Acked,
    PartiallyApplied,
    Completed,
    Rejected,
    Failed,
    Cancelled,
}

impl CommandStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Sent => "sent",
            Self::Acked => "acked",
            Self::PartiallyApplied => "partially_applied",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRequest {
    pub action: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalRequest {
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRequest {
    Transport(OutboundFrame),
    Http(HttpRequest),
    Replay(ReplayRequest),
    Internal(InternalRequest),
}

impl OutboundRequest {
    pub fn internal_label(label: &'static str) -> Self {
        Self::Internal(InternalRequest { label })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDispatch {
    pub command_id: CommandId,
    pub domain: ProtocolDomain,
    pub request: OutboundRequest,
}
