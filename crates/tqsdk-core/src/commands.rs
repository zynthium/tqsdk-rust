use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

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

#[derive(Clone, PartialEq, Eq)]
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
    PreInsertOrder(TradePreInsertOrderCommand),
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

impl std::fmt::Debug for TradeCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Login(cmd) => f.debug_tuple("Login").field(cmd).finish(),
            Self::ConfirmSettlement { account_id } => f
                .debug_struct("ConfirmSettlement")
                .field("account_id", account_id)
                .finish(),
            Self::QueryAccountInfo { account_id } => f
                .debug_struct("QueryAccountInfo")
                .field("account_id", account_id)
                .finish(),
            Self::QueryAccountRegister { account_id } => f
                .debug_struct("QueryAccountRegister")
                .field("account_id", account_id)
                .finish(),
            Self::QuerySettlementInfo {
                account_id,
                trading_day,
            } => f
                .debug_struct("QuerySettlementInfo")
                .field("account_id", account_id)
                .field("trading_day", trading_day)
                .finish(),
            Self::PreInsertOrder(cmd) => f.debug_tuple("PreInsertOrder").field(cmd).finish(),
            Self::InsertOrder(cmd) => f.debug_tuple("InsertOrder").field(cmd).finish(),
            Self::CancelOrder {
                account_id,
                order_id,
            } => f
                .debug_struct("CancelOrder")
                .field("account_id", account_id)
                .field("order_id", order_id)
                .finish(),
            Self::Transfer {
                account_id,
                bank_id,
                future_account,
                currency,
                amount,
                ..
            } => f
                .debug_struct("Transfer")
                .field("account_id", account_id)
                .field("bank_id", bank_id)
                .field("bank_password", &"[REDACTED]")
                .field("future_account", future_account)
                .field("future_password", &"[REDACTED]")
                .field("currency", currency)
                .field("amount", amount)
                .finish(),
            Self::SetRiskManagementRule { account_id, rule } => f
                .debug_struct("SetRiskManagementRule")
                .field("account_id", account_id)
                .field("rule", rule)
                .finish(),
        }
    }
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

#[derive(Clone, PartialEq, Eq)]
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

impl std::fmt::Debug for TradeLoginCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeLoginCommand")
            .field("account_id", &self.account_id)
            .field("broker_id", &self.broker_id)
            .field("password", &"[REDACTED]")
            .field("account_type", &self.account_type)
            .field("front_broker", &self.front_broker)
            .field("front_url", &self.front_url)
            .field("client_app_id", &self.client_app_id)
            .field("client_system_info", &self.client_system_info)
            .finish()
    }
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

    pub fn from_protocol_str(value: &str) -> Option<Self> {
        match value {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }
}

impl Serialize for TradeDirection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TradeDirection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_protocol_str(&value)
            .ok_or_else(|| serde::de::Error::unknown_variant(&value, &["BUY", "SELL"]))
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

    pub fn from_protocol_str(value: &str) -> Option<Self> {
        match value {
            "OPEN" => Some(Self::Open),
            "CLOSE" => Some(Self::Close),
            "CLOSETODAY" => Some(Self::CloseToday),
            _ => None,
        }
    }
}

impl Serialize for TradeOffset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TradeOffset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_protocol_str(&value).ok_or_else(|| {
            serde::de::Error::unknown_variant(&value, &["OPEN", "CLOSE", "CLOSETODAY"])
        })
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

    pub fn from_protocol_str(value: &str) -> Option<Self> {
        match value {
            "ANY" => Some(Self::Any),
            "LIMIT" => Some(Self::Limit),
            "BEST" => Some(Self::Best),
            "FIVELEVEL" => Some(Self::FiveLevel),
            _ => None,
        }
    }
}

impl Serialize for TradePriceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TradePriceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_protocol_str(&value).ok_or_else(|| {
            serde::de::Error::unknown_variant(&value, &["ANY", "LIMIT", "BEST", "FIVELEVEL"])
        })
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
pub struct TradePreInsertOrderCommand {
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
    pub hedge_flag: String,
    pub contingent_condition: String,
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

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Rejected | Self::Failed | Self::Cancelled
        )
    }
}

impl std::str::FromStr for CommandStatus {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "sent" => Ok(Self::Sent),
            "acked" => Ok(Self::Acked),
            "partially_applied" => Ok(Self::PartiallyApplied),
            "completed" => Ok(Self::Completed),
            "rejected" => Ok(Self::Rejected),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(()),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: Option<String>,
    pub body: Option<Value>,
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
pub struct QueryRequest {
    pub query_id: QueryId,
    pub query: String,
    pub variables: Option<Value>,
}

impl QueryRequest {
    pub fn body(&self) -> Value {
        let mut request = json!({
            "aid": "ins_query",
            "query_id": self.query_id.as_str(),
            "query": self.query,
        });
        if let Some(variables) = self.variables.as_ref()
            && !matches!(variables, Value::Object(map) if map.is_empty())
            && let Value::Object(fields) = &mut request
        {
            fields.insert("variables".to_string(), variables.clone());
        }
        request
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRequest {
    Transport(OutboundFrame),
    Http(HttpRequest),
    Query(QueryRequest),
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
    /// The runtime-assigned command id being dispatched.
    pub command_id: CommandId,
    /// The protocol domain that owns the request.
    pub domain: ProtocolDomain,
    /// Optional account routing key for multi-account trade dispatch.
    pub account_id: Option<AccountId>,
    /// The low-level request payload to deliver on the selected route.
    pub request: OutboundRequest,
}
