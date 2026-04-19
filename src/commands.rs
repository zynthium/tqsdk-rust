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
    InsertOrder {
        account_id: AccountId,
        symbol: Symbol,
        volume: i64,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCommand {
    Step,
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryCommand {
    Fetch { query_id: QueryId, path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaCommand {
    Refresh { schema_id: SchemaId },
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
