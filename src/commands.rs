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
    InsertOrder {
        account_id: AccountId,
        symbol: Symbol,
        volume: i64,
    },
    CancelOrder {
        account_id: AccountId,
        order_id: OrderId,
    },
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
