#![cfg_attr(not(test), forbid(unsafe_code))]
//! Low-level async substrate for Tianqin/TQSDK server interaction.
//!
//! This crate intentionally stays at the contract layer: protocol adapters,
//! transport/runtime coordination, state projection, schema/query/bootstrap
//! interaction, and trade/replay/session control. It does not add facade-style
//! user APIs or runtime ownership.

pub mod adapter;
pub mod auth;
pub mod commands;
pub mod error;
pub mod events;
pub mod http_executor;
pub mod ids;
pub mod runtime;
pub mod session_runtime;
pub mod state;
pub mod tq_auth;
pub mod transport;
pub mod types;

pub use adapter::{
    AdapterRegistry, MarketAdapter, ProtocolAdapter, QueryAdapter, ReplayAdapter, SchemaAdapter,
    SystemAdapter, TradeAdapter,
};
pub use auth::{AuthContext, AuthProvider, ContractFuture};
pub use commands::{
    CausationMeta, CommandEnvelope, CommandStatus, HttpMethod, HttpRequest, InternalRequest,
    MarketChartCommand, MarketCommand, OutboundDispatch, OutboundFrame, OutboundRequest,
    QueryCommand, ReplayCommand, ReplayRequest, RuntimeCommand, SchemaCommand, SystemCommand,
    TradeAccountType, TradeCommand, TradeDirection, TradeInsertOrderCommand, TradeLoginCommand,
    TradeOffset, TradePreInsertOrderCommand, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition,
};
pub use error::{ContractError, Result};
pub use events::{
    AuthEvent, FieldMutation, InputPayload, InternalEvent, IoEvent, MutationSource,
    NormalizedMutation, ReplayEvent, RuntimeInput, TimerEvent,
};
pub use http_executor::ReqwestHttpExecutor;
pub use ids::{
    AccountId, AuthId, ChartId, CommandId, CursorId, NotificationId, OrderId, ProtocolDomain,
    QueryId, ReplaySessionId, Revision, SchemaId, Symbol, TradeId,
};
pub use runtime::{
    CommitLog, CommitReadGuard, CursorLagged, OutboundEnvelope, Runtime, RuntimeHandle,
    RuntimeReader, SnapshotReadGuard,
};
pub use session_runtime::{
    PendingRouteStepOutcome, RoutePumpOutcome, RouteRequestExecutor, SessionRun, SessionRuntime,
    SessionRuntimeDeps, SessionStepOutcome,
};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, ObjectKey, PathSegment, SeriesKey, StatePath,
    StateReadView, StateSnapshot, UpdateCursor,
};
pub use tq_auth::{BrokerInfo, PasswordCredentials, TqAuthProvider};
pub use transport::{
    BootstrapResult, ConnectedSessionRoute, ConnectedTopology, DefaultRouteConnector,
    DispatchReceipt, EndpointConfig, HeartbeatPolicy, MarketSessionTarget, RawFrame,
    ReconnectPolicy, SessionBootstrap, SessionConfig, SessionPhase, SessionRoute,
    SessionRouteConnector, SessionRouteEndpoint, SessionTarget, SessionTopology,
    SessionTopologyResolver, TradeSessionTarget, Transport, WebSocketConnectOptions,
    WebSocketRouteConnector, WebSocketTransport,
};
pub use types::{
    Account, CategoryInfo, Chart, ChartInfo, EdbIndexData, FrequentCancellation,
    FrequentCancellationRule, Kline, Notification, Order, Position, PreInsertOrder, Quote,
    RiskManagementData, RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition,
    SecurityTrade, SelfTrade, SelfTradeRule, SettlementInfo, SymbolRanking, SymbolSettlement, Tick,
    Trade, TradePositionRatio, TradePositionRatioRule, TradingCalendarDay, TradingStatus,
    TradingTime,
};
