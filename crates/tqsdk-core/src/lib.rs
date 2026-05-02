#![cfg_attr(not(test), forbid(unsafe_code))]
//! Low-level async substrate for Tianqin/TQSDK server interaction.
//!
//! This crate intentionally stays at the contract layer: protocol adapters,
//! transport/runtime coordination, state projection, schema/query/bootstrap
//! interaction, and trade/replay/session control. It does not add facade-style
//! user APIs or runtime ownership.
//!
//! # Example
//!
//! ```
//! let handle = tqsdk_core::RuntimeHandle::new();
//! let reader = handle.reader();
//! let cursor = reader.cursor();
//!
//! assert_eq!(cursor.next_revision().get(), 1);
//! ```

pub mod adapter;
#[cfg(test)]
mod aggregation;
mod auth;
pub mod commands;
mod diff_protocol;
mod domain_event;
pub mod error;
pub mod events;
pub mod ids;
pub mod order_lifecycle;
pub mod runtime;
mod session_runtime;
pub mod state;
mod transport;
pub mod types;

#[doc(hidden)]
/// Temporary implementation bridge for sibling crates.
///
/// This module is not part of the stable public contract. It exists only while
/// `tqsdk-session` absorbs runtime assembly details from core.
pub mod internal {
    pub use crate::auth::DynAuthProvider;
    pub use crate::session_runtime::{
        RouteRequestExecutor, SessionRun, SessionRuntime, SessionRuntimeDeps,
    };
    pub use crate::transport::{
        DefaultRouteConnector, DynRouteConnectFuture, DynTransport, SessionBootstrap,
        WebSocketRouteConnector, WebSocketTransport,
    };
}

pub use adapter::{AdapterRegistry, ProtocolAdapter};
pub use auth::{AuthContext, AuthProvider};
pub use commands::{
    CausationMeta, CommandEnvelope, CommandStatus, HttpMethod, HttpRequest, InternalRequest,
    MarketChartCommand, MarketCommand, OutboundDispatch, OutboundFrame, OutboundRequest,
    QueryCommand, QueryRequest, ReplayCommand, ReplayRequest, RuntimeCommand, SchemaCommand,
    SystemCommand, TradeAccountType, TradeCommand, TradeDirection, TradeInsertOrderCommand,
    TradeLoginCommand, TradeOffset, TradePreInsertOrderCommand, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition,
};
pub use domain_event::{DomainEvent, MarketEvent, TradeEvent, collect_domain_events};
pub use error::{ContractError, ContractErrorKind, Result, RetryHint};
pub use events::{
    AuthEvent, FieldMutation, InputPayload, InternalEvent, IoEvent, MutationSource,
    NormalizedMutation, ReplayEvent, RuntimeInput, TimerEvent,
};
pub use ids::{
    AccountId, AuthId, ChartId, CommandId, CursorId, NotificationId, OrderId, ProtocolDomain,
    QueryId, ReplaySessionId, Revision, SchemaId, Symbol, TradeId,
};
pub use order_lifecycle::OrderLifecycle;
pub use runtime::{
    CommitLog, CommitReadGuard, CursorLagged, Runtime, RuntimeHandle, RuntimeReader,
    SnapshotReadGuard,
};
pub use state::{
    ChangeHit, ChangeSet, CommitResult, CommitScope, MarketStateReadGuard, MarketStateView,
    MarketTradeStateReadGuard, ObjectKey, PathSegment, SeriesKey, SharedCommitResult, StatePath,
    StateReadView, StateSnapshot, TradeStateReadGuard, TradeStateView, UpdateCursor,
};
pub use transport::{
    AuthDerivedTradeTarget, BootstrapResult, EndpointConfig, HeartbeatPolicy, MarketSessionTarget,
    RawFrame, ReconnectPolicy, SessionConfig, SessionPhase, SessionRoute, SessionRouteConnector,
    SessionRouteEndpoint, SessionTarget, SessionTopology, SessionTopologyResolver,
    TradeSessionTarget, Transport, WebSocketConnectOptions,
};
pub use types::{
    Account, CategoryInfo, Chart, ChartInfo, EdbIndexData, FrequentCancellation,
    FrequentCancellationRule, Kline, Notification, Order, Position, PreInsertOrder, Quote,
    RiskManagementData, RiskManagementRule, SecurityAccount, SecurityOrder, SecurityPosition,
    SecurityTrade, SelfTrade, SelfTradeRule, SettlementInfo, SymbolRanking, SymbolSettlement, Tick,
    Trade, TradePositionRatio, TradePositionRatioRule, TradingCalendarDay, TradingStatus,
    TradingTime,
};
