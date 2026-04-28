#![cfg_attr(not(test), forbid(unsafe_code))]
//! Rust async-native stream facade built on [`tqsdk_core`] and
//! [`tqsdk_session`].
//!
//! This crate owns diff-backed continuous consumption in multi-consumer stream
//! form. The current minimal surface exposes a shared-session [`TqStream`],
//! raw commit fan-out via [`CommitStream`], typed path decoding via
//! [`PathValueStream`], ready-window market streams via
//! [`KlineWindowStream`] / [`TickWindowStream`], dynamic
//! [`QuoteSubscription`] handles, a unified [`MarketEventStream`] for mixed
//! quote/tick/kline loops, minimal commit-backed trade event streams via
//! [`OrderEventStream`] / [`TradeEventStream`] and related account-scoped
//! wrappers, unified [`TradeObjectEventStream`] / [`TradeSessionEventStream`]
//! layers, managed [`CommitSink`] consumers for slow sink isolation with finite
//! retry / JSONL WAL options, [`StreamReconnectMonitor`] for typed reconnect
//! recovery reporting, [`StreamGracefulShutdown`] for explicit driver close and
//! sink flush orchestration, and direct access to the shared
//! [`tqsdk_core::RuntimeReader`].
//!
//! One-shot direct query, schema refresh, metadata, and other non-streaming
//! helpers remain in [`tqsdk_session`]. Use [`TqStream::session`] when a
//! stream-based flow needs to reuse the same underlying session for those
//! operations.
//!
//! # Runtime
//!
//! `tqsdk-stream` is a pure async substrate. Callers must provide their own
//! Tokio runtime.

mod api;
mod builder;
mod driver;
mod error;
mod event;
mod filter;
mod health;
mod market_event;
mod quote_subscription;
mod reconnect;
mod recovery;
mod shutdown;
mod sink;
mod typed;
mod window;

pub use api::{CommitStream, TqStream};
pub use builder::TqStreamBuilder;
pub use error::{
    Result, StreamErrorDiagnostic, StreamErrorKind, StreamFacadeError, StreamRetryDecision,
    StreamRetryGiveUpReason, StreamRetryPolicy, StreamRetryReport,
};
pub use event::{
    OrderEventStream, PositionEventStream, PreInsertOrderEventStream,
    RiskManagementDataEventStream, RiskManagementRuleEventStream, SecurityOrderEventStream,
    SecurityPositionEventStream, SecurityTradeEventStream, SessionReconnectEvent,
    SettlementInfoEventStream, TradeEventStream, TradeObjectEvent, TradeObjectEventStream,
    TradeSessionEvent, TradeSessionEventStream, TradeSessionEventUpdate,
};
pub use filter::{
    DomainCommitStream, FieldCommitStream, ObjectCommitStream, PathCommitStream, ScopeCommitStream,
};
pub use health::{StreamHealthSnapshot, StreamHealthStatus, StreamSessionPhase};
pub use market_event::{MarketEvent, MarketEventBuilder, MarketEventStream};
pub use quote_subscription::QuoteSubscription;
pub use reconnect::{StreamReconnectMonitor, StreamReconnectOutcome, StreamReconnectReport};
pub use recovery::StreamStartupRecovery;
pub use shutdown::{
    StreamGracefulShutdown, StreamGracefulShutdownReport, StreamShutdownError,
    StreamSinkShutdownError,
};
pub use sink::{
    CommitSink, StreamCommitJournal, StreamCommitJournalDomain, StreamCommitJournalRecord,
    StreamCommitJournalReplayReport, StreamCommitJournalScope, StreamSinkFuture, StreamSinkHandle,
    StreamSinkOptions, StreamSinkRetryPolicy, StreamSinkShutdownReport, StreamSinkStats,
    StreamSinkStatus, StreamSinkWalCompaction, StreamSinkWalCompactionReport,
    StreamSinkWalFsyncPolicy, StreamSinkWalRecord, StreamSinkWalRecordKind, StreamSinkWalRecovery,
    StreamSinkWalRecoveryReport,
};
pub use typed::{PathValueStream, ValueUpdate};
pub use window::{KlineWindow, KlineWindowStream, TickWindow, TickWindowStream};
