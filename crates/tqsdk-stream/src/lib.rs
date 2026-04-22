#![cfg_attr(not(test), forbid(unsafe_code))]
//! Rust async-native stream facade built on [`tqsdk_core`] and
//! [`tqsdk_session`].
//!
//! This crate owns diff-backed continuous consumption in multi-consumer stream
//! form. The current minimal surface exposes a shared-session [`TqStream`],
//! raw commit fan-out via [`CommitStream`], typed path decoding via
//! [`PathValueStream`], ready-window market streams via
//! [`KlineWindowStream`] / [`TickWindowStream`], minimal commit-backed trade
//! event streams via [`OrderEventStream`] / [`TradeEventStream`] and related
//! account-scoped wrappers, a unified [`TradeObjectEventStream`], and direct
//! access to the shared [`tqsdk_core::RuntimeReader`].
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
mod typed;
mod window;

pub use api::{CommitStream, TqStream};
pub use builder::TqStreamBuilder;
pub use error::{Result, StreamFacadeError};
pub use event::{
    OrderEventStream, PositionEventStream, PreInsertOrderEventStream,
    RiskManagementDataEventStream, RiskManagementRuleEventStream, SecurityOrderEventStream,
    SecurityPositionEventStream, SecurityTradeEventStream, SettlementInfoEventStream,
    TradeEventStream, TradeObjectEvent, TradeObjectEventStream,
};
pub use filter::{
    DomainCommitStream, FieldCommitStream, ObjectCommitStream, PathCommitStream, ScopeCommitStream,
};
pub use typed::{PathValueStream, ValueUpdate};
pub use window::{KlineWindow, KlineWindowStream, TickWindow, TickWindowStream};
