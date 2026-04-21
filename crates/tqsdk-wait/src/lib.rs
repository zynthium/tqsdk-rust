#![cfg_attr(not(test), forbid(unsafe_code))]
//! Python-style `wait_update()` facade built on [`tqsdk_core`] and
//! [`tqsdk_session`].
//!
//! This crate owns diff-backed continuous consumption in single-owner wait
//! form: lightweight object references, serial windows, and thin trade command
//! submission.
//!
//! One-shot direct query, schema refresh, metadata, and other non-streaming
//! helpers remain in [`tqsdk_session`]. Use [`TqApi::session`] when a wait-based
//! flow needs to reuse the same underlying session for those operations.
//!
//! # Runtime
//!
//! `tqsdk-wait` is a pure async substrate. Callers must provide their own
//! Tokio runtime.

mod api;
mod builder;
mod change;
mod driver;
mod error;
mod refs;
mod views;

pub use api::TqApi;
pub use builder::TqApiBuilder;
pub use change::ChangeTrackedRef;
pub use error::{Result, WaitFacadeError};
pub use refs::{
    AccountRef, KlineSerialRef, NotificationRef, OrderRef, PositionRef, PreInsertOrderRef,
    QuoteRef, RiskManagementDataRef, RiskManagementRuleRef, SecurityAccountRef, SecurityOrderRef,
    SecurityPositionRef, SecurityTradeRef, SettlementInfoRef, TickSerialRef, TradeRef,
    TradingStatusRef,
};
pub use views::{KlineWindow, TickWindow};
