#![cfg_attr(not(test), forbid(unsafe_code))]
//! Shared async session and direct-query layer built on top of [`tqsdk_core`].
//!
//! This crate owns one-shot request/response interactions that do not depend on a
//! particular continuous-consumption facade, including GraphQL/HTTP query,
//! schema refresh, metadata lookup, calendar data, settlement/ranking queries,
//! and related service helpers.
//!
//! Streaming or `wait_update()`-style continuous consumption intentionally lives
//! in higher facade crates such as `tqsdk-wait`.
//!
//! # Runtime
//!
//! `tqsdk-session` is a pure async substrate. Callers must provide their own
//! Tokio runtime.
//!
//! # Example
//!
//! ```
//! let builder = tqsdk_session::SessionClientBuilder::new("demo-user", "demo-pass")
//!     .enable_query()
//!     .futures_market();
//!
//! assert!(builder.query_enabled());
//! ```

mod builder;
mod client;
mod direct_query;
mod error;
#[cfg(feature = "live")]
mod http_executor;
mod instrument;
mod metadata;
mod order_intent;
mod recovery;
#[cfg(feature = "http-client")]
mod response_body;
#[cfg(feature = "services")]
mod services;
pub mod testing;
#[cfg(feature = "tq-auth")]
mod tq_auth;
#[cfg(feature = "tq-auth")]
mod tqkq;

pub use builder::SessionClientBuilder;
pub use client::{
    MarketChartLease, MarketQuoteLease, MarketTradingStatusLease, SessionClient, SessionProgress,
};
pub use direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionLevelQuotes, OptionQueryFilter, SessionDirectQuery, SessionMetadataQuery,
    SessionRawQuery, SessionServiceQuery, SymbolRankingType,
};
pub use error::{Result, SessionErrorDiagnostic, SessionErrorKind, SessionFacadeError};
pub use instrument::{InstrumentClass, InstrumentSpec, SymbolInfo};
pub use order_intent::{OrderIntentRecord, OrderIntentRegistration, OrderIntentSpec};
pub use recovery::{StartupRecoverySpec, StartupRecoveryStatus};
pub use tqsdk_core::RetryHint;
