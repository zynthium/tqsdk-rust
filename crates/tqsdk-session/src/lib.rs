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

mod builder;
mod client;
mod config;
mod direct_query;
mod error;
mod metadata;
mod services;

pub use builder::SessionClientBuilder;
pub use client::{SessionClient, SessionProgress};
pub use config::SessionFacadeConfig;
pub use direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionLevelQuotes, OptionQueryFilter, SessionDirectQuery, SessionMetadataQuery,
    SessionRawQuery, SessionServiceQuery, SymbolRankingType,
};
pub use error::{Result, SessionFacadeError};
