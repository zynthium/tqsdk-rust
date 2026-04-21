#![cfg_attr(not(test), forbid(unsafe_code))]

#[doc(hidden)]
pub mod builder;
#[doc(hidden)]
pub mod client;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod direct_query;
#[doc(hidden)]
pub mod error;
mod metadata;
mod services;

pub use builder::SessionClientBuilder;
pub use client::SessionClient;
pub use config::SessionFacadeConfig;
pub use direct_query::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionLevelQuotes, OptionQueryFilter, SessionDirectQuery, SessionMetadataQuery,
    SessionRawQuery, SessionServiceQuery, SymbolRankingType,
};
pub use error::{Result, SessionFacadeError};
