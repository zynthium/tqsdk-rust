#![cfg_attr(not(test), forbid(unsafe_code))]

pub mod builder;
pub mod client;
pub mod config;
pub mod direct_query;
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
