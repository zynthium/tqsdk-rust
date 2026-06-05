#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional market relay and cache service for `tqsdk-rust`.
//!
//! This crate is infrastructure. Existing SDK crates do not depend on it and
//! direct-to-TQ behavior remains the default unless users explicitly point the
//! market endpoint at a relay instance.

pub mod cache;
pub mod config;
pub mod error;
pub mod protocol;

pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use error::{RelayError, RelayResult};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
