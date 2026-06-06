#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional market relay and cache service for `tqsdk-rust`.
//!
//! This crate is infrastructure. Existing SDK crates do not depend on it and
//! direct-to-TQ behavior remains the default unless users explicitly point the
//! market endpoint at a relay instance.

pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod engine;
pub mod error;
pub mod interest;
pub mod kline;
pub mod observability;
pub mod protocol;
pub mod server;
pub mod upstream;

pub use bootstrap::{BootstrapQueue, BootstrapRequest};
pub use cache::MarketCache;
pub use config::{BootstrapConfig, RelayConfig};
pub use engine::{DownstreamFrame, RelayEngine};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use observability::{HealthSnapshot, MetricsSnapshot, RelaySourceStatus};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
pub use server::RelayServer;
pub use upstream::{FakeUpstreamTickSource, UpstreamTick, UpstreamTickSource};
