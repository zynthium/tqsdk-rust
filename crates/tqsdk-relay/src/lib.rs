#![cfg_attr(not(test), forbid(unsafe_code))]
//! Optional market relay and cache service for `tqsdk-rust`.
//!
//! This crate is infrastructure. Existing SDK crates do not depend on it and
//! direct-to-TQ behavior remains the default unless users explicitly point the
//! market endpoint at a relay instance.

pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod diagnostics;
pub mod engine;
pub mod error;
pub mod interest;
pub mod kline;
pub mod metrics_http;
pub mod observability;
pub mod protocol;
pub mod pump;
#[cfg(feature = "server")]
pub mod runtime;
pub mod server;
pub mod universe;
pub mod upstream;

pub use bootstrap::{BootstrapQueue, BootstrapRequest};
pub use cache::MarketCache;
pub use config::{
    BootstrapConfig, DailyRefreshTime, FuturesUniverseRefreshSchedule, RelayConfig,
    UpstreamInsListLimits, next_daily_refresh_delay,
};
pub use diagnostics::RelayStartupReport;
pub use engine::{DownstreamFrame, RelayEngine};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use metrics_http::serve_metrics_until;
pub use observability::{HealthSnapshot, MetricsSnapshot, RelaySourceStatus};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
pub use pump::{pump_available, pump_once};
#[cfg(feature = "server")]
pub use runtime::{
    connect_configured_upstream, resolve_configured_upstream_tick_chart,
    spawn_configured_upstream_pump, spawn_configured_upstream_pump_with_retry_interval,
};
pub use server::RelayServer;
#[cfg(feature = "metadata")]
pub use universe::SessionFuturesUniverseResolver;
pub use universe::{
    FuturesContract, FuturesProductCode, FuturesProductFilter, StaticFuturesUniverseResolver,
    futures_metadata_symbol_batches, resolve_futures_symbols,
};
#[cfg(feature = "server")]
pub use upstream::WebSocketUpstreamTickSource;
pub use upstream::{
    FakeUpstreamTickSource, UpstreamTick, UpstreamTickChart, UpstreamTickSource,
    decode_upstream_ticks,
};
