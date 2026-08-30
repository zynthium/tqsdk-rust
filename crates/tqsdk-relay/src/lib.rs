#![cfg_attr(not(test), forbid(unsafe_code))]

//! Optional market relay and cache service for `tqsdk-rust`.
//!
//! This crate is infrastructure. Existing SDK crates do not depend on it and
//! direct-to-TQ behavior remains the default unless users explicitly point the
//! market endpoint at a relay instance.

extern crate self as tqsdk_relay;

pub mod bootstrap;
pub mod cache;
pub mod config;
pub mod dashboard;
mod dashboard_read_model;
pub mod diagnostics;
pub mod engine;
pub mod error;
pub mod interest;
pub mod kline;
pub mod metrics_http;
mod metrics_http_impl;
pub mod observability;
pub mod protocol;
pub mod pump;
#[cfg(feature = "server")]
pub mod runtime;
pub mod server;
mod symbol_identity;
pub mod symbol_metrics;
pub mod universe;
pub mod universe_expression;
pub mod upstream;

pub use bootstrap::{BootstrapQueue, BootstrapRequest};
pub use cache::MarketCache;
pub use config::{
    BootstrapConfig, DailyRefreshTime, FuturesUniverseRefreshSchedule, RelayConfig,
    UpstreamInsListLimits, next_daily_refresh_delay,
};
pub use dashboard_read_model::{
    DashboardSnapshot, DashboardSnapshotInputs, DashboardSymbolMetricsSnapshot, DashboardSymbolRow,
    DashboardTimelineHistory, DashboardTimelineHistorySample, DashboardTimelineSample,
    DashboardTimelineScope, DashboardTimelineSeverity, DashboardTimelineSymbolSample,
};
pub use diagnostics::RelayStartupReport;
pub use engine::{DownstreamFrame, RelayEngine, RelayEvent, RelayEventKind};
pub use error::{RelayError, RelayResult};
pub use interest::{ClientId, InterestRegistry, SourceKey};
pub use kline::KlineSynthesis;
pub use metrics_http::serve_metrics_until;
pub use observability::{
    DecodeHealth, FlowIdleHealth, HealthSnapshot, MetricsSnapshot, RelaySourceStage,
    RelaySourceStatus,
};
pub use protocol::{
    DownstreamCommand, RelayKlineRow, RelayMarketFrame, RelayTickRow, SetChartCommand,
};
pub use pump::{pump_available, pump_once};
#[cfg(feature = "server")]
pub use runtime::{
    connect_configured_upstream, resolve_configured_upstream_tick_chart,
    resolve_configured_upstream_tick_charts, spawn_configured_upstream_pump,
    spawn_configured_upstream_pump_with_retry_interval,
};
pub use server::RelayServer;
pub use symbol_metrics::{
    SymbolCoverage, SymbolFlow, SymbolIntegrity, SymbolMetricsContext, SymbolMetricsQuery,
    SymbolMetricsSnapshot, SymbolMetricsSummary, SymbolProblemSeverity, SymbolSession, SymbolSort,
    SymbolStatus, SymbolSubscriptionCounts, SymbolTelemetryReadModel, SymbolTelemetrySnapshot,
    SymbolTelemetryStore, SymbolTradingPhase, SymbolTradingPhaseSource,
};
#[cfg(feature = "metadata")]
pub use universe::SessionFuturesUniverseResolver;
pub use universe::{
    FuturesContract, FuturesProductCode, StaticFuturesUniverseResolver,
    futures_metadata_symbol_batches, resolve_futures_universe_symbols,
};
pub use universe_expression::{
    ExpandedUniverseInput, SnapshotUniverseDispatch, UniverseClause, UniverseExpression,
    UniverseInput, UniverseMode, UniverseSelector, UniverseSelectorKind, UniverseSpec,
    UniverseView, parse_snapshot_universe_compatible,
};
#[cfg(feature = "server")]
pub use upstream::WebSocketUpstreamTickSource;
pub use upstream::{
    FakeUpstreamTickSource, UpstreamMarketDecodeReport, UpstreamMarketEvent, UpstreamQuote,
    UpstreamSourceProgress, UpstreamSourceUpdate, UpstreamTick, UpstreamTickChart,
    UpstreamTickDecodeReport, UpstreamTickSource, UpstreamTradingStatus,
    decode_upstream_market_report, decode_upstream_tick_report, decode_upstream_ticks,
};
