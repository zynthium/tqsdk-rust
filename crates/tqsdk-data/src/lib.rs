#![cfg_attr(not(test), deny(unsafe_code))]
//! Research and offline data tooling for `tqsdk-rust`.
//!
//! `tqsdk-data` hosts research/offline helpers that should not widen the
//! public surface of `tqsdk-session`, `tqsdk-wait`, or caller-owned live consumers.
//!
//! Current stabilized surfaces stay intentionally narrow:
//!
//! - `DataClient::new().query_his_cont_quotes(...)`
//! - `DataClient::new().query_his_cont_underlyings(...)`
//! - `DataClient::new().query_his_cont_underlying_segments(...)`
//! - `DataClient::new().query_trading_calendar(...)`
//! - `DataClient::new().query_trading_calendar_holidays(...)`
//! - `DataClient::new().query_trading_days(...)`
//! - `historical_cont_underlying_segments(...)`
//! - `DataClient::from_session(...).get_kline_data_page(...)`
//! - `DataClient::from_session(...).get_tick_data_page(...)`
//! - `DataClient::from_session(...).get_kline_data_series(...)`
//! - `DataClient::from_session(...).get_tick_data_series(...)`
//! - `KlineDataSeries::integrity_report()` / `TickDataSeries::integrity_report()`
//! - `HistorySeriesCache::open_tick_data_series_reader(...)`
//! - `LiveTickCacheWriter::new(...).push_ticks(...)` / `flush()`
//! - `DataClient::from_session(...).kline_data_download(...)`
//! - `DataClient::from_session(...).tick_data_download(...)`
//! - `KlineDataDownload::collect_remaining()`
//! - `TickDataDownload::collect_remaining()`
//! - `DataClient::from_session(...).query_option_greeks(...)`
//! - `DataClient::from_session(...).export_kline_data_csv(...)`
//! - `DataClient::from_session(...).export_tick_data_csv(...)`
//!
//! All of them return owned Rust-native data without committing to any
//! DataFrame, CSV writer, or polars integration yet.
//!
//! # Example
//!
//! ```
//! let request = tqsdk_data::KlineDataSeriesRequest::new(
//!     "SHFE.au2602",
//!     std::time::Duration::from_secs(60),
//!     1_000,
//!     2_000,
//! );
//! assert_eq!(request.symbol(), "SHFE.au2602");
//! # Ok::<(), tqsdk_data::DataError>(())
//! ```

mod aggregation;
mod backtest_history;
mod backtest_tick_cache;
mod client;
mod daily_kline_cache;
mod download;
mod error;
mod export;
mod greeks;
mod historical_fill_universe;
mod historical_universe;
mod historical_universe_acquisition;
mod historical_universe_artifact;
mod historical_universe_resolution;
mod history_series_cache;
mod integrity;
mod live_quote;
mod live_tick_cache_writer;
mod minute_kline_cache;
mod universe;
mod universe_expression;

#[cfg(fuzzing)]
#[doc(hidden)]
pub fn __fuzz_safe_cache_file_name(raw: &str) -> String {
    let mut name = raw
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '\0' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect::<String>();
    if name.is_empty() || name == "." || name == ".." {
        name = "fuzz.60.0.1".to_string();
    }
    if name.len() > 160 {
        name.truncate(160);
    }
    name
}

pub use aggregation::{
    CANONICAL_MINUTE_KLINE_NS, KlineSessionPosition, KlineSessionTemplate, KlineSessionWindow,
    MinuteKlineAggregationUpdate, MinuteKlineAggregator, TickKlineAggregationUpdate,
    TickKlineAggregator,
};
pub use backtest_history::{
    BACKTEST_HISTORY_METADATA_FORMAT_ID, BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
    BacktestHistoryAuthProvider, BacktestHistoryBatchReport, BacktestHistoryChunk,
    BacktestHistoryClient, BacktestHistoryClientBuilder, BacktestHistoryCollected,
    BacktestHistoryCollectedBatch, BacktestHistoryCoverageReport, BacktestHistoryCredentials,
    BacktestHistoryEvent, BacktestHistoryFailureReason, BacktestHistoryField,
    BacktestHistoryFillCancellation, BacktestHistoryFillConfig, BacktestHistoryFillFamily,
    BacktestHistoryFillProgress, BacktestHistoryFillSymbolResult, BacktestHistoryFillSymbolStatus,
    BacktestHistoryFillTerminalReport, BacktestHistoryFillTerminalStatus, BacktestHistoryFinality,
    BacktestHistoryInspection, BacktestHistoryKind, BacktestHistoryMaintenanceClient,
    BacktestHistoryMaintenanceClientBuilder, BacktestHistoryMarketKind,
    BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot, BacktestHistoryPhase,
    BacktestHistoryPhysicalSegment, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestHistoryRequestFailure, BacktestHistoryRequestId, BacktestHistoryRequestReport,
    BacktestHistoryRows, BacktestHistoryRun, BacktestHistorySchemaSeries, BacktestHistorySnapshot,
    BacktestHistorySnapshotError, BacktestHistorySnapshotEvent,
    BacktestHistorySnapshotFileDisposition, BacktestHistorySnapshotFileRole,
    BacktestHistorySnapshotManifestArtifact, BacktestHistorySnapshotManifestBuilder,
    BacktestHistorySnapshotQueryResources, BacktestHistorySnapshotResourceBudget,
    BacktestHistorySnapshotResourceReservation, BacktestHistorySnapshotRun,
    BacktestHistoryTelemetryEvent, BacktestHistoryTelemetryStream, BacktestHistoryTradingDay,
    BacktestHistoryValueKind, backtest_history_default_fields, backtest_history_resolve_fields,
    backtest_history_schema_fields, classify_backtest_history_snapshot_cache_path,
};
#[doc(hidden)]
pub use backtest_history::{
    plan_minute_cache_stale_partition_repair, resolve_backtest_metadata_snapshot,
    resolve_minute_cache_metadata_snapshot,
};
pub use backtest_tick_cache::{
    BacktestCachePolicy, BacktestTickCache, BacktestTickCacheDiagnostic,
    BacktestTickCacheDiagnosticReport, BacktestTickCacheFastInventory,
    BacktestTickCacheFastInventorySymbol, BacktestTickCacheInventory,
    BacktestTickCacheInventorySymbol, BacktestTickCacheLegacyPartitionLockRepair,
    BacktestTickCacheLockRepairFile, BacktestTickCacheLockRepairMode,
    BacktestTickCacheLockRepairReport, BacktestTickCacheLockRepairStatus,
    BacktestTickCacheOperationLock, BacktestTickCachePurgeReport, BacktestTickCacheStatus,
    BacktestTickCacheWriteReport, BacktestTickCoverage, BacktestTickFill, BacktestTickFillReport,
    BacktestTickProvisionalCoverage, BacktestTickTradingDayRange,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
pub use client::{
    DataClient, DataClientBuilder, HistoricalContQuotesRow, HistoricalContUnderlyingRow,
    HistoricalContUnderlyingSegment, KlineDataPage, KlineDataPageRequest, KlineDataSeries,
    KlineDataSeriesRequest, TickDataPage, TickDataPageRequest, TickDataSeries,
    TickDataSeriesRequest, TradingCalendarHolidays, TradingCalendarRow,
    historical_cont_underlying_segments,
};
pub use daily_kline_cache::{
    DAILY_KLINE_CACHE_FORMAT_ID, DAILY_KLINE_CACHE_SCHEMA_VERSION, DAILY_KLINE_DURATION_NS,
    DailyKlineCache, DailyKlineCacheDiagnosticReport, DailyKlineCacheDiagnosticScanReport,
    DailyKlineCacheDiagnosticStatus, DailyKlineCacheFastInventory,
    DailyKlineCacheFastInventorySymbol, DailyKlineCachePurgeReport, DailyKlineCacheSnapshot,
    DailyKlineCacheStatus, DailyKlineCacheWriteReport, DailyKlineCoverage,
};
pub use download::{
    DataDownloadProgress, KlineDataDownload, KlineDataDownloadPage, TickDataDownload,
    TickDataDownloadPage,
};
pub use error::{DataError, Result};
pub use export::{KlineCsvExportSummary, TickCsvExportSummary};
pub use greeks::{OptionGreeksRequest, OptionGreeksResult, OptionGreeksRow};
pub use historical_fill_universe::{
    HISTORICAL_FILL_UNIVERSE_CANONICALIZATION, HistoricalFillUniverseSpec,
};
pub use historical_universe::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DerivedView, DynamicUniverseScope,
    HistoricalCatalogProof, HistoricalUniverseFillTarget, HistoricalUniversePlan,
    HistoricalUniversePlanV3Execution, HistoricalUniversePlanV3Identity,
    HistoricalUniverseTimeline, UniverseBudget, UniverseInstrumentId, UniverseMemberChange,
    UniverseTimelineBatch,
};
pub use historical_universe_acquisition::{
    PROVIDER_CURRENT_FUTURES_SOURCE_IDENTITY, PROVIDER_CURRENT_PHYSICAL_FUTURES_EXCHANGES,
    ProviderCurrentHistoricalCatalogAcquirer,
};
pub use historical_universe_artifact::{
    HISTORICAL_UNIVERSE_ARTIFACT_FORMAT_VERSION, HISTORICAL_UNIVERSE_ARTIFACT_NAMESPACE,
    HistoricalAcquisitionContract, HistoricalCatalogAcquisition, HistoricalDataKind,
    HistoricalSemanticCatalog, HistoricalUniverseArtifactStore,
};
pub use historical_universe_resolution::{
    HISTORICAL_UNIVERSE_COMPILER_IDENTITY, HistoricalDependencyRole, HistoricalUniverseDependency,
    HistoricalUniverseKindTarget, HistoricalUniverseResolution,
    compile_historical_universe_resolution,
};
#[allow(deprecated)]
pub use history_series_cache::SERIES_FILE_HISTORY_SERIES_FORMAT_ID;
pub use history_series_cache::{
    HISTORY_SERIES_CACHE_FORMAT_ID, HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCache,
    HistorySeriesCacheFileReport, HistorySeriesCacheFileStatus,
    HistorySeriesCacheMaintenanceReport, HistorySeriesCacheMiss, HistorySeriesCacheReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageReport, HistorySeriesPurgeReport,
    TickDataSeriesReader, default_history_cache_dir,
};
pub use integrity::{
    DuplicatedHistoryRow, HistoryCacheStatus, HistoryDataKind, HistoryDuplicateField,
    HistoryIntegrityCheck, HistoryIntegrityReport, HistoryPermissionStatus,
    NonMonotonicHistoryTimestamp, OutOfRangeHistoryRow,
};
pub use live_tick_cache_writer::{LiveTickCacheWriteReport, LiveTickCacheWriter};
pub use minute_kline_cache::{
    MINUTE_KLINE_CACHE_FORMAT_ID, MINUTE_KLINE_CACHE_SCHEMA_VERSION, MINUTE_KLINE_DURATION_NS,
    MinuteKlineCache, MinuteKlineCacheDiagnosticFile, MinuteKlineCacheDiagnosticReport,
    MinuteKlineCacheDiagnosticStatus, MinuteKlineCacheInventory, MinuteKlineCacheInventorySymbol,
    MinuteKlineCacheMigrationReport, MinuteKlineCacheMonthReport, MinuteKlineCachePurgeReport,
    MinuteKlineCacheSnapshot, MinuteKlineCacheStatus, MinuteKlineCacheWriteReport,
    MinuteKlineCoverage, MinuteKlineReader, trading_month_for_timestamp_ns,
};
pub use universe::{
    DEFAULT_FUTURES_METADATA_BATCH_SIZE, FuturesContract, FuturesProductCode,
    FuturesUniverseResolver, SessionFuturesUniverseResolver, StaticFuturesUniverseResolver,
    contract_from_configured_symbol, expression_requires_activity_quotes,
    futures_contracts_from_symbol_info, futures_metadata_symbol_batches,
    resolve_futures_contracts_with_expression, resolve_futures_universe_symbols,
    resolve_static_symbols_with_expression, session_client_builder_for_futures_discovery,
    static_contracts_with_expression,
};
pub use universe_expression::{
    UniverseClause, UniverseExpression, UniverseSelector, UniverseSelectorKind,
};
