#![cfg_attr(not(test), deny(unsafe_code))]
//! Research and offline data tooling for `tqsdk-rust`.
//!
//! `tqsdk-data` hosts research/offline helpers that should not widen the
//! public surface of `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`.
//!
//! Current stabilized surfaces stay intentionally narrow:
//!
//! - `DataClient::new().query_his_cont_quotes(...)`
//! - `DataClient::from_session(...).get_kline_data_page(...)`
//! - `DataClient::from_session(...).get_tick_data_page(...)`
//! - `DataClient::from_session(...).get_kline_data_series(...)`
//! - `DataClient::from_session(...).get_tick_data_series(...)`
//! - `DataClient::from_session(...).kline_data_download(...)`
//! - `DataClient::from_session(...).tick_data_download(...)`
//! - `KlineDataDownload::collect_remaining()`
//! - `TickDataDownload::collect_remaining()`
//! - `DataClient::from_session(...).query_option_greeks(...)`
//! - `DataClient::from_session(...).export_kline_data_csv(...)`
//! - `DataClient::from_session(...).export_tick_data_csv(...)`
//! - `MarketCacheWriter` / `MarketCacheReader` / `MarketCacheReplay`
//!
//! All of them return owned Rust-native data without committing to any
//! DataFrame, CSV writer, or polars integration yet.
//!
//! # Example
//!
//! ```
//! let event = tqsdk_data::MarketCacheEvent::quote(
//!     "example",
//!     "SHFE.au2602",
//!     1_000,
//!     Some(900),
//!     tqsdk_core::Quote::default(),
//! )?;
//!
//! let mut bytes = Vec::new();
//! let mut writer = tqsdk_data::MarketCacheWriter::new(&mut bytes);
//! writer.write_event(&event)?;
//! writer.flush()?;
//! # Ok::<(), tqsdk_data::DataError>(())
//! ```

mod client;
mod download;
mod error;
mod export;
mod greeks;
mod history_series_cache;
mod live_quote;
mod market_cache;
#[cfg(feature = "stream")]
mod stream_cache;

pub use client::{
    DataClient, DataClientBuilder, HistoricalContQuotesRow, KlineDataPage, KlineDataPageRequest,
    KlineDataSeries, KlineDataSeriesRequest, TickDataPage, TickDataPageRequest, TickDataSeries,
    TickDataSeriesRequest,
};
pub use download::{
    DataDownloadProgress, KlineDataDownload, KlineDataDownloadPage, TickDataDownload,
    TickDataDownloadPage,
};
pub use error::{DataError, Result};
pub use export::{KlineCsvExportSummary, TickCsvExportSummary};
pub use greeks::{OptionGreeksRequest, OptionGreeksResult, OptionGreeksRow};
pub use history_series_cache::{
    HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCache, HistorySeriesCacheBackend,
    HistorySeriesCacheFileKind, HistorySeriesCacheFileReport, HistorySeriesCacheFileStatus,
    HistorySeriesCacheMaintenanceReport, HistorySeriesCacheMiss, HistorySeriesCacheReport,
    HistorySeriesCacheScanReport,
};
pub use market_cache::{
    MarketCacheEvent, MarketCachePayload, MarketCachePayloadKind, MarketCacheReader,
    MarketCacheReplay, MarketCacheWriter,
};
#[cfg(feature = "stream")]
pub use stream_cache::MarketCacheStreamWriter;
