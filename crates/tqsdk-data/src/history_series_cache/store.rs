use std::path::{Path, PathBuf};

use tqsdk_core::{Kline, Tick};

use crate::Result;

use super::{HistorySeriesCacheMaintenanceReport, HistorySeriesCacheScanReport};

pub const HISTORY_SERIES_CACHE_FORMAT_ID: &str = "tqsdk.tqbn.daily.v2";

#[deprecated(note = "use HISTORY_SERIES_CACHE_FORMAT_ID")]
pub const SERIES_FILE_HISTORY_SERIES_FORMAT_ID: &str = HISTORY_SERIES_CACHE_FORMAT_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistorySeriesKind {
    Kline { duration_ns: i64 },
    Tick,
}

impl HistorySeriesKind {
    #[must_use]
    pub(crate) fn duration_ns(self) -> i64 {
        match self {
            Self::Kline { duration_ns } => duration_ns,
            Self::Tick => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistorySeriesCoverageRequest {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCoverageReport {
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCoverageReport {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistorySeriesCoverageCommit {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
    pub id_range: Option<(i64, i64)>,
}

#[derive(Debug, Clone)]
pub(crate) enum HistorySeriesWriteRows<'a> {
    Klines(&'a [Kline]),
    Ticks(&'a [Tick]),
}

#[derive(Debug, Clone)]
pub(crate) struct HistorySeriesWriteSegment<'a> {
    pub symbol: &'a str,
    pub kind: HistorySeriesKind,
    pub declared_range_ns: Option<(i64, i64)>,
    pub rows: HistorySeriesWriteRows<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistorySeriesSegmentReport {
    pub path: PathBuf,
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub id_range: Option<(i64, i64)>,
    pub range_start_ns: Option<i64>,
    pub range_end_ns: Option<i64>,
    pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesPurgeReport {
    pub path: PathBuf,
    pub symbol: String,
    pub removed_files: usize,
    pub removed_bytes: u64,
}

impl HistorySeriesPurgeReport {
    #[must_use]
    pub fn removed(&self) -> bool {
        self.removed_files > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistorySeriesReadRequest {
    pub symbol: String,
    pub kind: HistorySeriesKind,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
}

#[derive(Debug, Clone)]
pub(crate) enum HistorySeriesRow {
    Kline(Kline),
    Tick(Tick),
}

pub(crate) trait HistorySeriesReader: Send {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>>;
}

pub(crate) trait HistorySeriesStore: Send + Sync {
    fn format_id(&self) -> &'static str;
    fn schema_version(&self) -> u32;
    fn root_dir(&self) -> &Path;
    fn series_path(&self, symbol: &str, kind: HistorySeriesKind) -> PathBuf;
    fn series_exists(&self, symbol: &str, kind: HistorySeriesKind) -> Result<bool> {
        Ok(self.series_path(symbol, kind).exists())
    }
    fn scan(&self) -> Result<HistorySeriesCacheScanReport>;
    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport>;
    fn compact_series(&self, symbol: &str, kind: HistorySeriesKind) -> Result<()>;
    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport>;
    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport>;
    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport>;
    fn purge_series(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
    ) -> Result<HistorySeriesPurgeReport>;
    fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>>;
}
