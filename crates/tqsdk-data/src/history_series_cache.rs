use std::path::{Path, PathBuf};
use std::sync::Arc;

use tqsdk_core::{Kline, Tick};

use crate::client::{
    KlineDataSeries, KlineDataSeriesRequest, TickDataSeries, TickDataSeriesRequest,
};
use crate::error::{DataError, Result};

mod ranges;
mod series_file_store;
mod storage;
mod store;

pub(crate) use ranges::{rangeset_difference, rangeset_intersection};
pub(crate) use store::{
    HistorySeriesCoverageCommit, HistorySeriesCoverageRequest, HistorySeriesKind,
    HistorySeriesReadRequest, HistorySeriesReader, HistorySeriesRow, HistorySeriesSegmentReport,
    HistorySeriesStore, HistorySeriesWriteRows, HistorySeriesWriteSegment,
};
pub use store::{
    HistorySeriesCoverageReport, HistorySeriesPurgeReport, SERIES_FILE_HISTORY_SERIES_FORMAT_ID,
};

const DEFAULT_CACHE_DIR: &str = ".tqsdk/data_series_1";
pub const HISTORY_SERIES_CACHE_SCHEMA_VERSION: u32 = 1;
const KLINE_DATA_COLS: usize = 7;
const TICK_1_LEVEL_DATA_COLS: usize = 11;
const TICK_5_LEVEL_DATA_COLS: usize = 27;
const TICK_TAIL_REFRESH_NS: i64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheReport {
    pub cache_dir: PathBuf,
    pub hit_rows: usize,
    pub downloaded_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCacheReport {
    pub(crate) fn new(
        cache_dir: PathBuf,
        hit_rows: usize,
        downloaded_ranges: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            cache_dir,
            hit_rows,
            downloaded_ranges,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheMiss {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub duration_ns: i64,
    pub start_datetime_ns: i64,
    pub end_datetime_ns: i64,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCacheMiss {
    fn new(
        cache_dir: PathBuf,
        symbol: impl Into<String>,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        missing_ranges: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            cache_dir,
            symbol: symbol.into(),
            duration_ns,
            start_datetime_ns,
            end_datetime_ns,
            missing_ranges,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySeriesCacheFileStatus {
    Readable,
    EmptySegment,
    InvalidRowWidth,
    IncompleteWrite,
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheFileReport {
    pub path: PathBuf,
    pub file_name: String,
    pub status: HistorySeriesCacheFileStatus,
    pub symbol: Option<String>,
    pub duration_ns: Option<i64>,
    pub id_range: Option<(i64, i64)>,
    pub row_width: Option<usize>,
    pub rows: usize,
    pub size_bytes: u64,
    pub schema_version: Option<u32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheScanReport {
    pub cache_dir: PathBuf,
    pub schema_version: u32,
    pub files: Vec<HistorySeriesCacheFileReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistorySeriesCacheMaintenanceReport {
    pub removed_files: usize,
    pub removed_bytes: u64,
}

#[derive(Clone)]
/// Coverage-aware local history series cache.
///
/// Low-level segment writes and coverage-bypassing window reads are internal
/// implementation details. Public callers should use typed range writers and
/// `read_*_data_series` cache-only readers instead.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCache;
///
/// let cache = HistorySeriesCache::open(std::env::temp_dir()).unwrap();
/// let _ = cache.write_kline_segment("SHFE.rb2601", 60_000_000_000, &[]);
/// let _ = cache.write_tick_segment("SHFE.rb2601", &[]);
/// let _ = cache.read_kline_window("SHFE.rb2601", 60_000_000_000, 0, 1);
/// let _ = cache.read_tick_window("SHFE.rb2601", 0, 1);
/// ```
///
/// The storage backend is identified by `format_id()`, not by a public enum.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCacheBackend;
///
/// let _ = HistorySeriesCacheBackend::SeriesFile;
/// ```
///
/// Series kind and generic coverage requests are internal storage details.
///
/// ```compile_fail
/// use tqsdk_data::{HistorySeriesCoverageRequest, HistorySeriesKind};
///
/// let _ = HistorySeriesCoverageRequest {
///     symbol: "SHFE.rb2601".to_string(),
///     kind: HistorySeriesKind::Tick,
///     range_start_ns: 0,
///     range_end_ns: 1,
/// };
/// ```
///
/// Missing ranges are exposed through typed coverage reports rather than
/// separate shallow wrapper methods.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCache;
///
/// let cache = HistorySeriesCache::open(std::env::temp_dir()).unwrap();
/// let _ = cache.missing_kline_datetime_ranges("SHFE.rb2601", 60_000_000_000, 0, 1);
/// let _ = cache.missing_tick_datetime_ranges("SHFE.rb2601", 0, 1);
/// ```
///
/// `.tqseries` is the canonical storage format, so `open` is the public
/// constructor; storage-format aliases stay internal.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCache;
///
/// let _ = HistorySeriesCache::open_series_file(std::env::temp_dir());
/// ```
///
/// Default cache-directory selection belongs to `DataClientBuilder`; explicit
/// cache handles use `open(root_dir)`.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCache;
///
/// let _ = HistorySeriesCache::open_default();
/// ```
///
/// Scan reports expose health/status, not storage-file taxonomy.
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCacheFileKind;
///
/// let _ = HistorySeriesCacheFileKind::Segment;
/// ```
///
/// ```compile_fail
/// use tqsdk_data::HistorySeriesCache;
///
/// let cache = HistorySeriesCache::open(std::env::temp_dir()).unwrap();
/// let scan = cache.scan().unwrap();
/// let _ = scan.files[0].kind;
/// ```
pub struct HistorySeriesCache {
    store: Arc<dyn HistorySeriesStore>,
}

impl HistorySeriesCache {
    #[must_use]
    fn from_store(store: Arc<dyn HistorySeriesStore>) -> Self {
        Self { store }
    }

    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        let store = series_file_store::SeriesFileHistoryStore::new(root_dir)?;
        Ok(Self::from_store(Arc::new(store)))
    }

    pub fn root_dir(&self) -> &Path {
        self.store.root_dir()
    }

    #[must_use]
    pub fn format_id(&self) -> &'static str {
        self.store.format_id()
    }

    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.store.schema_version()
    }

    pub fn kline_series_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.series_path(symbol, HistorySeriesKind::Kline { duration_ns })
    }

    pub fn tick_series_path(&self, symbol: &str) -> PathBuf {
        self.series_path(symbol, HistorySeriesKind::Tick)
    }

    pub(crate) fn series_path(&self, symbol: &str, kind: HistorySeriesKind) -> PathBuf {
        self.store.series_path(symbol, kind)
    }

    pub fn kline_coverage(
        &self,
        symbol: &str,
        duration_ns: i64,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<HistorySeriesCoverageReport> {
        self.coverage(HistorySeriesCoverageRequest {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Kline { duration_ns },
            range_start_ns,
            range_end_ns,
        })
    }

    pub fn tick_coverage(
        &self,
        symbol: &str,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<HistorySeriesCoverageReport> {
        self.coverage(HistorySeriesCoverageRequest {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns,
            range_end_ns,
        })
    }

    pub(crate) fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        self.store.coverage(request)
    }

    pub(crate) fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        self.store.write_segment(segment)
    }

    pub(crate) fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        self.store.commit_coverage(commit)
    }

    pub fn purge_kline_series(
        &self,
        symbol: &str,
        duration_ns: i64,
    ) -> Result<HistorySeriesPurgeReport> {
        self.purge_series(symbol, HistorySeriesKind::Kline { duration_ns })
    }

    pub fn purge_tick_series(&self, symbol: &str) -> Result<HistorySeriesPurgeReport> {
        self.purge_series(symbol, HistorySeriesKind::Tick)
    }

    pub(crate) fn purge_series(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
    ) -> Result<HistorySeriesPurgeReport> {
        self.store.purge_series(symbol, kind)
    }

    pub(crate) fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        self.store.open_reader(request)
    }

    pub fn read_kline_data_series(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataSeries> {
        let spec = request.validate()?;
        let coverage = self.kline_coverage(
            request.symbol(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        if !coverage.missing_ranges.is_empty() {
            return Err(DataError::CacheMiss(Box::new(HistorySeriesCacheMiss::new(
                self.root_dir().to_path_buf(),
                request.symbol(),
                spec.duration_ns,
                spec.start_datetime_ns,
                spec.end_datetime_ns,
                coverage.missing_ranges,
            ))));
        }

        let rows = self.read_kline_window(
            request.symbol(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = rows.len();
        Ok(KlineDataSeries::new(
            request.symbol().to_string(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            self.root_dir().to_path_buf(),
            hit_rows,
            Vec::new(),
        )))
    }

    pub fn read_tick_data_series(&self, request: TickDataSeriesRequest) -> Result<TickDataSeries> {
        let spec = request.validate()?;
        let coverage = self.tick_coverage(
            request.symbol(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        if !coverage.missing_ranges.is_empty() {
            return Err(DataError::CacheMiss(Box::new(HistorySeriesCacheMiss::new(
                self.root_dir().to_path_buf(),
                request.symbol(),
                0,
                spec.start_datetime_ns,
                spec.end_datetime_ns,
                coverage.missing_ranges,
            ))));
        }

        let rows = self.read_tick_window(
            request.symbol(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = rows.len();
        Ok(TickDataSeries::new(
            request.symbol().to_string(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            self.root_dir().to_path_buf(),
            hit_rows,
            Vec::new(),
        )))
    }

    pub fn write_kline_range(
        &self,
        symbol: &str,
        duration_ns: i64,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: &[Kline],
    ) -> Result<Option<(i64, i64)>> {
        Ok(self
            .write_segment(HistorySeriesWriteSegment {
                symbol,
                kind: HistorySeriesKind::Kline { duration_ns },
                declared_range_ns: Some((range_start_ns, range_end_ns)),
                rows: HistorySeriesWriteRows::Klines(rows),
            })?
            .id_range)
    }

    pub fn write_tick_range(
        &self,
        symbol: &str,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: &[Tick],
    ) -> Result<Option<(i64, i64)>> {
        Ok(self
            .write_segment(HistorySeriesWriteSegment {
                symbol,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: Some((range_start_ns, range_end_ns)),
                rows: HistorySeriesWriteRows::Ticks(rows),
            })?
            .id_range)
    }

    pub(crate) fn read_kline_window(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Kline>> {
        let mut reader = self.open_reader(HistorySeriesReadRequest {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Kline { duration_ns },
            range_start_ns: start_datetime_ns,
            range_end_ns: end_datetime_ns,
        })?;
        let mut rows = Vec::new();
        while let Some(row) = reader.next_row()? {
            if let HistorySeriesRow::Kline(row) = row {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub(crate) fn read_tick_window(
        &self,
        symbol: &str,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Tick>> {
        let mut reader = self.open_reader(HistorySeriesReadRequest {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns: start_datetime_ns,
            range_end_ns: end_datetime_ns,
        })?;
        let mut rows = Vec::new();
        while let Some(row) = reader.next_row()? {
            if let HistorySeriesRow::Tick(row) = row {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        self.store.scan()
    }

    pub fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        self.store.enforce_limits(max_bytes, retention_days)
    }
}

pub(crate) fn default_cache_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(DEFAULT_CACHE_DIR))
        .unwrap_or_else(|| std::env::temp_dir().join("tqsdk_data_series_1"))
}

fn merge_datetime_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for range in ranges {
        if range.0 >= range.1 {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && range.0 <= last.1
        {
            last.1 = last.1.max(range.1);
            continue;
        }
        merged.push(range);
    }
    merged
}
