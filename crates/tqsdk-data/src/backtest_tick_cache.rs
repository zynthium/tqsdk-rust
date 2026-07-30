use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use fs2::FileExt;
use tqsdk_core::Tick;

use crate::history_series_cache::{
    HistorySeriesCacheFileStatus, HistorySeriesCoverageCommit, HistorySeriesKind,
    HistorySeriesProvisionalCoverage, HistorySeriesWriteRows, HistorySeriesWriteSegment,
    TickDataSeriesReader,
};
use crate::{DataError, HistorySeriesCache, Result, TickDataSeries, TickDataSeriesRequest};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum BacktestCachePolicy {
    Disabled,
    CacheOnly,
    #[default]
    RemoteOnMiss,
    Refresh,
}

/// Tick-only backtest cache facade over the canonical history series cache.
///
/// The backtest facade does not expose the underlying generic cache handle.
///
/// ```compile_fail
/// let cache = tqsdk_data::BacktestTickCache::open(std::env::temp_dir()).unwrap();
/// let _ = cache.history_cache();
/// ```
#[derive(Clone)]
pub struct BacktestTickCache {
    history: HistorySeriesCache,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCoverage {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl BacktestTickCoverage {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// Durable, non-final high-water mark for an open trading-day snapshot.
///
/// A provisional checkpoint can resume a later fill, but it is intentionally
/// excluded from [`BacktestTickCoverage`] and cache-hit decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickProvisionalCoverage {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub complete_through_ns: i64,
    pub as_of_ns: i64,
    pub rows: usize,
    /// Inclusive remote/live tick id extent when one was observed.
    pub id_range: Option<(i64, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheStatus {
    pub backend_format: &'static str,
    pub cache_dir: PathBuf,
    pub series_path: PathBuf,
    pub series_path_exists: bool,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl BacktestTickCacheStatus {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCachePurgeReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub series_path: PathBuf,
    pub removed: bool,
    pub removed_files: usize,
    pub removed_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheWriteReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheInventory {
    pub backend_format: &'static str,
    pub cache_dir: PathBuf,
    pub symbols: Vec<BacktestTickCacheInventorySymbol>,
    pub total_files: usize,
    pub total_rows: usize,
    pub total_bytes: u64,
    pub total_days: usize,
    pub problem_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheInventorySymbol {
    pub symbol: String,
    pub files: usize,
    pub rows: usize,
    pub bytes: u64,
    pub days: usize,
    /// Observed on-disk id extent as `[first_id, one_past_last_id)`.
    ///
    /// This is a file inventory metric rather than request-scoped coverage: a
    /// trading-day partition can include rows just outside an inspected time range.
    pub id_range: Option<(i64, i64)>,
    pub problem_files: usize,
}

/// Lightweight filesystem inventory that does not decode TQBN record blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheFastInventory {
    pub backend_format: &'static str,
    pub cache_dir: PathBuf,
    pub symbols: Vec<BacktestTickCacheFastInventorySymbol>,
    pub total_files: usize,
    pub total_bytes: u64,
    pub total_days: usize,
    pub problem_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheFastInventorySymbol {
    pub symbol: String,
    pub files: usize,
    pub bytes: u64,
    pub days: usize,
    pub problem_files: usize,
}

/// Deep diagnostic result for one tick partition file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheDiagnostic {
    pub path: PathBuf,
    pub file_name: String,
    pub trading_day: Option<String>,
    pub symbol: String,
    pub status: HistorySeriesCacheFileStatus,
    pub id_range: Option<(i64, i64)>,
    pub rows: usize,
    pub size_bytes: u64,
    pub schema_version: Option<u32>,
    pub error: Option<String>,
}

impl BacktestTickCacheDiagnostic {
    #[must_use]
    pub fn is_problem(&self) -> bool {
        is_problem_file_status(self.status)
    }
}

/// Deep diagnostic projection for all tick partitions in a cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickCacheDiagnosticReport {
    pub backend_format: &'static str,
    pub cache_dir: PathBuf,
    pub files: Vec<BacktestTickCacheDiagnostic>,
    pub problem_files: usize,
}

/// Canonical TQBN trading-day range used by tick cache partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacktestTickTradingDayRange {
    pub trading_day: NaiveDate,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Advisory lock held while an operation needs a stable cache-root view.
#[derive(Debug)]
pub struct BacktestTickCacheOperationLock {
    cache_dir: PathBuf,
    path: PathBuf,
    file: File,
}

impl BacktestTickCacheOperationLock {
    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        self.cache_dir.as_path()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for BacktestTickCacheOperationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickFillReport {
    pub symbol: String,
    pub requested_range: (i64, i64),
    pub unique_rows: usize,
    /// Remote fill id extent as `(first_id, last_id)`, with both endpoints inclusive.
    pub id_range: Option<(i64, i64)>,
    pub first_datetime_ns: Option<i64>,
    pub last_datetime_ns: Option<i64>,
    pub complete: bool,
    pub gap_summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BacktestTickFill {
    symbol: String,
    range_start_ns: i64,
    range_end_ns: i64,
    rows_by_id: BTreeMap<i64, Tick>,
}

impl BacktestTickCache {
    #[must_use]
    pub fn new(history: HistorySeriesCache) -> Self {
        Self { history }
    }

    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(HistorySeriesCache::open(root_dir)?))
    }

    /// Open a cache root for inspection without creating files or directories.
    #[must_use]
    pub fn open_read_only(root_dir: impl AsRef<Path>) -> Self {
        Self::new(HistorySeriesCache::open_read_only(root_dir))
    }

    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        self.history.root_dir()
    }

    pub fn coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCoverage> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let report = self
            .history
            .tick_coverage(symbol, range_start_ns, range_end_ns)?;
        Ok(BacktestTickCoverage {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: report.symbol,
            range_start_ns: report.range_start_ns,
            range_end_ns: report.range_end_ns,
            cached_ranges: report.cached_ranges,
            missing_ranges: report.missing_ranges,
        })
    }

    pub fn inspect(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCacheStatus> {
        let coverage = self.coverage(symbol, range_start_ns, range_end_ns)?;
        let series_path = self.tick_series_path(coverage.symbol.as_str());
        let series_path_exists = self
            .history
            .series_exists(coverage.symbol.as_str(), HistorySeriesKind::Tick)?;
        Ok(BacktestTickCacheStatus {
            backend_format: self.history.format_id(),
            cache_dir: coverage.cache_dir,
            series_path_exists,
            series_path,
            symbol: coverage.symbol,
            range_start_ns: coverage.range_start_ns,
            range_end_ns: coverage.range_end_ns,
            cached_ranges: coverage.cached_ranges,
            missing_ranges: coverage.missing_ranges,
        })
    }

    pub fn require_coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCoverage> {
        let coverage = self.coverage(symbol, range_start_ns, range_end_ns)?;
        if coverage.is_complete() {
            Ok(coverage)
        } else {
            Err(DataError::InvalidState(
                "backtest tick cache coverage is incomplete",
            ))
        }
    }

    pub fn tick_series_path(&self, symbol: impl AsRef<str>) -> PathBuf {
        self.history.tick_series_path(symbol.as_ref())
    }

    pub fn inventory(&self) -> Result<BacktestTickCacheInventory> {
        let scan = self.history.scan()?;
        let mut days = BTreeSet::new();
        let mut symbols = BTreeMap::<String, InventorySymbolAccumulator>::new();
        let mut total_files = 0usize;
        let mut total_rows = 0usize;
        let mut total_bytes = 0u64;
        let mut problem_files = 0usize;

        for file in scan.files {
            if file.duration_ns != Some(0) {
                continue;
            }
            let Some(symbol) = file.symbol.clone() else {
                continue;
            };
            total_files = total_files.saturating_add(1);
            total_rows = total_rows.saturating_add(file.rows);
            total_bytes = total_bytes.saturating_add(file.size_bytes);
            let is_problem = is_problem_file_status(file.status);
            if is_problem {
                problem_files = problem_files.saturating_add(1);
            }
            let day = tick_inventory_day(file.file_name.as_str());
            if let Some(day) = day.as_ref() {
                days.insert(day.clone());
            }
            symbols
                .entry(symbol.clone())
                .or_insert_with(|| InventorySymbolAccumulator::new(symbol))
                .push(
                    file.rows,
                    file.size_bytes,
                    file.id_range,
                    day.as_deref(),
                    is_problem,
                );
        }

        Ok(BacktestTickCacheInventory {
            backend_format: self.history.format_id(),
            cache_dir: self.history.root_dir().to_path_buf(),
            symbols: symbols
                .into_values()
                .map(InventorySymbolAccumulator::finish)
                .collect(),
            total_files,
            total_rows,
            total_bytes,
            total_days: days.len(),
            problem_files,
        })
    }

    /// Return a filesystem-only inventory of daily TQBN tick partitions.
    ///
    /// Unlike [`Self::inventory`], this only reads each file's metadata and
    /// magic prefix. It is appropriate for a frequently refreshed status view,
    /// but it does not establish row counts, coverage, or full file health.
    pub fn fast_inventory(&self) -> Result<BacktestTickCacheFastInventory> {
        let mut days = BTreeSet::new();
        let mut symbols = BTreeMap::<String, FastInventorySymbolAccumulator>::new();
        let mut total_files = 0usize;
        let mut total_bytes = 0u64;
        let mut problem_files = 0usize;

        for entry in fast_tick_partition_files(self.history.root_dir())? {
            total_files = total_files.saturating_add(1);
            total_bytes = total_bytes.saturating_add(entry.size_bytes);
            if entry.is_problem {
                problem_files = problem_files.saturating_add(1);
            }
            days.insert(entry.trading_day.clone());
            symbols
                .entry(entry.symbol.clone())
                .or_insert_with(|| FastInventorySymbolAccumulator::new(entry.symbol.clone()))
                .push(entry.size_bytes, &entry.trading_day, entry.is_problem);
        }

        Ok(BacktestTickCacheFastInventory {
            backend_format: self.history.format_id(),
            cache_dir: self.history.root_dir().to_path_buf(),
            symbols: symbols
                .into_values()
                .map(FastInventorySymbolAccumulator::finish)
                .collect(),
            total_files,
            total_bytes,
            total_days: days.len(),
            problem_files,
        })
    }

    /// Decode all tick partitions and report their health without mutating the cache.
    pub fn diagnose(&self) -> Result<BacktestTickCacheDiagnosticReport> {
        let scan = self.history.scan()?;
        let mut files = scan
            .files
            .into_iter()
            .filter(|file| file.duration_ns == Some(0))
            .filter_map(|file| {
                let symbol = file.symbol?;
                Some(BacktestTickCacheDiagnostic {
                    trading_day: tick_inventory_day(file.file_name.as_str()),
                    path: file.path,
                    file_name: file.file_name,
                    symbol,
                    status: file.status,
                    id_range: file.id_range,
                    rows: file.rows,
                    size_bytes: file.size_bytes,
                    schema_version: file.schema_version,
                    error: file.error,
                })
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.file_name.cmp(&right.file_name));
        let problem_files = files.iter().filter(|file| file.is_problem()).count();
        Ok(BacktestTickCacheDiagnosticReport {
            backend_format: self.history.format_id(),
            cache_dir: self.history.root_dir().to_path_buf(),
            files,
            problem_files,
        })
    }

    /// Try to acquire the cache-root lock required by remote fill operations.
    ///
    /// The lock is advisory and intentionally separate from individual TQBN
    /// file locks: it prevents concurrent owners from requesting and writing
    /// the same missing historical ranges.
    pub fn try_acquire_remote_fill_lock(&self) -> Result<BacktestTickCacheOperationLock> {
        self.try_acquire_operation_lock("remote fill", true)
    }

    /// Try to acquire a shared stable-view lock for verification and diagnostics.
    pub fn try_acquire_consistency_read_lock(&self) -> Result<BacktestTickCacheOperationLock> {
        self.try_acquire_operation_lock("consistency read", false)
    }

    fn try_acquire_operation_lock(
        &self,
        operation: &'static str,
        exclusive: bool,
    ) -> Result<BacktestTickCacheOperationLock> {
        let cache_dir = self.history.root_dir().to_path_buf();
        fs::create_dir_all(&cache_dir)?;
        let path = cache_dir.join(".tqsdk-cache-operation.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let result = if exclusive {
            FileExt::try_lock_exclusive(&file)
        } else {
            FileExt::try_lock_shared(&file)
        };
        match result {
            Ok(()) => Ok(BacktestTickCacheOperationLock {
                cache_dir,
                path,
                file,
            }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Err(DataError::CacheBusy {
                cache_dir,
                operation,
            }),
            Err(error) => Err(error.into()),
        }
    }

    pub fn purge_symbol_ticks(
        &self,
        symbol: impl AsRef<str>,
    ) -> Result<BacktestTickCachePurgeReport> {
        let symbol = symbol.as_ref();
        if symbol.is_empty() {
            return Err(DataError::InvalidState(
                "backtest tick cache symbol must not be empty",
            ));
        }
        let report = self.history.purge_tick_series(symbol)?;
        let removed = report.removed();
        Ok(BacktestTickCachePurgeReport {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: report.symbol,
            series_path: report.path,
            removed,
            removed_files: report.removed_files,
            removed_bytes: report.removed_bytes,
        })
    }

    pub fn compact_symbol_ticks(&self, symbol: impl AsRef<str>) -> Result<()> {
        let symbol = symbol.as_ref();
        if symbol.is_empty() {
            return Err(DataError::InvalidState(
                "backtest tick cache symbol must not be empty",
            ));
        }
        self.history.compact_series(symbol, HistorySeriesKind::Tick)
    }

    pub fn store_ticks(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: impl IntoIterator<Item = Tick>,
    ) -> Result<BacktestTickCacheWriteReport> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let mut rows = rows.into_iter().collect::<Vec<_>>();
        if rows
            .iter()
            .any(|row| row.datetime < range_start_ns || row.datetime >= range_end_ns)
        {
            return Err(DataError::InvalidState(
                "tick row is outside declared backtest tick cache range",
            ));
        }
        normalize_tick_rows(&mut rows);
        let rows_len = rows.len();
        let id_range = tick_id_range(rows.iter().map(|row| row.id))?;
        let report = self.append_partial_ticks(symbol, rows)?;
        self.mark_complete(symbol, range_start_ns, range_end_ns, rows_len, id_range)?;
        Ok(BacktestTickCacheWriteReport {
            cache_dir: report.cache_dir,
            symbol: report.symbol,
            range_start_ns,
            range_end_ns,
            rows: rows_len,
        })
    }

    pub fn append_partial_ticks(
        &self,
        symbol: impl AsRef<str>,
        rows: impl IntoIterator<Item = Tick>,
    ) -> Result<BacktestTickCacheWriteReport> {
        self.append_partial_ticks_with_coverage(
            symbol,
            rows,
            std::iter::empty::<(i64, i64, usize, Option<(i64, i64)>)>(),
        )
    }

    pub(crate) fn append_partial_ticks_with_coverage(
        &self,
        symbol: impl AsRef<str>,
        rows: impl IntoIterator<Item = Tick>,
        coverage: impl IntoIterator<Item = (i64, i64, usize, Option<(i64, i64)>)>,
    ) -> Result<BacktestTickCacheWriteReport> {
        let symbol = symbol.as_ref();
        if symbol.is_empty() {
            return Err(DataError::InvalidState(
                "backtest tick cache symbol must not be empty",
            ));
        }
        let mut rows = rows.into_iter().collect::<Vec<_>>();
        normalize_tick_rows(&mut rows);
        let coverage = coverage
            .into_iter()
            .map(
                |(range_start_ns, range_end_ns, rows, id_range)| HistorySeriesCoverageCommit {
                    symbol: symbol.to_string(),
                    kind: HistorySeriesKind::Tick,
                    range_start_ns,
                    range_end_ns,
                    rows,
                    id_range,
                },
            )
            .collect::<Vec<_>>();
        self.history.write_segment_with_coverage(
            HistorySeriesWriteSegment {
                symbol,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(rows.as_slice()),
            },
            coverage.as_slice(),
        )?;
        Ok(BacktestTickCacheWriteReport {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: symbol.to_string(),
            range_start_ns: rows.iter().map(|row| row.datetime).min().unwrap_or(0),
            range_end_ns: rows
                .iter()
                .map(|row| row.datetime)
                .max()
                .map_or(0, |datetime| datetime.saturating_add(1)),
            rows: rows.len(),
        })
    }

    pub fn mark_complete(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: usize,
        id_range: Option<(i64, i64)>,
    ) -> Result<BacktestTickCoverage> {
        let symbol = symbol.as_ref();
        self.mark_complete_without_inspection(
            symbol,
            range_start_ns,
            range_end_ns,
            rows,
            id_range,
        )?;
        self.coverage(symbol, range_start_ns, range_end_ns)
    }

    /// Persist an open-day checkpoint without promoting it to final coverage.
    pub fn mark_provisional(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        complete_through_ns: i64,
        as_of_ns: i64,
        rows: usize,
        id_range: Option<(i64, i64)>,
    ) -> Result<BacktestTickProvisionalCoverage> {
        let symbol = symbol.as_ref();
        self.mark_provisional_without_inspection(
            symbol,
            range_start_ns,
            complete_through_ns,
            as_of_ns,
            rows,
            id_range,
        )?;
        self.provisional_coverage(symbol, range_start_ns, complete_through_ns)?
            .ok_or(DataError::InvalidState(
                "provisional tick coverage was not persisted",
            ))
    }

    pub(crate) fn mark_provisional_without_inspection(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        complete_through_ns: i64,
        as_of_ns: i64,
        rows: usize,
        id_range: Option<(i64, i64)>,
    ) -> Result<()> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, complete_through_ns)?;
        if complete_through_ns > as_of_ns {
            return Err(DataError::InvalidState(
                "provisional tick coverage cannot extend beyond its as-of time",
            ));
        }
        let start_day = backtest_tick_trading_day_for_timestamp_ns(range_start_ns)?;
        let complete_day =
            backtest_tick_trading_day_for_timestamp_ns(complete_through_ns.saturating_sub(1))?;
        let as_of_day = backtest_tick_trading_day_for_timestamp_ns(as_of_ns.saturating_sub(1))?;
        if start_day != complete_day || start_day != as_of_day {
            return Err(DataError::InvalidState(
                "provisional tick coverage must stay within one TQBN trading-day partition",
            ));
        }
        self.history
            .append_provisional(HistorySeriesProvisionalCoverage {
                symbol: symbol.to_string(),
                kind: HistorySeriesKind::Tick,
                range_start_ns,
                complete_through_ns,
                as_of_ns,
                rows,
                id_range,
            })?;
        Ok(())
    }

    /// Return the longest non-final checkpoint that starts at or before the
    /// requested range and is not already superseded by final coverage.
    pub fn provisional_coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<Option<BacktestTickProvisionalCoverage>> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        Ok(self
            .history
            .provisional_coverage(crate::history_series_cache::HistorySeriesCoverageRequest {
                symbol: symbol.to_string(),
                kind: HistorySeriesKind::Tick,
                range_start_ns,
                range_end_ns,
            })?
            .map(|checkpoint| BacktestTickProvisionalCoverage {
                cache_dir: self.history.root_dir().to_path_buf(),
                symbol: checkpoint.symbol,
                range_start_ns: checkpoint.range_start_ns,
                complete_through_ns: checkpoint.complete_through_ns,
                as_of_ns: checkpoint.as_of_ns,
                rows: checkpoint.rows,
                id_range: checkpoint.id_range,
            }))
    }

    /// Commit coverage after rows are durable without rescanning the series.
    ///
    /// Live recording keeps its own contiguous-id state, so it only needs the
    /// append operation here. Callers that need an authoritative coverage view
    /// must use [`Self::mark_complete`] or [`Self::coverage`] afterwards.
    pub(crate) fn mark_complete_without_inspection(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        rows: usize,
        id_range: Option<(i64, i64)>,
    ) -> Result<()> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        self.history.append_coverage(HistorySeriesCoverageCommit {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns,
            range_end_ns,
            rows,
            id_range,
        })
    }

    pub fn load_series(&self, request: TickDataSeriesRequest) -> Result<TickDataSeries> {
        self.require_coverage(
            request.symbol(),
            request.start_datetime_ns(),
            request.end_datetime_ns(),
        )?;
        self.history.read_tick_data_series(request)
    }

    /// Open the cache-backed reader used by the backtest-history query path.
    ///
    /// The public [`Self::load_series`] API intentionally remains final-only.
    /// An explicit provisional query can use an open-day checkpoint, but only
    /// after its durable high-water mark covers the complete effective range.
    #[allow(dead_code)]
    pub(crate) fn open_history_query_reader(
        &self,
        request: TickDataSeriesRequest,
        provisional_as_of_ns: Option<i64>,
    ) -> Result<TickDataSeriesReader> {
        let symbol = request.symbol();
        let start_datetime_ns = request.start_datetime_ns();
        let end_datetime_ns = request.end_datetime_ns();

        if provisional_as_of_ns.is_some() {
            let checkpoint = self
                .provisional_coverage(symbol, start_datetime_ns, end_datetime_ns)?
                .ok_or(DataError::InvalidState(
                    "backtest provisional tick cache coverage is unavailable",
                ))?;
            if checkpoint.complete_through_ns < end_datetime_ns {
                return Err(DataError::InvalidState(
                    "backtest provisional tick cache coverage is incomplete",
                ));
            }
        } else {
            self.require_coverage(symbol, start_datetime_ns, end_datetime_ns)?;
        }

        self.history.open_tick_data_series_reader_unchecked(
            symbol,
            start_datetime_ns,
            end_datetime_ns,
        )
    }
}

/// Return the canonical TQBN trading day for a timestamp in nanoseconds.
///
/// Daily tick partitions roll at 18:00:00 CST and weekend boundaries are
/// normalized to the following Monday.
pub fn backtest_tick_trading_day_for_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    crate::history_series_cache::tqbn_trading_day_for_timestamp_ns(timestamp_ns)
}

/// Return the complete canonical TQBN range for a requested trading day.
pub fn backtest_tick_trading_day_range(day: NaiveDate) -> Result<BacktestTickTradingDayRange> {
    let (trading_day, start_ns, end_ns) = crate::history_series_cache::tqbn_trading_day_range(day)?;
    Ok(BacktestTickTradingDayRange {
        trading_day,
        start_ns,
        end_ns,
    })
}

impl BacktestTickFill {
    #[must_use]
    pub fn new(symbol: impl Into<String>, range_start_ns: i64, range_end_ns: i64) -> Self {
        Self {
            symbol: symbol.into(),
            range_start_ns,
            range_end_ns,
            rows_by_id: BTreeMap::new(),
        }
    }

    pub fn push(&mut self, row: Tick) -> Result<bool> {
        if row.datetime < self.range_start_ns || row.datetime >= self.range_end_ns {
            return Ok(false);
        }
        Ok(self.rows_by_id.insert(row.id, row).is_none())
    }

    #[must_use]
    pub fn drain_rows(&self) -> Vec<Tick> {
        self.rows_by_id.values().cloned().collect()
    }

    pub fn finish(&self, end_tolerance_ns: i64) -> Result<BacktestTickFillReport> {
        self.finish_inner(end_tolerance_ns, false)
    }

    pub fn finish_after_idle(&self, end_tolerance_ns: i64) -> Result<BacktestTickFillReport> {
        self.finish_inner(end_tolerance_ns, true)
    }

    fn finish_inner(
        &self,
        end_tolerance_ns: i64,
        allow_idle_tail: bool,
    ) -> Result<BacktestTickFillReport> {
        let first = self.rows_by_id.values().next();
        let last = self.rows_by_id.values().next_back();
        let id_range = first.zip(last).map(|(first, last)| (first.id, last.id));
        let unique_rows = self.rows_by_id.len();
        let first_datetime_ns = first.map(|row| row.datetime);
        let last_datetime_ns = last.map(|row| row.datetime);
        let mut complete = first.is_some() || allow_idle_tail;
        let mut gap_summary = None;
        if let Some((first_id, last_id)) = id_range {
            let expected = last_id.saturating_sub(first_id).saturating_add(1);
            if expected != unique_rows as i64 {
                complete = false;
                gap_summary = Some(format!(
                    "tick id range {first_id}..={last_id} contains {unique_rows} unique rows"
                ));
            }
        } else if !allow_idle_tail {
            complete = false;
        }
        if !allow_idle_tail
            && last_datetime_ns
                .is_none_or(|last_ns| last_ns < self.range_end_ns.saturating_sub(end_tolerance_ns))
        {
            complete = false;
        }
        Ok(BacktestTickFillReport {
            symbol: self.symbol.clone(),
            requested_range: (self.range_start_ns, self.range_end_ns),
            unique_rows,
            id_range,
            first_datetime_ns,
            last_datetime_ns,
            complete,
            gap_summary,
        })
    }
}

#[derive(Debug, Default)]
struct InventorySymbolAccumulator {
    symbol: String,
    files: usize,
    rows: usize,
    bytes: u64,
    days: BTreeSet<String>,
    id_range: Option<(i64, i64)>,
    problem_files: usize,
}

impl InventorySymbolAccumulator {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            ..Self::default()
        }
    }

    fn push(
        &mut self,
        rows: usize,
        bytes: u64,
        id_range: Option<(i64, i64)>,
        day: Option<&str>,
        is_problem: bool,
    ) {
        self.files = self.files.saturating_add(1);
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
        if let Some(day) = day {
            self.days.insert(day.to_string());
        }
        if let Some((start, end)) = id_range {
            self.id_range = Some(match self.id_range {
                Some((current_start, current_end)) => {
                    (current_start.min(start), current_end.max(end))
                }
                None => (start, end),
            });
        }
        if is_problem {
            self.problem_files = self.problem_files.saturating_add(1);
        }
    }

    fn finish(self) -> BacktestTickCacheInventorySymbol {
        BacktestTickCacheInventorySymbol {
            symbol: self.symbol,
            files: self.files,
            rows: self.rows,
            bytes: self.bytes,
            days: self.days.len(),
            id_range: self.id_range,
            problem_files: self.problem_files,
        }
    }
}

#[derive(Debug, Default)]
struct FastInventorySymbolAccumulator {
    symbol: String,
    files: usize,
    bytes: u64,
    days: BTreeSet<String>,
    problem_files: usize,
}

impl FastInventorySymbolAccumulator {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            ..Self::default()
        }
    }

    fn push(&mut self, bytes: u64, trading_day: &str, is_problem: bool) {
        self.files = self.files.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.days.insert(trading_day.to_string());
        if is_problem {
            self.problem_files = self.problem_files.saturating_add(1);
        }
    }

    fn finish(self) -> BacktestTickCacheFastInventorySymbol {
        BacktestTickCacheFastInventorySymbol {
            symbol: self.symbol,
            files: self.files,
            bytes: self.bytes,
            days: self.days.len(),
            problem_files: self.problem_files,
        }
    }
}

#[derive(Debug)]
struct FastTickPartitionFile {
    symbol: String,
    trading_day: String,
    size_bytes: u64,
    is_problem: bool,
}

fn fast_tick_partition_files(root_dir: &Path) -> Result<Vec<FastTickPartitionFile>> {
    let series_root = root_dir.join("series");
    if !series_root.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for day_entry in fs::read_dir(&series_root)? {
        let day_entry = day_entry?;
        if !day_entry.file_type()?.is_dir() {
            continue;
        }
        let trading_day = day_entry.file_name().to_string_lossy().into_owned();
        if NaiveDate::parse_from_str(&trading_day, "%Y%m%d").is_err() {
            continue;
        }
        let tick_dir = day_entry.path().join("tick");
        if !tick_dir.is_dir() {
            continue;
        }
        for file_entry in fs::read_dir(tick_dir)? {
            let file_entry = file_entry?;
            if !file_entry.file_type()?.is_file() {
                continue;
            }
            let path = file_entry.path();
            let Some(symbol) = fast_tick_symbol_from_path(&path) else {
                continue;
            };
            let size_bytes = file_entry.metadata()?.len();
            let is_problem = fast_tqbn_magic_is_problem(&path, size_bytes)?;
            files.push(FastTickPartitionFile {
                symbol,
                trading_day: trading_day.clone(),
                size_bytes,
                is_problem,
            });
        }
    }
    files.sort_by(|left, right| {
        left.trading_day
            .cmp(&right.trading_day)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    Ok(files)
}

fn fast_tick_symbol_from_path(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_string_lossy();
    let encoded = file_name.strip_suffix(".tqbn")?;
    (!encoded.is_empty()).then(|| encoded.replace("%2F", "/"))
}

fn fast_tqbn_magic_is_problem(path: &Path, size_bytes: u64) -> Result<bool> {
    if size_bytes == 0 {
        return Ok(false);
    }
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic != *b"TQBN"),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn is_problem_file_status(status: HistorySeriesCacheFileStatus) -> bool {
    !matches!(
        status,
        HistorySeriesCacheFileStatus::Readable | HistorySeriesCacheFileStatus::EmptySegment
    )
}

fn tick_inventory_day(file_name: &str) -> Option<String> {
    let (day, rest) = file_name.split_once('/')?;
    if rest.starts_with("tick/") {
        NaiveDate::parse_from_str(day, "%Y%m%d")
            .ok()
            .map(|day| day.format("%Y-%m-%d").to_string())
    } else {
        None
    }
}

fn validate_range(symbol: &str, range_start_ns: i64, range_end_ns: i64) -> Result<()> {
    if symbol.is_empty() {
        return Err(DataError::InvalidState(
            "backtest tick cache symbol must not be empty",
        ));
    }
    if range_start_ns >= range_end_ns {
        return Err(DataError::InvalidState(
            "backtest tick cache range_start_ns must be less than range_end_ns",
        ));
    }
    Ok(())
}

fn tick_id_range(ids: impl IntoIterator<Item = i64>) -> Result<Option<(i64, i64)>> {
    let mut min_id = None;
    let mut max_id = None;
    for id in ids {
        min_id = Some(min_id.map_or(id, |value: i64| value.min(id)));
        max_id = Some(max_id.map_or(id, |value: i64| value.max(id)));
    }
    let Some(start) = min_id else {
        return Ok(None);
    };
    let end = max_id
        .and_then(|id: i64| id.checked_add(1))
        .ok_or(DataError::InvalidState(
            "backtest tick cache id range overflow",
        ))?;
    Ok(Some((start, end)))
}

fn normalize_tick_rows(rows: &mut Vec<Tick>) {
    if rows.windows(2).all(|pair| {
        let previous = &pair[0];
        let current = &pair[1];
        previous.id < current.id
            || (previous.id == current.id && previous.datetime < current.datetime)
    }) {
        return;
    }
    rows.sort_by_key(|row| (row.id, row.datetime, row.epoch));
    rows.dedup_by(|left, right| left.id == right.id && left.datetime == right.datetime);
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::NaiveDate;
    use tqsdk_core::Tick;

    use super::{
        BacktestTickCache, HistorySeriesKind, HistorySeriesWriteRows, HistorySeriesWriteSegment,
        TickDataSeriesRequest, backtest_tick_trading_day_range, normalize_tick_rows,
    };

    #[test]
    fn tick_normalization_keeps_monotonic_unique_rows_in_place() {
        let mut rows = vec![
            Tick {
                id: 1,
                datetime: 1_000,
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 2_000,
                ..Tick::default()
            },
        ];

        normalize_tick_rows(&mut rows);

        assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), [1, 2]);
    }

    #[test]
    fn tick_normalization_restores_legacy_sort_and_dedup_behavior() {
        let mut rows = vec![
            Tick {
                id: 2,
                datetime: 2_000,
                epoch: Some(2),
                ..Tick::default()
            },
            Tick {
                id: 1,
                datetime: 1_000,
                epoch: Some(1),
                ..Tick::default()
            },
            Tick {
                id: 2,
                datetime: 2_000,
                epoch: Some(3),
                ..Tick::default()
            },
        ];

        normalize_tick_rows(&mut rows);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 2);
    }

    #[test]
    fn provisional_history_query_reader_requires_checkpoint_without_relaxing_load_series() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("tqsdk-provisional-reader-{nanos}"));
        let cache = BacktestTickCache::open(&root).expect("cache should open");
        let day = backtest_tick_trading_day_range(
            NaiveDate::from_ymd_opt(2026, 1, 5).expect("test date must be valid"),
        )
        .expect("trading-day range should resolve");
        let start_ns = day.start_ns + 1_000;
        let end_ns = start_ns + 1_000;
        let row = Tick {
            id: 1,
            datetime: start_ns,
            ..Tick::default()
        };

        cache
            .history
            .write_segment(HistorySeriesWriteSegment {
                symbol: "SHFE.rb2601",
                kind: HistorySeriesKind::Tick,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(&row)),
            })
            .expect("raw row should be durable without final coverage");
        cache
            .mark_provisional(
                "SHFE.rb2601",
                start_ns,
                end_ns,
                end_ns,
                1,
                Some((row.id, row.id)),
            )
            .expect("provisional checkpoint should persist");

        let request = TickDataSeriesRequest::new("SHFE.rb2601", start_ns, end_ns);
        assert!(cache.load_series(request.clone()).is_err());

        let mut reader = cache
            .open_history_query_reader(request, Some(end_ns))
            .expect("provisional reader should accept complete checkpoint");
        assert_eq!(reader.next_tick().unwrap().unwrap().id, 1);
        assert!(reader.next_tick().unwrap().is_none());
    }
}
