use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use tqsdk_core::Tick;

use crate::history_series_cache::{
    HistorySeriesCacheFileStatus, HistorySeriesCoverageCommit, HistorySeriesKind,
    HistorySeriesWriteRows, HistorySeriesWriteSegment,
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
    pub id_range: Option<(i64, i64)>,
    pub problem_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestTickFillReport {
    pub symbol: String,
    pub requested_range: (i64, i64),
    pub unique_rows: usize,
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
        rows.sort_by_key(|row| (row.id, row.datetime, row.epoch));
        rows.dedup_by(|left, right| left.id == right.id && left.datetime == right.datetime);
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
        let symbol = symbol.as_ref();
        if symbol.is_empty() {
            return Err(DataError::InvalidState(
                "backtest tick cache symbol must not be empty",
            ));
        }
        let mut rows = rows.into_iter().collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.id, row.datetime, row.epoch));
        rows.dedup_by(|left, right| left.id == right.id && left.datetime == right.datetime);
        self.history.write_segment(HistorySeriesWriteSegment {
            symbol,
            kind: HistorySeriesKind::Tick,
            declared_range_ns: None,
            rows: HistorySeriesWriteRows::Ticks(rows.as_slice()),
        })?;
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
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let report = self.history.commit_coverage(HistorySeriesCoverageCommit {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns,
            range_end_ns,
            rows,
            id_range,
        })?;
        Ok(BacktestTickCoverage {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: report.symbol,
            range_start_ns: report.range_start_ns,
            range_end_ns: report.range_end_ns,
            cached_ranges: report.cached_ranges,
            missing_ranges: report.missing_ranges,
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

fn is_problem_file_status(status: HistorySeriesCacheFileStatus) -> bool {
    !matches!(
        status,
        HistorySeriesCacheFileStatus::Readable | HistorySeriesCacheFileStatus::EmptySegment
    )
}

fn tick_inventory_day(file_name: &str) -> Option<String> {
    let (day, rest) = file_name.split_once('/')?;
    if rest.starts_with("tick/") {
        Some(day.to_string())
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
