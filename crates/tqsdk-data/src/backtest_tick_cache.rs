use std::path::{Path, PathBuf};

use tqsdk_core::Tick;

use crate::{
    DataError, HistorySeriesCache, HistorySeriesCoverageRequest, HistorySeriesKind, Result,
    TickDataSeries, TickDataSeriesRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestCachePolicy {
    CacheOnly,
    RemoteOnMiss,
    RefreshMissing,
    RefreshAll,
}

impl Default for BacktestCachePolicy {
    fn default() -> Self {
        Self::RemoteOnMiss
    }
}

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
pub struct BacktestTickCacheWriteReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
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
    pub fn history_cache(&self) -> &HistorySeriesCache {
        &self.history
    }

    pub fn coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<BacktestTickCoverage> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        let report = self.history.coverage(HistorySeriesCoverageRequest {
            symbol: symbol.to_string(),
            kind: HistorySeriesKind::Tick,
            range_start_ns,
            range_end_ns,
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
        rows.sort_by_key(|row| (row.datetime, row.id, row.epoch));
        rows.dedup_by(|left, right| {
            left.datetime == right.datetime && left.id == right.id && left.epoch == right.epoch
        });
        self.history.write_tick_segment(symbol, rows.as_slice())?;
        self.history
            .record_declared_coverage_range(symbol, 0, range_start_ns, range_end_ns)?;
        Ok(BacktestTickCacheWriteReport {
            cache_dir: self.history.root_dir().to_path_buf(),
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            rows: rows.len(),
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
