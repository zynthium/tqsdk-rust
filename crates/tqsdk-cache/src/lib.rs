//! Shared contracts for the `tqsdk-cache` command-line tool.
//!
//! The crate intentionally manages only the canonical daily TQBN tick cache.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tqsdk::{
    BacktestCacheWarmupAction, BacktestCacheWarmupReport, BacktestRemoteFillConfig,
    BacktestTickCacheStatus,
};
use tqsdk_data::{
    BacktestTickCache, DataError, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range, default_history_cache_dir,
};

pub const REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingDayWindow {
    pub start_day: String,
    pub end_day: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TradingDayWindow {
    pub fn from_days(start_day: NaiveDate, end_day: NaiveDate) -> Result<Self, DataError> {
        let start = backtest_tick_trading_day_range(start_day)?;
        let end = backtest_tick_trading_day_range(end_day)?;
        if start.trading_day > end.trading_day {
            return Err(DataError::Validation(
                "start trading day must not be after end trading day".to_string(),
            ));
        }
        Ok(Self {
            start_day: start.trading_day.format("%Y-%m-%d").to_string(),
            end_day: end.trading_day.format("%Y-%m-%d").to_string(),
            start_ns: start.start_ns,
            end_ns: end.end_ns,
        })
    }

    pub fn closed_from_days(start_day: NaiveDate, end_day: NaiveDate) -> Result<Self, DataError> {
        let window = Self::from_days(start_day, end_day)?;
        let current_open_day = current_open_trading_day()?;
        let end_day = NaiveDate::parse_from_str(&window.end_day, "%Y-%m-%d").map_err(|error| {
            DataError::Validation(format!("invalid normalized TQBN trading day: {error}"))
        })?;
        if end_day >= current_open_day {
            return Err(DataError::Validation(format!(
                "requested end trading day {} is not closed; current open trading day is {}",
                window.end_day,
                current_open_day.format("%Y-%m-%d")
            )));
        }
        Ok(window)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheCoverageSnapshot {
    pub series_path: String,
    pub series_path_exists: bool,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
    pub complete: bool,
}

impl From<&BacktestTickCacheStatus> for CacheCoverageSnapshot {
    fn from(value: &BacktestTickCacheStatus) -> Self {
        Self {
            series_path: value.series_path.display().to_string(),
            series_path_exists: value.series_path_exists,
            cached_ranges: value.cached_ranges.clone(),
            missing_ranges: value.missing_ranges.clone(),
            complete: value.is_complete(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillReportSymbol {
    pub symbol: String,
    pub action: String,
    pub before: CacheCoverageSnapshot,
    pub after: CacheCoverageSnapshot,
    pub rows_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillConfigReport {
    pub symbol_batch_size: usize,
    pub symbol_concurrency: usize,
    pub idle_timeout_secs: u64,
    pub batch_timeout_secs: Option<u64>,
    pub slice_secs: Option<u64>,
    pub allow_empty_idle: bool,
}

impl From<BacktestRemoteFillConfig> for FillConfigReport {
    fn from(value: BacktestRemoteFillConfig) -> Self {
        Self {
            symbol_batch_size: value.symbol_batch_size,
            symbol_concurrency: value.symbol_concurrency,
            idle_timeout_secs: value.idle_timeout.as_secs(),
            batch_timeout_secs: value.batch_timeout.map(|value| value.as_secs()),
            slice_secs: value.slice.map(|value| value.as_secs()),
            allow_empty_idle: value.allow_empty_idle,
        }
    }
}

/// Stable, credential-free report written after a fill operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillReport {
    pub schema_version: u32,
    pub generated_at: String,
    /// Absolute canonical root used by `verify --report`.
    pub cache_dir: String,
    pub requested_days: TradingDayWindow,
    pub requested_range: (i64, i64),
    pub logical_symbols: Vec<String>,
    pub physical_symbols: Vec<FillReportSymbol>,
    pub fill_config: FillConfigReport,
    pub remote_used: bool,
    pub rows_written: usize,
    pub complete: bool,
    pub dry_run: bool,
}

impl FillReport {
    pub fn from_warmup(
        warmup: &BacktestCacheWarmupReport,
        cache_dir: &Path,
        requested_days: TradingDayWindow,
        fill_config: BacktestRemoteFillConfig,
        dry_run: bool,
    ) -> Self {
        let physical_symbols = warmup
            .symbols
            .iter()
            .map(|symbol| FillReportSymbol {
                symbol: symbol.symbol.clone(),
                action: warmup_action_name(symbol.action).to_string(),
                before: CacheCoverageSnapshot::from(&symbol.before),
                after: CacheCoverageSnapshot::from(&symbol.after),
                rows_written: symbol.rows_written,
            })
            .collect::<Vec<_>>();
        let complete = warmup.symbols_missing == 0
            && physical_symbols.iter().all(|symbol| symbol.after.complete);
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: cache_dir.display().to_string(),
            requested_range: warmup.requested_range,
            requested_days,
            logical_symbols: warmup.logical_symbols.clone(),
            physical_symbols,
            fill_config: fill_config.into(),
            remote_used: warmup.remote_used,
            rows_written: warmup.rows_written,
            complete,
            dry_run,
        }
    }

    pub fn physical_symbols(&self) -> Result<Vec<String>, DataError> {
        let mut symbols = self
            .physical_symbols
            .iter()
            .map(|symbol| symbol.symbol.trim())
            .filter(|symbol| !symbol.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(DataError::Validation(
                "fill report contains no physical cache symbols".to_string(),
            ));
        }
        Ok(symbols)
    }
}

pub fn open_cache(cache_dir: Option<&Path>) -> Result<(BacktestTickCache, PathBuf), DataError> {
    let root = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(default_history_cache_dir);
    let cache = BacktestTickCache::open(root)?;
    let canonical = fs::canonicalize(cache.cache_dir())?;
    Ok((cache, canonical))
}

/// Resolve and open a cache root without creating directories or files.
pub fn open_read_only_cache(
    cache_dir: Option<&Path>,
) -> Result<(BacktestTickCache, PathBuf), DataError> {
    let root = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(default_history_cache_dir);
    let canonical = match fs::canonicalize(&root) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if root.is_absolute() {
                root.clone()
            } else {
                std::env::current_dir()?.join(&root)
            }
        }
        Err(error) => return Err(error.into()),
    };
    Ok((
        BacktestTickCache::open_read_only(canonical.clone()),
        canonical,
    ))
}

pub fn default_fill_report_path(cache_dir: &Path) -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    cache_dir.join("reports").join(format!(
        "tqsdk-cache-fill-{timestamp}-{}.json",
        std::process::id()
    ))
}

pub fn write_fill_report(path: &Path, report: &FillReport) -> Result<(), DataError> {
    let parent = path.parent().ok_or_else(|| {
        DataError::Validation("fill report path must have a parent directory".to_string())
    })?;
    fs::create_dir_all(parent)?;
    let file = File::create(path)?;
    serde_json::to_writer_pretty(BufWriter::new(file), report)
        .map_err(|error| DataError::InvalidResponse(error.to_string()))
}

pub fn read_fill_report(path: &Path) -> Result<FillReport, DataError> {
    let file = File::open(path)?;
    let report: FillReport = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    if report.schema_version != REPORT_SCHEMA_VERSION {
        return Err(DataError::Validation(format!(
            "unsupported fill report schema {}; expected {REPORT_SCHEMA_VERSION}",
            report.schema_version
        )));
    }
    Ok(report)
}

pub fn warmup_action_name(action: BacktestCacheWarmupAction) -> &'static str {
    match action {
        BacktestCacheWarmupAction::SkippedComplete => "skipped_complete",
        BacktestCacheWarmupAction::MissingCacheOnly => "missing_cache_only",
        BacktestCacheWarmupAction::FilledRemote => "filled_remote",
        BacktestCacheWarmupAction::RefreshedRemote => "refreshed_remote",
    }
}

fn current_open_trading_day() -> Result<NaiveDate, DataError> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .ok_or(DataError::InvalidState(
            "system clock is before the Unix epoch or outside TQBN range",
        ))?;
    backtest_tick_trading_day_for_timestamp_ns(now_ns)
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, TimeZone};

    use super::TradingDayWindow;

    #[test]
    fn trading_day_window_normalizes_weekend_and_keeps_evening_boundary() {
        let window = TradingDayWindow::from_days(
            chrono::NaiveDate::from_ymd_opt(2026, 7, 17).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(window.start_day, "2026-07-17");
        assert_eq!(window.end_day, "2026-07-20");
        assert_eq!(window.start_ns, cst_ns(2026, 7, 16, 18, 0, 0));
        assert_eq!(window.end_ns, cst_ns(2026, 7, 20, 18, 0, 0));
    }

    fn cst_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
        FixedOffset::east_opt(8 * 60 * 60)
            .unwrap()
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }
}
