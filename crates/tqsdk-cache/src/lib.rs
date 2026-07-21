//! Shared contracts for the `tqsdk-cache` command-line tool.
//!
//! The crate intentionally manages only the canonical daily TQBN tick cache.

use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tqsdk::{
    BacktestCacheWarmupAction, BacktestCacheWarmupReport, BacktestRemoteFillConfig,
    BacktestTickCacheStatus,
};
use tqsdk_data::{
    BacktestTickCache, DataError, TradingCalendarRow, backtest_tick_trading_day_for_timestamp_ns,
    backtest_tick_trading_day_range, default_history_cache_dir,
};

pub const REPORT_SCHEMA_VERSION: u32 = 2;
pub const TRADING_CALENDAR_SCHEMA_VERSION: u32 = 1;

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

/// One generic Chinese trading-calendar entry used for CLI planning only.
///
/// It is intentionally distinct from TQBN coverage: calendar days help size
/// progress indicators, while cache coverage remains the authority for data
/// completeness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarDaySnapshot {
    pub date: String,
    pub trading: bool,
}

/// Credential-free cache-root snapshot of the generic trading calendar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarSnapshot {
    pub schema_version: u32,
    pub generated_at: String,
    pub source: String,
    pub days: Vec<TradingCalendarDaySnapshot>,
}

impl TradingCalendarSnapshot {
    pub fn from_rows(
        rows: impl IntoIterator<Item = TradingCalendarRow>,
    ) -> Result<Self, DataError> {
        let days = rows
            .into_iter()
            .map(|row| TradingCalendarDaySnapshot {
                date: row.date,
                trading: row.trading,
            })
            .collect::<Vec<_>>();
        let snapshot = Self {
            schema_version: TRADING_CALENDAR_SCHEMA_VERSION,
            generated_at: Utc::now().to_rfc3339(),
            source: "tqsdk-data.query_trading_calendar".to_string(),
            days,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    #[must_use]
    pub fn metadata(&self) -> TradingCalendarSnapshotMetadata {
        TradingCalendarSnapshotMetadata {
            schema_version: self.schema_version,
            generated_at: self.generated_at.clone(),
            source: self.source.clone(),
            hash: self.hash(),
            first_day: self.days.first().map(|day| day.date.clone()),
            last_day: self.days.last().map(|day| day.date.clone()),
            days: self.days.len(),
        }
    }

    pub fn covers(&self, start_day: NaiveDate, end_day: NaiveDate) -> Result<bool, DataError> {
        self.validate()?;
        if start_day > end_day {
            return Ok(false);
        }
        let mut expected = start_day;
        let mut days = self.days.iter();
        let mut current = days
            .next()
            .map(|day| parse_calendar_date(&day.date))
            .transpose()?;
        while expected <= end_day {
            while current.is_some_and(|day| day < expected) {
                current = days
                    .next()
                    .map(|day| parse_calendar_date(&day.date))
                    .transpose()?;
            }
            if current != Some(expected) {
                return Ok(false);
            }
            expected = expected.succ_opt().ok_or_else(|| {
                DataError::Validation("trading calendar date overflow".to_string())
            })?;
        }
        Ok(true)
    }

    pub fn trading_days_between(
        &self,
        start_day: NaiveDate,
        end_day: NaiveDate,
    ) -> Result<Vec<NaiveDate>, DataError> {
        if !self.covers(start_day, end_day)? {
            return Err(DataError::Validation(format!(
                "trading calendar snapshot does not cover {} to {}",
                start_day.format("%Y-%m-%d"),
                end_day.format("%Y-%m-%d")
            )));
        }
        let days = self
            .days
            .iter()
            .filter_map(|day| {
                let date = parse_calendar_date(&day.date).ok()?;
                (start_day <= date && date <= end_day && day.trading).then_some(date)
            })
            .collect::<Vec<_>>();
        Ok(days)
    }

    pub fn validate(&self) -> Result<(), DataError> {
        if self.schema_version != TRADING_CALENDAR_SCHEMA_VERSION {
            return Err(DataError::Validation(format!(
                "unsupported trading calendar schema {}; expected {TRADING_CALENDAR_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.days.is_empty() {
            return Err(DataError::Validation(
                "trading calendar snapshot contains no days".to_string(),
            ));
        }
        let mut previous = None;
        for day in &self.days {
            let date = parse_calendar_date(&day.date)?;
            if previous.is_some_and(|previous| date <= previous) {
                return Err(DataError::Validation(
                    "trading calendar snapshot days must be strictly ordered".to_string(),
                ));
            }
            previous = Some(date);
        }
        Ok(())
    }

    #[must_use]
    pub fn hash(&self) -> String {
        let mut value = 0xcbf2_9ce4_8422_2325_u64;
        for byte in self.source.bytes().chain(std::iter::once(0)) {
            value ^= u64::from(byte);
            value = value.wrapping_mul(0x0000_0100_0000_01b3);
        }
        for day in &self.days {
            for byte in day
                .date
                .bytes()
                .chain(std::iter::once(u8::from(day.trading)))
                .chain(std::iter::once(0))
            {
                value ^= u64::from(byte);
                value = value.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        format!("{value:016x}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarSnapshotMetadata {
    pub schema_version: u32,
    pub generated_at: String,
    pub source: String,
    pub hash: String,
    pub first_day: Option<String>,
    pub last_day: Option<String>,
    pub days: usize,
}

pub fn trading_calendar_snapshot_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("meta").join("trading-calendar-v1.json")
}

pub fn read_trading_calendar_snapshot(
    cache_dir: &Path,
) -> Result<Option<TradingCalendarSnapshot>, DataError> {
    let path = trading_calendar_snapshot_path(cache_dir);
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let snapshot: TradingCalendarSnapshot = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    snapshot.validate()?;
    Ok(Some(snapshot))
}

pub fn write_trading_calendar_snapshot(
    cache_dir: &Path,
    snapshot: &TradingCalendarSnapshot,
) -> Result<PathBuf, DataError> {
    snapshot.validate()?;
    let path = trading_calendar_snapshot_path(cache_dir);
    write_json_atomically(&path, snapshot)?;
    Ok(path)
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
    #[serde(default)]
    pub day_stats: Option<FillReportSymbolDayStats>,
}

/// Per-physical-cache-range day accounting recorded in a v2 fill report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillReportSymbolDayStats {
    pub symbol: String,
    pub planned_days: usize,
    pub covered_days: usize,
    pub missing_days: usize,
    pub received_days: usize,
}

/// Original CLI selector captured without credentials.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillSelectorReport {
    pub symbols: Vec<String>,
    pub universe: Option<String>,
}

/// Calendar source used to resolve and display a fill range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillReportCalendar {
    pub mode: String,
    pub source: String,
    pub snapshot: Option<TradingCalendarSnapshotMetadata>,
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
    #[serde(default)]
    pub selector: FillSelectorReport,
    #[serde(default)]
    pub resolved_range: Option<TradingDayWindow>,
    #[serde(default)]
    pub calendar: Option<FillReportCalendar>,
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
                day_stats: None,
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
            selector: FillSelectorReport::default(),
            resolved_range: None,
            calendar: None,
            logical_symbols: warmup.logical_symbols.clone(),
            physical_symbols,
            fill_config: fill_config.into(),
            remote_used: warmup.remote_used,
            rows_written: warmup.rows_written,
            complete,
            dry_run,
        }
    }

    #[must_use]
    pub fn with_v2_metadata(
        mut self,
        selector: FillSelectorReport,
        calendar: Option<FillReportCalendar>,
        day_stats: impl IntoIterator<Item = FillReportSymbolDayStats>,
    ) -> Self {
        self.selector = selector;
        self.resolved_range = Some(self.requested_days.clone());
        self.calendar = calendar;
        let mut remaining_day_stats = day_stats.into_iter().collect::<Vec<_>>();
        for symbol in &mut self.physical_symbols {
            if let Some(index) = remaining_day_stats
                .iter()
                .position(|stats| stats.symbol == symbol.symbol)
            {
                symbol.day_stats = Some(remaining_day_stats.remove(index));
            }
        }
        self
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
    write_json_atomically(path, report)
}

pub fn read_fill_report(path: &Path) -> Result<FillReport, DataError> {
    let file = File::open(path)?;
    let report: FillReport = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    if !matches!(report.schema_version, 1 | REPORT_SCHEMA_VERSION) {
        return Err(DataError::Validation(format!(
            "unsupported fill report schema {}; expected 1 or {REPORT_SCHEMA_VERSION}",
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

fn parse_calendar_date(value: &str) -> Result<NaiveDate, DataError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        DataError::Validation(format!("invalid trading calendar date {value:?}: {error}"))
    })
}

fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<(), DataError> {
    let parent = path.parent().ok_or_else(|| {
        DataError::Validation("JSON output path must have a parent directory".to_string())
    })?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            DataError::Validation("JSON output path must have a UTF-8 file name".to_string())
        })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temporary = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    let write_result = (|| -> Result<(), DataError> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
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
    use std::fs;

    use chrono::{FixedOffset, TimeZone};

    use super::{
        CacheCoverageSnapshot, FillConfigReport, FillReport, FillReportSymbol,
        FillReportSymbolDayStats, FillSelectorReport, REPORT_SCHEMA_VERSION,
        TradingCalendarSnapshot, TradingDayWindow, read_fill_report,
        read_trading_calendar_snapshot, write_trading_calendar_snapshot,
    };
    use tqsdk_data::TradingCalendarRow;

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

    #[test]
    fn calendar_snapshot_tracks_holidays_without_changing_tqbn_day_boundaries() {
        let snapshot = TradingCalendarSnapshot::from_rows(vec![
            TradingCalendarRow {
                date: "2026-07-17".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2026-07-18".to_string(),
                trading: false,
            },
            TradingCalendarRow {
                date: "2026-07-19".to_string(),
                trading: false,
            },
            TradingCalendarRow {
                date: "2026-07-20".to_string(),
                trading: true,
            },
        ])
        .unwrap();
        let start = chrono::NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        let end = chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap();

        assert!(snapshot.covers(start, end).unwrap());
        assert_eq!(
            snapshot.trading_days_between(start, end).unwrap(),
            vec![start, end]
        );
        assert_eq!(snapshot.hash(), snapshot.hash());
    }

    #[test]
    fn calendar_snapshot_round_trips_through_its_atomic_cache_path() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let snapshot = TradingCalendarSnapshot::from_rows(vec![
            TradingCalendarRow {
                date: "2026-07-17".to_string(),
                trading: true,
            },
            TradingCalendarRow {
                date: "2026-07-18".to_string(),
                trading: false,
            },
        ])
        .unwrap();

        let path = write_trading_calendar_snapshot(&root, &snapshot).unwrap();
        let restored = read_trading_calendar_snapshot(&root).unwrap().unwrap();

        assert_eq!(path.file_name().unwrap(), "trading-calendar-v1.json");
        assert_eq!(restored, snapshot);
        assert!(fs::read_dir(path.parent().unwrap()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn report_reader_accepts_v1_without_v2_metadata() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-v1-report-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("report.json");
        fs::write(
            &path,
            r#"{
  "schema_version": 1,
  "generated_at": "2026-07-21T00:00:00Z",
  "cache_dir": "/tmp/cache",
  "requested_days": {"start_day":"2026-07-17","end_day":"2026-07-20","start_ns":1,"end_ns":2},
  "requested_range": [1,2],
  "logical_symbols": ["SHFE.au2608"],
  "physical_symbols": [{
    "symbol":"SHFE.au2608",
    "action":"filled_remote",
    "before":{"series_path":"/tmp/a","series_path_exists":false,"cached_ranges":[],"missing_ranges":[[1,2]],"complete":false},
    "after":{"series_path":"/tmp/a","series_path_exists":true,"cached_ranges":[[1,2]],"missing_ranges":[],"complete":true},
    "rows_written": 10
  }],
  "fill_config":{"symbol_batch_size":1,"symbol_concurrency":1,"idle_timeout_secs":60,"batch_timeout_secs":null,"slice_secs":null,"allow_empty_idle":false},
  "remote_used":true,
  "rows_written":10,
  "complete":true,
  "dry_run":false
}"#,
        )
        .unwrap();

        let report = read_fill_report(&path).unwrap();
        assert_eq!(report.schema_version, 1);
        assert!(report.selector.symbols.is_empty());
        assert!(report.resolved_range.is_none());
        assert!(report.calendar.is_none());
        assert!(report.physical_symbols[0].day_stats.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn report_metadata_preserves_day_stats_for_duplicate_physical_symbols() {
        let requested_days = TradingDayWindow::from_days(
            chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 21).unwrap(),
        )
        .unwrap();
        let coverage = || CacheCoverageSnapshot {
            series_path: "/tmp/SHFE.au2608.tqbn".to_string(),
            series_path_exists: true,
            cached_ranges: vec![],
            missing_ranges: vec![],
            complete: true,
        };
        let report = FillReport {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at: "2026-07-21T00:00:00Z".to_string(),
            cache_dir: "/tmp/cache".to_string(),
            requested_range: (requested_days.start_ns, requested_days.end_ns),
            requested_days,
            selector: FillSelectorReport::default(),
            resolved_range: None,
            calendar: None,
            logical_symbols: vec![],
            physical_symbols: vec![
                FillReportSymbol {
                    symbol: "SHFE.au2608".to_string(),
                    action: "filled_remote".to_string(),
                    before: coverage(),
                    after: coverage(),
                    rows_written: 0,
                    day_stats: None,
                },
                FillReportSymbol {
                    symbol: "SHFE.au2608".to_string(),
                    action: "filled_remote".to_string(),
                    before: coverage(),
                    after: coverage(),
                    rows_written: 0,
                    day_stats: None,
                },
            ],
            fill_config: FillConfigReport {
                symbol_batch_size: 1,
                symbol_concurrency: 1,
                idle_timeout_secs: 60,
                batch_timeout_secs: None,
                slice_secs: None,
                allow_empty_idle: false,
            },
            remote_used: true,
            rows_written: 0,
            complete: true,
            dry_run: false,
        }
        .with_v2_metadata(
            FillSelectorReport::default(),
            None,
            [
                FillReportSymbolDayStats {
                    symbol: "SHFE.au2608".to_string(),
                    planned_days: 1,
                    covered_days: 1,
                    missing_days: 0,
                    received_days: 1,
                },
                FillReportSymbolDayStats {
                    symbol: "SHFE.au2608".to_string(),
                    planned_days: 2,
                    covered_days: 2,
                    missing_days: 0,
                    received_days: 2,
                },
            ],
        );

        assert_eq!(
            report.physical_symbols[0]
                .day_stats
                .as_ref()
                .unwrap()
                .planned_days,
            1
        );
        assert_eq!(
            report.physical_symbols[1]
                .day_stats
                .as_ref()
                .unwrap()
                .planned_days,
            2
        );
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
