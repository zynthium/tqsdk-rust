//! Shared contracts for the `tqsdk-cache` command-line tool.
//!
//! The crate owns shared CLI contracts for Tick, canonical-minute, and native-daily caches.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha1::{Digest, Sha1};
use tqsdk::{
    BacktestCacheWarmupAction, BacktestCacheWarmupReport,
    BacktestMinuteKlineCacheWarmupSymbolReport, BacktestRemoteFillConfig, BacktestTickCacheStatus,
};
use tqsdk_data::{
    BacktestHistoryFillSymbolStatus, BacktestHistoryFillTerminalReport,
    BacktestHistoryFillTerminalStatus, BacktestTickCache, DailyKlineCacheStatus, DataError,
    MinuteKlineCacheStatus, TradingCalendarHolidays, TradingCalendarRow,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
    default_history_cache_dir,
};

pub const REPORT_SCHEMA_VERSION: u32 = 2;
pub const MINUTE_FILL_REPORT_SCHEMA_VERSION: u32 = 1;
pub const DAILY_FILL_REPORT_SCHEMA_VERSION: u32 = 1;
pub const UNIFIED_FILL_REPORT_SCHEMA_VERSION: u32 = 4;
pub const TRADING_CALENDAR_SCHEMA_VERSION: u32 = 1;
pub const TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION: u32 = 1;

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

    /// Build a window that may end at the currently open trading day.
    ///
    /// Future trading days remain invalid. Callers must still use provisional
    /// coverage for the open-day portion.
    pub fn through_open_day_from_days(
        start_day: NaiveDate,
        end_day: NaiveDate,
    ) -> Result<Self, DataError> {
        let window = Self::from_days(start_day, end_day)?;
        let current_open_day = current_open_trading_day()?;
        let normalized_end =
            NaiveDate::parse_from_str(&window.end_day, "%Y-%m-%d").map_err(|error| {
                DataError::Validation(format!("invalid normalized TQBN trading day: {error}"))
            })?;
        if normalized_end > current_open_day {
            return Err(DataError::Validation(format!(
                "requested end trading day {} is in the future; current open trading day is {}",
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

/// Immutable compact snapshot of the raw holiday data used for calendar
/// planning.  It deliberately stores holidays instead of a finite daily
/// expansion, so any range inside its supported years can be derived locally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarHolidaysSnapshot {
    pub schema_version: u32,
    pub source_url: String,
    pub fetched_at: String,
    pub content_hash: String,
    pub supported_year_start: i32,
    pub supported_year_end: i32,
    pub holidays: Vec<String>,
}

impl TradingCalendarHolidaysSnapshot {
    /// Create an immutable snapshot from a normalized raw `tqsdk-data` value.
    pub fn from_holidays(holidays: TradingCalendarHolidays) -> Result<Self, DataError> {
        holidays.validate()?;
        let source_url = holidays.source_url;
        let holiday_dates = holidays.holidays;
        let supported_year_start = holiday_dates
            .first()
            .expect("validated holiday set must not be empty")
            .year();
        let supported_year_end = holiday_dates
            .last()
            .expect("validated holiday set must not be empty")
            .year();
        let holidays = holiday_dates
            .into_iter()
            .map(|date| date.format("%Y-%m-%d").to_string())
            .collect::<Vec<_>>();
        let snapshot = Self {
            schema_version: TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION,
            source_url,
            fetched_at: Utc::now().to_rfc3339(),
            content_hash: calendar_holidays_content_hash(&holidays),
            supported_year_start,
            supported_year_end,
            holidays,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Return whether this raw snapshot supports deriving the full date range.
    pub fn covers(&self, start_day: NaiveDate, end_day: NaiveDate) -> Result<bool, DataError> {
        self.validate()?;
        Ok(start_day <= end_day
            && self.supported_year_start <= start_day.year()
            && end_day.year() <= self.supported_year_end)
    }

    /// Derive all weekday, non-holiday trading days in an inclusive range.
    pub fn trading_days_between(
        &self,
        start_day: NaiveDate,
        end_day: NaiveDate,
    ) -> Result<Vec<NaiveDate>, DataError> {
        if !self.covers(start_day, end_day)? {
            return Err(DataError::Validation(format!(
                "trading calendar holidays support years {} to {}, not {} to {}",
                self.supported_year_start,
                self.supported_year_end,
                start_day.format("%Y-%m-%d"),
                end_day.format("%Y-%m-%d")
            )));
        }
        let holidays = self
            .holidays
            .iter()
            .map(|date| parse_calendar_date(date))
            .collect::<Result<Vec<_>, _>>()?;
        let mut days = Vec::new();
        let mut day = start_day;
        while day <= end_day {
            if day.weekday().number_from_monday() <= 5 && holidays.binary_search(&day).is_err() {
                days.push(day);
            }
            day = day.succ_opt().ok_or_else(|| {
                DataError::Validation("trading calendar date overflow".to_string())
            })?;
        }
        Ok(days)
    }

    pub fn validate(&self) -> Result<(), DataError> {
        if self.schema_version != TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION {
            return Err(DataError::Validation(format!(
                "unsupported trading calendar holidays schema {}; expected {TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.source_url.trim().is_empty() {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot source URL must not be empty".to_string(),
            ));
        }
        if self.fetched_at.trim().is_empty() {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot fetched_at must not be empty".to_string(),
            ));
        }
        if DateTime::parse_from_rfc3339(&self.fetched_at).is_err() {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot fetched_at must be RFC 3339".to_string(),
            ));
        }
        if self.holidays.is_empty() {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot contains no holidays".to_string(),
            ));
        }
        let dates = self
            .holidays
            .iter()
            .map(|date| parse_calendar_date(date))
            .collect::<Result<Vec<_>, _>>()?;
        if dates.windows(2).any(|dates| dates[0] >= dates[1]) {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot dates must be strictly ordered".to_string(),
            ));
        }
        let first_year = dates
            .first()
            .expect("non-empty validated holiday snapshot")
            .year();
        let last_year = dates
            .last()
            .expect("non-empty validated holiday snapshot")
            .year();
        if (self.supported_year_start, self.supported_year_end) != (first_year, last_year) {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot supported years do not match its dates"
                    .to_string(),
            ));
        }
        if self.content_hash != calendar_holidays_content_hash(&self.holidays) {
            return Err(DataError::Validation(
                "trading calendar holiday snapshot content hash does not match its dates"
                    .to_string(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn metadata(&self) -> TradingCalendarHolidaysSnapshotMetadata {
        TradingCalendarHolidaysSnapshotMetadata {
            schema_version: self.schema_version,
            source_url: self.source_url.clone(),
            fetched_at: self.fetched_at.clone(),
            content_hash: self.content_hash.clone(),
            supported_year_start: self.supported_year_start,
            supported_year_end: self.supported_year_end,
            holidays: self.holidays.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarHolidaysSnapshotMetadata {
    pub schema_version: u32,
    #[serde(default)]
    pub source_url: String,
    #[serde(default)]
    pub fetched_at: String,
    #[serde(default)]
    pub content_hash: String,
    #[serde(default)]
    pub supported_year_start: i32,
    #[serde(default)]
    pub supported_year_end: i32,
    #[serde(default)]
    pub holidays: usize,
}

/// Active pointer for the immutable raw-holiday snapshot set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCalendarHolidaysActivePointer {
    pub schema_version: u32,
    pub content_hash: String,
    pub activated_at: String,
}

impl TradingCalendarHolidaysActivePointer {
    fn validate(&self) -> Result<(), DataError> {
        if self.schema_version != TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION {
            return Err(DataError::Validation(format!(
                "unsupported trading calendar holiday pointer schema {}; expected {TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if !is_sha1_hex(&self.content_hash) {
            return Err(DataError::Validation(
                "trading calendar holiday pointer has an invalid content hash".to_string(),
            ));
        }
        if self.activated_at.trim().is_empty() {
            return Err(DataError::Validation(
                "trading calendar holiday pointer activated_at must not be empty".to_string(),
            ));
        }
        if DateTime::parse_from_rfc3339(&self.activated_at).is_err() {
            return Err(DataError::Validation(
                "trading calendar holiday pointer activated_at must be RFC 3339".to_string(),
            ));
        }
        Ok(())
    }
}

/// Directory containing the active pointer and immutable raw-holiday snapshots.
pub fn trading_calendar_holidays_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("meta").join("trading-calendar-holidays-v1")
}

pub fn trading_calendar_holidays_active_path(cache_dir: &Path) -> PathBuf {
    trading_calendar_holidays_dir(cache_dir).join("active.json")
}

pub fn trading_calendar_holidays_snapshot_path(cache_dir: &Path, content_hash: &str) -> PathBuf {
    trading_calendar_holidays_dir(cache_dir)
        .join("snapshots")
        .join(format!("{content_hash}.json"))
}

/// Read the active immutable raw-holiday snapshot, if one has been activated.
pub fn read_trading_calendar_holidays_snapshot(
    cache_dir: &Path,
) -> Result<Option<TradingCalendarHolidaysSnapshot>, DataError> {
    let active_path = trading_calendar_holidays_active_path(cache_dir);
    let file = match File::open(active_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let pointer: TradingCalendarHolidaysActivePointer =
        serde_json::from_reader(BufReader::new(file))
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    pointer.validate()?;
    let snapshot_path = trading_calendar_holidays_snapshot_path(cache_dir, &pointer.content_hash);
    let file = File::open(&snapshot_path).map_err(|error| {
        DataError::InvalidResponse(format!(
            "trading calendar holiday pointer references unavailable snapshot {}: {error}",
            snapshot_path.display()
        ))
    })?;
    let snapshot: TradingCalendarHolidaysSnapshot =
        serde_json::from_reader(BufReader::new(file))
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    snapshot.validate()?;
    if snapshot.content_hash != pointer.content_hash {
        return Err(DataError::Validation(
            "trading calendar holiday pointer does not match its snapshot".to_string(),
        ));
    }
    Ok(Some(snapshot))
}

/// Persist an immutable raw-holiday snapshot and atomically move the active pointer.
///
/// Existing content-addressed snapshots are never replaced.  Re-activating an
/// unchanged payload only updates `active.json`.
pub fn write_trading_calendar_holidays_snapshot(
    cache_dir: &Path,
    snapshot: &TradingCalendarHolidaysSnapshot,
) -> Result<PathBuf, DataError> {
    snapshot.validate()?;
    let snapshot_path = trading_calendar_holidays_snapshot_path(cache_dir, &snapshot.content_hash);
    write_json_immutably(&snapshot_path, snapshot)?;
    let existing_file = File::open(&snapshot_path)?;
    let existing: TradingCalendarHolidaysSnapshot =
        serde_json::from_reader(BufReader::new(existing_file))
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    existing.validate()?;
    if existing.content_hash != snapshot.content_hash
        || existing.source_url != snapshot.source_url
        || existing.holidays != snapshot.holidays
    {
        return Err(DataError::Validation(
            "existing trading calendar holiday snapshot conflicts with its content hash"
                .to_string(),
        ));
    }
    let pointer = TradingCalendarHolidaysActivePointer {
        schema_version: TRADING_CALENDAR_HOLIDAYS_SCHEMA_VERSION,
        content_hash: snapshot.content_hash.clone(),
        activated_at: Utc::now().to_rfc3339(),
    };
    pointer.validate()?;
    write_json_atomically(&trading_calendar_holidays_active_path(cache_dir), &pointer)?;
    Ok(snapshot_path)
}

fn calendar_holidays_content_hash(holidays: &[String]) -> String {
    let mut hasher = Sha1::new();
    for holiday in holidays {
        hasher.update(holiday.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

fn is_sha1_hex(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
    /// Whether the selected raw calendar snapshot is durable in this cache root.
    #[serde(default)]
    pub persisted: bool,
    pub snapshot: Option<TradingCalendarHolidaysSnapshotMetadata>,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    /// `final` for ordinary closed-day fills, `provisional` for an open-day snapshot.
    #[serde(default = "default_coverage_state")]
    pub coverage_state: String,
    /// Shared high-water mark across all physical symbols for a provisional fill.
    #[serde(default)]
    pub complete_through_ns: Option<i64>,
    /// Whether the requested trading-day window has final immutable coverage.
    pub day_complete: bool,
}

#[derive(Deserialize)]
struct FillReportWire {
    schema_version: u32,
    generated_at: String,
    cache_dir: String,
    requested_days: TradingDayWindow,
    requested_range: (i64, i64),
    #[serde(default)]
    selector: FillSelectorReport,
    #[serde(default)]
    resolved_range: Option<TradingDayWindow>,
    #[serde(default)]
    calendar: Option<FillReportCalendar>,
    logical_symbols: Vec<String>,
    physical_symbols: Vec<FillReportSymbol>,
    fill_config: FillConfigReport,
    remote_used: bool,
    rows_written: usize,
    complete: bool,
    dry_run: bool,
    #[serde(default = "default_coverage_state")]
    coverage_state: String,
    #[serde(default)]
    complete_through_ns: Option<i64>,
    #[serde(default)]
    day_complete: Option<bool>,
}

impl<'de> Deserialize<'de> for FillReport {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FillReportWire::deserialize(deserializer)?;
        let day_complete = wire.day_complete.unwrap_or(wire.complete);
        Ok(Self {
            schema_version: wire.schema_version,
            generated_at: wire.generated_at,
            cache_dir: wire.cache_dir,
            requested_days: wire.requested_days,
            requested_range: wire.requested_range,
            selector: wire.selector,
            resolved_range: wire.resolved_range,
            calendar: wire.calendar,
            logical_symbols: wire.logical_symbols,
            physical_symbols: wire.physical_symbols,
            fill_config: wire.fill_config,
            remote_used: wire.remote_used,
            rows_written: wire.rows_written,
            complete: wire.complete,
            dry_run: wire.dry_run,
            coverage_state: wire.coverage_state,
            complete_through_ns: wire.complete_through_ns,
            day_complete,
        })
    }
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
            coverage_state: default_coverage_state(),
            complete_through_ns: None,
            day_complete: complete,
        }
    }

    #[must_use]
    pub fn with_provisional_state(mut self, complete_through_ns: Option<i64>) -> Self {
        self.coverage_state = "provisional".to_string();
        self.complete_through_ns = complete_through_ns;
        self.day_complete = false;
        self
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

/// Cache-only coverage snapshot for a logical canonical-minute symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinuteCacheCoverageSnapshot {
    pub namespace_dir: String,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
    pub complete: bool,
}

impl From<&MinuteKlineCacheStatus> for MinuteCacheCoverageSnapshot {
    fn from(value: &MinuteKlineCacheStatus) -> Self {
        Self {
            namespace_dir: value.namespace_dir.display().to_string(),
            cached_ranges: value.cached_ranges.clone(),
            missing_ranges: value.missing_ranges.clone(),
            complete: value.is_complete(),
        }
    }
}

/// One logical canonical-minute symbol recorded in a minute fill report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinuteFillReportSymbol {
    pub symbol: String,
    pub action: String,
    pub before: MinuteCacheCoverageSnapshot,
    pub after: MinuteCacheCoverageSnapshot,
    pub rows_written: usize,
}

/// Stable credential-free report for a canonical 60-second Kline fill.
///
/// Unlike the legacy tick report, all symbols here are logical cache keys. A
/// `KQ.m@...` main contract is never expanded into duplicate physical minute
/// files in this report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinuteFillReport {
    pub schema_version: u32,
    pub cache_kind: String,
    pub generated_at: String,
    pub cache_dir: String,
    pub requested_days: TradingDayWindow,
    pub requested_range: (i64, i64),
    #[serde(default)]
    pub selector: FillSelectorReport,
    #[serde(default)]
    pub calendar: Option<FillReportCalendar>,
    pub market: String,
    pub logical_symbols: Vec<String>,
    pub symbols: Vec<MinuteFillReportSymbol>,
    pub remote_used: bool,
    pub rows_written: usize,
    /// Final-only coverage result. Provisional sidecars never make this true.
    pub complete: bool,
    #[serde(default)]
    pub provisional_as_of_ns: Option<i64>,
    #[serde(default)]
    pub provisional_complete_through_ns: Option<i64>,
    #[serde(default)]
    pub provisional_complete: bool,
    pub dry_run: bool,
}

impl MinuteFillReport {
    pub fn from_warmup(
        warmup: &BacktestCacheWarmupReport,
        cache_dir: &Path,
        requested_days: TradingDayWindow,
        market: impl Into<String>,
        dry_run: bool,
    ) -> Self {
        let symbols = warmup
            .minute_kline_symbols
            .iter()
            .map(minute_fill_report_symbol)
            .collect::<Vec<_>>();
        let complete = !symbols.is_empty() && symbols.iter().all(|symbol| symbol.after.complete);
        Self {
            schema_version: MINUTE_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: "minute".to_string(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: cache_dir.display().to_string(),
            requested_range: warmup.requested_range,
            requested_days,
            selector: FillSelectorReport::default(),
            calendar: None,
            market: market.into(),
            logical_symbols: warmup.logical_symbols.clone(),
            symbols,
            remote_used: warmup.remote_minute_kline_used,
            rows_written: warmup.minute_kline_rows_written,
            complete,
            provisional_as_of_ns: None,
            provisional_complete_through_ns: None,
            provisional_complete: false,
            dry_run,
        }
    }

    #[must_use]
    pub fn with_selector(mut self, selector: FillSelectorReport) -> Self {
        self.selector = selector;
        self
    }

    #[must_use]
    pub fn with_calendar(mut self, calendar: FillReportCalendar) -> Self {
        self.calendar = Some(calendar);
        self
    }

    #[must_use]
    pub fn with_provisional(
        mut self,
        as_of_ns: i64,
        complete_through_ns: Option<i64>,
        complete: bool,
    ) -> Self {
        self.provisional_as_of_ns = Some(as_of_ns);
        self.provisional_complete_through_ns = complete_through_ns;
        self.provisional_complete = complete;
        self
    }

    pub fn symbols(&self) -> Result<Vec<String>, DataError> {
        let mut symbols = self
            .symbols
            .iter()
            .map(|symbol| symbol.symbol.trim())
            .filter(|symbol| !symbol.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(DataError::Validation(
                "minute fill report contains no logical cache symbols".to_string(),
            ));
        }
        Ok(symbols)
    }
}

/// Cache-only coverage snapshot for one logical native-daily symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyCacheCoverageSnapshot {
    pub namespace_dir: String,
    pub path: String,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
    pub rows: usize,
    pub complete: bool,
}

impl From<&DailyKlineCacheStatus> for DailyCacheCoverageSnapshot {
    fn from(value: &DailyKlineCacheStatus) -> Self {
        Self {
            namespace_dir: value.namespace_dir.display().to_string(),
            path: value.path.display().to_string(),
            cached_ranges: value.cached_ranges.clone(),
            missing_ranges: value.missing_ranges.clone(),
            rows: value.rows,
            complete: value.is_complete(),
        }
    }
}

/// One logical native-daily symbol recorded in a daily fill report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyFillReportSymbol {
    pub symbol: String,
    pub after: DailyCacheCoverageSnapshot,
}

/// Stable credential-free report for a native daily Kline fill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyFillReport {
    pub schema_version: u32,
    pub cache_kind: String,
    pub generated_at: String,
    pub cache_dir: String,
    pub requested_days: TradingDayWindow,
    pub market: String,
    pub symbols: Vec<DailyFillReportSymbol>,
    pub remote_used: bool,
    pub rows_written: usize,
    pub complete: bool,
    pub dry_run: bool,
}

impl DailyFillReport {
    #[must_use]
    pub fn new(
        cache_dir: &Path,
        requested_days: TradingDayWindow,
        market: impl Into<String>,
        statuses: &[DailyKlineCacheStatus],
        rows_written: usize,
        remote_used: bool,
        dry_run: bool,
    ) -> Self {
        let symbols = statuses
            .iter()
            .map(|status| DailyFillReportSymbol {
                symbol: status.symbol.clone(),
                after: DailyCacheCoverageSnapshot::from(status),
            })
            .collect::<Vec<_>>();
        let complete = !symbols.is_empty() && symbols.iter().all(|symbol| symbol.after.complete);
        Self {
            schema_version: DAILY_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: "daily".to_string(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: cache_dir.display().to_string(),
            requested_days,
            market: market.into(),
            symbols,
            remote_used,
            rows_written,
            complete,
            dry_run,
        }
    }

    pub fn symbols(&self) -> Result<Vec<String>, DataError> {
        let mut symbols = self
            .symbols
            .iter()
            .map(|symbol| symbol.symbol.trim())
            .filter(|symbol| !symbol.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(DataError::Validation(
                "daily fill report contains no logical cache symbols".to_string(),
            ));
        }
        Ok(symbols)
    }
}

/// Normalized terminal state persisted by all new cache-fill reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnifiedFillReportStatus {
    Complete,
    Failed,
    Interrupted,
}

/// Durable coverage finality represented by a unified fill report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnifiedFillCoverageState {
    Final,
    Provisional,
    #[default]
    Incomplete,
}

/// One logical cache symbol in a schema-v4 fill report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedFillReportSymbol {
    pub symbol: String,
    pub status: UnifiedFillReportStatus,
    pub requested_ranges: Vec<(i64, i64)>,
    pub rows_written: usize,
    pub interrupted: bool,
    pub error: Option<String>,
}

/// Family-neutral, credential-free fill report written after planning begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedFillReport {
    pub schema_version: u32,
    pub cache_kind: String,
    pub generated_at: String,
    pub cache_dir: String,
    pub requested_days: TradingDayWindow,
    pub requested_range: (i64, i64),
    pub market: String,
    pub status: UnifiedFillReportStatus,
    pub interrupted: bool,
    pub error: Option<String>,
    pub symbols: Vec<UnifiedFillReportSymbol>,
    pub remote_used: bool,
    pub rows_written: usize,
    /// True only when ordinary durable final coverage is complete.
    pub complete: bool,
    #[serde(default)]
    pub final_complete: bool,
    #[serde(default)]
    pub coverage_state: UnifiedFillCoverageState,
    #[serde(default)]
    pub provisional_as_of_ns: Option<i64>,
    #[serde(default)]
    pub complete_through_ns: Option<i64>,
}

impl UnifiedFillReport {
    #[must_use]
    pub fn from_tick_fill(report: &FillReport) -> Self {
        let provisional_complete = report.coverage_state == "provisional"
            && report
                .complete_through_ns
                .is_some_and(|through| through >= report.requested_range.1);
        let operation_complete = report.complete || provisional_complete;
        let status = if operation_complete {
            UnifiedFillReportStatus::Complete
        } else {
            UnifiedFillReportStatus::Failed
        };
        let symbols = report
            .physical_symbols
            .iter()
            .map(|symbol| UnifiedFillReportSymbol {
                symbol: symbol.symbol.clone(),
                status: if symbol.after.complete || provisional_complete {
                    UnifiedFillReportStatus::Complete
                } else {
                    UnifiedFillReportStatus::Failed
                },
                requested_ranges: vec![report.requested_range],
                rows_written: symbol.rows_written,
                interrupted: false,
                error: None,
            })
            .collect();
        Self {
            schema_version: UNIFIED_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: "tick".to_string(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: report.cache_dir.clone(),
            requested_days: report.requested_days.clone(),
            requested_range: report.requested_range,
            market: "futures".to_string(),
            status,
            interrupted: false,
            error: None,
            symbols,
            remote_used: report.remote_used,
            rows_written: report.rows_written,
            complete: report.day_complete,
            final_complete: report.day_complete,
            coverage_state: if report.day_complete {
                UnifiedFillCoverageState::Final
            } else if provisional_complete {
                UnifiedFillCoverageState::Provisional
            } else {
                UnifiedFillCoverageState::Incomplete
            },
            provisional_as_of_ns: provisional_complete.then_some(report.requested_range.1),
            complete_through_ns: report.complete_through_ns,
        }
    }

    #[must_use]
    pub fn from_minute_fill(report: &MinuteFillReport) -> Self {
        let operation_complete = report.complete || report.provisional_complete;
        let status = if operation_complete {
            UnifiedFillReportStatus::Complete
        } else {
            UnifiedFillReportStatus::Failed
        };
        let symbols = report
            .symbols
            .iter()
            .map(|symbol| UnifiedFillReportSymbol {
                symbol: symbol.symbol.clone(),
                status: if symbol.after.complete || report.provisional_complete {
                    UnifiedFillReportStatus::Complete
                } else {
                    UnifiedFillReportStatus::Failed
                },
                requested_ranges: vec![report.requested_range],
                rows_written: symbol.rows_written,
                interrupted: false,
                error: None,
            })
            .collect();
        Self {
            schema_version: UNIFIED_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: "minute".to_string(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: report.cache_dir.clone(),
            requested_days: report.requested_days.clone(),
            requested_range: report.requested_range,
            market: report.market.clone(),
            status,
            interrupted: false,
            error: None,
            symbols,
            remote_used: report.remote_used,
            rows_written: report.rows_written,
            complete: report.complete,
            final_complete: report.complete,
            coverage_state: if report.complete {
                UnifiedFillCoverageState::Final
            } else if report.provisional_complete {
                UnifiedFillCoverageState::Provisional
            } else {
                UnifiedFillCoverageState::Incomplete
            },
            provisional_as_of_ns: report.provisional_as_of_ns,
            complete_through_ns: report.provisional_complete_through_ns,
        }
    }

    #[must_use]
    pub fn from_planned_terminal(
        cache_kind: impl Into<String>,
        cache_dir: &Path,
        requested_days: TradingDayWindow,
        market: impl Into<String>,
        symbols: &[String],
        status: UnifiedFillReportStatus,
        error: Option<String>,
    ) -> Self {
        let requested_range = (requested_days.start_ns, requested_days.end_ns);
        let interrupted = matches!(status, UnifiedFillReportStatus::Interrupted);
        Self {
            schema_version: UNIFIED_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: cache_kind.into(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: cache_dir.display().to_string(),
            requested_days,
            requested_range,
            market: market.into(),
            status,
            interrupted,
            error: error.clone(),
            symbols: symbols
                .iter()
                .map(|symbol| UnifiedFillReportSymbol {
                    symbol: symbol.clone(),
                    status,
                    requested_ranges: vec![requested_range],
                    rows_written: 0,
                    interrupted,
                    error: error.clone(),
                })
                .collect(),
            remote_used: false,
            rows_written: 0,
            complete: matches!(status, UnifiedFillReportStatus::Complete),
            final_complete: matches!(status, UnifiedFillReportStatus::Complete),
            coverage_state: if matches!(status, UnifiedFillReportStatus::Complete) {
                UnifiedFillCoverageState::Final
            } else {
                UnifiedFillCoverageState::Incomplete
            },
            provisional_as_of_ns: None,
            complete_through_ns: None,
        }
    }

    #[must_use]
    pub fn from_history_fill(
        cache_kind: impl Into<String>,
        cache_dir: &Path,
        requested_days: TradingDayWindow,
        market: impl Into<String>,
        report: &BacktestHistoryFillTerminalReport,
    ) -> Self {
        let mut symbols = BTreeMap::<String, UnifiedFillReportSymbol>::new();
        for result in report.symbols() {
            let status = unified_symbol_status(result.status);
            let entry =
                symbols
                    .entry(result.symbol.clone())
                    .or_insert_with(|| UnifiedFillReportSymbol {
                        symbol: result.symbol.clone(),
                        status,
                        requested_ranges: Vec::new(),
                        rows_written: 0,
                        interrupted: false,
                        error: None,
                    });
            entry.status = merge_unified_status(entry.status, status);
            if !entry.requested_ranges.contains(&result.requested_range) {
                entry.requested_ranges.push(result.requested_range);
            }
            entry.rows_written = entry.rows_written.saturating_add(result.rows_written);
            entry.interrupted |= matches!(status, UnifiedFillReportStatus::Interrupted);
            if entry.error.is_none() {
                entry.error.clone_from(&result.error);
            }
        }
        let symbols = symbols.into_values().collect::<Vec<_>>();
        let status = unified_terminal_status(report.status());
        let error = symbols.iter().find_map(|symbol| symbol.error.clone());
        Self {
            schema_version: UNIFIED_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: cache_kind.into(),
            generated_at: Utc::now().to_rfc3339(),
            cache_dir: cache_dir.display().to_string(),
            requested_range: (requested_days.start_ns, requested_days.end_ns),
            requested_days,
            market: market.into(),
            status,
            interrupted: matches!(status, UnifiedFillReportStatus::Interrupted),
            error,
            symbols,
            remote_used: report.symbols().iter().any(|symbol| symbol.remote_used),
            rows_written: report.rows_written(),
            complete: matches!(status, UnifiedFillReportStatus::Complete),
            final_complete: matches!(status, UnifiedFillReportStatus::Complete),
            coverage_state: if matches!(status, UnifiedFillReportStatus::Complete) {
                UnifiedFillCoverageState::Final
            } else {
                UnifiedFillCoverageState::Incomplete
            },
            provisional_as_of_ns: None,
            complete_through_ns: None,
        }
    }

    pub fn symbols(&self) -> Result<Vec<String>, DataError> {
        let mut symbols = self
            .symbols
            .iter()
            .map(|symbol| symbol.symbol.trim())
            .filter(|symbol| !symbol.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Err(DataError::Validation(
                "unified fill report contains no logical cache symbols".to_string(),
            ));
        }
        Ok(symbols)
    }
}

fn unified_symbol_status(status: BacktestHistoryFillSymbolStatus) -> UnifiedFillReportStatus {
    match status {
        BacktestHistoryFillSymbolStatus::Complete => UnifiedFillReportStatus::Complete,
        BacktestHistoryFillSymbolStatus::Failed => UnifiedFillReportStatus::Failed,
        BacktestHistoryFillSymbolStatus::Interrupted => UnifiedFillReportStatus::Interrupted,
    }
}

fn unified_terminal_status(status: BacktestHistoryFillTerminalStatus) -> UnifiedFillReportStatus {
    match status {
        BacktestHistoryFillTerminalStatus::Complete => UnifiedFillReportStatus::Complete,
        BacktestHistoryFillTerminalStatus::Failed => UnifiedFillReportStatus::Failed,
        BacktestHistoryFillTerminalStatus::Interrupted => UnifiedFillReportStatus::Interrupted,
    }
}

fn merge_unified_status(
    left: UnifiedFillReportStatus,
    right: UnifiedFillReportStatus,
) -> UnifiedFillReportStatus {
    match (left, right) {
        (UnifiedFillReportStatus::Failed, _) | (_, UnifiedFillReportStatus::Failed) => {
            UnifiedFillReportStatus::Failed
        }
        (UnifiedFillReportStatus::Interrupted, _) | (_, UnifiedFillReportStatus::Interrupted) => {
            UnifiedFillReportStatus::Interrupted
        }
        _ => UnifiedFillReportStatus::Complete,
    }
}

fn minute_fill_report_symbol(
    symbol: &BacktestMinuteKlineCacheWarmupSymbolReport,
) -> MinuteFillReportSymbol {
    MinuteFillReportSymbol {
        symbol: symbol.symbol.clone(),
        action: warmup_action_name(symbol.action).to_string(),
        before: MinuteCacheCoverageSnapshot::from(&symbol.before),
        after: MinuteCacheCoverageSnapshot::from(&symbol.after),
        rows_written: symbol.rows_written,
    }
}

/// Persisted fill-report variants understood by `verify --report`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistedFillReport {
    Tick(Box<FillReport>),
    Minute(Box<MinuteFillReport>),
    Daily(Box<DailyFillReport>),
    Unified(Box<UnifiedFillReport>),
}

fn default_coverage_state() -> String {
    "final".to_string()
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
    cache_dir.join("reports").join("tick").join(format!(
        "tqsdk-cache-fill-{timestamp}-{}.json",
        std::process::id()
    ))
}

/// Default report destination for a canonical-minute fill.
pub fn default_minute_fill_report_path(cache_dir: &Path) -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    cache_dir.join("reports").join("minute").join(format!(
        "tqsdk-cache-minute-fill-{timestamp}-{}.json",
        std::process::id()
    ))
}

/// Default report destination for a native-daily fill.
#[must_use]
pub fn default_daily_fill_report_path(cache_dir: &Path) -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    cache_dir.join("reports").join("daily").join(format!(
        "tqsdk-cache-daily-fill-{timestamp}-{}.json",
        std::process::id()
    ))
}

pub fn write_fill_report(path: &Path, report: &FillReport) -> Result<(), DataError> {
    write_json_atomically(path, report)
}

pub fn write_minute_fill_report(path: &Path, report: &MinuteFillReport) -> Result<(), DataError> {
    if report.schema_version != MINUTE_FILL_REPORT_SCHEMA_VERSION || report.cache_kind != "minute" {
        return Err(DataError::Validation(
            "minute fill report has an unsupported schema or cache kind".to_string(),
        ));
    }
    write_json_atomically(path, report)
}

pub fn write_daily_fill_report(path: &Path, report: &DailyFillReport) -> Result<(), DataError> {
    if report.schema_version != DAILY_FILL_REPORT_SCHEMA_VERSION || report.cache_kind != "daily" {
        return Err(DataError::Validation(
            "daily fill report has an unsupported schema or cache kind".to_string(),
        ));
    }
    write_json_atomically(path, report)
}

pub fn write_unified_fill_report(path: &Path, report: &UnifiedFillReport) -> Result<(), DataError> {
    if report.schema_version != UNIFIED_FILL_REPORT_SCHEMA_VERSION
        || !matches!(report.cache_kind.as_str(), "tick" | "minute" | "daily")
    {
        return Err(DataError::Validation(
            "unified fill report has an unsupported schema or cache kind".to_string(),
        ));
    }
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

pub fn read_persisted_fill_report(path: &Path) -> Result<PersistedFillReport, DataError> {
    let file = File::open(path)?;
    let value: serde_json::Value = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if schema_version == Some(3)
        || schema_version == Some(u64::from(UNIFIED_FILL_REPORT_SCHEMA_VERSION))
    {
        let mut report: UnifiedFillReport = serde_json::from_value(value)
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
        if report.schema_version == 3 {
            report.final_complete = report.complete;
            report.coverage_state = if report.complete {
                UnifiedFillCoverageState::Final
            } else {
                UnifiedFillCoverageState::Incomplete
            };
        }
        if !matches!(report.cache_kind.as_str(), "tick" | "minute" | "daily") {
            return Err(DataError::Validation(format!(
                "unsupported unified fill report cache kind {:?}",
                report.cache_kind
            )));
        }
        return Ok(PersistedFillReport::Unified(Box::new(report)));
    }
    if value.get("cache_kind").and_then(serde_json::Value::as_str) == Some("minute") {
        let report: MinuteFillReport = serde_json::from_value(value)
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
        if report.schema_version != MINUTE_FILL_REPORT_SCHEMA_VERSION {
            return Err(DataError::Validation(format!(
                "unsupported minute fill report schema {}; expected {MINUTE_FILL_REPORT_SCHEMA_VERSION}",
                report.schema_version
            )));
        }
        return Ok(PersistedFillReport::Minute(Box::new(report)));
    }
    if value.get("cache_kind").and_then(serde_json::Value::as_str) == Some("daily") {
        let report: DailyFillReport = serde_json::from_value(value)
            .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
        if report.schema_version != DAILY_FILL_REPORT_SCHEMA_VERSION {
            return Err(DataError::Validation(format!(
                "unsupported daily fill report schema {}; expected {DAILY_FILL_REPORT_SCHEMA_VERSION}",
                report.schema_version
            )));
        }
        return Ok(PersistedFillReport::Daily(Box::new(report)));
    }
    let report: FillReport = serde_json::from_value(value)
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    if !matches!(report.schema_version, 1 | REPORT_SCHEMA_VERSION) {
        return Err(DataError::Validation(format!(
            "unsupported fill report schema {}; expected 1 or {REPORT_SCHEMA_VERSION}",
            report.schema_version
        )));
    }
    Ok(PersistedFillReport::Tick(Box::new(report)))
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

fn write_json_immutably<T: Serialize>(path: &Path, value: &T) -> Result<(), DataError> {
    let parent = path.parent().ok_or_else(|| {
        DataError::Validation("JSON output path must have a parent directory".to_string())
    })?;
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| DataError::InvalidResponse(error.to_string()))?;
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let write_result = (|| -> Result<(), DataError> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        drop(file);
        let _ = fs::remove_file(path);
    }
    write_result
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
        FillReportSymbolDayStats, FillSelectorReport, MinuteFillReport, PersistedFillReport,
        REPORT_SCHEMA_VERSION, TradingCalendarHolidaysSnapshot, TradingCalendarSnapshot,
        TradingDayWindow, UnifiedFillCoverageState, UnifiedFillReport, read_fill_report,
        read_persisted_fill_report, read_trading_calendar_holidays_snapshot,
        read_trading_calendar_snapshot, write_trading_calendar_holidays_snapshot,
        write_trading_calendar_snapshot,
    };
    use tqsdk_data::{TradingCalendarHolidays, TradingCalendarRow};

    #[test]
    fn minute_provisional_completion_is_preserved_in_the_unified_report() {
        let window = TradingDayWindow::from_days(
            chrono::NaiveDate::from_ymd_opt(2026, 7, 29).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 29).unwrap(),
        )
        .unwrap();
        let report = MinuteFillReport {
            schema_version: super::MINUTE_FILL_REPORT_SCHEMA_VERSION,
            cache_kind: "minute".to_string(),
            generated_at: "2026-07-29T00:00:00Z".to_string(),
            cache_dir: "/tmp/cache".to_string(),
            requested_days: window.clone(),
            requested_range: (window.start_ns, window.end_ns),
            selector: FillSelectorReport::default(),
            calendar: None,
            market: "futures".to_string(),
            logical_symbols: Vec::new(),
            symbols: Vec::new(),
            remote_used: true,
            rows_written: 10,
            complete: false,
            provisional_as_of_ns: None,
            provisional_complete_through_ns: None,
            provisional_complete: false,
            dry_run: false,
        }
        .with_provisional(window.start_ns + 120, Some(window.start_ns + 60), true);

        let unified = UnifiedFillReport::from_minute_fill(&report);
        assert!(!unified.complete);
        assert!(!unified.final_complete);
        assert_eq!(
            unified.coverage_state,
            UnifiedFillCoverageState::Provisional
        );
        assert_eq!(unified.provisional_as_of_ns, report.provisional_as_of_ns);
        assert!(!report.complete);
        assert!(report.provisional_complete);
    }

    #[test]
    fn tick_provisional_completion_does_not_claim_final_coverage() {
        let window = TradingDayWindow::from_days(
            chrono::NaiveDate::from_ymd_opt(2026, 7, 29).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 29).unwrap(),
        )
        .unwrap();
        let report = FillReport {
            schema_version: REPORT_SCHEMA_VERSION,
            generated_at: "2026-07-29T00:00:00Z".to_string(),
            cache_dir: "/tmp/cache".to_string(),
            requested_days: window.clone(),
            requested_range: (window.start_ns, window.end_ns),
            selector: FillSelectorReport::default(),
            resolved_range: None,
            calendar: None,
            logical_symbols: Vec::new(),
            physical_symbols: Vec::new(),
            fill_config: FillConfigReport {
                symbol_batch_size: 1,
                symbol_concurrency: 1,
                idle_timeout_secs: 60,
                batch_timeout_secs: None,
                slice_secs: None,
                allow_empty_idle: false,
            },
            remote_used: true,
            rows_written: 10,
            complete: false,
            dry_run: false,
            coverage_state: "provisional".to_string(),
            complete_through_ns: Some(window.end_ns),
            day_complete: false,
        };

        let unified = UnifiedFillReport::from_tick_fill(&report);
        assert_eq!(unified.status, super::UnifiedFillReportStatus::Complete);
        assert!(!unified.complete);
        assert!(!unified.final_complete);
        assert_eq!(
            unified.coverage_state,
            UnifiedFillCoverageState::Provisional
        );
        assert_eq!(unified.provisional_as_of_ns, Some(window.end_ns));
    }

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
    fn raw_holiday_snapshot_is_immutable_and_content_addressed() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-calendar-holidays-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source = "https://example.invalid/holidays.json";
        let first = TradingCalendarHolidaysSnapshot::from_holidays(
            TradingCalendarHolidays::new(
                source,
                [
                    chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                    chrono::NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let path = write_trading_calendar_holidays_snapshot(&root, &first).unwrap();
        let first_bytes = fs::read(&path).unwrap();
        assert_eq!(path.parent().unwrap().file_name().unwrap(), "snapshots");
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            format!("{}.json", first.content_hash)
        );

        let mut refetched = first.clone();
        refetched.fetched_at = "2026-08-04T00:00:00Z".to_string();
        write_trading_calendar_holidays_snapshot(&root, &refetched).unwrap();
        let restored = read_trading_calendar_holidays_snapshot(&root)
            .unwrap()
            .unwrap();

        assert_eq!(restored, first);
        assert_eq!(fs::read(path).unwrap(), first_bytes);
        assert_eq!(restored.supported_year_start, 2025);
        assert_eq!(restored.supported_year_end, 2026);
        assert_eq!(
            restored
                .trading_days_between(
                    chrono::NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
                    chrono::NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
                )
                .unwrap(),
            vec![
                chrono::NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
            ]
        );

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
        assert_eq!(report.coverage_state, "final");
        assert!(report.complete_through_ns.is_none());
        assert!(report.day_complete);
        assert!(matches!(
            read_persisted_fill_report(&path).unwrap(),
            PersistedFillReport::Tick(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persisted_report_reader_accepts_legacy_minute_and_daily_v1() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-legacy-family-reports-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let window = serde_json::json!({
            "start_day": "2020-01-02",
            "end_day": "2020-01-02",
            "start_ns": 1,
            "end_ns": 2
        });
        let coverage = serde_json::json!({
            "namespace_dir": "/tmp/cache/minute-kline-v3",
            "cached_ranges": [[1, 2]],
            "missing_ranges": [],
            "complete": true
        });
        let minute_path = root.join("minute-v1.json");
        fs::write(
            &minute_path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "cache_kind": "minute",
                "generated_at": "2026-08-28T00:00:00Z",
                "cache_dir": "/tmp/cache",
                "requested_days": window,
                "requested_range": [1, 2],
                "market": "futures",
                "logical_symbols": ["SHFE.rb2601"],
                "symbols": [{
                    "symbol": "SHFE.rb2601",
                    "action": "skipped_complete",
                    "before": coverage,
                    "after": coverage,
                    "rows_written": 0
                }],
                "remote_used": false,
                "rows_written": 0,
                "complete": true,
                "dry_run": false
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            read_persisted_fill_report(&minute_path).unwrap(),
            PersistedFillReport::Minute(_)
        ));

        let daily_path = root.join("daily-v1.json");
        fs::write(
            &daily_path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "cache_kind": "daily",
                "generated_at": "2026-08-28T00:00:00Z",
                "cache_dir": "/tmp/cache",
                "requested_days": window,
                "market": "futures",
                "symbols": [{
                    "symbol": "SHFE.rb2601",
                    "after": {
                        "namespace_dir": "/tmp/cache/daily-kline-v1",
                        "path": "/tmp/cache/daily-kline-v1/SHFE.rb2601.tqdk",
                        "cached_ranges": [[1, 2]],
                        "missing_ranges": [],
                        "rows": 0,
                        "complete": true
                    }
                }],
                "remote_used": false,
                "rows_written": 0,
                "complete": true,
                "dry_run": false
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            read_persisted_fill_report(&daily_path).unwrap(),
            PersistedFillReport::Daily(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn persisted_report_reader_accepts_unified_v3() {
        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-unified-v3-report-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("daily-v3.json");
        fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 3,
                "cache_kind": "daily",
                "generated_at": "2026-08-28T00:00:00Z",
                "cache_dir": "/tmp/cache",
                "requested_days": {
                    "start_day": "2020-01-02",
                    "end_day": "2020-01-02",
                    "start_ns": 1,
                    "end_ns": 2
                },
                "requested_range": [1, 2],
                "market": "futures",
                "status": "complete",
                "interrupted": false,
                "error": null,
                "symbols": [{
                    "symbol": "SHFE.rb2601",
                    "status": "complete",
                    "requested_ranges": [[1, 2]],
                    "rows_written": 0,
                    "interrupted": false,
                    "error": null
                }],
                "remote_used": false,
                "rows_written": 0,
                "complete": true
            }))
            .unwrap(),
        )
        .unwrap();

        let PersistedFillReport::Unified(report) = read_persisted_fill_report(&path).unwrap()
        else {
            panic!("expected unified report");
        };
        assert!(report.final_complete);
        assert_eq!(report.coverage_state, UnifiedFillCoverageState::Final);
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
            coverage_state: "final".to_string(),
            complete_through_ns: None,
            day_complete: true,
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

        let root = std::env::temp_dir().join(format!(
            "tqsdk-cache-v2-report-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("tick-v2.json");
        super::write_fill_report(&path, &report).unwrap();
        assert!(matches!(
            read_persisted_fill_report(&path).unwrap(),
            PersistedFillReport::Tick(_)
        ));
        let _ = fs::remove_dir_all(root);
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
