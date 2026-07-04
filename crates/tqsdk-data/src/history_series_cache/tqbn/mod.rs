mod codec;
mod fixed;
mod format;
mod metadata;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{DataError, Result};
use crate::history_series_cache::{
    HistorySeriesCacheFileReport, HistorySeriesCacheFileStatus,
    HistorySeriesCacheMaintenanceReport, HistorySeriesCacheScanReport, HistorySeriesCoverageCommit,
    HistorySeriesCoverageReport, HistorySeriesCoverageRequest, HistorySeriesKind,
    HistorySeriesPurgeReport, HistorySeriesReadRequest, HistorySeriesReader, HistorySeriesRow,
    HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteRows,
    HistorySeriesWriteSegment,
};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, FixedOffset, NaiveDate, NaiveTime, TimeZone,
    Utc, Weekday,
};
use fs2::FileExt;
use tqsdk_core::{Kline, Tick};

use codec::{
    DecodedTqbnRecord, EncodedTickRecord, TqbnBlockType, checksum64_fnv1a, decode_block_payload,
    decode_file_prefix, decode_kline_record, decode_one_record, decode_tick1_record,
    decode_tick5_record, encode_file_prefix, encode_kline_record, encode_records_block,
    encode_tick_record,
};
use format::{
    FIXED_AMOUNT_SCALE, FIXED_PRICE_SCALE, TqbnCoverageRecordV1, TqbnKlineRecordV1, TqbnRType,
    TqbnRecordHeader, TqbnTick1RecordV1, TqbnTick5RecordV1,
};
use metadata::{TqbnMetadata, TqbnSchema, decode_metadata, encode_metadata};

pub(super) use format::{TQBN_FORMAT_ID, TQBN_SCHEMA_VERSION};

const ROOT_DIR_NAME: &str = "series";
const TICK_DIR_NAME: &str = "tick";
const KLINE_DIR_NAME: &str = "kline";
const TQBN_FILE_EXTENSION: &str = "tqbn";
const LOCK_FILE_NAME: &str = ".tqbn.lock";
const CST_OFFSET_SECONDS: i32 = 8 * 60 * 60;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const TQBN_PREFIX_HEADER_LEN: usize = 4 + 1 + 4 + 4 + 8;
const MAX_TQBN_PREFIX_METADATA_LEN: usize = 64 * 1024;
const TQBN_BLOCK_HEADER_LEN: usize = 4 + 1 + 3 + 8 + 8;
const MAX_TQBN_BLOCK_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) struct TqbnHistoryStore {
    root_dir: Arc<PathBuf>,
}

#[derive(Debug, Default)]
struct TqbnSeriesState {
    rows: Vec<HistorySeriesRow>,
    coverage: Vec<(i64, i64)>,
}

#[derive(Debug, Default)]
struct ParsedTqbnSeries {
    state: TqbnSeriesState,
    prefix: Option<codec::TqbnFilePrefix>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct TqbnSeriesMeta {
    path: PathBuf,
    symbol: String,
    kind: HistorySeriesKind,
    size_bytes: u64,
    modified: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TqbnPartitionRange {
    day: String,
    start_ns: i64,
    end_ns: i64,
}

struct TqbnReader {
    paths: Vec<PathBuf>,
    path_index: usize,
    symbol: String,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
    rows: Vec<HistorySeriesRow>,
}

type TqbnRowIdRange = Option<(i64, i64)>;
type TqbnRowDatetimeRange = Option<(i64, i64)>;
type TqbnAppendReport = (usize, TqbnRowIdRange, TqbnRowDatetimeRange);

impl TqbnHistoryStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(root_dir.join(ROOT_DIR_NAME))?;
        Ok(Self {
            root_dir: Arc::new(root_dir),
        })
    }

    pub(super) fn series_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        let kind = if duration_ns == 0 {
            HistorySeriesKind::Tick
        } else {
            HistorySeriesKind::Kline { duration_ns }
        };
        self.representative_series_path(symbol, kind)
    }

    fn representative_series_path(&self, symbol: &str, kind: HistorySeriesKind) -> PathBuf {
        match kind {
            HistorySeriesKind::Tick => self
                .root_dir
                .join(ROOT_DIR_NAME)
                .join(TICK_DIR_NAME)
                .join(escape_symbol_path_component(symbol)),
            HistorySeriesKind::Kline { duration_ns } => self
                .root_dir
                .join(ROOT_DIR_NAME)
                .join(KLINE_DIR_NAME)
                .join(duration_ns.to_string())
                .join(escape_symbol_path_component(symbol)),
        }
    }

    fn partition_series_path(&self, day: &str, symbol: &str, kind: HistorySeriesKind) -> PathBuf {
        let file_name = format!(
            "{}.{}",
            escape_symbol_path_component(symbol),
            TQBN_FILE_EXTENSION
        );
        match kind {
            HistorySeriesKind::Tick => self
                .root_dir
                .join(ROOT_DIR_NAME)
                .join(day)
                .join(TICK_DIR_NAME)
                .join(file_name),
            HistorySeriesKind::Kline { duration_ns } => self
                .root_dir
                .join(ROOT_DIR_NAME)
                .join(day)
                .join(KLINE_DIR_NAME)
                .join(duration_ns.to_string())
                .join(file_name),
        }
    }

    fn partition_paths_for_range(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<PathBuf>> {
        partition_ranges(start_ns, end_ns).map(|partitions| {
            partitions
                .into_iter()
                .map(|partition| self.partition_series_path(partition.day.as_str(), symbol, kind))
                .collect()
        })
    }

    fn partition_paths_for_series(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
    ) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for file in list_tqbn_file_metas(self.root_dir.as_path())? {
            if file.symbol == symbol && file.kind == kind {
                paths.push(file.path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn series_has_files(&self, symbol: &str, kind: HistorySeriesKind) -> Result<bool> {
        Ok(list_tqbn_file_metas(self.root_dir.as_path())?
            .into_iter()
            .any(|file| file.symbol == symbol && file.kind == kind))
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn append_coverage_to_partition_file(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<()> {
    let commit = HistorySeriesCoverageCommit {
        symbol: symbol.to_string(),
        kind,
        range_start_ns: start_ns,
        range_end_ns: end_ns,
        rows,
        id_range,
    };
    ensure_parent_dir(path)?;
    with_exclusive_tqbn_lock(path, || append_coverage_to_file(path, &commit))
}

fn segment_rows_summary(segment: &HistorySeriesWriteSegment<'_>) -> Result<TqbnAppendReport> {
    match (segment.kind, &segment.rows) {
        (HistorySeriesKind::Kline { duration_ns }, HistorySeriesWriteRows::Klines(rows)) => Ok((
            rows.len(),
            id_range(rows.iter().map(|row| row.id))?,
            datetime_range(rows.iter().map(|row| row.datetime), duration_ns)?,
        )),
        (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows)) => Ok((
            rows.len(),
            id_range(rows.iter().map(|row| row.id))?,
            datetime_range(
                rows.iter().map(|row| row.datetime),
                super::TICK_TAIL_REFRESH_NS,
            )?,
        )),
        _ => Err(DataError::InvalidState(
            "history TQBN write row kind does not match segment kind",
        )),
    }
}

fn partition_kline_slices(rows: &[Kline]) -> Result<Vec<(String, &[Kline])>> {
    partition_rows_by_trading_day(rows, |row| row.datetime)
}

fn partition_tick_slices(rows: &[Tick]) -> Result<Vec<(String, &[Tick])>> {
    partition_rows_by_trading_day(rows, |row| row.datetime)
}

fn partition_rows_by_trading_day<T>(
    rows: &[T],
    datetime: impl Fn(&T) -> i64,
) -> Result<Vec<(String, &[T])>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut partitions = Vec::new();
    let mut start = 0;
    let mut current_day = partition_day_for_timestamp_ns(datetime(&rows[0]))?;
    for index in 1..rows.len() {
        let day = partition_day_for_timestamp_ns(datetime(&rows[index]))?;
        if day != current_day {
            partitions.push((current_day, &rows[start..index]));
            start = index;
            current_day = day;
        }
    }
    partitions.push((current_day, &rows[start..]));
    Ok(partitions)
}

fn id_range_for_klines(rows: &[Kline]) -> Result<Option<(i64, i64)>> {
    id_range(rows.iter().map(|row| row.id))
}

fn id_range_for_ticks(rows: &[Tick]) -> Result<Option<(i64, i64)>> {
    id_range(rows.iter().map(|row| row.id))
}

fn merge_id_ranges(left: Option<(i64, i64)>, right: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (left, right) {
        (Some(left), Some(right)) => Some((left.0.min(right.0), left.1.max(right.1))),
        (Some(range), None) | (None, Some(range)) => Some(range),
        (None, None) => None,
    }
}

fn partition_day_for_timestamp_ns(timestamp_ns: i64) -> Result<String> {
    Ok(format_partition_day(trading_day_for_timestamp_ns(
        timestamp_ns,
    )?))
}

fn partition_ranges(start_ns: i64, end_ns: i64) -> Result<Vec<TqbnPartitionRange>> {
    validate_coverage_range(start_ns, end_ns)?;
    let mut ranges = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let day = trading_day_for_timestamp_ns(cursor)?;
        let boundary = trading_day_end_ns(day)?;
        let next = if boundary <= cursor {
            cursor.checked_add(1).ok_or_else(|| {
                DataError::InvalidResponse("TQBN partition range overflow".to_string())
            })?
        } else {
            boundary.min(end_ns)
        };
        if next <= cursor {
            return Err(DataError::InvalidResponse(
                "TQBN partition range did not advance".to_string(),
            ));
        }
        ranges.push(TqbnPartitionRange {
            day: format_partition_day(day),
            start_ns: cursor,
            end_ns: next,
        });
        cursor = next;
    }
    Ok(ranges)
}

fn trading_day_for_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = timestamp_ns.rem_euclid(NANOS_PER_SECOND) as u32;
    let utc = DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or_else(|| {
        DataError::InvalidResponse(format!("TQBN timestamp {timestamp_ns} is out of range"))
    })?;
    let local = utc.with_timezone(&cst_offset());
    let cutoff =
        NaiveTime::from_hms_opt(18, 0, 0).expect("fixed TQBN trading-day cutoff time is valid");
    let mut day = local.date_naive();
    if local.time() >= cutoff {
        day = add_days(day, 1)?;
    }
    normalize_weekend_trading_day(day)
}

fn trading_day_end_ns(day: NaiveDate) -> Result<i64> {
    trading_day_boundary_ns(day, 18)
}

fn trading_day_boundary_ns(day: NaiveDate, hours_from_midnight: i64) -> Result<i64> {
    let midnight = day
        .and_hms_opt(0, 0, 0)
        .expect("fixed TQBN trading-day midnight is valid");
    let local = midnight
        .checked_add_signed(ChronoDuration::hours(hours_from_midnight))
        .ok_or_else(|| {
            DataError::InvalidResponse("TQBN trading-day boundary overflow".to_string())
        })?;
    let local = cst_offset()
        .from_local_datetime(&local)
        .single()
        .ok_or_else(|| DataError::InvalidResponse("TQBN CST timestamp is ambiguous".to_string()))?;
    datetime_to_timestamp_ns(local)
}

fn datetime_to_timestamp_ns(datetime: DateTime<FixedOffset>) -> Result<i64> {
    let seconds_ns = datetime
        .timestamp()
        .checked_mul(NANOS_PER_SECOND)
        .ok_or_else(|| DataError::InvalidResponse("TQBN timestamp seconds overflow".to_string()))?;
    seconds_ns
        .checked_add(i64::from(datetime.timestamp_subsec_nanos()))
        .ok_or_else(|| DataError::InvalidResponse("TQBN timestamp nanos overflow".to_string()))
}

fn normalize_weekend_trading_day(mut day: NaiveDate) -> Result<NaiveDate> {
    while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
        day = add_days(day, 1)?;
    }
    Ok(day)
}

fn add_days(day: NaiveDate, days: i64) -> Result<NaiveDate> {
    day.checked_add_signed(ChronoDuration::days(days))
        .ok_or_else(|| DataError::InvalidResponse("TQBN trading-day date overflow".to_string()))
}

fn cst_offset() -> FixedOffset {
    FixedOffset::east_opt(CST_OFFSET_SECONDS).expect("fixed CST offset is valid")
}

fn format_partition_day(day: NaiveDate) -> String {
    day.format("%Y%m%d").to_string()
}

fn is_partition_day(value: &str) -> bool {
    value.len() == 8
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && NaiveDate::parse_from_str(value, "%Y%m%d").is_ok()
}

impl HistorySeriesStore for TqbnHistoryStore {
    fn format_id(&self) -> &'static str {
        TQBN_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        TQBN_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    fn series_path(&self, symbol: &str, kind: HistorySeriesKind) -> PathBuf {
        self.series_path(symbol, kind.duration_ns())
    }

    fn series_exists(&self, symbol: &str, kind: HistorySeriesKind) -> Result<bool> {
        self.series_has_files(symbol, kind)
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        let mut files = Vec::new();
        for path in list_series_tree_files(self.root_dir.as_path())? {
            files.push(scan_tqbn_tree_file(self.root_dir.as_path(), path)?);
        }
        files.sort_by(|left, right| left.file_name.cmp(&right.file_name));
        Ok(HistorySeriesCacheScanReport {
            cache_dir: self.root_dir.as_path().to_path_buf(),
            schema_version: TQBN_SCHEMA_VERSION,
            files,
        })
    }

    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        let mut report = HistorySeriesCacheMaintenanceReport::default();
        evict_expired_tqbn_files(self.root_dir.as_path(), retention_days, &mut report)?;
        compact_tqbn_files(self.root_dir.as_path())?;
        evict_tqbn_files_by_total_size(self.root_dir.as_path(), max_bytes, &mut report)?;
        Ok(report)
    }

    fn compact_series(&self, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
        for path in self.partition_paths_for_series(symbol, kind)? {
            compact_tqbn_file(&path, symbol, kind)?;
        }
        Ok(())
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        let mut coverage = Vec::new();
        for path in self.partition_paths_for_range(
            request.symbol.as_str(),
            request.kind,
            request.range_start_ns,
            request.range_end_ns,
        )? {
            let parsed = parse_tqbn_series_file(&path, request.symbol.as_str(), request.kind)?;
            if let Some(error) = parsed.error {
                return Err(DataError::InvalidResponse(error));
            }
            coverage.extend(parsed.state.coverage);
        }
        let cached_ranges = super::merge_datetime_ranges(coverage);
        let cached_ranges = super::rangeset_intersection(
            &[(request.range_start_ns, request.range_end_ns)],
            &cached_ranges,
        );
        let missing_ranges = super::rangeset_difference(
            &[(request.range_start_ns, request.range_end_ns)],
            &cached_ranges,
        );
        Ok(HistorySeriesCoverageReport {
            symbol: request.symbol,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
            cached_ranges,
            missing_ranges,
        })
    }

    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        validate_segment_rows(&segment)?;
        let (rows, id_range, datetime_range) = segment_rows_summary(&segment)?;
        let mut touched_path = None;
        let mut partition_stats = BTreeMap::new();

        match (segment.kind, &segment.rows) {
            (HistorySeriesKind::Kline { .. }, HistorySeriesWriteRows::Klines(rows_slice)) => {
                for (day, rows) in partition_kline_slices(rows_slice)? {
                    let path =
                        self.partition_series_path(day.as_str(), segment.symbol, segment.kind);
                    let id_range = id_range_for_klines(rows)?;
                    let stats = partition_stats.entry(day).or_insert((0, None));
                    stats.0 += rows.len();
                    stats.1 = merge_id_ranges(stats.1, id_range);
                    ensure_parent_dir(&path)?;
                    let partition_segment = HistorySeriesWriteSegment {
                        symbol: segment.symbol,
                        kind: segment.kind,
                        declared_range_ns: None,
                        rows: HistorySeriesWriteRows::Klines(rows),
                    };
                    with_exclusive_tqbn_lock(&path, || {
                        append_segment_to_file(&path, &partition_segment)
                    })?;
                    touched_path.get_or_insert(path);
                }
            }
            (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows_slice)) => {
                for (day, rows) in partition_tick_slices(rows_slice)? {
                    let path =
                        self.partition_series_path(day.as_str(), segment.symbol, segment.kind);
                    let id_range = id_range_for_ticks(rows)?;
                    let stats = partition_stats.entry(day).or_insert((0, None));
                    stats.0 += rows.len();
                    stats.1 = merge_id_ranges(stats.1, id_range);
                    ensure_parent_dir(&path)?;
                    let partition_segment = HistorySeriesWriteSegment {
                        symbol: segment.symbol,
                        kind: segment.kind,
                        declared_range_ns: None,
                        rows: HistorySeriesWriteRows::Ticks(rows),
                    };
                    with_exclusive_tqbn_lock(&path, || {
                        append_segment_to_file(&path, &partition_segment)
                    })?;
                    touched_path.get_or_insert(path);
                }
            }
            _ => {
                return Err(DataError::InvalidState(
                    "history TQBN write row kind does not match segment kind",
                ));
            }
        }

        if let Some((start_ns, end_ns)) = segment.declared_range_ns {
            for partition in partition_ranges(start_ns, end_ns)? {
                let path = self.partition_series_path(
                    partition.day.as_str(),
                    segment.symbol,
                    segment.kind,
                );
                let (partition_rows, partition_id_range) = partition_stats
                    .get(partition.day.as_str())
                    .copied()
                    .unwrap_or((0, None));
                append_coverage_to_partition_file(
                    &path,
                    segment.symbol,
                    segment.kind,
                    partition.start_ns,
                    partition.end_ns,
                    partition_rows,
                    partition_id_range,
                )?;
                touched_path.get_or_insert(path);
            }
        }

        Ok(HistorySeriesSegmentReport {
            path: touched_path
                .unwrap_or_else(|| self.series_path(segment.symbol, segment.kind.duration_ns())),
            symbol: segment.symbol.to_string(),
            kind: segment.kind,
            id_range,
            range_start_ns: datetime_range.map(|range| range.0),
            range_end_ns: datetime_range.map(|range| range.1),
            rows,
        })
    }

    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        validate_coverage_range(commit.range_start_ns, commit.range_end_ns)?;
        let symbol = commit.symbol.clone();
        let kind = commit.kind;
        for partition in partition_ranges(commit.range_start_ns, commit.range_end_ns)? {
            let path = self.partition_series_path(partition.day.as_str(), symbol.as_str(), kind);
            append_coverage_to_partition_file(
                &path,
                symbol.as_str(),
                kind,
                partition.start_ns,
                partition.end_ns,
                commit.rows,
                commit.id_range,
            )?;
        }
        self.coverage(HistorySeriesCoverageRequest {
            symbol,
            kind,
            range_start_ns: commit.range_start_ns,
            range_end_ns: commit.range_end_ns,
        })
    }

    fn purge_series(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
    ) -> Result<HistorySeriesPurgeReport> {
        let path = self.series_path(symbol, kind.duration_ns());
        let mut report = HistorySeriesPurgeReport {
            path: path.clone(),
            symbol: symbol.to_string(),
            removed_files: 0,
            removed_bytes: 0,
        };
        for file_path in self.partition_paths_for_series(symbol, kind)? {
            with_exclusive_tqbn_lock(&file_path, || {
                if let Some(size_bytes) = remove_tqbn_file_locked(&file_path)? {
                    report.removed_files += 1;
                    report.removed_bytes = report.removed_bytes.saturating_add(size_bytes);
                }
                Ok(())
            })?;
        }
        Ok(report)
    }

    fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        let paths = self.partition_paths_for_range(
            request.symbol.as_str(),
            request.kind,
            request.range_start_ns,
            request.range_end_ns,
        )?;
        Ok(Box::new(TqbnReader {
            paths,
            path_index: 0,
            symbol: request.symbol,
            kind: request.kind,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
            rows: Vec::new(),
        }))
    }
}

impl HistorySeriesReader for TqbnReader {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>> {
        loop {
            if let Some(row) = self.rows.pop() {
                return Ok(Some(row));
            }
            if self.path_index >= self.paths.len() {
                return Ok(None);
            }
            let path = &self.paths[self.path_index];
            self.path_index += 1;
            let parsed = parse_tqbn_series_file(path, self.symbol.as_str(), self.kind)?;
            if let Some(error) = parsed.error {
                return Err(DataError::InvalidResponse(error));
            }
            self.rows = rows_for_request(
                parsed.state.rows,
                self.kind,
                self.range_start_ns,
                self.range_end_ns,
            );
            self.rows.reverse();
        }
    }
}

fn append_segment_to_file(
    path: &Path,
    segment: &HistorySeriesWriteSegment<'_>,
) -> Result<HistorySeriesSegmentReport> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    ensure_tqbn_file_initialized(&mut file, segment.symbol, segment.kind)?;
    let (rows, id_range, datetime_range) = append_rows_block(&mut file, segment)?;
    if let Some((start, end)) = segment.declared_range_ns {
        append_coverage_block(&mut file, start, end, rows, id_range)?;
    }
    file.flush()?;
    file.sync_all()?;
    Ok(HistorySeriesSegmentReport {
        path: path.to_path_buf(),
        symbol: segment.symbol.to_string(),
        kind: segment.kind,
        id_range,
        range_start_ns: datetime_range.map(|range| range.0),
        range_end_ns: datetime_range.map(|range| range.1),
        rows,
    })
}

fn append_coverage_to_file(path: &Path, commit: &HistorySeriesCoverageCommit) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    ensure_tqbn_file_initialized(&mut file, commit.symbol.as_str(), commit.kind)?;
    append_coverage_block(
        &mut file,
        commit.range_start_ns,
        commit.range_end_ns,
        commit.rows,
        commit.id_range,
    )?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn ensure_tqbn_file_initialized(
    file: &mut File,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<()> {
    if file.metadata()?.len() == 0 {
        let metadata = match kind {
            HistorySeriesKind::Kline { duration_ns } => {
                TqbnMetadata::single_series_kline(symbol.to_string(), duration_ns)
            }
            HistorySeriesKind::Tick => {
                TqbnMetadata::single_series_tick(symbol.to_string(), tick_level_depth(symbol))
            }
        };
        let metadata = encode_metadata(&metadata)?;
        let prefix = encode_file_prefix(&metadata);
        file.write_all(&prefix.bytes)?;
        return Ok(());
    }

    file.seek(SeekFrom::Start(0))?;
    read_and_validate_tqbn_prefix(file, symbol, kind)?;
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

fn append_rows_block(
    file: &mut File,
    segment: &HistorySeriesWriteSegment<'_>,
) -> Result<TqbnAppendReport> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    match (segment.kind, &segment.rows) {
        (HistorySeriesKind::Kline { duration_ns }, HistorySeriesWriteRows::Klines(rows)) => {
            for row in *rows {
                record.clear();
                write_kline_record_bytes(&mut record, &encode_kline_record(row)?)?;
                append_record_to_blocks(file, &mut records, &record)?;
            }
            flush_records_block(file, &mut records)?;
            Ok((
                rows.len(),
                id_range(rows.iter().map(|row| row.id))?,
                datetime_range(rows.iter().map(|row| row.datetime), duration_ns)?,
            ))
        }
        (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows)) => {
            let five_level = tick_rows_use_five_levels(segment.symbol);
            for row in *rows {
                record.clear();
                match encode_tick_record(row, five_level)? {
                    EncodedTickRecord::Tick1(encoded) => {
                        write_tick1_record_bytes(&mut record, &encoded)?;
                    }
                    EncodedTickRecord::Tick5(encoded) => {
                        write_tick5_record_bytes(&mut record, &encoded)?;
                    }
                }
                append_record_to_blocks(file, &mut records, &record)?;
            }
            flush_records_block(file, &mut records)?;
            Ok((
                rows.len(),
                id_range(rows.iter().map(|row| row.id))?,
                datetime_range(
                    rows.iter().map(|row| row.datetime),
                    super::TICK_TAIL_REFRESH_NS,
                )?,
            ))
        }
        _ => Err(DataError::InvalidState(
            "history TQBN write row kind does not match segment kind",
        )),
    }
}

fn append_record_to_blocks(
    writer: &mut impl Write,
    records: &mut Vec<u8>,
    record: &[u8],
) -> Result<()> {
    append_record_to_blocks_with_limit(writer, records, record, MAX_TQBN_BLOCK_PAYLOAD_BYTES)
}

fn append_record_to_blocks_with_limit(
    writer: &mut impl Write,
    records: &mut Vec<u8>,
    record: &[u8],
    max_payload_bytes: usize,
) -> Result<()> {
    if record.len() > max_payload_bytes {
        return Err(DataError::InvalidResponse(format!(
            "TQBN record length {} exceeds max block payload {max_payload_bytes}",
            record.len()
        )));
    }
    let next_len = records.len().checked_add(record.len()).ok_or_else(|| {
        DataError::InvalidResponse("TQBN block records length overflow".to_string())
    })?;
    if next_len > max_payload_bytes {
        flush_records_block(writer, records)?;
    }
    records.extend_from_slice(record);
    Ok(())
}

fn flush_records_block(writer: &mut impl Write, records: &mut Vec<u8>) -> Result<()> {
    if !records.is_empty() {
        writer.write_all(&encode_records_block(records)?)?;
        records.clear();
    }
    Ok(())
}

fn append_coverage_block(
    file: &mut File,
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<()> {
    validate_coverage_range(start_ns, end_ns)?;
    let record = coverage_record(start_ns, end_ns, rows, id_range)?;
    let mut records = Vec::new();
    write_coverage_record_bytes(&mut records, &record)?;
    file.write_all(&encode_records_block(&records)?)?;
    Ok(())
}

fn coverage_record(
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<TqbnCoverageRecordV1> {
    let ts_event = u64::try_from(start_ns).map_err(|_| {
        DataError::InvalidResponse(format!(
            "TQBN coverage range start must be non-negative, got {start_ns}"
        ))
    })?;
    let rows = u64::try_from(rows)
        .map_err(|_| DataError::InvalidResponse("TQBN coverage row count overflow".to_string()))?;
    let (id_start, id_end, has_id_range) = match id_range {
        Some((start, end)) => (start, end, 1),
        None => (0, 0, 0),
    };
    Ok(TqbnCoverageRecordV1 {
        hd: TqbnRecordHeader::new::<TqbnCoverageRecordV1>(TqbnRType::Coverage, 1, ts_event),
        range_start_ns: start_ns,
        range_end_ns: end_ns,
        rows,
        id_start,
        id_end,
        has_id_range,
        reserved: [0; 7],
    })
}

fn parse_tqbn_series_file(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<ParsedTqbnSeries> {
    if !path.exists() {
        return Ok(ParsedTqbnSeries::default());
    }
    let mut file = File::open(path)?;
    let (prefix, offset) = read_and_validate_tqbn_prefix(&mut file, symbol, kind)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut parsed = ParsedTqbnSeries {
        prefix: Some(prefix),
        ..ParsedTqbnSeries::default()
    };
    match decode_blocks_streaming(&mut file, kind, &mut parsed.state) {
        Ok(()) => {}
        Err(error) => {
            parsed.error = Some(error.to_string());
        }
    }
    Ok(parsed)
}

fn read_and_validate_tqbn_prefix(
    file: &mut File,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<(codec::TqbnFilePrefix, usize)> {
    let (prefix, offset) = read_capped_tqbn_prefix(file)?;
    validate_tqbn_prefix(&prefix, symbol, kind)?;
    Ok((prefix, offset))
}

fn validate_tqbn_prefix(
    prefix: &codec::TqbnFilePrefix,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<()> {
    if prefix.schema_version != TQBN_SCHEMA_VERSION {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file schema version {} is unsupported; expected {TQBN_SCHEMA_VERSION}",
            prefix.schema_version
        )));
    }
    let metadata = decode_metadata(&prefix.metadata)?;
    validate_tqbn_metadata(&metadata, symbol, kind)
}

fn validate_tqbn_metadata(
    metadata: &TqbnMetadata,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<()> {
    if metadata.dataset != "tqsdk-history" {
        return Err(DataError::InvalidResponse(format!(
            "TQBN metadata dataset {} is unsupported",
            metadata.dataset
        )));
    }
    if metadata.symbol != symbol {
        return Err(DataError::InvalidResponse(format!(
            "TQBN metadata symbol {} does not match path symbol {symbol}",
            metadata.symbol
        )));
    }
    if metadata.duration_ns != kind.duration_ns() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN metadata duration {} does not match path duration {}",
            metadata.duration_ns,
            kind.duration_ns()
        )));
    }
    if metadata.price_scale != FIXED_PRICE_SCALE || metadata.amount_scale != FIXED_AMOUNT_SCALE {
        return Err(DataError::InvalidResponse(
            "TQBN metadata fixed-point scales are unsupported".to_string(),
        ));
    }

    match (metadata.schema, kind) {
        (TqbnSchema::Tick, HistorySeriesKind::Tick) => {
            let expected_depth = tick_level_depth(symbol);
            if metadata.level_depth != expected_depth {
                return Err(DataError::InvalidResponse(format!(
                    "TQBN metadata level depth {} does not match expected {expected_depth}",
                    metadata.level_depth
                )));
            }
        }
        (TqbnSchema::Kline, HistorySeriesKind::Kline { .. }) => {
            if metadata.level_depth != 0 {
                return Err(DataError::InvalidResponse(format!(
                    "TQBN kline metadata level depth {} is unsupported",
                    metadata.level_depth
                )));
            }
        }
        (schema, _) => {
            return Err(DataError::InvalidResponse(format!(
                "TQBN metadata schema {schema:?} does not match path duration {}",
                kind.duration_ns()
            )));
        }
    }

    if metadata.instruments.len() != 1 {
        return Err(DataError::InvalidResponse(format!(
            "TQBN metadata instrument count {} is unsupported",
            metadata.instruments.len()
        )));
    }
    let instrument = &metadata.instruments[0];
    if instrument.instrument_id != 1 || instrument.symbol != symbol {
        return Err(DataError::InvalidResponse(
            "TQBN metadata instrument mapping does not match single-series path".to_string(),
        ));
    }
    Ok(())
}

fn read_capped_tqbn_prefix(file: &mut File) -> Result<(codec::TqbnFilePrefix, usize)> {
    let mut bytes = Vec::with_capacity(TQBN_PREFIX_HEADER_LEN);
    read_up_to(file, TQBN_PREFIX_HEADER_LEN, &mut bytes)?;
    if bytes.len() < TQBN_PREFIX_HEADER_LEN {
        return decode_file_prefix(&bytes);
    }
    if &bytes[0..4] != b"TQBN" {
        return decode_file_prefix(&bytes);
    }

    let metadata_len = u32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
    if metadata_len > MAX_TQBN_PREFIX_METADATA_LEN {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file metadata length {metadata_len} exceeds max {MAX_TQBN_PREFIX_METADATA_LEN}"
        )));
    }

    let start_len = bytes.len();
    read_up_to(file, metadata_len, &mut bytes)?;
    if bytes.len() != start_len + metadata_len {
        return decode_file_prefix(&bytes);
    }
    decode_file_prefix(&bytes)
}

fn decode_blocks_streaming(
    file: &mut File,
    kind: HistorySeriesKind,
    state: &mut TqbnSeriesState,
) -> Result<()> {
    let mut offset = file.stream_position()?;
    while let Some(header) = read_block_header(file)? {
        let block_start = offset;
        offset = offset.saturating_add(TQBN_BLOCK_HEADER_LEN as u64);
        if &header[0..4] != b"TQBB" {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block magic mismatch at offset {block_start}"
            )));
        }

        let block_type = header[4];
        let block_flags = header[5];
        let records_len_u64 = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);
        let records_len = usize::try_from(records_len_u64).map_err(|_| {
            DataError::InvalidResponse(format!(
                "TQBN block records length {records_len_u64} does not fit in usize"
            ))
        })?;
        if records_len > MAX_TQBN_BLOCK_PAYLOAD_BYTES {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block records length {records_len} exceeds max {MAX_TQBN_BLOCK_PAYLOAD_BYTES}"
            )));
        }
        let records_checksum = u64::from_le_bytes([
            header[16], header[17], header[18], header[19], header[20], header[21], header[22],
            header[23],
        ]);

        let mut payload = vec![0_u8; records_len];
        read_exact_tqbn(file, &mut payload, || {
            format!(
                "TQBN block payload is truncated at offset {block_start}: requires {records_len} bytes"
            )
        })?;
        offset = offset.saturating_add(records_len as u64);

        let actual_checksum = checksum64_fnv1a(&payload);
        if actual_checksum != records_checksum {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block checksum mismatch at offset {block_start}: expected {records_checksum}, got {actual_checksum}"
            )));
        }
        let records = decode_block_payload(
            block_type,
            block_flags,
            payload,
            MAX_TQBN_BLOCK_PAYLOAD_BYTES,
        )?;

        match block_type {
            value if value == TqbnBlockType::Records as u8 => {
                decode_records_block(&records, kind, state)?;
            }
            value
                if value == TqbnBlockType::Metadata as u8
                    || value == TqbnBlockType::Index as u8 => {}
            value => {
                return Err(DataError::InvalidResponse(format!(
                    "TQBN block type {value} is unknown"
                )));
            }
        }
    }
    Ok(())
}

fn read_block_header(file: &mut File) -> Result<Option<[u8; TQBN_BLOCK_HEADER_LEN]>> {
    let mut header = [0_u8; TQBN_BLOCK_HEADER_LEN];
    let read = file.read(&mut header[..1])?;
    if read == 0 {
        return Ok(None);
    }
    read_exact_tqbn(file, &mut header[1..], || {
        format!(
            "TQBN block header is truncated: requires {TQBN_BLOCK_HEADER_LEN} bytes, got {read}"
        )
    })?;
    Ok(Some(header))
}

fn read_up_to(file: &mut File, len: usize, out: &mut Vec<u8>) -> Result<()> {
    let start_len = out.len();
    out.resize(start_len + len, 0);
    let mut read = 0;
    while read < len {
        let bytes_read = file.read(&mut out[start_len + read..start_len + len])?;
        if bytes_read == 0 {
            out.truncate(start_len + read);
            return Ok(());
        }
        read += bytes_read;
    }
    Ok(())
}

fn read_exact_tqbn(
    file: &mut File,
    mut buf: &mut [u8],
    message: impl FnOnce() -> String,
) -> Result<()> {
    while !buf.is_empty() {
        match file.read(buf) {
            Ok(0) => return Err(DataError::InvalidResponse(message())),
            Ok(bytes_read) => {
                let tmp = buf;
                buf = &mut tmp[bytes_read..];
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn decode_records_block(
    mut bytes: &[u8],
    kind: HistorySeriesKind,
    state: &mut TqbnSeriesState,
) -> Result<()> {
    while !bytes.is_empty() {
        let decoded = decode_one_record(bytes)?;
        let record_size = match decoded {
            DecodedTqbnRecord::Kline {
                bytes: record,
                record_size,
            } => {
                if matches!(kind, HistorySeriesKind::Kline { .. }) {
                    let record = read_kline_record_bytes(record)?;
                    state
                        .rows
                        .push(HistorySeriesRow::Kline(decode_kline_record(&record)?));
                }
                record_size
            }
            DecodedTqbnRecord::Tick1 {
                bytes: record,
                record_size,
            } => {
                if kind == HistorySeriesKind::Tick {
                    let record = read_tick1_record_bytes(record)?;
                    state
                        .rows
                        .push(HistorySeriesRow::Tick(decode_tick1_record(&record)?));
                }
                record_size
            }
            DecodedTqbnRecord::Tick5 {
                bytes: record,
                record_size,
            } => {
                if kind == HistorySeriesKind::Tick {
                    let record = read_tick5_record_bytes(record)?;
                    state
                        .rows
                        .push(HistorySeriesRow::Tick(decode_tick5_record(&record)?));
                }
                record_size
            }
            DecodedTqbnRecord::Coverage {
                bytes: record,
                record_size,
            } => {
                let record = read_coverage_record_bytes(record)?;
                if record.range_start_ns < record.range_end_ns {
                    state
                        .coverage
                        .push((record.range_start_ns, record.range_end_ns));
                }
                record_size
            }
            DecodedTqbnRecord::Unknown { record_size, .. } => record_size,
        };
        bytes = &bytes[record_size..];
    }
    Ok(())
}

fn rows_for_request(
    rows: Vec<HistorySeriesRow>,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
) -> Vec<HistorySeriesRow> {
    match kind {
        HistorySeriesKind::Kline { .. } => {
            let mut by_id = BTreeMap::new();
            for row in rows {
                if let HistorySeriesRow::Kline(row) = row
                    && row.datetime >= range_start_ns
                    && row.datetime < range_end_ns
                {
                    by_id.insert(row.id, row);
                }
            }
            by_id.into_values().map(HistorySeriesRow::Kline).collect()
        }
        HistorySeriesKind::Tick => {
            let mut by_id = BTreeMap::new();
            for row in rows {
                if let HistorySeriesRow::Tick(row) = row
                    && row.datetime >= range_start_ns
                    && row.datetime < range_end_ns
                {
                    by_id.insert(row.id, row);
                }
            }
            by_id.into_values().map(HistorySeriesRow::Tick).collect()
        }
    }
}

fn scan_tqbn_tree_file(root_dir: &Path, path: PathBuf) -> Result<HistorySeriesCacheFileReport> {
    let metadata = fs::metadata(&path)?;
    let size_bytes = metadata.len();
    let file_name = series_tree_file_name(root_dir, &path);
    let Some((symbol, kind)) = parse_series_tree_path(root_dir, &path) else {
        return Ok(HistorySeriesCacheFileReport {
            path,
            file_name,
            status: HistorySeriesCacheFileStatus::Ignored,
            symbol: None,
            duration_ns: None,
            id_range: None,
            row_width: None,
            rows: 0,
            size_bytes,
            schema_version: None,
            error: None,
        });
    };

    if size_bytes == 0 {
        return Ok(HistorySeriesCacheFileReport {
            path,
            file_name,
            status: HistorySeriesCacheFileStatus::EmptySegment,
            symbol: Some(symbol),
            duration_ns: Some(kind.duration_ns()),
            id_range: None,
            row_width: None,
            rows: 0,
            size_bytes,
            schema_version: Some(TQBN_SCHEMA_VERSION),
            error: None,
        });
    }

    match parse_tqbn_series_file(&path, symbol.as_str(), kind) {
        Ok(parsed) => Ok(HistorySeriesCacheFileReport {
            id_range: rows_id_range(&parsed.state.rows)?,
            row_width: row_width(kind),
            rows: parsed.state.rows.len(),
            status: if parsed.error.is_some() {
                HistorySeriesCacheFileStatus::IncompleteWrite
            } else {
                HistorySeriesCacheFileStatus::Readable
            },
            schema_version: Some(TQBN_SCHEMA_VERSION),
            error: parsed.error,
            path,
            file_name,
            symbol: Some(symbol),
            duration_ns: Some(kind.duration_ns()),
            size_bytes,
        }),
        Err(error) => Ok(HistorySeriesCacheFileReport {
            path,
            file_name,
            status: HistorySeriesCacheFileStatus::IncompleteWrite,
            symbol: Some(symbol),
            duration_ns: Some(kind.duration_ns()),
            id_range: None,
            row_width: row_width(kind),
            rows: 0,
            size_bytes,
            schema_version: None,
            error: Some(error.to_string()),
        }),
    }
}

fn list_series_tree_files(root_dir: &Path) -> Result<Vec<PathBuf>> {
    let series_root = root_dir.join(ROOT_DIR_NAME);
    if !series_root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_regular_files(&series_root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_regular_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_regular_files(&path, files)?;
        } else if entry.file_type()?.is_file() && path.extension().is_some_and(|ext| ext == "tqbn")
        {
            files.push(path);
        }
    }
    Ok(())
}

fn list_tqbn_file_metas(root_dir: &Path) -> Result<Vec<TqbnSeriesMeta>> {
    let mut files = Vec::new();
    for path in list_series_tree_files(root_dir)? {
        let Some((symbol, kind)) = parse_series_tree_path(root_dir, &path) else {
            continue;
        };
        let metadata = fs::metadata(&path)?;
        files.push(TqbnSeriesMeta {
            path,
            symbol,
            kind,
            size_bytes: metadata.len(),
            modified: metadata.modified().unwrap_or(UNIX_EPOCH),
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn evict_expired_tqbn_files(
    root_dir: &Path,
    retention_days: Option<u64>,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<()> {
    let Some(days) = retention_days else {
        return Ok(());
    };
    let ttl = Duration::from_secs(days.saturating_mul(24 * 60 * 60));
    let cutoff = SystemTime::now().checked_sub(ttl).unwrap_or(UNIX_EPOCH);
    for file in list_tqbn_file_metas(root_dir)? {
        remove_expired_tqbn_file(file.path.as_path(), cutoff, report)?;
    }
    Ok(())
}

fn compact_tqbn_files(root_dir: &Path) -> Result<()> {
    for file in list_tqbn_file_metas(root_dir)? {
        if !file.path.exists() {
            continue;
        }
        compact_tqbn_file(file.path.as_path(), file.symbol.as_str(), file.kind)?;
    }
    Ok(())
}

fn evict_tqbn_files_by_total_size(
    root_dir: &Path,
    max_bytes: Option<u64>,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<()> {
    let Some(limit) = max_bytes else {
        return Ok(());
    };
    loop {
        let mut files = list_tqbn_file_metas(root_dir)?;
        let total = files.iter().map(|file| file.size_bytes).sum::<u64>();
        if total <= limit {
            return Ok(());
        }
        files.sort_by(|left, right| {
            left.modified
                .cmp(&right.modified)
                .then_with(|| left.path.cmp(&right.path))
        });
        let Some(file) = files.first() else {
            return Ok(());
        };
        if !remove_tqbn_file_with_lock(file.path.as_path(), report)? {
            continue;
        }
    }
}

fn remove_expired_tqbn_file(
    path: &Path,
    cutoff: SystemTime,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<bool> {
    with_exclusive_tqbn_lock(path, || {
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if metadata.modified().unwrap_or(UNIX_EPOCH) > cutoff {
            return Ok(false);
        }
        if let Some(size_bytes) = remove_tqbn_file_locked(path)? {
            record_removed_tqbn_file(size_bytes, report);
            Ok(true)
        } else {
            Ok(false)
        }
    })
}

fn remove_tqbn_file_with_lock(
    path: &Path,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<bool> {
    with_exclusive_tqbn_lock(path, || {
        if let Some(size_bytes) = remove_tqbn_file_locked(path)? {
            record_removed_tqbn_file(size_bytes, report);
            Ok(true)
        } else {
            Ok(false)
        }
    })
}

fn remove_tqbn_file_locked(path: &Path) -> Result<Option<u64>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let size_bytes = metadata.len();
    match fs::remove_file(path) {
        Ok(()) => {
            sync_parent_dir(path)?;
            Ok(Some(size_bytes))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn record_removed_tqbn_file(size_bytes: u64, report: &mut HistorySeriesCacheMaintenanceReport) {
    report.removed_files += 1;
    report.removed_bytes = report.removed_bytes.saturating_add(size_bytes);
}

fn compact_tqbn_file(path: &Path, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
    with_exclusive_tqbn_lock(path, || compact_tqbn_file_locked(path, symbol, kind))
}

fn compact_tqbn_file_locked(path: &Path, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
    if fs::metadata(path)
        .map(|metadata| metadata.len() == 0)
        .unwrap_or(true)
    {
        return Ok(());
    }
    let parsed = parse_tqbn_series_file(path, symbol, kind)?;
    if let Some(error) = parsed.error {
        return Err(DataError::InvalidResponse(error));
    }
    let prefix = parsed.prefix.ok_or_else(|| {
        DataError::InvalidResponse("TQBN compaction requires file prefix".to_string())
    })?;
    let rows = compact_rows(parsed.state.rows, kind);
    let coverage = super::merge_datetime_ranges(parsed.state.coverage);
    let temp_path = compact_temp_path(path)?;
    let result = (|| -> Result<()> {
        let mut file = File::create(&temp_path)?;
        file.write_all(&prefix.bytes)?;
        write_compacted_rows_block(&mut file, symbol, kind, &rows)?;
        let id_range = rows_id_range(&rows)?;
        for (start_ns, end_ns) in coverage {
            append_coverage_block(&mut file, start_ns, end_ns, rows.len(), id_range)?;
        }
        file.flush()?;
        file.sync_all()?;
        fs::rename(&temp_path, path)?;
        sync_parent_dir(path)?;
        Ok(())
    })();
    if let Err(error) = result {
        if let Err(cleanup_error) = remove_compact_temp_file(&temp_path) {
            return Err(DataError::InvalidResponse(format!(
                "TQBN compaction failed for {}: {error}; also failed to remove temp file {}: {cleanup_error}",
                path.display(),
                temp_path.display()
            )));
        }
        return Err(error);
    }
    Ok(())
}

fn write_compacted_rows_block(
    file: &mut File,
    symbol: &str,
    kind: HistorySeriesKind,
    rows: &[HistorySeriesRow],
) -> Result<()> {
    match kind {
        HistorySeriesKind::Kline { .. } => {
            let rows = rows
                .iter()
                .filter_map(|row| match row {
                    HistorySeriesRow::Kline(row) => Some(row.clone()),
                    HistorySeriesRow::Tick(_) => None,
                })
                .collect::<Vec<_>>();
            append_rows_block(
                file,
                &HistorySeriesWriteSegment {
                    symbol,
                    kind,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Klines(&rows),
                },
            )?;
        }
        HistorySeriesKind::Tick => {
            let rows = rows
                .iter()
                .filter_map(|row| match row {
                    HistorySeriesRow::Tick(row) => Some(row.clone()),
                    HistorySeriesRow::Kline(_) => None,
                })
                .collect::<Vec<_>>();
            append_rows_block(
                file,
                &HistorySeriesWriteSegment {
                    symbol,
                    kind,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(&rows),
                },
            )?;
        }
    }
    Ok(())
}

fn compact_rows(rows: Vec<HistorySeriesRow>, kind: HistorySeriesKind) -> Vec<HistorySeriesRow> {
    match kind {
        HistorySeriesKind::Kline { .. } => {
            let mut by_id = BTreeMap::new();
            for row in rows {
                if let HistorySeriesRow::Kline(row) = row {
                    by_id.insert(row.id, row);
                }
            }
            by_id.into_values().map(HistorySeriesRow::Kline).collect()
        }
        HistorySeriesKind::Tick => {
            let mut by_id = BTreeMap::new();
            for row in rows {
                if let HistorySeriesRow::Tick(row) = row {
                    by_id.insert(row.id, row);
                }
            }
            by_id.into_values().map(HistorySeriesRow::Tick).collect()
        }
    }
}

fn compact_temp_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| DataError::InvalidResponse("history TQBN path is invalid".to_string()))?
        .to_string_lossy();
    Ok(path.with_file_name(format!("{file_name}.compact")))
}

fn remove_compact_temp_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_parent_dir(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn parse_series_tree_path(root_dir: &Path, path: &Path) -> Option<(String, HistorySeriesKind)> {
    let series_root = root_dir.join(ROOT_DIR_NAME);
    let relative = path.strip_prefix(series_root).ok()?;
    let mut components = relative.components();
    let day = components.next()?.as_os_str().to_string_lossy();
    if !is_partition_day(&day) {
        return None;
    }
    let kind_dir = components.next()?.as_os_str().to_string_lossy();
    let (symbol_file, kind) = match kind_dir.as_ref() {
        TICK_DIR_NAME => {
            let symbol_file = components.next()?.as_os_str().to_string_lossy();
            if components.next().is_some() {
                return None;
            }
            (symbol_file, HistorySeriesKind::Tick)
        }
        KLINE_DIR_NAME => {
            let duration = components
                .next()?
                .as_os_str()
                .to_string_lossy()
                .parse::<i64>()
                .ok()?;
            let symbol_file = components.next()?.as_os_str().to_string_lossy();
            if components.next().is_some() {
                return None;
            }
            (
                symbol_file,
                HistorySeriesKind::Kline {
                    duration_ns: duration,
                },
            )
        }
        _ => return None,
    };
    let symbol = symbol_file.strip_suffix(".tqbn")?;
    Some((unescape_symbol_path_component(symbol), kind))
}

fn series_tree_file_name(root_dir: &Path, path: &Path) -> String {
    let series_root = root_dir.join(ROOT_DIR_NAME);
    path.strip_prefix(series_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn with_exclusive_tqbn_lock<T>(path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let lock_dir = path.parent().ok_or_else(|| {
        DataError::InvalidResponse("history TQBN lock path is invalid".to_string())
    })?;
    let lock_path = lock_dir.join(LOCK_FILE_NAME);
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .truncate(false)
        .write(true)
        .open(lock_path)?;
    lock_file.lock_exclusive()?;
    let result = f();
    let unlock_result = fs2::FileExt::unlock(&lock_file);
    match (result, unlock_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(DataError::from(error)),
    }
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        DataError::InvalidResponse("history TQBN parent path is invalid".to_string())
    })?;
    let dir = File::open(parent)?;
    dir.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let _ = path.parent().ok_or_else(|| {
        DataError::InvalidResponse("history TQBN parent path is invalid".to_string())
    })?;
    Ok(())
}

fn validate_segment_rows(segment: &HistorySeriesWriteSegment<'_>) -> Result<()> {
    let datetimes: Vec<i64> = match (segment.kind, &segment.rows) {
        (HistorySeriesKind::Kline { .. }, HistorySeriesWriteRows::Klines(rows)) => {
            rows.iter().map(|row| row.datetime).collect()
        }
        (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows)) => {
            rows.iter().map(|row| row.datetime).collect()
        }
        _ => {
            return Err(DataError::InvalidState(
                "history TQBN write row kind does not match segment kind",
            ));
        }
    };
    if let Some((start_ns, end_ns)) = segment.declared_range_ns {
        validate_coverage_range(start_ns, end_ns)?;
        if datetimes
            .into_iter()
            .any(|datetime| datetime < start_ns || datetime >= end_ns)
        {
            return Err(DataError::InvalidState(
                "history TQBN row is outside declared coverage range",
            ));
        }
    }
    Ok(())
}

fn validate_coverage_range(start_ns: i64, end_ns: i64) -> Result<()> {
    if start_ns >= end_ns {
        return Err(DataError::InvalidState(
            "history TQBN declared range start must be less than end",
        ));
    }
    Ok(())
}

fn rows_id_range(rows: &[HistorySeriesRow]) -> Result<Option<(i64, i64)>> {
    id_range(rows.iter().map(|row| match row {
        HistorySeriesRow::Kline(row) => row.id,
        HistorySeriesRow::Tick(row) => row.id,
    }))
}

fn id_range(ids: impl IntoIterator<Item = i64>) -> Result<Option<(i64, i64)>> {
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
        .ok_or_else(|| DataError::InvalidResponse("TQBN segment id overflow".to_string()))?;
    Ok(Some((start, end)))
}

fn datetime_range(
    datetimes: impl IntoIterator<Item = i64>,
    width_ns: i64,
) -> Result<Option<(i64, i64)>> {
    let mut min_datetime = None;
    let mut max_datetime = None;
    for datetime in datetimes {
        min_datetime = Some(min_datetime.map_or(datetime, |value: i64| value.min(datetime)));
        max_datetime = Some(max_datetime.map_or(datetime, |value: i64| value.max(datetime)));
    }
    let Some(start) = min_datetime else {
        return Ok(None);
    };
    let end = max_datetime
        .and_then(|datetime: i64| datetime.checked_add(width_ns))
        .ok_or_else(|| DataError::InvalidResponse("TQBN segment datetime overflow".to_string()))?;
    Ok(Some((start, end)))
}

fn row_width(kind: HistorySeriesKind) -> Option<usize> {
    Some(match kind {
        HistorySeriesKind::Kline { .. } => std::mem::size_of::<TqbnKlineRecordV1>(),
        HistorySeriesKind::Tick => std::mem::size_of::<TqbnTick5RecordV1>(),
    })
}

fn tick_level_depth(symbol: &str) -> u8 {
    if tick_rows_use_five_levels(symbol) {
        5
    } else {
        1
    }
}

fn tick_rows_use_five_levels(symbol: &str) -> bool {
    matches!(symbol.split('.').next(), Some("SHFE" | "SSE" | "SZSE"))
}

fn escape_symbol_path_component(symbol: &str) -> String {
    symbol.replace('/', "%2F")
}

fn unescape_symbol_path_component(symbol: &str) -> String {
    symbol.replace("%2F", "/")
}

fn write_header(out: &mut Vec<u8>, header: &TqbnRecordHeader) {
    out.push(header.length_words);
    out.push(header.rtype);
    out.extend_from_slice(&header.flags.to_le_bytes());
    out.extend_from_slice(&header.instrument_id.to_le_bytes());
    out.extend_from_slice(&header.ts_event.to_le_bytes());
}

fn write_kline_record_bytes(out: &mut Vec<u8>, record: &TqbnKlineRecordV1) -> Result<()> {
    write_header(out, &record.hd);
    write_i64_fields(
        out,
        &[
            record.row_id,
            record.open,
            record.high,
            record.low,
            record.close,
            record.volume,
            record.open_oi,
            record.close_oi,
            record.epoch,
        ],
    );
    validate_record_len::<TqbnKlineRecordV1>(record.hd)
}

fn write_tick1_record_bytes(out: &mut Vec<u8>, record: &TqbnTick1RecordV1) -> Result<()> {
    write_header(out, &record.hd);
    write_i64_fields(
        out,
        &[
            record.row_id,
            record.last_price,
            record.average,
            record.highest,
            record.lowest,
            record.ask_price1,
            record.ask_volume1,
            record.bid_price1,
            record.bid_volume1,
            record.volume,
            record.amount,
            record.open_interest,
            record.epoch,
        ],
    );
    validate_record_len::<TqbnTick1RecordV1>(record.hd)
}

fn write_tick5_record_bytes(out: &mut Vec<u8>, record: &TqbnTick5RecordV1) -> Result<()> {
    write_header(out, &record.hd);
    write_i64_fields(
        out,
        &[
            record.row_id,
            record.last_price,
            record.average,
            record.highest,
            record.lowest,
            record.ask_price1,
            record.ask_volume1,
            record.bid_price1,
            record.bid_volume1,
            record.ask_price2,
            record.ask_volume2,
            record.bid_price2,
            record.bid_volume2,
            record.ask_price3,
            record.ask_volume3,
            record.bid_price3,
            record.bid_volume3,
            record.ask_price4,
            record.ask_volume4,
            record.bid_price4,
            record.bid_volume4,
            record.ask_price5,
            record.ask_volume5,
            record.bid_price5,
            record.bid_volume5,
            record.volume,
            record.amount,
            record.open_interest,
            record.epoch,
        ],
    );
    validate_record_len::<TqbnTick5RecordV1>(record.hd)
}

fn write_coverage_record_bytes(out: &mut Vec<u8>, record: &TqbnCoverageRecordV1) -> Result<()> {
    write_header(out, &record.hd);
    write_i64_fields(
        out,
        &[
            record.range_start_ns,
            record.range_end_ns,
            i64::from_le_bytes(record.rows.to_le_bytes()),
            record.id_start,
            record.id_end,
        ],
    );
    out.push(record.has_id_range);
    out.extend_from_slice(&record.reserved);
    validate_record_len::<TqbnCoverageRecordV1>(record.hd)
}

fn write_i64_fields(out: &mut Vec<u8>, fields: &[i64]) {
    for field in fields {
        out.extend_from_slice(&field.to_le_bytes());
    }
}

fn validate_record_len<R>(header: TqbnRecordHeader) -> Result<()> {
    let expected = std::mem::size_of::<R>();
    let actual = header.record_size();
    if actual != expected {
        return Err(DataError::InvalidResponse(format!(
            "TQBN encoded record length {actual} does not match expected {expected}"
        )));
    }
    Ok(())
}

fn read_kline_record_bytes(bytes: &[u8]) -> Result<TqbnKlineRecordV1> {
    let mut reader = RecordReader::new(bytes);
    Ok(TqbnKlineRecordV1 {
        hd: reader.read_header()?,
        row_id: reader.read_i64()?,
        open: reader.read_i64()?,
        high: reader.read_i64()?,
        low: reader.read_i64()?,
        close: reader.read_i64()?,
        volume: reader.read_i64()?,
        open_oi: reader.read_i64()?,
        close_oi: reader.read_i64()?,
        epoch: reader.read_i64()?,
    })
}

fn read_tick1_record_bytes(bytes: &[u8]) -> Result<TqbnTick1RecordV1> {
    let mut reader = RecordReader::new(bytes);
    Ok(TqbnTick1RecordV1 {
        hd: reader.read_header()?,
        row_id: reader.read_i64()?,
        last_price: reader.read_i64()?,
        average: reader.read_i64()?,
        highest: reader.read_i64()?,
        lowest: reader.read_i64()?,
        ask_price1: reader.read_i64()?,
        ask_volume1: reader.read_i64()?,
        bid_price1: reader.read_i64()?,
        bid_volume1: reader.read_i64()?,
        volume: reader.read_i64()?,
        amount: reader.read_i64()?,
        open_interest: reader.read_i64()?,
        epoch: reader.read_i64()?,
    })
}

fn read_tick5_record_bytes(bytes: &[u8]) -> Result<TqbnTick5RecordV1> {
    let mut reader = RecordReader::new(bytes);
    Ok(TqbnTick5RecordV1 {
        hd: reader.read_header()?,
        row_id: reader.read_i64()?,
        last_price: reader.read_i64()?,
        average: reader.read_i64()?,
        highest: reader.read_i64()?,
        lowest: reader.read_i64()?,
        ask_price1: reader.read_i64()?,
        ask_volume1: reader.read_i64()?,
        bid_price1: reader.read_i64()?,
        bid_volume1: reader.read_i64()?,
        ask_price2: reader.read_i64()?,
        ask_volume2: reader.read_i64()?,
        bid_price2: reader.read_i64()?,
        bid_volume2: reader.read_i64()?,
        ask_price3: reader.read_i64()?,
        ask_volume3: reader.read_i64()?,
        bid_price3: reader.read_i64()?,
        bid_volume3: reader.read_i64()?,
        ask_price4: reader.read_i64()?,
        ask_volume4: reader.read_i64()?,
        bid_price4: reader.read_i64()?,
        bid_volume4: reader.read_i64()?,
        ask_price5: reader.read_i64()?,
        ask_volume5: reader.read_i64()?,
        bid_price5: reader.read_i64()?,
        bid_volume5: reader.read_i64()?,
        volume: reader.read_i64()?,
        amount: reader.read_i64()?,
        open_interest: reader.read_i64()?,
        epoch: reader.read_i64()?,
    })
}

fn read_coverage_record_bytes(bytes: &[u8]) -> Result<TqbnCoverageRecordV1> {
    let mut reader = RecordReader::new(bytes);
    let hd = reader.read_header()?;
    let range_start_ns = reader.read_i64()?;
    let range_end_ns = reader.read_i64()?;
    let rows = u64::from_le_bytes(reader.read_i64()?.to_le_bytes());
    let id_start = reader.read_i64()?;
    let id_end = reader.read_i64()?;
    let has_id_range = reader.read_u8()?;
    let reserved = reader.read_u8_array::<7>()?;
    Ok(TqbnCoverageRecordV1 {
        hd,
        range_start_ns,
        range_end_ns,
        rows,
        id_start,
        id_end,
        has_id_range,
        reserved,
    })
}

struct RecordReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RecordReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_header(&mut self) -> Result<TqbnRecordHeader> {
        Ok(TqbnRecordHeader {
            length_words: self.read_u8()?,
            rtype: self.read_u8()?,
            flags: self.read_u16()?,
            instrument_id: self.read_u32()?,
            ts_event: self.read_u64()?,
        })
    }

    fn read_u8(&mut self) -> Result<u8> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    fn read_u8_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.read_exact(N)?;
        let mut array = [0_u8; N];
        array.copy_from_slice(bytes);
        Ok(array)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.read_u64()?.to_le_bytes()))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| DataError::InvalidResponse("TQBN record offset overflow".to_string()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| DataError::InvalidResponse("TQBN record is truncated".to_string()))?;
        self.offset = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tqsdk_core::{Kline, Tick};

    use crate::client::{KlineDataSeriesRequest, TickDataSeriesRequest};
    use crate::error::DataError;
    use crate::history_series_cache::{
        HistorySeriesCache, HistorySeriesCacheFileStatus, HistorySeriesCoverageCommit,
        HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesStore,
        HistorySeriesWriteRows, HistorySeriesWriteSegment,
    };

    use super::codec::{TqbnBlockType, decode_blocks, encode_block, encode_file_prefix};
    use super::{TqbnHistoryStore, TqbnMetadata, encode_metadata, tick_level_depth};

    const SYMBOL: &str = "SHFE.rb2601";
    const DURATION_NS: i64 = 60_000_000_000;

    #[test]
    fn tqbn_kline_range_round_trips_through_cache() {
        let cache = tqbn_cache("kline_range");
        let rows = vec![kline(1, 1_000, 10.0, 10.5), kline(2, 2_000, 10.5, 11.0)];

        cache
            .write_kline_range(SYMBOL, DURATION_NS, 1_000, 3_000, &rows)
            .unwrap();

        let series = cache
            .read_kline_data_series(KlineDataSeriesRequest::new(
                SYMBOL,
                Duration::from_nanos(DURATION_NS as u64),
                1_000,
                3_000,
            ))
            .unwrap();
        assert_eq!(series.rows().len(), 2);
        assert_kline_eq(&series.rows()[0], &rows[0]);
        assert_kline_eq(&series.rows()[1], &rows[1]);
    }

    #[test]
    fn tqbn_tick_five_level_range_round_trips_through_cache() {
        let cache = tqbn_cache("tick_range");
        let rows = vec![tick5(1, 1_000, 618.5, 623.5)];

        cache.write_tick_range(SYMBOL, 1_000, 2_000, &rows).unwrap();

        let series = cache
            .read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, 1_000, 2_000))
            .unwrap();
        assert_eq!(series.rows()[0].last_price, 618.5);
        assert_eq!(series.rows()[0].ask_price5, 623.5);
    }

    #[test]
    fn tqbn_record_block_writer_splits_at_payload_limit() {
        let mut out = Vec::new();
        let mut records = Vec::new();
        let record = vec![7_u8; 4];

        super::append_record_to_blocks_with_limit(&mut out, &mut records, &record, 8).unwrap();
        super::append_record_to_blocks_with_limit(&mut out, &mut records, &record, 8).unwrap();
        super::append_record_to_blocks_with_limit(&mut out, &mut records, &record, 8).unwrap();
        super::flush_records_block(&mut out, &mut records).unwrap();

        let blocks = decode_blocks(&out).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].block_type, TqbnBlockType::Records);
        assert_eq!(blocks[0].records.len(), 8);
        assert_eq!(blocks[1].records.len(), 4);
    }

    #[test]
    fn tqbn_record_block_writer_rejects_single_record_over_limit() {
        let mut out = Vec::new();
        let mut records = Vec::new();

        let error =
            super::append_record_to_blocks_with_limit(&mut out, &mut records, &[0_u8; 9], 8)
                .unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("record length 9 exceeds max block payload 8"))
        );
    }

    #[test]
    fn tqbn_coverage_only_commit_marks_range_complete() {
        let store = tqbn_store("coverage_only");

        let coverage = store
            .commit_coverage(HistorySeriesCoverageCommit {
                symbol: SYMBOL.to_string(),
                kind: HistorySeriesKind::Kline {
                    duration_ns: DURATION_NS,
                },
                range_start_ns: 1_000,
                range_end_ns: 3_000,
                rows: 0,
                id_range: None,
            })
            .unwrap();

        assert!(coverage.is_complete());
        assert_eq!(coverage.cached_ranges, vec![(1_000, 3_000)]);
    }

    #[test]
    fn tqbn_write_declared_range_marks_coverage_complete() {
        let store = tqbn_store("declared_range");

        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind: HistorySeriesKind::Tick,
                declared_range_ns: Some((1_000, 2_000)),
                rows: HistorySeriesWriteRows::Ticks(&[tick5(1, 1_000, 618.5, 623.5)]),
            })
            .unwrap();

        let coverage = store
            .coverage(HistorySeriesCoverageRequest {
                symbol: SYMBOL.to_string(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: 1_000,
                range_end_ns: 2_000,
            })
            .unwrap();
        assert!(coverage.is_complete());
    }

    #[test]
    fn tqbn_reader_uses_last_write_for_duplicate_row_id() {
        let cache = tqbn_cache("last_write_wins");
        let first = kline(7, 1_000, 10.0, 10.5);
        let second = kline(7, 1_000, 12.0, 12.5);

        cache
            .write_kline_range(SYMBOL, DURATION_NS, 1_000, 2_000, &[first])
            .unwrap();
        cache
            .write_kline_range(
                SYMBOL,
                DURATION_NS,
                1_000,
                2_000,
                std::slice::from_ref(&second),
            )
            .unwrap();

        let rows = cache
            .read_kline_window(SYMBOL, DURATION_NS, 1_000, 2_000)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_kline_eq(&rows[0], &second);
    }

    #[test]
    fn tqbn_series_path_reports_logical_daily_series_path() {
        let store = tqbn_store("path");

        assert_eq!(
            store.series_path("SHFE/rb2601", DURATION_NS),
            store
                .root_dir()
                .join("series")
                .join("kline")
                .join(DURATION_NS.to_string())
                .join("SHFE%2Frb2601")
        );
        assert_eq!(
            store.series_path("SHFE/rb2601", 0),
            store
                .root_dir()
                .join("series")
                .join("tick")
                .join("SHFE%2Frb2601")
        );
    }

    #[test]
    fn tqbn_scan_reports_truncated_block_but_coverage_rejects_it() {
        let store = tqbn_store("truncated-coverage");
        write_truncated_tick_file(&store);

        let scan = store.scan().unwrap();
        assert_eq!(scan.files.len(), 1);
        assert_eq!(
            scan.files[0].status,
            HistorySeriesCacheFileStatus::IncompleteWrite
        );
        assert!(
            scan.files[0]
                .error
                .as_deref()
                .is_some_and(|message| message.contains("truncated"))
        );

        let error = store
            .coverage(HistorySeriesCoverageRequest {
                symbol: SYMBOL.to_string(),
                kind: HistorySeriesKind::Tick,
                range_start_ns: 1_000,
                range_end_ns: 2_000,
            })
            .unwrap_err();
        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("truncated") && message.contains("block"))
        );
    }

    #[test]
    fn tqbn_read_rejects_truncated_block() {
        let store = tqbn_store("truncated-read-path");
        write_truncated_tick_file(&store);
        let cache = HistorySeriesCache::from_store(Arc::new(store));

        let error = cache
            .read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, 1_000, 2_000))
            .unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("truncated") && message.contains("block"))
        );
    }

    #[test]
    fn tqbn_scan_reports_oversized_block_without_allocating_payload() {
        let store = tqbn_store("oversized-scan");
        write_oversized_tick_block_header(&store);

        let scan = store.scan().unwrap();

        assert_eq!(scan.files.len(), 1);
        assert_eq!(
            scan.files[0].status,
            HistorySeriesCacheFileStatus::IncompleteWrite
        );
        assert!(
            scan.files[0]
                .error
                .as_deref()
                .is_some_and(|message| message.contains("exceeds max"))
        );
    }

    #[test]
    fn tqbn_read_rejects_oversized_block_without_allocating_payload() {
        let store = tqbn_store("oversized-read");
        write_oversized_tick_block_header(&store);
        let cache = HistorySeriesCache::from_store(Arc::new(store));

        let error = cache
            .read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, 1_000, 2_000))
            .unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("exceeds max"))
        );
    }

    fn tqbn_cache(test_name: &str) -> HistorySeriesCache {
        HistorySeriesCache::from_store(Arc::new(tqbn_store(test_name)))
    }

    fn tqbn_store(test_name: &str) -> TqbnHistoryStore {
        TqbnHistoryStore::new(test_root(test_name)).unwrap()
    }

    fn test_root(test_name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tqsdk-data-tqbn-{test_name}-{unique}-{}",
            std::process::id()
        ))
    }

    fn write_truncated_tick_file(store: &TqbnHistoryStore) {
        let path = store.partition_series_path("19700101", SYMBOL, HistorySeriesKind::Tick);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let prefix = valid_tick_prefix();
        let mut bytes = prefix.bytes;
        let mut block = encode_block(TqbnBlockType::Records, &[]);
        block.pop();
        bytes.extend_from_slice(&block);
        std::fs::write(path, bytes).unwrap();
    }

    fn write_oversized_tick_block_header(store: &TqbnHistoryStore) {
        let path = store.partition_series_path("19700101", SYMBOL, HistorySeriesKind::Tick);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let prefix = valid_tick_prefix();
        let mut bytes = prefix.bytes;
        bytes.extend_from_slice(b"TQBB");
        bytes.push(TqbnBlockType::Records as u8);
        bytes.extend_from_slice(&[0, 0, 0]);
        bytes.extend_from_slice(&(64 * 1024 * 1024 + 1_u64).to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    fn valid_tick_prefix() -> super::codec::TqbnFilePrefix {
        let metadata =
            TqbnMetadata::single_series_tick(SYMBOL.to_string(), tick_level_depth(SYMBOL));
        encode_file_prefix(&encode_metadata(&metadata).unwrap())
    }

    fn kline(id: i64, datetime: i64, open: f64, close: f64) -> Kline {
        Kline {
            id,
            datetime,
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            volume: 100 + id,
            open_oi: 200 + id,
            close_oi: 300 + id,
            epoch: Some(id),
        }
    }

    fn assert_kline_eq(actual: &Kline, expected: &Kline) {
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.datetime, expected.datetime);
        assert_eq!(actual.open, expected.open);
        assert_eq!(actual.high, expected.high);
        assert_eq!(actual.low, expected.low);
        assert_eq!(actual.close, expected.close);
        assert_eq!(actual.volume, expected.volume);
        assert_eq!(actual.open_oi, expected.open_oi);
        assert_eq!(actual.close_oi, expected.close_oi);
        assert_eq!(actual.epoch, expected.epoch);
    }

    fn tick5(id: i64, datetime: i64, last_price: f64, ask_price5: f64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            average: last_price - 0.5,
            highest: last_price + 1.0,
            lowest: last_price - 1.0,
            ask_price1: last_price + 0.5,
            ask_volume1: 10,
            bid_price1: last_price - 0.5,
            bid_volume1: 11,
            ask_price2: last_price + 1.5,
            ask_volume2: 20,
            bid_price2: last_price - 1.5,
            bid_volume2: 21,
            ask_price3: last_price + 2.5,
            ask_volume3: 30,
            bid_price3: last_price - 2.5,
            bid_volume3: 31,
            ask_price4: last_price + 3.5,
            ask_volume4: 40,
            bid_price4: last_price - 3.5,
            bid_volume4: 41,
            ask_price5,
            ask_volume5: 50,
            bid_price5: last_price - 4.5,
            bid_volume5: 51,
            volume: 1_000,
            amount: 1_234_567.8,
            open_interest: 888,
            epoch: Some(id),
        }
    }
}
