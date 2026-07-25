mod codec;
mod fixed;
mod format;
mod metadata;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{DataError, Result};
use crate::history_series_cache::{
    HistorySeriesCacheFileReport, HistorySeriesCacheFileStatus,
    HistorySeriesCacheMaintenanceReport, HistorySeriesCacheScanReport, HistorySeriesCoverageCommit,
    HistorySeriesCoverageReport, HistorySeriesCoverageRequest, HistorySeriesKind,
    HistorySeriesProvisionalCoverage, HistorySeriesPurgeReport, HistorySeriesReadRequest,
    HistorySeriesReader, HistorySeriesRow, HistorySeriesSegmentReport, HistorySeriesStore,
    HistorySeriesWriteRows, HistorySeriesWriteSegment,
};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, FixedOffset, NaiveDate, NaiveTime, TimeZone,
    Utc, Weekday,
};
use fs2::FileExt;
use tqsdk_core::{Kline, Tick};

use codec::{
    DecodedTqbnRecord, EncodedTickRecord, TqbnBlockType, checksum64_fnv1a, decode_block_payload,
    decode_block_payload_into, decode_file_prefix, decode_kline_record, decode_one_record,
    decode_tick1_record, decode_tick5_record, encode_block, encode_compacted_records_block,
    encode_file_prefix, encode_kline_record, encode_records_block, encode_tick_record,
    validate_block_flags,
};
use format::{
    FIXED_AMOUNT_SCALE, FIXED_PRICE_SCALE, TqbnCoverageRecordV1, TqbnKlineRecordV1,
    TqbnProvisionalCoverageRecordV1, TqbnRType, TqbnRecordHeader, TqbnTick1RecordV1,
    TqbnTick5RecordV1,
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
const TQBN_TARGET_RECORDS_BLOCK_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const TQBN_COVERAGE_INDEX_MAGIC: [u8; 4] = *b"TQCI";
const TQBN_COVERAGE_INDEX_VERSION: u8 = 1;
const TQBN_COVERAGE_INDEX_ROOT_FLAG: u8 = 0x01;
const TQBN_COVERAGE_INDEX_PROVISIONAL_FLAG: u8 = 0x02;
const TQBN_COVERAGE_INDEX_KNOWN_FLAGS: u8 =
    TQBN_COVERAGE_INDEX_ROOT_FLAG | TQBN_COVERAGE_INDEX_PROVISIONAL_FLAG;
const TQBN_COVERAGE_INDEX_NO_OFFSET: u64 = u64::MAX;
const TQBN_COVERAGE_INDEX_PAYLOAD_LEN: usize = 40;
const TQBN_RECORDS_INDEX_MAGIC: [u8; 4] = *b"TQRI";
const TQBN_RECORDS_INDEX_VERSION: u8 = 1;
const TQBN_RECORDS_INDEX_PAYLOAD_LEN: usize = 32;

#[derive(Debug, Clone)]
pub(super) struct TqbnHistoryStore {
    root_dir: Arc<PathBuf>,
    read_only: bool,
}

#[derive(Debug, Default)]
struct TqbnSeriesState {
    rows: Vec<HistorySeriesRow>,
    coverage: Vec<(i64, i64)>,
    provisional: Vec<TqbnProvisionalCoverage>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TqbnProvisionalCoverage {
    range_start_ns: i64,
    complete_through_ns: i64,
    as_of_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
}

struct TqbnReader {
    paths: Vec<PathBuf>,
    path_index: usize,
    symbol: String,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
    rows: std::vec::IntoIter<HistorySeriesRow>,
    partition: Option<TqbnStreamingPartition>,
    spare_records: Vec<u8>,
}

struct TqbnStreamingPartition {
    file: File,
    blocks: Vec<TqbnStreamingBlockPlan>,
    next_block_index: usize,
    active: Vec<TqbnStreamingBlockCursor>,
    spare_records: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct TqbnStreamingBlockPlan {
    descriptor: TqbnBlockDescriptor,
    first_id: i64,
    block_order: u64,
}

struct TqbnStreamingBlockCursor {
    block_order: u64,
    records: Vec<u8>,
    records_offset: usize,
    current: Option<HistorySeriesRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TqbnStreamingBlockScan {
    Empty,
    StrictlyIncreasing { first_id: i64 },
    NonIncreasing,
}

enum PreparedTqbnPartition {
    Missing,
    Streaming(TqbnStreamingPartition),
    Materialized(Vec<HistorySeriesRow>),
}

type TqbnRowIdRange = Option<(i64, i64)>;
type TqbnRowDatetimeRange = Option<(i64, i64)>;
type TqbnAppendReport = (usize, TqbnRowIdRange, TqbnRowDatetimeRange);
type TqbnRecordsBlockEncoder = fn(&[u8]) -> Result<Vec<u8>>;

#[derive(Debug, Default)]
struct PendingTqbnRecordsBlock {
    records: Vec<u8>,
    range_start_ns: Option<i64>,
    range_end_ns: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TqbnCoverageIndexV1 {
    flags: u8,
    previous_index_offset: u64,
    coverage_block_offset: u64,
    range_start_ns: i64,
    range_end_ns: i64,
}

#[derive(Debug, Default)]
struct TqbnIndexedCoverage {
    coverage: Vec<(i64, i64)>,
    provisional: Vec<TqbnProvisionalCoverage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TqbnRecordsIndexV1 {
    records_block_offset: u64,
    range_start_ns: i64,
    range_end_ns: i64,
}

#[derive(Debug, Clone, Copy)]
struct TqbnBlockDescriptor {
    block_type: u8,
    flags: u8,
    payload_offset: u64,
    payload_len: usize,
    payload_checksum: u64,
    end_offset: u64,
}

impl TqbnHistoryStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(root_dir.join(ROOT_DIR_NAME))?;
        Ok(Self {
            root_dir: Arc::new(root_dir),
            read_only: false,
        })
    }

    pub(super) fn new_read_only(root_dir: PathBuf) -> Self {
        Self {
            root_dir: Arc::new(root_dir),
            read_only: true,
        }
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(DataError::InvalidState(
                "history cache was opened read-only",
            ));
        }
        Ok(())
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
        let series_root = self.root_dir.join(ROOT_DIR_NAME);
        let entries = match fs::read_dir(&series_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let day = entry.file_name().to_string_lossy().into_owned();
            if !is_partition_day(&day) {
                continue;
            }
            let path = self.partition_series_path(day.as_str(), symbol, kind);
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_file() => return Ok(true),
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(false)
    }

    fn write_segment_with_coverage_fallback(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
        coverage: &[HistorySeriesCoverageCommit],
    ) -> Result<HistorySeriesSegmentReport> {
        let report = HistorySeriesStore::write_segment(self, segment)?;
        for commit in coverage {
            HistorySeriesStore::append_coverage(self, commit.clone())?;
        }
        Ok(report)
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

pub(crate) fn trading_day_for_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
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

pub(crate) fn trading_day_range(day: NaiveDate) -> Result<(NaiveDate, i64, i64)> {
    let day = normalize_weekend_trading_day(day)?;
    let mut previous_day = add_days(day, -1)?;
    while matches!(previous_day.weekday(), Weekday::Sat | Weekday::Sun) {
        previous_day = add_days(previous_day, -1)?;
    }
    Ok((
        day,
        trading_day_boundary_ns(previous_day, 18)?,
        trading_day_end_ns(day)?,
    ))
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
        self.ensure_writable()?;
        let mut report = HistorySeriesCacheMaintenanceReport::default();
        evict_expired_tqbn_files(self.root_dir.as_path(), retention_days, &mut report)?;
        compact_tqbn_files(self.root_dir.as_path())?;
        evict_tqbn_files_by_total_size(self.root_dir.as_path(), max_bytes, &mut report)?;
        Ok(report)
    }

    fn compact_series(&self, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
        self.ensure_writable()?;
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
            coverage.extend(parse_tqbn_coverage_file(
                &path,
                request.symbol.as_str(),
                request.kind,
            )?);
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

    fn provisional_coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<Option<HistorySeriesProvisionalCoverage>> {
        let mut provisional = Vec::new();
        let mut final_coverage = Vec::new();
        for path in self.partition_paths_for_range(
            request.symbol.as_str(),
            request.kind,
            request.range_start_ns,
            request.range_end_ns,
        )? {
            let parsed = parse_tqbn_checkpoint_file(&path, request.symbol.as_str(), request.kind)?;
            provisional.extend(parsed.provisional);
            final_coverage.extend(parsed.coverage);
        }
        let checkpoint = select_provisional_checkpoint(
            provisional,
            &super::merge_datetime_ranges(final_coverage),
            request.range_start_ns,
            request.range_end_ns,
        );
        Ok(
            checkpoint.map(|checkpoint| HistorySeriesProvisionalCoverage {
                symbol: request.symbol,
                kind: request.kind,
                range_start_ns: checkpoint.range_start_ns,
                complete_through_ns: checkpoint.complete_through_ns,
                as_of_ns: checkpoint.as_of_ns,
                rows: checkpoint.rows,
                id_range: checkpoint.id_range,
            }),
        )
    }

    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        self.ensure_writable()?;
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

    fn write_segment_with_coverage(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
        coverage: &[HistorySeriesCoverageCommit],
    ) -> Result<HistorySeriesSegmentReport> {
        self.ensure_writable()?;
        validate_segment_rows(&segment)?;
        let (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows)) =
            (segment.kind, &segment.rows)
        else {
            return self.write_segment_with_coverage_fallback(segment, coverage);
        };
        let partitions = partition_tick_slices(rows)?;
        let [(day, partition_rows)] = partitions.as_slice() else {
            return self.write_segment_with_coverage_fallback(segment, coverage);
        };
        if segment.declared_range_ns.is_some() {
            return self.write_segment_with_coverage_fallback(segment, coverage);
        }

        let mut partition_coverage = Vec::with_capacity(coverage.len());
        for commit in coverage {
            if commit.symbol.as_str() != segment.symbol || commit.kind != segment.kind {
                return self.write_segment_with_coverage_fallback(segment, coverage);
            }
            let ranges = partition_ranges(commit.range_start_ns, commit.range_end_ns)?;
            let [range] = ranges.as_slice() else {
                return self.write_segment_with_coverage_fallback(segment, coverage);
            };
            if range.day.as_str() != day.as_str() {
                return self.write_segment_with_coverage_fallback(segment, coverage);
            }
            partition_coverage.push(HistorySeriesCoverageCommit {
                symbol: commit.symbol.clone(),
                kind: commit.kind,
                range_start_ns: range.start_ns,
                range_end_ns: range.end_ns,
                rows: commit.rows,
                id_range: commit.id_range,
            });
        }
        if partition_coverage.is_empty() {
            return self.write_segment_with_coverage_fallback(segment, coverage);
        }

        let path = self.partition_series_path(day.as_str(), segment.symbol, segment.kind);
        ensure_parent_dir(&path)?;
        let partition_segment = HistorySeriesWriteSegment {
            symbol: segment.symbol,
            kind: segment.kind,
            declared_range_ns: None,
            rows: HistorySeriesWriteRows::Ticks(partition_rows),
        };
        with_exclusive_tqbn_lock(&path, || {
            append_segment_and_coverage_to_file(
                &path,
                &partition_segment,
                partition_coverage.as_slice(),
            )
        })
    }

    fn append_coverage(&self, commit: HistorySeriesCoverageCommit) -> Result<()> {
        self.ensure_writable()?;
        validate_coverage_range(commit.range_start_ns, commit.range_end_ns)?;
        for partition in partition_ranges(commit.range_start_ns, commit.range_end_ns)? {
            let path = self.partition_series_path(
                partition.day.as_str(),
                commit.symbol.as_str(),
                commit.kind,
            );
            append_coverage_to_partition_file(
                &path,
                commit.symbol.as_str(),
                commit.kind,
                partition.start_ns,
                partition.end_ns,
                commit.rows,
                commit.id_range,
            )?;
        }
        Ok(())
    }

    fn append_provisional(&self, commit: HistorySeriesProvisionalCoverage) -> Result<()> {
        self.ensure_writable()?;
        validate_provisional_coverage(&commit)?;
        for partition in partition_ranges(commit.range_start_ns, commit.complete_through_ns)? {
            let path = self.partition_series_path(
                partition.day.as_str(),
                commit.symbol.as_str(),
                commit.kind,
            );
            let partition_commit = HistorySeriesProvisionalCoverage {
                symbol: commit.symbol.clone(),
                kind: commit.kind,
                range_start_ns: partition.start_ns,
                complete_through_ns: partition.end_ns,
                as_of_ns: commit.as_of_ns,
                rows: commit.rows,
                id_range: commit.id_range,
            };
            ensure_parent_dir(&path)?;
            with_exclusive_tqbn_lock(&path, || {
                append_provisional_to_file(&path, &partition_commit)
            })?;
        }
        Ok(())
    }

    fn purge_series(
        &self,
        symbol: &str,
        kind: HistorySeriesKind,
    ) -> Result<HistorySeriesPurgeReport> {
        self.ensure_writable()?;
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
            rows: Vec::new().into_iter(),
            partition: None,
            spare_records: Vec::new(),
        }))
    }
}

impl HistorySeriesReader for TqbnReader {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>> {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(Some(row));
            }
            if let Some(partition) = self.partition.as_mut() {
                if let Some(row) =
                    partition.next_row(self.kind, self.range_start_ns, self.range_end_ns)?
                {
                    return Ok(Some(row));
                }
                let partition = self.partition.take().ok_or(DataError::InvalidState(
                    "TQBN streaming partition disappeared",
                ))?;
                self.spare_records = partition.into_spare_records();
                continue;
            }
            if self.path_index >= self.paths.len() {
                return Ok(None);
            }
            let path = &self.paths[self.path_index];
            self.path_index += 1;
            match prepare_tqbn_partition(
                path,
                self.symbol.as_str(),
                self.kind,
                self.range_start_ns,
                self.range_end_ns,
                &mut self.spare_records,
            )? {
                PreparedTqbnPartition::Missing => {}
                PreparedTqbnPartition::Streaming(partition) => {
                    self.partition = Some(partition);
                }
                PreparedTqbnPartition::Materialized(rows) => {
                    self.rows = rows.into_iter();
                }
            }
        }
    }
}

impl TqbnStreamingPartition {
    fn into_spare_records(mut self) -> Vec<u8> {
        debug_assert!(self.active.is_empty());
        std::mem::take(&mut self.spare_records)
    }

    fn next_row(
        &mut self,
        kind: HistorySeriesKind,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<Option<HistorySeriesRow>> {
        while self.active.is_empty() && self.next_block_index < self.blocks.len() {
            self.activate_next_block(kind, range_start_ns, range_end_ns)?;
        }
        if self.active.is_empty() {
            return Ok(None);
        }

        let mut next_id = self.next_active_id(kind)?;
        while self
            .blocks
            .get(self.next_block_index)
            .is_some_and(|block| block.first_id <= next_id)
        {
            self.activate_next_block(kind, range_start_ns, range_end_ns)?;
            next_id = self.next_active_id(kind)?;
        }

        let mut winner: Option<(u64, HistorySeriesRow)> = None;
        for cursor in &mut self.active {
            let current_id = cursor
                .current
                .as_ref()
                .and_then(|row| history_row_id(row, kind));
            if current_id != Some(next_id) {
                continue;
            }

            let row = cursor.current.take().ok_or(DataError::InvalidState(
                "TQBN streaming block cursor lost current row",
            ))?;
            if winner
                .as_ref()
                .is_none_or(|(block_order, _)| cursor.block_order > *block_order)
            {
                winner = Some((cursor.block_order, row));
            }
            cursor.advance(kind, range_start_ns, range_end_ns)?;
        }
        for cursor in &mut self.active {
            if cursor.current.is_none() {
                let mut records = std::mem::take(&mut cursor.records);
                records.clear();
                if records.capacity() > self.spare_records.capacity() {
                    std::mem::swap(&mut records, &mut self.spare_records);
                }
            }
        }
        self.active.retain(|cursor| cursor.current.is_some());

        winner
            .map(|(_, row)| row)
            .ok_or(DataError::InvalidState(
                "TQBN streaming merge produced no winning row",
            ))
            .map(Some)
    }

    fn next_active_id(&self, kind: HistorySeriesKind) -> Result<i64> {
        self.active
            .iter()
            .filter_map(|cursor| {
                cursor
                    .current
                    .as_ref()
                    .and_then(|row| history_row_id(row, kind))
            })
            .min()
            .ok_or(DataError::InvalidState(
                "TQBN streaming block cursor has no current row",
            ))
    }

    fn activate_next_block(
        &mut self,
        kind: HistorySeriesKind,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<()> {
        let block = *self
            .blocks
            .get(self.next_block_index)
            .ok_or(DataError::InvalidState(
                "TQBN streaming block plan exhausted",
            ))?;
        self.next_block_index += 1;

        let mut records = std::mem::take(&mut self.spare_records);
        read_decoded_tqbn_block_payload_into(&mut self.file, block.descriptor, &mut records)?;
        let mut cursor = TqbnStreamingBlockCursor {
            block_order: block.block_order,
            records,
            records_offset: 0,
            current: None,
        };
        cursor.advance(kind, range_start_ns, range_end_ns)?;
        if cursor.current.is_some() {
            self.active.push(cursor);
        }
        Ok(())
    }
}

impl TqbnStreamingBlockCursor {
    fn advance(
        &mut self,
        kind: HistorySeriesKind,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<()> {
        self.current = None;
        while self.records_offset < self.records.len() {
            let decoded = decode_one_record(&self.records[self.records_offset..])?;
            let (row, record_size) = decode_history_row_record(decoded, kind)?;
            self.records_offset =
                self.records_offset
                    .checked_add(record_size)
                    .ok_or_else(|| {
                        DataError::InvalidResponse(
                            "TQBN streaming records offset overflow".to_string(),
                        )
                    })?;
            if let Some(row) =
                row.filter(|row| history_row_in_datetime_range(row, range_start_ns, range_end_ns))
            {
                self.current = Some(row);
                return Ok(());
            }
        }
        Ok(())
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
    let first_block_offset = ensure_tqbn_file_initialized(&mut file, segment.symbol, segment.kind)?;
    let coverage_index_offset = match segment.declared_range_ns {
        Some(_) => find_latest_tqbn_coverage_index(&mut file, first_block_offset)?,
        None => None,
    };
    let (rows, id_range, datetime_range) = append_rows_block(&mut file, segment)?;
    if let Some((start, end)) = segment.declared_range_ns {
        file.flush()?;
        // Coverage is allowed to lag after a crash, but it must never become
        // durable before the rows it claims to cover.
        file.sync_data()?;
        let _ =
            append_coverage_block(&mut file, coverage_index_offset, start, end, rows, id_range)?;
    }
    file.flush()?;
    file.sync_data()?;
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

fn append_segment_and_coverage_to_file(
    path: &Path,
    segment: &HistorySeriesWriteSegment<'_>,
    coverage: &[HistorySeriesCoverageCommit],
) -> Result<HistorySeriesSegmentReport> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    let first_block_offset = ensure_tqbn_file_initialized(&mut file, segment.symbol, segment.kind)?;
    let mut coverage_index_offset = find_latest_tqbn_coverage_index(&mut file, first_block_offset)?;
    let (rows, id_range, datetime_range) = append_rows_block(&mut file, segment)?;
    file.flush()?;
    // Coverage is allowed to lag after a crash, but it must never become
    // durable before the rows it claims to cover.
    file.sync_data()?;
    for commit in coverage {
        coverage_index_offset = append_coverage_block(
            &mut file,
            coverage_index_offset,
            commit.range_start_ns,
            commit.range_end_ns,
            commit.rows,
            commit.id_range,
        )?;
    }
    file.flush()?;
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
    let first_block_offset =
        ensure_tqbn_file_initialized(&mut file, commit.symbol.as_str(), commit.kind)?;
    let coverage_index_offset = find_latest_tqbn_coverage_index(&mut file, first_block_offset)?;
    let _ = append_coverage_block(
        &mut file,
        coverage_index_offset,
        commit.range_start_ns,
        commit.range_end_ns,
        commit.rows,
        commit.id_range,
    )?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn append_provisional_to_file(
    path: &Path,
    commit: &HistorySeriesProvisionalCoverage,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    let first_block_offset =
        ensure_tqbn_file_initialized(&mut file, commit.symbol.as_str(), commit.kind)?;
    let coverage_index_offset = find_latest_tqbn_coverage_index(&mut file, first_block_offset)?;
    let _ = append_provisional_block(&mut file, coverage_index_offset, commit)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn ensure_tqbn_file_initialized(
    file: &mut File,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<u64> {
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
        let first_block_offset = u64::try_from(prefix.bytes.len()).map_err(|_| {
            DataError::InvalidResponse("TQBN file prefix length exceeds u64::MAX".to_string())
        })?;
        append_tqbn_coverage_index_root(&mut *file)?;
        return Ok(first_block_offset);
    }

    file.seek(SeekFrom::Start(0))?;
    let (_, first_block_offset) = read_and_validate_tqbn_prefix(file, symbol, kind)?;
    file.seek(SeekFrom::End(0))?;
    Ok(first_block_offset as u64)
}

fn append_rows_block(
    file: &mut File,
    segment: &HistorySeriesWriteSegment<'_>,
) -> Result<TqbnAppendReport> {
    append_rows_block_with_encoder(file, segment, encode_records_block)
}

fn append_compacted_rows_block(
    file: &mut File,
    segment: &HistorySeriesWriteSegment<'_>,
) -> Result<TqbnAppendReport> {
    append_rows_block_with_encoder(file, segment, encode_compacted_records_block)
}

fn append_rows_block_with_encoder(
    file: &mut File,
    segment: &HistorySeriesWriteSegment<'_>,
    encode_records: TqbnRecordsBlockEncoder,
) -> Result<TqbnAppendReport> {
    let mut block = PendingTqbnRecordsBlock::default();
    let mut record = Vec::new();
    match (segment.kind, &segment.rows) {
        (HistorySeriesKind::Kline { duration_ns }, HistorySeriesWriteRows::Klines(rows)) => {
            for row in *rows {
                record.clear();
                write_kline_record_bytes(&mut record, &encode_kline_record(row)?)?;
                append_indexed_record_to_blocks(
                    file,
                    &mut block,
                    &record,
                    row.datetime,
                    encode_records,
                )?;
            }
            flush_indexed_records_block(file, &mut block, encode_records)?;
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
                append_indexed_record_to_blocks(
                    file,
                    &mut block,
                    &record,
                    row.datetime,
                    encode_records,
                )?;
            }
            flush_indexed_records_block(file, &mut block, encode_records)?;
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

fn append_indexed_record_to_blocks(
    file: &mut File,
    block: &mut PendingTqbnRecordsBlock,
    record: &[u8],
    datetime_ns: i64,
    encode_records: TqbnRecordsBlockEncoder,
) -> Result<()> {
    if record.len() > MAX_TQBN_BLOCK_PAYLOAD_BYTES {
        return Err(DataError::InvalidResponse(format!(
            "TQBN record length {} exceeds max block payload {MAX_TQBN_BLOCK_PAYLOAD_BYTES}",
            record.len()
        )));
    }
    let next_len = block
        .records
        .len()
        .checked_add(record.len())
        .ok_or_else(|| {
            DataError::InvalidResponse("TQBN block records length overflow".to_string())
        })?;
    if !block.records.is_empty() && next_len > TQBN_TARGET_RECORDS_BLOCK_PAYLOAD_BYTES {
        flush_indexed_records_block(file, block, encode_records)?;
    }

    let range_end_ns = datetime_ns.checked_add(1).ok_or_else(|| {
        DataError::InvalidResponse("TQBN records index datetime overflow".to_string())
    })?;
    block.range_start_ns = Some(
        block
            .range_start_ns
            .map_or(datetime_ns, |current| current.min(datetime_ns)),
    );
    block.range_end_ns = Some(
        block
            .range_end_ns
            .map_or(range_end_ns, |current| current.max(range_end_ns)),
    );
    block.records.extend_from_slice(record);
    Ok(())
}

fn flush_indexed_records_block(
    file: &mut File,
    block: &mut PendingTqbnRecordsBlock,
    encode_records: TqbnRecordsBlockEncoder,
) -> Result<()> {
    if block.records.is_empty() {
        return Ok(());
    }
    let range_start_ns = block.range_start_ns.ok_or(DataError::InvalidState(
        "TQBN records block start is missing",
    ))?;
    let range_end_ns = block
        .range_end_ns
        .ok_or(DataError::InvalidState("TQBN records block end is missing"))?;
    let encoded = encode_records(&block.records)?;
    let records_block_offset = file.seek(SeekFrom::End(0))?;
    file.write_all(&encoded)?;
    append_tqbn_records_index(
        file,
        TqbnRecordsIndexV1 {
            records_block_offset,
            range_start_ns,
            range_end_ns,
        },
    )?;
    block.records.clear();
    block.range_start_ns = None;
    block.range_end_ns = None;
    Ok(())
}

#[cfg(test)]
fn append_record_to_blocks_with_limit(
    writer: &mut impl Write,
    records: &mut Vec<u8>,
    record: &[u8],
    max_payload_bytes: usize,
) -> Result<()> {
    append_record_to_blocks_with_limit_and_encoder(
        writer,
        records,
        record,
        max_payload_bytes,
        encode_records_block,
    )
}

#[cfg(test)]
fn append_record_to_blocks_with_limit_and_encoder(
    writer: &mut impl Write,
    records: &mut Vec<u8>,
    record: &[u8],
    max_payload_bytes: usize,
    encode_records: TqbnRecordsBlockEncoder,
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
        flush_records_block_with_encoder(writer, records, encode_records)?;
    }
    records.extend_from_slice(record);
    Ok(())
}

#[cfg(test)]
fn flush_records_block(writer: &mut impl Write, records: &mut Vec<u8>) -> Result<()> {
    flush_records_block_with_encoder(writer, records, encode_records_block)
}

#[cfg(test)]
fn flush_records_block_with_encoder(
    writer: &mut impl Write,
    records: &mut Vec<u8>,
    encode_records: TqbnRecordsBlockEncoder,
) -> Result<()> {
    if !records.is_empty() {
        writer.write_all(&encode_records(records)?)?;
        records.clear();
    }
    Ok(())
}

fn append_tqbn_records_index(file: &mut File, index: TqbnRecordsIndexV1) -> Result<u64> {
    let index_offset = file.seek(SeekFrom::End(0))?;
    let mut payload = Vec::with_capacity(TQBN_RECORDS_INDEX_PAYLOAD_LEN);
    payload.extend_from_slice(&TQBN_RECORDS_INDEX_MAGIC);
    payload.push(TQBN_RECORDS_INDEX_VERSION);
    payload.push(0);
    payload.extend_from_slice(&[0, 0]);
    payload.extend_from_slice(&index.records_block_offset.to_le_bytes());
    payload.extend_from_slice(&index.range_start_ns.to_le_bytes());
    payload.extend_from_slice(&index.range_end_ns.to_le_bytes());
    debug_assert_eq!(payload.len(), TQBN_RECORDS_INDEX_PAYLOAD_LEN);
    file.write_all(&encode_block(TqbnBlockType::Index, &payload))?;
    Ok(index_offset)
}

fn append_coverage_block(
    file: &mut File,
    previous_index_offset: Option<u64>,
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<Option<u64>> {
    validate_coverage_range(start_ns, end_ns)?;
    let record = coverage_record(start_ns, end_ns, rows, id_range)?;
    let mut records = Vec::new();
    write_coverage_record_bytes(&mut records, &record)?;
    let coverage_block_offset = file.seek(SeekFrom::End(0))?;
    // Coverage blocks stay uncompressed so an index can validate its exact
    // fixed-size payload without inflating ordinary market-data blocks.
    file.write_all(&encode_block(TqbnBlockType::Records, &records))?;

    let Some(previous_index_offset) = previous_index_offset else {
        return Ok(None);
    };
    let index = TqbnCoverageIndexV1 {
        flags: 0,
        previous_index_offset,
        coverage_block_offset,
        range_start_ns: start_ns,
        range_end_ns: end_ns,
    };
    append_tqbn_coverage_index(file, index).map(Some)
}

fn append_provisional_block(
    file: &mut File,
    previous_index_offset: Option<u64>,
    commit: &HistorySeriesProvisionalCoverage,
) -> Result<Option<u64>> {
    validate_provisional_coverage(commit)?;
    let record = provisional_coverage_record(commit)?;
    let mut records = Vec::new();
    write_provisional_coverage_record_bytes(&mut records, &record)?;
    let coverage_block_offset = file.seek(SeekFrom::End(0))?;
    file.write_all(&encode_block(TqbnBlockType::Records, &records))?;

    let Some(previous_index_offset) = previous_index_offset else {
        return Ok(None);
    };
    append_tqbn_coverage_index(
        file,
        TqbnCoverageIndexV1 {
            flags: TQBN_COVERAGE_INDEX_PROVISIONAL_FLAG,
            previous_index_offset,
            coverage_block_offset,
            range_start_ns: commit.range_start_ns,
            range_end_ns: commit.complete_through_ns,
        },
    )
    .map(Some)
}

fn append_tqbn_coverage_index_root(file: &mut File) -> Result<u64> {
    append_tqbn_coverage_index(
        file,
        TqbnCoverageIndexV1 {
            flags: TQBN_COVERAGE_INDEX_ROOT_FLAG,
            previous_index_offset: TQBN_COVERAGE_INDEX_NO_OFFSET,
            coverage_block_offset: TQBN_COVERAGE_INDEX_NO_OFFSET,
            range_start_ns: 0,
            range_end_ns: 0,
        },
    )
}

fn append_tqbn_coverage_index(file: &mut File, index: TqbnCoverageIndexV1) -> Result<u64> {
    let index_offset = file.seek(SeekFrom::End(0))?;
    let mut payload = Vec::with_capacity(TQBN_COVERAGE_INDEX_PAYLOAD_LEN);
    payload.extend_from_slice(&TQBN_COVERAGE_INDEX_MAGIC);
    payload.push(TQBN_COVERAGE_INDEX_VERSION);
    payload.push(index.flags);
    payload.extend_from_slice(&[0, 0]);
    payload.extend_from_slice(&index.previous_index_offset.to_le_bytes());
    payload.extend_from_slice(&index.coverage_block_offset.to_le_bytes());
    payload.extend_from_slice(&index.range_start_ns.to_le_bytes());
    payload.extend_from_slice(&index.range_end_ns.to_le_bytes());
    debug_assert_eq!(payload.len(), TQBN_COVERAGE_INDEX_PAYLOAD_LEN);
    file.write_all(&encode_block(TqbnBlockType::Index, &payload))?;
    Ok(index_offset)
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

fn provisional_coverage_record(
    commit: &HistorySeriesProvisionalCoverage,
) -> Result<TqbnProvisionalCoverageRecordV1> {
    let ts_event = u64::try_from(commit.range_start_ns).map_err(|_| {
        DataError::InvalidResponse(format!(
            "TQBN provisional range start must be non-negative, got {}",
            commit.range_start_ns
        ))
    })?;
    let rows = u64::try_from(commit.rows).map_err(|_| {
        DataError::InvalidResponse("TQBN provisional row count overflow".to_string())
    })?;
    let (id_start, id_end, has_id_range) = match commit.id_range {
        Some((start, end)) => (start, end, 1),
        None => (0, 0, 0),
    };
    Ok(TqbnProvisionalCoverageRecordV1 {
        hd: TqbnRecordHeader::new::<TqbnProvisionalCoverageRecordV1>(
            TqbnRType::ProvisionalCoverage,
            1,
            ts_event,
        ),
        range_start_ns: commit.range_start_ns,
        complete_through_ns: commit.complete_through_ns,
        as_of_ns: commit.as_of_ns,
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
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(ParsedTqbnSeries::default()),
        Err(error) => return Err(error.into()),
    };
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

fn prepare_tqbn_partition(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
    spare_records: &mut Vec<u8>,
) -> Result<PreparedTqbnPartition> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(PreparedTqbnPartition::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    let (_, first_block_offset) = read_and_validate_tqbn_prefix(&mut file, symbol, kind)?;
    let file_len = file.metadata()?.len();
    if let Some(blocks) = plan_tqbn_streaming_blocks(
        &mut file,
        kind,
        range_start_ns,
        range_end_ns,
        file_len,
        first_block_offset as u64,
        spare_records,
    )? {
        return Ok(PreparedTqbnPartition::Streaming(TqbnStreamingPartition {
            file,
            blocks,
            next_block_index: 0,
            active: Vec::new(),
            spare_records: std::mem::take(spare_records),
        }));
    }
    Ok(PreparedTqbnPartition::Materialized(
        parse_tqbn_rows_for_range(path, symbol, kind, range_start_ns, range_end_ns)?,
    ))
}

fn plan_tqbn_streaming_blocks(
    file: &mut File,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
    file_len: u64,
    first_block_offset: u64,
    spare_records: &mut Vec<u8>,
) -> Result<Option<Vec<TqbnStreamingBlockPlan>>> {
    let mut next_block_offset = first_block_offset;
    let mut blocks = Vec::new();
    while let Some(descriptor) = read_next_tqbn_records_block_descriptor_for_range(
        file,
        range_start_ns,
        range_end_ns,
        file_len,
        &mut next_block_offset,
    )? {
        read_decoded_tqbn_block_payload_into(file, descriptor, spare_records)?;
        let first_id =
            match scan_tqbn_streaming_block(spare_records, kind, range_start_ns, range_end_ns)? {
                TqbnStreamingBlockScan::Empty => continue,
                TqbnStreamingBlockScan::StrictlyIncreasing { first_id } => first_id,
                TqbnStreamingBlockScan::NonIncreasing => return Ok(None),
            };
        blocks.push(TqbnStreamingBlockPlan {
            descriptor,
            first_id,
            block_order: descriptor.payload_offset,
        });
    }
    blocks.sort_unstable_by_key(|block| (block.first_id, block.block_order));
    Ok(Some(blocks))
}

fn scan_tqbn_streaming_block(
    mut records: &[u8],
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
) -> Result<TqbnStreamingBlockScan> {
    let mut first_id = None;
    let mut previous_id = None;
    while !records.is_empty() {
        let decoded = decode_one_record(records)?;
        let (row, record_size) = decode_history_row_record(decoded, kind)?;
        records = &records[record_size..];
        let Some(row) =
            row.filter(|row| history_row_in_datetime_range(row, range_start_ns, range_end_ns))
        else {
            continue;
        };
        let Some(row_id) = history_row_id(&row, kind) else {
            return Ok(TqbnStreamingBlockScan::NonIncreasing);
        };
        if previous_id.is_some_and(|previous_id| row_id <= previous_id) {
            return Ok(TqbnStreamingBlockScan::NonIncreasing);
        }
        first_id.get_or_insert(row_id);
        previous_id = Some(row_id);
    }
    Ok(first_id.map_or(TqbnStreamingBlockScan::Empty, |first_id| {
        TqbnStreamingBlockScan::StrictlyIncreasing { first_id }
    }))
}

fn parse_tqbn_rows_for_range(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
) -> Result<Vec<HistorySeriesRow>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let (_, offset) = read_and_validate_tqbn_prefix(&mut file, symbol, kind)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut state = TqbnSeriesState::default();
    decode_blocks_streaming_for_range(&mut file, kind, range_start_ns, range_end_ns, &mut state)?;
    Ok(rows_for_request(
        state.rows,
        kind,
        range_start_ns,
        range_end_ns,
    ))
}

fn parse_tqbn_coverage_file(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<Vec<(i64, i64)>> {
    Ok(parse_tqbn_checkpoint_file(path, symbol, kind)?.coverage)
}

fn parse_tqbn_checkpoint_file(
    path: &Path,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<TqbnIndexedCoverage> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(TqbnIndexedCoverage::default());
        }
        Err(error) => return Err(error.into()),
    };
    let (_, offset) = read_and_validate_tqbn_prefix(&mut file, symbol, kind)?;
    if let Some(indexed) = try_parse_tqbn_checkpoint_index_chain(&mut file, offset as u64)? {
        return Ok(indexed);
    }
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut indexed = TqbnIndexedCoverage::default();
    decode_blocks_streaming_with(&mut file, |records| {
        decode_checkpoint_records_block(records, &mut indexed)
    })?;
    Ok(indexed)
}

#[cfg(test)]
fn try_parse_tqbn_coverage_index_chain(
    file: &mut File,
    first_block_offset: u64,
) -> Result<Option<Vec<(i64, i64)>>> {
    Ok(
        try_parse_tqbn_checkpoint_index_chain(file, first_block_offset)?
            .map(|indexed| indexed.coverage),
    )
}

fn try_parse_tqbn_checkpoint_index_chain(
    file: &mut File,
    first_block_offset: u64,
) -> Result<Option<TqbnIndexedCoverage>> {
    let file_len = file.metadata()?.len();
    let index_block_len = (TQBN_BLOCK_HEADER_LEN + TQBN_COVERAGE_INDEX_PAYLOAD_LEN) as u64;
    let Some(tail_offset) = file_len.checked_sub(index_block_len) else {
        return Ok(None);
    };
    if tail_offset < first_block_offset {
        return Ok(None);
    }
    let Some(mut index) = read_tqbn_coverage_index_at(file, tail_offset, file_len)? else {
        return Ok(None);
    };

    let mut index_offset = tail_offset;
    let mut indexed = TqbnIndexedCoverage::default();
    let max_links = file_len / index_block_len + 1;
    for _ in 0..max_links {
        if index.flags == TQBN_COVERAGE_INDEX_ROOT_FLAG {
            if index_offset != first_block_offset
                || index.previous_index_offset != TQBN_COVERAGE_INDEX_NO_OFFSET
                || index.coverage_block_offset != TQBN_COVERAGE_INDEX_NO_OFFSET
                || index.range_start_ns != 0
                || index.range_end_ns != 0
            {
                return Ok(None);
            }
            indexed.coverage.reverse();
            indexed.provisional.reverse();
            return Ok(Some(indexed));
        }
        if !matches!(index.flags, 0 | TQBN_COVERAGE_INDEX_PROVISIONAL_FLAG)
            || index.previous_index_offset == TQBN_COVERAGE_INDEX_NO_OFFSET
            || index.coverage_block_offset == TQBN_COVERAGE_INDEX_NO_OFFSET
            || index.previous_index_offset < first_block_offset
            || index.coverage_block_offset < first_block_offset
            || index.previous_index_offset >= index_offset
            || index.coverage_block_offset >= index_offset
            || index.range_start_ns >= index.range_end_ns
        {
            return Ok(None);
        }

        if index.flags == TQBN_COVERAGE_INDEX_PROVISIONAL_FLAG {
            let Some(checkpoint) = read_tqbn_indexed_provisional_coverage(
                file,
                index.coverage_block_offset,
                index_offset,
                file_len,
            )?
            else {
                return Ok(None);
            };
            if (checkpoint.range_start_ns, checkpoint.complete_through_ns)
                != (index.range_start_ns, index.range_end_ns)
            {
                return Ok(None);
            }
            indexed.provisional.push(checkpoint);
        } else {
            let Some(range) = read_tqbn_indexed_coverage_range(
                file,
                index.coverage_block_offset,
                index_offset,
                file_len,
            )?
            else {
                return Ok(None);
            };
            if range != (index.range_start_ns, index.range_end_ns) {
                return Ok(None);
            }
            indexed.coverage.push(range);
        }

        index_offset = index.previous_index_offset;
        let Some(previous) = read_tqbn_coverage_index_at(file, index_offset, file_len)? else {
            return Ok(None);
        };
        index = previous;
    }
    Ok(None)
}

fn find_latest_tqbn_coverage_index(
    file: &mut File,
    first_block_offset: u64,
) -> Result<Option<u64>> {
    let file_len = file.metadata()?.len();
    let index_block_len = (TQBN_BLOCK_HEADER_LEN + TQBN_COVERAGE_INDEX_PAYLOAD_LEN) as u64;
    if let Some(tail_offset) = file_len.checked_sub(index_block_len)
        && tail_offset >= first_block_offset
        && read_tqbn_coverage_index_at(file, tail_offset, file_len)?.is_some()
    {
        file.seek(SeekFrom::End(0))?;
        return Ok(Some(tail_offset));
    }

    let mut offset = first_block_offset;
    let mut latest = None;
    while offset < file_len {
        let descriptor = match read_tqbn_block_descriptor_at(file, offset, file_len) {
            Ok(descriptor) => descriptor,
            Err(_) => {
                file.seek(SeekFrom::End(0))?;
                return Ok(None);
            }
        };
        if descriptor.block_type == TqbnBlockType::Index as u8 {
            let payload = match read_tqbn_block_payload(file, descriptor) {
                Ok(payload) => payload,
                Err(_) => {
                    file.seek(SeekFrom::End(0))?;
                    return Ok(None);
                }
            };
            if decode_tqbn_coverage_index(&payload).is_some() {
                latest = Some(offset);
            } else if decode_tqbn_records_index(&payload).is_none() {
                file.seek(SeekFrom::End(0))?;
                return Ok(None);
            }
        }
        offset = descriptor.end_offset;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(latest)
}

fn read_tqbn_coverage_index_at(
    file: &mut File,
    block_offset: u64,
    file_len: u64,
) -> Result<Option<TqbnCoverageIndexV1>> {
    let descriptor = match read_tqbn_block_descriptor_at(file, block_offset, file_len) {
        Ok(descriptor) => descriptor,
        Err(_) => return Ok(None),
    };
    if descriptor.block_type != TqbnBlockType::Index as u8
        || descriptor.flags != 0
        || descriptor.payload_len != TQBN_COVERAGE_INDEX_PAYLOAD_LEN
    {
        return Ok(None);
    }
    let payload = match read_tqbn_block_payload(file, descriptor) {
        Ok(payload) => payload,
        Err(_) => return Ok(None),
    };
    Ok(decode_tqbn_coverage_index(&payload))
}

fn read_tqbn_indexed_coverage_range(
    file: &mut File,
    coverage_block_offset: u64,
    index_offset: u64,
    file_len: u64,
) -> Result<Option<(i64, i64)>> {
    let descriptor = match read_tqbn_block_descriptor_at(file, coverage_block_offset, file_len) {
        Ok(descriptor) => descriptor,
        Err(_) => return Ok(None),
    };
    if descriptor.block_type != TqbnBlockType::Records as u8
        || descriptor.flags != 0
        || descriptor.end_offset != index_offset
        || descriptor.payload_len != std::mem::size_of::<TqbnCoverageRecordV1>()
    {
        return Ok(None);
    }
    let payload = match read_tqbn_block_payload(file, descriptor) {
        Ok(payload) => payload,
        Err(_) => return Ok(None),
    };
    let mut coverage = Vec::new();
    if decode_coverage_records_block(&payload, &mut coverage).is_err() {
        return Ok(None);
    }
    let [range] = coverage.as_slice() else {
        return Ok(None);
    };
    Ok(Some(*range))
}

fn read_tqbn_indexed_provisional_coverage(
    file: &mut File,
    coverage_block_offset: u64,
    index_offset: u64,
    file_len: u64,
) -> Result<Option<TqbnProvisionalCoverage>> {
    let descriptor = match read_tqbn_block_descriptor_at(file, coverage_block_offset, file_len) {
        Ok(descriptor) => descriptor,
        Err(_) => return Ok(None),
    };
    if descriptor.block_type != TqbnBlockType::Records as u8
        || descriptor.flags != 0
        || descriptor.end_offset != index_offset
        || descriptor.payload_len != std::mem::size_of::<TqbnProvisionalCoverageRecordV1>()
    {
        return Ok(None);
    }
    let payload = match read_tqbn_block_payload(file, descriptor) {
        Ok(payload) => payload,
        Err(_) => return Ok(None),
    };
    let mut indexed = TqbnIndexedCoverage::default();
    if decode_checkpoint_records_block(&payload, &mut indexed).is_err() {
        return Ok(None);
    }
    let [checkpoint] = indexed.provisional.as_slice() else {
        return Ok(None);
    };
    Ok(Some(*checkpoint))
}

fn decode_tqbn_coverage_index(payload: &[u8]) -> Option<TqbnCoverageIndexV1> {
    if payload.len() != TQBN_COVERAGE_INDEX_PAYLOAD_LEN
        || payload[0..4] != TQBN_COVERAGE_INDEX_MAGIC
        || payload[4] != TQBN_COVERAGE_INDEX_VERSION
        || payload[5] & !TQBN_COVERAGE_INDEX_KNOWN_FLAGS != 0
        || payload[6..8] != [0, 0]
    {
        return None;
    }
    Some(TqbnCoverageIndexV1 {
        flags: payload[5],
        previous_index_offset: u64::from_le_bytes(payload[8..16].try_into().ok()?),
        coverage_block_offset: u64::from_le_bytes(payload[16..24].try_into().ok()?),
        range_start_ns: i64::from_le_bytes(payload[24..32].try_into().ok()?),
        range_end_ns: i64::from_le_bytes(payload[32..40].try_into().ok()?),
    })
}

fn decode_tqbn_records_index(payload: &[u8]) -> Option<TqbnRecordsIndexV1> {
    if payload.len() != TQBN_RECORDS_INDEX_PAYLOAD_LEN
        || payload[0..4] != TQBN_RECORDS_INDEX_MAGIC
        || payload[4] != TQBN_RECORDS_INDEX_VERSION
        || payload[5..8] != [0, 0, 0]
    {
        return None;
    }
    let index = TqbnRecordsIndexV1 {
        records_block_offset: u64::from_le_bytes(payload[8..16].try_into().ok()?),
        range_start_ns: i64::from_le_bytes(payload[16..24].try_into().ok()?),
        range_end_ns: i64::from_le_bytes(payload[24..32].try_into().ok()?),
    };
    (index.range_start_ns < index.range_end_ns).then_some(index)
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
    decode_blocks_streaming_with(file, |records| decode_records_block(records, kind, state))
}

fn decode_blocks_streaming_for_range(
    file: &mut File,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
    state: &mut TqbnSeriesState,
) -> Result<()> {
    let file_len = file.metadata()?.len();
    let mut next_block_offset = file.stream_position()?;
    while let Some(descriptor) = read_next_tqbn_records_block_descriptor_for_range(
        file,
        range_start_ns,
        range_end_ns,
        file_len,
        &mut next_block_offset,
    )? {
        let records = read_decoded_tqbn_block_payload(file, descriptor)?;
        let mut block_state = TqbnSeriesState::default();
        decode_records_block(&records, kind, &mut block_state)?;
        state.rows.extend(block_state.rows);
        state.coverage.extend(block_state.coverage);
        state.provisional.extend(block_state.provisional);
    }
    Ok(())
}

fn read_next_tqbn_records_block_descriptor_for_range(
    file: &mut File,
    range_start_ns: i64,
    range_end_ns: i64,
    file_len: u64,
    next_block_offset: &mut u64,
) -> Result<Option<TqbnBlockDescriptor>> {
    while *next_block_offset < file_len {
        let block_offset = *next_block_offset;
        let descriptor = read_tqbn_block_descriptor_at(file, block_offset, file_len)?;
        validate_block_flags(descriptor.block_type, descriptor.flags)?;
        match descriptor.block_type {
            value if value == TqbnBlockType::Records as u8 => {
                let (intersects_range, block_end_offset) = if let Some((index_descriptor, index)) =
                    read_following_tqbn_records_index(file, block_offset, descriptor, file_len)?
                {
                    (
                        index.range_start_ns < range_end_ns && range_start_ns < index.range_end_ns,
                        index_descriptor.end_offset,
                    )
                } else {
                    (true, descriptor.end_offset)
                };
                *next_block_offset = block_end_offset;
                if intersects_range {
                    return Ok(Some(descriptor));
                }
            }
            value
                if value == TqbnBlockType::Metadata as u8
                    || value == TqbnBlockType::Index as u8 =>
            {
                let _ = read_decoded_tqbn_block_payload(file, descriptor)?;
                *next_block_offset = descriptor.end_offset;
            }
            value => {
                return Err(DataError::InvalidResponse(format!(
                    "TQBN block type {value} is unknown"
                )));
            }
        }
    }
    Ok(None)
}

fn read_following_tqbn_records_index(
    file: &mut File,
    records_block_offset: u64,
    records_descriptor: TqbnBlockDescriptor,
    file_len: u64,
) -> Result<Option<(TqbnBlockDescriptor, TqbnRecordsIndexV1)>> {
    if records_descriptor.end_offset >= file_len {
        return Ok(None);
    }
    let index_descriptor =
        read_tqbn_block_descriptor_at(file, records_descriptor.end_offset, file_len)?;
    if index_descriptor.block_type != TqbnBlockType::Index as u8 {
        return Ok(None);
    }
    let payload = read_decoded_tqbn_block_payload(file, index_descriptor)?;
    let Some(index) = decode_tqbn_records_index(&payload) else {
        return Ok(None);
    };
    if index.records_block_offset != records_block_offset {
        return Ok(None);
    }
    Ok(Some((index_descriptor, index)))
}

fn read_decoded_tqbn_block_payload(
    file: &mut File,
    descriptor: TqbnBlockDescriptor,
) -> Result<Vec<u8>> {
    let payload = read_tqbn_block_payload(file, descriptor)?;
    decode_block_payload(
        descriptor.block_type,
        descriptor.flags,
        payload,
        MAX_TQBN_BLOCK_PAYLOAD_BYTES,
    )
}

fn read_decoded_tqbn_block_payload_into(
    file: &mut File,
    descriptor: TqbnBlockDescriptor,
    decoded: &mut Vec<u8>,
) -> Result<()> {
    let payload = read_tqbn_block_payload(file, descriptor)?;
    decode_block_payload_into(
        descriptor.block_type,
        descriptor.flags,
        payload,
        MAX_TQBN_BLOCK_PAYLOAD_BYTES,
        decoded,
    )
}

fn decode_blocks_streaming_with(
    file: &mut File,
    mut decode_records: impl FnMut(&[u8]) -> Result<()>,
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
                decode_records(&records)?;
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

fn read_tqbn_block_descriptor_at(
    file: &mut File,
    block_start: u64,
    file_len: u64,
) -> Result<TqbnBlockDescriptor> {
    if block_start >= file_len {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block offset {block_start} is outside file length {file_len}"
        )));
    }
    file.seek(SeekFrom::Start(block_start))?;
    let header = read_block_header(file)?.ok_or_else(|| {
        DataError::InvalidResponse(format!(
            "TQBN block header is missing at offset {block_start}"
        ))
    })?;
    if &header[0..4] != b"TQBB" {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block magic mismatch at offset {block_start}"
        )));
    }
    let records_len_u64 = u64::from_le_bytes([
        header[8], header[9], header[10], header[11], header[12], header[13], header[14],
        header[15],
    ]);
    let payload_len = usize::try_from(records_len_u64).map_err(|_| {
        DataError::InvalidResponse(format!(
            "TQBN block records length {records_len_u64} does not fit in usize"
        ))
    })?;
    if payload_len > MAX_TQBN_BLOCK_PAYLOAD_BYTES {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block records length {payload_len} exceeds max {MAX_TQBN_BLOCK_PAYLOAD_BYTES}"
        )));
    }
    let payload_offset = block_start
        .checked_add(TQBN_BLOCK_HEADER_LEN as u64)
        .ok_or_else(|| DataError::InvalidResponse("TQBN block offset overflow".to_string()))?;
    let end_offset = payload_offset.checked_add(records_len_u64).ok_or_else(|| {
        DataError::InvalidResponse("TQBN block records length overflow".to_string())
    })?;
    if end_offset > file_len {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block payload is truncated at offset {block_start}: requires {payload_len} bytes"
        )));
    }
    let payload_checksum = u64::from_le_bytes([
        header[16], header[17], header[18], header[19], header[20], header[21], header[22],
        header[23],
    ]);
    Ok(TqbnBlockDescriptor {
        block_type: header[4],
        flags: header[5],
        payload_offset,
        payload_len,
        payload_checksum,
        end_offset,
    })
}

fn read_tqbn_block_payload(file: &mut File, descriptor: TqbnBlockDescriptor) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(descriptor.payload_offset))?;
    let mut payload = vec![0_u8; descriptor.payload_len];
    read_exact_tqbn(file, &mut payload, || {
        format!(
            "TQBN block payload is truncated at offset {}: requires {} bytes",
            descriptor
                .payload_offset
                .saturating_sub(TQBN_BLOCK_HEADER_LEN as u64),
            descriptor.payload_len
        )
    })?;
    let actual_checksum = checksum64_fnv1a(&payload);
    if actual_checksum != descriptor.payload_checksum {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block checksum mismatch at offset {}: expected {}, got {actual_checksum}",
            descriptor
                .payload_offset
                .saturating_sub(TQBN_BLOCK_HEADER_LEN as u64),
            descriptor.payload_checksum
        )));
    }
    Ok(payload)
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

fn decode_history_row_record(
    decoded: DecodedTqbnRecord<'_>,
    kind: HistorySeriesKind,
) -> Result<(Option<HistorySeriesRow>, usize)> {
    let (row, record_size) = match decoded {
        DecodedTqbnRecord::Kline {
            bytes: record,
            record_size,
        } => {
            let row = if matches!(kind, HistorySeriesKind::Kline { .. }) {
                let record = read_kline_record_bytes(record)?;
                Some(HistorySeriesRow::Kline(decode_kline_record(&record)?))
            } else {
                None
            };
            (row, record_size)
        }
        DecodedTqbnRecord::Tick1 {
            bytes: record,
            record_size,
        } => {
            let row = if kind == HistorySeriesKind::Tick {
                let record = read_tick1_record_bytes(record)?;
                Some(HistorySeriesRow::Tick(decode_tick1_record(&record)?))
            } else {
                None
            };
            (row, record_size)
        }
        DecodedTqbnRecord::Tick5 {
            bytes: record,
            record_size,
        } => {
            let row = if kind == HistorySeriesKind::Tick {
                let record = read_tick5_record_bytes(record)?;
                Some(HistorySeriesRow::Tick(decode_tick5_record(&record)?))
            } else {
                None
            };
            (row, record_size)
        }
        DecodedTqbnRecord::Coverage { record_size, .. }
        | DecodedTqbnRecord::ProvisionalCoverage { record_size, .. }
        | DecodedTqbnRecord::Unknown { record_size, .. } => (None, record_size),
    };
    Ok((row, record_size))
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
            DecodedTqbnRecord::ProvisionalCoverage {
                bytes: record,
                record_size,
            } => {
                state
                    .provisional
                    .push(decode_provisional_coverage_record(record)?);
                record_size
            }
            DecodedTqbnRecord::Unknown { record_size, .. } => record_size,
        };
        bytes = &bytes[record_size..];
    }
    Ok(())
}

fn decode_coverage_records_block(bytes: &[u8], coverage: &mut Vec<(i64, i64)>) -> Result<()> {
    let mut indexed = TqbnIndexedCoverage::default();
    decode_checkpoint_records_block(bytes, &mut indexed)?;
    coverage.extend(indexed.coverage);
    Ok(())
}

fn decode_checkpoint_records_block(
    mut bytes: &[u8],
    indexed: &mut TqbnIndexedCoverage,
) -> Result<()> {
    while !bytes.is_empty() {
        let decoded = decode_one_record(bytes)?;
        let record_size = match decoded {
            DecodedTqbnRecord::Coverage {
                bytes: record,
                record_size,
            } => {
                let record = read_coverage_record_bytes(record)?;
                if record.range_start_ns < record.range_end_ns {
                    indexed
                        .coverage
                        .push((record.range_start_ns, record.range_end_ns));
                }
                record_size
            }
            DecodedTqbnRecord::ProvisionalCoverage {
                bytes: record,
                record_size,
            } => {
                indexed
                    .provisional
                    .push(decode_provisional_coverage_record(record)?);
                record_size
            }
            DecodedTqbnRecord::Kline { record_size, .. }
            | DecodedTqbnRecord::Tick1 { record_size, .. }
            | DecodedTqbnRecord::Tick5 { record_size, .. }
            | DecodedTqbnRecord::Unknown { record_size, .. } => record_size,
        };
        bytes = &bytes[record_size..];
    }
    Ok(())
}

fn decode_provisional_coverage_record(bytes: &[u8]) -> Result<TqbnProvisionalCoverage> {
    let record = read_provisional_coverage_record_bytes(bytes)?;
    if record.range_start_ns >= record.complete_through_ns {
        return Err(DataError::InvalidResponse(
            "TQBN provisional range start must be less than complete-through".to_string(),
        ));
    }
    if record.complete_through_ns > record.as_of_ns {
        return Err(DataError::InvalidResponse(
            "TQBN provisional complete-through exceeds as-of time".to_string(),
        ));
    }
    let start_day = trading_day_for_timestamp_ns(record.range_start_ns)?;
    let complete_day = trading_day_for_timestamp_ns(record.complete_through_ns.saturating_sub(1))?;
    let as_of_day = trading_day_for_timestamp_ns(record.as_of_ns.saturating_sub(1))?;
    if start_day != complete_day || start_day != as_of_day {
        return Err(DataError::InvalidResponse(
            "TQBN provisional coverage crosses a trading-day partition".to_string(),
        ));
    }
    if record.has_id_range > 1 || record.reserved != [0; 7] {
        return Err(DataError::InvalidResponse(
            "TQBN provisional flags are invalid".to_string(),
        ));
    }
    let rows = usize::try_from(record.rows).map_err(|_| {
        DataError::InvalidResponse("TQBN provisional row count exceeds usize::MAX".to_string())
    })?;
    let id_range = (record.has_id_range == 1).then_some((record.id_start, record.id_end));
    if id_range.is_some_and(|(start, end)| start > end) {
        return Err(DataError::InvalidResponse(
            "TQBN provisional id range is invalid".to_string(),
        ));
    }
    Ok(TqbnProvisionalCoverage {
        range_start_ns: record.range_start_ns,
        complete_through_ns: record.complete_through_ns,
        as_of_ns: record.as_of_ns,
        rows,
        id_range,
    })
}

fn rows_for_request(
    rows: Vec<HistorySeriesRow>,
    kind: HistorySeriesKind,
    range_start_ns: i64,
    range_end_ns: i64,
) -> Vec<HistorySeriesRow> {
    if history_rows_are_strictly_increasing(&rows, kind) {
        return rows
            .into_iter()
            .filter(|row| history_row_in_datetime_range(row, range_start_ns, range_end_ns))
            .collect();
    }
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

fn history_rows_are_strictly_increasing(
    rows: &[HistorySeriesRow],
    kind: HistorySeriesKind,
) -> bool {
    let mut previous_id = None;
    for row in rows {
        let Some(row_id) = history_row_id(row, kind) else {
            return false;
        };
        if previous_id.is_some_and(|previous_id| row_id <= previous_id) {
            return false;
        }
        previous_id = Some(row_id);
    }
    true
}

fn history_row_id(row: &HistorySeriesRow, kind: HistorySeriesKind) -> Option<i64> {
    match (kind, row) {
        (HistorySeriesKind::Kline { .. }, HistorySeriesRow::Kline(row)) => Some(row.id),
        (HistorySeriesKind::Tick, HistorySeriesRow::Tick(row)) => Some(row.id),
        _ => None,
    }
}

fn history_row_in_datetime_range(row: &HistorySeriesRow, start_ns: i64, end_ns: i64) -> bool {
    match row {
        HistorySeriesRow::Kline(row) => row.datetime >= start_ns && row.datetime < end_ns,
        HistorySeriesRow::Tick(row) => row.datetime >= start_ns && row.datetime < end_ns,
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
    let provisional = compact_provisional_coverage(parsed.state.provisional, &coverage);
    let temp_path = compact_temp_path(path)?;
    let result = (|| -> Result<()> {
        let mut file = File::create(&temp_path)?;
        file.write_all(&prefix.bytes)?;
        let mut coverage_index_offset = append_tqbn_coverage_index_root(&mut file)?;
        write_compacted_rows_block(&mut file, symbol, kind, &rows)?;
        let id_range = rows_id_range(&rows)?;
        for (start_ns, end_ns) in coverage {
            coverage_index_offset = append_coverage_block(
                &mut file,
                Some(coverage_index_offset),
                start_ns,
                end_ns,
                rows.len(),
                id_range,
            )?
            .ok_or(DataError::InvalidState(
                "TQBN compaction lost its coverage index root",
            ))?;
        }
        for checkpoint in provisional {
            coverage_index_offset = append_provisional_block(
                &mut file,
                Some(coverage_index_offset),
                &HistorySeriesProvisionalCoverage {
                    symbol: symbol.to_string(),
                    kind,
                    range_start_ns: checkpoint.range_start_ns,
                    complete_through_ns: checkpoint.complete_through_ns,
                    as_of_ns: checkpoint.as_of_ns,
                    rows: checkpoint.rows,
                    id_range: checkpoint.id_range,
                },
            )?
            .ok_or(DataError::InvalidState(
                "TQBN compaction lost its coverage index root",
            ))?;
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

fn compact_provisional_coverage(
    provisional: Vec<TqbnProvisionalCoverage>,
    final_coverage: &[(i64, i64)],
) -> Vec<TqbnProvisionalCoverage> {
    let mut by_start = BTreeMap::<i64, TqbnProvisionalCoverage>::new();
    for checkpoint in provisional {
        if super::rangeset_difference(
            &[(checkpoint.range_start_ns, checkpoint.complete_through_ns)],
            final_coverage,
        )
        .is_empty()
        {
            continue;
        }
        let replace = by_start
            .get(&checkpoint.range_start_ns)
            .is_none_or(|current| {
                (checkpoint.complete_through_ns, checkpoint.as_of_ns)
                    > (current.complete_through_ns, current.as_of_ns)
            });
        if replace {
            by_start.insert(checkpoint.range_start_ns, checkpoint);
        }
    }
    by_start.into_values().collect()
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
            append_compacted_rows_block(
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
            append_compacted_rows_block(
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
    if history_rows_are_strictly_increasing(&rows, kind) {
        return rows;
    }
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

fn validate_provisional_coverage(commit: &HistorySeriesProvisionalCoverage) -> Result<()> {
    if commit.kind != HistorySeriesKind::Tick {
        return Err(DataError::InvalidState(
            "provisional history coverage is only supported for ticks",
        ));
    }
    validate_coverage_range(commit.range_start_ns, commit.complete_through_ns)?;
    if commit.complete_through_ns > commit.as_of_ns {
        return Err(DataError::InvalidState(
            "history provisional coverage cannot extend beyond its as-of time",
        ));
    }
    let start_day = trading_day_for_timestamp_ns(commit.range_start_ns)?;
    let complete_day = trading_day_for_timestamp_ns(commit.complete_through_ns.saturating_sub(1))?;
    let as_of_day = trading_day_for_timestamp_ns(commit.as_of_ns.saturating_sub(1))?;
    if start_day != complete_day || start_day != as_of_day {
        return Err(DataError::InvalidState(
            "history provisional coverage must stay within one TQBN trading-day partition",
        ));
    }
    if commit.id_range.is_some_and(|(start, end)| start > end) {
        return Err(DataError::InvalidState(
            "history provisional coverage id range is invalid",
        ));
    }
    Ok(())
}

fn select_provisional_checkpoint(
    provisional: Vec<TqbnProvisionalCoverage>,
    final_coverage: &[(i64, i64)],
    range_start_ns: i64,
    range_end_ns: i64,
) -> Option<TqbnProvisionalCoverage> {
    provisional
        .into_iter()
        .filter(|checkpoint| {
            checkpoint.range_start_ns <= range_start_ns
                && checkpoint.complete_through_ns > range_start_ns
                && checkpoint.range_start_ns < range_end_ns
        })
        .filter(|checkpoint| {
            !super::rangeset_difference(
                &[(checkpoint.range_start_ns, checkpoint.complete_through_ns)],
                final_coverage,
            )
            .is_empty()
        })
        .max_by_key(|checkpoint| (checkpoint.complete_through_ns, checkpoint.as_of_ns))
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

fn write_provisional_coverage_record_bytes(
    out: &mut Vec<u8>,
    record: &TqbnProvisionalCoverageRecordV1,
) -> Result<()> {
    write_header(out, &record.hd);
    write_i64_fields(
        out,
        &[
            record.range_start_ns,
            record.complete_through_ns,
            record.as_of_ns,
            i64::from_le_bytes(record.rows.to_le_bytes()),
            record.id_start,
            record.id_end,
        ],
    );
    out.push(record.has_id_range);
    out.extend_from_slice(&record.reserved);
    validate_record_len::<TqbnProvisionalCoverageRecordV1>(record.hd)
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

fn read_provisional_coverage_record_bytes(bytes: &[u8]) -> Result<TqbnProvisionalCoverageRecordV1> {
    let mut reader = RecordReader::new(bytes);
    let hd = reader.read_header()?;
    let range_start_ns = reader.read_i64()?;
    let complete_through_ns = reader.read_i64()?;
    let as_of_ns = reader.read_i64()?;
    let rows = u64::from_le_bytes(reader.read_i64()?.to_le_bytes());
    let id_start = reader.read_i64()?;
    let id_end = reader.read_i64()?;
    let has_id_range = reader.read_u8()?;
    let reserved = reader.read_u8_array::<7>()?;
    Ok(TqbnProvisionalCoverageRecordV1 {
        hd,
        range_start_ns,
        complete_through_ns,
        as_of_ns,
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
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::NaiveDate;
    use tqsdk_core::{Kline, Tick};

    use crate::client::{KlineDataSeriesRequest, TickDataSeriesRequest};
    use crate::error::DataError;
    use crate::history_series_cache::{
        HistorySeriesCache, HistorySeriesCacheFileStatus, HistorySeriesCoverageCommit,
        HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesProvisionalCoverage,
        HistorySeriesReader, HistorySeriesRow, HistorySeriesStore, HistorySeriesWriteRows,
        HistorySeriesWriteSegment,
    };

    use super::codec::{TqbnBlockType, decode_blocks, encode_block, encode_file_prefix};
    use super::{
        TQBN_BLOCK_HEADER_LEN, TQBN_COVERAGE_INDEX_PAYLOAD_LEN, TqbnHistoryStore, TqbnMetadata,
        TqbnReader, coverage_record, encode_metadata, history_row_id,
        history_rows_are_strictly_increasing, parse_tqbn_coverage_file,
        read_and_validate_tqbn_prefix, read_tqbn_block_descriptor_at, read_tqbn_coverage_index_at,
        rows_for_request, tick_level_depth, trading_day_range, try_parse_tqbn_coverage_index_chain,
        write_coverage_record_bytes,
    };

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
    fn tqbn_range_reader_skips_out_of_range_indexed_records_blocks() {
        let cache = tqbn_cache("range_reader_skips_unrelated_block");
        cache
            .write_tick_range(SYMBOL, 1_000, 2_000, &[tick5(1, 1_000, 618.5, 623.5)])
            .unwrap();
        cache
            .write_tick_range(SYMBOL, 3_000, 4_000, &[tick5(2, 3_000, 619.5, 624.5)])
            .unwrap();

        let path = cache
            .root_dir()
            .join("series")
            .join("19700101")
            .join("tick")
            .join("SHFE.rb2601.tqbn");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, HistorySeriesKind::Tick).unwrap();
        let file_len = file.metadata().unwrap().len();
        let mut offset = first_block_offset as u64;
        let payload_offset = loop {
            let descriptor = read_tqbn_block_descriptor_at(&mut file, offset, file_len).unwrap();
            if descriptor.block_type == TqbnBlockType::Records as u8 {
                break descriptor.payload_offset;
            }
            offset = descriptor.end_offset;
        };
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let series = cache
            .read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, 3_000, 4_000))
            .unwrap();

        assert_eq!(series.rows().len(), 1);
        assert_eq!(series.rows()[0].id, 2);
    }

    #[test]
    fn tqbn_range_reader_rejects_unknown_flags_in_skipped_block() {
        let cache = tqbn_cache("range_reader_validates_skipped_block_flags");
        cache
            .write_tick_range(SYMBOL, 1_000, 2_000, &[tick5(1, 1_000, 618.5, 623.5)])
            .unwrap();
        cache
            .write_tick_range(SYMBOL, 3_000, 4_000, &[tick5(2, 3_000, 619.5, 624.5)])
            .unwrap();

        let path = cache
            .root_dir()
            .join("series")
            .join("19700101")
            .join("tick")
            .join("SHFE.rb2601.tqbn");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, HistorySeriesKind::Tick).unwrap();
        let file_len = file.metadata().unwrap().len();
        let mut offset = first_block_offset as u64;
        let block_offset = loop {
            let descriptor = read_tqbn_block_descriptor_at(&mut file, offset, file_len).unwrap();
            if descriptor.block_type == TqbnBlockType::Records as u8 {
                break offset;
            }
            offset = descriptor.end_offset;
        };
        file.seek(SeekFrom::Start(block_offset + 5)).unwrap();
        file.write_all(&[0x80]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let error = cache
            .read_tick_data_series(TickDataSeriesRequest::new(SYMBOL, 3_000, 4_000))
            .unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("unsupported bits"))
        );
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

        store
            .append_coverage(HistorySeriesCoverageCommit {
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
        let coverage = store
            .coverage(HistorySeriesCoverageRequest {
                symbol: SYMBOL.to_string(),
                kind: HistorySeriesKind::Kline {
                    duration_ns: DURATION_NS,
                },
                range_start_ns: 1_000,
                range_end_ns: 3_000,
            })
            .unwrap();

        assert!(coverage.is_complete());
        assert_eq!(coverage.cached_ranges, vec![(1_000, 3_000)]);
    }

    #[test]
    fn tqbn_provisional_coverage_rejects_cross_partition_at_store_boundary() {
        let store = tqbn_store("provisional_cross_partition");
        let (_, range_start_ns, range_end_ns) =
            trading_day_range(NaiveDate::from_ymd_opt(2026, 7, 24).unwrap()).unwrap();

        let error = store
            .append_provisional(HistorySeriesProvisionalCoverage {
                symbol: SYMBOL.to_string(),
                kind: HistorySeriesKind::Tick,
                range_start_ns,
                complete_through_ns: range_end_ns,
                as_of_ns: range_end_ns.saturating_add(1),
                rows: 0,
                id_range: None,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            DataError::InvalidState(
                "history provisional coverage must stay within one TQBN trading-day partition"
            )
        ));
    }

    #[test]
    fn tqbn_coverage_index_chain_round_trips_multiple_commits() {
        let store = tqbn_store("coverage_index_chain");
        let kind = HistorySeriesKind::Tick;
        for (start_ns, end_ns) in [(1_000, 2_000), (2_000, 3_000)] {
            store
                .append_coverage(HistorySeriesCoverageCommit {
                    symbol: SYMBOL.to_string(),
                    kind,
                    range_start_ns: start_ns,
                    range_end_ns: end_ns,
                    rows: 0,
                    id_range: None,
                })
                .unwrap();
        }

        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut file = File::open(path).unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, kind).unwrap();

        assert_eq!(
            try_parse_tqbn_coverage_index_chain(&mut file, first_block_offset as u64).unwrap(),
            Some(vec![(1_000, 2_000), (2_000, 3_000)])
        );
    }

    #[test]
    fn tqbn_coverage_index_falls_back_after_trailing_rows() {
        let store = tqbn_store("coverage_index_trailing_rows");
        let kind = HistorySeriesKind::Tick;
        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind,
                declared_range_ns: Some((1_000, 2_000)),
                rows: HistorySeriesWriteRows::Ticks(&[tick5(1, 1_000, 618.5, 623.5)]),
            })
            .unwrap();
        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&[tick5(2, 1_500, 618.6, 623.6)]),
            })
            .unwrap();

        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut file = File::open(&path).unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, kind).unwrap();
        assert_eq!(
            try_parse_tqbn_coverage_index_chain(&mut file, first_block_offset as u64).unwrap(),
            None
        );
        assert_eq!(
            parse_tqbn_coverage_file(&path, SYMBOL, kind).unwrap(),
            vec![(1_000, 2_000)]
        );
    }

    #[test]
    fn tqbn_coverage_index_rejects_corrupt_referenced_coverage_block() {
        let store = tqbn_store("coverage_index_corrupt_block");
        let kind = HistorySeriesKind::Tick;
        store
            .append_coverage(HistorySeriesCoverageCommit {
                symbol: SYMBOL.to_string(),
                kind,
                range_start_ns: 1_000,
                range_end_ns: 2_000,
                rows: 0,
                id_range: None,
            })
            .unwrap();

        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut file = File::open(&path).unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, kind).unwrap();
        let file_len = file.metadata().unwrap().len();
        let tail_offset =
            file_len - (TQBN_BLOCK_HEADER_LEN + TQBN_COVERAGE_INDEX_PAYLOAD_LEN) as u64;
        let index = read_tqbn_coverage_index_at(&mut file, tail_offset, file_len)
            .unwrap()
            .unwrap();
        drop(file);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(
            index.coverage_block_offset + TQBN_BLOCK_HEADER_LEN as u64,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_data().unwrap();
        drop(file);

        let mut file = File::open(&path).unwrap();
        assert_eq!(
            try_parse_tqbn_coverage_index_chain(&mut file, first_block_offset as u64).unwrap(),
            None
        );
        let error = parse_tqbn_coverage_file(&path, SYMBOL, kind).unwrap_err();
        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("checksum mismatch"))
        );
    }

    #[test]
    fn tqbn_coverage_index_falls_back_for_legacy_file_and_compaction_upgrades_it() {
        let store = tqbn_store("coverage_index_legacy");
        let kind = HistorySeriesKind::Tick;
        let path = store.partition_series_path("19700101", SYMBOL, kind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let record = coverage_record(1_000, 2_000, 0, None).unwrap();
        let mut records = Vec::new();
        write_coverage_record_bytes(&mut records, &record).unwrap();
        let mut bytes = valid_tick_prefix().bytes;
        bytes.extend_from_slice(&encode_block(TqbnBlockType::Records, &records));
        std::fs::write(&path, bytes).unwrap();

        let mut file = File::open(&path).unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, kind).unwrap();
        assert_eq!(
            try_parse_tqbn_coverage_index_chain(&mut file, first_block_offset as u64).unwrap(),
            None
        );
        assert_eq!(
            parse_tqbn_coverage_file(&path, SYMBOL, kind).unwrap(),
            vec![(1_000, 2_000)]
        );

        store.compact_series(SYMBOL, kind).unwrap();
        let mut file = File::open(&path).unwrap();
        let (_, first_block_offset) =
            read_and_validate_tqbn_prefix(&mut file, SYMBOL, kind).unwrap();
        assert_eq!(
            try_parse_tqbn_coverage_index_chain(&mut file, first_block_offset as u64).unwrap(),
            Some(vec![(1_000, 2_000)])
        );
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
    fn tqbn_reader_fast_path_requires_strictly_increasing_ids() {
        let kind = HistorySeriesKind::Tick;
        let ordered = vec![
            HistorySeriesRow::Tick(tick5(1, 1_000, 618.5, 623.5)),
            HistorySeriesRow::Tick(tick5(2, 2_000, 618.6, 623.6)),
        ];
        assert!(history_rows_are_strictly_increasing(&ordered, kind));

        let selected = rows_for_request(ordered, kind, 1_000, 3_000);
        assert_eq!(
            selected
                .iter()
                .map(|row| match row {
                    HistorySeriesRow::Tick(row) => row.id,
                    HistorySeriesRow::Kline(_) => unreachable!("tick reader returned kline"),
                })
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let duplicate = vec![
            HistorySeriesRow::Tick(tick5(1, 1_000, 618.5, 623.5)),
            HistorySeriesRow::Tick(tick5(1, 2_000, 618.6, 623.6)),
        ];
        assert!(!history_rows_are_strictly_increasing(&duplicate, kind));
    }

    #[test]
    fn tqbn_reader_streams_strictly_increasing_indexed_blocks() {
        let store = tqbn_store("reader_streams_increasing_blocks");
        let kind = HistorySeriesKind::Tick;
        for row in [tick5(1, 1_000, 618.5, 623.5), tick5(2, 2_000, 618.6, 623.6)] {
            store
                .write_segment(HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(&row)),
                })
                .unwrap();
        }
        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut reader = TqbnReader {
            paths: vec![path],
            path_index: 0,
            symbol: SYMBOL.to_string(),
            kind,
            range_start_ns: 1_000,
            range_end_ns: 3_000,
            rows: Vec::new().into_iter(),
            partition: None,
            spare_records: Vec::new(),
        };

        let first = reader.next_row().unwrap().unwrap();
        assert_eq!(history_row_id(&first, kind), Some(1));
        assert_eq!(
            reader.rows.len(),
            0,
            "strictly increasing partitions must retain only the current records block"
        );
        let second = reader.next_row().unwrap().unwrap();
        assert_eq!(history_row_id(&second, kind), Some(2));
        assert!(reader.next_row().unwrap().is_none());
    }

    #[test]
    fn tqbn_reader_streams_overlapping_blocks_with_last_write_wins() {
        let store = tqbn_store("reader_streams_overlapping_blocks");
        let kind = HistorySeriesKind::Tick;
        let first_block = [
            tick5(1, 1_000, 618.5, 623.5),
            tick5(2, 2_000, 618.6, 623.6),
            tick5(3, 3_000, 618.7, 623.7),
        ];
        let second_block = [
            tick5(2, 2_000, 628.6, 633.6),
            tick5(3, 3_000, 628.7, 633.7),
            tick5(4, 4_000, 628.8, 633.8),
        ];
        for rows in [&first_block[..], &second_block[..]] {
            store
                .write_segment(HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(rows),
                })
                .unwrap();
        }

        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut reader = TqbnReader {
            paths: vec![path],
            path_index: 0,
            symbol: SYMBOL.to_string(),
            kind,
            range_start_ns: 1_000,
            range_end_ns: 5_000,
            rows: Vec::new().into_iter(),
            partition: None,
            spare_records: Vec::new(),
        };

        let first = reader.next_row().unwrap().unwrap();
        assert_eq!(history_row_id(&first, kind), Some(1));
        assert!(
            reader.partition.is_some(),
            "overlapping ordered blocks must stay on the streaming merge path"
        );
        assert_eq!(
            reader.rows.len(),
            0,
            "streaming merge must not materialize future rows"
        );

        let mut rows = vec![first];
        while let Some(row) = reader.next_row().unwrap() {
            rows.push(row);
        }
        let ticks = rows
            .into_iter()
            .map(|row| match row {
                HistorySeriesRow::Tick(row) => row,
                HistorySeriesRow::Kline(_) => unreachable!("tick reader returned kline"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ticks.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(ticks[1].last_price, 628.6);
        assert_eq!(ticks[2].last_price, 628.7);
    }

    #[test]
    fn tqbn_reader_reuses_decoded_records_buffer_across_partitions() {
        let store = tqbn_store("reader_reuses_records_across_partitions");
        let kind = HistorySeriesKind::Tick;
        let second_day_ns = 86_400 * super::NANOS_PER_SECOND + 1_000;
        for row in [
            tick5(1, 1_000, 618.5, 623.5),
            tick5(2, second_day_ns, 618.6, 623.6),
        ] {
            store
                .write_segment(HistorySeriesWriteSegment {
                    symbol: SYMBOL,
                    kind,
                    declared_range_ns: None,
                    rows: HistorySeriesWriteRows::Ticks(std::slice::from_ref(&row)),
                })
                .unwrap();
        }

        let mut reader = TqbnReader {
            paths: vec![
                store.partition_series_path("19700101", SYMBOL, kind),
                store.partition_series_path("19700102", SYMBOL, kind),
            ],
            path_index: 0,
            symbol: SYMBOL.to_string(),
            kind,
            range_start_ns: 0,
            range_end_ns: second_day_ns + 1_000,
            rows: Vec::new().into_iter(),
            partition: None,
            spare_records: Vec::new(),
        };

        assert_eq!(
            history_row_id(&reader.next_row().unwrap().unwrap(), kind),
            Some(1)
        );
        let first_buffer = reader
            .partition
            .as_ref()
            .expect("first partition remains open")
            .spare_records
            .as_ptr();

        assert_eq!(
            history_row_id(&reader.next_row().unwrap().unwrap(), kind),
            Some(2)
        );
        let second_buffer = reader
            .partition
            .as_ref()
            .expect("second partition remains open")
            .spare_records
            .as_ptr();
        assert_eq!(second_buffer, first_buffer);
        assert!(reader.next_row().unwrap().is_none());
    }

    #[test]
    fn tqbn_reader_materializes_out_of_order_indexed_blocks() {
        let store = tqbn_store("reader_materializes_out_of_order_blocks");
        let kind = HistorySeriesKind::Tick;
        let rows = [tick5(2, 1_000, 618.5, 623.5), tick5(1, 2_000, 618.6, 623.6)];
        store
            .write_segment(HistorySeriesWriteSegment {
                symbol: SYMBOL,
                kind,
                declared_range_ns: None,
                rows: HistorySeriesWriteRows::Ticks(&rows),
            })
            .unwrap();
        let path = store.partition_series_path("19700101", SYMBOL, kind);
        let mut reader = TqbnReader {
            paths: vec![path],
            path_index: 0,
            symbol: SYMBOL.to_string(),
            kind,
            range_start_ns: 1_000,
            range_end_ns: 3_000,
            rows: Vec::new().into_iter(),
            partition: None,
            spare_records: Vec::new(),
        };

        let first = reader.next_row().unwrap().unwrap();
        assert_eq!(history_row_id(&first, kind), Some(1));
        assert_eq!(
            reader.rows.len(),
            1,
            "out-of-order partitions must retain materialized last-write-wins rows"
        );
        let second = reader.next_row().unwrap().unwrap();
        assert_eq!(history_row_id(&second, kind), Some(2));
        assert!(reader.next_row().unwrap().is_none());
    }

    #[test]
    fn tqbn_reader_slow_path_keeps_last_write_and_id_order() {
        let kind = HistorySeriesKind::Tick;
        let rows = vec![
            HistorySeriesRow::Tick(tick5(3, 3_000, 618.5, 623.5)),
            HistorySeriesRow::Tick(tick5(1, 1_000, 618.6, 623.6)),
            HistorySeriesRow::Tick(tick5(3, 3_000, 618.7, 623.7)),
            HistorySeriesRow::Tick(tick5(2, 2_000, 618.8, 623.8)),
        ];
        assert!(!history_rows_are_strictly_increasing(&rows, kind));

        let rows = rows_for_request(rows, kind, 1_000, 4_000);
        let ticks = rows
            .into_iter()
            .map(|row| match row {
                HistorySeriesRow::Tick(row) => row,
                HistorySeriesRow::Kline(_) => unreachable!("tick reader returned kline"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ticks.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(ticks[2].last_price, 618.7);
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
