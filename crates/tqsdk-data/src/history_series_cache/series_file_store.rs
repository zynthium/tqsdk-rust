use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::storage::{SeriesLayout, write_kline_row, write_tick_row};
use super::{
    HISTORY_SERIES_CACHE_SCHEMA_VERSION, HistorySeriesCacheMaintenanceReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageCommit, HistorySeriesCoverageReport,
    HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesReadRequest, HistorySeriesReader,
    HistorySeriesRow, HistorySeriesSegmentReport, HistorySeriesStore, HistorySeriesWriteRows,
    HistorySeriesWriteSegment, SERIES_FILE_HISTORY_SERIES_FORMAT_ID,
};

const ROOT_DIR_NAME: &str = "series";
const TICK_FILE_NAME: &str = "tick.tqseries";
const FILE_MAGIC: &[u8; 8] = b"TQHSF1\0\0";
const CHUNK_MAGIC: &[u8; 4] = b"TQSC";
const CHUNK_HEADER_LEN: usize = 24;

#[derive(Debug, Clone)]
pub(super) struct SeriesFileHistoryStore {
    root_dir: Arc<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkKind {
    Meta = 1,
    Rows = 2,
    Coverage = 3,
}

#[derive(Debug, Default)]
struct SeriesFileState {
    rows: Vec<HistorySeriesRow>,
    coverage: Vec<(i64, i64)>,
}

struct SeriesFileReader {
    rows: Vec<HistorySeriesRow>,
    index: usize,
}

type SeriesRowIdRange = Option<(i64, i64)>;
type SeriesRowDatetimeRange = Option<(i64, i64)>;
type RowsAppendReport = (usize, SeriesRowIdRange, SeriesRowDatetimeRange);

impl SeriesFileHistoryStore {
    pub(super) fn new(root_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(root_dir.join(ROOT_DIR_NAME))?;
        Ok(Self {
            root_dir: Arc::new(root_dir),
        })
    }

    pub(super) fn series_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.root_dir
            .join(ROOT_DIR_NAME)
            .join(escape_symbol_path_component(symbol))
            .join(if duration_ns == 0 {
                TICK_FILE_NAME.to_string()
            } else {
                format!("{duration_ns}.tqseries")
            })
    }
}

impl HistorySeriesStore for SeriesFileHistoryStore {
    fn format_id(&self) -> &'static str {
        SERIES_FILE_HISTORY_SERIES_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        HISTORY_SERIES_CACHE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    fn uses_mmap_backend(&self) -> bool {
        false
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        super::empty_scan_report(self.root_dir.as_path())
    }

    fn enforce_limits(
        &self,
        _max_bytes: Option<u64>,
        _retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        Ok(HistorySeriesCacheMaintenanceReport::default())
    }

    fn coverage(
        &self,
        request: HistorySeriesCoverageRequest,
    ) -> Result<HistorySeriesCoverageReport> {
        let path = self.series_path(request.symbol.as_str(), request.kind.duration_ns());
        let state = scan_series_file(&path, request.kind)?;
        let cached_ranges = super::merge_datetime_ranges(state.coverage);
        let cached_ranges = super::rangeset_intersection(
            &[(request.range_start_ns, request.range_end_ns)],
            &cached_ranges,
        );
        let missing_ranges = super::rangeset_difference(
            &[(request.range_start_ns, request.range_end_ns)],
            &cached_ranges,
        );
        Ok(HistorySeriesCoverageReport {
            cached_ranges,
            missing_ranges,
            symbol: request.symbol,
            kind: request.kind,
            range_start_ns: request.range_start_ns,
            range_end_ns: request.range_end_ns,
        })
    }

    fn write_segment(
        &self,
        segment: HistorySeriesWriteSegment<'_>,
    ) -> Result<HistorySeriesSegmentReport> {
        validate_segment_rows(&segment)?;
        let path = self.series_path(segment.symbol, segment.kind.duration_ns());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        with_exclusive_series_lock(&path, || append_segment_to_file(&path, &segment))
    }

    fn commit_coverage(
        &self,
        commit: HistorySeriesCoverageCommit,
    ) -> Result<HistorySeriesCoverageReport> {
        validate_coverage_range(commit.range_start_ns, commit.range_end_ns)?;
        let symbol = commit.symbol.clone();
        let kind = commit.kind;
        let path = self.series_path(symbol.as_str(), kind.duration_ns());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        with_exclusive_series_lock(&path, || append_coverage_to_file(&path, &commit))?;
        self.coverage(HistorySeriesCoverageRequest {
            symbol,
            kind,
            range_start_ns: commit.range_start_ns,
            range_end_ns: commit.range_end_ns,
        })
    }

    fn open_reader(
        &self,
        request: HistorySeriesReadRequest,
    ) -> Result<Box<dyn HistorySeriesReader>> {
        let path = self.series_path(request.symbol.as_str(), request.kind.duration_ns());
        let state = scan_series_file(&path, request.kind)?;
        let rows = rows_for_request(
            state.rows,
            request.kind,
            request.range_start_ns,
            request.range_end_ns,
        );
        Ok(Box::new(SeriesFileReader { rows, index: 0 }))
    }
}

impl HistorySeriesReader for SeriesFileReader {
    fn next_row(&mut self) -> Result<Option<HistorySeriesRow>> {
        let row = self.rows.get(self.index).cloned();
        if row.is_some() {
            self.index += 1;
        }
        Ok(row)
    }
}

fn with_exclusive_series_lock<T>(path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    lock_file.lock_exclusive()?;
    let result = f();
    let unlock_result = fs2::FileExt::unlock(&lock_file);
    match (result, unlock_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(DataError::from(error)),
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
    ensure_series_file_initialized(&mut file, segment.symbol, segment.kind)?;
    let (rows, id_range, datetime_range) = append_rows_chunk(&mut file, segment)?;
    if let Some((start, end)) = segment.declared_range_ns {
        append_coverage_chunk(&mut file, start, end, rows, id_range)?;
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
    ensure_series_file_initialized(&mut file, commit.symbol.as_str(), commit.kind)?;
    append_coverage_chunk(
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

fn ensure_series_file_initialized(
    file: &mut File,
    symbol: &str,
    kind: HistorySeriesKind,
) -> Result<()> {
    if file.metadata()?.len() == 0 {
        file.write_all(FILE_MAGIC)?;
        let mut payload = Vec::new();
        append_u64(&mut payload, u64::from(HISTORY_SERIES_CACHE_SCHEMA_VERSION));
        append_i64(&mut payload, kind.duration_ns());
        append_string(&mut payload, symbol)?;
        write_chunk(file, ChunkKind::Meta, &payload)?;
        return Ok(());
    }

    let mut magic = [0_u8; FILE_MAGIC.len()];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut magic)?;
    if &magic != FILE_MAGIC {
        return Err(DataError::InvalidResponse(format!(
            "invalid history series-file header: {}",
            symbol
        )));
    }
    file.seek(SeekFrom::End(0))?;
    Ok(())
}

fn append_rows_chunk(
    file: &mut File,
    segment: &HistorySeriesWriteSegment<'_>,
) -> Result<RowsAppendReport> {
    let mut payload = Vec::new();
    match (segment.kind, &segment.rows) {
        (HistorySeriesKind::Kline { duration_ns }, HistorySeriesWriteRows::Klines(rows)) => {
            append_u64(&mut payload, rows.len() as u64);
            for row in *rows {
                write_kline_row(&mut payload, row)?;
            }
            if !rows.is_empty() {
                write_chunk(file, ChunkKind::Rows, &payload)?;
            }
            Ok((
                rows.len(),
                id_range(rows.iter().map(|row| row.id))?,
                datetime_range(rows.iter().map(|row| row.datetime), duration_ns)?,
            ))
        }
        (HistorySeriesKind::Tick, HistorySeriesWriteRows::Ticks(rows)) => {
            append_u64(&mut payload, rows.len() as u64);
            let five_level = tick_rows_use_five_levels(segment.symbol);
            for row in *rows {
                write_tick_row(&mut payload, row, five_level)?;
            }
            if !rows.is_empty() {
                write_chunk(file, ChunkKind::Rows, &payload)?;
            }
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
            "history series write row kind does not match segment kind",
        )),
    }
}

fn append_coverage_chunk(
    file: &mut File,
    start_ns: i64,
    end_ns: i64,
    rows: usize,
    id_range: Option<(i64, i64)>,
) -> Result<()> {
    validate_coverage_range(start_ns, end_ns)?;
    let mut payload = Vec::new();
    append_i64(&mut payload, start_ns);
    append_i64(&mut payload, end_ns);
    append_u64(&mut payload, rows as u64);
    match id_range {
        Some((start_id, end_id)) => {
            payload.push(1);
            append_i64(&mut payload, start_id);
            append_i64(&mut payload, end_id);
        }
        None => {
            payload.push(0);
            append_i64(&mut payload, 0);
            append_i64(&mut payload, 0);
        }
    }
    write_chunk(file, ChunkKind::Coverage, &payload)
}

fn scan_series_file(path: &Path, kind: HistorySeriesKind) -> Result<SeriesFileState> {
    if !path.exists() {
        return Ok(SeriesFileState::default());
    }
    let bytes = fs::read(path)?;
    if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
        return Err(DataError::InvalidResponse(format!(
            "invalid history series-file header: {}",
            path.display()
        )));
    }
    let mut offset = FILE_MAGIC.len();
    let mut state = SeriesFileState::default();
    while offset + CHUNK_HEADER_LEN <= bytes.len() {
        if &bytes[offset..offset + 4] != CHUNK_MAGIC {
            break;
        }
        let kind_byte = bytes[offset + 4];
        let len_offset = offset + 8;
        let payload_len = read_u64_at(&bytes, len_offset)?;
        let payload_len = usize::try_from(payload_len).map_err(|_| {
            DataError::InvalidResponse("history series-file payload is too large".to_string())
        })?;
        let checksum_offset = offset + 16;
        let checksum = read_u64_at(&bytes, checksum_offset)?;
        let payload_start = offset + CHUNK_HEADER_LEN;
        let payload_end = payload_start.saturating_add(payload_len);
        if payload_end > bytes.len() {
            break;
        }
        let payload = &bytes[payload_start..payload_end];
        if checksum64(payload) != checksum {
            break;
        }
        match kind_byte {
            2 => decode_rows_payload(payload, kind, &mut state.rows)?,
            3 => decode_coverage_payload(payload, &mut state.coverage)?,
            _ => {}
        }
        offset = payload_end;
    }
    Ok(state)
}

fn decode_rows_payload(
    payload: &[u8],
    kind: HistorySeriesKind,
    rows: &mut Vec<HistorySeriesRow>,
) -> Result<()> {
    let mut offset = 0;
    let row_count = read_u64(payload, &mut offset)?;
    let row_count = usize::try_from(row_count).map_err(|_| {
        DataError::InvalidResponse("history series-file row count is too large".to_string())
    })?;
    if row_count == 0 {
        return Ok(());
    }
    let remaining = payload.len().saturating_sub(offset);
    match kind {
        HistorySeriesKind::Kline { duration_ns } => {
            let row_size = SeriesLayout::Kline { duration_ns }.row_size();
            validate_rows_payload_width(remaining, row_count, row_size)?;
            for _ in 0..row_count {
                rows.push(HistorySeriesRow::Kline(read_kline(payload, &mut offset)?));
            }
        }
        HistorySeriesKind::Tick => {
            if remaining % row_count != 0 {
                return Err(DataError::InvalidResponse(
                    "history series-file tick rows payload width mismatch".to_string(),
                ));
            }
            let row_size = remaining / row_count;
            let one_level_size = SeriesLayout::Tick { five_level: false }.row_size();
            let five_level_size = SeriesLayout::Tick { five_level: true }.row_size();
            let five_level = if row_size == five_level_size {
                true
            } else if row_size == one_level_size {
                false
            } else {
                return Err(DataError::InvalidResponse(
                    "history series-file tick row width is unknown".to_string(),
                ));
            };
            for _ in 0..row_count {
                rows.push(HistorySeriesRow::Tick(read_tick(
                    payload,
                    &mut offset,
                    five_level,
                )?));
            }
        }
    }
    Ok(())
}

fn decode_coverage_payload(payload: &[u8], coverage: &mut Vec<(i64, i64)>) -> Result<()> {
    let mut offset = 0;
    let start_ns = read_i64_le(payload, &mut offset)?;
    let end_ns = read_i64_le(payload, &mut offset)?;
    if start_ns < end_ns {
        coverage.push((start_ns, end_ns));
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
                "history series write row kind does not match segment kind",
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
                "history series row is outside declared coverage range",
            ));
        }
    }
    Ok(())
}

fn validate_coverage_range(start_ns: i64, end_ns: i64) -> Result<()> {
    if start_ns >= end_ns {
        return Err(DataError::InvalidState(
            "history series declared range start must be less than end",
        ));
    }
    Ok(())
}

fn validate_rows_payload_width(remaining: usize, row_count: usize, row_size: usize) -> Result<()> {
    let expected = row_count.checked_mul(row_size).ok_or_else(|| {
        DataError::InvalidResponse("history series-file rows payload overflow".to_string())
    })?;
    if remaining != expected {
        return Err(DataError::InvalidResponse(
            "history series-file rows payload width mismatch".to_string(),
        ));
    }
    Ok(())
}

fn read_kline(bytes: &[u8], offset: &mut usize) -> Result<Kline> {
    Ok(Kline {
        id: read_i64_ne(bytes, offset)?,
        datetime: read_i64_ne(bytes, offset)?,
        open: read_f64_ne(bytes, offset)?,
        high: read_f64_ne(bytes, offset)?,
        low: read_f64_ne(bytes, offset)?,
        close: read_f64_ne(bytes, offset)?,
        volume: read_f64_ne(bytes, offset)? as i64,
        open_oi: read_f64_ne(bytes, offset)? as i64,
        close_oi: read_f64_ne(bytes, offset)? as i64,
        ..Kline::default()
    })
}

fn read_tick(bytes: &[u8], offset: &mut usize, five_level: bool) -> Result<Tick> {
    let mut row = Tick {
        id: read_i64_ne(bytes, offset)?,
        datetime: read_i64_ne(bytes, offset)?,
        last_price: read_f64_ne(bytes, offset)?,
        highest: read_f64_ne(bytes, offset)?,
        lowest: read_f64_ne(bytes, offset)?,
        average: read_f64_ne(bytes, offset)?,
        volume: read_f64_ne(bytes, offset)? as i64,
        amount: read_f64_ne(bytes, offset)?,
        open_interest: read_f64_ne(bytes, offset)? as i64,
        ..Tick::default()
    };
    read_tick_level(
        bytes,
        offset,
        &mut row.bid_price1,
        &mut row.bid_volume1,
        &mut row.ask_price1,
        &mut row.ask_volume1,
    )?;
    if five_level {
        read_tick_level(
            bytes,
            offset,
            &mut row.bid_price2,
            &mut row.bid_volume2,
            &mut row.ask_price2,
            &mut row.ask_volume2,
        )?;
        read_tick_level(
            bytes,
            offset,
            &mut row.bid_price3,
            &mut row.bid_volume3,
            &mut row.ask_price3,
            &mut row.ask_volume3,
        )?;
        read_tick_level(
            bytes,
            offset,
            &mut row.bid_price4,
            &mut row.bid_volume4,
            &mut row.ask_price4,
            &mut row.ask_volume4,
        )?;
        read_tick_level(
            bytes,
            offset,
            &mut row.bid_price5,
            &mut row.bid_volume5,
            &mut row.ask_price5,
            &mut row.ask_volume5,
        )?;
    }
    Ok(row)
}

fn read_tick_level(
    bytes: &[u8],
    offset: &mut usize,
    bid_price: &mut f64,
    bid_volume: &mut i64,
    ask_price: &mut f64,
    ask_volume: &mut i64,
) -> Result<()> {
    *bid_price = read_f64_ne(bytes, offset)?;
    *bid_volume = read_f64_ne(bytes, offset)? as i64;
    *ask_price = read_f64_ne(bytes, offset)?;
    *ask_volume = read_f64_ne(bytes, offset)? as i64;
    Ok(())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series-file offset overflow".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series-file chunk header is truncated".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    Ok(u64::from_le_bytes(array))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = read_u64_at(bytes, *offset)?;
    *offset += 8;
    Ok(value)
}

fn read_i64_le(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series-file offset overflow".to_string())
    })?;
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series-file payload is truncated".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    *offset = end;
    Ok(i64::from_le_bytes(array))
}

fn read_i64_ne(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series-file offset overflow".to_string())
    })?;
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series-file row is truncated".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    *offset = end;
    Ok(i64::from_ne_bytes(array))
}

fn read_f64_ne(bytes: &[u8], offset: &mut usize) -> Result<f64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series-file offset overflow".to_string())
    })?;
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series-file row is truncated".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    *offset = end;
    Ok(f64::from_ne_bytes(array))
}

fn write_chunk(writer: &mut impl Write, kind: ChunkKind, payload: &[u8]) -> Result<()> {
    writer.write_all(CHUNK_MAGIC)?;
    writer.write_all(&[kind as u8, 0, 0, 0])?;
    writer.write_all(&(payload.len() as u64).to_le_bytes())?;
    writer.write_all(&checksum64(payload).to_le_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn append_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn append_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn append_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|_| {
        DataError::InvalidResponse(
            "history series symbol is too long for series-file metadata".to_string(),
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
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
        .ok_or_else(|| {
            DataError::InvalidResponse("history series segment id overflow".to_string())
        })?;
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
        .ok_or_else(|| {
            DataError::InvalidResponse("history series segment datetime overflow".to_string())
        })?;
    Ok(Some((start, end)))
}

fn tick_rows_use_five_levels(symbol: &str) -> bool {
    matches!(symbol.split('.').next(), Some("SHFE" | "SSE" | "SZSE"))
}

fn escape_symbol_path_component(symbol: &str) -> String {
    symbol.replace('/', "%2F")
}
