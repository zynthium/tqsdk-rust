#![expect(
    dead_code,
    reason = "legacy .tqseries store is retained privately but no longer default"
)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::storage::{SeriesLayout, write_kline_row, write_tick_row};
use super::{
    HistorySeriesCacheFileReport, HistorySeriesCacheMaintenanceReport,
    HistorySeriesCacheScanReport, HistorySeriesCoverageCommit, HistorySeriesCoverageReport,
    HistorySeriesCoverageRequest, HistorySeriesKind, HistorySeriesPurgeReport,
    HistorySeriesReadRequest, HistorySeriesReader, HistorySeriesRow, HistorySeriesSegmentReport,
    HistorySeriesStore, HistorySeriesWriteRows, HistorySeriesWriteSegment,
};

const ROOT_DIR_NAME: &str = "series";
const TICK_FILE_NAME: &str = "tick.tqseries";
const SERIES_FILE_FORMAT_ID: &str = "tqsdk.series-file.v1";
const SERIES_FILE_SCHEMA_VERSION: u32 = 1;
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

#[derive(Debug, Default)]
struct ParsedSeriesFile {
    state: SeriesFileState,
    schema_version: Option<u32>,
    error: Option<String>,
}

struct SeriesFileReader {
    rows: Vec<HistorySeriesRow>,
    index: usize,
}

#[derive(Debug, Clone)]
struct SeriesFileMeta {
    path: PathBuf,
    symbol: String,
    kind: HistorySeriesKind,
    size_bytes: u64,
    modified: SystemTime,
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
        SERIES_FILE_FORMAT_ID
    }

    fn schema_version(&self) -> u32 {
        SERIES_FILE_SCHEMA_VERSION
    }

    fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    fn series_path(&self, symbol: &str, kind: HistorySeriesKind) -> PathBuf {
        self.series_path(symbol, kind.duration_ns())
    }

    fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        let mut files = Vec::new();
        for path in list_series_tree_files(self.root_dir.as_path())? {
            files.push(scan_series_tree_file(self.root_dir.as_path(), path)?);
        }
        files.sort_by(|left, right| left.file_name.cmp(&right.file_name));
        Ok(HistorySeriesCacheScanReport {
            cache_dir: self.root_dir.as_path().to_path_buf(),
            schema_version: SERIES_FILE_SCHEMA_VERSION,
            files,
        })
    }

    fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        let mut report = HistorySeriesCacheMaintenanceReport::default();
        evict_expired_series_files(self.root_dir.as_path(), retention_days, &mut report)?;
        compact_series_files(self.root_dir.as_path())?;
        evict_series_files_by_total_size(self.root_dir.as_path(), max_bytes, &mut report)?;
        Ok(report)
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
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(report),
            Err(error) => return Err(error.into()),
        };
        fs::remove_file(path)?;
        report.removed_files = 1;
        report.removed_bytes = metadata.len();
        Ok(report)
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
        } else if entry.file_type()?.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn list_series_file_metas(root_dir: &Path) -> Result<Vec<SeriesFileMeta>> {
    let mut files = Vec::new();
    for path in list_series_tree_files(root_dir)? {
        let Some((symbol, kind)) = parse_series_tree_path(root_dir, &path) else {
            continue;
        };
        let metadata = fs::metadata(&path)?;
        files.push(SeriesFileMeta {
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

fn scan_series_tree_file(root_dir: &Path, path: PathBuf) -> Result<HistorySeriesCacheFileReport> {
    let metadata = fs::metadata(&path)?;
    let size_bytes = metadata.len();
    let file_name = series_tree_file_name(root_dir, &path);
    let Some((symbol, kind)) = parse_series_tree_path(root_dir, &path) else {
        return Ok(HistorySeriesCacheFileReport {
            path,
            file_name,
            status: super::HistorySeriesCacheFileStatus::Ignored,
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
            status: super::HistorySeriesCacheFileStatus::EmptySegment,
            symbol: Some(symbol.clone()),
            duration_ns: Some(kind.duration_ns()),
            id_range: None,
            row_width: Some(layout_for_symbol_kind(&symbol, kind).row_size()),
            rows: 0,
            size_bytes,
            schema_version: Some(SERIES_FILE_SCHEMA_VERSION),
            error: None,
        });
    }

    match parse_series_file(&path, kind) {
        Ok(parsed) => Ok(HistorySeriesCacheFileReport {
            id_range: rows_id_range(&parsed.state.rows)?,
            row_width: Some(layout_for_symbol_kind(&symbol, kind).row_size()),
            rows: parsed.state.rows.len(),
            status: if parsed.error.is_some() {
                super::HistorySeriesCacheFileStatus::IncompleteWrite
            } else {
                super::HistorySeriesCacheFileStatus::Readable
            },
            schema_version: parsed.schema_version,
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
            status: super::HistorySeriesCacheFileStatus::IncompleteWrite,
            symbol: Some(symbol.clone()),
            duration_ns: Some(kind.duration_ns()),
            id_range: None,
            row_width: Some(layout_for_symbol_kind(&symbol, kind).row_size()),
            rows: 0,
            size_bytes,
            schema_version: None,
            error: Some(error.to_string()),
        }),
    }
}

fn evict_expired_series_files(
    root_dir: &Path,
    retention_days: Option<u64>,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<()> {
    let Some(days) = retention_days else {
        return Ok(());
    };
    let ttl = Duration::from_secs(days.saturating_mul(24 * 60 * 60));
    let cutoff = SystemTime::now().checked_sub(ttl).unwrap_or(UNIX_EPOCH);
    for file in list_series_file_metas(root_dir)? {
        if file.modified <= cutoff {
            remove_series_file(file.path.as_path(), file.size_bytes, report)?;
        }
    }
    Ok(())
}

fn compact_series_files(root_dir: &Path) -> Result<()> {
    for file in list_series_file_metas(root_dir)? {
        if !file.path.exists() {
            continue;
        }
        compact_series_file(file.path.as_path(), file.symbol.as_str(), file.kind)?;
    }
    Ok(())
}

fn evict_series_files_by_total_size(
    root_dir: &Path,
    max_bytes: Option<u64>,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<()> {
    let Some(limit) = max_bytes else {
        return Ok(());
    };
    let mut files = list_series_file_metas(root_dir)?;
    let mut total = files.iter().map(|file| file.size_bytes).sum::<u64>();
    if total <= limit {
        return Ok(());
    }
    files.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.path.cmp(&right.path))
    });
    for file in files {
        if total <= limit {
            break;
        }
        if remove_series_file(file.path.as_path(), file.size_bytes, report)? {
            total = total.saturating_sub(file.size_bytes);
        }
    }
    Ok(())
}

fn remove_series_file(
    path: &Path,
    size_bytes: u64,
    report: &mut HistorySeriesCacheMaintenanceReport,
) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => {
            report.removed_files += 1;
            report.removed_bytes = report.removed_bytes.saturating_add(size_bytes);
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn compact_series_file(path: &Path, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
    with_exclusive_series_lock(path, || compact_series_file_locked(path, symbol, kind))
}

fn compact_series_file_locked(path: &Path, symbol: &str, kind: HistorySeriesKind) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let parsed = parse_series_file(path, kind)?;
    let rows = compact_rows(parsed.state.rows, kind);
    let coverage = super::merge_datetime_ranges(parsed.state.coverage);
    let temp_path = compact_temp_path(path)?;
    {
        let mut file = File::create(&temp_path)?;
        ensure_series_file_initialized(&mut file, symbol, kind)?;
        write_compacted_rows_chunk(&mut file, symbol, kind, &rows)?;
        for (start_ns, end_ns) in coverage {
            append_coverage_chunk(
                &mut file,
                start_ns,
                end_ns,
                rows.len(),
                rows_id_range(&rows)?,
            )?;
        }
        file.flush()?;
        file.sync_all()?;
    }
    fs::rename(temp_path, path)?;
    Ok(())
}

fn write_compacted_rows_chunk(
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
            append_rows_chunk(
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
            append_rows_chunk(
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

fn rows_id_range(rows: &[HistorySeriesRow]) -> Result<Option<(i64, i64)>> {
    id_range(rows.iter().map(|row| match row {
        HistorySeriesRow::Kline(row) => row.id,
        HistorySeriesRow::Tick(row) => row.id,
    }))
}

fn layout_for_symbol_kind(symbol: &str, kind: HistorySeriesKind) -> SeriesLayout {
    match kind {
        HistorySeriesKind::Kline { duration_ns } => SeriesLayout::Kline { duration_ns },
        HistorySeriesKind::Tick => SeriesLayout::Tick {
            five_level: tick_rows_use_five_levels(symbol),
        },
    }
}

fn compact_temp_path(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| {
            DataError::InvalidResponse("history series-file path is invalid".to_string())
        })?
        .to_string_lossy();
    Ok(path.with_file_name(format!("{file_name}.compact")))
}

fn series_tree_file_name(root_dir: &Path, path: &Path) -> String {
    let series_root = root_dir.join(ROOT_DIR_NAME);
    path.strip_prefix(series_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn parse_series_tree_path(root_dir: &Path, path: &Path) -> Option<(String, HistorySeriesKind)> {
    let series_root = root_dir.join(ROOT_DIR_NAME);
    let relative = path.strip_prefix(series_root).ok()?;
    let mut components = relative.components();
    let symbol = components.next()?.as_os_str().to_string_lossy();
    let file_name = components.next()?.as_os_str().to_string_lossy();
    if components.next().is_some() {
        return None;
    }
    let kind = if file_name == TICK_FILE_NAME {
        HistorySeriesKind::Tick
    } else {
        let duration = file_name.strip_suffix(".tqseries")?.parse::<i64>().ok()?;
        HistorySeriesKind::Kline {
            duration_ns: duration,
        }
    };
    Some((unescape_symbol_path_component(&symbol), kind))
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
        append_u64(&mut payload, u64::from(SERIES_FILE_SCHEMA_VERSION));
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
    Ok(parse_series_file(path, kind)?.state)
}

fn parse_series_file(path: &Path, kind: HistorySeriesKind) -> Result<ParsedSeriesFile> {
    if !path.exists() {
        return Ok(ParsedSeriesFile::default());
    }
    let bytes = fs::read(path)?;
    if bytes.len() < FILE_MAGIC.len() || &bytes[..FILE_MAGIC.len()] != FILE_MAGIC {
        return Err(DataError::InvalidResponse(format!(
            "invalid history series-file header: {}",
            path.display()
        )));
    }
    let mut offset = FILE_MAGIC.len();
    let mut parsed = ParsedSeriesFile::default();
    while offset + CHUNK_HEADER_LEN <= bytes.len() {
        if &bytes[offset..offset + 4] != CHUNK_MAGIC {
            parsed.error = Some("history series-file chunk magic mismatch".to_string());
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
            parsed.error = Some("history series-file chunk payload is truncated".to_string());
            break;
        }
        let payload = &bytes[payload_start..payload_end];
        if checksum64(payload) != checksum {
            parsed.error = Some("history series-file chunk checksum mismatch".to_string());
            break;
        }
        match kind_byte {
            1 => parsed.schema_version = Some(decode_meta_schema_version(payload)?),
            2 => decode_rows_payload(payload, kind, &mut parsed.state.rows)?,
            3 => decode_coverage_payload(payload, &mut parsed.state.coverage)?,
            _ => {}
        }
        offset = payload_end;
    }
    if offset < bytes.len() && parsed.error.is_none() {
        parsed.error = Some("history series-file trailing bytes are incomplete".to_string());
    }
    Ok(parsed)
}

fn decode_meta_schema_version(payload: &[u8]) -> Result<u32> {
    let mut offset = 0;
    let version = read_u64(payload, &mut offset)?;
    u32::try_from(version).map_err(|_| {
        DataError::InvalidResponse("history series-file schema version is too large".to_string())
    })
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

fn unescape_symbol_path_component(symbol: &str) -> String {
    symbol.replace("%2F", "/")
}
