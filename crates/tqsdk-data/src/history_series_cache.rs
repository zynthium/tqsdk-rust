#![allow(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use memmap2::Mmap;
use tqsdk_core::{Kline, Tick};

use crate::client::{
    KlineDataSeries, KlineDataSeriesRequest, TickDataSeries, TickDataSeriesRequest,
};
use crate::error::{DataError, Result};

const DEFAULT_CACHE_DIR: &str = ".tqsdk/data_series_1";
pub const HISTORY_SERIES_CACHE_SCHEMA_VERSION: u32 = 1;
const KLINE_DATA_COLS: usize = 7;
const TICK_1_LEVEL_DATA_COLS: usize = 11;
const TICK_5_LEVEL_DATA_COLS: usize = 27;
const TICK_TAIL_REFRESH_NS: i64 = 100;

type IdRange = (i64, i64);
type DatetimeRange = (i64, i64);
type CachedSegment = (IdRange, DatetimeRange);
type MergeGroup = Vec<(IdRange, i64)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySeriesCacheBackend {
    Mmap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheReport {
    pub cache_dir: PathBuf,
    pub backend: HistorySeriesCacheBackend,
    pub hit_rows: usize,
    pub downloaded_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCacheReport {
    pub(crate) fn new(
        cache_dir: PathBuf,
        hit_rows: usize,
        downloaded_ranges: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            cache_dir,
            backend: HistorySeriesCacheBackend::Mmap,
            hit_rows,
            downloaded_ranges,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheMiss {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub duration_ns: i64,
    pub start_datetime_ns: i64,
    pub end_datetime_ns: i64,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl HistorySeriesCacheMiss {
    fn new(
        cache_dir: PathBuf,
        symbol: impl Into<String>,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        missing_ranges: Vec<(i64, i64)>,
    ) -> Self {
        Self {
            cache_dir,
            symbol: symbol.into(),
            duration_ns,
            start_datetime_ns,
            end_datetime_ns,
            missing_ranges,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySeriesCacheFileKind {
    Segment,
    Lock,
    Temp,
    MergeTemp,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySeriesCacheFileStatus {
    Readable,
    EmptySegment,
    InvalidRowWidth,
    IncompleteWrite,
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheFileReport {
    pub path: PathBuf,
    pub file_name: String,
    pub kind: HistorySeriesCacheFileKind,
    pub status: HistorySeriesCacheFileStatus,
    pub symbol: Option<String>,
    pub duration_ns: Option<i64>,
    pub id_range: Option<(i64, i64)>,
    pub row_width: Option<usize>,
    pub rows: usize,
    pub size_bytes: u64,
    pub schema_version: Option<u32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySeriesCacheScanReport {
    pub cache_dir: PathBuf,
    pub schema_version: u32,
    pub files: Vec<HistorySeriesCacheFileReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistorySeriesCacheMaintenanceReport {
    pub removed_files: usize,
    pub removed_bytes: u64,
}

impl HistorySeriesCacheMaintenanceReport {
    fn record_removed(&mut self, size_bytes: u64) {
        self.removed_files += 1;
        self.removed_bytes = self.removed_bytes.saturating_add(size_bytes);
    }
}

#[derive(Clone)]
pub struct HistorySeriesCache {
    inner: Arc<HistorySeriesCacheInner>,
}

struct HistorySeriesCacheInner {
    root_dir: PathBuf,
    lock: Mutex<()>,
}

struct CacheFileMeta {
    path: PathBuf,
    size_bytes: u64,
    modified: SystemTime,
}

pub struct HistorySeriesCacheGuard<'a> {
    _process_guard: MutexGuard<'a, ()>,
    lock_file: File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeriesLayout {
    Kline { duration_ns: i64 },
    Tick { five_level: bool },
}

impl Drop for HistorySeriesCacheGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
    }
}

impl HistorySeriesCache {
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir)?;
        Ok(Self {
            inner: Arc::new(HistorySeriesCacheInner {
                root_dir: canonical_or_original(&root_dir),
                lock: Mutex::new(()),
            }),
        })
    }

    pub fn python_compatible_default() -> Result<Self> {
        Self::open(default_cache_dir())
    }

    pub fn root_dir(&self) -> &Path {
        &self.inner.root_dir
    }

    pub fn uses_mmap_backend(&self) -> bool {
        true
    }

    pub(crate) fn lock_series(
        &self,
        symbol: &str,
        duration_ns: i64,
    ) -> Result<HistorySeriesCacheGuard<'_>> {
        let process_guard = self
            .inner
            .lock
            .lock()
            .map_err(|_| DataError::InvalidState("history series cache lock poisoned"))?;
        fs::create_dir_all(&self.inner.root_dir)?;
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.lock_path(symbol, duration_ns))?;
        lock_file.lock_exclusive()?;
        Ok(HistorySeriesCacheGuard {
            _process_guard: process_guard,
            lock_file,
        })
    }

    pub fn cached_id_ranges(&self, symbol: &str, duration_ns: i64) -> Result<Vec<(i64, i64)>> {
        let mut ranges = Vec::new();
        if !self.inner.root_dir.exists() {
            return Ok(ranges);
        }
        for entry in fs::read_dir(&self.inner.root_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            let Some((file_symbol, file_duration_ns, range)) = parse_data_file_name(&filename)
            else {
                continue;
            };
            if file_symbol == symbol && file_duration_ns == duration_ns {
                if entry.metadata()?.len() == 0 {
                    continue;
                }
                ranges.push(range);
            }
        }
        ranges.sort_unstable();
        Ok(ranges)
    }

    fn cached_segments(
        &self,
        symbol: &str,
        duration_ns: i64,
        id_ranges: &[(i64, i64)],
    ) -> Result<Vec<CachedSegment>> {
        let layout = layout_for(symbol, duration_ns);
        let mut segments = Vec::new();
        for &(start_id, end_id) in id_ranges {
            let path = self.data_file_path(symbol, duration_ns, start_id, end_id);
            let mapped = MappedSeriesFile::open(path, layout)?;
            if mapped.row_count == 0 {
                continue;
            }
            let first_dt = mapped.datetime_at(0)?;
            let last_dt = mapped.datetime_at(mapped.row_count - 1)?;
            let end_dt = match layout {
                SeriesLayout::Kline { duration_ns } => last_dt.checked_add(duration_ns),
                SeriesLayout::Tick { .. } => last_dt.checked_add(TICK_TAIL_REFRESH_NS),
            }
            .ok_or_else(|| {
                DataError::InvalidResponse("history series datetime range overflow".to_string())
            })?;
            segments.push(((start_id, end_id), (first_dt, end_dt)));
        }
        Ok(segments)
    }

    pub fn missing_kline_datetime_ranges(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let _guard = self.lock_series(symbol, duration_ns)?;
        self.missing_kline_datetime_ranges_unlocked(
            symbol,
            duration_ns,
            start_datetime_ns,
            end_datetime_ns,
        )
    }

    pub(crate) fn missing_kline_datetime_ranges_unlocked(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let id_ranges = self.cached_id_ranges(symbol, duration_ns)?;
        let mut dt_ranges = self
            .cached_segments(symbol, duration_ns, &id_ranges)?
            .into_iter()
            .map(|(_, dt_range)| dt_range)
            .collect();
        trim_last_datetime_range(&mut dt_ranges, duration_ns);
        Ok(rangeset_difference(
            &[(start_datetime_ns, end_datetime_ns)],
            &dt_ranges,
        ))
    }

    pub fn missing_tick_datetime_ranges(
        &self,
        symbol: &str,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let _guard = self.lock_series(symbol, 0)?;
        self.missing_tick_datetime_ranges_unlocked(symbol, start_datetime_ns, end_datetime_ns)
    }

    pub(crate) fn missing_tick_datetime_ranges_unlocked(
        &self,
        symbol: &str,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let id_ranges = self.cached_id_ranges(symbol, 0)?;
        let mut dt_ranges = self
            .cached_segments(symbol, 0, &id_ranges)?
            .into_iter()
            .map(|(_, dt_range)| dt_range)
            .collect();
        trim_last_datetime_range(&mut dt_ranges, TICK_TAIL_REFRESH_NS);
        Ok(rangeset_difference(
            &[(start_datetime_ns, end_datetime_ns)],
            &dt_ranges,
        ))
    }

    pub fn read_kline_data_series(
        &self,
        request: KlineDataSeriesRequest,
    ) -> Result<KlineDataSeries> {
        let spec = request.validate()?;
        let _guard = self.lock_series(request.symbol(), spec.duration_ns)?;
        let missing_ranges = self.missing_kline_datetime_ranges_unlocked(
            request.symbol(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        if !missing_ranges.is_empty() {
            return Err(DataError::CacheMiss(Box::new(HistorySeriesCacheMiss::new(
                self.root_dir().to_path_buf(),
                request.symbol(),
                spec.duration_ns,
                spec.start_datetime_ns,
                spec.end_datetime_ns,
                missing_ranges,
            ))));
        }
        self.merge_adjacent_files_unlocked(request.symbol(), spec.duration_ns)?;
        let rows = self.read_kline_window_unlocked(
            request.symbol(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = rows.len();
        Ok(KlineDataSeries::new(
            request.symbol().to_string(),
            spec.duration_ns,
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            self.root_dir().to_path_buf(),
            hit_rows,
            Vec::new(),
        )))
    }

    pub fn read_tick_data_series(&self, request: TickDataSeriesRequest) -> Result<TickDataSeries> {
        let spec = request.validate()?;
        let _guard = self.lock_series(request.symbol(), 0)?;
        let missing_ranges = self.missing_tick_datetime_ranges_unlocked(
            request.symbol(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        if !missing_ranges.is_empty() {
            return Err(DataError::CacheMiss(Box::new(HistorySeriesCacheMiss::new(
                self.root_dir().to_path_buf(),
                request.symbol(),
                0,
                spec.start_datetime_ns,
                spec.end_datetime_ns,
                missing_ranges,
            ))));
        }
        self.merge_adjacent_files_unlocked(request.symbol(), 0)?;
        let rows = self.read_tick_window_unlocked(
            request.symbol(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
        )?;
        let hit_rows = rows.len();
        Ok(TickDataSeries::new(
            request.symbol().to_string(),
            spec.start_datetime_ns,
            spec.end_datetime_ns,
            rows,
        )
        .with_cache_report(HistorySeriesCacheReport::new(
            self.root_dir().to_path_buf(),
            hit_rows,
            Vec::new(),
        )))
    }

    pub fn write_kline_segment(
        &self,
        symbol: &str,
        duration_ns: i64,
        rows: &[Kline],
    ) -> Result<Option<(i64, i64)>> {
        let _guard = self.lock_series(symbol, duration_ns)?;
        self.write_kline_segment_unlocked(symbol, duration_ns, rows)
    }

    pub(crate) fn write_kline_segment_unlocked(
        &self,
        symbol: &str,
        duration_ns: i64,
        rows: &[Kline],
    ) -> Result<Option<(i64, i64)>> {
        if rows.is_empty() {
            return Ok(None);
        }
        let temp_path = self.temp_path(symbol, duration_ns);
        let mut file = File::create(&temp_path)?;
        {
            let mut writer = BufWriter::new(&mut file);
            for row in rows {
                write_i64(&mut writer, row.id)?;
                write_i64(&mut writer, row.datetime)?;
                write_f64(&mut writer, row.open)?;
                write_f64(&mut writer, row.high)?;
                write_f64(&mut writer, row.low)?;
                write_f64(&mut writer, row.close)?;
                write_f64(&mut writer, row.volume as f64)?;
                write_f64(&mut writer, row.open_oi as f64)?;
                write_f64(&mut writer, row.close_oi as f64)?;
            }
            writer.flush()?;
        }
        file.sync_all()?;
        let range = range_from_ids(rows.iter().map(|row| row.id))?;
        let target = self.data_file_path(symbol, duration_ns, range.0, range.1);
        fs::rename(temp_path, target)?;
        Ok(Some(range))
    }

    pub fn write_tick_segment(&self, symbol: &str, rows: &[Tick]) -> Result<Option<(i64, i64)>> {
        let _guard = self.lock_series(symbol, 0)?;
        self.write_tick_segment_unlocked(symbol, rows)
    }

    pub(crate) fn write_tick_segment_unlocked(
        &self,
        symbol: &str,
        rows: &[Tick],
    ) -> Result<Option<(i64, i64)>> {
        if rows.is_empty() {
            return Ok(None);
        }
        let temp_path = self.temp_path(symbol, 0);
        let mut file = File::create(&temp_path)?;
        let five_level = tick_uses_five_levels(symbol);
        {
            let mut writer = BufWriter::new(&mut file);
            for row in rows {
                write_i64(&mut writer, row.id)?;
                write_i64(&mut writer, row.datetime)?;
                write_f64(&mut writer, row.last_price)?;
                write_f64(&mut writer, row.highest)?;
                write_f64(&mut writer, row.lowest)?;
                write_f64(&mut writer, row.average)?;
                write_f64(&mut writer, row.volume as f64)?;
                write_f64(&mut writer, row.amount)?;
                write_f64(&mut writer, row.open_interest as f64)?;
                write_tick_level(
                    &mut writer,
                    row.bid_price1,
                    row.bid_volume1,
                    row.ask_price1,
                    row.ask_volume1,
                )?;
                if five_level {
                    write_tick_level(
                        &mut writer,
                        row.bid_price2,
                        row.bid_volume2,
                        row.ask_price2,
                        row.ask_volume2,
                    )?;
                    write_tick_level(
                        &mut writer,
                        row.bid_price3,
                        row.bid_volume3,
                        row.ask_price3,
                        row.ask_volume3,
                    )?;
                    write_tick_level(
                        &mut writer,
                        row.bid_price4,
                        row.bid_volume4,
                        row.ask_price4,
                        row.ask_volume4,
                    )?;
                    write_tick_level(
                        &mut writer,
                        row.bid_price5,
                        row.bid_volume5,
                        row.ask_price5,
                        row.ask_volume5,
                    )?;
                }
            }
            writer.flush()?;
        }
        file.sync_all()?;
        let range = range_from_ids(rows.iter().map(|row| row.id))?;
        let target = self.data_file_path(symbol, 0, range.0, range.1);
        fs::rename(temp_path, target)?;
        Ok(Some(range))
    }

    pub fn read_kline_window(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Kline>> {
        let _guard = self.lock_series(symbol, duration_ns)?;
        self.read_kline_window_unlocked(symbol, duration_ns, start_datetime_ns, end_datetime_ns)
    }

    pub(crate) fn read_kline_window_unlocked(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Kline>> {
        let id_ranges = self.cached_id_ranges(symbol, duration_ns)?;
        let segments = self.cached_segments(symbol, duration_ns, &id_ranges)?;
        let rows = self.read_window(
            symbol,
            layout_for(symbol, duration_ns),
            start_datetime_ns,
            end_datetime_ns,
            &segments,
        )?;
        Ok(dedup_klines(rows.into_klines()))
    }

    pub fn read_tick_window(
        &self,
        symbol: &str,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Tick>> {
        let _guard = self.lock_series(symbol, 0)?;
        self.read_tick_window_unlocked(symbol, start_datetime_ns, end_datetime_ns)
    }

    pub(crate) fn read_tick_window_unlocked(
        &self,
        symbol: &str,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
    ) -> Result<Vec<Tick>> {
        let id_ranges = self.cached_id_ranges(symbol, 0)?;
        let segments = self.cached_segments(symbol, 0, &id_ranges)?;
        let rows = self.read_window(
            symbol,
            layout_for(symbol, 0),
            start_datetime_ns,
            end_datetime_ns,
            &segments,
        )?;
        Ok(dedup_ticks(rows.into_ticks()))
    }

    pub fn merge_adjacent_files(&self, symbol: &str, duration_ns: i64) -> Result<()> {
        let _guard = self.lock_series(symbol, duration_ns)?;
        self.merge_adjacent_files_unlocked(symbol, duration_ns)
    }

    pub(crate) fn merge_adjacent_files_unlocked(
        &self,
        symbol: &str,
        duration_ns: i64,
    ) -> Result<()> {
        let ranges = self.cached_id_ranges(symbol, duration_ns)?;
        if ranges.len() <= 1 {
            return Ok(());
        }
        let layout = layout_for(symbol, duration_ns);
        for group in build_merge_groups(&ranges)? {
            if group.len() <= 1 {
                continue;
            }
            let first_start = group[0].0.0;
            let last_end = group
                .last()
                .map(|(range, _)| range.1)
                .unwrap_or(first_start);
            let temp_path = self.merge_temp_path(symbol, duration_ns);
            let mut file = File::create(&temp_path)?;
            {
                let mut writer = BufWriter::new(&mut file);
                for (range, rows_to_copy) in &group {
                    let path = self.data_file_path(symbol, duration_ns, range.0, range.1);
                    let mapped = MappedSeriesFile::open(path, layout)?;
                    mapped.write_rows_to(*rows_to_copy, &mut writer)?;
                }
                writer.flush()?;
            }
            file.sync_all()?;
            for (range, _) in &group {
                let _ = fs::remove_file(self.data_file_path(symbol, duration_ns, range.0, range.1));
            }
            fs::rename(
                temp_path,
                self.data_file_path(symbol, duration_ns, first_start, last_end),
            )?;
        }
        Ok(())
    }

    pub fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        let _guard = self
            .inner
            .lock
            .lock()
            .map_err(|_| DataError::InvalidState("history series cache lock poisoned"))?;
        let mut files = Vec::new();
        if self.inner.root_dir.exists() {
            for entry in fs::read_dir(&self.inner.root_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                files.push(self.scan_file(entry.path())?);
            }
        }
        files.sort_by(|left, right| left.file_name.cmp(&right.file_name));
        Ok(HistorySeriesCacheScanReport {
            cache_dir: self.root_dir().to_path_buf(),
            schema_version: HISTORY_SERIES_CACHE_SCHEMA_VERSION,
            files,
        })
    }

    pub fn enforce_limits(
        &self,
        max_bytes: Option<u64>,
        retention_days: Option<u64>,
    ) -> Result<HistorySeriesCacheMaintenanceReport> {
        let _guard = self
            .inner
            .lock
            .lock()
            .map_err(|_| DataError::InvalidState("history series cache lock poisoned"))?;
        let mut report = HistorySeriesCacheMaintenanceReport::default();
        self.evict_expired_files(retention_days, &mut report)?;
        self.evict_by_total_size(max_bytes, &mut report)?;
        Ok(report)
    }

    fn scan_file(&self, path: PathBuf) -> Result<HistorySeriesCacheFileReport> {
        let metadata = fs::metadata(&path)?;
        let size_bytes = metadata.len();
        let file_name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some((symbol, duration_ns, id_range)) = parse_data_file_name(&file_name) {
            let layout = layout_for(&symbol, duration_ns);
            let row_width = layout.row_size();
            let (status, rows, schema_version, error) = if size_bytes == 0 {
                (
                    HistorySeriesCacheFileStatus::EmptySegment,
                    0,
                    Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION),
                    None,
                )
            } else if size_bytes % row_width as u64 != 0 {
                (
                    HistorySeriesCacheFileStatus::InvalidRowWidth,
                    0,
                    Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION),
                    Some(format!(
                        "file length {size_bytes} is not a multiple of row width {row_width}"
                    )),
                )
            } else {
                (
                    HistorySeriesCacheFileStatus::Readable,
                    (size_bytes / row_width as u64) as usize,
                    Some(HISTORY_SERIES_CACHE_SCHEMA_VERSION),
                    None,
                )
            };
            return Ok(HistorySeriesCacheFileReport {
                path,
                file_name,
                kind: HistorySeriesCacheFileKind::Segment,
                status,
                symbol: Some(symbol),
                duration_ns: Some(duration_ns),
                id_range: Some(id_range),
                row_width: Some(row_width),
                rows,
                size_bytes,
                schema_version,
                error,
            });
        }

        let (kind, status) = classify_non_segment_file(&file_name);
        Ok(HistorySeriesCacheFileReport {
            path,
            file_name,
            kind,
            status,
            symbol: None,
            duration_ns: None,
            id_range: None,
            row_width: None,
            rows: 0,
            size_bytes,
            schema_version: None,
            error: None,
        })
    }

    fn list_segment_files(&self) -> Result<Vec<CacheFileMeta>> {
        if !self.inner.root_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.inner.root_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if parse_data_file_name(&file_name).is_none() {
                continue;
            }
            let metadata = entry.metadata()?;
            files.push(CacheFileMeta {
                path: entry.path(),
                size_bytes: metadata.len(),
                modified: metadata.modified().unwrap_or(UNIX_EPOCH),
            });
        }
        Ok(files)
    }

    fn evict_expired_files(
        &self,
        retention_days: Option<u64>,
        report: &mut HistorySeriesCacheMaintenanceReport,
    ) -> Result<()> {
        let Some(days) = retention_days else {
            return Ok(());
        };
        let ttl = Duration::from_secs(days.saturating_mul(24 * 60 * 60));
        let cutoff = SystemTime::now().checked_sub(ttl).unwrap_or(UNIX_EPOCH);
        for file in self.list_segment_files()? {
            if file.modified <= cutoff && fs::remove_file(&file.path).is_ok() {
                report.record_removed(file.size_bytes);
            }
        }
        Ok(())
    }

    fn evict_by_total_size(
        &self,
        max_bytes: Option<u64>,
        report: &mut HistorySeriesCacheMaintenanceReport,
    ) -> Result<()> {
        let Some(limit) = max_bytes else {
            return Ok(());
        };
        let mut files = self.list_segment_files()?;
        let mut total = files.iter().map(|file| file.size_bytes).sum::<u64>();
        if total <= limit {
            return Ok(());
        }
        files.sort_by_key(|file| file.modified);
        for file in files {
            if total <= limit {
                break;
            }
            if fs::remove_file(&file.path).is_ok() {
                total = total.saturating_sub(file.size_bytes);
                report.record_removed(file.size_bytes);
            }
        }
        Ok(())
    }

    fn read_window(
        &self,
        symbol: &str,
        layout: SeriesLayout,
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        segments: &[CachedSegment],
    ) -> Result<WindowRows> {
        let mut rows = match layout {
            SeriesLayout::Kline { .. } => WindowRows::Kline(Vec::new()),
            SeriesLayout::Tick { .. } => WindowRows::Tick(Vec::new()),
        };
        for (range_id, range_dt) in segments {
            let target =
                rangeset_intersection(&[(start_datetime_ns, end_datetime_ns)], &[*range_dt]);
            let Some(&(target_start, target_end)) = target.first() else {
                continue;
            };
            let path = self.data_file_path(symbol, layout.duration_ns(), range_id.0, range_id.1);
            let mapped = MappedSeriesFile::open(path, layout)?;
            let Some(start_index) = mapped.last_index_where(|dt| dt <= target_start)? else {
                continue;
            };
            let Some(end_index) = mapped.last_index_where(|dt| dt < target_end)? else {
                continue;
            };
            if start_index > end_index {
                continue;
            }
            for index in start_index..=end_index {
                rows.push(mapped.read_row(index)?);
            }
        }
        Ok(rows)
    }

    fn data_file_path(
        &self,
        symbol: &str,
        duration_ns: i64,
        start_id: i64,
        end_id: i64,
    ) -> PathBuf {
        self.inner
            .root_dir
            .join(format!("{symbol}.{duration_ns}.{start_id}.{end_id}"))
    }

    fn temp_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.inner
            .root_dir
            .join(format!("{symbol}.{duration_ns}.temp"))
    }

    fn merge_temp_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        self.inner
            .root_dir
            .join(format!("{symbol}.{duration_ns}.merge.{suffix}"))
    }

    fn lock_path(&self, symbol: &str, duration_ns: i64) -> PathBuf {
        self.inner
            .root_dir
            .join(format!(".{symbol}.{duration_ns}.lock"))
    }
}

struct MappedSeriesFile {
    mmap: Option<Mmap>,
    row_count: usize,
    layout: SeriesLayout,
}

impl MappedSeriesFile {
    fn open(path: PathBuf, layout: SeriesLayout) -> Result<Self> {
        let file = File::open(&path)?;
        let len = file.metadata()?.len() as usize;
        let row_size = layout.row_size();
        if len == 0 {
            return Ok(Self {
                mmap: None,
                row_count: 0,
                layout,
            });
        }
        if len % row_size != 0 {
            return Err(DataError::InvalidResponse(format!(
                "history series cache file length does not match row width: {}",
                path.display()
            )));
        }
        let mmap = map_file(&file)?;
        Ok(Self {
            mmap: Some(mmap),
            row_count: len / row_size,
            layout,
        })
    }

    fn datetime_at(&self, index: usize) -> Result<i64> {
        self.read_i64(index, 8)
    }

    fn last_index_where<F>(&self, predicate: F) -> Result<Option<usize>>
    where
        F: Fn(i64) -> bool,
    {
        let mut lo = 0usize;
        let mut hi = self.row_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if predicate(self.datetime_at(mid)?) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 { Ok(None) } else { Ok(Some(lo - 1)) }
    }

    fn read_row(&self, index: usize) -> Result<DecodedRow> {
        let mut offset = index.checked_mul(self.layout.row_size()).ok_or_else(|| {
            DataError::InvalidResponse("history series cache offset overflow".to_string())
        })?;
        let mmap = self.mmap.as_ref().ok_or_else(|| {
            DataError::InvalidResponse("history series cache row index out of bounds".to_string())
        })?;
        let id = read_i64_from(mmap, offset)?;
        offset += 8;
        let datetime = read_i64_from(mmap, offset)?;
        offset += 8;
        match self.layout {
            SeriesLayout::Kline { .. } => Ok(DecodedRow::Kline(Kline {
                id,
                datetime,
                open: read_f64_advance(mmap, &mut offset)?,
                high: read_f64_advance(mmap, &mut offset)?,
                low: read_f64_advance(mmap, &mut offset)?,
                close: read_f64_advance(mmap, &mut offset)?,
                volume: read_f64_advance(mmap, &mut offset)? as i64,
                open_oi: read_f64_advance(mmap, &mut offset)? as i64,
                close_oi: read_f64_advance(mmap, &mut offset)? as i64,
                ..Kline::default()
            })),
            SeriesLayout::Tick { five_level } => {
                let mut row = Tick {
                    id,
                    datetime,
                    last_price: read_f64_advance(mmap, &mut offset)?,
                    highest: read_f64_advance(mmap, &mut offset)?,
                    lowest: read_f64_advance(mmap, &mut offset)?,
                    average: read_f64_advance(mmap, &mut offset)?,
                    volume: read_f64_advance(mmap, &mut offset)? as i64,
                    amount: read_f64_advance(mmap, &mut offset)?,
                    open_interest: read_f64_advance(mmap, &mut offset)? as i64,
                    ..Tick::default()
                };
                read_tick_level(
                    mmap,
                    &mut offset,
                    &mut row.bid_price1,
                    &mut row.bid_volume1,
                    &mut row.ask_price1,
                    &mut row.ask_volume1,
                )?;
                if five_level {
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price2,
                        &mut row.bid_volume2,
                        &mut row.ask_price2,
                        &mut row.ask_volume2,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price3,
                        &mut row.bid_volume3,
                        &mut row.ask_price3,
                        &mut row.ask_volume3,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price4,
                        &mut row.bid_volume4,
                        &mut row.ask_price4,
                        &mut row.ask_volume4,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price5,
                        &mut row.bid_volume5,
                        &mut row.ask_price5,
                        &mut row.ask_volume5,
                    )?;
                }
                Ok(DecodedRow::Tick(row))
            }
        }
    }

    fn read_i64(&self, index: usize, field_offset: usize) -> Result<i64> {
        let offset = index
            .checked_mul(self.layout.row_size())
            .and_then(|base| base.checked_add(field_offset))
            .ok_or_else(|| {
                DataError::InvalidResponse("history series cache offset overflow".to_string())
            })?;
        let mmap = self.mmap.as_ref().ok_or_else(|| {
            DataError::InvalidResponse("history series cache row index out of bounds".to_string())
        })?;
        read_i64_from(mmap, offset)
    }

    fn write_rows_to(&self, rows_to_copy: i64, writer: &mut impl Write) -> Result<()> {
        let rows_to_copy = usize::try_from(rows_to_copy.max(0)).map_err(|_| {
            DataError::InvalidResponse("history series merge row count overflow".to_string())
        })?;
        let bytes_to_copy = rows_to_copy
            .checked_mul(self.layout.row_size())
            .ok_or_else(|| {
                DataError::InvalidResponse("history series merge byte count overflow".to_string())
            })?;
        if let Some(mmap) = &self.mmap {
            writer.write_all(&mmap[..bytes_to_copy.min(mmap.len())])?;
        }
        Ok(())
    }
}

enum DecodedRow {
    Kline(Kline),
    Tick(Tick),
}

enum WindowRows {
    Kline(Vec<Kline>),
    Tick(Vec<Tick>),
}

impl WindowRows {
    fn push(&mut self, row: DecodedRow) {
        match (self, row) {
            (Self::Kline(rows), DecodedRow::Kline(row)) => rows.push(row),
            (Self::Tick(rows), DecodedRow::Tick(row)) => rows.push(row),
            _ => {}
        }
    }

    fn into_klines(self) -> Vec<Kline> {
        match self {
            Self::Kline(rows) => rows,
            Self::Tick(_) => Vec::new(),
        }
    }

    fn into_ticks(self) -> Vec<Tick> {
        match self {
            Self::Tick(rows) => rows,
            Self::Kline(_) => Vec::new(),
        }
    }
}

impl SeriesLayout {
    fn row_size(self) -> usize {
        let cols = match self {
            Self::Kline { .. } => KLINE_DATA_COLS,
            Self::Tick { five_level: true } => TICK_5_LEVEL_DATA_COLS,
            Self::Tick { five_level: false } => TICK_1_LEVEL_DATA_COLS,
        };
        (2 + cols) * 8
    }

    fn duration_ns(self) -> i64 {
        match self {
            Self::Kline { duration_ns } => duration_ns,
            Self::Tick { .. } => 0,
        }
    }
}

fn map_file(file: &File) -> Result<Mmap> {
    // SAFETY: the mapping is read-only and all typed access below copies bytes
    // into fixed-size arrays before decoding. The public API returns owned rows,
    // so mmap lifetimes never escape this module.
    unsafe { Mmap::map(file) }.map_err(DataError::from)
}

fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn default_cache_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(DEFAULT_CACHE_DIR))
        .unwrap_or_else(|| std::env::temp_dir().join("tqsdk_data_series_1"))
}

fn parse_data_file_name(filename: &str) -> Option<(String, i64, (i64, i64))> {
    if filename.starts_with('.')
        || filename.ends_with(".lock")
        || filename.ends_with(".temp")
        || filename.contains(".merge.")
    {
        return None;
    }
    let parts = filename.split('.').collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }
    let end = parts.last()?.parse::<i64>().ok()?;
    let start = parts.get(parts.len() - 2)?.parse::<i64>().ok()?;
    let duration_ns = parts.get(parts.len() - 3)?.parse::<i64>().ok()?;
    if start >= end {
        return None;
    }
    let symbol = parts[..parts.len() - 3].join(".");
    if symbol.is_empty() {
        return None;
    }
    Some((symbol, duration_ns, (start, end)))
}

fn classify_non_segment_file(
    filename: &str,
) -> (HistorySeriesCacheFileKind, HistorySeriesCacheFileStatus) {
    if filename.starts_with('.') || filename.ends_with(".lock") {
        (
            HistorySeriesCacheFileKind::Lock,
            HistorySeriesCacheFileStatus::Ignored,
        )
    } else if filename.ends_with(".temp") {
        (
            HistorySeriesCacheFileKind::Temp,
            HistorySeriesCacheFileStatus::IncompleteWrite,
        )
    } else if filename.contains(".merge.") {
        (
            HistorySeriesCacheFileKind::MergeTemp,
            HistorySeriesCacheFileStatus::IncompleteWrite,
        )
    } else {
        (
            HistorySeriesCacheFileKind::Unknown,
            HistorySeriesCacheFileStatus::Ignored,
        )
    }
}

fn layout_for(symbol: &str, duration_ns: i64) -> SeriesLayout {
    if duration_ns == 0 {
        SeriesLayout::Tick {
            five_level: tick_uses_five_levels(symbol),
        }
    } else {
        SeriesLayout::Kline { duration_ns }
    }
}

fn tick_uses_five_levels(symbol: &str) -> bool {
    matches!(symbol.split('.').next(), Some("SHFE" | "SSE" | "SZSE"))
}

fn trim_last_datetime_range(ranges: &mut Vec<(i64, i64)>, width: i64) {
    if let Some(last) = ranges.last_mut() {
        last.1 = last.1.saturating_sub(width).max(last.0);
        if last.0 == last.1 {
            ranges.pop();
        }
    }
}

pub(crate) fn rangeset_difference(
    requested: &[(i64, i64)],
    cached: &[(i64, i64)],
) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for &(start, end) in requested {
        if start >= end {
            continue;
        }
        let mut cursor = start;
        for &(cached_start, cached_end) in cached {
            if cached_end <= cursor {
                continue;
            }
            if cached_start >= end {
                break;
            }
            if cursor < cached_start {
                result.push((cursor, cached_start.min(end)));
            }
            cursor = cursor.max(cached_end);
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            result.push((cursor, end));
        }
    }
    result
}

pub(crate) fn rangeset_intersection(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for &(left_start, left_end) in left {
        for &(right_start, right_end) in right {
            let start = left_start.max(right_start);
            let end = left_end.min(right_end);
            if start < end {
                result.push((start, end));
            }
        }
    }
    result
}

fn range_from_ids(ids: impl IntoIterator<Item = i64>) -> Result<(i64, i64)> {
    let mut min_id = None;
    let mut max_id = None;
    for id in ids {
        min_id = Some(min_id.map_or(id, |value: i64| value.min(id)));
        max_id = Some(max_id.map_or(id, |value: i64| value.max(id)));
    }
    let start = min_id
        .ok_or_else(|| DataError::InvalidResponse("history series segment is empty".to_string()))?;
    let end = max_id
        .and_then(|id: i64| id.checked_add(1))
        .ok_or_else(|| {
            DataError::InvalidResponse("history series segment id overflow".to_string())
        })?;
    Ok((start, end))
}

fn build_merge_groups(ranges: &[IdRange]) -> Result<Vec<MergeGroup>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let mut groups = vec![vec![(ranges[0], ranges[0].1 - ranges[0].0)]];
    for index in 1..ranges.len() {
        let previous = ranges[index - 1];
        let current = ranges[index];
        if current.0 < previous.1 - 1 {
            return Err(DataError::InvalidResponse(
                "history series cache ranges overlap unexpectedly".to_string(),
            ));
        }
        if current.0 == previous.1 {
            groups
                .last_mut()
                .expect("merge group exists")
                .push((current, current.1 - current.0));
        } else if current.0 == previous.1 - 1 {
            if let Some(last_group) = groups.last_mut()
                && let Some(last_entry) = last_group.last_mut()
            {
                last_entry.1 = (previous.1 - 1) - previous.0;
            }
            groups
                .last_mut()
                .expect("merge group exists")
                .push((current, current.1 - current.0));
        } else {
            groups.push(vec![(current, current.1 - current.0)]);
        }
    }
    Ok(groups)
}

fn dedup_klines(rows: Vec<Kline>) -> Vec<Kline> {
    let mut by_id = BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
}

fn dedup_ticks(rows: Vec<Tick>) -> Vec<Tick> {
    let mut by_id = BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
}

fn write_tick_level(
    writer: &mut impl Write,
    bid_price: f64,
    bid_volume: i64,
    ask_price: f64,
    ask_volume: i64,
) -> Result<()> {
    write_f64(writer, bid_price)?;
    write_f64(writer, bid_volume as f64)?;
    write_f64(writer, ask_price)?;
    write_f64(writer, ask_volume as f64)
}

fn write_i64(writer: &mut impl Write, value: i64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}

fn write_f64(writer: &mut impl Write, value: f64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}

fn read_tick_level(
    mmap: &[u8],
    offset: &mut usize,
    bid_price: &mut f64,
    bid_volume: &mut i64,
    ask_price: &mut f64,
    ask_volume: &mut i64,
) -> Result<()> {
    *bid_price = read_f64_advance(mmap, offset)?;
    *bid_volume = read_f64_advance(mmap, offset)? as i64;
    *ask_price = read_f64_advance(mmap, offset)?;
    *ask_volume = read_f64_advance(mmap, offset)? as i64;
    Ok(())
}

fn read_f64_advance(bytes: &[u8], offset: &mut usize) -> Result<f64> {
    let value = read_f64_from(bytes, *offset)?;
    *offset += 8;
    Ok(value)
}

fn read_i64_from(bytes: &[u8], offset: usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series cache offset overflow".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series cache row width mismatch".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    Ok(i64::from_ne_bytes(array))
}

fn read_f64_from(bytes: &[u8], offset: usize) -> Result<f64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series cache offset overflow".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series cache row width mismatch".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    Ok(f64::from_ne_bytes(array))
}
