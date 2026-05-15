use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use tqsdk_core::{Kline, Tick};

use crate::client::{
    KlineDataSeries, KlineDataSeriesRequest, TickDataSeries, TickDataSeriesRequest,
};
use crate::error::{DataError, Result};

mod paths;
mod ranges;
mod storage;

use paths::{
    canonical_or_original, classify_non_segment_file, default_cache_dir, parse_data_file_name,
};
use ranges::{
    build_merge_groups, dedup_klines, dedup_ticks, range_from_ids, trim_last_datetime_range,
};
pub(crate) use ranges::{rangeset_difference, rangeset_intersection};
use storage::{
    MappedSeriesFile, SeriesLayout, WindowRows, layout_for, tick_uses_five_levels, write_f64,
    write_i64, write_tick_level,
};

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
    global_gate: RwLock<()>,
    active_series: Mutex<HashSet<SeriesKey>>,
    active_series_changed: Condvar,
    range_index: Mutex<RangeIndex>,
}

struct CacheFileMeta {
    path: PathBuf,
    size_bytes: u64,
    modified: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    symbol: String,
    duration_ns: i64,
}

impl SeriesKey {
    fn new(symbol: &str, duration_ns: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            duration_ns,
        }
    }
}

#[derive(Default)]
struct RangeIndex {
    root_modified: Option<SystemTime>,
    ranges: HashMap<SeriesKey, Vec<IdRange>>,
}

pub struct HistorySeriesCacheGuard<'a> {
    _global_guard: RwLockReadGuard<'a, ()>,
    inner: &'a HistorySeriesCacheInner,
    series_key: SeriesKey,
    lock_file: File,
}

impl Drop for HistorySeriesCacheGuard<'_> {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
        if let Ok(mut active) = self.inner.active_series.lock() {
            active.remove(&self.series_key);
            self.inner.active_series_changed.notify_all();
        }
    }
}

impl HistorySeriesCache {
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir)?;
        Ok(Self {
            inner: Arc::new(HistorySeriesCacheInner {
                root_dir: canonical_or_original(&root_dir),
                global_gate: RwLock::new(()),
                active_series: Mutex::new(HashSet::new()),
                active_series_changed: Condvar::new(),
                range_index: Mutex::new(RangeIndex::default()),
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
        let global_guard = self
            .inner
            .global_gate
            .read()
            .map_err(|_| DataError::InvalidState("history series cache gate poisoned"))?;
        let series_key = SeriesKey::new(symbol, duration_ns);
        let mut active = self
            .inner
            .active_series
            .lock()
            .map_err(|_| DataError::InvalidState("history series cache lock poisoned"))?;
        while active.contains(&series_key) {
            active = self
                .inner
                .active_series_changed
                .wait(active)
                .map_err(|_| DataError::InvalidState("history series cache lock poisoned"))?;
        }
        active.insert(series_key.clone());
        drop(active);

        fs::create_dir_all(&self.inner.root_dir)?;
        let lock_file = match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.lock_path(symbol, duration_ns))
        {
            Ok(file) => file,
            Err(err) => {
                self.release_active_series(&series_key);
                return Err(err.into());
            }
        };
        if let Err(err) = lock_file.lock_exclusive() {
            self.release_active_series(&series_key);
            return Err(err.into());
        }
        Ok(HistorySeriesCacheGuard {
            _global_guard: global_guard,
            inner: self.inner.as_ref(),
            series_key,
            lock_file,
        })
    }

    pub fn cached_id_ranges(&self, symbol: &str, duration_ns: i64) -> Result<Vec<(i64, i64)>> {
        let _guard = self
            .inner
            .global_gate
            .read()
            .map_err(|_| DataError::InvalidState("history series cache gate poisoned"))?;
        self.cached_id_ranges_unlocked(symbol, duration_ns)
    }

    fn cached_id_ranges_unlocked(&self, symbol: &str, duration_ns: i64) -> Result<Vec<(i64, i64)>> {
        let key = SeriesKey::new(symbol, duration_ns);
        let root_modified = self.root_modified();
        let mut index = self
            .inner
            .range_index
            .lock()
            .map_err(|_| DataError::InvalidState("history series cache index poisoned"))?;
        if index.root_modified == root_modified {
            return Ok(index.ranges.get(&key).cloned().unwrap_or_default());
        }
        index.ranges = self.scan_id_ranges_by_series()?;
        index.root_modified = root_modified;
        Ok(index.ranges.get(&key).cloned().unwrap_or_default())
    }

    fn release_active_series(&self, series_key: &SeriesKey) {
        if let Ok(mut active) = self.inner.active_series.lock() {
            active.remove(series_key);
            self.inner.active_series_changed.notify_all();
        }
    }

    fn invalidate_range_index(&self) {
        if let Ok(mut index) = self.inner.range_index.lock() {
            index.root_modified = None;
            index.ranges.clear();
        }
    }

    fn root_modified(&self) -> Option<SystemTime> {
        fs::metadata(&self.inner.root_dir)
            .and_then(|metadata| metadata.modified())
            .ok()
    }

    fn scan_id_ranges_by_series(&self) -> Result<HashMap<SeriesKey, Vec<IdRange>>> {
        let mut ranges_by_series: HashMap<SeriesKey, Vec<IdRange>> = HashMap::new();
        if !self.inner.root_dir.exists() {
            return Ok(ranges_by_series);
        }
        for entry in fs::read_dir(&self.inner.root_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            let Some((symbol, duration_ns, range)) = parse_data_file_name(&filename) else {
                continue;
            };
            if entry.metadata()?.len() == 0 {
                continue;
            }
            ranges_by_series
                .entry(SeriesKey::new(&symbol, duration_ns))
                .or_default()
                .push(range);
        }
        for ranges in ranges_by_series.values_mut() {
            ranges.sort_unstable();
        }
        Ok(ranges_by_series)
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
        let id_ranges = self.cached_id_ranges_unlocked(symbol, duration_ns)?;
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
        let id_ranges = self.cached_id_ranges_unlocked(symbol, 0)?;
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
        self.invalidate_range_index();
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
        self.invalidate_range_index();
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
        let id_ranges = self.cached_id_ranges_unlocked(symbol, duration_ns)?;
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
        let id_ranges = self.cached_id_ranges_unlocked(symbol, 0)?;
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
        let ranges = self.cached_id_ranges_unlocked(symbol, duration_ns)?;
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
                    let expected_rows = usize::try_from(range.1 - range.0).map_err(|_| {
                        DataError::InvalidResponse(
                            "history series cache range row count overflow".to_string(),
                        )
                    })?;
                    if mapped.row_count() != expected_rows {
                        return Err(DataError::InvalidResponse(
                            "history series cache range does not match row count".to_string(),
                        ));
                    }
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
            self.invalidate_range_index();
        }
        Ok(())
    }

    pub fn scan(&self) -> Result<HistorySeriesCacheScanReport> {
        let _guard = self
            .inner
            .global_gate
            .write()
            .map_err(|_| DataError::InvalidState("history series cache gate poisoned"))?;
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
            .global_gate
            .write()
            .map_err(|_| DataError::InvalidState("history series cache gate poisoned"))?;
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
                self.invalidate_range_index();
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
                self.invalidate_range_index();
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::HistorySeriesCache;

    #[test]
    fn in_process_locks_do_not_block_independent_series() {
        let cache = HistorySeriesCache::open(temp_dir("independent-series-locks")).unwrap();
        let _guard = cache.lock_series("SHFE.au2602", 60_000_000_000).unwrap();
        let other_cache = cache.clone();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let _other_guard = other_cache
                .lock_series("DCE.m2601", 60_000_000_000)
                .unwrap();
            tx.send(()).unwrap();
        });

        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(()) => {}
            Err(err) => {
                drop(_guard);
                let _ = handle.join();
                panic!("independent series lock was blocked: {err}");
            }
        }
        handle.join().unwrap();
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "tqsdk-data-history-series-cache-unit-{name}-{nanos}"
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.canonicalize().unwrap_or(dir)
    }
}
