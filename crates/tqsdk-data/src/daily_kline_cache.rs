//! Native daily-Kline cache for local backtests.
//!
//! One logical futures symbol owns one atomically replaced file.  Daily rows
//! arrive only from the official server-backtest `1d` chart; longer periods
//! are derived in memory by the backtest-history executor.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use fs2::FileExt;
use tqsdk_core::Kline;

use crate::backtest_history::minute_cache_snapshots_are_compatible;
use crate::backtest_tick_cache::{
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
use crate::minute_kline_cache::MinuteKlineCacheSnapshot;
use crate::{DataError, Result};

/// Canonical native daily-Kline duration accepted by this cache.
pub const DAILY_KLINE_DURATION_NS: i64 = 86_400_000_000_000;

/// Stable identity for the independent daily-Kline cache format.
pub const DAILY_KLINE_CACHE_FORMAT_ID: &str = "tqsdk.daily-kline.single-file.v1";

/// Public format version stored in every daily-Kline file.
pub const DAILY_KLINE_CACHE_SCHEMA_VERSION: u32 = 1;

/// Daily and canonical-minute files use the same immutable metadata identity.
pub type DailyKlineCacheSnapshot = MinuteKlineCacheSnapshot;

const ROOT_DIR_NAME: &str = "daily-kline-v1";
const FILE_EXTENSION: &str = "tqdk";
const FILE_MAGIC: [u8; 4] = *b"TQDK";
const FILE_VERSION: u16 = 1;
const FILE_HEADER_BYTES: usize = 24;
const KLINE_ROW_BYTES: usize = 80;
const MAX_STRING_BYTES: usize = 32 * 1024;
const MAX_COVERAGE_RECORDS: usize = 100_000;
const MAX_ROWS: usize = 5_000_000;
const NONE_EPOCH: i64 = i64::MIN;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Final coverage for one daily-Kline range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyKlineCoverage {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl DailyKlineCoverage {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// Typed result of daily-Kline cache inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyKlineCacheStatus {
    pub format_id: &'static str,
    pub cache_dir: PathBuf,
    pub namespace_dir: PathBuf,
    pub path: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl DailyKlineCacheStatus {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// Result of one final daily-Kline range write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyKlineCacheWriteReport {
    pub cache_dir: PathBuf,
    pub path: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
    pub cached_ranges: Vec<(i64, i64)>,
}

/// Deep validation classification for one daily-Kline symbol file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DailyKlineCacheDiagnosticStatus {
    Missing,
    Readable,
    UnsupportedVersion,
    Corrupt,
}

impl DailyKlineCacheDiagnosticStatus {
    #[must_use]
    pub fn is_problem(self) -> bool {
        matches!(self, Self::UnsupportedVersion | Self::Corrupt)
    }
}

/// Read-only, deep diagnostic result for one daily-Kline symbol file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyKlineCacheDiagnosticReport {
    pub format_id: &'static str,
    pub cache_dir: PathBuf,
    pub namespace_dir: PathBuf,
    pub path: PathBuf,
    pub symbol: String,
    pub status: DailyKlineCacheDiagnosticStatus,
    pub schema_version: Option<u32>,
    pub rows: usize,
    pub cached_ranges: Vec<(i64, i64)>,
    pub size_bytes: u64,
    pub error: Option<String>,
}

/// Result of explicit daily-Kline symbol deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyKlineCachePurgeReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub removed: bool,
    pub removed_bytes: u64,
}

/// Independent native `1d` Kline store for local backtests.
///
/// The cache has no retention, TTL, background refresh, or automatic repair.
/// A bad file is a hard error until operators inspect it and explicitly call
/// [`Self::purge_symbol`].
#[derive(Debug, Clone)]
pub struct DailyKlineCache {
    root_dir: PathBuf,
    read_only: bool,
}

impl DailyKlineCache {
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let cache = Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            read_only: false,
        };
        fs::create_dir_all(cache.namespace_dir())?;
        Ok(cache)
    }

    /// Opens cache for read-only inspection without creating directories.
    #[must_use]
    pub fn open_read_only(root_dir: impl AsRef<Path>) -> Self {
        Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            read_only: true,
        }
    }

    #[must_use]
    pub fn root_dir(&self) -> &Path {
        self.root_dir.as_path()
    }

    #[must_use]
    pub fn namespace_dir(&self) -> PathBuf {
        self.root_dir.join(ROOT_DIR_NAME)
    }

    #[must_use]
    pub fn format_id(&self) -> &'static str {
        DAILY_KLINE_CACHE_FORMAT_ID
    }

    #[must_use]
    pub fn schema_version(&self) -> u32 {
        DAILY_KLINE_CACHE_SCHEMA_VERSION
    }

    /// Returns the single native daily-Kline file owned by a logical symbol.
    #[must_use]
    pub fn symbol_file_path(&self, symbol: impl AsRef<str>) -> PathBuf {
        self.namespace_dir().join(format!(
            "{}.{}",
            escaped_symbol(symbol.as_ref()),
            FILE_EXTENSION
        ))
    }

    /// Inspects final coverage without falling back to a cache miss on any
    /// incompatible, corrupt, or unsupported file.
    pub fn inspect(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &DailyKlineCacheSnapshot,
    ) -> Result<DailyKlineCacheStatus> {
        validate_range(range_start_ns, range_end_ns)?;
        validate_snapshot(snapshot)?;
        let symbol = symbol.as_ref();
        validate_symbol(symbol)?;
        let path = self.symbol_file_path(symbol);
        let file = match load_file(self.root_dir.as_path(), path.as_path(), symbol, snapshot)? {
            Some(file) => file,
            None => {
                return Ok(DailyKlineCacheStatus {
                    format_id: self.format_id(),
                    cache_dir: self.root_dir.clone(),
                    namespace_dir: self.namespace_dir(),
                    path,
                    symbol: symbol.to_string(),
                    range_start_ns,
                    range_end_ns,
                    rows: 0,
                    cached_ranges: Vec::new(),
                    missing_ranges: vec![(range_start_ns, range_end_ns)],
                });
            }
        };
        let cached_ranges =
            intersect_ranges(file.coverage.as_slice(), (range_start_ns, range_end_ns));
        Ok(DailyKlineCacheStatus {
            format_id: self.format_id(),
            cache_dir: self.root_dir.clone(),
            namespace_dir: self.namespace_dir(),
            path,
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            rows: file.rows.len(),
            missing_ranges: missing_ranges(
                cached_ranges.as_slice(),
                (range_start_ns, range_end_ns),
            ),
            cached_ranges,
        })
    }

    pub fn coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &DailyKlineCacheSnapshot,
    ) -> Result<DailyKlineCoverage> {
        let status = self.inspect(symbol, range_start_ns, range_end_ns, snapshot)?;
        Ok(DailyKlineCoverage {
            cache_dir: status.cache_dir,
            symbol: status.symbol,
            range_start_ns,
            range_end_ns,
            cached_ranges: status.cached_ranges,
            missing_ranges: status.missing_ranges,
        })
    }

    /// Stores terminally confirmed native `1d` rows and final coverage.
    ///
    /// Current and future CST trading days are rejected even when server chart
    /// happened to return rows; their daily bar is not final yet.
    pub fn store_final_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &DailyKlineCacheSnapshot,
        rows: &[Kline],
    ) -> Result<DailyKlineCacheWriteReport> {
        let now_ns = Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            DataError::InvalidResponse("daily kline cache current timestamp overflow".to_string())
        })?;
        self.store_final_range_at(
            symbol.as_ref(),
            range_start_ns,
            range_end_ns,
            snapshot,
            rows,
            now_ns,
        )
    }

    pub(crate) fn store_final_range_at(
        &self,
        symbol: &str,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &DailyKlineCacheSnapshot,
        rows: &[Kline],
        now_ns: i64,
    ) -> Result<DailyKlineCacheWriteReport> {
        self.ensure_writable()?;
        validate_range(range_start_ns, range_end_ns)?;
        validate_snapshot(snapshot)?;
        validate_symbol(symbol)?;
        let current_day = backtest_tick_trading_day_for_timestamp_ns(now_ns)?;
        let current_range = backtest_tick_trading_day_range(current_day)?;
        if range_end_ns > current_range.start_ns {
            return Err(DataError::Validation(
                "daily kline cache may only claim final coverage before current CST trading day"
                    .to_string(),
            ));
        }
        if rows
            .iter()
            .any(|row| row.datetime < range_start_ns || row.datetime >= range_end_ns)
        {
            return Err(DataError::InvalidResponse(
                "daily kline cache rows must stay inside stored final coverage".to_string(),
            ));
        }

        let path = self.symbol_file_path(symbol);
        let _lock = SymbolFileLock::acquire(path.as_path())?;
        let existing = load_file(self.root_dir.as_path(), path.as_path(), symbol, snapshot)?;
        let mut rows_by_datetime = existing
            .as_ref()
            .map(|file| {
                file.rows
                    .iter()
                    .cloned()
                    .map(|row| (row.datetime, row))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        for row in rows {
            rows_by_datetime.insert(row.datetime, row.clone());
        }
        let rows = rows_by_datetime.into_values().collect::<Vec<_>>();
        let mut coverage = existing.map_or_else(Vec::new, |file| file.coverage);
        coverage.push((range_start_ns, range_end_ns));
        let coverage = merge_ranges(coverage)?;
        let file = DailyFile {
            symbol: symbol.to_string(),
            snapshot: snapshot.clone(),
            coverage,
            rows,
        };
        validate_file(&file)?;
        write_file_atomically(path.as_path(), &file)?;
        Ok(DailyKlineCacheWriteReport {
            cache_dir: self.root_dir.clone(),
            path,
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            rows: file.rows.len(),
            cached_ranges: file.coverage,
        })
    }

    /// Reads a complete final-covered range. Incomplete coverage is never
    /// silently represented as an empty row set.
    pub fn read_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &DailyKlineCacheSnapshot,
    ) -> Result<Vec<Kline>> {
        let status = self.inspect(symbol.as_ref(), range_start_ns, range_end_ns, snapshot)?;
        if !status.is_complete() {
            return Err(DataError::InvalidState(
                "daily kline cache coverage incomplete",
            ));
        }
        let Some(file) = load_file(
            self.root_dir.as_path(),
            status.path.as_path(),
            symbol.as_ref(),
            snapshot,
        )?
        else {
            return Err(DataError::InvalidState(
                "daily kline cache coverage disappeared during read",
            ));
        };
        Ok(file
            .rows
            .into_iter()
            .filter(|row| row.datetime >= range_start_ns && row.datetime < range_end_ns)
            .collect())
    }

    /// Reads and validates one symbol file without modifying it.
    pub fn diagnose(&self, symbol: impl AsRef<str>) -> Result<DailyKlineCacheDiagnosticReport> {
        let symbol = symbol.as_ref();
        validate_symbol(symbol)?;
        let path = self.symbol_file_path(symbol);
        let metadata = match fs::metadata(path.as_path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DailyKlineCacheDiagnosticReport {
                    format_id: self.format_id(),
                    cache_dir: self.root_dir.clone(),
                    namespace_dir: self.namespace_dir(),
                    path,
                    symbol: symbol.to_string(),
                    status: DailyKlineCacheDiagnosticStatus::Missing,
                    schema_version: None,
                    rows: 0,
                    cached_ranges: Vec::new(),
                    size_bytes: 0,
                    error: None,
                });
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() {
            return Ok(diagnostic_problem(
                self,
                path,
                symbol,
                DailyKlineCacheDiagnosticStatus::Corrupt,
                metadata.len(),
                "daily kline cache path is not a regular file".to_string(),
            ));
        }
        match load_file_unchecked(path.as_path()) {
            Ok(file) if file.symbol == symbol => Ok(DailyKlineCacheDiagnosticReport {
                format_id: self.format_id(),
                cache_dir: self.root_dir.clone(),
                namespace_dir: self.namespace_dir(),
                path,
                symbol: symbol.to_string(),
                status: DailyKlineCacheDiagnosticStatus::Readable,
                schema_version: Some(DAILY_KLINE_CACHE_SCHEMA_VERSION),
                rows: file.rows.len(),
                cached_ranges: file.coverage,
                size_bytes: metadata.len(),
                error: None,
            }),
            Ok(_) => Ok(diagnostic_problem(
                self,
                path,
                symbol,
                DailyKlineCacheDiagnosticStatus::Corrupt,
                metadata.len(),
                "daily kline cache symbol does not match file path".to_string(),
            )),
            Err(error) => {
                let status = if error
                    .to_string()
                    .contains("unsupported daily kline cache version")
                {
                    DailyKlineCacheDiagnosticStatus::UnsupportedVersion
                } else {
                    DailyKlineCacheDiagnosticStatus::Corrupt
                };
                Ok(diagnostic_problem(
                    self,
                    path,
                    symbol,
                    status,
                    metadata.len(),
                    error.to_string(),
                ))
            }
        }
    }

    /// Explicit destructive repair. It removes whole logical-symbol file only.
    pub fn purge_symbol(&self, symbol: impl AsRef<str>) -> Result<DailyKlineCachePurgeReport> {
        self.ensure_writable()?;
        let symbol = symbol.as_ref();
        validate_symbol(symbol)?;
        let path = self.symbol_file_path(symbol);
        let _lock = SymbolFileLock::acquire(path.as_path())?;
        match fs::metadata(path.as_path()) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(DataError::InvalidResponse(
                        "daily kline cache purge target is not a regular file".to_string(),
                    ));
                }
                fs::remove_file(path)?;
                Ok(DailyKlineCachePurgeReport {
                    cache_dir: self.root_dir.clone(),
                    symbol: symbol.to_string(),
                    removed: true,
                    removed_bytes: metadata.len(),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(DailyKlineCachePurgeReport {
                    cache_dir: self.root_dir.clone(),
                    symbol: symbol.to_string(),
                    removed: false,
                    removed_bytes: 0,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(DataError::InvalidState("daily kline cache is read-only"));
        }
        Ok(())
    }
}

struct DailyFile {
    symbol: String,
    snapshot: DailyKlineCacheSnapshot,
    coverage: Vec<(i64, i64)>,
    rows: Vec<Kline>,
}

struct SymbolFileLock {
    file: File,
}

impl SymbolFileLock {
    fn acquire(data_path: &Path) -> Result<Self> {
        let parent = data_path.parent().ok_or_else(|| {
            DataError::InvalidState("daily kline cache file has no parent directory")
        })?;
        fs::create_dir_all(parent)?;
        let lock_path = data_path.with_extension(format!("{FILE_EXTENSION}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }
}

impl Drop for SymbolFileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn diagnostic_problem(
    cache: &DailyKlineCache,
    path: PathBuf,
    symbol: &str,
    status: DailyKlineCacheDiagnosticStatus,
    size_bytes: u64,
    error: String,
) -> DailyKlineCacheDiagnosticReport {
    DailyKlineCacheDiagnosticReport {
        format_id: cache.format_id(),
        cache_dir: cache.root_dir.clone(),
        namespace_dir: cache.namespace_dir(),
        path,
        symbol: symbol.to_string(),
        status,
        schema_version: None,
        rows: 0,
        cached_ranges: Vec::new(),
        size_bytes,
        error: Some(error),
    }
}

fn load_file(
    cache_dir: &Path,
    path: &Path,
    expected_symbol: &str,
    expected_snapshot: &DailyKlineCacheSnapshot,
) -> Result<Option<DailyFile>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(DataError::InvalidResponse(
                    "daily kline cache path is not a regular file".to_string(),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let file = load_file_unchecked(path)?;
    if file.symbol != expected_symbol {
        return Err(DataError::InvalidResponse(
            "daily kline cache symbol does not match file path".to_string(),
        ));
    }
    if !minute_cache_snapshots_are_compatible(
        cache_dir,
        expected_symbol,
        &file.snapshot,
        expected_snapshot,
        file.coverage.as_slice(),
    )? {
        return Err(DataError::InvalidResponse(
            "daily kline cache metadata snapshot mismatch".to_string(),
        ));
    }
    Ok(Some(file))
}

fn load_file_unchecked(path: &Path) -> Result<DailyFile> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < FILE_HEADER_BYTES {
        return Err(DataError::InvalidResponse(
            "daily kline cache file is shorter than header".to_string(),
        ));
    }
    if bytes[..4] != FILE_MAGIC {
        return Err(DataError::InvalidResponse(
            "daily kline cache magic mismatch".to_string(),
        ));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("fixed header slice"));
    if version != FILE_VERSION {
        return Err(DataError::InvalidResponse(format!(
            "unsupported daily kline cache version {version}"
        )));
    }
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed header slice"));
    let checksum = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed header slice"));
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        DataError::InvalidResponse("daily kline cache payload length overflows usize".to_string())
    })?;
    if bytes.len() != FILE_HEADER_BYTES.saturating_add(payload_len) {
        return Err(DataError::InvalidResponse(
            "daily kline cache payload length does not match file size".to_string(),
        ));
    }
    let payload = &bytes[FILE_HEADER_BYTES..];
    if fnv1a(payload) != checksum {
        return Err(DataError::InvalidResponse(
            "daily kline cache checksum mismatch".to_string(),
        ));
    }
    let mut cursor = 0usize;
    let symbol = read_string(payload, &mut cursor)?;
    let snapshot = DailyKlineCacheSnapshot::new(
        read_u32(payload, &mut cursor)?,
        read_string(payload, &mut cursor)?,
        read_string(payload, &mut cursor)?,
    )?;
    let coverage_len = read_len(payload, &mut cursor, MAX_COVERAGE_RECORDS, "coverage")?;
    let mut coverage = Vec::with_capacity(coverage_len);
    for _ in 0..coverage_len {
        coverage.push((
            read_i64(payload, &mut cursor)?,
            read_i64(payload, &mut cursor)?,
        ));
    }
    let rows_len = read_len(payload, &mut cursor, MAX_ROWS, "rows")?;
    let row_bytes = rows_len.checked_mul(KLINE_ROW_BYTES).ok_or_else(|| {
        DataError::InvalidResponse("daily kline cache row payload overflows usize".to_string())
    })?;
    if payload.len().saturating_sub(cursor) != row_bytes {
        return Err(DataError::InvalidResponse(
            "daily kline cache row payload width mismatch".to_string(),
        ));
    }
    let mut rows = Vec::with_capacity(rows_len);
    for _ in 0..rows_len {
        let epoch = read_i64(payload, &mut cursor)?;
        rows.push(Kline {
            id: read_i64(payload, &mut cursor)?,
            datetime: read_i64(payload, &mut cursor)?,
            open: read_f64(payload, &mut cursor)?,
            high: read_f64(payload, &mut cursor)?,
            low: read_f64(payload, &mut cursor)?,
            close: read_f64(payload, &mut cursor)?,
            volume: read_i64(payload, &mut cursor)?,
            open_oi: read_i64(payload, &mut cursor)?,
            close_oi: read_i64(payload, &mut cursor)?,
            epoch: (epoch != NONE_EPOCH).then_some(epoch),
        });
    }
    let file = DailyFile {
        symbol,
        snapshot,
        coverage,
        rows,
    };
    validate_file(&file)?;
    Ok(file)
}

fn write_file_atomically(path: &Path, file: &DailyFile) -> Result<()> {
    let payload = encode_file(file)?;
    let mut bytes = Vec::with_capacity(FILE_HEADER_BYTES.saturating_add(payload.len()));
    bytes.extend_from_slice(&FILE_MAGIC);
    bytes.extend_from_slice(&FILE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&fnv1a(payload.as_slice()).to_le_bytes());
    bytes.extend_from_slice(payload.as_slice());
    let parent = path
        .parent()
        .ok_or_else(|| DataError::InvalidState("daily kline cache file has no parent directory"))?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!(
        "{FILE_EXTENSION}.tmp-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let result = (|| -> Result<()> {
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temp.as_path())?;
        output.write_all(bytes.as_slice())?;
        output.sync_all()?;
        drop(output);
        fs::rename(temp.as_path(), path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

fn encode_file(file: &DailyFile) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    write_string(&mut payload, file.symbol.as_str())?;
    payload.extend_from_slice(&file.snapshot.version.to_le_bytes());
    write_string(&mut payload, file.snapshot.calendar_hash.as_str())?;
    write_string(&mut payload, file.snapshot.session_hash.as_str())?;
    write_len(&mut payload, file.coverage.len(), "coverage")?;
    for &(start_ns, end_ns) in &file.coverage {
        payload.extend_from_slice(&start_ns.to_le_bytes());
        payload.extend_from_slice(&end_ns.to_le_bytes());
    }
    write_len(&mut payload, file.rows.len(), "rows")?;
    for row in &file.rows {
        payload.extend_from_slice(&row.epoch.unwrap_or(NONE_EPOCH).to_le_bytes());
        payload.extend_from_slice(&row.id.to_le_bytes());
        payload.extend_from_slice(&row.datetime.to_le_bytes());
        payload.extend_from_slice(&row.open.to_le_bytes());
        payload.extend_from_slice(&row.high.to_le_bytes());
        payload.extend_from_slice(&row.low.to_le_bytes());
        payload.extend_from_slice(&row.close.to_le_bytes());
        payload.extend_from_slice(&row.volume.to_le_bytes());
        payload.extend_from_slice(&row.open_oi.to_le_bytes());
        payload.extend_from_slice(&row.close_oi.to_le_bytes());
    }
    Ok(payload)
}

fn validate_file(file: &DailyFile) -> Result<()> {
    validate_symbol(file.symbol.as_str())?;
    validate_snapshot(&file.snapshot)?;
    if file.coverage.len() > MAX_COVERAGE_RECORDS {
        return Err(DataError::InvalidResponse(
            "daily kline cache has too many coverage records".to_string(),
        ));
    }
    if file.rows.len() > MAX_ROWS {
        return Err(DataError::InvalidResponse(
            "daily kline cache has too many rows".to_string(),
        ));
    }
    let coverage = merge_ranges(file.coverage.clone())?;
    if coverage != file.coverage {
        return Err(DataError::InvalidResponse(
            "daily kline cache coverage is not sorted and merged".to_string(),
        ));
    }
    if file
        .rows
        .windows(2)
        .any(|pair| pair[0].datetime >= pair[1].datetime)
    {
        return Err(DataError::InvalidResponse(
            "daily kline cache rows are not strictly sorted by datetime".to_string(),
        ));
    }
    if file
        .rows
        .iter()
        .any(|row| !range_contains(file.coverage.as_slice(), row.datetime))
    {
        return Err(DataError::InvalidResponse(
            "daily kline cache row falls outside final coverage".to_string(),
        ));
    }
    Ok(())
}

fn validate_snapshot(snapshot: &DailyKlineCacheSnapshot) -> Result<()> {
    DailyKlineCacheSnapshot::new(
        snapshot.version,
        snapshot.calendar_hash.clone(),
        snapshot.session_hash.clone(),
    )
    .map(|_| ())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.trim().is_empty() || symbol == "." || symbol == ".." {
        return Err(DataError::Validation(
            "daily kline cache symbol must not be empty, . or ..".to_string(),
        ));
    }
    if symbol.len() > 160 {
        return Err(DataError::Validation(
            "daily kline cache symbol exceeds 160 bytes".to_string(),
        ));
    }
    Ok(())
}

fn validate_range(start_ns: i64, end_ns: i64) -> Result<()> {
    if start_ns >= end_ns {
        return Err(DataError::Validation(
            "daily kline cache range must satisfy start < end".to_string(),
        ));
    }
    Ok(())
}

fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Result<Vec<(i64, i64)>> {
    for &(start_ns, end_ns) in &ranges {
        validate_range(start_ns, end_ns)?;
    }
    ranges.sort_unstable();
    let mut merged = Vec::<(i64, i64)>::new();
    for range in ranges {
        match merged.last_mut() {
            Some(previous) if range.0 <= previous.1 => previous.1 = previous.1.max(range.1),
            _ => merged.push(range),
        }
    }
    Ok(merged)
}

fn intersect_ranges(ranges: &[(i64, i64)], requested: (i64, i64)) -> Vec<(i64, i64)> {
    ranges
        .iter()
        .filter_map(|&(start_ns, end_ns)| {
            let start_ns = start_ns.max(requested.0);
            let end_ns = end_ns.min(requested.1);
            (start_ns < end_ns).then_some((start_ns, end_ns))
        })
        .collect()
}

fn missing_ranges(cached: &[(i64, i64)], requested: (i64, i64)) -> Vec<(i64, i64)> {
    let mut cursor = requested.0;
    let mut missing = Vec::new();
    for &(start_ns, end_ns) in cached {
        if start_ns > cursor {
            missing.push((cursor, start_ns));
        }
        cursor = cursor.max(end_ns);
    }
    if cursor < requested.1 {
        missing.push((cursor, requested.1));
    }
    missing
}

fn range_contains(ranges: &[(i64, i64)], datetime: i64) -> bool {
    ranges
        .iter()
        .any(|&(start_ns, end_ns)| start_ns <= datetime && datetime < end_ns)
}

fn escaped_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .map(|character| match character {
            '/' | '\\' | ':' | '\0' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect()
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() > MAX_STRING_BYTES {
        return Err(DataError::Validation(
            "daily kline cache string exceeds maximum length".to_string(),
        ));
    }
    output.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn write_len(output: &mut Vec<u8>, value: usize, field: &str) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        DataError::Validation(format!("daily kline cache {field} count exceeds u32"))
    })?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_string(input: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_u32(input, cursor)? as usize;
    if len > MAX_STRING_BYTES {
        return Err(DataError::InvalidResponse(
            "daily kline cache string exceeds maximum length".to_string(),
        ));
    }
    let bytes = read_bytes(input, cursor, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        DataError::InvalidResponse("daily kline cache string is not UTF-8".to_string())
    })
}

fn read_len(input: &[u8], cursor: &mut usize, max: usize, field: &str) -> Result<usize> {
    let value = read_u32(input, cursor)? as usize;
    if value > max {
        return Err(DataError::InvalidResponse(format!(
            "daily kline cache {field} count exceeds maximum"
        )));
    }
    Ok(value)
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        read_bytes(input, cursor, 4)?
            .try_into()
            .expect("fixed u32 slice"),
    ))
}

fn read_i64(input: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(
        read_bytes(input, cursor, 8)?
            .try_into()
            .expect("fixed i64 slice"),
    ))
}

fn read_f64(input: &[u8], cursor: &mut usize) -> Result<f64> {
    Ok(f64::from_le_bytes(
        read_bytes(input, cursor, 8)?
            .try_into()
            .expect("fixed f64 slice"),
    ))
}

fn read_bytes<'a>(input: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.checked_add(len).ok_or_else(|| {
        DataError::InvalidResponse("daily kline cache cursor overflow".to_string())
    })?;
    let bytes = input.get(*cursor..end).ok_or_else(|| {
        DataError::InvalidResponse("daily kline cache file is truncated".to_string())
    })?;
    *cursor = end;
    Ok(bytes)
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}
