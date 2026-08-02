//! Canonical, monthly-partitioned 60-second Kline cache for local backtests.
//!
//! This cache deliberately does not reuse daily TQBN history-series files.  A
//! 60-second Kline is the durable canonical Kline input for the v4 backtest
//! path; higher periods are derived by `tqsdk-task` at replay time.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use fs2::FileExt;
use tqsdk_core::Kline;

use crate::backtest_tick_cache::{
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};
use crate::{DataError, Result};

/// The only durable Kline period accepted by [`MinuteKlineCache`].
pub const MINUTE_KLINE_DURATION_NS: i64 = 60_000_000_000;

/// Stable identity for the independent v4 monthly-minute cache format.
pub const MINUTE_KLINE_CACHE_FORMAT_ID: &str = "tqsdk.minute-kline.monthly.v4";

/// Public format version stored in every monthly-minute file.
pub const MINUTE_KLINE_CACHE_SCHEMA_VERSION: u32 = 4;

const ROOT_DIR_NAME: &str = "minute-kline-v3";
const FILE_EXTENSION: &str = "tqmk";
const FILE_MAGIC: [u8; 4] = *b"TQMK";
const FILE_VERSION: u16 = 4;
const FILE_HEADER_BYTES: usize = 36;
const COVERAGE_BYTES: usize = 16;
const KLINE_ROW_BYTES: usize = 80;
const MAX_METADATA_BYTES: usize = 32 * 1024;
const MAX_COVERAGE_RECORDS: usize = 100_000;
const MAX_ROWS_PER_MONTH: usize = 2_000_000;
const NONE_EPOCH: i64 = i64::MIN;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(test)]
std::thread_local! {
    static TEST_MONTH_SCAN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Compatibility identity for a cache file's calendar and session definition.
///
/// A reader must provide the same snapshot that wrote a month.  A mismatch is
/// an error, never a best-effort cache miss: aggregation boundaries would no
/// longer be trustworthy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheSnapshot {
    pub version: u32,
    pub calendar_hash: String,
    pub session_hash: String,
}

impl MinuteKlineCacheSnapshot {
    pub fn new(
        version: u32,
        calendar_hash: impl Into<String>,
        session_hash: impl Into<String>,
    ) -> Result<Self> {
        let snapshot = Self {
            version,
            calendar_hash: calendar_hash.into(),
            session_hash: session_hash.into(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Stable fallback for markets that use the repository-wide CST trading-day
    /// convention and no instrument-specific session override.
    #[must_use]
    pub fn cst_v1() -> Self {
        Self {
            version: 1,
            calendar_hash: "cst-trading-day-v1".to_string(),
            session_hash: "cst-trading-day-v1".to_string(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version == 0 {
            return Err(DataError::Validation(
                "minute kline cache snapshot version must be greater than zero".to_string(),
            ));
        }
        validate_metadata_string("calendar hash", self.calendar_hash.as_str())?;
        validate_metadata_string("session hash", self.session_hash.as_str())
    }
}

/// Final coverage for one requested 60-second Kline range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCoverage {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
}

impl MinuteKlineCoverage {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// One monthly file participating in a write or inspection result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheMonthReport {
    pub trading_month: String,
    pub path: PathBuf,
    pub present: bool,
    pub rows: usize,
    pub cached_ranges: Vec<(i64, i64)>,
}

/// Result of one final 60-second range write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheWriteReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub rows: usize,
    pub months: Vec<MinuteKlineCacheMonthReport>,
}

/// Typed inspection result for the monthly-minute namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheStatus {
    pub format_id: &'static str,
    pub cache_dir: PathBuf,
    pub namespace_dir: PathBuf,
    pub symbol: String,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub cached_ranges: Vec<(i64, i64)>,
    pub missing_ranges: Vec<(i64, i64)>,
    pub months: Vec<MinuteKlineCacheMonthReport>,
}

impl MinuteKlineCacheStatus {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// Internal compatibility scan for an explicit stale-partition repair.
///
/// Normal cache reads intentionally stop at the first snapshot mismatch. This
/// scan instead records only exact snapshot mismatches so an opt-in operator
/// repair can remove those monthly partitions before retrying the ordinary,
/// fail-closed read path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MinuteKlineCacheSnapshotCompatibility {
    pub(crate) mismatched_ranges: Vec<(i64, i64)>,
}

/// Result of explicit v4 monthly-minute cache deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCachePurgeReport {
    pub cache_dir: PathBuf,
    pub symbol: String,
    pub requested_range: Option<(i64, i64)>,
    pub removed_files: usize,
    pub removed_bytes: u64,
    pub removed_months: Vec<String>,
}

/// One logical symbol summarized by a filesystem-only minute-cache inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheInventorySymbol {
    pub symbol: String,
    pub files: usize,
    pub bytes: u64,
    pub months: Vec<String>,
}

/// Fast, best-effort filesystem inventory for the minute-cache namespace.
///
/// It does not open or decode cache files, and never creates the cache root.
/// Use [`MinuteKlineCache::diagnose`] when a stable, deep validation pass is
/// required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheInventory {
    pub format_id: &'static str,
    pub cache_dir: PathBuf,
    pub namespace_dir: PathBuf,
    pub total_files: usize,
    pub total_bytes: u64,
    pub symbols: Vec<MinuteKlineCacheInventorySymbol>,
}

/// Deep validation classification for one monthly-minute file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinuteKlineCacheDiagnosticStatus {
    /// A current-format file decoded, validated, and passed its checksum.
    Readable,
    /// A v3 file is deliberately not readable or writable by the v4 cache.
    LegacyUnsupported,
    /// The header uses an unknown future or otherwise unsupported version.
    UnsupportedVersion,
    /// The path or file contents are malformed, truncated, or inconsistent.
    Corrupt,
}

impl MinuteKlineCacheDiagnosticStatus {
    #[must_use]
    pub fn is_problem(self) -> bool {
        !matches!(self, Self::Readable)
    }
}

/// Deep diagnostic details for one monthly-minute file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheDiagnosticFile {
    pub path: PathBuf,
    pub trading_month: String,
    pub symbol: String,
    pub status: MinuteKlineCacheDiagnosticStatus,
    pub schema_version: Option<u32>,
    pub rows: usize,
    pub cached_ranges: Vec<(i64, i64)>,
    pub size_bytes: u64,
    pub error: Option<String>,
}

/// Result of a deep, read-only minute-cache integrity pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineCacheDiagnosticReport {
    pub format_id: &'static str,
    pub cache_dir: PathBuf,
    pub namespace_dir: PathBuf,
    pub total_files: usize,
    pub total_bytes: u64,
    pub problem_files: usize,
    pub files: Vec<MinuteKlineCacheDiagnosticFile>,
}

/// Independent v4 store for canonical 60-second Kline history.
///
/// It has no automatic retention or max-byte eviction.  Callers may explicitly
/// use [`Self::purge_range`] or [`Self::purge_symbol`] for destructive
/// maintenance.  Every store mutation atomically rewrites one monthly file;
/// this also performs non-destructive compaction after a successful final fill.
#[derive(Debug, Clone)]
pub struct MinuteKlineCache {
    root_dir: PathBuf,
    read_only: bool,
}

impl MinuteKlineCache {
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let cache = Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            read_only: false,
        };
        fs::create_dir_all(cache.namespace_dir())?;
        Ok(cache)
    }

    /// Open a namespace for inspection without creating directories or files.
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
        MINUTE_KLINE_CACHE_FORMAT_ID
    }

    #[must_use]
    pub fn schema_version(&self) -> u32 {
        MINUTE_KLINE_CACHE_SCHEMA_VERSION
    }

    /// Return a filesystem-only inventory without creating the cache root.
    pub fn fast_inventory(&self) -> Result<MinuteKlineCacheInventory> {
        let mut symbols = BTreeMap::<String, MinuteKlineCacheInventorySymbol>::new();
        let mut total_files = 0usize;
        let mut total_bytes = 0u64;
        for entry in self.month_files()? {
            let metadata = fs::metadata(entry.path.as_path())?;
            let symbol = symbol_from_file_path(entry.path.as_path())
                .unwrap_or_else(|_| entry.file_stem.clone());
            let record =
                symbols
                    .entry(symbol.clone())
                    .or_insert_with(|| MinuteKlineCacheInventorySymbol {
                        symbol,
                        files: 0,
                        bytes: 0,
                        months: Vec::new(),
                    });
            record.files = record.files.saturating_add(1);
            record.bytes = record.bytes.saturating_add(metadata.len());
            record.months.push(entry.trading_month);
            total_files = total_files.saturating_add(1);
            total_bytes = total_bytes.saturating_add(metadata.len());
        }
        let mut symbols = symbols.into_values().collect::<Vec<_>>();
        for symbol in &mut symbols {
            symbol.months.sort();
            symbol.months.dedup();
        }
        Ok(MinuteKlineCacheInventory {
            format_id: self.format_id(),
            cache_dir: self.root_dir.clone(),
            namespace_dir: self.namespace_dir(),
            total_files,
            total_bytes,
            symbols,
        })
    }

    /// Decode and validate every current-format monthly file without writing.
    ///
    /// Legacy v3 files and malformed files are reported as problems rather
    /// than being upgraded, replaced, or removed.  This preserves the cache's
    /// fail-closed contract and leaves destructive maintenance explicit.
    pub fn diagnose(&self) -> Result<MinuteKlineCacheDiagnosticReport> {
        let mut files = Vec::new();
        let mut total_bytes = 0u64;
        for entry in self.month_files()? {
            let size_bytes = fs::metadata(entry.path.as_path())?.len();
            total_bytes = total_bytes.saturating_add(size_bytes);
            files.push(diagnose_month_file(entry, size_bytes));
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let problem_files = files.iter().filter(|file| file.status.is_problem()).count();
        Ok(MinuteKlineCacheDiagnosticReport {
            format_id: self.format_id(),
            cache_dir: self.root_dir.clone(),
            namespace_dir: self.namespace_dir(),
            total_files: files.len(),
            total_bytes,
            problem_files,
            files,
        })
    }

    /// Return the fixed path for `symbol × trading-YYYYMM`.
    #[must_use]
    pub fn month_file_path(&self, symbol: &str, trading_month: &str) -> PathBuf {
        self.month_file_path_unchecked(symbol, trading_month)
    }

    pub fn coverage(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<MinuteKlineCoverage> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        snapshot.validate()?;

        let mut cached_ranges = Vec::new();
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            let Some(scan) = scan_existing_month(
                path.as_path(),
                symbol,
                slice.trading_month.as_str(),
                snapshot,
            )?
            else {
                continue;
            };
            cached_ranges.extend(
                scan.coverage
                    .into_iter()
                    .filter_map(|range| intersect_ranges(range, (slice.start_ns, slice.end_ns))),
            );
        }

        let cached_ranges = merge_ranges(cached_ranges);
        let missing_ranges = missing_ranges(range_start_ns, range_end_ns, &cached_ranges);
        Ok(MinuteKlineCoverage {
            cache_dir: self.root_dir.clone(),
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            cached_ranges,
            missing_ranges,
        })
    }

    pub fn inspect(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<MinuteKlineCacheStatus> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        snapshot.validate()?;

        let mut cached_ranges = Vec::new();
        let mut months = Vec::new();
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            let scan = scan_existing_month(
                path.as_path(),
                symbol,
                slice.trading_month.as_str(),
                snapshot,
            )?;
            months.push(match scan {
                Some(scan) => {
                    cached_ranges.extend(scan.coverage.iter().filter_map(|range| {
                        intersect_ranges(*range, (slice.start_ns, slice.end_ns))
                    }));
                    MinuteKlineCacheMonthReport {
                        trading_month: slice.trading_month,
                        path,
                        present: true,
                        rows: scan.rows,
                        cached_ranges: scan.coverage,
                    }
                }
                None => MinuteKlineCacheMonthReport {
                    trading_month: slice.trading_month,
                    path,
                    present: false,
                    rows: 0,
                    cached_ranges: Vec::new(),
                },
            });
        }
        let cached_ranges = merge_ranges(cached_ranges);
        let missing_ranges = missing_ranges(range_start_ns, range_end_ns, &cached_ranges);
        Ok(MinuteKlineCacheStatus {
            format_id: self.format_id(),
            cache_dir: self.root_dir.clone(),
            namespace_dir: self.namespace_dir(),
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            cached_ranges,
            missing_ranges,
            months,
        })
    }

    pub(crate) fn snapshot_compatibility(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<MinuteKlineCacheSnapshotCompatibility> {
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        snapshot.validate()?;

        let mut mismatched_ranges = Vec::new();
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            match scan_existing_month(
                path.as_path(),
                symbol,
                slice.trading_month.as_str(),
                snapshot,
            ) {
                Ok(Some(_)) | Ok(None) => {}
                Err(error) if is_snapshot_mismatch(&error) => {
                    mismatched_ranges.push((slice.start_ns, slice.end_ns));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(MinuteKlineCacheSnapshotCompatibility { mismatched_ranges })
    }

    /// Store a server-confirmed final 60-second range.
    ///
    /// A range touching the current CST trading day is rejected.  The v4 cache
    /// intentionally has no provisional-minute coverage: a future fill must
    /// revisit that day after it closes before claiming a cache hit.
    pub fn store_final_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
        rows: &[Kline],
    ) -> Result<MinuteKlineCacheWriteReport> {
        let now_ns = Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            DataError::InvalidResponse("minute kline cache current timestamp overflow".to_string())
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

    /// Open a bounded-memory reader after verifying final coverage.
    ///
    /// Opening validates every selected monthly file in a streaming pass.  The
    /// returned reader then streams rows from the files without materializing a
    /// month in memory.
    pub fn open_reader(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<MinuteKlineReader> {
        let symbol = symbol.as_ref();
        let coverage = self.coverage(symbol, range_start_ns, range_end_ns, snapshot)?;
        if !coverage.is_complete() {
            return Err(DataError::InvalidState(
                "minute kline cache coverage is incomplete",
            ));
        }

        let paths = split_trading_month_range(range_start_ns, range_end_ns)?
            .into_iter()
            .map(|slice| {
                let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
                (slice.trading_month, path)
            })
            .collect();
        Ok(MinuteKlineReader {
            cache_dir: self.root_dir.clone(),
            read_only: self.read_only,
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            snapshot: snapshot.clone(),
            paths,
            next_path: 0,
            current: None,
        })
    }

    /// Convenience materialization on top of [`Self::open_reader`].
    pub fn read_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<Vec<Kline>> {
        let mut reader = self.open_reader(symbol, range_start_ns, range_end_ns, snapshot)?;
        let mut rows = Vec::new();
        while let Some(row) = reader.next_kline()? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Delete only monthly files intersecting the requested range.
    ///
    /// This is the primitive used by a `Refresh` policy.  It intentionally
    /// removes whole matching monthly partitions, never unrelated months.
    pub fn purge_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
    ) -> Result<MinuteKlineCachePurgeReport> {
        self.ensure_writable()?;
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;

        let mut removed_files = 0usize;
        let mut removed_bytes = 0u64;
        let mut removed_months = Vec::new();
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            let _lock = MonthFileLock::acquire(path.as_path(), self.root_dir.as_path())?;
            match fs::metadata(path.as_path()) {
                Ok(metadata) => {
                    if !metadata.is_file() {
                        return Err(format_error(path.as_path(), "path is not a regular file"));
                    }
                    fs::remove_file(path.as_path())?;
                    removed_files = removed_files.saturating_add(1);
                    removed_bytes = removed_bytes.saturating_add(metadata.len());
                    removed_months.push(slice.trading_month);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(MinuteKlineCachePurgeReport {
            cache_dir: self.root_dir.clone(),
            symbol: symbol.to_string(),
            requested_range: Some((range_start_ns, range_end_ns)),
            removed_files,
            removed_bytes,
            removed_months,
        })
    }

    /// Explicitly delete every monthly-minute partition for a symbol.
    pub fn purge_symbol(&self, symbol: impl AsRef<str>) -> Result<MinuteKlineCachePurgeReport> {
        self.ensure_writable()?;
        let symbol = symbol.as_ref();
        validate_symbol(symbol)?;
        let namespace = self.namespace_dir();
        let entries = match fs::read_dir(namespace.as_path()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(MinuteKlineCachePurgeReport {
                    cache_dir: self.root_dir.clone(),
                    symbol: symbol.to_string(),
                    requested_range: None,
                    removed_files: 0,
                    removed_bytes: 0,
                    removed_months: Vec::new(),
                });
            }
            Err(error) => return Err(error.into()),
        };

        let mut removed_files = 0usize;
        let mut removed_bytes = 0u64;
        let mut removed_months = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let directory = entry.file_name().to_string_lossy().into_owned();
            let Some(trading_month) = directory.strip_prefix("trading-") else {
                continue;
            };
            if !is_trading_month(trading_month) {
                continue;
            }
            let path = self.month_file_path_unchecked(symbol, trading_month);
            let _lock = MonthFileLock::acquire(path.as_path(), self.root_dir.as_path())?;
            match fs::metadata(path.as_path()) {
                Ok(metadata) => {
                    if !metadata.is_file() {
                        return Err(format_error(path.as_path(), "path is not a regular file"));
                    }
                    fs::remove_file(path.as_path())?;
                    removed_files = removed_files.saturating_add(1);
                    removed_bytes = removed_bytes.saturating_add(metadata.len());
                    removed_months.push(trading_month.to_string());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        removed_months.sort();
        Ok(MinuteKlineCachePurgeReport {
            cache_dir: self.root_dir.clone(),
            symbol: symbol.to_string(),
            requested_range: None,
            removed_files,
            removed_bytes,
            removed_months,
        })
    }

    /// Rewrite selected monthly files without changing rows or final coverage.
    pub fn compact_range(
        &self,
        symbol: impl AsRef<str>,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<()> {
        self.ensure_writable()?;
        let symbol = symbol.as_ref();
        validate_range(symbol, range_start_ns, range_end_ns)?;
        snapshot.validate()?;
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            let _lock = MonthFileLock::acquire(path.as_path(), self.root_dir.as_path())?;
            let Some(month) = load_existing_month(
                path.as_path(),
                symbol,
                slice.trading_month.as_str(),
                snapshot,
            )?
            else {
                continue;
            };
            write_month_atomically(path.as_path(), &month)?;
        }
        Ok(())
    }

    fn store_final_range_at(
        &self,
        symbol: &str,
        range_start_ns: i64,
        range_end_ns: i64,
        snapshot: &MinuteKlineCacheSnapshot,
        rows: &[Kline],
        now_ns: i64,
    ) -> Result<MinuteKlineCacheWriteReport> {
        self.ensure_writable()?;
        validate_range(symbol, range_start_ns, range_end_ns)?;
        snapshot.validate()?;
        reject_open_or_future_final_range(range_end_ns, now_ns)?;
        validate_input_rows(symbol, range_start_ns, range_end_ns, rows)?;

        let mut months = Vec::new();
        for slice in split_trading_month_range(range_start_ns, range_end_ns)? {
            let path = self.month_file_path_unchecked(symbol, slice.trading_month.as_str());
            let rows_for_month = rows
                .iter()
                .filter(|row| row.datetime >= slice.start_ns && row.datetime < slice.end_ns)
                .cloned()
                .collect::<Vec<_>>();
            let report = self.store_one_month(
                path.as_path(),
                symbol,
                slice.trading_month.as_str(),
                (slice.start_ns, slice.end_ns),
                snapshot,
                rows_for_month,
            )?;
            months.push(report);
        }
        Ok(MinuteKlineCacheWriteReport {
            cache_dir: self.root_dir.clone(),
            symbol: symbol.to_string(),
            range_start_ns,
            range_end_ns,
            rows: rows.len(),
            months,
        })
    }

    fn store_one_month(
        &self,
        path: &Path,
        symbol: &str,
        trading_month: &str,
        coverage: (i64, i64),
        snapshot: &MinuteKlineCacheSnapshot,
        incoming_rows: Vec<Kline>,
    ) -> Result<MinuteKlineCacheMonthReport> {
        let _lock = MonthFileLock::acquire(path, self.root_dir.as_path())?;
        let existing = load_existing_month(path, symbol, trading_month, snapshot)?;
        let mut rows_by_datetime = existing
            .as_ref()
            .map(|month| {
                month
                    .rows
                    .iter()
                    .cloned()
                    .map(|row| (row.datetime, row))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        for row in incoming_rows {
            rows_by_datetime.insert(row.datetime, row);
        }
        let rows = rows_by_datetime.into_values().collect::<Vec<_>>();
        validate_stored_rows(path, trading_month, rows.as_slice())?;

        let mut ranges = existing
            .as_ref()
            .map(|month| month.coverage.clone())
            .unwrap_or_default();
        ranges.push(coverage);
        let coverage = merge_ranges(ranges);
        validate_stored_coverage(path, trading_month, coverage.as_slice())?;
        let month = MonthFile {
            metadata: MonthMetadata {
                symbol: symbol.to_string(),
                trading_month: trading_month.to_string(),
                snapshot: snapshot.clone(),
            },
            coverage: coverage.clone(),
            rows,
        };
        write_month_atomically(path, &month)?;
        Ok(MinuteKlineCacheMonthReport {
            trading_month: trading_month.to_string(),
            path: path.to_path_buf(),
            present: true,
            rows: month.rows.len(),
            cached_ranges: coverage,
        })
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(DataError::InvalidState(
                "minute kline cache was opened read-only",
            ));
        }
        Ok(())
    }

    fn month_file_path_unchecked(&self, symbol: &str, trading_month: &str) -> PathBuf {
        self.namespace_dir()
            .join(format!("trading-{trading_month}"))
            .join(format!(
                "{}.{}",
                escape_symbol_path_component(symbol),
                FILE_EXTENSION
            ))
    }

    fn month_files(&self) -> Result<Vec<MonthFilePath>> {
        let namespace = self.namespace_dir();
        let entries = match fs::read_dir(namespace.as_path()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut files = Vec::new();
        for directory in entries {
            let directory = directory?;
            if !directory.file_type()?.is_dir() {
                continue;
            }
            let directory_name = directory.file_name().to_string_lossy().into_owned();
            let Some(trading_month) = directory_name.strip_prefix("trading-") else {
                continue;
            };
            if !is_trading_month(trading_month) {
                continue;
            }
            for file in fs::read_dir(directory.path())? {
                let file = file?;
                if !file.file_type()?.is_file() {
                    continue;
                }
                let file_name = file.file_name().to_string_lossy().into_owned();
                let Some(file_stem) = file_name.strip_suffix(&format!(".{FILE_EXTENSION}")) else {
                    continue;
                };
                files.push(MonthFilePath {
                    path: file.path(),
                    trading_month: trading_month.to_string(),
                    file_stem: file_stem.to_string(),
                });
            }
        }
        Ok(files)
    }
}

/// Streaming 60-second rows from final monthly-minute cache files.
pub struct MinuteKlineReader {
    cache_dir: PathBuf,
    read_only: bool,
    symbol: String,
    range_start_ns: i64,
    range_end_ns: i64,
    snapshot: MinuteKlineCacheSnapshot,
    paths: Vec<(String, PathBuf)>,
    next_path: usize,
    current: Option<LockedMonthRowReader>,
}

impl MinuteKlineReader {
    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    #[must_use]
    pub fn range_start_ns(&self) -> i64 {
        self.range_start_ns
    }

    #[must_use]
    pub fn range_end_ns(&self) -> i64 {
        self.range_end_ns
    }

    pub fn next_kline(&mut self) -> Result<Option<Kline>> {
        loop {
            if let Some(current) = self.current.as_mut() {
                match current.reader.next_row()? {
                    Some(row)
                        if row.datetime >= self.range_start_ns
                            && row.datetime < self.range_end_ns =>
                    {
                        return Ok(Some(row));
                    }
                    Some(_) => continue,
                    None => {
                        self.current = None;
                        continue;
                    }
                }
            }

            let Some((trading_month, path)) = self.paths.get(self.next_path).cloned() else {
                return Ok(None);
            };
            self.next_path = self.next_path.saturating_add(1);
            let lock_file = MonthFileLock::acquire_shared(
                path.as_path(),
                self.cache_dir.as_path(),
                self.read_only,
            )?;
            let data_file = File::open(path.as_path())?;
            self.current = Some(LockedMonthRowReader {
                lock_file,
                reader: MonthRowReader::open(
                    data_file,
                    path.as_path(),
                    self.symbol.as_str(),
                    trading_month.as_str(),
                    &self.snapshot,
                )?,
            });
        }
    }

    #[allow(dead_code)]
    pub(crate) fn next_kline_chunk(&mut self, target_bytes: usize) -> Result<Vec<Kline>> {
        if target_bytes == 0 {
            return Err(DataError::Validation(
                "minute reader chunk target_bytes must be greater than zero".to_string(),
            ));
        }
        let mut rows = Vec::new();
        let row_bytes = std::mem::size_of::<Kline>();
        while rows.is_empty() || rows.len().saturating_mul(row_bytes) < target_bytes {
            let Some(row) = self.next_kline()? else {
                break;
            };
            rows.push(row);
        }
        Ok(rows)
    }
}

struct LockedMonthRowReader {
    lock_file: File,
    reader: MonthRowReader,
}

impl Drop for LockedMonthRowReader {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
    }
}

#[derive(Debug, Clone)]
struct MonthMetadata {
    symbol: String,
    trading_month: String,
    snapshot: MinuteKlineCacheSnapshot,
}

#[derive(Debug, Clone)]
struct MonthFile {
    metadata: MonthMetadata,
    coverage: Vec<(i64, i64)>,
    rows: Vec<Kline>,
}

#[derive(Debug, Clone)]
struct MonthFilePath {
    path: PathBuf,
    trading_month: String,
    file_stem: String,
}

#[derive(Debug)]
struct MonthScan {
    metadata: MonthMetadata,
    coverage: Vec<(i64, i64)>,
    rows: usize,
}

#[derive(Debug, Clone)]
struct TradingMonthSlice {
    trading_month: String,
    start_ns: i64,
    end_ns: i64,
}

#[derive(Debug, Clone, Copy)]
struct DiskHeader {
    coverage_count: usize,
    row_count: usize,
    checksum: u64,
}

struct MonthRowReader {
    path: PathBuf,
    trading_month: String,
    reader: BufReader<File>,
    rows_remaining: usize,
    checksum: u64,
    expected_checksum: u64,
}

impl MonthRowReader {
    fn open(
        file: File,
        path: &Path,
        symbol: &str,
        trading_month: &str,
        snapshot: &MinuteKlineCacheSnapshot,
    ) -> Result<Self> {
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let (header, metadata_bytes) = read_file_prefix(&mut reader, path, file_len)?;
        let metadata = decode_metadata(path, metadata_bytes.as_slice())?;
        validate_expected_metadata(path, &metadata, symbol, trading_month, snapshot)?;
        let mut checksum = checksum_bytes(FNV_OFFSET_BASIS, metadata_bytes.as_slice());
        for _ in 0..header.coverage_count {
            let mut bytes = [0_u8; COVERAGE_BYTES];
            read_exact_format(&mut reader, &mut bytes, path, "coverage")?;
            checksum = checksum_bytes(checksum, &bytes);
        }
        Ok(Self {
            path: path.to_path_buf(),
            trading_month: trading_month.to_string(),
            reader,
            rows_remaining: header.row_count,
            checksum,
            expected_checksum: header.checksum,
        })
    }

    fn next_row(&mut self) -> Result<Option<Kline>> {
        if self.rows_remaining == 0 {
            if self.checksum != self.expected_checksum {
                return Err(format_error(
                    self.path.as_path(),
                    "payload checksum mismatch",
                ));
            }
            return Ok(None);
        }
        let mut bytes = [0_u8; KLINE_ROW_BYTES];
        read_exact_format(
            &mut self.reader,
            &mut bytes,
            self.path.as_path(),
            "Kline row",
        )?;
        self.checksum = checksum_bytes(self.checksum, &bytes);
        self.rows_remaining -= 1;
        let row = decode_kline(bytes.as_slice());
        validate_one_stored_row(self.path.as_path(), self.trading_month.as_str(), &row)?;
        Ok(Some(row))
    }
}

struct MonthFileLock {
    file: File,
}

impl MonthFileLock {
    fn acquire(path: &Path, root_dir: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let lock_path = path.with_extension(format!("{FILE_EXTENSION}.lock"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Self { file }),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(DataError::CacheBusy {
                    cache_dir: root_dir.to_path_buf(),
                    operation: "minute kline cache monthly write",
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn acquire_shared(path: &Path, root_dir: &Path, read_only: bool) -> Result<File> {
        let lock_path = path.with_extension(format!("{FILE_EXTENSION}.lock"));
        let file = if read_only {
            OpenOptions::new()
                .read(true)
                .open(lock_path)
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        DataError::InvalidState("read-only minute kline monthly lock is missing")
                    } else {
                        DataError::from(error)
                    }
                })?
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(lock_path)?
        };
        match FileExt::try_lock_shared(&file) {
            Ok(()) => Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(DataError::CacheBusy {
                    cache_dir: root_dir.to_path_buf(),
                    operation: "minute kline cache monthly read",
                })
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for MonthFileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn scan_existing_month(
    path: &Path,
    symbol: &str,
    trading_month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
) -> Result<Option<MonthScan>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(format_error(path, "path is not a regular file"));
            }
            scan_month_file(path, symbol, trading_month, snapshot).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn load_existing_month(
    path: &Path,
    symbol: &str,
    trading_month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
) -> Result<Option<MonthFile>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(format_error(path, "path is not a regular file"));
            }
            load_month_file(path, symbol, trading_month, snapshot).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn scan_month_file(
    path: &Path,
    symbol: &str,
    trading_month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
) -> Result<MonthScan> {
    let scan = scan_month_file_unchecked(path)?;
    validate_expected_metadata(path, &scan.metadata, symbol, trading_month, snapshot)?;
    Ok(scan)
}

fn scan_month_file_unchecked(path: &Path) -> Result<MonthScan> {
    #[cfg(test)]
    TEST_MONTH_SCAN_COUNT.with(|count| count.set(count.get().saturating_add(1)));

    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let (header, metadata_bytes) = read_file_prefix(&mut reader, path, file_len)?;
    let metadata = decode_metadata(path, metadata_bytes.as_slice())?;
    let mut checksum = checksum_bytes(FNV_OFFSET_BASIS, metadata_bytes.as_slice());
    let mut coverage = Vec::with_capacity(header.coverage_count);
    for _ in 0..header.coverage_count {
        let mut bytes = [0_u8; COVERAGE_BYTES];
        read_exact_format(&mut reader, &mut bytes, path, "coverage")?;
        checksum = checksum_bytes(checksum, &bytes);
        coverage.push(decode_coverage(bytes.as_slice()));
    }
    validate_stored_coverage(path, metadata.trading_month.as_str(), coverage.as_slice())?;
    for _ in 0..header.row_count {
        let mut bytes = [0_u8; KLINE_ROW_BYTES];
        read_exact_format(&mut reader, &mut bytes, path, "Kline row")?;
        checksum = checksum_bytes(checksum, &bytes);
        let row = decode_kline(bytes.as_slice());
        validate_one_stored_row(path, metadata.trading_month.as_str(), &row)?;
    }
    if checksum != header.checksum {
        return Err(format_error(path, "payload checksum mismatch"));
    }
    Ok(MonthScan {
        metadata,
        coverage,
        rows: header.row_count,
    })
}

fn diagnose_month_file(entry: MonthFilePath, size_bytes: u64) -> MinuteKlineCacheDiagnosticFile {
    let fallback_symbol =
        symbol_from_file_path(entry.path.as_path()).unwrap_or_else(|_| entry.file_stem.clone());
    let mut file = MinuteKlineCacheDiagnosticFile {
        path: entry.path.clone(),
        trading_month: entry.trading_month.clone(),
        symbol: fallback_symbol,
        status: MinuteKlineCacheDiagnosticStatus::Corrupt,
        schema_version: None,
        rows: 0,
        cached_ranges: Vec::new(),
        size_bytes,
        error: None,
    };
    let version = match read_month_file_version(entry.path.as_path()) {
        Ok(version) => {
            file.schema_version = Some(u32::from(version));
            version
        }
        Err(error) => {
            file.error = Some(error.to_string());
            return file;
        }
    };
    if version == 3 {
        file.status = MinuteKlineCacheDiagnosticStatus::LegacyUnsupported;
        file.error = Some(
            "minute kline cache schema v3 is legacy and is not migrated or overwritten automatically"
                .to_string(),
        );
        return file;
    }
    if version != FILE_VERSION {
        file.status = MinuteKlineCacheDiagnosticStatus::UnsupportedVersion;
        file.error = Some(format!(
            "unsupported minute kline cache schema version {version}; expected {FILE_VERSION}"
        ));
        return file;
    }

    match scan_month_file_unchecked(entry.path.as_path()).and_then(|scan| {
        let path_symbol = symbol_from_file_path(entry.path.as_path())?;
        if scan.metadata.symbol != path_symbol {
            return Err(format_error(
                entry.path.as_path(),
                "symbol metadata does not match the monthly filename",
            ));
        }
        if scan.metadata.trading_month != entry.trading_month {
            return Err(format_error(
                entry.path.as_path(),
                "trading month metadata does not match the parent directory",
            ));
        }
        Ok(scan)
    }) {
        Ok(scan) => {
            file.symbol = scan.metadata.symbol;
            file.rows = scan.rows;
            file.cached_ranges = scan.coverage;
            file.status = MinuteKlineCacheDiagnosticStatus::Readable;
        }
        Err(error) => file.error = Some(error.to_string()),
    }
    file
}

fn read_month_file_version(path: &Path) -> Result<u16> {
    let mut file = File::open(path)?;
    let mut prefix = [0_u8; 6];
    read_exact_format(&mut file, &mut prefix, path, "file magic and version")?;
    if prefix[..4] != FILE_MAGIC {
        return Err(format_error(path, "unexpected magic"));
    }
    Ok(u16::from_le_bytes([prefix[4], prefix[5]]))
}

fn symbol_from_file_path(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format_error(path, "monthly filename is not UTF-8"))?;
    let file_stem = file_name
        .strip_suffix(&format!(".{FILE_EXTENSION}"))
        .ok_or_else(|| format_error(path, "monthly filename has an unexpected extension"))?;
    unescape_symbol_path_component(file_stem).map_err(|reason| {
        format_error(
            path,
            format!("invalid escaped symbol in filename: {reason}"),
        )
    })
}

fn unescape_symbol_path_component(value: &str) -> std::result::Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index = index.saturating_add(1);
            continue;
        }
        let high = *bytes
            .get(index.saturating_add(1))
            .ok_or_else(|| "truncated percent escape".to_string())?;
        let low = *bytes
            .get(index.saturating_add(2))
            .ok_or_else(|| "truncated percent escape".to_string())?;
        let high = hex_value(high).ok_or_else(|| "invalid percent escape".to_string())?;
        let low = hex_value(low).ok_or_else(|| "invalid percent escape".to_string())?;
        decoded.push((high << 4) | low);
        index = index.saturating_add(3);
    }
    String::from_utf8(decoded).map_err(|_| "symbol is not UTF-8".to_string())
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn load_month_file(
    path: &Path,
    symbol: &str,
    trading_month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
) -> Result<MonthFile> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let (header, metadata_bytes) = read_file_prefix(&mut reader, path, file_len)?;
    let metadata = decode_metadata(path, metadata_bytes.as_slice())?;
    validate_expected_metadata(path, &metadata, symbol, trading_month, snapshot)?;
    let mut checksum = checksum_bytes(FNV_OFFSET_BASIS, metadata_bytes.as_slice());
    let mut coverage = Vec::with_capacity(header.coverage_count);
    for _ in 0..header.coverage_count {
        let mut bytes = [0_u8; COVERAGE_BYTES];
        read_exact_format(&mut reader, &mut bytes, path, "coverage")?;
        checksum = checksum_bytes(checksum, &bytes);
        coverage.push(decode_coverage(bytes.as_slice()));
    }
    validate_stored_coverage(path, trading_month, coverage.as_slice())?;
    let mut rows = Vec::with_capacity(header.row_count);
    for _ in 0..header.row_count {
        let mut bytes = [0_u8; KLINE_ROW_BYTES];
        read_exact_format(&mut reader, &mut bytes, path, "Kline row")?;
        checksum = checksum_bytes(checksum, &bytes);
        let row = decode_kline(bytes.as_slice());
        validate_one_stored_row(path, trading_month, &row)?;
        rows.push(row);
    }
    if checksum != header.checksum {
        return Err(format_error(path, "payload checksum mismatch"));
    }
    Ok(MonthFile {
        metadata,
        coverage,
        rows,
    })
}

fn write_month_atomically(path: &Path, month: &MonthFile) -> Result<()> {
    let bytes = encode_month_file(month)?;
    let parent = path
        .parent()
        .ok_or_else(|| format_error(path, "monthly path has no parent directory"))?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| format_error(path, "monthly path has no file name"))?
        .to_string_lossy();
    let mut temp_path = None;
    let mut temp_file = None;
    for attempt in 0_u32..128 {
        let candidate = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            attempt
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(candidate.as_path())
        {
            Ok(file) => {
                temp_path = Some(candidate);
                temp_file = Some(file);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let temp_path =
        temp_path.ok_or_else(|| format_error(path, "cannot allocate atomic temp file"))?;
    let write_result = (|| -> Result<()> {
        let mut file = temp_file.expect("temp path and file are created together");
        file.write_all(bytes.as_slice())?;
        file.sync_all()?;
        drop(file);
        fs::rename(temp_path.as_path(), path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(temp_path.as_path());
    }
    write_result
}

fn encode_month_file(month: &MonthFile) -> Result<Vec<u8>> {
    month.metadata.snapshot.validate()?;
    validate_symbol(month.metadata.symbol.as_str())?;
    if !is_trading_month(month.metadata.trading_month.as_str()) {
        return Err(DataError::Validation(
            "minute kline cache trading month must be YYYYMM".to_string(),
        ));
    }
    validate_stored_coverage(
        Path::new("<new minute-kline month>"),
        month.metadata.trading_month.as_str(),
        month.coverage.as_slice(),
    )?;
    validate_stored_rows(
        Path::new("<new minute-kline month>"),
        month.metadata.trading_month.as_str(),
        month.rows.as_slice(),
    )?;
    if month.coverage.len() > MAX_COVERAGE_RECORDS || month.rows.len() > MAX_ROWS_PER_MONTH {
        return Err(DataError::Validation(
            "minute kline cache month exceeds format record limits".to_string(),
        ));
    }

    let metadata = encode_metadata(&month.metadata)?;
    let coverage_len = month
        .coverage
        .len()
        .checked_mul(COVERAGE_BYTES)
        .ok_or_else(|| {
            DataError::InvalidResponse("minute kline coverage size overflow".to_string())
        })?;
    let row_len = month
        .rows
        .len()
        .checked_mul(KLINE_ROW_BYTES)
        .ok_or_else(|| DataError::InvalidResponse("minute kline row size overflow".to_string()))?;
    let payload_len = metadata
        .len()
        .checked_add(coverage_len)
        .and_then(|value| value.checked_add(row_len))
        .ok_or_else(|| {
            DataError::InvalidResponse("minute kline payload size overflow".to_string())
        })?;
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(metadata.as_slice());
    for range in &month.coverage {
        encode_coverage(&mut payload, *range);
    }
    for row in &month.rows {
        encode_kline(&mut payload, row);
    }
    let checksum = checksum_bytes(FNV_OFFSET_BASIS, payload.as_slice());
    let metadata_len = u32::try_from(metadata.len()).map_err(|_| {
        DataError::InvalidResponse("minute kline metadata is too large".to_string())
    })?;
    let coverage_count = u64::try_from(month.coverage.len()).map_err(|_| {
        DataError::InvalidResponse("minute kline coverage count overflow".to_string())
    })?;
    let row_count = u64::try_from(month.rows.len())
        .map_err(|_| DataError::InvalidResponse("minute kline row count overflow".to_string()))?;
    let mut bytes = Vec::with_capacity(FILE_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(&FILE_MAGIC);
    bytes.extend_from_slice(&FILE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&metadata_len.to_le_bytes());
    bytes.extend_from_slice(&coverage_count.to_le_bytes());
    bytes.extend_from_slice(&row_count.to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(payload.as_slice());
    Ok(bytes)
}

fn read_file_prefix<R: Read>(
    reader: &mut R,
    path: &Path,
    file_len: u64,
) -> Result<(DiskHeader, Vec<u8>)> {
    let mut bytes = [0_u8; FILE_HEADER_BYTES];
    read_exact_format(reader, &mut bytes, path, "file header")?;
    if bytes[..4] != FILE_MAGIC {
        return Err(format_error(path, "unexpected magic"));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != FILE_VERSION {
        return Err(format_error(path, "unsupported format version"));
    }
    let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
    if reserved != 0 {
        return Err(format_error(path, "unknown header flags"));
    }
    let metadata_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    if metadata_len > MAX_METADATA_BYTES {
        return Err(format_error(path, "metadata exceeds maximum size"));
    }
    let coverage_count = u64::from_le_bytes([
        bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
    ]);
    let row_count = u64::from_le_bytes([
        bytes[20], bytes[21], bytes[22], bytes[23], bytes[24], bytes[25], bytes[26], bytes[27],
    ]);
    let checksum = u64::from_le_bytes([
        bytes[28], bytes[29], bytes[30], bytes[31], bytes[32], bytes[33], bytes[34], bytes[35],
    ]);
    let coverage_count = usize::try_from(coverage_count)
        .map_err(|_| format_error(path, "coverage count does not fit platform usize"))?;
    let row_count = usize::try_from(row_count)
        .map_err(|_| format_error(path, "row count does not fit platform usize"))?;
    if coverage_count > MAX_COVERAGE_RECORDS {
        return Err(format_error(path, "coverage count exceeds format limit"));
    }
    if row_count > MAX_ROWS_PER_MONTH {
        return Err(format_error(path, "row count exceeds format limit"));
    }
    let expected_payload_len = metadata_len
        .checked_add(
            coverage_count
                .checked_mul(COVERAGE_BYTES)
                .ok_or_else(|| format_error(path, "coverage byte length overflow"))?,
        )
        .and_then(|value| {
            row_count
                .checked_mul(KLINE_ROW_BYTES)
                .and_then(|rows| value.checked_add(rows))
        })
        .ok_or_else(|| format_error(path, "payload byte length overflow"))?;
    let expected_file_len = u64::try_from(
        FILE_HEADER_BYTES
            .checked_add(expected_payload_len)
            .ok_or_else(|| format_error(path, "file length overflow"))?,
    )
    .map_err(|_| format_error(path, "file length does not fit u64"))?;
    if file_len != expected_file_len {
        return Err(format_error(path, "file length does not match header"));
    }
    let mut metadata = vec![0_u8; metadata_len];
    read_exact_format(reader, metadata.as_mut_slice(), path, "metadata")?;
    Ok((
        DiskHeader {
            coverage_count,
            row_count,
            checksum,
        },
        metadata,
    ))
}

fn encode_metadata(metadata: &MonthMetadata) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&metadata.snapshot.version.to_le_bytes());
    encode_string(&mut bytes, metadata.symbol.as_str())?;
    encode_string(&mut bytes, metadata.trading_month.as_str())?;
    encode_string(&mut bytes, metadata.snapshot.calendar_hash.as_str())?;
    encode_string(&mut bytes, metadata.snapshot.session_hash.as_str())?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(DataError::Validation(
            "minute kline cache metadata exceeds maximum size".to_string(),
        ));
    }
    Ok(bytes)
}

fn decode_metadata(path: &Path, bytes: &[u8]) -> Result<MonthMetadata> {
    let mut cursor = ByteCursor::new(bytes);
    let version = cursor.read_u32(path, "snapshot version")?;
    let symbol = cursor.read_string(path, "symbol")?;
    let trading_month = cursor.read_string(path, "trading month")?;
    let calendar_hash = cursor.read_string(path, "calendar hash")?;
    let session_hash = cursor.read_string(path, "session hash")?;
    if !cursor.is_done() {
        return Err(format_error(path, "metadata has trailing bytes"));
    }
    let snapshot = MinuteKlineCacheSnapshot {
        version,
        calendar_hash,
        session_hash,
    };
    snapshot
        .validate()
        .map_err(|error| format_error(path, format!("invalid snapshot metadata: {error}")))?;
    validate_symbol(symbol.as_str())
        .map_err(|error| format_error(path, format!("invalid symbol metadata: {error}")))?;
    if !is_trading_month(trading_month.as_str()) {
        return Err(format_error(path, "invalid trading month metadata"));
    }
    Ok(MonthMetadata {
        symbol,
        trading_month,
        snapshot,
    })
}

fn validate_expected_metadata(
    path: &Path,
    metadata: &MonthMetadata,
    symbol: &str,
    trading_month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
) -> Result<()> {
    if metadata.symbol != symbol {
        return Err(format_error(path, "symbol metadata mismatch"));
    }
    if metadata.trading_month != trading_month {
        return Err(format_error(path, "trading month metadata mismatch"));
    }
    if metadata.snapshot != *snapshot {
        return Err(format_error(
            path,
            "calendar/session snapshot mismatch; refusing stale minute Kline cache",
        ));
    }
    Ok(())
}

fn is_snapshot_mismatch(error: &DataError) -> bool {
    matches!(
        error,
        DataError::InvalidResponse(message)
            if message.contains("calendar/session snapshot mismatch")
    )
}

fn encode_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|_| {
        DataError::Validation("minute kline cache metadata string exceeds u16 length".to_string())
    })?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u32(&mut self, path: &Path, field: &str) -> Result<u32> {
        let bytes = self.read_exact(path, field, 4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_string(&mut self, path: &Path, field: &str) -> Result<String> {
        let len = self.read_exact(path, field, 2)?;
        let len = u16::from_le_bytes([len[0], len[1]]) as usize;
        let bytes = self.read_exact(path, field, len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| format_error(path, format!("{field} is not UTF-8")))
    }

    fn read_exact(&mut self, path: &Path, field: &str, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| format_error(path, format!("{field} offset overflow")))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| format_error(path, format!("metadata is truncated at {field}")))?;
        self.offset = end;
        Ok(bytes)
    }

    fn is_done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn encode_coverage(bytes: &mut Vec<u8>, range: (i64, i64)) {
    bytes.extend_from_slice(&range.0.to_le_bytes());
    bytes.extend_from_slice(&range.1.to_le_bytes());
}

fn decode_coverage(bytes: &[u8]) -> (i64, i64) {
    let start = i64::from_le_bytes(bytes[..8].try_into().expect("fixed coverage bytes"));
    let end = i64::from_le_bytes(bytes[8..16].try_into().expect("fixed coverage bytes"));
    (start, end)
}

fn encode_kline(bytes: &mut Vec<u8>, row: &Kline) {
    bytes.extend_from_slice(&row.id.to_le_bytes());
    bytes.extend_from_slice(&row.datetime.to_le_bytes());
    bytes.extend_from_slice(&row.open.to_bits().to_le_bytes());
    bytes.extend_from_slice(&row.high.to_bits().to_le_bytes());
    bytes.extend_from_slice(&row.low.to_bits().to_le_bytes());
    bytes.extend_from_slice(&row.close.to_bits().to_le_bytes());
    bytes.extend_from_slice(&row.volume.to_le_bytes());
    bytes.extend_from_slice(&row.open_oi.to_le_bytes());
    bytes.extend_from_slice(&row.close_oi.to_le_bytes());
    bytes.extend_from_slice(&row.epoch.unwrap_or(NONE_EPOCH).to_le_bytes());
}

fn decode_kline(bytes: &[u8]) -> Kline {
    Kline {
        id: read_i64(bytes, 0),
        datetime: read_i64(bytes, 8),
        open: f64::from_bits(read_u64(bytes, 16)),
        high: f64::from_bits(read_u64(bytes, 24)),
        low: f64::from_bits(read_u64(bytes, 32)),
        close: f64::from_bits(read_u64(bytes, 40)),
        volume: read_i64(bytes, 48),
        open_oi: read_i64(bytes, 56),
        close_oi: read_i64(bytes, 64),
        epoch: (read_i64(bytes, 72) != NONE_EPOCH).then(|| read_i64(bytes, 72)),
    }
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed Kline bytes"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed Kline bytes"),
    )
}

fn validate_input_rows(
    symbol: &str,
    range_start_ns: i64,
    range_end_ns: i64,
    rows: &[Kline],
) -> Result<()> {
    for row in rows {
        if row.datetime < range_start_ns || row.datetime >= range_end_ns {
            return Err(DataError::Validation(format!(
                "minute kline row for {symbol} is outside declared final range"
            )));
        }
        if row.datetime.rem_euclid(MINUTE_KLINE_DURATION_NS) != 0 {
            return Err(DataError::Validation(format!(
                "minute kline row for {symbol} is not aligned to 60 seconds"
            )));
        }
    }
    Ok(())
}

fn validate_stored_rows(path: &Path, trading_month: &str, rows: &[Kline]) -> Result<()> {
    let mut previous = None;
    for row in rows {
        validate_one_stored_row(path, trading_month, row)?;
        if previous.is_some_and(|datetime| datetime >= row.datetime) {
            return Err(format_error(
                path,
                "Kline rows are not strictly datetime-ordered",
            ));
        }
        previous = Some(row.datetime);
    }
    Ok(())
}

fn validate_one_stored_row(path: &Path, trading_month: &str, row: &Kline) -> Result<()> {
    if row.datetime.rem_euclid(MINUTE_KLINE_DURATION_NS) != 0 {
        return Err(format_error(path, "Kline row is not aligned to 60 seconds"));
    }
    let row_month = trading_month_for_timestamp_ns(row.datetime)?;
    if row_month != trading_month {
        return Err(format_error(
            path,
            "Kline row belongs to another trading month",
        ));
    }
    Ok(())
}

fn validate_stored_coverage(path: &Path, trading_month: &str, ranges: &[(i64, i64)]) -> Result<()> {
    let mut previous = None;
    for range in ranges {
        if range.0 >= range.1 {
            return Err(format_error(path, "coverage range is empty or reversed"));
        }
        let end_timestamp = range
            .1
            .checked_sub(1)
            .ok_or_else(|| format_error(path, "coverage end underflow"))?;
        if trading_month_for_timestamp_ns(range.0)? != trading_month
            || trading_month_for_timestamp_ns(end_timestamp)? != trading_month
        {
            return Err(format_error(
                path,
                "coverage crosses trading-month boundary",
            ));
        }
        if previous.is_some_and(|end| end >= range.0) {
            return Err(format_error(
                path,
                "coverage ranges overlap or are unordered",
            ));
        }
        previous = Some(range.1);
    }
    Ok(())
}

fn reject_open_or_future_final_range(range_end_ns: i64, now_ns: i64) -> Result<()> {
    let current_day = backtest_tick_trading_day_for_timestamp_ns(now_ns)?;
    let current = backtest_tick_trading_day_range(current_day)?;
    if range_end_ns > current.start_ns {
        return Err(DataError::InvalidState(
            "minute kline cache cannot mark the current or a future trading day final",
        ));
    }
    Ok(())
}

fn split_trading_month_range(start_ns: i64, end_ns: i64) -> Result<Vec<TradingMonthSlice>> {
    validate_timestamp_range(start_ns, end_ns)?;
    let mut slices = Vec::new();
    let mut cursor = start_ns;
    while cursor < end_ns {
        let trading_month = trading_month_for_timestamp_ns(cursor)?;
        let final_month =
            trading_month_for_timestamp_ns(end_ns.checked_sub(1).ok_or_else(|| {
                DataError::InvalidResponse("minute kline range end underflow".to_string())
            })?)?;
        let next = if trading_month == final_month {
            end_ns
        } else {
            first_timestamp_after_month(cursor, end_ns, trading_month.as_str())?
        };
        if next <= cursor {
            return Err(DataError::InvalidResponse(
                "minute kline trading-month partition did not advance".to_string(),
            ));
        }
        slices.push(TradingMonthSlice {
            trading_month,
            start_ns: cursor,
            end_ns: next,
        });
        cursor = next;
    }
    Ok(slices)
}

fn first_timestamp_after_month(start_ns: i64, end_ns: i64, month: &str) -> Result<i64> {
    let mut low = start_ns;
    let mut high = end_ns;
    while i128::from(high) - i128::from(low) > 1 {
        let middle = ((i128::from(low) + i128::from(high)) / 2) as i64;
        if trading_month_for_timestamp_ns(middle)? == month {
            low = middle;
        } else {
            high = middle;
        }
    }
    Ok(high)
}

/// CST trading-month key used by the monthly-minute cache (`YYYYMM`).
pub fn trading_month_for_timestamp_ns(timestamp_ns: i64) -> Result<String> {
    Ok(backtest_tick_trading_day_for_timestamp_ns(timestamp_ns)?
        .format("%Y%m")
        .to_string())
}

fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.retain(|range| range.0 < range.1);
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && range.0 <= previous.1
        {
            previous.1 = previous.1.max(range.1);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn missing_ranges(start_ns: i64, end_ns: i64, cached_ranges: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut missing = Vec::new();
    let mut cursor = start_ns;
    for range in cached_ranges {
        if range.1 <= cursor || range.0 >= end_ns {
            continue;
        }
        if range.0 > cursor {
            missing.push((cursor, range.0.min(end_ns)));
        }
        cursor = cursor.max(range.1);
        if cursor >= end_ns {
            break;
        }
    }
    if cursor < end_ns {
        missing.push((cursor, end_ns));
    }
    missing
}

fn intersect_ranges(left: (i64, i64), right: (i64, i64)) -> Option<(i64, i64)> {
    let start = left.0.max(right.0);
    let end = left.1.min(right.1);
    (start < end).then_some((start, end))
}

fn validate_range(symbol: &str, start_ns: i64, end_ns: i64) -> Result<()> {
    validate_symbol(symbol)?;
    validate_timestamp_range(start_ns, end_ns)
}

fn validate_timestamp_range(start_ns: i64, end_ns: i64) -> Result<()> {
    if start_ns >= end_ns {
        return Err(DataError::Validation(
            "minute kline cache range start must be before range end".to_string(),
        ));
    }
    Ok(())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() {
        return Err(DataError::Validation(
            "minute kline cache symbol must not be empty".to_string(),
        ));
    }
    if symbol.len() > 16 * 1024 {
        return Err(DataError::Validation(
            "minute kline cache symbol is too long".to_string(),
        ));
    }
    Ok(())
}

fn validate_metadata_string(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(DataError::Validation(format!(
            "minute kline cache {name} must not be empty"
        )));
    }
    if value.len() > u16::MAX as usize {
        return Err(DataError::Validation(format!(
            "minute kline cache {name} exceeds u16 length"
        )));
    }
    Ok(())
}

fn is_trading_month(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn escape_symbol_path_component(symbol: &str) -> String {
    let mut escaped = String::new();
    for byte in symbol.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push_str(&format!("{byte:02X}"));
        }
    }
    if escaped.is_empty() {
        "_".to_string()
    } else {
        escaped
    }
}

fn checksum_bytes(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(FNV_PRIME);
    }
    state
}

fn read_exact_format<R: Read>(
    reader: &mut R,
    bytes: &mut [u8],
    path: &Path,
    section: &str,
) -> Result<()> {
    reader
        .read_exact(bytes)
        .map_err(|error| format_error(path, format!("{section} is truncated: {error}")))
}

fn format_error(path: &Path, reason: impl AsRef<str>) -> DataError {
    DataError::InvalidResponse(format!(
        "minute kline cache {}: {}",
        path.display(),
        reason.as_ref()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn trading_month_switches_on_friday_night_before_monday_month() {
        let before = utc_ns(2026, 1, 30, 9, 59);
        let after = utc_ns(2026, 1, 30, 10, 0);

        assert_eq!(trading_month_for_timestamp_ns(before).unwrap(), "202601");
        assert_eq!(trading_month_for_timestamp_ns(after).unwrap(), "202602");
    }

    #[test]
    fn split_range_uses_exact_trading_month_boundary() {
        let start = utc_ns(2026, 1, 30, 9, 59);
        let end = utc_ns(2026, 1, 30, 10, 1);
        let slices = split_trading_month_range(start, end).unwrap();

        assert_eq!(
            slices
                .iter()
                .map(|slice| (slice.trading_month.as_str(), slice.start_ns, slice.end_ns))
                .collect::<Vec<_>>(),
            vec![
                ("202601", start, utc_ns(2026, 1, 30, 10, 0)),
                ("202602", utc_ns(2026, 1, 30, 10, 0), end)
            ]
        );
    }

    #[test]
    fn final_coverage_rejects_open_trading_day() {
        let now = utc_ns(2026, 7, 29, 2, 0);
        let day = backtest_tick_trading_day_for_timestamp_ns(now).unwrap();
        let range = backtest_tick_trading_day_range(day).unwrap();

        assert!(reject_open_or_future_final_range(range.end_ns, now).is_err());
        assert!(reject_open_or_future_final_range(range.start_ns, now).is_ok());
    }

    #[test]
    fn minute_reader_chunk_requires_a_nonzero_target_and_shares_the_row_cursor() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("tqsdk-minute-chunk-reader-{nanos}"));
        let cache = MinuteKlineCache::open(&root).expect("cache should open");
        let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1")
            .expect("snapshot should be valid");
        let start = utc_ns(2026, 1, 5, 2, 0);
        let minute_ns = 60_000_000_000;
        let end = start + 2 * minute_ns;
        cache
            .store_final_range(
                "SHFE.rb2601",
                start,
                end,
                &snapshot,
                &[
                    Kline {
                        id: 1,
                        datetime: start,
                        open: 10.0,
                        high: 10.0,
                        low: 10.0,
                        close: 10.0,
                        volume: 1,
                        open_oi: 1,
                        close_oi: 1,
                        ..Kline::default()
                    },
                    Kline {
                        id: 2,
                        datetime: start + minute_ns,
                        open: 11.0,
                        high: 11.0,
                        low: 11.0,
                        close: 11.0,
                        volume: 2,
                        open_oi: 2,
                        close_oi: 2,
                        ..Kline::default()
                    },
                ],
            )
            .expect("minute rows should write");

        let mut reader = cache
            .open_reader("SHFE.rb2601", start, end, &snapshot)
            .expect("reader should open");
        assert!(matches!(
            reader.next_kline_chunk(0),
            Err(DataError::Validation(_))
        ));
        assert_eq!(
            reader
                .next_kline_chunk(std::mem::size_of::<Kline>())
                .expect("chunk should read")
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(reader.next_kline().unwrap().unwrap().id, 2);
    }

    #[test]
    fn inspect_scans_each_present_month_once() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("tqsdk-minute-inspect-scan-{nanos}"));
        let cache = MinuteKlineCache::open(&root).expect("cache should open");
        let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1")
            .expect("snapshot should be valid");
        let start = utc_ns(2026, 1, 5, 2, 0);
        let end = start + MINUTE_KLINE_DURATION_NS;
        cache
            .store_final_range("SHFE.rb2601", start, end, &snapshot, &[])
            .expect("minute coverage should write");

        TEST_MONTH_SCAN_COUNT.with(|count| count.set(0));
        cache
            .inspect("SHFE.rb2601", start, end, &snapshot)
            .expect("minute cache should inspect");
        let scans = TEST_MONTH_SCAN_COUNT.with(std::cell::Cell::get);

        assert_eq!(scans, 1, "inspect should decode each selected month once");
        let _ = std::fs::remove_dir_all(root);
    }

    fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }
}
