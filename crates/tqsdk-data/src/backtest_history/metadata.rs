#[cfg(all(feature = "live", feature = "services"))]
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::NaiveDate;
#[cfg(all(feature = "live", feature = "services"))]
use chrono::{Datelike, Days, FixedOffset, TimeZone, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
#[cfg(all(feature = "live", feature = "services"))]
use tqsdk_core::TradingTime;

use crate::{DataError, KlineSessionTemplate, MinuteKlineCache, MinuteKlineCacheSnapshot, Result};
#[cfg(all(feature = "live", feature = "services"))]
use crate::{
    HistoricalContUnderlyingSegment, KlineSessionWindow, TradingCalendarRow,
    trading_month_for_timestamp_ns,
};

use super::{
    BacktestHistoryAuthProvider, BacktestHistoryCredentials, BacktestHistoryPhysicalSegment,
};

/// On-disk format identifier for backtest history metadata snapshots.
pub const BACKTEST_HISTORY_METADATA_FORMAT_ID: &str = "tqsdk.backtest-history-metadata.v1";
/// Schema version for [`BacktestHistoryMetadataSnapshot`].
pub const BACKTEST_HISTORY_METADATA_SCHEMA_VERSION: u32 = 1;

const METADATA_NAMESPACE: &str = "backtest-history-metadata-v1";
const ACTIVE_FILE_NAME: &str = "active.json";
const LOCK_FILE_NAME: &str = ".metadata.lock";
const SNAPSHOTS_DIR_NAME: &str = "snapshots";

/// Market family represented by a metadata snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BacktestHistoryMarketKind {
    Futures,
}

/// One calendar day covered by an immutable metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestHistoryTradingDay {
    pub date: String,
    pub is_trading_day: bool,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Immutable calendar, session, and logical-to-physical mapping snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestHistoryMetadataSnapshot {
    pub schema_version: u32,
    pub market_kind: BacktestHistoryMarketKind,
    pub logical_symbol: String,
    pub captured_at_ns: i64,
    pub trading_days: Vec<BacktestHistoryTradingDay>,
    pub session: KlineSessionTemplate,
    pub physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub snapshot_hash: String,
}

/// Explicit operator repair plan for mixed canonical-minute cache snapshots.
///
/// The normal reader remains fail-closed. This plan only identifies monthly
/// partitions an operator may explicitly purge before a remote fill retries
/// with the active snapshot that the remote fill will use.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteCacheStalePartitionRepairPlan {
    pub snapshot_hash: String,
    pub stale_ranges: Vec<(i64, i64)>,
}

impl BacktestHistoryMetadataSnapshot {
    /// Returns whether this immutable physical mapping covers `range` in full.
    fn covers_range(&self, range: (i64, i64)) -> bool {
        physical_segments_cover_range(self.physical_segments.as_slice(), range)
    }
}

/// Durable cache for immutable metadata snapshots and their active pointer.
#[derive(Clone)]
pub struct BacktestHistoryMetadataCache {
    root_dir: PathBuf,
    writable: bool,
}

impl BacktestHistoryMetadataCache {
    /// Opens a metadata root that may create sidecar directories on store.
    pub fn open(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir)?;
        Ok(Self {
            root_dir,
            writable: true,
        })
    }

    /// Opens a metadata root without creating files or directories.
    #[must_use]
    pub fn open_read_only(root_dir: impl AsRef<Path>) -> Self {
        Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            writable: false,
        }
    }

    /// Returns the active immutable snapshot for a logical symbol, if present.
    ///
    /// Missing sidecars are an offline cache miss. Corrupt sidecars fail closed
    /// and are left byte-for-byte untouched for explicit maintenance to inspect.
    pub fn load_active(
        &self,
        logical_symbol: &str,
    ) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
        let symbol_dir = self.symbol_dir(logical_symbol)?;
        match fs::metadata(&symbol_dir) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(metadata_response_error(format!(
                    "symbol namespace {} is not a directory",
                    symbol_dir.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }

        let _lock = MetadataLock::acquire_shared(&symbol_dir, &self.root_dir)?;
        read_active_snapshot(&symbol_dir, logical_symbol)
    }

    /// Returns every validated immutable snapshot retained for one logical
    /// symbol. The active pointer is intentionally not used as a filter: old
    /// snapshots can still authenticate a durable canonical-minute partition
    /// that was written before a later metadata refresh moved the pointer.
    ///
    /// Missing sidecars are an offline cache miss. Malformed snapshot files
    /// fail closed rather than being ignored.
    fn load_snapshots(&self, logical_symbol: &str) -> Result<Vec<BacktestHistoryMetadataSnapshot>> {
        let symbol_dir = self.symbol_dir(logical_symbol)?;
        match fs::metadata(&symbol_dir) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(metadata_response_error(format!(
                    "symbol namespace {} is not a directory",
                    symbol_dir.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        }

        let _lock = MetadataLock::acquire_shared(&symbol_dir, &self.root_dir)?;
        let snapshots_dir = symbol_dir.join(SNAPSHOTS_DIR_NAME);
        let entries = match fs::read_dir(&snapshots_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut snapshot_hashes = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(snapshot_hash) = name.strip_suffix(".json") else {
                return Err(metadata_response_error(format!(
                    "snapshot directory {} contains an unexpected entry {name}",
                    snapshots_dir.display()
                )));
            };
            if !is_sha1_hex(snapshot_hash) {
                return Err(metadata_response_error(format!(
                    "snapshot directory {} contains an invalid snapshot hash {snapshot_hash}",
                    snapshots_dir.display()
                )));
            }
            snapshot_hashes.push(snapshot_hash.to_string());
        }
        snapshot_hashes.sort();
        snapshot_hashes
            .into_iter()
            .map(|snapshot_hash| read_snapshot(&symbol_dir, logical_symbol, &snapshot_hash))
            .collect()
    }

    fn load_snapshot_by_hash(
        &self,
        logical_symbol: &str,
        snapshot_hash: &str,
    ) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
        if !is_sha1_hex(snapshot_hash) {
            return Ok(None);
        }
        let symbol_dir = self.symbol_dir(logical_symbol)?;
        match fs::metadata(&symbol_dir) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(metadata_response_error(format!(
                    "symbol namespace {} is not a directory",
                    symbol_dir.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }

        let _lock = MetadataLock::acquire_shared(&symbol_dir, &self.root_dir)?;
        let snapshot_path = symbol_dir
            .join(SNAPSHOTS_DIR_NAME)
            .join(format!("{snapshot_hash}.json"));
        match fs::metadata(snapshot_path.as_path()) {
            Ok(metadata) if !metadata.is_file() => Err(metadata_response_error(format!(
                "snapshot {} is not a regular file",
                snapshot_path.display()
            ))),
            Ok(_) => read_snapshot(&symbol_dir, logical_symbol, snapshot_hash).map(Some),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Stores an immutable snapshot and atomically makes it active.
    ///
    /// Existing snapshots are never removed. Supplying a snapshot whose hash is
    /// already present verifies its exact bytes rather than overwriting it.
    pub fn store_snapshot(
        &self,
        snapshot: BacktestHistoryMetadataSnapshot,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        self.store_snapshot_with_active_policy(snapshot, false)
    }

    /// Stores a remote-miss snapshot without allowing a narrower mapping to
    /// replace an already broader active pointer.
    ///
    /// The returned snapshot remains available to the request that fetched it,
    /// while cache-backed historical requests retain a broadly useful active
    /// mapping. Session or schema changes always advance the active pointer.
    pub(crate) fn store_snapshot_for_remote_miss(
        &self,
        snapshot: BacktestHistoryMetadataSnapshot,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        self.store_snapshot_with_active_policy(snapshot, true)
    }

    fn store_snapshot_with_active_policy(
        &self,
        snapshot: BacktestHistoryMetadataSnapshot,
        retain_broader_active: bool,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        self.ensure_writable()?;
        let snapshot = normalize_snapshot_for_store(snapshot)?;
        let symbol_dir = self.symbol_dir(snapshot.logical_symbol.as_str())?;
        fs::create_dir_all(symbol_dir.join(SNAPSHOTS_DIR_NAME))?;
        let _lock = MetadataLock::acquire_exclusive(&symbol_dir, &self.root_dir)?;
        let active_before = if retain_broader_active {
            read_active_snapshot(&symbol_dir, snapshot.logical_symbol.as_str())?
        } else {
            None
        };

        let snapshot_path = symbol_dir
            .join(SNAPSHOTS_DIR_NAME)
            .join(format!("{}.json", snapshot.snapshot_hash));
        let snapshot_bytes = serde_json::to_vec(&snapshot).map_err(|error| {
            DataError::InvalidResponse(format!("cannot encode backtest metadata snapshot: {error}"))
        })?;
        match fs::read(&snapshot_path) {
            Ok(existing) => {
                if existing != snapshot_bytes {
                    return Err(metadata_response_error(format!(
                        "existing snapshot {} does not match its hash",
                        snapshot_path.display()
                    )));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                write_new_atomically(&snapshot_path, snapshot_bytes.as_slice())?;
            }
            Err(error) => return Err(error.into()),
        }

        if active_before
            .as_ref()
            .is_none_or(|active| !should_keep_broader_active_snapshot(active, &snapshot))
        {
            write_active_snapshot(&symbol_dir, snapshot.snapshot_hash.as_str())?;
        }
        Ok(snapshot)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(DataError::InvalidState(
                "backtest history metadata cache was opened read-only",
            ))
        }
    }

    fn symbol_dir(&self, logical_symbol: &str) -> Result<PathBuf> {
        validate_logical_symbol(logical_symbol)?;
        Ok(self
            .root_dir
            .join(METADATA_NAMESPACE)
            .join(escape_symbol_path_component(logical_symbol)))
    }
}

fn read_active_snapshot(
    symbol_dir: &Path,
    logical_symbol: &str,
) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
    let active_path = symbol_dir.join(ACTIVE_FILE_NAME);
    let active_bytes = match fs::read(&active_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let pointer: ActiveSnapshotPointer =
        serde_json::from_slice(&active_bytes).map_err(|error| {
            metadata_response_error(format!(
                "active pointer {} is invalid JSON: {error}",
                active_path.display()
            ))
        })?;
    pointer.validate(&active_path)?;
    read_snapshot(symbol_dir, logical_symbol, pointer.snapshot_hash.as_str()).map(Some)
}

fn write_active_snapshot(symbol_dir: &Path, snapshot_hash: &str) -> Result<()> {
    let active = ActiveSnapshotPointer {
        format_id: BACKTEST_HISTORY_METADATA_FORMAT_ID.to_string(),
        schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
        snapshot_hash: snapshot_hash.to_string(),
    };
    let active_bytes = serde_json::to_vec(&active).map_err(|error| {
        DataError::InvalidResponse(format!("cannot encode metadata active pointer: {error}"))
    })?;
    write_replace_atomically(&symbol_dir.join(ACTIVE_FILE_NAME), active_bytes.as_slice())
}

fn should_keep_broader_active_snapshot(
    active: &BacktestHistoryMetadataSnapshot,
    candidate: &BacktestHistoryMetadataSnapshot,
) -> bool {
    if active.snapshot_hash == candidate.snapshot_hash {
        return true;
    }
    if active.schema_version != candidate.schema_version
        || active.market_kind != candidate.market_kind
        || active.session.snapshot_hash() != candidate.session.snapshot_hash()
    {
        return false;
    }
    let Some(active_range) = metadata_physical_coverage_range(active) else {
        return false;
    };
    let Some(candidate_range) = metadata_physical_coverage_range(candidate) else {
        return false;
    };
    if candidate.covers_range(active_range) {
        return false;
    }
    let active_span = active_range.1.saturating_sub(active_range.0);
    let candidate_span = candidate_range.1.saturating_sub(candidate_range.0);
    active_span >= candidate_span
}

fn metadata_physical_coverage_range(
    snapshot: &BacktestHistoryMetadataSnapshot,
) -> Option<(i64, i64)> {
    let start_ns = snapshot.physical_segments.first()?.start_ns;
    let end_ns = snapshot.physical_segments.last()?.end_ns;
    (start_ns < end_ns).then_some((start_ns, end_ns))
}

fn read_snapshot(
    symbol_dir: &Path,
    logical_symbol: &str,
    snapshot_hash: &str,
) -> Result<BacktestHistoryMetadataSnapshot> {
    let snapshot_path = symbol_dir
        .join(SNAPSHOTS_DIR_NAME)
        .join(format!("{snapshot_hash}.json"));
    let snapshot_bytes = fs::read(&snapshot_path).map_err(|error| {
        metadata_response_error(format!(
            "snapshot {} cannot be read: {error}",
            snapshot_path.display()
        ))
    })?;
    let snapshot: BacktestHistoryMetadataSnapshot = serde_json::from_slice(&snapshot_bytes)
        .map_err(|error| {
            metadata_response_error(format!(
                "snapshot {} is invalid JSON: {error}",
                snapshot_path.display()
            ))
        })?;
    validate_loaded_snapshot(&snapshot, logical_symbol, snapshot_hash)?;
    Ok(snapshot)
}

/// Resolves the metadata snapshot that covers one backtest range.
///
/// The active snapshot remains preferred whenever it covers the full range.
/// A remote fill may retain a narrower, disjoint snapshot without replacing a
/// broader active pointer; in that case the compatible retained snapshot is
/// the authoritative mapping for the requested range.
#[doc(hidden)]
pub fn resolve_backtest_metadata_snapshot(
    cache_dir: &Path,
    logical_symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
    validate_logical_symbol(logical_symbol)?;
    if start_ns >= end_ns {
        return Err(DataError::Validation(
            "backtest metadata resolution requires start_ns < end_ns".to_string(),
        ));
    }
    let metadata_cache = BacktestHistoryMetadataCache::open_read_only(cache_dir);
    let Some(active) = metadata_cache.load_active(logical_symbol)? else {
        return Ok(None);
    };
    if active.covers_range((start_ns, end_ns)) {
        return Ok(Some(active));
    }
    Ok(
        existing_snapshot_for_remote_range(&metadata_cache, logical_symbol, (start_ns, end_ns))?
            .or(Some(active)),
    )
}

/// Resolves the metadata snapshot that can safely read a canonical-minute
/// cache range.
///
/// The active pointer is preferred whenever it covers the range. When it does
/// not, a compatible retained snapshot can provide the mapping for a remote
/// miss. Existing monthly partitions may retain an older snapshot hash only
/// when immutable sidecars prove identical session, trading-day, and physical
/// mapping semantics over every cached range that will be read. Session or
/// mapping changes never become best-effort cache hits.
#[doc(hidden)]
pub fn resolve_minute_cache_metadata_snapshot(
    cache_dir: &Path,
    logical_symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
    let metadata_cache = BacktestHistoryMetadataCache::open_read_only(cache_dir);
    let Some(active) =
        resolve_backtest_metadata_snapshot(cache_dir, logical_symbol, start_ns, end_ns)?
    else {
        return Ok(None);
    };
    let minute_cache = MinuteKlineCache::open_read_only(cache_dir);
    let active_snapshot = minute_cache_snapshot_from_metadata(&active)?;
    let active_error =
        match minute_cache.inspect(logical_symbol, start_ns, end_ns, &active_snapshot) {
            Ok(_) => return Ok(Some(active)),
            Err(error) if is_minute_snapshot_mismatch(&error) => error,
            Err(error) => return Err(error),
        };

    for historical in metadata_cache.load_snapshots(logical_symbol)? {
        if historical.snapshot_hash == active.snapshot_hash
            || historical.schema_version != active.schema_version
            || historical.session.snapshot_hash() != active.session.snapshot_hash()
            || !historical.covers_range((start_ns, end_ns))
        {
            continue;
        }
        let snapshot = minute_cache_snapshot_from_metadata(&historical)?;
        match minute_cache.inspect(logical_symbol, start_ns, end_ns, &snapshot) {
            Ok(status) if status.months.iter().any(|month| month.present) => {
                return Ok(Some(historical));
            }
            Ok(_) => {}
            Err(error) if is_minute_snapshot_mismatch(&error) => {}
            Err(error) => return Err(error),
        }
    }

    Err(active_error)
}

/// Plans an explicit repair for stale monthly partitions before a subsequent
/// remote fill. When a persisted snapshot covers the whole requested range,
/// only partitions that conflict with that snapshot are selected. When none
/// does, every present partition is selected because the remote metadata
/// refresh will establish a new authoritative snapshot.
///
/// This never writes or removes cache data. Callers must keep the ordinary
/// fail-closed reader as the default and invoke a destructive purge only after
/// an explicit operator choice.
#[doc(hidden)]
pub fn plan_minute_cache_stale_partition_repair(
    cache_dir: &Path,
    logical_symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Option<MinuteCacheStalePartitionRepairPlan>> {
    validate_logical_symbol(logical_symbol)?;
    if start_ns >= end_ns {
        return Err(DataError::Validation(
            "minute cache stale repair requires start_ns < end_ns".to_string(),
        ));
    }
    let Some(active) =
        resolve_backtest_metadata_snapshot(cache_dir, logical_symbol, start_ns, end_ns)?
    else {
        return Ok(None);
    };
    let minute_cache = MinuteKlineCache::open_read_only(cache_dir);
    let snapshot = minute_cache_snapshot_from_metadata(&active)?;
    let compatibility =
        minute_cache.snapshot_compatibility(logical_symbol, start_ns, end_ns, &snapshot)?;
    let stale_ranges = if active.covers_range((start_ns, end_ns)) {
        compatibility.mismatched_ranges
    } else {
        compatibility.present_ranges
    };
    if stale_ranges.is_empty() {
        return Ok(None);
    }
    Ok(Some(MinuteCacheStalePartitionRepairPlan {
        snapshot_hash: active.snapshot_hash,
        stale_ranges,
    }))
}

fn minute_cache_snapshot_from_metadata(
    metadata: &BacktestHistoryMetadataSnapshot,
) -> Result<MinuteKlineCacheSnapshot> {
    MinuteKlineCacheSnapshot::new(
        metadata.schema_version,
        metadata.snapshot_hash.clone(),
        metadata.session.snapshot_hash(),
    )
}

pub(crate) fn minute_cache_snapshots_are_compatible(
    cache_dir: &Path,
    logical_symbol: &str,
    stored: &MinuteKlineCacheSnapshot,
    expected: &MinuteKlineCacheSnapshot,
    comparison_ranges: &[(i64, i64)],
) -> Result<bool> {
    if stored == expected || comparison_ranges.is_empty() {
        return Ok(true);
    }
    if stored.version != expected.version || stored.session_hash != expected.session_hash {
        return Ok(false);
    }

    let metadata_cache = BacktestHistoryMetadataCache::open_read_only(cache_dir);
    let Some(stored_metadata) =
        metadata_cache.load_snapshot_by_hash(logical_symbol, stored.calendar_hash.as_str())?
    else {
        return Ok(false);
    };
    let Some(expected_metadata) =
        metadata_cache.load_snapshot_by_hash(logical_symbol, expected.calendar_hash.as_str())?
    else {
        return Ok(false);
    };
    if !metadata_matches_minute_snapshot(&stored_metadata, stored)
        || !metadata_matches_minute_snapshot(&expected_metadata, expected)
        || stored_metadata.schema_version != expected_metadata.schema_version
        || stored_metadata.market_kind != expected_metadata.market_kind
        || stored_metadata.logical_symbol != expected_metadata.logical_symbol
        || stored_metadata.session != expected_metadata.session
    {
        return Ok(false);
    }

    Ok(comparison_ranges.iter().all(|range| {
        physical_segments_cover_range(stored_metadata.physical_segments.as_slice(), *range)
            && physical_segments_cover_range(expected_metadata.physical_segments.as_slice(), *range)
            && trading_days_cover_range(stored_metadata.trading_days.as_slice(), *range)
            && trading_days_cover_range(expected_metadata.trading_days.as_slice(), *range)
            && clipped_physical_segments(&stored_metadata, *range)
                == clipped_physical_segments(&expected_metadata, *range)
            && clipped_trading_days(&stored_metadata, *range)
                == clipped_trading_days(&expected_metadata, *range)
    }))
}

fn metadata_matches_minute_snapshot(
    metadata: &BacktestHistoryMetadataSnapshot,
    snapshot: &MinuteKlineCacheSnapshot,
) -> bool {
    metadata.schema_version == snapshot.version
        && metadata.snapshot_hash == snapshot.calendar_hash
        && metadata.session.snapshot_hash() == snapshot.session_hash
}

fn clipped_physical_segments(
    metadata: &BacktestHistoryMetadataSnapshot,
    range: (i64, i64),
) -> Vec<BacktestHistoryPhysicalSegment> {
    metadata
        .physical_segments
        .iter()
        .filter_map(|segment| {
            let start_ns = segment.start_ns.max(range.0);
            let end_ns = segment.end_ns.min(range.1);
            (start_ns < end_ns).then(|| BacktestHistoryPhysicalSegment {
                physical_symbol: segment.physical_symbol.clone(),
                start_ns,
                end_ns,
            })
        })
        .collect()
}

fn clipped_trading_days(
    metadata: &BacktestHistoryMetadataSnapshot,
    range: (i64, i64),
) -> Vec<BacktestHistoryTradingDay> {
    metadata
        .trading_days
        .iter()
        .filter_map(|day| {
            let start_ns = day.start_ns.max(range.0);
            let end_ns = day.end_ns.min(range.1);
            (start_ns < end_ns).then(|| BacktestHistoryTradingDay {
                date: day.date.clone(),
                is_trading_day: day.is_trading_day,
                start_ns,
                end_ns,
            })
        })
        .collect()
}

fn trading_days_cover_range(days: &[BacktestHistoryTradingDay], range: (i64, i64)) -> bool {
    if range.0 >= range.1 {
        return false;
    }
    let mut cursor = range.0;
    for day in days {
        if day.end_ns <= cursor || day.start_ns >= range.1 {
            continue;
        }
        if day.start_ns > cursor {
            return false;
        }
        cursor = cursor.max(day.end_ns);
        if cursor >= range.1 {
            return true;
        }
    }
    false
}

fn is_minute_snapshot_mismatch(error: &DataError) -> bool {
    matches!(
        error,
        DataError::InvalidResponse(message)
            if message.contains("calendar/session snapshot mismatch")
    )
}

/// Ensures that a logical symbol has a persisted metadata snapshot covering a
/// remote-query range. This is intentionally crate-private: callers reach it
/// only after the query path has proved that a remote operation is needed.
///
/// A present snapshot remains authoritative regardless of its age. Remote
/// refreshes expand to whole canonical-minute trading months, so later misses
/// can reuse one immutable snapshot rather than create incompatible partial
/// month identities. `CacheOnly` callers never call this function and
/// consequently remain fully offline.
pub(crate) async fn ensure_metadata_for_remote_miss(
    cache_dir: &Path,
    auth_provider: Option<&Arc<dyn BacktestHistoryAuthProvider>>,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<BacktestHistoryMetadataSnapshot> {
    validate_metadata_refresh_request(symbol, start_ns, end_ns)?;
    #[cfg(all(feature = "live", feature = "services"))]
    let metadata_range = canonical_minute_metadata_range(start_ns, end_ns)?;
    #[cfg(not(all(feature = "live", feature = "services")))]
    let metadata_range = (start_ns, end_ns);
    let read_only = BacktestHistoryMetadataCache::open_read_only(cache_dir);
    if let Some(snapshot) = existing_snapshot_for_remote_range(&read_only, symbol, metadata_range)?
    {
        return Ok(snapshot);
    }

    #[cfg(all(feature = "live", feature = "services"))]
    {
        let provider = auth_provider.ok_or_else(|| {
            DataError::Validation(
                "remote backtest metadata refresh requires auth_env() or auth_provider()"
                    .to_string(),
            )
        })?;
        let credentials = provider.load().await?;
        let cache = BacktestHistoryMetadataCache::open(cache_dir)?;
        if let Some(snapshot) = existing_snapshot_for_remote_range(&cache, symbol, metadata_range)?
        {
            return Ok(snapshot);
        }
        refresh_metadata_from_official(
            &cache,
            credentials,
            symbol,
            metadata_range.0,
            metadata_range.1,
        )
        .await
    }

    #[cfg(not(all(feature = "live", feature = "services")))]
    {
        let _ = auth_provider;
        Err(DataError::RemoteBacktestHistoryFillUnavailable)
    }
}

fn existing_snapshot_for_remote_range(
    cache: &BacktestHistoryMetadataCache,
    symbol: &str,
    range: (i64, i64),
) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
    let active = cache.load_active(symbol)?;
    if active
        .as_ref()
        .is_some_and(|snapshot| metadata_snapshot_covers_range(snapshot, range))
    {
        return Ok(active);
    }
    for snapshot in cache.load_snapshots(symbol)? {
        if !metadata_snapshot_covers_range(&snapshot, range) {
            continue;
        }
        if active.as_ref().is_some_and(|current| {
            snapshot.schema_version != current.schema_version
                || snapshot.market_kind != current.market_kind
                || snapshot.session.snapshot_hash() != current.session.snapshot_hash()
        }) {
            continue;
        }
        return Ok(Some(snapshot));
    }
    Ok(None)
}

#[cfg(all(feature = "live", feature = "services"))]
fn canonical_minute_metadata_range(start_ns: i64, end_ns: i64) -> Result<(i64, i64)> {
    let start_month = trading_month_for_timestamp_ns(start_ns)?;
    let end_timestamp_ns = end_ns
        .checked_sub(1)
        .ok_or_else(|| DataError::Validation("metadata refresh range end underflow".to_string()))?;
    let end_month = trading_month_for_timestamp_ns(end_timestamp_ns)?;
    let start = trading_month_start_date(start_month.as_str())?;
    let end = next_trading_month_start_date(end_month.as_str())?;
    Ok((
        cst_datetime_ns(start, 18, 0, 0)?,
        cst_datetime_ns(end, 18, 0, 0)?,
    ))
}

#[cfg(all(feature = "live", feature = "services"))]
fn trading_month_start_date(month: &str) -> Result<NaiveDate> {
    if month.len() != 6 || !month.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DataError::Validation(format!(
            "invalid canonical-minute trading month {month}"
        )));
    }
    let year = month[..4].parse::<i32>().map_err(|error| {
        DataError::Validation(format!(
            "invalid canonical-minute trading month year: {error}"
        ))
    })?;
    let month_number = month[4..].parse::<u32>().map_err(|error| {
        DataError::Validation(format!(
            "invalid canonical-minute trading month number: {error}"
        ))
    })?;
    NaiveDate::from_ymd_opt(year, month_number, 1).ok_or_else(|| {
        DataError::Validation(format!("invalid canonical-minute trading month {month}"))
    })
}

#[cfg(all(feature = "live", feature = "services"))]
fn next_trading_month_start_date(month: &str) -> Result<NaiveDate> {
    let start = trading_month_start_date(month)?;
    let (year, month_number) = if start.month() == 12 {
        (
            start.year().checked_add(1).ok_or_else(|| {
                DataError::Validation("canonical-minute trading month year overflow".to_string())
            })?,
            1,
        )
    } else {
        (start.year(), start.month() + 1)
    };
    NaiveDate::from_ymd_opt(year, month_number, 1).ok_or_else(|| {
        DataError::Validation("canonical-minute trading month is out of range".to_string())
    })
}

#[cfg(all(feature = "live", feature = "services"))]
async fn refresh_metadata_from_official(
    cache: &BacktestHistoryMetadataCache,
    credentials: BacktestHistoryCredentials,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<BacktestHistoryMetadataSnapshot> {
    validate_metadata_refresh_request(symbol, start_ns, end_ns)?;
    let (calendar_start, calendar_end) = metadata_calendar_bounds(start_ns, end_ns)?;
    let data_client = crate::DataClient::new();
    let calendar = data_client
        .query_trading_calendar(calendar_start, calendar_end)
        .await?;
    let mapping_days = calendar.iter().filter(|day| day.trading).count();
    if mapping_days == 0 {
        return Err(DataError::InvalidResponse(
            "official trading calendar returned no trading days for metadata refresh".to_string(),
        ));
    }

    let historical_segments = if is_main_continuous_contract(symbol) {
        data_client
            .query_his_cont_underlying_segments(symbol, mapping_days, Some(calendar_end))
            .await?
    } else {
        Vec::new()
    };
    let physical_segments = physical_segments_for_snapshot(
        symbol,
        (start_ns, end_ns),
        calendar.as_slice(),
        historical_segments.as_slice(),
    )?;

    let session = metadata_query_session_builder(credentials).build()?;
    let session =
        session_template_from_official_metadata(&session, symbol, &physical_segments).await?;
    let snapshot = BacktestHistoryMetadataSnapshot {
        schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
        market_kind: BacktestHistoryMarketKind::Futures,
        logical_symbol: symbol.to_string(),
        captured_at_ns: Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            DataError::InvalidResponse(
                "current timestamp overflowed while recording backtest metadata".to_string(),
            )
        })?,
        trading_days: metadata_trading_days(calendar.as_slice())?,
        session,
        physical_segments,
        snapshot_hash: String::new(),
    };
    cache.store_snapshot_for_remote_miss(snapshot)
}

/// Metadata direct-query helpers travel over the stock market websocket when
/// no dedicated query URL is configured. This session is intentionally
/// separate from the futures server-backtest stream used to fill Tick/60s
/// cache partitions.
#[cfg(all(feature = "live", feature = "services"))]
fn metadata_query_session_builder(
    credentials: BacktestHistoryCredentials,
) -> tqsdk_session::SessionClientBuilder {
    let (user, pass) = credentials.into_parts();
    tqsdk_session::SessionClientBuilder::new(user, pass)
        .enable_query()
        .stock_market()
}

#[cfg(all(feature = "live", feature = "services"))]
async fn session_template_from_official_metadata(
    session: &tqsdk_session::SessionClient,
    logical_symbol: &str,
    physical_segments: &[BacktestHistoryPhysicalSegment],
) -> Result<KlineSessionTemplate> {
    let mut symbols = Vec::with_capacity(physical_segments.len().saturating_add(1));
    let mut seen = BTreeSet::new();
    for symbol in std::iter::once(logical_symbol).chain(
        physical_segments
            .iter()
            .map(|segment| segment.physical_symbol.as_str()),
    ) {
        if seen.insert(symbol) {
            symbols.push(symbol);
        }
    }
    let infos = session.query_symbol_info(symbols.as_slice()).await?;
    let logical_info = infos
        .iter()
        .find(|info| info.instrument_id.as_str() == logical_symbol)
        .ok_or_else(|| {
            DataError::InvalidResponse(format!(
                "official metadata did not return the requested symbol {logical_symbol}"
            ))
        })?;
    validate_futures_symbol_info(logical_symbol, logical_info.class)?;
    let session_info = infos
        .iter()
        .find(|info| {
            is_supported_futures_class(info.class)
                && (!info.trading_time.day.is_empty() || !info.trading_time.night.is_empty())
        })
        .unwrap_or(logical_info);
    validate_futures_symbol_info(session_info.instrument_id.as_str(), session_info.class)?;
    session_template_from_trading_time(
        &session_info.trading_time,
        session_info.instrument_id.as_str(),
    )
}

#[cfg(all(feature = "live", feature = "services"))]
fn validate_futures_symbol_info(symbol: &str, class: tqsdk_session::InstrumentClass) -> Result<()> {
    if is_supported_futures_class(class) {
        Ok(())
    } else {
        Err(DataError::Validation(format!(
            "backtest history metadata for {symbol} is not a futures instrument"
        )))
    }
}

#[cfg(all(feature = "live", feature = "services"))]
fn is_supported_futures_class(class: tqsdk_session::InstrumentClass) -> bool {
    matches!(
        class,
        tqsdk_session::InstrumentClass::Future
            | tqsdk_session::InstrumentClass::Continuous
            | tqsdk_session::InstrumentClass::Index
    )
}

#[cfg(all(feature = "live", feature = "services"))]
fn session_template_from_trading_time(
    trading_time: &TradingTime,
    symbol: &str,
) -> Result<KlineSessionTemplate> {
    let mut windows = Vec::with_capacity(trading_time.day.len() + trading_time.night.len());
    for (index, pair) in trading_time.night.iter().enumerate() {
        let (start, end) = parse_trading_time_pair(pair, symbol, "night", index)?;
        windows.push(KlineSessionWindow::new(
            trading_time_offset_ns(start)?,
            trading_time_offset_ns(end)?,
        )?);
    }
    for (index, pair) in trading_time.day.iter().enumerate() {
        let (start, end) = parse_trading_time_pair(pair, symbol, "day", index)?;
        windows.push(KlineSessionWindow::new(
            trading_time_offset_ns(start)?,
            trading_time_offset_ns(end)?,
        )?);
    }
    if windows.is_empty() {
        return Err(DataError::InvalidResponse(format!(
            "official metadata for {symbol} has no trading_time windows"
        )));
    }
    windows.sort_by_key(|window| (window.start_offset_ns, window.end_offset_ns));
    let snapshot_hash = format!(
        "official-trading-time-v1-{:x}",
        Sha1::digest(serde_json::to_vec(windows.as_slice()).map_err(|error| {
            DataError::InvalidResponse(format!(
                "cannot encode official trading_time for {symbol}: {error}"
            ))
        })?)
    );
    KlineSessionTemplate::new(snapshot_hash, windows)
}

#[cfg(all(feature = "live", feature = "services"))]
fn parse_trading_time_pair(
    pair: &[String],
    symbol: &str,
    session_kind: &str,
    index: usize,
) -> Result<(i64, i64)> {
    let [start, end] = pair else {
        return Err(DataError::InvalidResponse(format!(
            "official {session_kind} trading_time window {index} for {symbol} must contain exactly two endpoints"
        )));
    };
    Ok((
        parse_trading_time_endpoint(start, symbol, session_kind, index)?,
        parse_trading_time_endpoint(end, symbol, session_kind, index)?,
    ))
}

#[cfg(all(feature = "live", feature = "services"))]
fn parse_trading_time_endpoint(
    value: &str,
    symbol: &str,
    session_kind: &str,
    index: usize,
) -> Result<i64> {
    let mut pieces = value.split(':');
    let hour = pieces
        .next()
        .and_then(|part| part.parse::<i64>().ok())
        .filter(|hour| (0..48).contains(hour));
    let minute = pieces
        .next()
        .and_then(|part| part.parse::<i64>().ok())
        .filter(|minute| (0..60).contains(minute));
    let second = pieces
        .next()
        .and_then(|part| part.parse::<i64>().ok())
        .filter(|second| (0..60).contains(second));
    if pieces.next().is_some() || hour.is_none() || minute.is_none() || second.is_none() {
        return Err(DataError::InvalidResponse(format!(
            "official {session_kind} trading_time endpoint {value:?} at index {index} for {symbol} is invalid"
        )));
    }
    let seconds = hour
        .and_then(|hour| hour.checked_mul(60 * 60))
        .and_then(|seconds| minute.and_then(|minute| seconds.checked_add(minute * 60)))
        .and_then(|seconds| second.and_then(|second| seconds.checked_add(second)))
        .ok_or_else(|| {
            DataError::InvalidResponse(format!(
                "official {session_kind} trading_time endpoint {value:?} at index {index} for {symbol} overflowed"
            ))
        })?;
    Ok(seconds)
}

#[cfg(all(feature = "live", feature = "services"))]
fn trading_time_offset_ns(seconds_since_midnight: i64) -> Result<i64> {
    const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
    const TRADING_DAY_START_SECONDS: i64 = 18 * 60 * 60;
    let timestamp_seconds = if seconds_since_midnight < TRADING_DAY_START_SECONDS {
        seconds_since_midnight.checked_add(SECONDS_PER_DAY)
    } else {
        Some(seconds_since_midnight)
    }
    .ok_or_else(|| {
        DataError::InvalidResponse("official trading_time offset overflowed".to_string())
    })?;
    let offset_seconds = timestamp_seconds
        .checked_sub(TRADING_DAY_START_SECONDS)
        .ok_or_else(|| {
            DataError::InvalidResponse(
                "official trading_time predates trading-day start".to_string(),
            )
        })?;
    let offset_ns = offset_seconds.checked_mul(1_000_000_000).ok_or_else(|| {
        DataError::InvalidResponse("official trading_time nanosecond offset overflowed".to_string())
    })?;
    if !(0..=24 * 60 * 60 * 1_000_000_000).contains(&offset_ns) {
        return Err(DataError::InvalidResponse(
            "official trading_time exceeds the canonical CST trading day".to_string(),
        ));
    }
    Ok(offset_ns)
}

#[cfg(all(feature = "live", feature = "services"))]
fn metadata_calendar_bounds(start_ns: i64, end_ns: i64) -> Result<(NaiveDate, NaiveDate)> {
    let start_date = cst_date_from_timestamp_ns(start_ns)?;
    let end_date = cst_date_from_timestamp_ns(end_ns.checked_sub(1).ok_or_else(|| {
        DataError::Validation("metadata refresh end_ns underflowed".to_string())
    })?)?;
    let lower = start_date.checked_sub_days(Days::new(14)).ok_or_else(|| {
        DataError::Validation("metadata refresh calendar start predates chrono range".to_string())
    })?;
    let upper = end_date.checked_add_days(Days::new(14)).ok_or_else(|| {
        DataError::Validation("metadata refresh calendar end exceeds chrono range".to_string())
    })?;
    Ok((lower, upper))
}

#[cfg(all(feature = "live", feature = "services"))]
fn metadata_trading_days(
    calendar: &[TradingCalendarRow],
) -> Result<Vec<BacktestHistoryTradingDay>> {
    if calendar.is_empty() {
        return Err(DataError::InvalidResponse(
            "official trading calendar returned no rows for metadata refresh".to_string(),
        ));
    }
    calendar
        .iter()
        .map(|row| {
            let date = parse_calendar_date(row.date.as_str())?;
            let start_ns = cst_datetime_ns(date, 0, 0, 0)?;
            let end_ns = cst_datetime_ns(
                date.checked_add_days(Days::new(1)).ok_or_else(|| {
                    DataError::InvalidResponse(
                        "official trading calendar date exceeds chrono range".to_string(),
                    )
                })?,
                0,
                0,
                0,
            )?;
            Ok(BacktestHistoryTradingDay {
                date: row.date.clone(),
                is_trading_day: row.trading,
                start_ns,
                end_ns,
            })
        })
        .collect()
}

#[cfg(all(feature = "live", feature = "services"))]
fn physical_segments_for_snapshot(
    logical_symbol: &str,
    requested_range: (i64, i64),
    calendar: &[TradingCalendarRow],
    historical_segments: &[HistoricalContUnderlyingSegment],
) -> Result<Vec<BacktestHistoryPhysicalSegment>> {
    if !is_main_continuous_contract(logical_symbol) {
        return Ok(vec![BacktestHistoryPhysicalSegment {
            physical_symbol: logical_symbol.to_string(),
            start_ns: requested_range.0,
            end_ns: requested_range.1,
        }]);
    }

    let mut segments = Vec::with_capacity(historical_segments.len());
    for segment in historical_segments {
        if segment.symbol != logical_symbol {
            return Err(DataError::InvalidResponse(format!(
                "official continuous mapping symbol {} does not match {logical_symbol}",
                segment.symbol
            )));
        }
        if segment.underlying.trim().is_empty() {
            return Err(DataError::InvalidResponse(format!(
                "official continuous mapping for {logical_symbol} has an empty underlying"
            )));
        }
        let start_date = parse_calendar_date(segment.start_date.as_str())?;
        let end_date = parse_calendar_date(segment.end_date.as_str())?;
        let start_ns = continuous_segment_start_ns(start_date, calendar)?;
        let end_ns = continuous_segment_end_ns(end_date, calendar)?;
        if start_ns.is_some_and(|start_ns| end_ns <= start_ns) {
            return Err(DataError::InvalidResponse(format!(
                "official continuous mapping segment {} for {logical_symbol} has an invalid range",
                segment.underlying
            )));
        }
        if end_ns <= requested_range.0 {
            continue;
        }
        let start_ns = match start_ns {
            Some(start_ns) => start_ns.max(requested_range.0),
            None => {
                let first_known_end_ns = cst_datetime_ns(start_date, 18, 0, 0)?;
                if requested_range.0 < first_known_end_ns {
                    return Err(DataError::InvalidResponse(format!(
                        "official trading calendar has no prior trading day before {} and the requested range cannot be clipped to the truncated mapping",
                        start_date.format("%Y-%m-%d")
                    )));
                }
                requested_range.0
            }
        };
        let end_ns = end_ns.min(requested_range.1);
        if start_ns < end_ns {
            segments.push(BacktestHistoryPhysicalSegment {
                physical_symbol: segment.underlying.clone(),
                start_ns,
                end_ns,
            });
        }
    }
    segments.sort_by(|left, right| {
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.end_ns.cmp(&right.end_ns))
            .then_with(|| left.physical_symbol.cmp(&right.physical_symbol))
    });
    if let Some(first) = segments.first_mut()
        && first.start_ns > requested_range.0
    {
        // The official table has no active underlying before a newly listed
        // product's first mapping. Project that leading prefix onto the first
        // physical contract so the server-backtest source can confirm it as a
        // terminal zero-row range. Only the leading edge is extended; gaps
        // between later mappings remain invalid below.
        first.start_ns = requested_range.0;
    }
    if !physical_segments_cover_range(segments.as_slice(), requested_range) {
        return Err(DataError::InvalidResponse(format!(
            "official continuous mapping does not cover [{}, {}) for {logical_symbol}",
            requested_range.0, requested_range.1
        )));
    }
    Ok(segments)
}

pub(crate) fn metadata_snapshot_covers_range(
    snapshot: &BacktestHistoryMetadataSnapshot,
    range: (i64, i64),
) -> bool {
    snapshot.covers_range(range)
}

fn physical_segments_cover_range(
    segments: &[BacktestHistoryPhysicalSegment],
    range: (i64, i64),
) -> bool {
    if range.0 >= range.1 {
        return false;
    }
    let mut cursor = range.0;
    for segment in segments {
        if segment.end_ns <= cursor || segment.start_ns >= range.1 {
            continue;
        }
        if segment.start_ns > cursor {
            return false;
        }
        cursor = cursor.max(segment.end_ns);
        if cursor >= range.1 {
            return true;
        }
    }
    false
}

#[cfg(all(feature = "live", feature = "services"))]
fn continuous_segment_start_ns(
    date: NaiveDate,
    calendar: &[TradingCalendarRow],
) -> Result<Option<i64>> {
    let position = calendar
        .iter()
        .position(|row| row.date == date.format("%Y-%m-%d").to_string() && row.trading)
        .ok_or_else(|| {
            DataError::InvalidResponse(format!(
                "official trading calendar does not contain continuous segment trading day {}",
                date.format("%Y-%m-%d")
            ))
        })?;
    let Some(previous) = calendar[..position].iter().rev().find(|row| row.trading) else {
        return Ok(None);
    };
    cst_datetime_ns(parse_calendar_date(previous.date.as_str())?, 18, 0, 0).map(Some)
}

#[cfg(all(feature = "live", feature = "services"))]
fn continuous_segment_end_ns(date: NaiveDate, calendar: &[TradingCalendarRow]) -> Result<i64> {
    let present = calendar
        .iter()
        .any(|row| row.date == date.format("%Y-%m-%d").to_string() && row.trading);
    if !present {
        return Err(DataError::InvalidResponse(format!(
            "official trading calendar does not contain continuous segment trading day {}",
            date.format("%Y-%m-%d")
        )));
    }
    cst_datetime_ns(date, 18, 0, 0)
}

#[cfg(all(feature = "live", feature = "services"))]
fn cst_date_from_timestamp_ns(timestamp_ns: i64) -> Result<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let nanoseconds = timestamp_ns.rem_euclid(1_000_000_000) as u32;
    let timestamp = Utc
        .timestamp_opt(seconds, nanoseconds)
        .single()
        .ok_or_else(|| {
            DataError::Validation(
                "metadata refresh timestamp cannot be represented in UTC".to_string(),
            )
        })?;
    Ok(timestamp.with_timezone(&cst_offset()).date_naive())
}

#[cfg(all(feature = "live", feature = "services"))]
fn cst_datetime_ns(date: NaiveDate, hour: u32, minute: u32, second: u32) -> Result<i64> {
    let local = date.and_hms_opt(hour, minute, second).ok_or_else(|| {
        DataError::InvalidResponse("failed to build CST metadata timestamp".to_string())
    })?;
    cst_offset()
        .from_local_datetime(&local)
        .single()
        .ok_or_else(|| {
            DataError::InvalidResponse("failed to resolve CST metadata timestamp".to_string())
        })?
        .timestamp_nanos_opt()
        .ok_or_else(|| DataError::InvalidResponse("CST metadata timestamp overflowed".to_string()))
}

#[cfg(all(feature = "live", feature = "services"))]
fn cst_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("China Standard Time offset must be valid")
}

#[cfg(all(feature = "live", feature = "services"))]
fn parse_calendar_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|error| {
        DataError::InvalidResponse(format!(
            "official trading calendar date {value:?} is invalid: {error}"
        ))
    })
}

#[cfg(all(feature = "live", feature = "services"))]
fn is_main_continuous_contract(symbol: &str) -> bool {
    symbol.starts_with("KQ.m@")
}

fn validate_metadata_refresh_request(symbol: &str, start_ns: i64, end_ns: i64) -> Result<()> {
    validate_logical_symbol(symbol)?;
    if start_ns >= end_ns {
        return Err(DataError::Validation(
            "metadata refresh requires start_ns < end_ns".to_string(),
        ));
    }
    Ok(())
}

fn store_explicit_metadata_refresh_snapshot(
    cache: &BacktestHistoryMetadataCache,
    snapshot: BacktestHistoryMetadataSnapshot,
) -> Result<BacktestHistoryMetadataSnapshot> {
    cache.store_snapshot(snapshot)
}

/// Explicit-only metadata maintenance entry point.
///
/// Query APIs do not expose refresh or purge operations. The server resolver is
/// attached in the subsequent session/fill integration step; inspection remains
/// fully available without live/service features.
#[derive(Clone)]
pub struct BacktestHistoryMaintenanceClient {
    cache: BacktestHistoryMetadataCache,
    auth_provider: Option<Arc<dyn BacktestHistoryAuthProvider>>,
}

/// Builder for [`BacktestHistoryMaintenanceClient`].
pub struct BacktestHistoryMaintenanceClientBuilder {
    cache_dir: PathBuf,
    auth_provider: Option<Arc<dyn BacktestHistoryAuthProvider>>,
}

impl BacktestHistoryMaintenanceClientBuilder {
    /// Uses the standard `TQ_AUTH_*` environment only for explicit refreshes.
    #[must_use]
    pub fn auth_env(mut self) -> Self {
        self.auth_provider = Some(Arc::new(EnvironmentMetadataAuthProvider));
        self
    }

    /// Uses an application-supplied lazy credential source for explicit refreshes.
    #[must_use]
    pub fn auth_provider(mut self, provider: impl BacktestHistoryAuthProvider + 'static) -> Self {
        self.auth_provider = Some(Arc::new(provider));
        self
    }

    /// Builds a maintenance client. Authentication is not needed for inspection.
    pub fn build(self) -> Result<BacktestHistoryMaintenanceClient> {
        Ok(BacktestHistoryMaintenanceClient {
            cache: BacktestHistoryMetadataCache::open(self.cache_dir)?,
            auth_provider: self.auth_provider,
        })
    }
}

impl BacktestHistoryMaintenanceClient {
    /// Starts configuring explicit metadata maintenance at one cache root.
    #[must_use]
    pub fn builder(cache_dir: impl Into<PathBuf>) -> BacktestHistoryMaintenanceClientBuilder {
        BacktestHistoryMaintenanceClientBuilder {
            cache_dir: cache_dir.into(),
            auth_provider: None,
        }
    }

    /// Inspects the active local snapshot without requiring authentication.
    pub fn inspect_metadata(
        &self,
        symbol: &str,
    ) -> Result<Option<BacktestHistoryMetadataSnapshot>> {
        self.cache.load_active(symbol)
    }

    /// Explicitly refreshes metadata from the official source.
    pub async fn refresh_metadata(
        &self,
        symbol: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        validate_metadata_refresh_request(symbol, start_ns, end_ns)?;
        let provider = self.auth_provider.as_ref().ok_or(DataError::InvalidState(
            "backtest metadata refresh requires an explicit auth provider",
        ))?;
        let credentials = provider.load().await?;
        #[cfg(all(feature = "live", feature = "services"))]
        {
            let snapshot =
                refresh_metadata_from_official(&self.cache, credentials, symbol, start_ns, end_ns)
                    .await?;
            store_explicit_metadata_refresh_snapshot(&self.cache, snapshot)
        }
        #[cfg(not(all(feature = "live", feature = "services")))]
        {
            let _ = credentials;
            Err(DataError::RemoteBacktestHistoryFillUnavailable)
        }
    }
}

struct EnvironmentMetadataAuthProvider;

impl BacktestHistoryAuthProvider for EnvironmentMetadataAuthProvider {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>,
    > {
        Box::pin(async {
            let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
                DataError::Validation(
                    "TQ_AUTH_USER is required for backtest metadata refresh".to_string(),
                )
            })?;
            let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
                DataError::Validation(
                    "TQ_AUTH_PASS is required for backtest metadata refresh".to_string(),
                )
            })?;
            BacktestHistoryCredentials::new(user, pass).validate()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveSnapshotPointer {
    format_id: String,
    schema_version: u32,
    snapshot_hash: String,
}

impl ActiveSnapshotPointer {
    fn validate(&self, path: &Path) -> Result<()> {
        if self.format_id != BACKTEST_HISTORY_METADATA_FORMAT_ID {
            return Err(metadata_response_error(format!(
                "active pointer {} has unsupported format {}",
                path.display(),
                self.format_id
            )));
        }
        if self.schema_version != BACKTEST_HISTORY_METADATA_SCHEMA_VERSION {
            return Err(metadata_response_error(format!(
                "active pointer {} has unsupported schema version {}",
                path.display(),
                self.schema_version
            )));
        }
        if !is_sha1_hex(self.snapshot_hash.as_str()) {
            return Err(metadata_response_error(format!(
                "active pointer {} has invalid snapshot hash",
                path.display()
            )));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct CanonicalSnapshotBody<'a> {
    format_id: &'static str,
    schema_version: u32,
    market_kind: BacktestHistoryMarketKind,
    logical_symbol: &'a str,
    captured_at_ns: i64,
    trading_days: &'a [BacktestHistoryTradingDay],
    session: &'a KlineSessionTemplate,
    physical_segments: &'a [BacktestHistoryPhysicalSegment],
}

fn normalize_snapshot_for_store(
    mut snapshot: BacktestHistoryMetadataSnapshot,
) -> Result<BacktestHistoryMetadataSnapshot> {
    validate_snapshot_body(&snapshot)?;
    let hash = snapshot_hash(&snapshot)?;
    if !snapshot.snapshot_hash.is_empty() && snapshot.snapshot_hash != hash {
        return Err(DataError::Validation(
            "backtest metadata snapshot_hash does not match its canonical body".to_string(),
        ));
    }
    snapshot.snapshot_hash = hash;
    Ok(snapshot)
}

fn validate_loaded_snapshot(
    snapshot: &BacktestHistoryMetadataSnapshot,
    requested_symbol: &str,
    pointer_hash: &str,
) -> Result<()> {
    validate_snapshot_body(snapshot)?;
    if snapshot.logical_symbol != requested_symbol {
        return Err(metadata_response_error(format!(
            "active snapshot symbol {} does not match requested symbol {requested_symbol}",
            snapshot.logical_symbol
        )));
    }
    let computed_hash = snapshot_hash(snapshot)?;
    if snapshot.snapshot_hash != computed_hash || pointer_hash != computed_hash {
        return Err(metadata_response_error(
            "active snapshot hash does not match its canonical body",
        ));
    }
    Ok(())
}

fn validate_snapshot_body(snapshot: &BacktestHistoryMetadataSnapshot) -> Result<()> {
    if snapshot.schema_version != BACKTEST_HISTORY_METADATA_SCHEMA_VERSION {
        return Err(DataError::Validation(format!(
            "unsupported backtest metadata schema version {}",
            snapshot.schema_version
        )));
    }
    validate_logical_symbol(snapshot.logical_symbol.as_str())?;
    if snapshot.trading_days.is_empty() {
        return Err(DataError::Validation(
            "backtest metadata snapshot must contain at least one trading day".to_string(),
        ));
    }

    let mut previous_date = None;
    for day in &snapshot.trading_days {
        NaiveDate::parse_from_str(day.date.as_str(), "%Y-%m-%d").map_err(|error| {
            DataError::Validation(format!(
                "backtest metadata trading day {} is invalid: {error}",
                day.date
            ))
        })?;
        if day.end_ns <= day.start_ns {
            return Err(DataError::Validation(format!(
                "backtest metadata trading day {} has an invalid range",
                day.date
            )));
        }
        if previous_date
            .as_ref()
            .is_some_and(|date: &String| day.date <= *date)
        {
            return Err(DataError::Validation(
                "backtest metadata trading days must be strictly date-ordered".to_string(),
            ));
        }
        previous_date = Some(day.date.clone());
    }

    KlineSessionTemplate::new(
        snapshot.session.snapshot_hash().to_string(),
        snapshot.session.windows().to_vec(),
    )?;

    let mut previous_end = None;
    for segment in &snapshot.physical_segments {
        validate_logical_symbol(segment.physical_symbol.as_str())?;
        if segment.end_ns <= segment.start_ns {
            return Err(DataError::Validation(format!(
                "backtest metadata physical segment {} has an invalid range",
                segment.physical_symbol
            )));
        }
        if previous_end.is_some_and(|end| segment.start_ns < end) {
            return Err(DataError::Validation(
                "backtest metadata physical segments must be ordered and non-overlapping"
                    .to_string(),
            ));
        }
        previous_end = Some(segment.end_ns);
    }
    Ok(())
}

fn snapshot_hash(snapshot: &BacktestHistoryMetadataSnapshot) -> Result<String> {
    let body = CanonicalSnapshotBody {
        format_id: BACKTEST_HISTORY_METADATA_FORMAT_ID,
        schema_version: snapshot.schema_version,
        market_kind: snapshot.market_kind,
        logical_symbol: snapshot.logical_symbol.as_str(),
        captured_at_ns: snapshot.captured_at_ns,
        trading_days: snapshot.trading_days.as_slice(),
        session: &snapshot.session,
        physical_segments: snapshot.physical_segments.as_slice(),
    };
    let bytes = serde_json::to_vec(&body).map_err(|error| {
        DataError::InvalidResponse(format!(
            "cannot encode canonical metadata snapshot: {error}"
        ))
    })?;
    Ok(format!("{:x}", Sha1::digest(bytes)))
}

struct MetadataLock {
    file: File,
}

impl MetadataLock {
    fn acquire_exclusive(symbol_dir: &Path, root_dir: &Path) -> Result<Self> {
        fs::create_dir_all(symbol_dir)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(symbol_dir.join(LOCK_FILE_NAME))?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Self { file }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Err(DataError::CacheBusy {
                cache_dir: root_dir.to_path_buf(),
                operation: "backtest history metadata write",
            }),
            Err(error) => Err(error.into()),
        }
    }

    fn acquire_shared(symbol_dir: &Path, root_dir: &Path) -> Result<Self> {
        let lock_path = symbol_dir.join(LOCK_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .open(&lock_path)
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    metadata_response_error(format!(
                        "metadata lock {} is missing",
                        lock_path.display()
                    ))
                } else {
                    DataError::from(error)
                }
            })?;
        match FileExt::try_lock_shared(&file) {
            Ok(()) => Ok(Self { file }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Err(DataError::CacheBusy {
                cache_dir: root_dir.to_path_buf(),
                operation: "backtest history metadata read",
            }),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for MetadataLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn write_new_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        return Err(metadata_response_error(format!(
            "snapshot {} unexpectedly already exists",
            path.display()
        )));
    }
    write_atomically(path, bytes)
}

fn write_replace_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    write_atomically(path, bytes)
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        DataError::InvalidResponse(format!("metadata path {} has no parent", path.display()))
    })?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            DataError::InvalidResponse(format!(
                "metadata path {} has no valid file name",
                path.display()
            ))
        })?;
    let (temp_path, mut temp_file) = create_temp_file(parent, file_name)?;
    let result = (|| -> Result<()> {
        temp_file.write_all(bytes)?;
        temp_file.flush()?;
        temp_file.sync_all()?;
        drop(temp_file);
        fs::rename(&temp_path, path)?;
        sync_parent_dir(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn create_temp_file(parent: &Path, file_name: &str) -> Result<(PathBuf, File)> {
    for attempt in 0_u32..128 {
        let candidate = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            attempt
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(DataError::InvalidResponse(format!(
        "cannot allocate an atomic metadata temp file under {}",
        parent.display()
    )))
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        DataError::InvalidResponse(format!("metadata path {} has no parent", path.display()))
    })?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_: &Path) -> Result<()> {
    Ok(())
}

fn validate_logical_symbol(symbol: &str) -> Result<()> {
    if symbol.is_empty() || symbol.trim() != symbol {
        return Err(DataError::Validation(
            "backtest metadata symbol must be non-empty and trimmed".to_string(),
        ));
    }
    if matches!(symbol, "." | "..") {
        return Err(DataError::Validation(
            "backtest metadata symbol must not be a path traversal component".to_string(),
        ));
    }
    Ok(())
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
    escaped
}

fn is_sha1_hex(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn metadata_response_error(reason: impl AsRef<str>) -> DataError {
    DataError::InvalidResponse(format!("backtest history metadata: {}", reason.as_ref()))
}

#[cfg(test)]
mod snapshot_store_tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::{TimeZone, Utc};
    use tqsdk_core::Kline;

    use super::*;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn remote_miss_snapshot_does_not_narrow_a_broader_active_snapshot() {
        let root = test_root("metadata-retain-broad-active");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let broad = cache.store_snapshot(snapshot(1_000, 10_000, 1)).unwrap();
        let narrow = cache
            .store_snapshot_for_remote_miss(snapshot(4_000, 5_000, 2))
            .unwrap();

        assert_ne!(broad.snapshot_hash, narrow.snapshot_hash);
        assert_eq!(
            cache
                .load_active("KQ.m@SHFE.au")
                .unwrap()
                .unwrap()
                .snapshot_hash,
            broad.snapshot_hash
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_refresh_snapshot_replaces_a_broader_active_snapshot() {
        let root = test_root("metadata-explicit-refresh-promotes-active");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let broad = cache.store_snapshot(snapshot(1_000, 10_000, 1)).unwrap();
        let refreshed =
            store_explicit_metadata_refresh_snapshot(&cache, snapshot(20_000, 21_000, 2)).unwrap();

        assert_ne!(broad.snapshot_hash, refreshed.snapshot_hash);
        assert_eq!(
            cache
                .load_active("KQ.m@SHFE.au")
                .unwrap()
                .unwrap()
                .snapshot_hash,
            refreshed.snapshot_hash
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_miss_snapshot_promotes_a_broader_snapshot_over_a_narrow_active_snapshot() {
        let root = test_root("metadata-promote-broad-active");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let narrow = cache.store_snapshot(snapshot(4_000, 5_000, 1)).unwrap();
        let broad = cache
            .store_snapshot_for_remote_miss(snapshot(1_000, 10_000, 2))
            .unwrap();

        assert_ne!(narrow.snapshot_hash, broad.snapshot_hash);
        assert_eq!(
            cache
                .load_active("KQ.m@SHFE.au")
                .unwrap()
                .unwrap()
                .snapshot_hash,
            broad.snapshot_hash
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_miss_reuses_a_retained_snapshot_when_the_active_snapshot_is_elsewhere() {
        let root = test_root("metadata-reuse-retained-range");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let broad = cache.store_snapshot(snapshot(1_000, 10_000, 1)).unwrap();
        let retained = cache
            .store_snapshot_for_remote_miss(snapshot(20_000, 21_000, 2))
            .unwrap();

        assert_eq!(
            cache
                .load_active("KQ.m@SHFE.au")
                .unwrap()
                .unwrap()
                .snapshot_hash,
            broad.snapshot_hash
        );
        assert_eq!(
            existing_snapshot_for_remote_range(&cache, "KQ.m@SHFE.au", (20_000, 21_000))
                .unwrap()
                .unwrap()
                .snapshot_hash,
            retained.snapshot_hash
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn minute_metadata_resolver_uses_retained_snapshot_when_active_misses_range() {
        let root = test_root("metadata-minute-retained-range");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let active = cache.store_snapshot(snapshot(1_000, 10_000, 1)).unwrap();
        let retained = cache
            .store_snapshot_for_remote_miss(snapshot(20_000, 21_000, 2))
            .unwrap();

        let resolved =
            resolve_minute_cache_metadata_snapshot(&root, "KQ.m@SHFE.au", 20_000, 21_000)
                .unwrap()
                .unwrap();

        assert_ne!(resolved.snapshot_hash, active.snapshot_hash);
        assert_eq!(resolved.snapshot_hash, retained.snapshot_hash);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_repair_rebuilds_present_months_when_no_snapshot_covers_the_full_range() {
        let root = test_root("metadata-minute-repair-refresh-range");
        let cache = BacktestHistoryMetadataCache::open(&root).unwrap();
        let january_start = utc_ns(2020, 1, 6, 1, 0);
        let february_start = utc_ns(2020, 2, 3, 1, 0);
        let minute_ns = 60 * 1_000_000_000;
        let january_end = february_start;
        let february_end = february_start + minute_ns;

        let broad = cache
            .store_snapshot(snapshot(january_start, january_end, 1))
            .unwrap();
        let narrow = cache
            .store_snapshot_for_remote_miss(snapshot(february_start, february_end, 2))
            .unwrap();
        assert_eq!(
            cache
                .load_active("KQ.m@SHFE.au")
                .unwrap()
                .unwrap()
                .snapshot_hash,
            broad.snapshot_hash
        );

        let broad_snapshot = minute_cache_snapshot_from_metadata(&broad).unwrap();
        let narrow_snapshot = minute_cache_snapshot_from_metadata(&narrow).unwrap();
        let minute_cache = MinuteKlineCache::open(&root).unwrap();
        minute_cache
            .store_final_range(
                "KQ.m@SHFE.au",
                january_start,
                january_start + minute_ns,
                &broad_snapshot,
                &[Kline {
                    id: 1,
                    datetime: january_start,
                    ..Kline::default()
                }],
            )
            .unwrap();
        minute_cache
            .store_final_range(
                "KQ.m@SHFE.au",
                february_start,
                february_end,
                &narrow_snapshot,
                &[Kline {
                    id: 2,
                    datetime: february_start,
                    ..Kline::default()
                }],
            )
            .unwrap();

        let repair = plan_minute_cache_stale_partition_repair(
            &root,
            "KQ.m@SHFE.au",
            january_start,
            february_end,
        )
        .unwrap()
        .expect("an uncovered range must rebuild every present month");
        assert_eq!(repair.snapshot_hash, broad.snapshot_hash);
        assert_eq!(repair.stale_ranges.len(), 2);
        assert!(
            repair
                .stale_ranges
                .iter()
                .any(|range| range.0 <= january_start && range.1 > january_start)
        );
        assert!(
            repair
                .stale_ranges
                .iter()
                .any(|range| range.0 <= february_start && range.1 > february_start)
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    fn test_root(label: &str) -> PathBuf {
        let nonce = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("tqsdk-data-{label}-{}-{nonce}", std::process::id()))
    }

    fn snapshot(
        start_ns: i64,
        end_ns: i64,
        captured_at_ns: i64,
    ) -> BacktestHistoryMetadataSnapshot {
        BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: "KQ.m@SHFE.au".to_string(),
            captured_at_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au2606".to_string(),
                start_ns,
                end_ns,
            }],
            snapshot_hash: String::new(),
        }
    }

    fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }
}

#[cfg(all(test, feature = "live", feature = "services"))]
mod tests {
    use super::*;

    #[test]
    fn metadata_query_session_uses_stock_target_for_websocket_query_helpers() {
        let builder = metadata_query_session_builder(BacktestHistoryCredentials::new(
            "test-user",
            "test-pass",
        ));
        let target = builder.market_target_ref();

        assert!(builder.query_enabled());
        assert!(target.stock);
        assert!(!target.backtest);
    }

    #[test]
    fn official_trading_time_uses_the_canonical_18h_trading_day_anchor() {
        let template = session_template_from_trading_time(
            &TradingTime {
                night: vec![vec!["21:00:00".to_string(), "02:30:00".to_string()]],
                day: vec![
                    vec!["09:00:00".to_string(), "10:15:00".to_string()],
                    vec!["10:30:00".to_string(), "11:30:00".to_string()],
                    vec!["13:30:00".to_string(), "15:00:00".to_string()],
                ],
            },
            "KQ.i@SHFE.au",
        )
        .unwrap();

        let hour_ns = 60 * 60 * 1_000_000_000;
        assert_eq!(
            template.windows(),
            &[
                KlineSessionWindow::new(3 * hour_ns, 8 * hour_ns + 30 * 60 * 1_000_000_000)
                    .unwrap(),
                KlineSessionWindow::new(15 * hour_ns, 16 * hour_ns + 15 * 60 * 1_000_000_000)
                    .unwrap(),
                KlineSessionWindow::new(
                    16 * hour_ns + 30 * 60 * 1_000_000_000,
                    17 * hour_ns + 30 * 60 * 1_000_000_000
                )
                .unwrap(),
                KlineSessionWindow::new(19 * hour_ns + 30 * 60 * 1_000_000_000, 21 * hour_ns)
                    .unwrap(),
            ]
        );
    }

    #[test]
    fn continuous_mapping_uses_the_previous_real_trading_day_at_18h() {
        let calendar = vec![
            calendar_row("2026-01-01", false),
            calendar_row("2026-01-02", true),
            calendar_row("2026-01-03", false),
            calendar_row("2026-01-04", false),
            calendar_row("2026-01-05", true),
            calendar_row("2026-01-06", true),
        ];
        let requested = (
            cst_datetime_ns(date("2026-01-02"), 18, 0, 0).unwrap(),
            cst_datetime_ns(date("2026-01-06"), 18, 0, 0).unwrap(),
        );
        let segments = physical_segments_for_snapshot(
            "KQ.m@SHFE.au",
            requested,
            calendar.as_slice(),
            &[
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@SHFE.au".to_string(),
                    underlying: "SHFE.au2602".to_string(),
                    start_date: "2026-01-05".to_string(),
                    end_date: "2026-01-05".to_string(),
                    trading_days: 1,
                },
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@SHFE.au".to_string(),
                    underlying: "SHFE.au2604".to_string(),
                    start_date: "2026-01-06".to_string(),
                    end_date: "2026-01-06".to_string(),
                    trading_days: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(
            segments,
            vec![
                BacktestHistoryPhysicalSegment {
                    physical_symbol: "SHFE.au2602".to_string(),
                    start_ns: requested.0,
                    end_ns: cst_datetime_ns(date("2026-01-05"), 18, 0, 0).unwrap(),
                },
                BacktestHistoryPhysicalSegment {
                    physical_symbol: "SHFE.au2604".to_string(),
                    start_ns: cst_datetime_ns(date("2026-01-05"), 18, 0, 0).unwrap(),
                    end_ns: requested.1,
                },
            ]
        );
    }

    #[test]
    fn continuous_mapping_clips_truncated_first_segment_to_requested_start() {
        let calendar = vec![
            calendar_row("2025-06-27", true),
            calendar_row("2025-07-10", true),
            calendar_row("2025-07-11", true),
        ];
        let requested = (
            cst_datetime_ns(date("2025-07-10"), 18, 0, 0).unwrap(),
            cst_datetime_ns(date("2025-07-11"), 18, 0, 0).unwrap(),
        );

        let segments = physical_segments_for_snapshot(
            "KQ.m@SHFE.au",
            requested,
            calendar.as_slice(),
            &[HistoricalContUnderlyingSegment {
                symbol: "KQ.m@SHFE.au".to_string(),
                underlying: "SHFE.au2508".to_string(),
                start_date: "2025-06-27".to_string(),
                end_date: "2025-07-11".to_string(),
                trading_days: 3,
            }],
        )
        .unwrap();

        assert_eq!(
            segments,
            vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au2508".to_string(),
                start_ns: requested.0,
                end_ns: requested.1,
            }]
        );
    }

    #[test]
    fn continuous_mapping_covers_a_pre_listing_prefix_with_the_first_contract() {
        let calendar = vec![
            calendar_row("2025-07-11", true),
            calendar_row("2025-07-14", true),
            calendar_row("2025-07-15", true),
            calendar_row("2025-07-16", true),
            calendar_row("2025-07-17", true),
            calendar_row("2025-07-18", true),
            calendar_row("2025-07-21", true),
            calendar_row("2025-07-22", true),
            calendar_row("2025-07-23", true),
        ];
        let requested = (
            cst_datetime_ns(date("2025-07-11"), 18, 0, 0).unwrap(),
            cst_datetime_ns(date("2025-07-23"), 18, 0, 0).unwrap(),
        );

        let segments = physical_segments_for_snapshot(
            "KQ.m@CZCE.PL",
            requested,
            calendar.as_slice(),
            &[HistoricalContUnderlyingSegment {
                symbol: "KQ.m@CZCE.PL".to_string(),
                underlying: "CZCE.PL601".to_string(),
                start_date: "2025-07-22".to_string(),
                end_date: "2025-07-23".to_string(),
                trading_days: 2,
            }],
        )
        .unwrap();

        assert_eq!(
            segments,
            vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "CZCE.PL601".to_string(),
                start_ns: requested.0,
                end_ns: requested.1,
            }]
        );
    }

    #[test]
    fn continuous_mapping_still_rejects_an_internal_gap() {
        let calendar = vec![
            calendar_row("2025-12-31", true),
            calendar_row("2026-01-01", false),
            calendar_row("2026-01-02", true),
            calendar_row("2026-01-03", true),
            calendar_row("2026-01-04", false),
            calendar_row("2026-01-05", false),
            calendar_row("2026-01-06", true),
            calendar_row("2026-01-07", true),
        ];
        let requested = (
            cst_datetime_ns(date("2025-12-31"), 18, 0, 0).unwrap(),
            cst_datetime_ns(date("2026-01-07"), 18, 0, 0).unwrap(),
        );

        let error = physical_segments_for_snapshot(
            "KQ.m@CZCE.PL",
            requested,
            calendar.as_slice(),
            &[
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@CZCE.PL".to_string(),
                    underlying: "CZCE.PL601".to_string(),
                    start_date: "2026-01-02".to_string(),
                    end_date: "2026-01-03".to_string(),
                    trading_days: 2,
                },
                HistoricalContUnderlyingSegment {
                    symbol: "KQ.m@CZCE.PL".to_string(),
                    underlying: "CZCE.PL603".to_string(),
                    start_date: "2026-01-07".to_string(),
                    end_date: "2026-01-07".to_string(),
                    trading_days: 1,
                },
            ],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DataError::InvalidResponse(message)
                if message.contains("official continuous mapping does not cover")
        ));
    }

    #[test]
    fn canonical_minute_metadata_range_expands_to_full_trading_months() {
        let start_ns = cst_datetime_ns(date("2026-07-14"), 18, 0, 0).unwrap();
        let end_ns = cst_datetime_ns(date("2026-07-27"), 18, 0, 0).unwrap();

        assert_eq!(
            canonical_minute_metadata_range(start_ns, end_ns).unwrap(),
            (
                cst_datetime_ns(date("2026-07-01"), 18, 0, 0).unwrap(),
                cst_datetime_ns(date("2026-08-01"), 18, 0, 0).unwrap(),
            )
        );
    }

    fn calendar_row(date: &str, trading: bool) -> TradingCalendarRow {
        TradingCalendarRow {
            date: date.to_string(),
            trading,
        }
    }

    fn date(value: &str) -> NaiveDate {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()
    }
}
