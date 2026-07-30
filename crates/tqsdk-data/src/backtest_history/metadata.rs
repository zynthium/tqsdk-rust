use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::NaiveDate;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::{DataError, KlineSessionTemplate, Result};

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

        let snapshot_path = symbol_dir
            .join(SNAPSHOTS_DIR_NAME)
            .join(format!("{}.json", pointer.snapshot_hash));
        let snapshot_bytes = fs::read(&snapshot_path).map_err(|error| {
            metadata_response_error(format!(
                "active snapshot {} cannot be read: {error}",
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
        validate_loaded_snapshot(&snapshot, logical_symbol, pointer.snapshot_hash.as_str())?;
        Ok(Some(snapshot))
    }

    /// Stores an immutable snapshot and atomically makes it active.
    ///
    /// Existing snapshots are never removed. Supplying a snapshot whose hash is
    /// already present verifies its exact bytes rather than overwriting it.
    pub fn store_snapshot(
        &self,
        snapshot: BacktestHistoryMetadataSnapshot,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        self.ensure_writable()?;
        let snapshot = normalize_snapshot_for_store(snapshot)?;
        let symbol_dir = self.symbol_dir(snapshot.logical_symbol.as_str())?;
        fs::create_dir_all(symbol_dir.join(SNAPSHOTS_DIR_NAME))?;
        let _lock = MetadataLock::acquire_exclusive(&symbol_dir, &self.root_dir)?;

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

        let active = ActiveSnapshotPointer {
            format_id: BACKTEST_HISTORY_METADATA_FORMAT_ID.to_string(),
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            snapshot_hash: snapshot.snapshot_hash.clone(),
        };
        let active_bytes = serde_json::to_vec(&active).map_err(|error| {
            DataError::InvalidResponse(format!("cannot encode metadata active pointer: {error}"))
        })?;
        write_replace_atomically(&symbol_dir.join(ACTIVE_FILE_NAME), active_bytes.as_slice())?;
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
    ///
    /// This pre-integration implementation keeps auth lazy and fails rather
    /// than silently replacing a local snapshot. Task 5 attaches the official
    /// server-backtest metadata resolver to this same API.
    pub async fn refresh_metadata(
        &self,
        symbol: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<BacktestHistoryMetadataSnapshot> {
        validate_logical_symbol(symbol)?;
        if start_ns >= end_ns {
            return Err(DataError::Validation(
                "metadata refresh requires start_ns < end_ns".to_string(),
            ));
        }
        let provider = self.auth_provider.as_ref().ok_or(DataError::InvalidState(
            "backtest metadata refresh requires an explicit auth provider",
        ))?;
        let _credentials = provider.load().await?;
        Err(DataError::InvalidState(
            "backtest metadata refresh is not attached to the server resolver yet",
        ))
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
