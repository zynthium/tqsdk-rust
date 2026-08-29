//! Read-only validation for published history snapshot manifests.

use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::snapshot::{BacktestHistorySnapshotError, map_manifest_error};
use crate::history_series_cache::tqbn_snapshot_requires_zstd;

const MANIFEST_VERSION: u32 = 1;
const SNAPSHOTS_DIR: &str = "snapshots";
const CURRENT_FILE: &str = "CURRENT";
const MANIFEST_FILE: &str = "manifest.json";
const LEASE_FILE: &str = "lease.lock";
const MAX_CURRENT_RETRIES: usize = 8;

/// Stable file role used by immutable history snapshot manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BacktestHistorySnapshotFileRole {
    /// Append/recovery-capable Tick file; never safe to hardlink.
    TqbnMutableLayout,
    /// Atomically replaced immutable minute generation.
    TqmkImmutableGeneration,
    /// Atomically replaced immutable daily generation.
    TqdkImmutableGeneration,
    /// Content-addressed metadata snapshot.
    MetadataContentAddressed,
    /// Independently copied pointer such as `active.json`.
    PointerCopy,
}

impl BacktestHistorySnapshotFileRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TqbnMutableLayout => "tqbn_mutable_layout",
            Self::TqmkImmutableGeneration => "tqmk_immutable_generation",
            Self::TqdkImmutableGeneration => "tqdk_immutable_generation",
            Self::MetadataContentAddressed => "metadata_content_addressed",
            Self::PointerCopy => "pointer_copy",
        }
    }

    /// Whether the immutable snapshot contract permits hardlink cloning.
    #[must_use]
    pub const fn allows_hardlink(self) -> bool {
        !matches!(self, Self::TqbnMutableLayout | Self::PointerCopy)
    }
}

/// Publisher disposition for one cache-root relative path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistorySnapshotFileDisposition {
    /// Copy/clone the file and record this manifest role.
    Include(BacktestHistorySnapshotFileRole),
    /// Exclude and recreate the lock/sidecar in the staged generation.
    Rebuild,
}

/// Classifies one cache-root relative file path using the data-owned allowlist.
pub fn classify_backtest_history_snapshot_cache_path(
    path: impl AsRef<Path>,
) -> Result<BacktestHistorySnapshotFileDisposition, BacktestHistorySnapshotError> {
    classify_cache_relative_path(path.as_ref()).map_err(map_manifest_error)
}

/// Deterministic manifest artifact produced from a stable staged cache view.
#[derive(Debug, Clone)]
pub struct BacktestHistorySnapshotManifestArtifact {
    snapshot_id: String,
    identity_sha256: String,
    metadata_snapshot_hash: String,
    manifest_bytes: Vec<u8>,
}

impl BacktestHistorySnapshotManifestArtifact {
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        self.snapshot_id.as_str()
    }

    #[must_use]
    pub fn identity_sha256(&self) -> &str {
        self.identity_sha256.as_str()
    }

    #[must_use]
    pub fn metadata_snapshot_hash(&self) -> &str {
        self.metadata_snapshot_hash.as_str()
    }

    #[must_use]
    pub fn manifest_bytes(&self) -> &[u8] {
        self.manifest_bytes.as_slice()
    }
}

/// Builds manifest v1 without duplicating canonical identity or role rules in publishers.
#[derive(Debug, Clone)]
pub struct BacktestHistorySnapshotManifestBuilder {
    created_at: DateTime<Utc>,
    required_features: Vec<String>,
    catalog_complete: bool,
    catalog_symbols: Vec<String>,
}

impl BacktestHistorySnapshotManifestBuilder {
    #[must_use]
    pub fn new(created_at: DateTime<Utc>) -> Self {
        Self {
            created_at,
            required_features: Vec::new(),
            catalog_complete: false,
            catalog_symbols: Vec::new(),
        }
    }

    #[must_use]
    pub fn required_features<I, S>(mut self, features: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_features = features.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn catalog<I, S>(mut self, complete: bool, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.catalog_complete = complete;
        self.catalog_symbols = symbols.into_iter().map(Into::into).collect();
        self
    }

    pub fn build(
        mut self,
        cache_dir: impl AsRef<Path>,
    ) -> Result<BacktestHistorySnapshotManifestArtifact, BacktestHistorySnapshotError> {
        self.required_features.sort();
        self.required_features.dedup();
        self.catalog_symbols.sort();
        self.catalog_symbols.dedup();
        build_manifest_artifact(
            cache_dir.as_ref(),
            self.created_at,
            self.required_features,
            self.catalog_complete,
            self.catalog_symbols,
        )
        .map_err(map_manifest_error)
    }

    fn from_validated(manifest: &ValidatedSnapshotManifest) -> Self {
        Self {
            created_at: manifest
                .created_at
                .parse()
                .expect("validated manifest created_at must remain RFC3339 UTC"),
            required_features: manifest.required_features.clone(),
            catalog_complete: manifest.catalog_complete,
            catalog_symbols: manifest.catalog_symbols.clone(),
        }
    }
}

/// Coarse, machine-readable manifest validation disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotManifestErrorKind {
    /// No published generation is currently available.
    Unavailable,
    /// The on-disk generation violates the manifest integrity contract.
    Corrupt,
    /// The generation is validly encoded but unsupported by this reader.
    Incompatible,
}

/// Manifest validation failure retaining both disposition and human context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotManifestError {
    kind: SnapshotManifestErrorKind,
    message: String,
}

impl SnapshotManifestError {
    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotManifestErrorKind::Unavailable,
            message: message.into(),
        }
    }

    fn corrupt(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotManifestErrorKind::Corrupt,
            message: message.into(),
        }
    }

    fn incompatible(message: impl Into<String>) -> Self {
        Self {
            kind: SnapshotManifestErrorKind::Incompatible,
            message: message.into(),
        }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> SnapshotManifestErrorKind {
        self.kind
    }

    #[must_use]
    pub(crate) fn message(&self) -> &str {
        self.message.as_str()
    }
}

impl fmt::Display for SnapshotManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "history snapshot manifest {:?}: {}",
            self.kind, self.message
        )
    }
}

impl std::error::Error for SnapshotManifestError {}

/// A validated, immutable generation ready for a later snapshot reader.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedSnapshotManifest {
    generation_dir: PathBuf,
    cache_dir: PathBuf,
    snapshot_id: String,
    identity_sha256: String,
    created_at: String,
    required_features: Vec<String>,
    metadata_snapshot_hash: String,
    catalog_complete: bool,
    catalog_symbols: Vec<String>,
    file_roles: Vec<BacktestHistorySnapshotFileRole>,
    _lease: Arc<GenerationLease>,
}

#[derive(Debug)]
struct GenerationLease {
    _file: File,
}

impl ValidatedSnapshotManifest {
    #[must_use]
    pub(crate) fn generation_dir(&self) -> &Path {
        self.generation_dir.as_path()
    }

    #[must_use]
    pub(crate) fn cache_dir(&self) -> &Path {
        self.cache_dir.as_path()
    }

    #[must_use]
    pub(crate) fn snapshot_id(&self) -> &str {
        self.snapshot_id.as_str()
    }

    #[must_use]
    pub(crate) fn identity_sha256(&self) -> &str {
        self.identity_sha256.as_str()
    }

    pub(crate) fn created_at(&self) -> &str {
        self.created_at.as_str()
    }

    #[must_use]
    pub(crate) fn metadata_snapshot_hash(&self) -> &str {
        self.metadata_snapshot_hash.as_str()
    }

    #[must_use]
    pub(crate) const fn catalog_complete(&self) -> bool {
        self.catalog_complete
    }

    #[must_use]
    pub(crate) fn catalog_contains(&self, symbol: &str) -> bool {
        self.catalog_symbols
            .binary_search_by(|candidate| candidate.as_str().cmp(symbol))
            .is_ok()
    }

    pub(crate) fn catalog_symbols(&self) -> &[String] {
        self.catalog_symbols.as_slice()
    }

    pub(crate) fn file_roles(&self) -> &[BacktestHistorySnapshotFileRole] {
        self.file_roles.as_slice()
    }

    pub(crate) fn manifest_builder(&self) -> BacktestHistorySnapshotManifestBuilder {
        BacktestHistorySnapshotManifestBuilder::from_validated(self)
    }

    pub(crate) fn lifecycle_pin(&self) -> super::BacktestHistoryLifecyclePin {
        self._lease.clone()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    manifest_version: u32,
    snapshot_id: String,
    identity_sha256: String,
    created_at: String,
    minimum_reader: String,
    #[serde(default)]
    required_features: Vec<String>,
    cache_formats: Vec<CacheFormat>,
    metadata_snapshot_hash: String,
    catalog: Catalog,
    #[serde(default)]
    coverage_summary: Vec<Value>,
    files: Vec<ManifestFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheFormat {
    family: String,
    format_id: String,
    schema_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Catalog {
    complete: bool,
    symbols: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestFile {
    path: String,
    role: String,
    size: u64,
    sha256: String,
}

fn build_manifest_artifact(
    cache_dir: &Path,
    created_at: DateTime<Utc>,
    required_features: Vec<String>,
    catalog_complete: bool,
    catalog_symbols: Vec<String>,
) -> Result<BacktestHistorySnapshotManifestArtifact, SnapshotManifestError> {
    reject_symlink_ancestors(cache_dir)?;
    require_regular_directory(
        cache_dir,
        "snapshot cache directory",
        SnapshotManifestErrorKind::Unavailable,
    )?;

    let mut required_features = required_features.into_iter().collect::<BTreeSet<_>>();
    let mut files = Vec::new();
    collect_manifest_input_files(cache_dir, cache_dir, &mut files, &mut required_features)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));

    let metadata_snapshot_hash = metadata_snapshot_hash(files.as_slice())?;
    let mut manifest = SnapshotManifest {
        manifest_version: MANIFEST_VERSION,
        snapshot_id: String::new(),
        identity_sha256: String::new(),
        created_at: created_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        minimum_reader: env!("CARGO_PKG_VERSION").to_string(),
        required_features: required_features.into_iter().collect(),
        cache_formats: vec![
            CacheFormat {
                family: "daily".to_string(),
                format_id: "tqsdk.daily-kline.single-file.v1".to_string(),
                schema_version: 1,
            },
            CacheFormat {
                family: "minute".to_string(),
                format_id: "tqsdk.minute-kline.monthly.v5".to_string(),
                schema_version: 5,
            },
            CacheFormat {
                family: "tick".to_string(),
                format_id: "tqsdk.tqbn.daily.v3".to_string(),
                schema_version: 3,
            },
        ],
        metadata_snapshot_hash: metadata_snapshot_hash.clone(),
        catalog: Catalog {
            complete: catalog_complete,
            symbols: catalog_symbols,
        },
        coverage_summary: Vec::new(),
        files,
    };

    let mut value = serde_json::to_value(&manifest).map_err(|error| {
        SnapshotManifestError::corrupt(format!("cannot encode snapshot manifest: {error}"))
    })?;
    let identity_sha256 = sha256_prefixed(canonical_identity_payload(&value)?.as_slice());
    let snapshot_id = format!(
        "s-{}-{}",
        created_at.format("%Y%m%d"),
        &identity_sha256[7..15]
    );
    manifest.snapshot_id.clone_from(&snapshot_id);
    manifest.identity_sha256.clone_from(&identity_sha256);
    value = serde_json::to_value(&manifest).map_err(|error| {
        SnapshotManifestError::corrupt(format!("cannot encode snapshot manifest: {error}"))
    })?;
    validate_identity(&manifest, &value)?;
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|error| {
        SnapshotManifestError::corrupt(format!("cannot encode snapshot manifest: {error}"))
    })?;

    Ok(BacktestHistorySnapshotManifestArtifact {
        snapshot_id,
        identity_sha256,
        metadata_snapshot_hash,
        manifest_bytes,
    })
}

fn collect_manifest_input_files(
    cache_dir: &Path,
    directory: &Path,
    output: &mut Vec<ManifestFile>,
    required_features: &mut BTreeSet<String>,
) -> Result<(), SnapshotManifestError> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "snapshot cache directory {} cannot be enumerated: {error}",
                directory.display()
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "snapshot cache directory {} cannot be enumerated: {error}",
                directory.display()
            ))
        })?;
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "snapshot cache entry {} unavailable: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(SnapshotManifestError::corrupt(format!(
                "snapshot cache entry {} is symlink",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_manifest_input_files(cache_dir, path.as_path(), output, required_features)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(SnapshotManifestError::corrupt(format!(
                "snapshot cache entry {} is not regular file",
                path.display()
            )));
        }

        let relative = path.strip_prefix(cache_dir).map_err(|_| {
            SnapshotManifestError::corrupt(format!(
                "snapshot cache entry {} escapes cache root",
                path.display()
            ))
        })?;
        let disposition = classify_cache_relative_path(relative)?;
        let BacktestHistorySnapshotFileDisposition::Include(role) = disposition else {
            continue;
        };
        let bytes = fs::read(path.as_path()).map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "snapshot cache entry {} cannot be read: {error}",
                path.display()
            ))
        })?;
        if role == BacktestHistorySnapshotFileRole::TqbnMutableLayout
            && tqbn_snapshot_requires_zstd(bytes.as_slice()).map_err(|error| {
                SnapshotManifestError::corrupt(format!(
                    "snapshot TQBN entry {} cannot be inspected: {error}",
                    path.display()
                ))
            })?
        {
            required_features.insert("tqbn-zstd".to_string());
        }
        let relative = relative
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        output.push(ManifestFile {
            path: format!("cache/{relative}"),
            role: role.as_str().to_string(),
            size: metadata.len(),
            sha256: sha256_prefixed(bytes.as_slice()),
        });
    }
    Ok(())
}

fn classify_cache_relative_path(
    path: &Path,
) -> Result<BacktestHistorySnapshotFileDisposition, SnapshotManifestError> {
    if path.is_absolute() || path.as_os_str().is_empty() {
        return Err(SnapshotManifestError::corrupt(format!(
            "snapshot cache path {:?} must be non-empty and relative",
            path
        )));
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(SnapshotManifestError::corrupt(format!(
                "snapshot cache path {:?} is not normalized",
                path
            )));
        }
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| SnapshotManifestError::corrupt("snapshot cache path is not UTF-8"))?;
    if is_rebuildable_cache_lock(file_name) {
        return Ok(BacktestHistorySnapshotFileDisposition::Rebuild);
    }

    let value = path.to_string_lossy();
    let role = if value.ends_with(".tqbn") {
        BacktestHistorySnapshotFileRole::TqbnMutableLayout
    } else if value.ends_with(".tqmk") {
        BacktestHistorySnapshotFileRole::TqmkImmutableGeneration
    } else if value.ends_with(".tqdk") {
        BacktestHistorySnapshotFileRole::TqdkImmutableGeneration
    } else if file_name == "active.json" {
        BacktestHistorySnapshotFileRole::PointerCopy
    } else if value.ends_with(".json")
        && path
            .components()
            .any(|component| component.as_os_str() == "snapshots")
    {
        BacktestHistorySnapshotFileRole::MetadataContentAddressed
    } else {
        return Err(SnapshotManifestError::incompatible(format!(
            "snapshot cache path {:?} has no known manifest role",
            path
        )));
    };
    Ok(BacktestHistorySnapshotFileDisposition::Include(role))
}

/// Opens and validates the generation selected by `CURRENT` without repairing it.
pub(crate) fn open_current_manifest(
    history_root: &Path,
) -> Result<ValidatedSnapshotManifest, SnapshotManifestError> {
    open_current_manifest_with_retries(history_root, MAX_CURRENT_RETRIES)
}

pub(crate) fn open_generation_manifest(
    history_root: &Path,
    generation_dir: &Path,
) -> Result<ValidatedSnapshotManifest, SnapshotManifestError> {
    reject_symlink_ancestors(history_root)?;
    require_regular_directory(
        history_root,
        "history root",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    let namespace = generation_dir.parent().ok_or_else(|| {
        SnapshotManifestError::corrupt("generation must have a namespace directory")
    })?;
    if namespace.parent() != Some(history_root)
        || !matches!(
            namespace.file_name().and_then(|value| value.to_str()),
            Some(SNAPSHOTS_DIR | "staging")
        )
    {
        return Err(SnapshotManifestError::corrupt(
            "generation must be a direct child of history-root snapshots/ or staging/",
        ));
    }
    let snapshot_id = generation_dir
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| SnapshotManifestError::corrupt("generation name must be UTF-8"))?;
    if !is_safe_snapshot_id(snapshot_id) {
        return Err(SnapshotManifestError::corrupt(
            "generation name is not a safe snapshot id",
        ));
    }
    require_regular_directory(
        generation_dir,
        "generation",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    let lease = acquire_generation_lease(generation_dir)?;
    load_generation_manifest(history_root, generation_dir, snapshot_id, lease)
}

fn load_generation_manifest(
    history_root: &Path,
    generation_dir: &Path,
    snapshot_id: &str,
    lease: Arc<GenerationLease>,
) -> Result<ValidatedSnapshotManifest, SnapshotManifestError> {
    validate_generation_layout(generation_dir)?;
    let manifest_path = generation_dir.join(MANIFEST_FILE);
    let manifest_bytes = read_regular_file(
        manifest_path.as_path(),
        "manifest",
        SnapshotManifestErrorKind::Corrupt,
    )?;
    let manifest_value: Value =
        serde_json::from_slice(manifest_bytes.as_slice()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "manifest {} is invalid JSON: {error}",
                manifest_path.display()
            ))
        })?;
    let manifest: SnapshotManifest =
        serde_json::from_value(manifest_value.clone()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "manifest {} has invalid fields: {error}",
                manifest_path.display()
            ))
        })?;
    validate_manifest(
        history_root,
        generation_dir,
        snapshot_id,
        &manifest,
        &manifest_value,
        lease,
    )
}

fn open_current_manifest_with_retries(
    history_root: &Path,
    retries_remaining: usize,
) -> Result<ValidatedSnapshotManifest, SnapshotManifestError> {
    if retries_remaining == 0 {
        return Err(SnapshotManifestError::unavailable(
            "CURRENT changed repeatedly while acquiring a snapshot lease",
        ));
    }
    reject_symlink_ancestors(history_root)?;
    require_regular_directory(
        history_root,
        "history root",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    require_regular_directory(
        history_root.join(SNAPSHOTS_DIR).as_path(),
        "snapshots directory",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    let current_path = history_root.join(CURRENT_FILE);
    let current = read_regular_file(
        current_path.as_path(),
        "CURRENT",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    let snapshot_id = parse_current(current.as_slice())?;
    let generation_dir = history_root.join(SNAPSHOTS_DIR).join(snapshot_id.as_str());
    let generation_metadata = fs::symlink_metadata(generation_dir.as_path()).map_err(|error| {
        SnapshotManifestError::unavailable(format!(
            "generation {} is unavailable: {error}",
            generation_dir.display()
        ))
    })?;
    if generation_metadata.file_type().is_symlink() || !generation_metadata.is_dir() {
        return Err(SnapshotManifestError::corrupt(format!(
            "generation {} is not a regular directory",
            generation_dir.display()
        )));
    }
    let lease = acquire_generation_lease(generation_dir.as_path())?;
    let confirmed_current = read_regular_file(
        current_path.as_path(),
        "CURRENT",
        SnapshotManifestErrorKind::Unavailable,
    )?;
    if parse_current(confirmed_current.as_slice())? != snapshot_id {
        return open_current_manifest_with_retries(history_root, retries_remaining - 1);
    }
    validate_generation_layout(generation_dir.as_path())?;

    let manifest_path = generation_dir.join(MANIFEST_FILE);
    let manifest_bytes = read_regular_file(
        manifest_path.as_path(),
        "manifest",
        SnapshotManifestErrorKind::Corrupt,
    )?;
    let manifest_value: Value =
        serde_json::from_slice(manifest_bytes.as_slice()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "manifest {} is invalid JSON: {error}",
                manifest_path.display()
            ))
        })?;
    let manifest: SnapshotManifest =
        serde_json::from_value(manifest_value.clone()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "manifest {} has invalid fields: {error}",
                manifest_path.display()
            ))
        })?;

    validate_manifest(
        history_root,
        generation_dir.as_path(),
        snapshot_id.as_str(),
        &manifest,
        &manifest_value,
        lease,
    )
}

fn reject_symlink_ancestors(path: &Path) -> Result<(), SnapshotManifestError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                SnapshotManifestError::unavailable(format!(
                    "cannot resolve history root against current directory: {error}"
                ))
            })?
            .join(path)
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(current.as_path()).map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "history root ancestor {} unavailable: {error}",
                current.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(SnapshotManifestError::corrupt(format!(
                "history root ancestor {} is a symlink",
                current.display()
            )));
        }
    }
    Ok(())
}

fn acquire_generation_lease(
    generation_dir: &Path,
) -> Result<Arc<GenerationLease>, SnapshotManifestError> {
    let lease_path = generation_dir.join(LEASE_FILE);
    let metadata = fs::symlink_metadata(lease_path.as_path()).map_err(|error| {
        SnapshotManifestError::corrupt(format!(
            "generation lease {} unavailable: {error}",
            lease_path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(SnapshotManifestError::corrupt(format!(
            "generation lease {} is not a regular file",
            lease_path.display()
        )));
    }
    let file = OpenOptions::new()
        .read(true)
        .open(lease_path.as_path())
        .map_err(|error| {
            SnapshotManifestError::unavailable(format!(
                "generation lease {} cannot be opened: {error}",
                lease_path.display()
            ))
        })?;
    FileExt::try_lock_shared(&file).map_err(|error| {
        SnapshotManifestError::unavailable(format!(
            "generation lease {} cannot be acquired: {error}",
            lease_path.display()
        ))
    })?;
    Ok(Arc::new(GenerationLease { _file: file }))
}

fn validate_generation_layout(generation_dir: &Path) -> Result<(), SnapshotManifestError> {
    let mut seen = BTreeSet::new();
    for entry in fs::read_dir(generation_dir).map_err(|error| {
        SnapshotManifestError::corrupt(format!(
            "generation {} cannot be enumerated: {error}",
            generation_dir.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "generation {} cannot be enumerated: {error}",
                generation_dir.display()
            ))
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !matches!(name.as_str(), "cache" | MANIFEST_FILE | LEASE_FILE) {
            return Err(SnapshotManifestError::corrupt(format!(
                "generation contains unexpected entry {name}"
            )));
        }
        let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
            SnapshotManifestError::corrupt(format!("generation entry {name} unavailable: {error}"))
        })?;
        let valid = if name == "cache" {
            metadata.is_dir()
        } else {
            metadata.is_file()
        };
        if metadata.file_type().is_symlink() || !valid {
            return Err(SnapshotManifestError::corrupt(format!(
                "generation entry {name} has invalid file type"
            )));
        }
        seen.insert(name);
    }
    let expected = ["cache", LEASE_FILE, MANIFEST_FILE]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    if seen != expected {
        return Err(SnapshotManifestError::corrupt(
            "generation layout is incomplete",
        ));
    }
    Ok(())
}

fn read_regular_file(
    path: &Path,
    label: &str,
    absent_kind: SnapshotManifestErrorKind,
) -> Result<Vec<u8>, SnapshotManifestError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| match absent_kind {
        SnapshotManifestErrorKind::Unavailable => SnapshotManifestError::unavailable(format!(
            "{label} {} is unavailable: {error}",
            path.display()
        )),
        SnapshotManifestErrorKind::Corrupt => SnapshotManifestError::corrupt(format!(
            "{label} {} is unavailable: {error}",
            path.display()
        )),
        SnapshotManifestErrorKind::Incompatible => {
            unreachable!("incompatible cannot describe an absent file")
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(SnapshotManifestError::corrupt(format!(
            "{label} {} is not a regular non-symlink file",
            path.display()
        )));
    }
    fs::read(path).map_err(|error| {
        SnapshotManifestError::corrupt(format!("cannot read {label} {}: {error}", path.display()))
    })
}

fn parse_current(bytes: &[u8]) -> Result<String, SnapshotManifestError> {
    let current = std::str::from_utf8(bytes).map_err(|error| {
        SnapshotManifestError::corrupt(format!("CURRENT is not UTF-8: {error}"))
    })?;
    if !current.ends_with('\n') || current[..current.len() - 1].contains('\n') {
        return Err(SnapshotManifestError::corrupt(
            "CURRENT must contain exactly one snapshot_id followed by one newline",
        ));
    }
    let snapshot_id = &current[..current.len() - 1];
    if !is_safe_snapshot_id(snapshot_id) {
        return Err(SnapshotManifestError::corrupt(format!(
            "CURRENT contains unsafe snapshot_id {snapshot_id:?}"
        )));
    }
    Ok(snapshot_id.to_string())
}

fn validate_manifest(
    history_root: &Path,
    generation_dir: &Path,
    current_snapshot_id: &str,
    manifest: &SnapshotManifest,
    manifest_value: &Value,
    lease: Arc<GenerationLease>,
) -> Result<ValidatedSnapshotManifest, SnapshotManifestError> {
    if manifest.manifest_version != MANIFEST_VERSION {
        return Err(SnapshotManifestError::incompatible(format!(
            "manifest version {} is unsupported; expected {MANIFEST_VERSION}",
            manifest.manifest_version
        )));
    }
    validate_required_features(manifest.required_features.as_slice())?;
    if manifest.snapshot_id != current_snapshot_id
        || generation_dir
            .file_name()
            .is_none_or(|name| name != std::ffi::OsStr::new(manifest.snapshot_id.as_str()))
    {
        return Err(SnapshotManifestError::corrupt(
            "CURRENT, generation directory, and manifest snapshot_id must agree",
        ));
    }
    validate_snapshot_id(manifest)?;
    if !reader_version_is_compatible(manifest.minimum_reader.as_str()) {
        return Err(SnapshotManifestError::incompatible(format!(
            "manifest minimum_reader {} exceeds reader {}",
            manifest.minimum_reader,
            env!("CARGO_PKG_VERSION")
        )));
    }
    validate_formats(manifest.cache_formats.as_slice())?;
    validate_sorted_unique(
        manifest.catalog.symbols.as_slice(),
        "catalog symbols",
        SnapshotManifestErrorKind::Corrupt,
    )?;
    validate_files(generation_dir, manifest.files.as_slice())?;
    validate_identity(manifest, manifest_value)?;

    let cache_dir = generation_dir.join("cache");
    let cache_metadata = fs::symlink_metadata(cache_dir.as_path()).map_err(|error| {
        SnapshotManifestError::corrupt(format!(
            "cache directory {} is unavailable: {error}",
            cache_dir.display()
        ))
    })?;
    if cache_metadata.file_type().is_symlink() || !cache_metadata.is_dir() {
        return Err(SnapshotManifestError::corrupt(format!(
            "cache directory {} is not a regular directory",
            cache_dir.display()
        )));
    }
    if generation_dir.parent().and_then(Path::parent) != Some(history_root) {
        return Err(SnapshotManifestError::corrupt(
            "generation directory is not nested under the supplied history root",
        ));
    }
    let mut file_roles = manifest
        .files
        .iter()
        .map(|file| manifest_role(file.role.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    file_roles.sort();
    file_roles.dedup();

    Ok(ValidatedSnapshotManifest {
        generation_dir: generation_dir.to_path_buf(),
        cache_dir,
        snapshot_id: manifest.snapshot_id.clone(),
        identity_sha256: manifest.identity_sha256.clone(),
        created_at: manifest.created_at.clone(),
        required_features: manifest.required_features.clone(),
        metadata_snapshot_hash: manifest.metadata_snapshot_hash.clone(),
        catalog_complete: manifest.catalog.complete,
        catalog_symbols: manifest.catalog.symbols.clone(),
        file_roles,
        _lease: lease,
    })
}

fn validate_required_features(features: &[String]) -> Result<(), SnapshotManifestError> {
    validate_sorted_unique(
        features,
        "required features",
        SnapshotManifestErrorKind::Corrupt,
    )?;
    for feature in features {
        match feature.as_str() {
            "tqbn-zstd" if cfg!(feature = "tqbn-zstd") => {}
            "tqbn-zstd" => {
                return Err(SnapshotManifestError::incompatible(
                    "manifest requires tqbn-zstd but reader was built without it",
                ));
            }
            _ => {
                return Err(SnapshotManifestError::incompatible(format!(
                    "manifest requires unknown reader feature {feature}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_snapshot_id(manifest: &SnapshotManifest) -> Result<(), SnapshotManifestError> {
    if !is_safe_snapshot_id(manifest.snapshot_id.as_str()) {
        return Err(SnapshotManifestError::corrupt(
            "manifest snapshot_id is unsafe",
        ));
    }
    let created_at =
        DateTime::parse_from_rfc3339(manifest.created_at.as_str()).map_err(|error| {
            SnapshotManifestError::corrupt(format!("manifest created_at is invalid: {error}"))
        })?;
    if created_at.offset().local_minus_utc() != 0 {
        return Err(SnapshotManifestError::corrupt(
            "manifest created_at must use UTC offset",
        ));
    }
    let hash = parse_sha256(manifest.identity_sha256.as_str(), "identity_sha256")?;
    let expected = format!(
        "s-{}-{}",
        created_at.with_timezone(&Utc).format("%Y%m%d"),
        &hash[..8]
    );
    if manifest.snapshot_id != expected {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest snapshot_id {} does not match created_at and identity hash",
            manifest.snapshot_id
        )));
    }
    Ok(())
}

fn validate_formats(formats: &[CacheFormat]) -> Result<(), SnapshotManifestError> {
    let expected = [
        ("daily", "tqsdk.daily-kline.single-file.v1", 1),
        ("minute", "tqsdk.minute-kline.monthly.v5", 5),
        ("tick", "tqsdk.tqbn.daily.v3", 3),
    ];
    if formats.len() != expected.len() {
        return Err(SnapshotManifestError::incompatible(
            "manifest must declare exactly the tick, minute, and daily cache formats",
        ));
    }
    for (format, expected) in formats.iter().zip(expected) {
        if (
            format.family.as_str(),
            format.format_id.as_str(),
            format.schema_version,
        ) != expected
        {
            return Err(SnapshotManifestError::incompatible(format!(
                "unsupported cache format {} {} v{}",
                format.family, format.format_id, format.schema_version
            )));
        }
    }
    Ok(())
}

fn validate_files(
    generation_dir: &Path,
    files: &[ManifestFile],
) -> Result<(), SnapshotManifestError> {
    let paths = files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    validate_sorted_unique(
        paths.as_slice(),
        "manifest file paths",
        SnapshotManifestErrorKind::Corrupt,
    )?;
    for file in files {
        let relative = normalize_cache_relative_path(file.path.as_str())?;
        validate_role(file, relative.as_path())?;
        let path = generation_dir.join(relative);
        let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "manifest file {} is unavailable: {error}",
                file.path
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(SnapshotManifestError::corrupt(format!(
                "manifest file {} is not a regular non-symlink file",
                file.path
            )));
        }
        if file.role == "tqbn_mutable_layout" {
            #[cfg(unix)]
            if metadata.nlink() > 1 {
                return Err(SnapshotManifestError::corrupt(format!(
                    "manifest file {} uses a forbidden hardlink",
                    file.path
                )));
            }
            #[cfg(not(unix))]
            return Err(SnapshotManifestError::incompatible(
                "reader platform cannot verify tqbn hardlink count",
            ));
        }
        if metadata.len() != file.size {
            return Err(SnapshotManifestError::corrupt(format!(
                "manifest file {} size differs from manifest",
                file.path
            )));
        }
        let bytes = fs::read(path.as_path()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "cannot hash manifest file {}: {error}",
                file.path
            ))
        })?;
        if sha256_prefixed(bytes.as_slice()) != file.sha256 {
            return Err(SnapshotManifestError::corrupt(format!(
                "manifest file {} SHA-256 differs from manifest",
                file.path
            )));
        }
    }
    validate_cache_inventory(generation_dir, paths.as_slice())?;
    Ok(())
}

fn require_regular_directory(
    path: &Path,
    label: &str,
    missing_kind: SnapshotManifestErrorKind,
) -> Result<(), SnapshotManifestError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| match missing_kind {
        SnapshotManifestErrorKind::Unavailable => SnapshotManifestError::unavailable(format!(
            "{label} {} unavailable: {error}",
            path.display()
        )),
        SnapshotManifestErrorKind::Corrupt => SnapshotManifestError::corrupt(format!(
            "{label} {} unavailable: {error}",
            path.display()
        )),
        SnapshotManifestErrorKind::Incompatible => SnapshotManifestError::incompatible(format!(
            "{label} {} unavailable: {error}",
            path.display()
        )),
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SnapshotManifestError::corrupt(format!(
            "{label} {} is not a regular non-symlink directory",
            path.display()
        )));
    }
    Ok(())
}

fn validate_cache_inventory(
    generation_dir: &Path,
    listed_paths: &[&str],
) -> Result<(), SnapshotManifestError> {
    let cache_dir = generation_dir.join("cache");
    let mut actual = BTreeSet::new();
    collect_cache_inventory(generation_dir, cache_dir.as_path(), &mut actual)?;
    let listed = listed_paths.iter().copied().collect::<BTreeSet<_>>();
    let actual_refs = actual.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if actual_refs != listed {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest file inventory differs from cache directory: listed={listed:?}, actual={actual_refs:?}"
        )));
    }
    Ok(())
}

fn collect_cache_inventory(
    generation_dir: &Path,
    directory: &Path,
    output: &mut BTreeSet<String>,
) -> Result<(), SnapshotManifestError> {
    for entry in fs::read_dir(directory).map_err(|error| {
        SnapshotManifestError::corrupt(format!(
            "cannot enumerate cache directory {}: {error}",
            directory.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "cannot enumerate cache directory {}: {error}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
            SnapshotManifestError::corrupt(format!(
                "cannot inspect cache entry {}: {error}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(SnapshotManifestError::corrupt(format!(
                "cache entry {} is a symlink",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_cache_inventory(generation_dir, path.as_path(), output)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(SnapshotManifestError::corrupt(format!(
                "cache entry {} is not a regular file",
                path.display()
            )));
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if is_rebuildable_cache_lock(file_name.as_ref()) {
            continue;
        }
        let relative = path.strip_prefix(generation_dir).map_err(|_| {
            SnapshotManifestError::corrupt(format!(
                "cache entry {} escapes generation directory",
                path.display()
            ))
        })?;
        let value = relative
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        if !(value.ends_with(".tqbn")
            || value.ends_with(".tqmk")
            || value.ends_with(".tqdk")
            || value.ends_with(".json"))
        {
            return Err(SnapshotManifestError::corrupt(format!(
                "cache file {value:?} has no manifest role"
            )));
        }
        output.insert(value);
    }
    Ok(())
}

fn is_rebuildable_cache_lock(file_name: &str) -> bool {
    file_name == ".tqsdk-cache-operation.lock"
        || file_name == ".metadata.lock"
        || file_name.ends_with(".tqbn.lock")
        || file_name.ends_with(".tqmk.lock")
        || file_name.ends_with(".tqdk.lock")
}

fn normalize_cache_relative_path(path: &str) -> Result<PathBuf, SnapshotManifestError> {
    let value = Path::new(path);
    if !value.starts_with("cache") || value.is_absolute() {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest path {path:?} must be relative to cache/"
        )));
    }
    let mut components = value.components();
    if components.next() != Some(Component::Normal("cache".as_ref())) {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest path {path:?} must start with cache/"
        )));
    }
    let mut normalized = PathBuf::from("cache");
    for component in components {
        let Component::Normal(component) = component else {
            return Err(SnapshotManifestError::corrupt(format!(
                "manifest path {path:?} is not normalized"
            )));
        };
        normalized.push(component);
    }
    if normalized == Path::new("cache") || normalized.to_string_lossy() != path {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest path {path:?} is not normalized"
        )));
    }
    Ok(normalized)
}

fn validate_role(file: &ManifestFile, path: &Path) -> Result<(), SnapshotManifestError> {
    let declared_role = manifest_role(file.role.as_str())?;
    let relative = path.strip_prefix("cache").map_err(|_| {
        SnapshotManifestError::corrupt(format!(
            "manifest file {} is outside cache role namespace",
            file.path
        ))
    })?;
    let disposition = classify_cache_relative_path(relative)?;
    let BacktestHistorySnapshotFileDisposition::Include(role) = disposition else {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest file {} names a rebuildable path",
            file.path
        )));
    };
    if declared_role != role {
        return Err(SnapshotManifestError::corrupt(format!(
            "manifest file {} does not match role {}",
            file.path, file.role
        )));
    }
    Ok(())
}

fn manifest_role(role: &str) -> Result<BacktestHistorySnapshotFileRole, SnapshotManifestError> {
    match role {
        "tqbn_mutable_layout" => Ok(BacktestHistorySnapshotFileRole::TqbnMutableLayout),
        "tqmk_immutable_generation" => Ok(BacktestHistorySnapshotFileRole::TqmkImmutableGeneration),
        "tqdk_immutable_generation" => Ok(BacktestHistorySnapshotFileRole::TqdkImmutableGeneration),
        "metadata_content_addressed" => {
            Ok(BacktestHistorySnapshotFileRole::MetadataContentAddressed)
        }
        "pointer_copy" => Ok(BacktestHistorySnapshotFileRole::PointerCopy),
        _ => Err(SnapshotManifestError::incompatible(format!(
            "manifest has unknown file role {role}"
        ))),
    }
}

fn validate_identity(
    manifest: &SnapshotManifest,
    value: &Value,
) -> Result<(), SnapshotManifestError> {
    let expected = sha256_prefixed(canonical_identity_payload(value)?.as_slice());
    if manifest.identity_sha256 != expected {
        return Err(SnapshotManifestError::corrupt(
            "manifest identity_sha256 does not match canonical payload",
        ));
    }
    parse_sha256(
        manifest.metadata_snapshot_hash.as_str(),
        "metadata_snapshot_hash",
    )?;
    let expected_metadata_hash = metadata_snapshot_hash(manifest.files.as_slice())?;
    if manifest.metadata_snapshot_hash != expected_metadata_hash {
        return Err(SnapshotManifestError::corrupt(
            "metadata_snapshot_hash does not match the metadata file inventory",
        ));
    }
    Ok(())
}

fn metadata_snapshot_hash(files: &[ManifestFile]) -> Result<String, SnapshotManifestError> {
    let payload = files
        .iter()
        .filter(|file| {
            matches!(
                file.role.as_str(),
                "metadata_content_addressed" | "pointer_copy"
            )
        })
        .map(|file| [file.path.as_str(), file.role.as_str(), file.sha256.as_str()])
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        SnapshotManifestError::corrupt(format!(
            "cannot encode metadata inventory identity: {error}"
        ))
    })?;
    Ok(sha256_prefixed(bytes.as_slice()))
}

fn canonical_identity_payload(value: &Value) -> Result<Vec<u8>, SnapshotManifestError> {
    let mut output = Vec::new();
    write_canonical_json(value, &mut output, true)?;
    Ok(output)
}

fn write_canonical_json(
    value: &Value,
    output: &mut Vec<u8>,
    omit_identity_fields: bool,
) -> Result<(), SnapshotManifestError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => output.extend_from_slice(
            serde_json::to_string(value)
                .map_err(|error| {
                    SnapshotManifestError::corrupt(format!("cannot canonicalize string: {error}"))
                })?
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output, false)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut first = true;
            for key in keys {
                if omit_identity_fields && matches!(key.as_str(), "snapshot_id" | "identity_sha256")
                {
                    continue;
                }
                if !first {
                    output.push(b',');
                }
                first = false;
                output.extend_from_slice(
                    serde_json::to_string(key)
                        .map_err(|error| {
                            SnapshotManifestError::corrupt(format!(
                                "cannot canonicalize key: {error}"
                            ))
                        })?
                        .as_bytes(),
                );
                output.push(b':');
                write_canonical_json(
                    values
                        .get(key)
                        .expect("canonicalized object key must exist"),
                    output,
                    false,
                )?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn validate_sorted_unique(
    values: &[impl AsRef<str>],
    label: &str,
    kind: SnapshotManifestErrorKind,
) -> Result<(), SnapshotManifestError> {
    let values = values.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    if values.windows(2).all(|pair| pair[0] < pair[1]) {
        return Ok(());
    }
    let message = format!("{label} must be sorted and unique");
    Err(match kind {
        SnapshotManifestErrorKind::Unavailable => SnapshotManifestError::unavailable(message),
        SnapshotManifestErrorKind::Corrupt => SnapshotManifestError::corrupt(message),
        SnapshotManifestErrorKind::Incompatible => SnapshotManifestError::incompatible(message),
    })
}

fn reader_version_is_compatible(minimum_reader: &str) -> bool {
    let Some(minimum) = parse_version(minimum_reader) else {
        return false;
    };
    let Some(current) = parse_version(env!("CARGO_PKG_VERSION")) else {
        return false;
    };
    minimum <= current
}

fn parse_version(value: &str) -> Option<(u32, u32, u32)> {
    let mut values = value.split('.');
    let parsed = (
        values.next()?.parse().ok()?,
        values.next()?.parse().ok()?,
        values.next()?.parse().ok()?,
    );
    values.next().is_none().then_some(parsed)
}

fn is_safe_snapshot_id(value: &str) -> bool {
    let mut pieces = value.split('-');
    matches!(pieces.next(), Some("s"))
        && pieces
            .next()
            .is_some_and(|date| date.len() == 8 && date.bytes().all(|byte| byte.is_ascii_digit()))
        && pieces.next().is_some_and(|hash| {
            hash.len() == 8 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        && pieces.next().is_none()
}

fn parse_sha256<'a>(value: &'a str, label: &str) -> Result<&'a str, SnapshotManifestError> {
    let Some(hash) = value.strip_prefix("sha256:") else {
        return Err(SnapshotManifestError::corrupt(format!(
            "{label} lacks sha256: prefix"
        )));
    };
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SnapshotManifestError::corrupt(format!(
            "{label} is not a SHA-256 hex digest"
        )));
    }
    Ok(hash)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backtest_history::store_worker::BlockingScanTestGate;
    use crate::backtest_history::{
        BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
        BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot,
        BacktestHistoryPhysicalSegment, BacktestHistoryRequest, BacktestHistorySnapshot,
        BacktestHistoryTradingDay,
    };
    use crate::{DailyKlineCache, DailyKlineCacheSnapshot, KlineSessionTemplate};

    #[test]
    fn opens_a_valid_current_manifest() {
        let root = valid_root("valid");
        let validated = open_current_manifest(root.as_path()).unwrap();
        assert!(validated.catalog_complete());
        assert!(validated.catalog_contains("KQ.i@SHFE.au"));
        assert!(!validated.catalog_contains("SHFE.missing"));
        assert!(validated.cache_dir().ends_with("cache"));
        remove_root(root);
    }

    #[tokio::test]
    async fn pinned_query_holds_lease_until_detached_blocking_scan_exits() {
        let root = valid_root("pinned-query");
        let manifest = open_current_manifest(root.as_path()).unwrap();
        let lease_path = generation_dir(root.as_path()).join(LEASE_FILE);
        let exclusive = OpenOptions::new().read(true).open(lease_path).unwrap();

        let cache_dir = manifest.cache_dir().to_path_buf();
        fs::remove_dir_all(&cache_dir).unwrap();
        fs::create_dir_all(&cache_dir).unwrap();
        let logical_symbol = "KQ.i@SHFE.au";
        let physical_symbol = "SHFE.au2612";
        let start_ns = 1_767_572_800_000_000_000_i64;
        let end_ns = 1_767_659_200_000_000_000_i64;
        let metadata = BacktestHistoryMetadataCache::open(&cache_dir)
            .unwrap()
            .store_snapshot(BacktestHistoryMetadataSnapshot {
                schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
                market_kind: BacktestHistoryMarketKind::Futures,
                logical_symbol: logical_symbol.to_string(),
                captured_at_ns: end_ns,
                trading_days: vec![BacktestHistoryTradingDay {
                    date: "2026-01-05".to_string(),
                    is_trading_day: true,
                    start_ns,
                    end_ns,
                }],
                session: KlineSessionTemplate::cst_trading_day(),
                physical_segments: vec![BacktestHistoryPhysicalSegment {
                    physical_symbol: physical_symbol.to_string(),
                    start_ns,
                    end_ns,
                }],
                snapshot_hash: String::new(),
            })
            .unwrap();
        let cache_snapshot = DailyKlineCacheSnapshot::new(
            metadata.schema_version,
            metadata.snapshot_hash.clone(),
            metadata.session.snapshot_hash(),
        )
        .unwrap();
        DailyKlineCache::open(&cache_dir)
            .unwrap()
            .store_final_range(logical_symbol, start_ns, end_ns, &cache_snapshot, &[])
            .unwrap();
        let snapshot = BacktestHistorySnapshot::from_validated_manifest(manifest).unwrap();
        let (gate, entered) = BlockingScanTestGate::install();
        let run = snapshot
            .query(BacktestHistoryRequest::kline(
                1,
                logical_symbol,
                Duration::from_secs(86_400),
                start_ns,
                end_ns,
            ))
            .await
            .unwrap();
        drop(snapshot);

        tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(1)))
            .await
            .unwrap()
            .expect("blocking scan must enter the test gate");
        let locked_while_running = FileExt::try_lock_exclusive(&exclusive).is_err();
        drop(run);
        let locked_after_run_drop = FileExt::try_lock_exclusive(&exclusive).is_err();
        gate.release();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if FileExt::try_lock_exclusive(&exclusive).is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("generation lease must release after blocking scan exits");
        FileExt::unlock(&exclusive).unwrap();

        assert!(locked_while_running);
        assert!(locked_after_run_drop);
        remove_root(root);
    }

    #[test]
    fn rejects_incompatible_reader_and_format() {
        let root = valid_root("incompatible-reader");
        rewrite_manifest(root.as_path(), |manifest| {
            manifest["minimum_reader"] = Value::String("99.0.0".to_string())
        });
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Incompatible
        );
        remove_root(root);

        let root = valid_root("incompatible-format");
        rewrite_manifest(root.as_path(), |manifest| {
            manifest["cache_formats"][0]["schema_version"] = Value::from(99)
        });
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Incompatible
        );
        remove_root(root);
    }

    #[test]
    fn rejects_path_escape_unknown_role_and_file_hash_mismatch() {
        let root = valid_root("path-escape");
        rewrite_manifest(root.as_path(), |manifest| {
            manifest["files"][0]["path"] = Value::String("cache/../escape.tqbn".to_string())
        });
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);

        let root = valid_root("unknown-role");
        rewrite_manifest(root.as_path(), |manifest| {
            manifest["files"][0]["role"] = Value::String("unknown".to_string())
        });
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Incompatible
        );
        remove_root(root);

        let root = valid_root("file-hash");
        fs::write(
            root.join("snapshots")
                .read_dir()
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path()
                .join("cache/series/20260829/tick/SHFE.au2612.tqbn"),
            b"changed",
        )
        .unwrap();
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_manifest_file() {
        use std::os::unix::fs::symlink;

        let root = valid_root("symlink");
        let generation = generation_dir(root.as_path());
        let file = generation.join("cache/series/20260829/tick/SHFE.au2612.tqbn");
        let target = generation.join("cache/target.tqbn");
        fs::rename(&file, &target).unwrap();
        symlink(target, file).unwrap();
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);
    }

    #[test]
    fn rejects_identity_mismatch() {
        let root = valid_root("identity");
        rewrite_manifest(root.as_path(), |manifest| {
            manifest["metadata_snapshot_hash"] = Value::String(sha256_prefixed(b"other"))
        });
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);
    }

    #[test]
    fn required_features_are_checked_against_the_compiled_reader() {
        let feature = vec!["tqbn-zstd".to_string()];
        if cfg!(feature = "tqbn-zstd") {
            validate_required_features(feature.as_slice()).unwrap();
        } else {
            assert_eq!(
                validate_required_features(feature.as_slice())
                    .unwrap_err()
                    .kind(),
                SnapshotManifestErrorKind::Incompatible
            );
        }
        assert_eq!(
            validate_required_features(&["unknown-reader-feature".to_string()])
                .unwrap_err()
                .kind(),
            SnapshotManifestErrorKind::Incompatible
        );
    }

    #[test]
    fn rejects_unlisted_cache_and_generation_entries() {
        let root = valid_root("unlisted-cache");
        fs::write(generation_dir(&root).join("cache/rogue.lock"), b"rogue").unwrap();
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);

        let root = valid_root("unlisted-generation");
        fs::write(generation_dir(&root).join("unexpected"), b"rogue").unwrap();
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        remove_root(root);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_tqbn_hardlinks_and_symlinked_history_roots() {
        use std::os::unix::fs::symlink;

        let root = valid_root("hardlink");
        let cache_file = generation_dir(&root).join("cache/series/20260829/tick/SHFE.au2612.tqbn");
        let hardlink = root.with_extension("tqbn-hardlink");
        fs::hard_link(&cache_file, &hardlink).unwrap();
        assert_eq!(
            open_current_manifest(root.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        fs::remove_file(hardlink).unwrap();
        remove_root(root);

        let root = valid_root("root-symlink");
        let alias = root.with_extension("alias");
        symlink(&root, &alias).unwrap();
        assert_eq!(
            open_current_manifest(alias.as_path()).unwrap_err().kind(),
            SnapshotManifestErrorKind::Corrupt
        );
        fs::remove_file(alias).unwrap();
        remove_root(root);
    }

    fn valid_root(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("tqsdk-history-snapshot-{name}-{}", unique_suffix()));
        let cache_file = root.join("snapshots/pending/cache/series/20260829/tick/SHFE.au2612.tqbn");
        fs::create_dir_all(cache_file.parent().unwrap()).unwrap();
        fs::write(&cache_file, b"tick cache").unwrap();
        let metadata_file = root.join("snapshots/pending/cache/backtest-history-metadata-v1/KQ.i%40SHFE.au/snapshots/meta.json");
        fs::create_dir_all(metadata_file.parent().unwrap()).unwrap();
        fs::write(&metadata_file, b"metadata").unwrap();
        let active_file = root.join(
            "snapshots/pending/cache/backtest-history-metadata-v1/KQ.i%40SHFE.au/active.json",
        );
        fs::write(&active_file, b"{}\n").unwrap();
        fs::write(root.join("snapshots/pending/lease.lock"), []).unwrap();

        let mut manifest = serde_json::json!({
            "manifest_version": 1,
            "snapshot_id": "",
            "identity_sha256": "",
            "created_at": "2026-08-29T00:00:00Z",
            "minimum_reader": "0.1.0",
            "required_features": [],
            "cache_formats": [
                {"family": "daily", "format_id": "tqsdk.daily-kline.single-file.v1", "schema_version": 1},
                {"family": "minute", "format_id": "tqsdk.minute-kline.monthly.v5", "schema_version": 5},
                {"family": "tick", "format_id": "tqsdk.tqbn.daily.v3", "schema_version": 3}
            ],
            "metadata_snapshot_hash": sha256_prefixed(b"metadata snapshot"),
            "catalog": {"complete": true, "symbols": ["KQ.i@SHFE.au"]},
            "files": [
                {"path": "cache/backtest-history-metadata-v1/KQ.i%40SHFE.au/active.json", "role": "pointer_copy", "size": fs::metadata(&active_file).unwrap().len(), "sha256": sha256_prefixed(&fs::read(&active_file).unwrap())},
                {"path": "cache/backtest-history-metadata-v1/KQ.i%40SHFE.au/snapshots/meta.json", "role": "metadata_content_addressed", "size": fs::metadata(&metadata_file).unwrap().len(), "sha256": sha256_prefixed(&fs::read(&metadata_file).unwrap())},
                {"path": "cache/series/20260829/tick/SHFE.au2612.tqbn", "role": "tqbn_mutable_layout", "size": fs::metadata(&cache_file).unwrap().len(), "sha256": sha256_prefixed(&fs::read(&cache_file).unwrap())}
            ]
        });
        let parsed: SnapshotManifest = serde_json::from_value(manifest.clone()).unwrap();
        manifest["metadata_snapshot_hash"] =
            Value::String(metadata_snapshot_hash(parsed.files.as_slice()).unwrap());
        let identity = sha256_prefixed(&canonical_identity_payload(&manifest).unwrap());
        let snapshot_id = format!("s-20260829-{}", &identity[7..15]);
        manifest["identity_sha256"] = Value::String(identity);
        manifest["snapshot_id"] = Value::String(snapshot_id.clone());
        let generation = root.join("snapshots").join(snapshot_id);
        fs::rename(root.join("snapshots/pending"), &generation).unwrap();
        fs::write(
            generation.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        fs::write(
            root.join("CURRENT"),
            format!("{}\n", manifest["snapshot_id"].as_str().unwrap()),
        )
        .unwrap();
        root
    }

    fn rewrite_manifest(root: &Path, edit: impl FnOnce(&mut Value)) {
        let generation = generation_dir(root);
        let path = generation.join("manifest.json");
        let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        edit(&mut manifest);
        fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }

    fn generation_dir(root: &Path) -> PathBuf {
        root.join("snapshots")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn remove_root(root: PathBuf) {
        let _ = fs::remove_dir_all(root);
    }
}
