use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use fs2::FileExt;
use serde::Deserialize;
use serde_json::{Value, json};
use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryPolicy, BacktestHistoryRequest, BacktestHistorySnapshot,
    BacktestHistorySnapshotFileDisposition, BacktestHistorySnapshotFileRole,
    BacktestHistorySnapshotManifestBuilder, BacktestTickCache,
    classify_backtest_history_snapshot_cache_path,
};

use super::{CliError, CommandOutcome};

const SNAPSHOTS_DIR: &str = "snapshots";
const STAGING_DIR: &str = "staging";
const CURRENT_FILE: &str = "CURRENT";
const PUBLISHER_LOCK: &str = ".tqsdk-cache-snapshot.lock";

#[derive(Debug, Args)]
pub(super) struct SnapshotArgs {
    /// Immutable history snapshot root; distinct from the writable cache root.
    #[arg(long, value_name = "DIR")]
    history_root: PathBuf,

    #[command(subcommand)]
    command: SnapshotCommand,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    /// Inspect the generation selected by CURRENT.
    Inspect,
    /// Plan a role-aware clone without creating roots, locks, or staging files.
    DryRun(CloneArgs),
    /// Stage a clone; safe immutable files may be hardlinked.
    Clone(CloneArgs),
    /// Stage an independent import; every included file is copied.
    Import(CloneArgs),
    /// Validate a staging generation before prewarm work is attached.
    Prewarm(VerificationArgs),
    /// Strictly validate a staging or retained generation.
    Verify(VerificationArgs),
    /// Publish a fully validated staging generation and atomically switch CURRENT.
    Publish(PublishArgs),
    /// Reconcile CURRENT/temp state, optionally restoring an explicit retained generation.
    Recover(RecoverArgs),
    /// Atomically switch CURRENT to a retained verified generation.
    Rollback(MutationGenerationArgs),
    /// Recompute manifest/file/metadata validity for one generation.
    Scrub(OptionalGenerationArgs),
    /// Retain CURRENT plus previous compatible generations and remove only unleased excess.
    Gc(GcArgs),
}

impl SnapshotCommand {
    fn name(&self) -> &'static str {
        match self {
            Self::Inspect => "snapshot inspect",
            Self::DryRun(_) => "snapshot dry-run",
            Self::Clone(_) => "snapshot clone",
            Self::Import(_) => "snapshot import",
            Self::Prewarm(_) => "snapshot prewarm",
            Self::Verify(_) => "snapshot verify",
            Self::Publish(_) => "snapshot publish",
            Self::Recover(_) => "snapshot recover",
            Self::Rollback(_) => "snapshot rollback",
            Self::Scrub(_) => "snapshot scrub",
            Self::Gc(_) => "snapshot gc",
        }
    }
}

#[derive(Debug, Clone, Args)]
struct CloneArgs {
    /// Existing writable cache root to clone/import under its exclusive stable-view gate.
    #[arg(long, value_name = "DIR")]
    source_cache_dir: PathBuf,
    /// Deterministic UTC manifest timestamp; defaults to current UTC time.
    #[arg(long, value_name = "RFC3339")]
    created_at: Option<DateTime<Utc>>,
    /// Explicit served symbol; repeat for the publisher catalog.
    #[arg(long = "catalog-symbol", value_name = "SYMBOL")]
    catalog_symbols: Vec<String>,
    /// Assert that catalog symbols are the complete served universe.
    #[arg(long)]
    catalog_complete: bool,
}

#[derive(Debug, Args)]
struct VerificationArgs {
    #[arg(long)]
    snapshot_id: String,
    /// JSON request list used for CacheOnly inspect plus real query smoke.
    #[arg(long, value_name = "FILE")]
    request_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct VerificationRequestFile {
    requests: Vec<VerificationRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "series", rename_all = "snake_case")]
enum VerificationRequest {
    Tick {
        request_id: u64,
        symbol: String,
        start_ns: i64,
        end_ns: i64,
    },
    Minute {
        request_id: u64,
        symbol: String,
        duration_ns: u64,
        start_ns: i64,
        end_ns: i64,
    },
    Daily {
        request_id: u64,
        symbol: String,
        start_ns: i64,
        end_ns: i64,
    },
}

#[derive(Debug, Args)]
struct OptionalGenerationArgs {
    #[arg(long)]
    snapshot_id: Option<String>,
}

#[derive(Debug, Args)]
struct PublishArgs {
    #[arg(long)]
    snapshot_id: String,
}

#[derive(Debug, Args)]
struct MutationGenerationArgs {
    #[arg(long)]
    snapshot_id: String,
    /// Apply the pointer mutation; without this flag the command is read-only.
    #[arg(long)]
    apply: bool,
}

#[derive(Debug, Args)]
struct RecoverArgs {
    /// Explicit retained generation used only when CURRENT is absent or invalid.
    #[arg(long)]
    snapshot_id: Option<String>,
    /// Apply cleanup or pointer restoration; otherwise report the plan only.
    #[arg(long)]
    apply: bool,
}

#[derive(Debug, Args)]
struct GcArgs {
    /// Total compatible generations to retain, including CURRENT.
    #[arg(long, default_value_t = 3)]
    retain: usize,
    /// Remove eligible generations; otherwise report the plan only.
    #[arg(long)]
    apply: bool,
}

pub(super) async fn execute(args: SnapshotArgs) -> Result<CommandOutcome, CliError> {
    let command = args.command.name();
    let value = match args.command {
        SnapshotCommand::Inspect => inspect(args.history_root.as_path(), command)?,
        SnapshotCommand::DryRun(clone) => plan_clone(args.history_root.as_path(), &clone, command)?,
        SnapshotCommand::Clone(clone) => stage_clone(
            args.history_root.as_path(),
            &clone,
            CloneMode::Clone,
            command,
        )?,
        SnapshotCommand::Import(clone) => stage_clone(
            args.history_root.as_path(),
            &clone,
            CloneMode::Import,
            command,
        )?,
        SnapshotCommand::Prewarm(prewarm_args) => {
            prewarm(args.history_root.as_path(), &prewarm_args, command).await?
        }
        SnapshotCommand::Verify(verify_args) => {
            verify(args.history_root.as_path(), &verify_args, command).await?
        }
        SnapshotCommand::Publish(publish) => {
            publish_generation(args.history_root.as_path(), &publish, command)?
        }
        SnapshotCommand::Recover(recover_args) => {
            recover(args.history_root.as_path(), &recover_args, command)?
        }
        SnapshotCommand::Rollback(rollback_args) => {
            rollback(args.history_root.as_path(), &rollback_args, command)?
        }
        SnapshotCommand::Scrub(scrub_args) => {
            scrub(args.history_root.as_path(), &scrub_args, command)?
        }
        SnapshotCommand::Gc(gc_args) => gc(args.history_root.as_path(), &gc_args, command)?,
    };
    Ok(CommandOutcome {
        value,
        exit_code: 0,
    })
}

#[derive(Debug, Clone, Copy)]
enum CloneMode {
    Clone,
    Import,
}

impl CloneMode {
    const fn allows_hardlink(self) -> bool {
        matches!(self, Self::Clone)
    }
}

#[derive(Debug, Clone, Copy)]
enum GenerationNamespace {
    Staging,
    Snapshots,
}

impl GenerationNamespace {
    const fn dir(self) -> &'static str {
        match self {
            Self::Staging => STAGING_DIR,
            Self::Snapshots => SNAPSHOTS_DIR,
        }
    }
}

#[derive(Debug, Default)]
struct CloneStats {
    roles: BTreeMap<&'static str, RoleStats>,
    copied_bytes: u64,
    hardlinked_bytes: u64,
}

#[derive(Debug, Default)]
struct RoleStats {
    files: u64,
    bytes: u64,
    copied_files: u64,
    hardlinked_files: u64,
}

impl CloneStats {
    fn record(&mut self, role: BacktestHistorySnapshotFileRole, bytes: u64, hardlinked: bool) {
        let stats = self.roles.entry(role.as_str()).or_default();
        stats.files += 1;
        stats.bytes = stats.bytes.saturating_add(bytes);
        if hardlinked {
            stats.hardlinked_files += 1;
            self.hardlinked_bytes = self.hardlinked_bytes.saturating_add(bytes);
        } else {
            stats.copied_files += 1;
            self.copied_bytes = self.copied_bytes.saturating_add(bytes);
        }
    }

    fn as_value(&self) -> Value {
        let roles = self
            .roles
            .iter()
            .map(|(role, stats)| {
                (
                    (*role).to_string(),
                    json!({
                        "files": stats.files,
                        "bytes": stats.bytes,
                        "copied_files": stats.copied_files,
                        "hardlinked_files": stats.hardlinked_files,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        json!({
            "roles": roles,
            "copied_bytes": self.copied_bytes,
            "hardlinked_bytes": self.hardlinked_bytes,
            "reflink_supported": false,
            "reflink_bytes": 0,
        })
    }
}

struct PublisherLock {
    _file: File,
}

fn inspect(history_root: &Path, command: &str) -> Result<Value, CliError> {
    let snapshot = BacktestHistorySnapshot::open(history_root)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    Ok(snapshot_value(command, &snapshot, "current"))
}

fn plan_clone(history_root: &Path, args: &CloneArgs, command: &str) -> Result<Value, CliError> {
    require_source_root(args.source_cache_dir.as_path())?;
    let stats = inspect_source(args.source_cache_dir.as_path(), CloneMode::Clone)?;
    let mut value = stats.as_value();
    let object = value.as_object_mut().expect("clone stats object");
    object.insert("command".into(), Value::String(command.into()));
    object.insert("read_only".into(), Value::Bool(true));
    object.insert(
        "history_root_exists".into(),
        Value::Bool(history_root.exists()),
    );
    object.insert("source_cache_dir".into(), json!(args.source_cache_dir));
    Ok(value)
}

fn stage_clone(
    history_root: &Path,
    args: &CloneArgs,
    mode: CloneMode,
    command: &str,
) -> Result<Value, CliError> {
    require_source_root(args.source_cache_dir.as_path())?;
    prepare_history_root(history_root)?;
    let _publisher = acquire_publisher_lock(history_root)?;
    let source_cache = BacktestTickCache::open(args.source_cache_dir.as_path())?;
    let _stable_view = source_cache.try_acquire_consistency_read_lock()?;
    let created_at = args.created_at.unwrap_or_else(Utc::now);
    let source_before = manifest_builder(created_at, args)
        .build(args.source_cache_dir.as_path())
        .map_err(|error| CliError::Migration(error.to_string()))?;

    let work =
        history_root
            .join(STAGING_DIR)
            .join(format!(".clone-{}-{}", std::process::id(), nonce()?));
    let work_cache = work.join("cache");
    fs::create_dir(&work)?;
    fs::create_dir(&work_cache)?;
    let result = (|| {
        let mut stats = CloneStats::default();
        clone_directory(
            args.source_cache_dir.as_path(),
            args.source_cache_dir.as_path(),
            work_cache.as_path(),
            mode,
            &mut stats,
        )?;
        test_failpoint("clone")?;
        let source_after = manifest_builder(created_at, args)
            .build(args.source_cache_dir.as_path())
            .map_err(|error| CliError::Migration(error.to_string()))?;
        if source_before.identity_sha256() != source_after.identity_sha256() {
            return Err(CliError::Migration(
                "source cache changed while stable-view clone was captured".into(),
            ));
        }
        let artifact = manifest_builder(created_at, args)
            .build(work_cache.as_path())
            .map_err(|error| CliError::Migration(error.to_string()))?;
        if source_after.identity_sha256() != artifact.identity_sha256() {
            return Err(CliError::Migration(
                "staged cache identity differs from stable source".into(),
            ));
        }
        write_new_synced(work.join("lease.lock").as_path(), b"")?;
        write_new_synced(
            work.join("manifest.json").as_path(),
            artifact.manifest_bytes(),
        )?;
        test_failpoint("manifest_sync")?;
        sync_tree(work_cache.as_path())?;
        sync_directory(work.as_path())?;

        let staged_destination = history_root.join(STAGING_DIR).join(artifact.snapshot_id());
        let published_destination = history_root
            .join(SNAPSHOTS_DIR)
            .join(artifact.snapshot_id());
        let mut idempotent = false;
        let (destination, namespace) = if staged_destination.exists() {
            let existing =
                BacktestHistorySnapshot::open_generation(history_root, &staged_destination)
                    .map_err(|error| CliError::Migration(error.to_string()))?;
            if existing.identity_sha256() != artifact.identity_sha256() {
                return Err(CliError::Migration(format!(
                    "snapshot identity collision at {}",
                    staged_destination.display()
                )));
            }
            idempotent = true;
            fs::remove_dir_all(&work)?;
            (staged_destination, "staging")
        } else if published_destination.exists() {
            let existing =
                BacktestHistorySnapshot::open_generation(history_root, &published_destination)
                    .map_err(|error| CliError::Migration(error.to_string()))?;
            if existing.identity_sha256() != artifact.identity_sha256() {
                return Err(CliError::Migration(format!(
                    "snapshot identity collision at {}",
                    published_destination.display()
                )));
            }
            idempotent = true;
            fs::remove_dir_all(&work)?;
            (published_destination, "snapshots")
        } else {
            fs::rename(&work, &staged_destination)?;
            sync_directory(history_root.join(STAGING_DIR).as_path())?;
            (staged_destination, "staging")
        };
        let verified = BacktestHistorySnapshot::open_generation(history_root, &destination)
            .map_err(|error| CliError::Migration(error.to_string()))?;
        Ok(json!({
            "command": command,
            "snapshot_id": verified.snapshot_id(),
            "identity_sha256": verified.identity_sha256(),
            "metadata_snapshot_hash": verified.metadata_snapshot_hash(),
            "namespace": namespace,
            "idempotent": idempotent,
            "copy": stats.as_value(),
        }))
    })();
    if result.is_err() && work.exists() {
        let _ = fs::remove_dir_all(&work);
    }
    result
}

fn manifest_builder(
    created_at: DateTime<Utc>,
    args: &CloneArgs,
) -> BacktestHistorySnapshotManifestBuilder {
    BacktestHistorySnapshotManifestBuilder::new(created_at)
        .catalog(args.catalog_complete, args.catalog_symbols.iter().cloned())
}

fn inspect_source(source: &Path, mode: CloneMode) -> Result<CloneStats, CliError> {
    let mut stats = CloneStats::default();
    inspect_source_directory(source, source, mode, &mut stats)?;
    Ok(stats)
}

fn inspect_source_directory(
    source_root: &Path,
    directory: &Path,
    mode: CloneMode,
    stats: &mut CloneStats,
) -> Result<(), CliError> {
    for entry in sorted_entries(directory)? {
        let path = entry.path();
        let metadata = fs::symlink_metadata(path.as_path())?;
        if metadata.file_type().is_symlink() {
            return Err(CliError::Migration(format!(
                "source cache entry {} is symlink",
                path.display()
            )));
        }
        if metadata.is_dir() {
            inspect_source_directory(source_root, path.as_path(), mode, stats)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(CliError::Migration(format!(
                "source cache entry {} is not regular file",
                path.display()
            )));
        }
        let relative = path
            .strip_prefix(source_root)
            .map_err(|_| CliError::Migration("source path escapes cache root".into()))?;
        if let BacktestHistorySnapshotFileDisposition::Include(role) =
            classify_backtest_history_snapshot_cache_path(relative)
                .map_err(|error| CliError::Migration(error.to_string()))?
        {
            let hardlinked = mode.allows_hardlink() && role.allows_hardlink();
            stats.record(role, metadata.len(), hardlinked);
        }
    }
    Ok(())
}

fn clone_directory(
    source_root: &Path,
    directory: &Path,
    destination_root: &Path,
    mode: CloneMode,
    stats: &mut CloneStats,
) -> Result<(), CliError> {
    for entry in sorted_entries(directory)? {
        let source = entry.path();
        let metadata = fs::symlink_metadata(source.as_path())?;
        if metadata.file_type().is_symlink() {
            return Err(CliError::Migration(format!(
                "source cache entry {} is symlink",
                source.display()
            )));
        }
        let relative = source
            .strip_prefix(source_root)
            .map_err(|_| CliError::Migration("source path escapes cache root".into()))?;
        let destination = destination_root.join(relative);
        if metadata.is_dir() {
            fs::create_dir(&destination)?;
            clone_directory(source_root, source.as_path(), destination_root, mode, stats)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(CliError::Migration(format!(
                "source cache entry {} is not regular file",
                source.display()
            )));
        }
        let disposition = classify_backtest_history_snapshot_cache_path(relative)
            .map_err(|error| CliError::Migration(error.to_string()))?;
        let BacktestHistorySnapshotFileDisposition::Include(role) = disposition else {
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            write_new_synced(destination.as_path(), b"")?;
            continue;
        };
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let hardlinked = mode.allows_hardlink()
            && role.allows_hardlink()
            && fs::hard_link(source.as_path(), destination.as_path()).is_ok();
        if !hardlinked {
            fs::copy(source.as_path(), destination.as_path())?;
        }
        File::open(destination.as_path())?.sync_all()?;
        stats.record(role, metadata.len(), hardlinked);
    }
    Ok(())
}

async fn prewarm(
    history_root: &Path,
    args: &VerificationArgs,
    command: &str,
) -> Result<Value, CliError> {
    let request_file = args
        .request_file
        .as_deref()
        .ok_or_else(|| CliError::Usage("snapshot prewarm requires --request-file".to_string()))?;
    let requests = read_verification_requests(Some(request_file))?;
    if requests.is_empty() {
        return Err(CliError::Usage(
            "snapshot prewarm request file must not be empty".to_string(),
        ));
    }
    prepare_history_root(history_root)?;
    let _publisher = acquire_publisher_lock(history_root)?;
    validate_snapshot_id(args.snapshot_id.as_str())?;
    let source = history_root.join(STAGING_DIR).join(&args.snapshot_id);
    let source_snapshot = BacktestHistorySnapshot::open_generation(history_root, &source)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    let builder = source_snapshot.manifest_builder();

    let work = history_root.join(STAGING_DIR).join(format!(
        ".prewarm-{}-{}",
        std::process::id(),
        nonce()?
    ));
    let work_cache = work.join("cache");
    fs::create_dir(&work)?;
    fs::create_dir(&work_cache)?;
    let result = async {
        let mut stats = CloneStats::default();
        clone_directory(
            source.join("cache").as_path(),
            source.join("cache").as_path(),
            work_cache.as_path(),
            CloneMode::Clone,
            &mut stats,
        )?;
        test_failpoint("prewarm_clone")?;
        let materialize_requests = requests
            .iter()
            .map(|request| request.request.clone())
            .collect::<Vec<_>>();
        let client = BacktestHistoryClient::builder(work_cache.as_path())
            .policy(BacktestHistoryPolicy::RemoteOnMiss)
            .build()?;
        client.materialize_cache(materialize_requests).await?;
        test_failpoint("prewarm")?;

        let artifact = builder
            .build(work_cache.as_path())
            .map_err(|error| CliError::Migration(error.to_string()))?;
        write_new_synced(work.join("lease.lock").as_path(), b"")?;
        write_new_synced(
            work.join("manifest.json").as_path(),
            artifact.manifest_bytes(),
        )?;
        sync_tree(work_cache.as_path())?;
        sync_directory(work.as_path())?;
        let destination = history_root.join(STAGING_DIR).join(artifact.snapshot_id());
        if destination.exists() {
            let existing = BacktestHistorySnapshot::open_generation(history_root, &destination)
                .map_err(|error| CliError::Migration(error.to_string()))?;
            if existing.identity_sha256() != artifact.identity_sha256() {
                return Err(CliError::Migration(format!(
                    "prewarm identity collision at {}",
                    destination.display()
                )));
            }
            fs::remove_dir_all(&work)?;
        } else {
            fs::rename(&work, &destination)?;
            sync_directory(history_root.join(STAGING_DIR).as_path())?;
        }
        let snapshot = BacktestHistorySnapshot::open_generation(history_root, &destination)
            .map_err(|error| CliError::Migration(error.to_string()))?;
        let families = verify_snapshot_requests(&snapshot, requests.as_slice()).await?;
        write_ready_marker(history_root, &snapshot, families.as_slice())?;
        Ok(json!({
            "command": command,
            "source_snapshot_id": args.snapshot_id,
            "snapshot_id": snapshot.snapshot_id(),
            "identity_sha256": snapshot.identity_sha256(),
            "requests": requests.len(),
            "query_smoke_verified": true,
            "copy": stats.as_value(),
        }))
    }
    .await;
    if result.is_err() && work.exists() {
        let _ = fs::remove_dir_all(&work);
    }
    result
}

async fn verify(
    history_root: &Path,
    args: &VerificationArgs,
    command: &str,
) -> Result<Value, CliError> {
    let _publisher = acquire_publisher_lock(history_root)?;
    validate_snapshot_id(args.snapshot_id.as_str())?;
    let staged = history_root.join(STAGING_DIR).join(&args.snapshot_id);
    let (generation, namespace) = if staged.exists() {
        (staged, GenerationNamespace::Staging)
    } else {
        (
            history_root.join(SNAPSHOTS_DIR).join(&args.snapshot_id),
            GenerationNamespace::Snapshots,
        )
    };
    let snapshot = BacktestHistorySnapshot::open_generation(history_root, &generation)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    let requests = read_verification_requests(args.request_file.as_deref())?;
    let families = verify_snapshot_requests(&snapshot, requests.as_slice()).await?;
    write_ready_marker(history_root, &snapshot, families.as_slice())?;
    Ok(json!({
        "command": command,
        "snapshot_id": snapshot.snapshot_id(),
        "identity_sha256": snapshot.identity_sha256(),
        "namespace": namespace.dir(),
        "requests": requests.len(),
        "families": families,
        "query_smoke_verified": true,
    }))
}

#[derive(Debug, Clone)]
struct PreparedVerificationRequest {
    family: BacktestHistorySnapshotFileRole,
    request: BacktestHistoryRequest,
}

fn read_verification_requests(
    path: Option<&Path>,
) -> Result<Vec<PreparedVerificationRequest>, CliError> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let wire: VerificationRequestFile = serde_json::from_slice(fs::read(path)?.as_slice())?;
    wire.requests
        .into_iter()
        .map(|request| match request {
            VerificationRequest::Tick {
                request_id,
                symbol,
                start_ns,
                end_ns,
            } => Ok(PreparedVerificationRequest {
                family: BacktestHistorySnapshotFileRole::TqbnMutableLayout,
                request: BacktestHistoryRequest::tick(request_id, symbol, start_ns, end_ns),
            }),
            VerificationRequest::Minute {
                request_id,
                symbol,
                duration_ns,
                start_ns,
                end_ns,
            } => Ok(PreparedVerificationRequest {
                family: BacktestHistorySnapshotFileRole::TqmkImmutableGeneration,
                request: BacktestHistoryRequest::kline(
                    request_id,
                    symbol,
                    Duration::from_nanos(duration_ns),
                    start_ns,
                    end_ns,
                ),
            }),
            VerificationRequest::Daily {
                request_id,
                symbol,
                start_ns,
                end_ns,
            } => Ok(PreparedVerificationRequest {
                family: BacktestHistorySnapshotFileRole::TqdkImmutableGeneration,
                request: BacktestHistoryRequest::kline(
                    request_id,
                    symbol,
                    Duration::from_secs(86_400),
                    start_ns,
                    end_ns,
                ),
            }),
        })
        .collect()
}

async fn verify_snapshot_requests(
    snapshot: &BacktestHistorySnapshot,
    requests: &[PreparedVerificationRequest],
) -> Result<Vec<&'static str>, CliError> {
    let required = snapshot
        .file_roles()
        .iter()
        .copied()
        .filter(is_data_role)
        .collect::<BTreeSet<_>>();
    let provided = requests
        .iter()
        .map(|request| request.family)
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&provided).copied().collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CliError::Migration(format!(
            "query smoke requests do not cover manifest data roles: {}",
            missing
                .iter()
                .map(|role| role.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    for request in requests {
        snapshot
            .inspect(request.request.clone())
            .await
            .map_err(|error| CliError::Migration(error.to_string()))?;
        snapshot
            .query(request.request.clone())
            .await
            .map_err(|error| CliError::Migration(error.to_string()))?
            .collect()
            .await
            .map_err(|error| CliError::Migration(error.to_string()))?;
    }
    Ok(provided.into_iter().map(|role| role.as_str()).collect())
}

fn is_data_role(role: &BacktestHistorySnapshotFileRole) -> bool {
    matches!(
        role,
        BacktestHistorySnapshotFileRole::TqbnMutableLayout
            | BacktestHistorySnapshotFileRole::TqmkImmutableGeneration
            | BacktestHistorySnapshotFileRole::TqdkImmutableGeneration
    )
}

fn write_ready_marker(
    history_root: &Path,
    snapshot: &BacktestHistorySnapshot,
    families: &[&str],
) -> Result<(), CliError> {
    let staging = history_root.join(STAGING_DIR);
    let target = ready_marker_path(history_root, snapshot.snapshot_id());
    let temporary = staging.join(format!(
        ".ready-{}.tmp-{}-{}",
        snapshot.snapshot_id(),
        std::process::id(),
        nonce()?
    ));
    let bytes = serde_json::to_vec(&json!({
        "snapshot_id": snapshot.snapshot_id(),
        "identity_sha256": snapshot.identity_sha256(),
        "families": families,
    }))?;
    write_new_synced(temporary.as_path(), bytes.as_slice())?;
    fs::rename(temporary, target)?;
    sync_directory(staging.as_path())?;
    Ok(())
}

fn require_ready_marker(
    history_root: &Path,
    snapshot: &BacktestHistorySnapshot,
) -> Result<(), CliError> {
    let required = snapshot
        .file_roles()
        .iter()
        .filter(|role| is_data_role(role))
        .map(|role| role.as_str())
        .collect::<BTreeSet<_>>();
    if required.is_empty() {
        return Ok(());
    }
    let path = ready_marker_path(history_root, snapshot.snapshot_id());
    let value: Value = serde_json::from_slice(fs::read(path.as_path())?.as_slice())?;
    if value["snapshot_id"] != snapshot.snapshot_id()
        || value["identity_sha256"] != snapshot.identity_sha256()
    {
        return Err(CliError::Migration(format!(
            "verification marker {} does not match staged snapshot identity",
            path.display()
        )));
    }
    let families = value["families"]
        .as_array()
        .ok_or_else(|| CliError::Migration("verification marker families must be an array".into()))?
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    if !required.is_subset(&families) {
        return Err(CliError::Migration(format!(
            "verification marker {} does not cover every manifest data role",
            path.display()
        )));
    }
    Ok(())
}

fn ready_marker_path(history_root: &Path, snapshot_id: &str) -> PathBuf {
    history_root
        .join(STAGING_DIR)
        .join(format!(".ready-{snapshot_id}.json"))
}

fn publish_generation(
    history_root: &Path,
    args: &PublishArgs,
    command: &str,
) -> Result<Value, CliError> {
    prepare_history_root(history_root)?;
    let _publisher = acquire_publisher_lock(history_root)?;
    validate_snapshot_id(args.snapshot_id.as_str())?;
    let staging = history_root.join(STAGING_DIR).join(&args.snapshot_id);
    let published = history_root.join(SNAPSHOTS_DIR).join(&args.snapshot_id);
    let source = if staging.exists() {
        staging.as_path()
    } else if published.exists() {
        published.as_path()
    } else {
        return Err(CliError::Migration(format!(
            "snapshot {} is neither staged nor published",
            args.snapshot_id
        )));
    };
    let snapshot = BacktestHistorySnapshot::open_generation(history_root, source)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    let already_current = if source == published.as_path()
        && read_current(history_root).is_ok_and(|snapshot_id| snapshot_id == args.snapshot_id)
    {
        let current = BacktestHistorySnapshot::open(history_root)
            .map_err(|error| CliError::Migration(error.to_string()))?;
        if current.snapshot_id() != snapshot.snapshot_id()
            || current.identity_sha256() != snapshot.identity_sha256()
        {
            return Err(CliError::Migration(
                "CURRENT changed while validating idempotent publish".into(),
            ));
        }
        true
    } else {
        false
    };
    if already_current {
        sync_directory(history_root.join(SNAPSHOTS_DIR).as_path())?;
        sync_directory(history_root.join(STAGING_DIR).as_path())?;
        test_failpoint("history_root_sync")?;
        sync_directory(history_root)?;
        return Ok(json!({
            "command": command,
            "snapshot_id": args.snapshot_id,
            "identity_sha256": snapshot.identity_sha256(),
            "committed": true,
            "idempotent": true,
            "maintenance_warning": cleanup_ready_marker(history_root, args.snapshot_id.as_str()),
        }));
    }
    require_ready_marker(history_root, &snapshot)?;
    sync_tree(source.join("cache").as_path())?;
    File::open(source.join("manifest.json"))?.sync_all()?;
    sync_directory(source)?;
    if source == staging.as_path() {
        if published.exists() {
            let existing = BacktestHistorySnapshot::open_generation(history_root, &published)
                .map_err(|error| CliError::Migration(error.to_string()))?;
            if existing.identity_sha256() != snapshot.identity_sha256() {
                return Err(CliError::Migration(format!(
                    "published snapshot {} differs from staged identity",
                    args.snapshot_id
                )));
            }
            fs::remove_dir_all(&staging)?;
        } else {
            fs::rename(&staging, &published)?;
        }
        sync_directory(history_root.join(SNAPSHOTS_DIR).as_path())?;
        test_failpoint("snapshot_rename")?;
        sync_directory(history_root.join(STAGING_DIR).as_path())?;
    }
    atomic_write_current(history_root, args.snapshot_id.as_str())?;
    let ready = ready_marker_path(history_root, args.snapshot_id.as_str());
    let mut cleanup_warning = None;
    if ready.exists() {
        if let Err(error) = fs::remove_file(ready)
            .and_then(|()| File::open(history_root.join(STAGING_DIR))?.sync_all())
        {
            cleanup_warning = Some(format!(
                "snapshot committed but verification marker cleanup failed: {error}"
            ));
        }
    }
    Ok(json!({
        "command": command,
        "snapshot_id": args.snapshot_id,
        "identity_sha256": snapshot.identity_sha256(),
        "committed": true,
        "idempotent": already_current,
        "maintenance_warning": cleanup_warning,
    }))
}

fn cleanup_ready_marker(history_root: &Path, snapshot_id: &str) -> Option<String> {
    let ready = ready_marker_path(history_root, snapshot_id);
    if ready.exists()
        && let Err(error) = fs::remove_file(ready)
            .and_then(|()| File::open(history_root.join(STAGING_DIR))?.sync_all())
    {
        return Some(format!(
            "snapshot committed but verification marker cleanup failed: {error}"
        ));
    }
    None
}

fn validate_generation(
    history_root: &Path,
    snapshot_id: &str,
    namespace: GenerationNamespace,
    command: &str,
) -> Result<Value, CliError> {
    validate_snapshot_id(snapshot_id)?;
    let generation = history_root.join(namespace.dir()).join(snapshot_id);
    let snapshot = BacktestHistorySnapshot::open_generation(history_root, generation)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    Ok(snapshot_value(command, &snapshot, namespace.dir()))
}

fn rollback(
    history_root: &Path,
    args: &MutationGenerationArgs,
    command: &str,
) -> Result<Value, CliError> {
    let _publisher = if args.apply {
        Some(acquire_publisher_lock(history_root)?)
    } else {
        None
    };
    validate_snapshot_id(args.snapshot_id.as_str())?;
    let target = history_root.join(SNAPSHOTS_DIR).join(&args.snapshot_id);
    let snapshot = BacktestHistorySnapshot::open_generation(history_root, &target)
        .map_err(|error| CliError::Migration(error.to_string()))?;
    let previous = read_current(history_root).ok();
    if args.apply {
        atomic_write_current(history_root, args.snapshot_id.as_str())?;
    }
    Ok(json!({
        "command": command,
        "snapshot_id": snapshot.snapshot_id(),
        "previous_snapshot_id": previous,
        "applied": args.apply,
        "read_only": !args.apply,
    }))
}

fn scrub(
    history_root: &Path,
    args: &OptionalGenerationArgs,
    command: &str,
) -> Result<Value, CliError> {
    let _publisher = acquire_publisher_lock(history_root)?;
    match args.snapshot_id.as_deref() {
        Some(snapshot_id) => validate_generation(
            history_root,
            snapshot_id,
            GenerationNamespace::Snapshots,
            command,
        ),
        None => inspect(history_root, command),
    }
}

fn recover(history_root: &Path, args: &RecoverArgs, command: &str) -> Result<Value, CliError> {
    let _publisher = if args.apply {
        Some(acquire_publisher_lock(history_root)?)
    } else {
        None
    };
    let current = BacktestHistorySnapshot::open(history_root);
    let (current_snapshot_id, needs_restore) = match current {
        Ok(snapshot) => (Some(snapshot.snapshot_id().to_string()), false),
        Err(_) => (None, true),
    };
    if needs_restore && args.snapshot_id.is_none() {
        return Err(CliError::Migration(
            "CURRENT is unavailable or invalid; --snapshot-id is required for explicit recovery"
                .into(),
        ));
    }
    let restore = args.snapshot_id.as_deref();
    if let Some(snapshot_id) = restore {
        validate_generation(
            history_root,
            snapshot_id,
            GenerationNamespace::Snapshots,
            command,
        )?;
    }
    let temp_files = current_temp_files(history_root)?;
    let staging_temp_dirs = staging_temp_dirs(history_root)?;
    if args.apply {
        for path in &temp_files {
            fs::remove_file(path)?;
        }
        for path in &staging_temp_dirs {
            remove_file_or_directory(path)?;
        }
        if !staging_temp_dirs.is_empty() {
            sync_directory(history_root.join(STAGING_DIR).as_path())?;
        }
        if needs_restore {
            atomic_write_current(history_root, restore.expect("checked recovery target"))?;
        }
        sync_directory(history_root)?;
    }
    Ok(json!({
        "command": command,
        "current_snapshot_id": current_snapshot_id,
        "restore_snapshot_id": if needs_restore { restore } else { None },
        "current_temp_files": temp_files,
        "staging_temp_dirs": staging_temp_dirs,
        "applied": args.apply,
        "read_only": !args.apply,
    }))
}

fn gc(history_root: &Path, args: &GcArgs, command: &str) -> Result<Value, CliError> {
    if args.retain == 0 {
        return Err(CliError::Usage("--retain must be at least 1".into()));
    }
    let _publisher = if args.apply {
        Some(acquire_publisher_lock(history_root)?)
    } else {
        None
    };
    let current = read_current(history_root)?;
    let snapshots_dir = history_root.join(SNAPSHOTS_DIR);
    let mut compatible = Vec::new();
    for entry in sorted_entries(snapshots_dir.as_path())? {
        let path = entry.path();
        if !fs::symlink_metadata(&path)?.is_dir() {
            return Err(CliError::Migration(format!(
                "snapshot namespace entry {} is not a directory",
                path.display()
            )));
        }
        let id = entry
            .file_name()
            .into_string()
            .map_err(|_| CliError::Migration("snapshot id must be UTF-8".into()))?;
        validate_snapshot_id(id.as_str())?;
        let snapshot = BacktestHistorySnapshot::open_generation(history_root, &path)
            .map_err(|error| CliError::Migration(error.to_string()))?;
        compatible.push((id, snapshot.created_at().to_string()));
        drop(snapshot);
    }
    compatible.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let mut retained = BTreeSet::from([current.clone()]);
    for (snapshot_id, _) in &compatible {
        if retained.len() >= args.retain {
            break;
        }
        retained.insert(snapshot_id.clone());
    }
    let mut removed = Vec::new();
    let mut leased = Vec::new();
    let mut eligible = Vec::new();
    for (snapshot_id, _) in compatible.into_iter().rev() {
        if retained.contains(&snapshot_id) {
            continue;
        }
        eligible.push(snapshot_id.clone());
        if !args.apply {
            continue;
        }
        let generation = snapshots_dir.join(&snapshot_id);
        let lease_path = generation.join("lease.lock");
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lease_path)?;
        match FileExt::try_lock_exclusive(&lease) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                leased.push(snapshot_id);
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        if read_current(history_root)? == snapshot_id {
            continue;
        }
        let tombstone = history_root.join(STAGING_DIR).join(format!(
            ".gc-{}-{}-{}",
            snapshot_id,
            std::process::id(),
            nonce()?
        ));
        fs::rename(&generation, &tombstone)?;
        sync_directory(snapshots_dir.as_path())?;
        sync_directory(history_root.join(STAGING_DIR).as_path())?;
        test_failpoint("gc_rename")?;
        fs::remove_dir_all(&tombstone)?;
        test_failpoint("gc_delete")?;
        removed.push(snapshot_id);
        drop(lease);
    }
    if args.apply && !removed.is_empty() {
        sync_directory(history_root.join(STAGING_DIR).as_path())?;
    }
    removed.sort();
    leased.sort();
    eligible.sort();
    Ok(json!({
        "command": command,
        "current_snapshot_id": current,
        "retain": args.retain,
        "eligible": eligible,
        "removed": removed,
        "leased": leased,
        "applied": args.apply,
        "read_only": !args.apply,
    }))
}

fn snapshot_value(command: &str, snapshot: &BacktestHistorySnapshot, namespace: &str) -> Value {
    json!({
        "command": command,
        "snapshot_id": snapshot.snapshot_id(),
        "identity_sha256": snapshot.identity_sha256(),
        "metadata_snapshot_hash": snapshot.metadata_snapshot_hash(),
        "catalog_complete": snapshot.catalog_complete(),
        "namespace": namespace,
        "valid": true,
    })
}

fn prepare_history_root(history_root: &Path) -> Result<(), CliError> {
    reject_symlink_ancestors_that_exist(history_root)?;
    fs::create_dir_all(history_root)?;
    let metadata = fs::symlink_metadata(history_root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CliError::Migration(format!(
            "history root {} is not a regular directory",
            history_root.display()
        )));
    }
    fs::create_dir_all(history_root.join(SNAPSHOTS_DIR))?;
    fs::create_dir_all(history_root.join(STAGING_DIR))?;
    require_regular_directory(history_root.join(SNAPSHOTS_DIR).as_path(), "snapshots")?;
    require_regular_directory(history_root.join(STAGING_DIR).as_path(), "staging")?;
    Ok(())
}

fn require_regular_directory(path: &Path, label: &str) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CliError::Migration(format!(
            "{label} directory {} is not a regular directory",
            path.display()
        )));
    }
    Ok(())
}

fn require_source_root(source: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        CliError::Migration(format!(
            "source cache root {} unavailable: {error}",
            source.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CliError::Migration(format!(
            "source cache root {} is not a regular directory",
            source.display()
        )));
    }
    Ok(())
}

fn acquire_publisher_lock(history_root: &Path) -> Result<PublisherLock, CliError> {
    let path = history_root.join(PUBLISHER_LOCK);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    FileExt::try_lock_exclusive(&file).map_err(|error| {
        if error.kind() == ErrorKind::WouldBlock {
            CliError::Migration(format!(
                "snapshot publisher operation lock {} is busy",
                path.display()
            ))
        } else {
            error.into()
        }
    })?;
    Ok(PublisherLock { _file: file })
}

fn atomic_write_current(history_root: &Path, snapshot_id: &str) -> Result<(), CliError> {
    validate_snapshot_id(snapshot_id)?;
    let temporary = history_root.join(format!(".CURRENT.tmp-{}-{}", std::process::id(), nonce()?));
    write_new_synced(temporary.as_path(), format!("{snapshot_id}\n").as_bytes())?;
    test_failpoint("current_temp_sync")?;
    fs::rename(temporary.as_path(), history_root.join(CURRENT_FILE))?;
    test_failpoint("current_rename")?;
    sync_directory(history_root)?;
    test_failpoint("history_root_sync")?;
    Ok(())
}

fn read_current(history_root: &Path) -> Result<String, CliError> {
    let value = fs::read_to_string(history_root.join(CURRENT_FILE))?;
    let snapshot_id = value.strip_suffix('\n').ok_or_else(|| {
        CliError::Migration("CURRENT must contain one snapshot id and trailing newline".into())
    })?;
    if snapshot_id.contains('\n') {
        return Err(CliError::Migration(
            "CURRENT must contain exactly one snapshot id".into(),
        ));
    }
    validate_snapshot_id(snapshot_id)?;
    Ok(snapshot_id.to_string())
}

fn validate_snapshot_id(snapshot_id: &str) -> Result<(), CliError> {
    let valid = snapshot_id
        .strip_prefix("s-")
        .and_then(|value| value.split_once('-'))
        .is_some_and(|(date, hash)| {
            date.len() == 8
                && date.bytes().all(|byte| byte.is_ascii_digit())
                && hash.len() == 8
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        });
    if !valid {
        return Err(CliError::Usage(format!(
            "invalid snapshot id {snapshot_id:?}"
        )));
    }
    Ok(())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_tree(directory: &Path) -> Result<(), CliError> {
    for entry in sorted_entries(directory)? {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            sync_tree(path.as_path())?;
        } else if metadata.is_file() {
            File::open(&path)?.sync_all()?;
        } else {
            return Err(CliError::Migration(format!(
                "cannot sync non-regular snapshot entry {}",
                path.display()
            )));
        }
    }
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> Result<(), CliError> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn sorted_entries(directory: &Path) -> Result<Vec<fs::DirEntry>, CliError> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn current_temp_files(history_root: &Path) -> Result<Vec<PathBuf>, CliError> {
    if !history_root.exists() {
        return Ok(Vec::new());
    }
    let mut paths = sorted_entries(history_root)?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.starts_with(".CURRENT.tmp-"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn staging_temp_dirs(history_root: &Path) -> Result<Vec<PathBuf>, CliError> {
    let staging = history_root.join(STAGING_DIR);
    let snapshots = history_root.join(SNAPSHOTS_DIR);
    if !staging.exists() {
        return Ok(Vec::new());
    }
    let mut paths = sorted_entries(staging.as_path())?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| {
                    name.starts_with(".clone-")
                        || name.starts_with(".prewarm-")
                        || name.starts_with(".gc-")
                        || name.contains(".tmp-")
                        || name
                            .strip_prefix(".ready-")
                            .and_then(|value| value.strip_suffix(".json"))
                            .is_some_and(|snapshot_id| {
                                !staging.join(snapshot_id).exists()
                                    && !snapshots.join(snapshot_id).exists()
                            })
                })
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn remove_file_or_directory(path: &Path) -> Result<(), CliError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn test_failpoint(name: &str) -> Result<(), CliError> {
    #[cfg(debug_assertions)]
    if std::env::var("TQSDK_CACHE_TEST_SNAPSHOT_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(70);
    }
    let _ = name;
    Ok(())
}

fn reject_symlink_ancestors_that_exist(path: &Path) -> Result<(), CliError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(CliError::Migration(format!(
                    "path ancestor {} is symlink",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn nonce() -> Result<u128, CliError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|_| CliError::Migration("system clock is before UNIX epoch".into()))
}
