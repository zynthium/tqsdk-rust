//! Explicit offline migration; never called by query or fill.
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use fs2::FileExt;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{BacktestTickCache, DailyKlineCache, DataError, MinuteKlineCache, Result};

/// Per-file migration is resumable; a successful report covers both namespaces.
#[derive(Debug, Default, Serialize)]
pub struct KlineCacheMigrationReport {
    pub verified_files: usize,
    pub legacy_files: usize,
    pub migrated_files: usize,
    pub source_bytes: u64,
}

impl KlineCacheMigrationReport {
    pub(crate) fn record(&mut self, path: &Path, bytes: usize, legacy: bool, apply: bool) {
        self.verified_files += 1;
        self.legacy_files += usize::from(legacy);
        self.migrated_files += usize::from(legacy && apply);
        self.source_bytes += bytes as u64;
        if self.verified_files.is_multiple_of(1000) {
            eprintln!(
                "verified={} migrated={} last={}",
                self.verified_files,
                self.migrated_files,
                path.display()
            );
        }
    }
}

/// Validate or migrate raw TQDK v1 / TQMK v5 and KLOG files to the common container.
///
/// Takes the exclusive root gate. `backup_dir` must be outside the cache root;
/// existing backups must match the source byte-for-byte. No network is used.
/// Failure leaves completed files valid and the operation may be rerun.
pub fn migrate_kline_cache(
    root: impl AsRef<Path>,
    backup_dir: impl AsRef<Path>,
    apply: bool,
) -> Result<KlineCacheMigrationReport> {
    let root = root.as_ref().canonicalize()?;
    let gate = BacktestTickCache::open_read_only(&root);
    let _lock =
        gate.try_acquire_existing_consistency_read_lock()?
            .ok_or(DataError::InvalidState(
                "migration requires an existing cache-root operation lock",
            ))?;
    if root.join("CURRENT").exists()
        || root.ancestors().any(|directory| {
            directory.join("manifest.json").is_file() && directory.join("lease.lock").exists()
        })
    {
        return Err(DataError::Validation(
            "published snapshots are immutable; migrate a private writable clone and republish"
                .into(),
        ));
    }
    for namespace in ["daily-kline-v1", "minute-kline-v3"] {
        reject_symlinks(&root.join(namespace))?;
    }
    let backup = backup_dir.as_ref();
    let backup = if backup.exists() {
        backup.canonicalize()?
    } else {
        let parent = backup
            .parent()
            .ok_or(DataError::InvalidState("missing backup parent"))?
            .canonicalize()?;
        parent.join(
            backup
                .file_name()
                .ok_or(DataError::InvalidState("missing backup name"))?,
        )
    };
    if backup.starts_with(&root) || root.starts_with(&backup) {
        return Err(DataError::Validation(
            "migration backup must be outside the cache root".into(),
        ));
    }
    if apply {
        fs::create_dir_all(&backup)?;
    }
    let mut report = KlineCacheMigrationReport::default();
    let daily = if apply {
        DailyKlineCache::open(&root)?
    } else {
        DailyKlineCache::open_read_only(&root)
    };
    let minute = if apply {
        MinuteKlineCache::open(&root)?
    } else {
        MinuteKlineCache::open_read_only(&root)
    };
    daily.migrate_append_files(&backup, apply, &mut report)?;
    minute.migrate_append_files(&backup, apply, &mut report)?;
    if apply {
        let path = backup.join(format!(
            "migration-report-{}.json",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(
            &serde_json::to_vec_pretty(&report)
                .map_err(|error| DataError::InvalidResponse(error.to_string()))?,
        )?;
        file.sync_all()?;
        File::open(&backup)?.sync_all()?;
    }
    Ok(report)
}

fn reject_symlinks(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
        return Err(DataError::Validation(format!(
            "migration rejects non-regular namespace entries: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            reject_symlinks(&entry?.path())?;
        }
    }
    Ok(())
}

pub(crate) fn backup_partition(root: &Path, path: &Path, backup: &Path) -> Result<()> {
    if !fs::symlink_metadata(path)?.is_file() || path.canonicalize()? != path {
        return Err(DataError::Validation(
            "migration rejects symlink/non-regular partitions".into(),
        ));
    }
    let relative = path
        .strip_prefix(root)
        .map_err(|_| DataError::InvalidState("migration path outside root"))?;
    let target = backup.join(relative);
    let parent = target
        .parent()
        .ok_or(DataError::InvalidState("missing backup parent"))?;
    fs::create_dir_all(parent)?;
    if parent.canonicalize()? != parent {
        return Err(DataError::Validation(
            "migration rejects symlink backup directories".into(),
        ));
    }
    if target.exists() {
        if !fs::symlink_metadata(&target)?.is_file() {
            return Err(DataError::Validation(
                "migration rejects non-regular backups".into(),
            ));
        }
        if digest(path)? != digest(&target)? {
            return Err(DataError::Validation(format!(
                "backup differs from source: {}",
                target.display()
            )));
        }
    } else {
        File::open(path)?.sync_all()?;
        match fs::hard_link(path, &target) {
            Ok(()) => {}
            Err(_) => {
                let temporary = target.with_extension(format!(
                    "backup-{}-{}.tmp",
                    std::process::id(),
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
                ));
                let mut output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temporary)?;
                std::io::copy(&mut File::open(path)?, &mut output)?;
                output.sync_all()?;
                fs::rename(&temporary, &target)?;
            }
        }
    }
    for directory in parent
        .ancestors()
        .take_while(|directory| directory.starts_with(backup))
    {
        File::open(directory)?.sync_all()?;
    }
    if let Some(parent) = backup.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub(crate) fn partition_lock(path: &Path, apply: bool) -> Result<Option<File>> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .ok_or(DataError::InvalidState("missing partition extension"))?;
    let lock = path.with_extension(format!("{extension}.lock"));
    let file = match OpenOptions::new()
        .read(true)
        .write(apply)
        .create(apply)
        .truncate(false)
        .open(lock)
    {
        Ok(file) => file,
        Err(error) if !apply && error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    FileExt::try_lock_exclusive(&file)?;
    Ok(Some(file))
}

fn digest(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = file.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    Ok(hash.finalize().to_vec())
}
