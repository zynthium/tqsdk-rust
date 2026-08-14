//! Scenario: repair missing legacy and per-file locks for an existing Tick TQBN cache.
//!
//! - `DryRun` inspects every unique Tick partition's legacy `.tqbn.lock` and every existing
//!   `<file>.tqbn.lock` without creating a companion lock.
//! - `Apply` creates only missing lock files; it does not rewrite TQBN bytes, rows, coverage, or indexes.
//! - The cache owner must stop other readers and writers, then hold the exclusive root gate.
//! - Any legacy-partition or per-file failure remains in the report, so callers must not treat a
//!   partial repair as success.

use tqsdk_data::{
    BacktestTickCache, BacktestTickCacheLegacyPartitionLockRepair, BacktestTickCacheLockRepairMode,
    Result,
};

fn main() -> Result<()> {
    let cache_dir =
        std::env::var_os("TQ_HISTORY_CACHE_DIR").unwrap_or_else(|| ".tqsdk/data_series_1".into());
    let cache = BacktestTickCache::open(cache_dir)?;
    let _exclusive_gate = cache.try_acquire_consistency_read_lock()?;

    let preview = cache.repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)?;
    let legacy_partitions: &[BacktestTickCacheLegacyPartitionLockRepair] =
        &preview.legacy_partition_locks;
    println!(
        "legacy_partitions={} legacy_missing={} files={} missing={} invalid={}",
        legacy_partitions.len(),
        preview.legacy_partition_locks_missing,
        preview.files.len(),
        preview.missing_files,
        preview.legacy_partition_locks_failed + preview.failed_files
    );

    if std::env::var_os("TQ_CACHE_REPAIR_LOCKS_APPLY").is_some() {
        let repaired = cache.repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)?;
        println!(
            "legacy_created={} files_created={} legacy_failed={} files_failed={}",
            repaired.legacy_partition_locks_created,
            repaired.created_files,
            repaired.legacy_partition_locks_failed,
            repaired.failed_files
        );
    }

    Ok(())
}
