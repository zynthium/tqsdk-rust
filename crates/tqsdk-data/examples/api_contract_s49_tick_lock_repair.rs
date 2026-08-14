//! Scenario: repair missing companion locks for an existing Tick TQBN cache.
//!
//! - `DryRun` inspects every existing Tick `.tqbn` without creating a companion lock.
//! - `Apply` creates only missing `<file>.tqbn.lock` files; it does not rewrite TQBN rows or coverage.
//! - The cache owner must stop other readers and writers, then hold the exclusive root gate.
//! - Any per-file failure remains in the report, so callers must not treat a partial repair as success.

use tqsdk_data::{BacktestTickCache, BacktestTickCacheLockRepairMode, Result};

fn main() -> Result<()> {
    let cache_dir =
        std::env::var_os("TQ_HISTORY_CACHE_DIR").unwrap_or_else(|| ".tqsdk/data_series_1".into());
    let cache = BacktestTickCache::open(cache_dir)?;
    let _exclusive_gate = cache.try_acquire_consistency_read_lock()?;

    let preview = cache.repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)?;
    println!(
        "scanned={} missing={} invalid={}",
        preview.files.len(),
        preview.missing_files,
        preview.failed_files
    );

    if std::env::var_os("TQ_CACHE_REPAIR_LOCKS_APPLY").is_some() {
        let repaired = cache.repair_tick_locks(BacktestTickCacheLockRepairMode::Apply)?;
        println!(
            "created={} already_present={} failed={}",
            repaired.created_files, repaired.already_present_files, repaired.failed_files
        );
    }

    Ok(())
}
