use chrono::{TimeZone, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use tqsdk_data::{
    BacktestTickCache, DailyKlineCache, MinuteKlineCache, MinuteKlineCacheSnapshot,
    migrate_kline_cache,
};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "kline-migration-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn raw_payload(path: &Path) -> Vec<u8> {
    let bytes = fs::read(path).unwrap();
    if &bytes[..8] != b"TQKLOG01" {
        return bytes;
    }
    let slot = &bytes[56..104]; // A fresh file publishes generation 1 in slot 1.
    let offset = u64::from_le_bytes(slot[8..16].try_into().unwrap()) as usize;
    let len = u64::from_le_bytes(slot[16..24].try_into().unwrap()) as usize;
    let index: serde_json::Value = serde_json::from_slice(&bytes[offset..offset + len]).unwrap();
    let segment = &index["segments"][0];
    let start = segment["offset"].as_u64().unwrap() as usize;
    let size = segment["len"].as_u64().unwrap() as usize;
    bytes[start..start + size].to_vec()
}

#[test]
fn migration_is_lossless_resumable_and_exclusively_gated() {
    let temp = Root::new();
    let root = temp.0.join("cache");
    let backup = temp.0.join("backup");
    let gate = BacktestTickCache::open(&root).unwrap();
    let root_lock = gate.try_acquire_consistency_read_lock().unwrap();
    assert!(migrate_kline_cache(&root, &backup, true).is_err());
    assert!(!backup.exists());
    drop(root_lock);
    let day = Utc
        .with_ymd_and_hms(2024, 1, 2, 2, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let daily = DailyKlineCache::open(&root).unwrap();
    daily
        .store_final_range("SHFE.au2406", day, day + 86_400_000_000_000, &snapshot, &[])
        .unwrap();
    let minute = MinuteKlineCache::open(&root).unwrap();
    minute
        .store_final_range("SHFE.au2406", day, day + 60_000_000_000, &snapshot, &[])
        .unwrap();
    let paths = [
        daily.symbol_file_path("SHFE.au2406"),
        minute.month_file_path("SHFE.au2406", "202401"),
    ];
    let originals: Vec<_> = paths
        .iter()
        .map(|path| {
            let bytes = raw_payload(path);
            fs::write(path, &bytes).unwrap();
            bytes
        })
        .collect();
    let dry = migrate_kline_cache(&root, &backup, false).unwrap();
    assert_eq!(dry.legacy_files, 2);
    assert!(!backup.exists());
    assert!(
        daily
            .inspect("SHFE.au2406", day, day + 86_400_000_000_000, &snapshot)
            .is_err()
    );
    assert!(
        minute
            .inspect("SHFE.au2406", day, day + 60_000_000_000, &snapshot)
            .is_err()
    );
    assert!(
        daily
            .store_final_range("SHFE.au2406", day, day + 86_400_000_000_000, &snapshot, &[])
            .is_err()
    );
    assert!(
        minute
            .store_final_range("SHFE.au2406", day, day + 60_000_000_000, &snapshot, &[])
            .is_err()
    );
    for (path, bytes) in paths.iter().zip(&originals) {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    let report = migrate_kline_cache(&root, &backup, true).unwrap();
    assert_eq!(report.migrated_files, 2);
    for (path, bytes) in paths.iter().zip(&originals) {
        assert_eq!(&raw_payload(path), bytes);
        assert_eq!(
            &fs::read(backup.join(path.strip_prefix(&root).unwrap())).unwrap(),
            bytes
        );
    }
    let repeat = migrate_kline_cache(&root, &backup, true).unwrap();
    assert_eq!(repeat.legacy_files, 0);
    assert_eq!(repeat.verified_files, 2);
    daily
        .store_final_range(
            "SHFE.au2406",
            day + 86_400_000_000_000,
            day + 2 * 86_400_000_000_000,
            &snapshot,
            &[],
        )
        .unwrap();
    minute
        .store_final_range(
            "SHFE.au2406",
            day + 60_000_000_000,
            day + 2 * 60_000_000_000,
            &snapshot,
            &[],
        )
        .unwrap();
    let after_append = migrate_kline_cache(&root, &backup, true).unwrap();
    assert_eq!(after_append.verified_files, 2);
    assert_eq!(after_append.migrated_files, 0);
    assert_eq!(daily.diagnose_all().unwrap().problem_files, 0);
    assert_eq!(minute.diagnose().unwrap().problem_files, 0);
}

#[test]
fn legacy_migration_rejects_shared_and_wrong_root_tokens() {
    let temp = Root::new();
    let root = temp.0.join("cache");
    let minute = MinuteKlineCache::open(&root).unwrap();
    let gate = BacktestTickCache::open(&root).unwrap();
    let shared = gate.try_acquire_remote_fill_shared_lock().unwrap();
    assert!(minute.migrate_legacy_v4_with_lock(&shared).is_err());
    assert!(minute.migrate_legacy_v4().is_err());
    drop(shared);
    let other = BacktestTickCache::open(temp.0.join("other")).unwrap();
    let wrong = other.try_acquire_consistency_read_lock().unwrap();
    assert!(minute.migrate_legacy_v4_with_lock(&wrong).is_err());
    let correct = gate.try_acquire_consistency_read_lock().unwrap();
    assert_eq!(
        minute
            .migrate_legacy_v4_with_lock(&correct)
            .unwrap()
            .rewritten_files,
        0
    );
}
