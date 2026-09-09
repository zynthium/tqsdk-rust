use chrono::{TimeZone, Utc};
#[path = "support/legacy_minute.rs"]
mod legacy_minute;
use std::fs;
use std::path::PathBuf;
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

// An independent old-format fixture: new daily files no longer contain a
// nested raw envelope that can be extracted from their first block.
fn raw_empty_daily(
    symbol: &str,
    snapshot: &MinuteKlineCacheSnapshot,
    start: i64,
    end: i64,
) -> Vec<u8> {
    let string = |output: &mut Vec<u8>, value: &str| {
        output.extend_from_slice(&(value.len() as u32).to_le_bytes());
        output.extend_from_slice(value.as_bytes());
    };
    let mut payload = Vec::new();
    string(&mut payload, symbol);
    payload.extend_from_slice(&snapshot.version.to_le_bytes());
    string(&mut payload, &snapshot.calendar_hash);
    string(&mut payload, &snapshot.session_hash);
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&start.to_le_bytes());
    payload.extend_from_slice(&end.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    let checksum = payload.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    let mut bytes = Vec::from(*b"TQDK");
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes
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
        .enumerate()
        .map(|(index, path)| {
            let bytes = if index == 0 {
                raw_empty_daily("SHFE.au2406", &snapshot, day, day + 86_400_000_000_000)
            } else {
                legacy_minute::raw_month(
                    5,
                    "SHFE.au2406",
                    "202401",
                    &snapshot,
                    (day, day + 60_000_000_000),
                    &[],
                )
            };
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
    for (index, (path, bytes)) in paths.iter().zip(&originals).enumerate() {
        if index == 1 {
            assert_eq!(&fs::read(path).unwrap()[..8], b"TQHIST01");
        } else {
            assert_eq!(&fs::read(path).unwrap()[..8], b"TQHIST01");
            assert!(
                daily
                    .inspect("SHFE.au2406", day, day + 86_400_000_000_000, &snapshot)
                    .unwrap()
                    .is_complete()
            );
            assert!(
                daily
                    .read_range("SHFE.au2406", day, day + 86_400_000_000_000, &snapshot)
                    .unwrap()
                    .is_empty()
            );
        }
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
