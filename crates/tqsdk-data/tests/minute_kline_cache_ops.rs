use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{MinuteKlineCache, MinuteKlineCacheDiagnosticStatus, MinuteKlineCacheSnapshot};

const MINUTE_NS: i64 = 60_000_000_000;

#[test]
fn fast_inventory_is_read_only_for_a_missing_root() {
    let root = temp_dir("fast-inventory-read-only");
    std::fs::remove_dir_all(&root).unwrap();
    let cache = MinuteKlineCache::open_read_only(&root);

    let report = cache.fast_inventory().unwrap();

    assert_eq!(report.total_files, 0);
    assert_eq!(report.total_bytes, 0);
    assert!(report.symbols.is_empty());
    assert!(!root.exists());
}

#[test]
fn diagnose_distinguishes_readable_v4_and_legacy_v3_month_files() {
    let root = temp_dir("diagnose-versions");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 15, 2, 0);
    cache
        .store_final_range(
            "SHFE.rb2601",
            start,
            start + MINUTE_NS,
            &snapshot,
            &[kline(1, start, 10.0)],
        )
        .unwrap();

    let legacy_path = cache.month_file_path("SHFE.au2608", "202601");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    std::fs::write(&legacy_path, legacy_v3_header()).unwrap();

    let report = cache.diagnose().unwrap();

    assert_eq!(report.files.len(), 2);
    assert_eq!(report.problem_files, 1);
    assert!(report.files.iter().any(|file| {
        file.symbol == "SHFE.rb2601"
            && file.status == MinuteKlineCacheDiagnosticStatus::Readable
            && file.schema_version == Some(4)
            && file.rows == 1
    }));
    assert!(report.files.iter().any(|file| {
        file.symbol == "SHFE.au2608"
            && file.status == MinuteKlineCacheDiagnosticStatus::LegacyUnsupported
            && file.schema_version == Some(3)
            && file.rows == 0
    }));
}

fn legacy_v3_header() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"TQMK");
    bytes.extend_from_slice(&3_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    bytes
}

fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn kline(id: i64, datetime: i64, close: f64) -> Kline {
    Kline {
        id,
        datetime,
        open: close,
        high: close,
        low: close,
        close,
        volume: id,
        open_oi: id,
        close_oi: id,
        ..Kline::default()
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("tqsdk-minute-kline-{name}-{nanos}"));
    std::fs::create_dir_all(&root).unwrap();
    root
}
