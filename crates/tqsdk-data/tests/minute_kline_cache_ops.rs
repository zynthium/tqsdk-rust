use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{MinuteKlineCache, MinuteKlineCacheDiagnosticStatus, MinuteKlineCacheSnapshot};

const MINUTE_NS: i64 = 60_000_000_000;
type KlineBits = (i64, i64, u64, u64, u64, u64, i64, i64, i64, Option<i64>);

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
fn diagnose_distinguishes_readable_v5_and_legacy_v3_month_files() {
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
            && file.schema_version == Some(5)
            && file.rows == 1
    }));
    assert!(report.files.iter().any(|file| {
        file.symbol == "SHFE.au2608"
            && file.status == MinuteKlineCacheDiagnosticStatus::LegacyUnsupported
            && file.schema_version == Some(3)
            && file.rows == 0
    }));
}

#[test]
fn explicit_v4_migration_rewrites_a_month_without_changing_kline_rows() {
    let root = temp_dir("migrate-v4");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 15, 2, 0);
    let end = start + 2 * MINUTE_NS;
    let expected = vec![kline(1, start, 10.0), kline(2, start + MINUTE_NS, 11.0)];
    cache
        .store_final_range("SHFE.rb2601", start, end, &snapshot, &expected)
        .unwrap();

    let path = cache.month_file_path("SHFE.rb2601", "202601");
    rewrite_current_month_as_v4(path.as_path());
    let before = cache.diagnose().unwrap();
    assert_eq!(before.files[0].schema_version, Some(4));
    assert_eq!(
        before.files[0].status,
        MinuteKlineCacheDiagnosticStatus::LegacyUnsupported
    );

    let migration = cache.migrate_legacy_v4().unwrap();
    assert_eq!(migration.source_files, 1);
    assert_eq!(migration.rewritten_files, 1);

    let after = cache.diagnose().unwrap();
    assert_eq!(after.problem_files, 0);
    assert_eq!(after.files[0].schema_version, Some(5));
    let actual = cache
        .read_range("SHFE.rb2601", start, end, &snapshot)
        .unwrap();
    assert_eq!(kline_bits(&actual), kline_bits(&expected));
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

fn rewrite_current_month_as_v4(path: &std::path::Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let metadata_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let coverage_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
    let row_count = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;
    let rows_offset = 36 + metadata_len + 16 * coverage_count;
    if u16::from_le_bytes(bytes[6..8].try_into().unwrap()) == 1 {
        let decoded = zstd::bulk::decompress(&bytes[rows_offset..], row_count * 80).unwrap();
        bytes.truncate(rows_offset);
        bytes.extend_from_slice(&decoded);
    }
    bytes[4..6].copy_from_slice(&4_u16.to_le_bytes());
    bytes[6..8].copy_from_slice(&0_u16.to_le_bytes());
    std::fs::write(path, bytes).unwrap();
}

fn kline_bits(rows: &[Kline]) -> Vec<KlineBits> {
    rows.iter()
        .map(|row| {
            (
                row.id,
                row.datetime,
                row.open.to_bits(),
                row.high.to_bits(),
                row.low.to_bits(),
                row.close.to_bits(),
                row.volume,
                row.open_oi,
                row.close_oi,
                row.epoch,
            )
        })
        .collect()
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
