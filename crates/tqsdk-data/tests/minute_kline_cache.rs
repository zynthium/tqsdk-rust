use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{MinuteKlineCache, MinuteKlineCacheSnapshot};

const MINUTE_NS: i64 = 60_000_000_000;

#[test]
fn final_60s_rows_are_partitioned_by_trading_month_and_streamed() {
    let root = temp_dir("partitioned");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 30, 2, 0);
    let end = utc_ns(2026, 2, 2, 2, 2);
    let rows = vec![kline(1, start, 10.0), kline(2, end - MINUTE_NS, 11.0)];

    let report = cache
        .store_final_range("SHFE.rb2601", start, end, &snapshot, &rows)
        .unwrap();

    assert_eq!(report.months.len(), 2);
    assert!(cache.month_file_path("SHFE.rb2601", "202601").exists());
    assert!(cache.month_file_path("SHFE.rb2601", "202602").exists());
    assert!(
        cache
            .coverage("SHFE.rb2601", start, end, &snapshot)
            .unwrap()
            .is_complete()
    );

    let mut reader = cache
        .open_reader("SHFE.rb2601", start, end, &snapshot)
        .unwrap();
    assert_eq!(reader.next_kline().unwrap().unwrap().close, 10.0);
    assert_eq!(reader.next_kline().unwrap().unwrap().close, 11.0);
    assert!(reader.next_kline().unwrap().is_none());
}

#[test]
fn snapshot_mismatch_and_corrupt_month_file_fail_closed() {
    let root = temp_dir("fail-closed");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 15, 2, 0);
    let end = start + MINUTE_NS;
    cache
        .store_final_range(
            "SHFE.rb2601",
            start,
            end,
            &snapshot,
            &[kline(1, start, 10.0)],
        )
        .unwrap();

    let changed_snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v2", "session-v1").unwrap();
    assert!(
        cache
            .coverage("SHFE.rb2601", start, end, &changed_snapshot)
            .is_err()
    );

    std::fs::write(cache.month_file_path("SHFE.rb2601", "202601"), b"broken").unwrap();
    assert!(
        cache
            .coverage("SHFE.rb2601", start, end, &snapshot)
            .is_err()
    );
}

#[test]
fn purge_range_only_removes_intersecting_months() {
    let root = temp_dir("purge");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let january = utc_ns(2026, 1, 15, 2, 0);
    let february = utc_ns(2026, 2, 2, 2, 0);
    cache
        .store_final_range(
            "SHFE.rb2601",
            january,
            january + MINUTE_NS,
            &snapshot,
            &[kline(1, january, 10.0)],
        )
        .unwrap();
    cache
        .store_final_range(
            "SHFE.rb2601",
            february,
            february + MINUTE_NS,
            &snapshot,
            &[kline(2, february, 11.0)],
        )
        .unwrap();

    let report = cache
        .purge_range("SHFE.rb2601", january, january + MINUTE_NS)
        .unwrap();

    assert_eq!(report.removed_files, 1);
    assert!(!cache.month_file_path("SHFE.rb2601", "202601").exists());
    assert!(cache.month_file_path("SHFE.rb2601", "202602").exists());
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
