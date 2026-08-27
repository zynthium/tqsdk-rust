use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{MinuteKlineCache, MinuteKlineCacheSnapshot};

const MINUTE_NS: i64 = 60_000_000_000;
type KlineBits = (i64, i64, u64, u64, u64, u64, i64, i64, i64, Option<i64>);

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
fn reader_uses_a_month_snapshot_without_blocking_atomic_replacement() {
    let root = temp_dir("reader-month-snapshot");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let first = utc_ns(2026, 1, 15, 2, 0);
    let second = first + MINUTE_NS;
    let end = second + MINUTE_NS;
    cache
        .store_final_range(
            "SHFE.rb2601",
            first,
            end,
            &snapshot,
            &[kline(1, first, 10.0), kline(2, second, 11.0)],
        )
        .unwrap();

    let mut reader = cache
        .open_reader("SHFE.rb2601", first, end, &snapshot)
        .unwrap();
    assert_eq!(reader.next_kline().unwrap().unwrap().id, 1);

    cache
        .store_final_range(
            "SHFE.rb2601",
            first,
            end,
            &snapshot,
            &[kline(1, first, 20.0), kline(2, second, 21.0)],
        )
        .expect("an opened reader must not block atomic month replacement");

    assert_eq!(reader.next_kline().unwrap().unwrap().close, 11.0);
    let replacement = cache
        .read_range("SHFE.rb2601", first, end, &snapshot)
        .unwrap();
    assert_eq!(
        replacement.iter().map(|row| row.close).collect::<Vec<_>>(),
        vec![20.0, 21.0]
    );
}

#[test]
fn final_empty_range_records_coverage_without_inventing_rows() {
    let root = temp_dir("final-empty-range");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 15, 2, 0);
    let end = start + MINUTE_NS;

    let report = cache
        .store_final_range("SHFE.rb2601", start, end, &snapshot, &[])
        .unwrap();

    assert_eq!(report.rows, 0);
    assert!(
        cache
            .coverage("SHFE.rb2601", start, end, &snapshot)
            .unwrap()
            .is_complete()
    );
    assert!(
        cache
            .open_reader("SHFE.rb2601", start, end, &snapshot)
            .unwrap()
            .next_kline()
            .unwrap()
            .is_none()
    );
}

#[test]
fn new_month_files_use_v5_compression_and_round_trip_every_kline_field() {
    let root = temp_dir("v5-compression-round-trip");
    let cache = MinuteKlineCache::open(&root).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::new(1, "calendar-v1", "session-v1").unwrap();
    let start = utc_ns(2026, 1, 15, 2, 0);
    let rows = (0_i64..1_024)
        .map(|offset| kline(offset + 1, start + offset * MINUTE_NS, 10.0))
        .collect::<Vec<_>>();
    let end = start + i64::try_from(rows.len()).unwrap() * MINUTE_NS;

    cache
        .store_final_range("SHFE.rb2601", start, end, &snapshot, &rows)
        .unwrap();

    let path = cache.month_file_path("SHFE.rb2601", "202601");
    let report = cache.diagnose().unwrap();
    assert_eq!(report.problem_files, 0);
    assert_eq!(report.files[0].schema_version, Some(5));
    assert!(
        std::fs::metadata(&path).unwrap().len() < 36 + 16 + 80 * u64::try_from(rows.len()).unwrap(),
        "a repeated month should be smaller than v4's fixed 80-byte rows"
    );

    let actual = cache
        .read_range("SHFE.rb2601", start, end, &snapshot)
        .unwrap();
    assert_eq!(kline_bits(&actual), kline_bits(&rows));
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

    let path = cache.month_file_path("SHFE.rb2601", "202601");
    std::fs::write(&path, b"broken").unwrap();
    let bytes_before_read = std::fs::read(&path).unwrap();
    assert!(
        cache
            .coverage("SHFE.rb2601", start, end, &snapshot)
            .is_err()
    );
    assert!(path.exists());
    assert_eq!(std::fs::read(path).unwrap(), bytes_before_read);
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
