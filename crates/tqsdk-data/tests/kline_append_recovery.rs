use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{
    DailyKlineCache, DailyKlineCacheSnapshot, MinuteKlineCache, MinuteKlineCacheSnapshot,
};

struct Root(PathBuf);
impl Root {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "kline-append-{name}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        )))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn start() -> i64 {
    Utc.with_ymd_and_hms(2024, 1, 2, 2, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}
fn row(id: i64, datetime: i64) -> Kline {
    Kline {
        id,
        datetime,
        open: 1.0,
        high: 2.0,
        low: 1.0,
        close: 2.0,
        volume: 1,
        open_oi: 1,
        close_oi: 1,
        ..Kline::default()
    }
}

#[test]
fn daily_append_preserves_prefix_recovers_tail_and_isolates_existing_hardlinks() {
    let root = Root::new("daily");
    let cache = DailyKlineCache::open(&root.0).unwrap();
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    let day = 86_400_000_000_000;
    let put = |index: i64| {
        cache
            .store_final_range(
                "SHFE.au2406",
                start() + index * day,
                start() + (index + 1) * day,
                &snapshot,
                &[row(index, start() + index * day)],
            )
            .unwrap()
    };
    put(0);
    put(1);
    let path = cache.symbol_file_path("SHFE.au2406");
    let before = fs::read(&path).unwrap();
    assert_eq!(&before[..8], b"TQKLOG01");
    put(2);
    let after = fs::read(&path).unwrap();
    assert_eq!(&before[104..], &after[104..before.len()]);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"interrupted payload")
        .unwrap();
    assert_eq!(
        cache
            .read_range("SHFE.au2406", start(), start() + 3 * day, &snapshot)
            .unwrap()
            .len(),
        3
    );
    let linked = root.0.join("old-snapshot");
    fs::hard_link(&path, &linked).unwrap();
    let published = fs::read(&linked).unwrap();
    put(3);
    assert_eq!(fs::read(linked).unwrap(), published);
    assert_eq!(
        cache
            .read_range("SHFE.au2406", start(), start() + 4 * day, &snapshot)
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn minute_append_keeps_open_reader_snapshot_and_round_trips_after_recovery() {
    let root = Root::new("minute");
    let cache = MinuteKlineCache::open(&root.0).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let minute = 60_000_000_000;
    let put = |index: i64| {
        cache
            .store_final_range(
                "SHFE.au2406",
                start() + index * minute,
                start() + (index + 1) * minute,
                &snapshot,
                &[row(index, start() + index * minute)],
            )
            .unwrap()
    };
    put(0);
    put(1);
    let path = cache.month_file_path("SHFE.au2406", "202401");
    let before = fs::read(&path).unwrap();
    let mut pinned = cache
        .open_reader("SHFE.au2406", start(), start() + 2 * minute, &snapshot)
        .unwrap();
    assert_eq!(pinned.next_kline().unwrap().unwrap().id, 0);
    put(2);
    assert_eq!(pinned.next_kline().unwrap().unwrap().id, 1);
    assert!(pinned.next_kline().unwrap().is_none());
    let after = fs::read(&path).unwrap();
    assert_eq!(&before[104..], &after[104..before.len()]);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"torn index")
        .unwrap();
    assert!(
        cache
            .coverage("SHFE.au2406", start(), start() + 3 * minute, &snapshot)
            .unwrap()
            .is_complete()
    );
    assert!(cache.diagnose().unwrap().problem_files > 0);
    put(3);
    assert_eq!(
        cache
            .read_range("SHFE.au2406", start(), start() + 4 * minute, &snapshot)
            .unwrap()
            .len(),
        4
    );
    assert_eq!(cache.diagnose().unwrap().problem_files, 0);
}

#[test]
fn many_daily_appends_have_bounded_index_overhead() {
    let root = Root::new("growth");
    let cache = DailyKlineCache::open(&root.0).unwrap();
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    let day = 86_400_000_000_000;
    for index in 0..192 {
        cache
            .store_final_range(
                "SHFE.au2406",
                start() + index * day,
                start() + (index + 1) * day,
                &snapshot,
                &[row(index, start() + index * day)],
            )
            .unwrap();
    }
    assert!(
        fs::metadata(cache.symbol_file_path("SHFE.au2406"))
            .unwrap()
            .len()
            < 512 * 1024
    );
    assert_eq!(
        cache
            .read_range("SHFE.au2406", start(), start() + 192 * day, &snapshot)
            .unwrap()
            .len(),
        192
    );
}

#[test]
fn diagnostics_wait_for_inflight_append_before_checking_tail() {
    use fs2::FileExt;
    use std::sync::mpsc;
    use std::time::Duration;
    let root = Root::new("diagnostic-pin");
    let cache = MinuteKlineCache::open(&root.0).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let minute = 60_000_000_000;
    for index in 0..2 {
        cache
            .store_final_range(
                "SHFE.au2406",
                start() + index * minute,
                start() + (index + 1) * minute,
                &snapshot,
                &[],
            )
            .unwrap();
    }
    let path = cache.month_file_path("SHFE.au2406", "202401");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.with_extension("tqmk.lock"))
        .unwrap();
    FileExt::lock_exclusive(&lock).unwrap();
    let file = OpenOptions::new().append(true).open(&path).unwrap();
    let length = file.metadata().unwrap().len();
    (&file).write_all(b"in-flight payload").unwrap();
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        tx.send(cache.diagnose()).unwrap();
    });
    assert!(matches!(
        rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    file.set_len(length).unwrap();
    drop(lock);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .problem_files,
        0
    );
    thread.join().unwrap();
}
