use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, Kline, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput, Tick,
};
use tqsdk_data::{
    DataClientBuilder, DataError, HistorySeriesCache, HistorySeriesCacheFileStatus,
    KlineDataSeriesRequest,
};
use tqsdk_session::testing::ManualSession;

#[test]
fn builder_does_not_enable_history_cache_by_default() {
    let client = DataClientBuilder::new().build().unwrap();

    assert!(client.history_cache().is_none());
}

#[test]
fn builder_history_cache_dir_is_inert_without_enable_flag() {
    let dir = temp_dir("builder-dir-without-enable");
    let file_path = dir.join("not-a-directory");
    std::fs::write(&file_path, b"occupied").unwrap();

    let client = DataClientBuilder::new()
        .history_cache_dir(&file_path)
        .build()
        .unwrap();

    assert!(client.history_cache().is_none());
}

#[test]
fn builder_enables_python_compatible_cache_with_custom_dir() {
    let dir = temp_dir("builder-custom-dir");
    let client = DataClientBuilder::new()
        .history_cache_enabled(true)
        .history_cache_dir(&dir)
        .build()
        .unwrap();

    let cache = client
        .history_cache()
        .expect("enabled builder should install history cache");
    assert_eq!(cache.root_dir(), dir.as_path());
    assert!(cache.uses_mmap_backend());
}

#[test]
fn builder_reports_cache_open_errors() {
    let dir = temp_dir("builder-open-error");
    let file_path = dir.join("not-a-directory");
    std::fs::write(&file_path, b"occupied").unwrap();

    let build_result = DataClientBuilder::new()
        .history_cache_enabled(true)
        .history_cache_dir(&file_path)
        .build();
    let Err(err) = build_result else {
        panic!("builder should report cache open errors");
    };

    assert!(matches!(err, DataError::Io(_)));
}

#[test]
fn kline_cache_reads_python_compatible_raw_file_with_mmap() {
    let dir = temp_dir("kline-python-compatible");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[
                kline(10, 1_000, 10.0),
                kline(11, 61_000_000_000, 11.0),
                kline(12, 121_000_000_000, 12.0),
            ],
        )
        .unwrap();

    let series = cache
        .read_kline_window("SHFE.au2602", 60_000_000_000, 1_000, 121_000_000_000)
        .unwrap();

    assert_eq!(series.len(), 2);
    assert_eq!(series[0].id, 10);
    assert_eq!(series[1].id, 11);
    assert_eq!(
        cache
            .cached_id_ranges("SHFE.au2602", 60_000_000_000)
            .unwrap(),
        vec![(10, 13)]
    );
    assert!(dir.join("SHFE.au2602.60000000000.10.13").exists());
}

#[test]
fn empty_cache_segment_is_ignored_when_computing_missing_ranges() {
    let dir = temp_dir("empty-segment");
    std::fs::File::create(dir.join("SHFE.au2602.60000000000.1.2")).unwrap();
    let cache = HistorySeriesCache::open(&dir).unwrap();

    let missing = cache
        .missing_kline_datetime_ranges("SHFE.au2602", 60_000_000_000, 0, 60_000_000_000)
        .unwrap();

    assert_eq!(missing, vec![(0, 60_000_000_000)]);
    assert!(
        cache
            .cached_id_ranges("SHFE.au2602", 60_000_000_000)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn empty_cache_segment_does_not_shift_read_window_segments() {
    let dir = temp_dir("empty-segment-read-window");
    std::fs::File::create(dir.join("SHFE.au2602.60000000000.1.2")).unwrap();
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(10, 600_000_000_000, 10.0)],
        )
        .unwrap();

    let rows = cache
        .read_kline_window(
            "SHFE.au2602",
            60_000_000_000,
            600_000_000_000,
            660_000_000_000,
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 10);
}

#[test]
fn cache_report_counts_downloaded_rows_not_downloaded_ranges() {
    run_on_tokio(async {
        let dir = temp_dir("cache-report-hit-rows");
        let cache = HistorySeriesCache::open(&dir).unwrap();
        cache
            .write_kline_segment(
                "SHFE.ao2609",
                60_000_000_000,
                &[kline(1, 1_713_660_000_000_000_000, 1.0)],
            )
            .unwrap();
        let (manual, handle) = manual_session_and_handle();
        seed_auth_features(&handle, &["tq_dl"]);
        let client = DataClientBuilder::new()
            .with_session(manual.client_clone())
            .history_cache_enabled(true)
            .history_cache_dir(&dir)
            .build()
            .unwrap();

        let seed_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            for sequence in 1..=64 {
                seed_ready_kline_chart(
                    &handle,
                    &format!("data-series-kline-SHFE_ao2609-60000000000-{sequence}"),
                    "SHFE.ao2609",
                    60_000_000_000,
                    2,
                    4,
                    false,
                );
            }
        });

        let series = client
            .get_kline_data_series(
                KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    1_713_660_000_000_000_000,
                    1_713_660_180_000_000_000,
                )
                .with_timeout(Duration::from_millis(200)),
            )
            .await
            .unwrap();

        seed_thread.join().unwrap();
        let report = series
            .cache_report()
            .expect("cache report should be present");
        assert_eq!(series.len(), 3);
        assert_eq!(report.hit_rows, 1);
        assert_eq!(report.downloaded_ranges.len(), 1);
    });
}

#[test]
fn tick_cache_uses_python_five_level_layout_for_shfe_symbols() {
    let dir = temp_dir("tick-five-level");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_tick_segment("SHFE.au2602", &[tick(20, 2_000, 20.0)])
        .unwrap();

    let file = dir.join("SHFE.au2602.0.20.21");
    let len = std::fs::metadata(file).unwrap().len();

    assert_eq!(len, 29 * 8);
    let rows = cache.read_tick_window("SHFE.au2602", 2_000, 2_100).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 20);
    assert_eq!(rows[0].ask_price5, 25.5);
}

#[test]
fn cached_datetime_ranges_trim_mutable_tail_before_diff() {
    let dir = temp_dir("diff-tail");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();

    let missing = cache
        .missing_kline_datetime_ranges("SHFE.au2602", 60_000_000_000, 0, 120_000_000_000)
        .unwrap();

    assert_eq!(missing, vec![(60_000_000_000, 120_000_000_000)]);
}

#[test]
fn merge_handles_adjacent_segments_and_duplicate_tail_row() {
    let dir = temp_dir("merge-duplicate-tail");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[
                kline(2, 60_000_000_000, 22.0),
                kline(3, 120_000_000_000, 3.0),
            ],
        )
        .unwrap();

    cache
        .merge_adjacent_files("SHFE.au2602", 60_000_000_000)
        .unwrap();
    let rows = cache
        .read_kline_window("SHFE.au2602", 60_000_000_000, 0, 180_000_000_000)
        .unwrap();

    assert_eq!(
        cache
            .cached_id_ranges("SHFE.au2602", 60_000_000_000)
            .unwrap(),
        vec![(1, 4)]
    );
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(rows[1].close, 22.0);
}

#[test]
fn corrupted_cache_file_returns_typed_error() {
    let dir = temp_dir("corrupt");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SHFE.au2602.60000000000.1.2"), [1_u8, 2, 3]).unwrap();
    let cache = HistorySeriesCache::open(&dir).unwrap();

    let err = cache
        .read_kline_window("SHFE.au2602", 60_000_000_000, 0, 60_000_000_000)
        .unwrap_err();

    assert!(matches!(err, DataError::InvalidResponse(message) if message.contains("row width")));
}

#[test]
fn merge_adjacent_files_rejects_segment_shorter_than_filename_range() {
    let dir = temp_dir("merge-short-segment");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(3, 120_000_000_000, 3.0)],
        )
        .unwrap();
    std::fs::rename(
        dir.join("SHFE.au2602.60000000000.1.3"),
        dir.join("SHFE.au2602.60000000000.1.4"),
    )
    .unwrap();

    let err = cache
        .merge_adjacent_files("SHFE.au2602", 60_000_000_000)
        .unwrap_err();

    assert!(matches!(err, DataError::InvalidResponse(message)
        if message.contains("history series cache range does not match row count")));
    assert!(dir.join("SHFE.au2602.60000000000.1.4").exists());
    assert!(dir.join("SHFE.au2602.60000000000.3.4").exists());
}

#[test]
fn merge_adjacent_files_rejects_copy_count_larger_than_mapped_segment() {
    let dir = temp_dir("merge-copy-overflow");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment("SHFE.au2602", 60_000_000_000, &[kline(1, 0, 1.0)])
        .unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();
    std::fs::rename(
        dir.join("SHFE.au2602.60000000000.1.2"),
        dir.join("SHFE.au2602.60000000000.1.3"),
    )
    .unwrap();

    let err = cache
        .merge_adjacent_files("SHFE.au2602", 60_000_000_000)
        .unwrap_err();

    assert!(matches!(err, DataError::InvalidResponse(message)
        if message.contains("history series cache range does not match row count")
            || message.contains("history series merge requested more rows than segment contains")));
}

#[test]
fn cache_only_kline_reader_returns_series_without_session() {
    let dir = temp_dir("cache-only-hit");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();

    let series = cache
        .read_kline_data_series(KlineDataSeriesRequest::new(
            "SHFE.au2602",
            Duration::from_secs(60),
            0,
            60_000_000_000,
        ))
        .unwrap();

    assert_eq!(series.len(), 1);
    assert_eq!(series.rows()[0].id, 1);
    let report = series
        .cache_report()
        .expect("cache-only read reports cache");
    assert_eq!(report.hit_rows, 1);
    assert!(report.downloaded_ranges.is_empty());
}

#[test]
fn cache_only_kline_reader_reports_missing_ranges_without_download() {
    let dir = temp_dir("cache-only-miss");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    let err = cache
        .read_kline_data_series(KlineDataSeriesRequest::new(
            "SHFE.au2602",
            Duration::from_secs(60),
            0,
            60_000_000_000,
        ))
        .unwrap_err();

    assert!(matches!(err, DataError::CacheMiss(miss)
            if miss.symbol == "SHFE.au2602"
                && miss.duration_ns == 60_000_000_000
                && miss.missing_ranges == vec![(0, 60_000_000_000)]));
}

#[test]
fn cache_scan_reports_schema_and_corrupt_segment_status() {
    let dir = temp_dir("scan-corrupt");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment("SHFE.au2602", 60_000_000_000, &[kline(1, 0, 1.0)])
        .unwrap();
    std::fs::write(dir.join("SHFE.au2602.60000000000.10.11"), [1_u8, 2, 3]).unwrap();
    std::fs::write(dir.join("SHFE.au2602.60000000000.temp"), [1_u8, 2, 3]).unwrap();

    let report = cache.scan().unwrap();

    assert_eq!(report.schema_version, 1);
    assert!(report.files.iter().any(|file| {
        file.file_name == "SHFE.au2602.60000000000.1.2"
            && file.status == HistorySeriesCacheFileStatus::Readable
            && file.rows == 1
    }));
    assert!(report.files.iter().any(|file| {
        file.file_name == "SHFE.au2602.60000000000.10.11"
            && file.status == HistorySeriesCacheFileStatus::InvalidRowWidth
    }));
    assert!(report.files.iter().any(|file| {
        file.file_name == "SHFE.au2602.60000000000.temp"
            && file.status == HistorySeriesCacheFileStatus::IncompleteWrite
    }));
}

#[test]
fn cache_enforce_limits_removes_expired_and_oldest_segments_only() {
    let dir = temp_dir("enforce-limits");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment("SHFE.au2602", 60_000_000_000, &[kline(1, 0, 1.0)])
        .unwrap();
    std::fs::write(dir.join(".SHFE.au2602.60000000000.lock"), b"lock").unwrap();
    std::fs::write(dir.join("SHFE.au2602.60000000000.temp"), b"temp").unwrap();

    let report = cache.enforce_limits(None, Some(0)).unwrap();

    assert_eq!(report.removed_files, 1);
    assert!(!dir.join("SHFE.au2602.60000000000.1.2").exists());
    assert!(dir.join(".SHFE.au2602.60000000000.lock").exists());
    assert!(dir.join("SHFE.au2602.60000000000.temp").exists());

    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(10, 600_000_000_000, 10.0)],
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(20, 1_200_000_000_000, 20.0)],
        )
        .unwrap();

    let report = cache.enforce_limits(Some(72), None).unwrap();

    assert_eq!(report.removed_files, 1);
    assert!(!dir.join("SHFE.au2602.60000000000.10.11").exists());
    assert!(dir.join("SHFE.au2602.60000000000.20.21").exists());
}

#[test]
fn builder_history_cache_retention_policy_runs_after_cache_hit_read() {
    run_on_tokio(async {
        let dir = temp_dir("builder-retention-policy");
        let cache = HistorySeriesCache::open(&dir).unwrap();
        cache
            .write_kline_segment(
                "SHFE.ao2609",
                60_000_000_000,
                &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
            )
            .unwrap();
        let (manual, handle) = manual_session_and_handle();
        seed_auth_features(&handle, &["tq_dl"]);
        let client = DataClientBuilder::new()
            .with_session(manual.client_clone())
            .history_cache_enabled(true)
            .history_cache_dir(&dir)
            .history_cache_retention_days(0)
            .build()
            .unwrap();

        let series = client
            .get_kline_data_series(KlineDataSeriesRequest::new(
                "SHFE.ao2609",
                Duration::from_secs(60),
                0,
                60_000_000_000,
            ))
            .await
            .unwrap();

        assert_eq!(series.len(), 1);
        assert!(!dir.join("SHFE.ao2609.60000000000.1.3").exists());
    });
}

#[test]
fn cache_hit_still_requires_tq_dl_permission() {
    run_on_tokio(async {
        let dir = temp_dir("hit-requires-permission");
        let cache = HistorySeriesCache::open(&dir).unwrap();
        cache
            .write_kline_segment(
                "SHFE.ao2609",
                60_000_000_000,
                &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
            )
            .unwrap();
        let (manual, handle) = manual_session_and_handle();
        seed_auth_features(&handle, &["query"]);
        let client = DataClientBuilder::new()
            .with_session(manual.client_clone())
            .history_cache_enabled(true)
            .history_cache_dir(&dir)
            .build()
            .unwrap();

        let err = client
            .get_kline_data_series(KlineDataSeriesRequest::new(
                "SHFE.ao2609",
                Duration::from_secs(60),
                0,
                120_000_000_000,
            ))
            .await
            .unwrap_err();

        assert!(
            matches!(err, DataError::PermissionDenied(message) if message.contains("tq_dl permission"))
        );
    });
}

#[test]
fn cache_miss_download_uses_official_set_chart_sequence() {
    run_on_tokio(async {
        let dir = temp_dir("official-download-sequence");
        let (manual, handle) = manual_session_and_handle();
        let session = manual.client_clone();
        seed_auth_features(&handle, &["tq_dl"]);
        let client = DataClientBuilder::new()
            .with_session(session.clone())
            .history_cache_enabled(true)
            .history_cache_dir(&dir)
            .build()
            .unwrap();

        let seed_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            for sequence in 1..=64 {
                seed_ready_kline_chart(
                    &handle,
                    &format!("data-series-kline-SHFE_ao2609-60000000000-{sequence}"),
                    "SHFE.ao2609",
                    60_000_000_000,
                    1,
                    2,
                    false,
                );
            }
        });

        let series = client
            .get_kline_data_series(
                KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    1_713_660_000_000_000_000,
                    1_713_660_120_000_000_000,
                )
                .with_timeout(Duration::from_millis(200)),
            )
            .await
            .unwrap();

        seed_thread.join().unwrap();
        assert_eq!(series.len(), 2);
        let submitted = submitted_set_chart_messages(&manual);
        assert!(submitted.iter().any(|command| {
            command
                .get("chart_id")
                .and_then(|value| value.as_str())
                .is_some_and(|chart_id| {
                    chart_id.starts_with("data-series-kline-SHFE_ao2609-60000000000-")
                })
                && command.get("view_width").and_then(|value| value.as_u64()) == Some(2_000)
                && command
                    .get("focus_datetime")
                    .and_then(|value| value.as_i64())
                    == Some(1_713_660_000_000_000_000)
                && command
                    .get("focus_position")
                    .and_then(|value| value.as_u64())
                    == Some(0)
                && command.get("left_kline_id").is_none()
        }));
        assert!(submitted.iter().any(|command| {
            command
                .get("chart_id")
                .and_then(|value| value.as_str())
                .is_some_and(|chart_id| {
                    chart_id.starts_with("data-series-kline-SHFE_ao2609-60000000000-")
                })
                && command.get("ins_list").and_then(|value| value.as_str()) == Some("")
        }));
    });
}

#[test]
fn cache_miss_download_continues_when_short_page_has_more_data() {
    run_on_tokio(async {
        let dir = temp_dir("short-page-more-data");
        let (manual, handle) = manual_session_and_handle();
        let session = manual.client_clone();
        seed_auth_features(&handle, &["tq_dl"]);
        let client = DataClientBuilder::new()
            .with_session(session.clone())
            .history_cache_enabled(true)
            .history_cache_dir(&dir)
            .build()
            .unwrap();
        let submitted = Arc::new(Mutex::new(Vec::new()));
        let submitted_for_driver = Arc::clone(&submitted);

        let seed_thread = std::thread::spawn(move || {
            drive_short_page_more_data_download(
                &manual,
                &handle,
                submitted_for_driver,
                "SHFE.ao2609",
                60_000_000_000,
            );
        });

        let series = client
            .get_kline_data_series(
                KlineDataSeriesRequest::new(
                    "SHFE.ao2609",
                    Duration::from_secs(60),
                    1_713_660_000_000_000_000,
                    1_713_660_120_000_000_000,
                )
                .with_timeout(Duration::from_millis(500)),
            )
            .await
            .unwrap();

        seed_thread.join().unwrap();
        assert_eq!(
            series.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let submitted = submitted.lock().unwrap();
        assert!(submitted.iter().any(|command| {
            command
                .get("left_kline_id")
                .and_then(|value| value.as_i64())
                == Some(2)
        }));
    });
}

fn run_on_tokio<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(future)
}

fn manual_session_and_handle() -> (ManualSession, RuntimeHandle) {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let manual = ManualSession::from_runtime(handle.clone());
    (manual, handle)
}

fn submitted_set_chart_messages(manual: &ManualSession) -> Vec<serde_json::Value> {
    manual
        .drain_dispatches()
        .unwrap()
        .into_iter()
        .filter_map(|dispatch| match dispatch.request {
            OutboundRequest::Transport(OutboundFrame::Text(text)) => {
                serde_json::from_str::<serde_json::Value>(&text).ok()
            }
            _ => None,
        })
        .filter(|value| value.get("aid").and_then(|aid| aid.as_str()) == Some("set_chart"))
        .collect()
}

fn drain_set_chart_messages(manual: &ManualSession) -> Vec<serde_json::Value> {
    manual
        .drain_dispatches()
        .unwrap()
        .into_iter()
        .filter_map(|dispatch| match dispatch.request {
            OutboundRequest::Transport(OutboundFrame::Text(text)) => {
                serde_json::from_str::<serde_json::Value>(&text).ok()
            }
            _ => None,
        })
        .filter(|value| value.get("aid").and_then(|aid| aid.as_str()) == Some("set_chart"))
        .collect()
}

fn drive_short_page_more_data_download(
    manual: &ManualSession,
    handle: &RuntimeHandle,
    submitted: Arc<Mutex<Vec<serde_json::Value>>>,
    symbol: &str,
    duration_ns: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut responded_focus = false;
    let mut responded_left = false;
    while Instant::now() < deadline {
        for command in drain_set_chart_messages(manual) {
            submitted.lock().unwrap().push(command.clone());
            if command.get("ins_list").and_then(|value| value.as_str()) == Some("") {
                return;
            }
            let Some(chart_id) = command.get("chart_id").and_then(|value| value.as_str()) else {
                continue;
            };
            if command.get("focus_datetime").is_some() && !responded_focus {
                seed_ready_kline_chart_with_state(
                    handle,
                    SeedKlineChart {
                        chart_id,
                        symbol,
                        duration_ns,
                        left_id: 1,
                        right_id: 1,
                        more_data: true,
                        request_state: ChartRequestState::Focus {
                            datetime: command
                                .get("focus_datetime")
                                .and_then(|value| value.as_i64())
                                .expect("focus datetime is present"),
                            position: command
                                .get("focus_position")
                                .and_then(|value| value.as_u64())
                                .and_then(|value| usize::try_from(value).ok())
                                .expect("focus position is present"),
                        },
                    },
                );
                responded_focus = true;
            } else if command
                .get("left_kline_id")
                .and_then(|value| value.as_i64())
                == Some(2)
                && !responded_left
            {
                seed_ready_kline_chart_with_state(
                    handle,
                    SeedKlineChart {
                        chart_id,
                        symbol,
                        duration_ns,
                        left_id: 2,
                        right_id: 2,
                        more_data: false,
                        request_state: ChartRequestState::LeftId(2),
                    },
                );
                responded_left = true;
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("history series short-page test driver timed out");
}

fn seed_auth_features(handle: &RuntimeHandle, features: &[&str]) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "system".to_string(),
                domains: vec![ProtocolDomain::System],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "auth": {
                            "context": {
                                "features": features,
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed auth features should produce a commit");
}

fn seed_ready_kline_chart(
    handle: &RuntimeHandle,
    chart_id: &str,
    symbol: &str,
    duration_ns: i64,
    left_id: i64,
    right_id: i64,
    more_data: bool,
) {
    seed_ready_kline_chart_with_state(
        handle,
        SeedKlineChart {
            chart_id,
            symbol,
            duration_ns,
            left_id,
            right_id,
            more_data,
            request_state: ChartRequestState::Focus {
                datetime: 1_713_660_000_000_000_000,
                position: 0,
            },
        },
    );
}

struct SeedKlineChart<'a> {
    chart_id: &'a str,
    symbol: &'a str,
    duration_ns: i64,
    left_id: i64,
    right_id: i64,
    more_data: bool,
    request_state: ChartRequestState,
}

enum ChartRequestState {
    Focus { datetime: i64, position: usize },
    LeftId(i64),
}

fn seed_ready_kline_chart_with_state(handle: &RuntimeHandle, seed: SeedKlineChart<'_>) {
    let state = match seed.request_state {
        ChartRequestState::Focus { datetime, position } => json!({
            "ins_list": seed.symbol,
            "duration": seed.duration_ns,
            "view_width": 2000,
            "focus_datetime": datetime,
            "focus_position": position,
        }),
        ChartRequestState::LeftId(left_id) => json!({
            "ins_list": seed.symbol,
            "duration": seed.duration_ns,
            "view_width": 2000,
            "left_kline_id": left_id,
        }),
    };
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            seed.chart_id: {
                                "state": state,
                                "left_id": seed.left_id,
                                "right_id": seed.right_id,
                                "more_data": seed.more_data,
                                "ready": true,
                            }
                        },
                        "klines": {
                            seed.symbol: {
                                seed.duration_ns.to_string(): {
                                    "data": {
                                        "1": {
                                            "id": 1,
                                            "datetime": 1_713_660_000_000_000_000_i64,
                                            "open": 618.0,
                                            "high": 620.0,
                                            "low": 617.0,
                                            "close": 619.0,
                                            "volume": 12,
                                            "open_oi": 100,
                                            "close_oi": 101
                                        },
                                        "2": {
                                            "id": 2,
                                            "datetime": 1_713_660_060_000_000_000_i64,
                                            "open": 619.0,
                                            "high": 621.0,
                                            "low": 618.0,
                                            "close": 620.0,
                                            "volume": 15,
                                            "open_oi": 101,
                                            "close_oi": 103
                                        },
                                        "3": {
                                            "id": 3,
                                            "datetime": 1_713_660_120_000_000_000_i64,
                                            "open": 620.0,
                                            "high": 622.0,
                                            "low": 619.0,
                                            "close": 621.0,
                                            "volume": 16,
                                            "open_oi": 103,
                                            "close_oi": 104
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed ready kline chart should produce a commit");
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("tqsdk-data-history-series-cache-{name}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    canonical_or_original(&dir)
}

fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn kline(id: i64, datetime: i64, close: f64) -> Kline {
    Kline {
        id,
        datetime,
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: id * 10,
        open_oi: id * 100,
        close_oi: id * 100 + 1,
        ..Kline::default()
    }
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        highest: last_price + 1.0,
        lowest: last_price - 1.0,
        average: last_price + 0.5,
        volume: id * 10,
        amount: last_price * 10.0,
        open_interest: id * 100,
        bid_price1: last_price - 0.1,
        bid_volume1: 1,
        ask_price1: last_price + 0.1,
        ask_volume1: 2,
        bid_price2: last_price - 0.2,
        bid_volume2: 3,
        ask_price2: last_price + 0.2,
        ask_volume2: 4,
        bid_price3: last_price - 0.3,
        bid_volume3: 5,
        ask_price3: last_price + 0.3,
        ask_volume3: 6,
        bid_price4: last_price - 0.4,
        bid_volume4: 7,
        ask_price4: last_price + 0.4,
        ask_volume4: 8,
        bid_price5: last_price - 0.5,
        bid_volume5: 9,
        ask_price5: last_price + 5.5,
        ask_volume5: 10,
        ..Tick::default()
    }
}
