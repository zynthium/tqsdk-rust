use tqsdk_core::{Kline, Tick};
use tqsdk_data::{HistoryCacheStatus, HistoryIntegrityCheck, HistoryPermissionStatus};

#[test]
fn kline_integrity_report_detects_gaps_duplicates_and_timestamp_regressions() {
    let rows = vec![
        kline(1, 0),
        kline(2, 60),
        kline(3, 60),
        kline(4, 180),
        kline(5, 120),
    ];

    let report = HistoryIntegrityCheck::kline("SHFE.au2602", 60, 0, 300).inspect_klines(&rows);

    assert_eq!(report.requested_range, (0, 300));
    assert_eq!(report.returned_range, Some((0, 180)));
    assert_eq!(report.missing_intervals, vec![(240, 300)]);
    assert_eq!(report.duplicated_rows.len(), 1);
    assert_eq!(report.duplicated_rows[0].first_index, 1);
    assert_eq!(report.duplicated_rows[0].duplicate_index, 2);
    assert_eq!(report.non_monotonic_timestamps.len(), 1);
    assert_eq!(report.non_monotonic_timestamps[0].previous_index, 3);
    assert_eq!(report.non_monotonic_timestamps[0].index, 4);
}

#[test]
fn tick_integrity_report_exposes_cache_and_permission_state_without_fixed_gap_assumptions() {
    let rows = vec![tick(10, 1_000), tick(12, 1_500)];

    let report = HistoryIntegrityCheck::tick("SHFE.au2602", 1_000, 2_000)
        .with_permission_status(HistoryPermissionStatus::Checked)
        .with_cache_usage(1, vec![(1_400, 1_700)])
        .inspect_ticks(&rows);

    assert_eq!(report.permission_status, HistoryPermissionStatus::Checked);
    assert_eq!(
        report.cache_status,
        HistoryCacheStatus::MissDownloaded {
            hit_rows: 1,
            downloaded_rows: 1,
            downloaded_ranges: vec![(1_400, 1_700)],
        }
    );
    assert!(report.missing_intervals.is_empty());
}

fn kline(id: i64, datetime: i64) -> Kline {
    Kline {
        id,
        datetime,
        ..Kline::default()
    }
}

fn tick(id: i64, datetime: i64) -> Tick {
    Tick {
        id,
        datetime,
        ..Tick::default()
    }
}
