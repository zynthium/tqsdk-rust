use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryPhase, BacktestHistorySnapshotFileDisposition,
    BacktestHistoryTelemetryEvent, backtest_history_snapshot_cache_path_requires_placeholder,
    classify_backtest_history_snapshot_cache_path,
};

#[test]
fn existing_telemetry_literals_and_exhaustive_disposition_matches_still_compile() {
    let event = BacktestHistoryTelemetryEvent {
        request_id: None,
        symbol: "SHFE.au2608".into(),
        phase: BacktestHistoryPhase::Fill,
        completed_rows: 0,
        latest_cursor_ns: None,
        message: "legacy consumer".into(),
    };
    assert_eq!(event.completed_rows, 0);
    let path = ".backtest-history-staging/minute-v1/private.json";
    match classify_backtest_history_snapshot_cache_path(path).unwrap() {
        BacktestHistorySnapshotFileDisposition::Include(_) => panic!("journal must not publish"),
        BacktestHistorySnapshotFileDisposition::Rebuild => {}
    }
    assert!(!backtest_history_snapshot_cache_path_requires_placeholder(path).unwrap());
    // Configuration remains opt-in and does not create a cache or access a source.
    let _ = BacktestHistoryClient::builder("unused-offline-cache")
        .build()
        .unwrap()
        .on_fill_durability(|event| {
            let _ = event.progress.staged_rows;
        });
}
