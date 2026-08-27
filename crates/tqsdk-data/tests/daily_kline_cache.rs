use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{
    BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryMarketKind,
    BacktestHistoryMetadataCache, BacktestHistoryMetadataSnapshot, BacktestHistoryPhysicalSegment,
    BacktestHistoryTradingDay, DailyKlineCache, DailyKlineCacheDiagnosticStatus,
    DailyKlineCacheSnapshot, KlineSessionTemplate,
};

const DAY_NS: i64 = 86_400_000_000_000;

#[test]
fn daily_cache_stores_final_rows_in_one_symbol_file() {
    let root = temp_dir("store-final-rows");
    let cache = DailyKlineCache::open(&root).unwrap();
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    let start_ns = Utc
        .with_ymd_and_hms(2024, 1, 2, 0, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let end_ns = start_ns + 2 * DAY_NS;
    let rows = vec![kline(2, start_ns + DAY_NS, 20.0), kline(1, start_ns, 10.0)];

    cache
        .store_final_range("SHFE.au2402", start_ns, end_ns, &snapshot, &rows)
        .unwrap();

    let status = cache
        .inspect("SHFE.au2402", start_ns, end_ns, &snapshot)
        .unwrap();
    assert!(status.is_complete());
    assert_eq!(status.cached_ranges, vec![(start_ns, end_ns)]);

    let rows = cache
        .read_range("SHFE.au2402", start_ns, end_ns, &snapshot)
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.datetime).collect::<Vec<_>>(),
        vec![start_ns, start_ns + DAY_NS]
    );
    assert_eq!(
        rows.iter().map(|row| row.close).collect::<Vec<_>>(),
        vec![10.5, 20.5]
    );
    assert_eq!(
        cache.symbol_file_path("SHFE.au2402"),
        root.join("daily-kline-v1/SHFE.au2402.tqdk")
    );
}

#[test]
fn daily_cache_rejects_snapshot_mismatch_without_downgrading_to_a_cache_miss() {
    let root = temp_dir("snapshot-mismatch");
    let cache = DailyKlineCache::open(&root).unwrap();
    let start_ns = utc_ns(2024, 1, 2);
    let end_ns = start_ns + DAY_NS;
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    cache
        .store_final_range(
            "SHFE.au2402",
            start_ns,
            end_ns,
            &snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();

    let different_snapshot =
        DailyKlineCacheSnapshot::new(2, "different-calendar", "different-session").unwrap();
    let error = cache
        .coverage("SHFE.au2402", start_ns, end_ns, &different_snapshot)
        .unwrap_err();
    assert!(error.to_string().contains("metadata snapshot mismatch"));
}

#[test]
fn daily_cache_reheaders_a_compatible_retained_metadata_snapshot_when_extending_coverage() {
    let root = temp_dir("compatible-snapshot-extension");
    let symbol = "KQ.m@SHFE.au";
    let start_ns = utc_ns(2024, 1, 2);
    let cached_end_ns = start_ns + DAY_NS;
    let extended_end_ns = cached_end_ns + DAY_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let initial = metadata_cache
        .store_snapshot(metadata_snapshot(
            symbol,
            start_ns,
            cached_end_ns,
            "SHFE.au2402",
        ))
        .unwrap();
    let initial_snapshot = cache_snapshot(&initial);
    let cache = DailyKlineCache::open(&root).unwrap();
    cache
        .store_final_range(
            symbol,
            start_ns,
            cached_end_ns,
            &initial_snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();

    let extended = metadata_cache
        .store_snapshot(metadata_snapshot(
            symbol,
            start_ns,
            extended_end_ns,
            "SHFE.au2402",
        ))
        .unwrap();
    let extended_snapshot = cache_snapshot(&extended);
    assert_ne!(initial_snapshot, extended_snapshot);
    assert_eq!(
        cache
            .coverage(symbol, start_ns, extended_end_ns, &extended_snapshot)
            .unwrap()
            .missing_ranges,
        vec![(cached_end_ns, extended_end_ns)]
    );

    cache
        .store_final_range(
            symbol,
            cached_end_ns,
            extended_end_ns,
            &extended_snapshot,
            &[kline(2, cached_end_ns, 11.0)],
        )
        .unwrap();

    // The cache file must now be bound to the extended header, rather than
    // merely accepting the old header through retained-sidecar compatibility.
    let initial_sidecar = root
        .join("backtest-history-metadata-v1")
        .join("KQ.m%40SHFE.au")
        .join("snapshots")
        .join(format!("{}.json", initial.snapshot_hash));
    std::fs::remove_file(initial_sidecar).unwrap();
    assert_eq!(
        cache
            .read_range(symbol, start_ns, extended_end_ns, &extended_snapshot)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn daily_cache_refuses_incompatible_snapshot_reheader_without_changing_file_bytes() {
    let root = temp_dir("incompatible-snapshot-extension");
    let symbol = "KQ.m@SHFE.au";
    let start_ns = utc_ns(2024, 1, 2);
    let cached_end_ns = start_ns + DAY_NS;
    let extended_end_ns = cached_end_ns + DAY_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let initial = metadata_cache
        .store_snapshot(metadata_snapshot(
            symbol,
            start_ns,
            cached_end_ns,
            "SHFE.au2402",
        ))
        .unwrap();
    let initial_snapshot = cache_snapshot(&initial);
    let cache = DailyKlineCache::open(&root).unwrap();
    cache
        .store_final_range(
            symbol,
            start_ns,
            cached_end_ns,
            &initial_snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();
    let path = cache.symbol_file_path(symbol);
    let before = std::fs::read(&path).unwrap();

    let incompatible = metadata_cache
        .store_snapshot(metadata_snapshot(
            symbol,
            start_ns,
            extended_end_ns,
            "SHFE.au2404",
        ))
        .unwrap();
    let error = cache
        .store_final_range(
            symbol,
            cached_end_ns,
            extended_end_ns,
            &cache_snapshot(&incompatible),
            &[kline(2, cached_end_ns, 11.0)],
        )
        .unwrap_err();
    assert!(error.to_string().contains("metadata snapshot mismatch"));
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn daily_cache_diagnoses_corruption_and_explicit_purge_restores_a_missing_symbol() {
    let root = temp_dir("corrupt-and-purge");
    let cache = DailyKlineCache::open(&root).unwrap();
    let start_ns = utc_ns(2024, 1, 2);
    let end_ns = start_ns + DAY_NS;
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    cache
        .store_final_range(
            "SHFE.au2402",
            start_ns,
            end_ns,
            &snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();
    let path = cache.symbol_file_path("SHFE.au2402");
    std::fs::write(&path, b"not a daily Kline cache file").unwrap();

    let diagnose = cache.diagnose("SHFE.au2402").unwrap();
    assert_eq!(diagnose.status, DailyKlineCacheDiagnosticStatus::Corrupt);
    assert!(
        cache
            .read_range("SHFE.au2402", start_ns, end_ns, &snapshot)
            .is_err()
    );

    let purge = cache.purge_symbol("SHFE.au2402").unwrap();
    assert!(purge.removed);
    assert!(purge.removed_bytes > 0);
    assert_eq!(
        cache.diagnose("SHFE.au2402").unwrap().status,
        DailyKlineCacheDiagnosticStatus::Missing
    );
}

#[test]
fn daily_cache_rejects_an_unsupported_file_version() {
    let root = temp_dir("unsupported-version");
    let cache = DailyKlineCache::open(&root).unwrap();
    let start_ns = utc_ns(2024, 1, 2);
    let end_ns = start_ns + DAY_NS;
    let snapshot = DailyKlineCacheSnapshot::cst_v1();
    cache
        .store_final_range(
            "SHFE.au2402",
            start_ns,
            end_ns,
            &snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();
    let path = cache.symbol_file_path("SHFE.au2402");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();

    assert_eq!(
        cache.diagnose("SHFE.au2402").unwrap().status,
        DailyKlineCacheDiagnosticStatus::UnsupportedVersion
    );
    assert!(
        cache
            .coverage("SHFE.au2402", start_ns, end_ns, &snapshot)
            .is_err()
    );
}

#[test]
fn daily_cache_refuses_current_or_future_final_coverage() {
    let root = temp_dir("open-day");
    let cache = DailyKlineCache::open(&root).unwrap();
    let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
    let error = cache
        .store_final_range(
            "SHFE.au2402",
            now_ns,
            now_ns + 1,
            &DailyKlineCacheSnapshot::cst_v1(),
            &[],
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("only claim final coverage before current CST trading day")
    );
}

fn kline(id: i64, datetime: i64, open: f64) -> Kline {
    Kline {
        id,
        datetime,
        open,
        high: open + 1.0,
        low: open - 1.0,
        close: open + 0.5,
        volume: id * 10,
        open_oi: id * 100,
        close_oi: id * 100 + 1,
        ..Kline::default()
    }
}

fn metadata_snapshot(
    logical_symbol: &str,
    start_ns: i64,
    end_ns: i64,
    physical_symbol: &str,
) -> BacktestHistoryMetadataSnapshot {
    BacktestHistoryMetadataSnapshot {
        schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
        market_kind: BacktestHistoryMarketKind::Futures,
        logical_symbol: logical_symbol.to_string(),
        captured_at_ns: end_ns,
        trading_days: vec![BacktestHistoryTradingDay {
            date: "2024-01-02".to_string(),
            is_trading_day: true,
            start_ns,
            end_ns,
        }],
        session: KlineSessionTemplate::cst_trading_day(),
        physical_segments: vec![BacktestHistoryPhysicalSegment {
            physical_symbol: physical_symbol.to_string(),
            start_ns,
            end_ns,
        }],
        snapshot_hash: String::new(),
    }
}

fn cache_snapshot(metadata: &BacktestHistoryMetadataSnapshot) -> DailyKlineCacheSnapshot {
    DailyKlineCacheSnapshot::new(
        metadata.schema_version,
        metadata.snapshot_hash.clone(),
        metadata.session.snapshot_hash(),
    )
    .unwrap()
}

fn utc_ns(year: i32, month: u32, day: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-data-daily-kline-{name}-{}-{nonce}",
        std::process::id()
    ))
}
