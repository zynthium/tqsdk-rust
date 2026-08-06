use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{TimeZone, Utc};
use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    BACKTEST_HISTORY_METADATA_SCHEMA_VERSION, BacktestHistoryAuthProvider, BacktestHistoryClient,
    BacktestHistoryCredentials, BacktestHistoryMarketKind, BacktestHistoryMetadataCache,
    BacktestHistoryMetadataSnapshot, BacktestHistoryPhysicalSegment, BacktestHistoryPolicy,
    BacktestHistoryRequest, BacktestHistoryRows, BacktestHistoryTradingDay, BacktestTickCache,
    KlineSessionTemplate, MinuteKlineCache, MinuteKlineCacheSnapshot,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
    plan_minute_cache_stale_partition_repair, resolve_minute_cache_metadata_snapshot,
};

#[path = "support/backtest_history.rs"]
mod support;

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;

#[tokio::test]
async fn cache_materialization_skips_readback_and_reports_only_new_writes() {
    let root = temp_dir("materialize-cache-hit");
    let symbol = "SHFE.au2608";
    let timestamp = utc_ns(2026, 1, 5, 1, 0, 0);
    let day = backtest_tick_trading_day_for_timestamp_ns(timestamp).unwrap();
    let range = backtest_tick_trading_day_range(day).unwrap();
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(
            symbol,
            range.start_ns,
            range.end_ns,
            [tick(1, timestamp, 100.0, 100, 10)],
        )
        .unwrap();

    let report = cache_only_client(&root)
        .materialize_cache([BacktestHistoryRequest::tick(
            1,
            symbol,
            range.start_ns,
            range.end_ns,
        )])
        .await
        .unwrap();

    assert!(report.failed.is_empty());
    assert_eq!(report.completed.len(), 1);
    assert_eq!(report.completed[0].rows, 0);
    assert!(!report.completed[0].remote_used);
}

#[tokio::test]
async fn cache_only_tick_and_15s_requests_read_the_same_durable_tick_source() {
    let root = temp_dir("tick-and-15s");
    let symbol = "SHFE.au2608";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let day = backtest_tick_trading_day_for_timestamp_ns(start_ns).unwrap();
    let day_range = backtest_tick_trading_day_range(day).unwrap();
    let rows = vec![
        tick(1, start_ns, 100.0, 100, 10),
        tick(2, start_ns + 10 * SECOND_NS, 101.0, 104, 11),
        tick(3, start_ns + 16 * SECOND_NS, 102.0, 107, 12),
        tick(4, start_ns + 26 * SECOND_NS, 103.0, 111, 13),
        // Official server-backtest charts assign an exact bucket-boundary
        // Tick to the preceding bar while using it to open the next one.
        tick(5, start_ns + 30 * SECOND_NS, 104.0, 113, 14),
    ];
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(symbol, day_range.start_ns, day_range.end_ns, rows)
        .unwrap();
    let client = cache_only_client(&root);

    let ticks = client
        .query(BacktestHistoryRequest::tick(
            1,
            symbol,
            start_ns + 5 * SECOND_NS,
            start_ns + 27 * SECOND_NS,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Ticks(ticks) = ticks.rows else {
        panic!("Tick request must return Tick rows");
    };
    assert_eq!(
        ticks.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );

    let klines = client
        .query(BacktestHistoryRequest::kline(
            2,
            symbol,
            Duration::from_secs(15),
            start_ns,
            start_ns + 30 * SECOND_NS,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(klines.request.rows, 2);
    let BacktestHistoryRows::Klines { duration_ns, rows } = klines.rows else {
        panic!("15-second request must return Kline rows");
    };
    assert_eq!(duration_ns, 15 * SECOND_NS);
    assert_eq!(
        rows.iter().map(|row| row.datetime).collect::<Vec<_>>(),
        vec![start_ns, start_ns + 15 * SECOND_NS]
    );
    assert_eq!(
        rows.iter().map(|row| row.volume).collect::<Vec<_>>(),
        vec![104, 9]
    );
    assert_eq!(rows[1].close_oi, 14);
}

#[tokio::test]
async fn cache_only_60s_passthrough_and_5m_aggregation_use_canonical_minutes() {
    let root = temp_dir("minute-and-5m");
    let symbol = "KQ.i@SHFE.au";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + 10 * MINUTE_NS;
    let source_rows = (0_i64..10)
        .map(|offset| {
            kline(
                100 + offset,
                start_ns + offset * MINUTE_NS,
                offset as f64 + 10.0,
            )
        })
        .collect::<Vec<_>>();
    MinuteKlineCache::open(&root)
        .unwrap()
        .store_final_range(
            symbol,
            start_ns,
            end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &source_rows,
        )
        .unwrap();
    let client = cache_only_client(&root);

    let minute = client
        .query(BacktestHistoryRequest::kline(
            1,
            symbol,
            Duration::from_secs(60),
            start_ns,
            end_ns,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Klines {
        duration_ns,
        rows: minute_rows,
    } = minute.rows
    else {
        panic!("60-second request must return Kline rows");
    };
    assert_eq!(duration_ns, MINUTE_NS);
    assert_eq!(
        minute_rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        source_rows.iter().map(|row| row.id).collect::<Vec<_>>()
    );

    let five_minute = client
        .query(BacktestHistoryRequest::kline(
            2,
            symbol,
            Duration::from_secs(5 * 60),
            start_ns,
            end_ns,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(five_minute.request.rows, 2);
    let BacktestHistoryRows::Klines {
        duration_ns,
        rows: five_minute_rows,
    } = five_minute.rows
    else {
        panic!("five-minute request must return Kline rows");
    };
    assert_eq!(duration_ns, 5 * MINUTE_NS);
    assert_eq!(
        five_minute_rows
            .iter()
            .map(|row| row.datetime)
            .collect::<Vec<_>>(),
        vec![start_ns, start_ns + 5 * MINUTE_NS]
    );
    assert_eq!(
        five_minute_rows
            .iter()
            .map(|row| row.volume)
            .collect::<Vec<_>>(),
        vec![5, 5]
    );
}

#[tokio::test]
async fn cache_only_minute_query_reuses_immutable_snapshot_after_active_metadata_moves() {
    let root = temp_dir("minute-historical-metadata-snapshot");
    let symbol = "KQ.i@SHFE.au";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + MINUTE_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let initial = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: start_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns,
                end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    let initial_snapshot = MinuteKlineCacheSnapshot::new(
        initial.schema_version,
        initial.snapshot_hash.clone(),
        initial.session.snapshot_hash(),
    )
    .unwrap();
    MinuteKlineCache::open(&root)
        .unwrap()
        .store_final_range(
            symbol,
            start_ns,
            end_ns,
            &initial_snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();

    let advanced = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            captured_at_ns: start_ns.saturating_add(1),
            snapshot_hash: String::new(),
            ..initial.clone()
        })
        .unwrap();
    assert_ne!(initial.snapshot_hash, advanced.snapshot_hash);

    let collected = cache_only_client(&root)
        .query(BacktestHistoryRequest::kline(
            1,
            symbol,
            Duration::from_secs(60),
            start_ns,
            end_ns,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Klines { rows, .. } = collected.rows else {
        panic!("60-second request must return Kline rows");
    };
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1]);
}

#[test]
fn rolling_minute_metadata_extension_preserves_compatible_cached_prefix() {
    let root = temp_dir("minute-compatible-metadata-extension");
    let symbol = "KQ.m@SHFE.au";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let cached_end_ns = start_ns + MINUTE_NS;
    let extended_end_ns = cached_end_ns + MINUTE_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let initial = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: start_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns: cached_end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au2602".to_string(),
                start_ns,
                end_ns: cached_end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    let initial_snapshot = MinuteKlineCacheSnapshot::new(
        initial.schema_version,
        initial.snapshot_hash.clone(),
        initial.session.snapshot_hash(),
    )
    .unwrap();
    let cache = MinuteKlineCache::open(&root).unwrap();
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
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            captured_at_ns: cached_end_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns: extended_end_ns,
            }],
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au2602".to_string(),
                start_ns,
                end_ns: extended_end_ns,
            }],
            snapshot_hash: String::new(),
            ..initial.clone()
        })
        .unwrap();
    assert_ne!(initial.snapshot_hash, extended.snapshot_hash);

    let resolved = resolve_minute_cache_metadata_snapshot(&root, symbol, start_ns, extended_end_ns)
        .unwrap()
        .expect("extended metadata should remain compatible with the cached prefix");
    assert_eq!(resolved.snapshot_hash, extended.snapshot_hash);
    let resolved_snapshot = MinuteKlineCacheSnapshot::new(
        resolved.schema_version,
        resolved.snapshot_hash,
        resolved.session.snapshot_hash(),
    )
    .unwrap();
    let status = cache
        .inspect(symbol, start_ns, extended_end_ns, &resolved_snapshot)
        .unwrap();
    assert_eq!(status.cached_ranges, vec![(start_ns, cached_end_ns)]);
    assert_eq!(
        status.missing_ranges,
        vec![(cached_end_ns, extended_end_ns)]
    );

    cache
        .store_final_range(
            symbol,
            cached_end_ns,
            extended_end_ns,
            &resolved_snapshot,
            &[kline(2, cached_end_ns, 11.0)],
        )
        .unwrap();
    let status = cache
        .inspect(symbol, start_ns, extended_end_ns, &resolved_snapshot)
        .unwrap();
    assert!(status.is_complete());
    assert_eq!(
        cache
            .read_range(symbol, start_ns, extended_end_ns, &resolved_snapshot)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let changed_calendar = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            captured_at_ns: extended_end_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: false,
                start_ns,
                end_ns: extended_end_ns,
            }],
            snapshot_hash: String::new(),
            ..extended
        })
        .unwrap();
    let changed_calendar_snapshot = MinuteKlineCacheSnapshot::new(
        changed_calendar.schema_version,
        changed_calendar.snapshot_hash,
        changed_calendar.session.snapshot_hash(),
    )
    .unwrap();
    let error = cache
        .inspect(
            symbol,
            start_ns,
            extended_end_ns,
            &changed_calendar_snapshot,
        )
        .expect_err("a trading-day change must invalidate cached minute coverage");
    assert!(
        error
            .to_string()
            .contains("calendar/session snapshot mismatch"),
        "{error}"
    );
}

#[tokio::test]
async fn remote_on_miss_minute_query_uses_a_complete_historical_snapshot_without_auth() {
    let root = temp_dir("minute-historical-metadata-without-auth");
    let symbol = "KQ.i@SHFE.au";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + MINUTE_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let initial = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
            market_kind: BacktestHistoryMarketKind::Futures,
            logical_symbol: symbol.to_string(),
            captured_at_ns: start_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-05".to_string(),
                is_trading_day: true,
                start_ns,
                end_ns,
            }],
            session: KlineSessionTemplate::cst_trading_day(),
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns,
                end_ns,
            }],
            snapshot_hash: String::new(),
        })
        .unwrap();
    let initial_snapshot = MinuteKlineCacheSnapshot::new(
        initial.schema_version,
        initial.snapshot_hash.clone(),
        initial.session.snapshot_hash(),
    )
    .unwrap();
    MinuteKlineCache::open(&root)
        .unwrap()
        .store_final_range(
            symbol,
            start_ns,
            end_ns,
            &initial_snapshot,
            &[kline(1, start_ns, 10.0)],
        )
        .unwrap();

    let later_start_ns = end_ns + MINUTE_NS;
    metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            captured_at_ns: later_start_ns,
            trading_days: vec![BacktestHistoryTradingDay {
                date: "2026-01-06".to_string(),
                is_trading_day: true,
                start_ns: later_start_ns,
                end_ns: later_start_ns + MINUTE_NS,
            }],
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: symbol.to_string(),
                start_ns: later_start_ns,
                end_ns: later_start_ns + MINUTE_NS,
            }],
            snapshot_hash: String::new(),
            ..initial.clone()
        })
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let collected = BacktestHistoryClient::builder(root)
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_provider(CountingAuthProvider {
            calls: Arc::clone(&calls),
        })
        .blocking_workers(1)
        .build()
        .unwrap()
        .query(BacktestHistoryRequest::kline(
            1,
            symbol,
            Duration::from_secs(60),
            start_ns,
            end_ns,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Klines { rows, .. } = collected.rows else {
        panic!("60-second request must return Kline rows");
    };
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), vec![1]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn minute_stale_repair_targets_only_months_stale_against_the_active_snapshot() {
    let root = temp_dir("minute-stale-repair");
    let symbol = "KQ.m@SHFE.au";
    let january_start = utc_ns(2020, 1, 2, 1, 0, 0);
    let february_start = utc_ns(2020, 2, 3, 1, 0, 0);
    let march_start = utc_ns(2020, 3, 2, 1, 0, 0);
    let end_ns = march_start + MINUTE_NS;
    let metadata_cache = BacktestHistoryMetadataCache::open(&root).unwrap();
    let snapshot_body = BacktestHistoryMetadataSnapshot {
        schema_version: BACKTEST_HISTORY_METADATA_SCHEMA_VERSION,
        market_kind: BacktestHistoryMarketKind::Futures,
        logical_symbol: symbol.to_string(),
        captured_at_ns: january_start,
        trading_days: vec![BacktestHistoryTradingDay {
            date: "2020-01-02".to_string(),
            is_trading_day: true,
            start_ns: january_start,
            end_ns,
        }],
        session: KlineSessionTemplate::cst_trading_day(),
        physical_segments: vec![BacktestHistoryPhysicalSegment {
            physical_symbol: symbol.to_string(),
            start_ns: january_start,
            end_ns,
        }],
        snapshot_hash: String::new(),
    };
    let retained = metadata_cache
        .store_snapshot(snapshot_body.clone())
        .unwrap();
    let retained_snapshot = MinuteKlineCacheSnapshot::new(
        retained.schema_version,
        retained.snapshot_hash.clone(),
        retained.session.snapshot_hash(),
    )
    .unwrap();
    let cache = MinuteKlineCache::open(&root).unwrap();
    for (id, start_ns) in [(1, january_start), (2, february_start)] {
        cache
            .store_final_range(
                symbol,
                start_ns,
                start_ns + MINUTE_NS,
                &retained_snapshot,
                &[kline(id, start_ns, 10.0)],
            )
            .unwrap();
    }

    let outlier = metadata_cache
        .store_snapshot(BacktestHistoryMetadataSnapshot {
            captured_at_ns: january_start + 1,
            physical_segments: vec![BacktestHistoryPhysicalSegment {
                physical_symbol: "SHFE.au9999".to_string(),
                start_ns: january_start,
                end_ns,
            }],
            snapshot_hash: String::new(),
            ..snapshot_body.clone()
        })
        .unwrap();
    let outlier_snapshot = MinuteKlineCacheSnapshot::new(
        outlier.schema_version,
        outlier.snapshot_hash.clone(),
        outlier.session.snapshot_hash(),
    )
    .unwrap();
    cache
        .store_final_range(
            symbol,
            march_start,
            end_ns,
            &outlier_snapshot,
            &[kline(3, march_start, 10.0)],
        )
        .unwrap();

    let repair = plan_minute_cache_stale_partition_repair(&root, symbol, january_start, end_ns)
        .unwrap()
        .expect("mixed cache snapshots should produce an explicit repair plan");
    assert_eq!(repair.snapshot_hash, outlier.snapshot_hash);
    assert_eq!(repair.stale_ranges.len(), 2);
    assert!(
        repair
            .stale_ranges
            .iter()
            .any(|range| range.0 <= january_start)
    );
    assert!(
        repair
            .stale_ranges
            .iter()
            .any(|range| range.0 <= february_start)
    );
    assert!(
        repair
            .stale_ranges
            .iter()
            .all(|range| range.1 <= march_start)
    );

    for (range_start_ns, range_end_ns) in repair.stale_ranges {
        cache
            .purge_range(symbol, range_start_ns, range_end_ns)
            .unwrap();
    }
    assert!(!cache.month_file_path(symbol, "202001").exists());
    assert!(!cache.month_file_path(symbol, "202002").exists());
    assert!(cache.month_file_path(symbol, "202003").exists());
    let resolved = resolve_minute_cache_metadata_snapshot(&root, symbol, january_start, end_ns)
        .unwrap()
        .expect("the active snapshot should read the remaining months");
    assert_eq!(resolved.snapshot_hash, outlier.snapshot_hash);
    assert!(
        cache
            .inspect(
                symbol,
                january_start,
                end_ns,
                &MinuteKlineCacheSnapshot::new(
                    resolved.schema_version,
                    resolved.snapshot_hash,
                    resolved.session.snapshot_hash(),
                )
                .unwrap(),
            )
            .is_ok()
    );
}

#[tokio::test]
async fn a_cache_miss_fails_only_its_request_and_does_not_cancel_a_cache_hit() {
    let root = temp_dir("batch-isolation");
    let symbol = "SHFE.au2608";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + 30 * SECOND_NS;
    let day = backtest_tick_trading_day_for_timestamp_ns(start_ns).unwrap();
    let day_range = backtest_tick_trading_day_range(day).unwrap();
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(
            symbol,
            day_range.start_ns,
            day_range.end_ns,
            vec![tick(1, start_ns, 100.0, 100, 10)],
        )
        .unwrap();

    let collected = cache_only_client(&root)
        .query_batch([
            BacktestHistoryRequest::tick(1, symbol, start_ns, end_ns),
            BacktestHistoryRequest::kline(
                2,
                symbol,
                Duration::from_secs(60),
                start_ns,
                start_ns + MINUTE_NS,
            ),
        ])
        .await
        .unwrap()
        .collect_all(8 * 1024 * 1024)
        .await
        .unwrap();

    assert_eq!(collected.completed.len(), 1);
    assert_eq!(collected.completed[0].request.request_id, 1);
    assert_eq!(collected.failed.len(), 1);
    assert_eq!(collected.failed[0].request_id, 2);
}

#[tokio::test]
async fn remote_on_miss_cache_hit_without_metadata_does_not_load_authentication() {
    let root = temp_dir("remote-cache-hit-without-metadata");
    let symbol = "KQ.i@SHFE.au";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + 30 * SECOND_NS;
    let day = backtest_tick_trading_day_for_timestamp_ns(start_ns).unwrap();
    let day_range = backtest_tick_trading_day_range(day).unwrap();
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(
            symbol,
            day_range.start_ns,
            day_range.end_ns,
            vec![tick(1, start_ns, 100.0, 1, 1)],
        )
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let collected = BacktestHistoryClient::builder(root)
        .policy(BacktestHistoryPolicy::RemoteOnMiss)
        .auth_provider(CountingAuthProvider {
            calls: Arc::clone(&calls),
        })
        .build()
        .unwrap()
        .query(BacktestHistoryRequest::tick(1, symbol, start_ns, end_ns))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(collected.request.rows, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn collection_limit_includes_the_retained_request_allocation() {
    let root = temp_dir("collect-limit");
    let symbol = "SHFE.au2608";
    let start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let end_ns = start_ns + SECOND_NS;
    let day = backtest_tick_trading_day_for_timestamp_ns(start_ns).unwrap();
    let day_range = backtest_tick_trading_day_range(day).unwrap();
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(
            symbol,
            day_range.start_ns,
            day_range.end_ns,
            vec![tick(1, start_ns, 100.0, 1, 1)],
        )
        .unwrap();
    let empty_request_bytes = BacktestHistoryRows::Ticks(Vec::new())
        .estimated_heap_bytes()
        .unwrap();

    let error = cache_only_client(&root)
        .query(BacktestHistoryRequest::tick(1, symbol, start_ns, end_ns))
        .await
        .unwrap()
        .collect_all(empty_request_bytes)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        tqsdk_data::DataError::CollectLimitExceeded {
            limit_bytes,
            attempted_bytes,
        } if limit_bytes == empty_request_bytes && attempted_bytes > limit_bytes
    ));
}

#[tokio::test]
async fn kq_main_tick_query_clamps_warmup_to_its_persisted_physical_segment() {
    let root = temp_dir("kq-main-segment");
    let logical_symbol = "KQ.m@SHFE.au";
    let physical_symbol = "SHFE.au2608";
    let segment_start_ns = utc_ns(2026, 1, 5, 1, 0, 0);
    let segment_end_ns = segment_start_ns + MINUTE_NS;
    BacktestHistoryMetadataCache::open(&root)
        .unwrap()
        .store_snapshot(support::snapshot(
            logical_symbol,
            segment_start_ns,
            vec![support::segment(
                physical_symbol,
                segment_start_ns,
                segment_end_ns,
            )],
        ))
        .unwrap();
    BacktestTickCache::open(&root)
        .unwrap()
        .store_ticks(
            physical_symbol,
            segment_start_ns,
            segment_end_ns,
            vec![
                tick(1, segment_start_ns + 10 * SECOND_NS, 100.0, 10, 1),
                tick(2, segment_start_ns + 20 * SECOND_NS, 101.0, 11, 2),
            ],
        )
        .unwrap();

    let collected = cache_only_client(&root)
        .query(BacktestHistoryRequest::tick(
            1,
            logical_symbol,
            segment_start_ns + 10 * SECOND_NS,
            segment_start_ns + 30 * SECOND_NS,
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let BacktestHistoryRows::Ticks(rows) = collected.rows else {
        panic!("KQ.m Tick query must return Tick rows");
    };
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(collected.request.physical_segments.len(), 1);
    assert_eq!(
        collected.request.physical_segments[0].physical_symbol,
        physical_symbol
    );
    assert_eq!(
        collected.request.coverage.expanded_source_range,
        (segment_start_ns, segment_start_ns + 30 * SECOND_NS)
    );
}

fn cache_only_client(root: &std::path::Path) -> BacktestHistoryClient {
    BacktestHistoryClient::builder(root.to_path_buf())
        .policy(BacktestHistoryPolicy::CacheOnly)
        .blocking_workers(1)
        .build()
        .unwrap()
}

fn tick(id: i64, datetime: i64, last_price: f64, volume: i64, open_interest: i64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        volume,
        open_interest,
        ..Tick::default()
    }
}

fn kline(id: i64, datetime: i64, price: f64) -> Kline {
    Kline {
        id,
        datetime,
        open: price,
        high: price + 0.5,
        low: price - 0.5,
        close: price + 0.25,
        volume: 1,
        open_oi: id,
        close_oi: id + 1,
        ..Kline::default()
    }
}

fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "tqsdk-backtest-history-query-{name}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

struct CountingAuthProvider {
    calls: Arc<AtomicUsize>,
}

impl BacktestHistoryAuthProvider for CountingAuthProvider {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = tqsdk_data::Result<BacktestHistoryCredentials>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(BacktestHistoryCredentials::new("unused", "unused"))
        })
    }
}
