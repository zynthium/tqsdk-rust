use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tqsdk::advanced::task::{HistoryBacktestTickSource, ReplayMarketEvent, ReplayMarketSource};
use tqsdk::prelude::*;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote,
    RuntimeHandle, RuntimeInput, Symbol, Tick,
};
use tqsdk_data::{MinuteKlineCache, MinuteKlineCacheSnapshot, TickDataSeriesRequest};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::{MinuteKlineAggregator, MinuteKlineSessionTemplate};

static NEXT_TEMP_CACHE_DIR_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn prelude_exposes_default_strategy_surface() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<TqBuilder>();

    let _builder = Tq::futures().auth("demo-user", "demo-pass");
    let _: Option<Tq> = None;
    let _: Option<QuoteRef> = None;
    let _: Option<QuoteSet> = None;
    let _: Option<TargetPos> = None;
    let _: Option<MarketCachePolicy> = None;
    let _: Option<RecordTicksHealth> = None;
    let _: Option<RecordTicksFlushReport> = None;
    let _: Option<RecordTicksReport> = None;
    let _: Option<BacktestRemoteFillInspectionProgress> = None;
    let _: Option<BacktestRemoteFillTelemetry> = None;
}

#[tokio::test]
async fn facade_replay_backtest_accepts_empty_replay() {
    let replay = ReplayMarketSource::new(vec![]);
    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .connect()
        .await
        .unwrap();
    assert!(!tq.next().await.unwrap());
}

#[tokio::test]
async fn facade_backtest_cache_mode_requires_declared_symbols() {
    let cache_dir = temp_cache_dir();
    let result = Tq::futures()
        .backtest(1_000, 61_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .cache_only()
        .prepare()
        .await;
    let err = match result {
        Ok(_) => panic!("cache-backed backtest unexpectedly prepared without symbols"),
        Err(error) => error,
    };

    assert!(
        err.to_string().contains("at least one symbol"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn facade_backtest_cache_only_missing_canonical_minute_is_read_only() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let result = Tq::futures()
        .backtest(0, 60_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .cache_only()
        .kline(symbol, std::time::Duration::from_secs(60), 2)
        .unwrap()
        .prepare()
        .await;

    let err = match result {
        Ok(_) => panic!("missing canonical minute cache unexpectedly prepared"),
        Err(error) => error,
    };
    assert!(
        err.to_string()
            .contains("canonical 60-second Kline cache coverage is incomplete"),
        "unexpected error: {err}"
    );
    assert!(
        !cache_dir.join("minute-kline-v3").exists(),
        "cache-only inspection must not create the minute-K namespace"
    );
}

#[tokio::test]
async fn facade_history_cache_operations_include_canonical_minutes() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let minute_cache = MinuteKlineCache::open(&cache_dir).unwrap();
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    minute_cache
        .store_final_range(
            symbol,
            0,
            120_000_000_000,
            &snapshot,
            &[
                minute_kline(1, 0, 100.0),
                minute_kline(2, 60_000_000_000, 101.0),
            ],
        )
        .unwrap();

    let builder = Tq::futures()
        .backtest(0, 120_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .cache_only()
        .kline(symbol, std::time::Duration::from_secs(60), 2)
        .unwrap();

    let statuses = builder.inspect_history_cache().await.unwrap();
    assert!(matches!(
        &statuses[..],
        [tqsdk::BacktestHistoryCacheStatus::MinuteKline(status)]
            if status.symbol == symbol && status.missing_ranges.is_empty()
    ));

    let purges = builder.purge_history_cache().await.unwrap();
    assert!(matches!(
        &purges[..],
        [tqsdk::BacktestHistoryCachePurgeReport::MinuteKline(report)]
            if report.symbol == symbol && report.removed_files == 1
    ));
    assert!(
        !minute_cache.month_file_path(symbol, "197001").exists(),
        "range purge must remove the intersecting monthly minute partition"
    );
}

#[tokio::test]
async fn facade_backtest_cache_mode_replays_cached_ticks() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let mut tq = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 100.0);
    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 101.0);
    assert!(!tq.next().await.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn facade_backtest_uses_default_history_cache_when_cache_dir_is_omitted() {
    static HISTORY_CACHE_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    let _lock = HISTORY_CACHE_ENV_LOCK.lock().await;
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let _env = EnvVarGuard::set("TQSDK_HISTORY_CACHE_DIR", &cache_dir);
    let cache = BacktestTickCache::open(tqsdk_data::default_history_cache_dir()).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .symbol(symbol)
        .cache_only()
        .prepare()
        .await
        .unwrap();

    assert_eq!(prepared.data_report().cache_dir, cache_dir);
    assert!(!prepared.data_report().remote_used);
}

#[tokio::test]
async fn facade_backtest_universe_accepts_static_selector_expression() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(symbol, 1_000, 3_000, [tick(1, 1_000, 100.0)])
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .universe("symbol:SHFE.rb2501")
        .unwrap()
        .cache_only()
        .prepare()
        .await
        .unwrap();

    assert_eq!(prepared.data_report().resolved_symbols, 1);
    assert!(!prepared.data_report().remote_used);
    assert_eq!(
        prepared.tick_sources(),
        &[HistoryBacktestTickSource {
            replay_symbol: symbol.to_string(),
            cache_symbol: symbol.to_string(),
            start_ns: 1_000,
            end_ns: 3_000,
        }]
    );
}

#[tokio::test]
async fn facade_quotes_universe_accepts_static_selector_expression() {
    let symbol = "SHFE.rb2601";
    let mut tq = Tq::from_api(manual_tq_api());

    let quotes = tq
        .quotes_universe(format!("symbol:{symbol}"))
        .await
        .unwrap();

    assert_eq!(quotes.symbols().collect::<Vec<_>>(), vec![symbol]);
}

#[tokio::test]
async fn facade_backtest_remote_on_miss_requires_auth_only_when_cache_missing() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let prepared = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap();
    assert!(!prepared.data_report().remote_used);

    let missing = match Tq::futures()
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
    {
        Ok(_) => panic!("remote-on-miss unexpectedly prepared with missing cache and no auth"),
        Err(error) => error,
    };
    assert!(
        missing
            .to_string()
            .contains("remote backtest cache fill requires auth")
    );
}

#[tokio::test]
async fn facade_backtest_remote_on_miss_prepare_marks_remote_used_when_cache_missing() {
    let cache_dir = temp_cache_dir();
    let prepared = Tq::futures()
        .auth("demo-user", "demo-pass")
        .backtest(1_000, 4_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol("SHFE.rb2601")
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .prepare()
        .await
        .unwrap();

    assert!(prepared.data_report().remote_used);
    assert_eq!(prepared.data_report().resolved_symbols, 1);
}

#[tokio::test]
#[ignore = "live server-backtest regression; requires TQ_AUTH_USER/TQ_AUTH_PASS"]
async fn server_backtest_fills_final_canonical_minutes_for_an_index_symbol() {
    let Ok(user) = std::env::var("TQ_AUTH_USER") else {
        return;
    };
    let Ok(pass) = std::env::var("TQ_AUTH_PASS") else {
        return;
    };
    let cache_dir = temp_cache_dir();
    let start_ns = 1_577_872_800_000_000_000_i64;
    let end_ns = 1_577_959_200_000_000_000_i64;

    let warmup = Tq::futures()
        .auth(user, pass)
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .remote_on_miss()
        .remote_fill_config(
            BacktestRemoteFillConfig::default()
                .with_symbol_batch_size(1)
                .with_symbol_concurrency(1)
                .with_idle_timeout(Duration::from_secs(30))
                .with_batch_timeout(Some(Duration::from_secs(90))),
        )
        .kline("KQ.i@SHFE.au", Duration::from_secs(60), 1)
        .unwrap()
        .warmup()
        .await
        .expect("server backtest should produce final 60-second Kline coverage");

    assert!(warmup.remote_minute_kline_used);
    assert_eq!(warmup.minute_kline_symbols.len(), 1);
    assert!(warmup.minute_kline_symbols[0].after.is_complete());

    let rows = MinuteKlineCache::open(&cache_dir)
        .unwrap()
        .read_range(
            "KQ.i@SHFE.au",
            start_ns,
            end_ns,
            &MinuteKlineCacheSnapshot::cst_v1(),
        )
        .unwrap();
    assert!(
        rows.len() >= 100,
        "expected a substantive closed-day minute series"
    );
    assert!(
        rows.iter()
            .all(|row| row.datetime >= start_ns && row.datetime < end_ns)
    );
    assert!(
        rows.windows(2)
            .all(|pair| pair[0].datetime < pair[1].datetime)
    );
}

#[tokio::test]
#[ignore = "live server-backtest protocol probe; requires TQ_AUTH_USER/TQ_AUTH_PASS"]
async fn server_backtest_can_ready_a_canonical_minute_chart_without_a_tick_driver() {
    let Ok(user) = std::env::var("TQ_AUTH_USER") else {
        return;
    };
    let Ok(pass) = std::env::var("TQ_AUTH_PASS") else {
        return;
    };
    let start_ns = 1_577_872_800_000_000_000_i64;
    let end_ns = 1_577_959_200_000_000_000_i64;
    let mut api = tqsdk_wait::TqApiBuilder::new(user, pass)
        .futures_backtest(start_ns, end_ns)
        .unwrap()
        .backtest_cache_fill_mode()
        .build()
        .await
        .unwrap();
    let handle = api
        .kline("KQ.i@SHFE.au", Duration::from_secs(60), 10_000)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while !handle.is_ready().unwrap() {
        if api.step_until(Some(deadline)).await.unwrap().is_none()
            && tokio::time::Instant::now() >= deadline
        {
            panic!("server-side 60-second Kline chart did not become ready without a tick driver");
        }
    }

    assert!(handle.has_rows().unwrap());
}

#[tokio::test]
#[ignore = "live canonical-minute acceptance; requires TQ_AUTH_USER/TQ_AUTH_PASS"]
async fn canonical_minute_aggregation_matches_remote_index_klines() {
    let Ok(user) = std::env::var("TQ_AUTH_USER") else {
        return;
    };
    let Ok(pass) = std::env::var("TQ_AUTH_PASS") else {
        return;
    };
    // A closed trading day avoids provisional last bars and gives both the
    // SHFE and CFFEX index series enough 5m/15m bars to expose an anchoring,
    // OHLC, volume, or open-interest aggregation error.
    let start_ns = 1_577_872_800_000_000_000_i64;
    let end_ns = 1_577_959_200_000_000_000_i64;
    let durations = [Duration::from_secs(5 * 60), Duration::from_secs(15 * 60)];

    for symbol in ["KQ.i@SHFE.au", "KQ.i@CFFEX.IF"] {
        let cache_dir = temp_cache_dir();
        Tq::futures()
            .auth(user.clone(), pass.clone())
            .backtest(start_ns, end_ns)
            .cache_dir(&cache_dir)
            .unwrap()
            .remote_on_miss()
            .remote_fill_config(
                BacktestRemoteFillConfig::default()
                    .with_symbol_batch_size(1)
                    .with_symbol_concurrency(1)
                    .with_idle_timeout(Duration::from_secs(30))
                    .with_batch_timeout(Some(Duration::from_secs(90))),
            )
            .kline(symbol, durations[0], 1)
            .unwrap()
            .kline(symbol, durations[1], 1)
            .unwrap()
            .warmup()
            .await
            .unwrap_or_else(|error| panic!("canonical-minute warmup failed for {symbol}: {error}"));

        let minutes = MinuteKlineCache::open(&cache_dir)
            .unwrap()
            .read_range(
                symbol,
                start_ns,
                end_ns,
                &MinuteKlineCacheSnapshot::cst_v1(),
            )
            .unwrap();
        assert!(
            minutes.len() >= 100,
            "expected a substantive canonical-minute series for {symbol}, got {} rows",
            minutes.len()
        );

        let remote = collect_remote_index_klines(
            user.as_str(),
            pass.as_str(),
            symbol,
            start_ns,
            end_ns,
            durations.as_slice(),
        )
        .await;
        for duration in durations {
            let duration_ns = i64::try_from(duration.as_nanos()).unwrap();
            let local = aggregate_final_minutes(&minutes, duration_ns, end_ns);
            let remote = remote
                .get(&duration_ns)
                .expect("all requested remote Kline periods are collected");
            assert_local_aggregate_matches_remote(symbol, duration_ns, &local, remote);
        }
    }
}

#[tokio::test]
async fn facade_record_ticks_writes_live_ticks_to_backtest_cache() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let mut tq = Tq::from_api(manual_tq_api());

    let report = tq.record_ticks(&cache_dir, [symbol]).await.unwrap();
    assert_eq!(report.symbols, vec![symbol.to_string()]);
    assert_eq!(report.cache_dir, cache_dir);

    seed_ready_record_tick_chart(&tq, symbol);
    assert!(tq.next().await.unwrap());
    let health = tq
        .record_ticks_health()
        .expect("record ticks health should be available");
    assert_eq!(health.flush_count, 1);
    assert_eq!(health.total_appended_rows, 2);
    assert!(!health.gap_detected);
    assert_eq!(health.symbols[0].last_seen_id, Some(201));
    assert_eq!(health.symbols[0].total_appended_rows, 2);

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let series = cache
        .load_series(TickDataSeriesRequest::new(
            symbol,
            1_713_660_000_000_000_000,
            1_713_660_000_500_000_001,
        ))
        .unwrap();
    assert_eq!(
        series.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![200, 201]
    );
}

#[tokio::test]
async fn facade_record_ticks_only_processes_changed_tick_subscription() {
    let first_symbol = "SHFE.rb2601";
    let second_symbol = "DCE.m2601";
    let cache_dir = temp_cache_dir();
    let start_ns = 1_713_660_000_000_000_000_i64;
    let mut tq = Tq::from_api(manual_tq_api());

    tq.record_ticks(&cache_dir, [first_symbol, second_symbol])
        .await
        .unwrap();
    seed_record_tick_chart_rows(&tq, first_symbol, [tick(200, start_ns, 618.0)]);
    assert!(tq.next().await.unwrap());
    seed_record_tick_chart_rows(&tq, second_symbol, [tick(300, start_ns, 2698.0)]);
    assert!(tq.next().await.unwrap());

    seed_record_tick_chart_rows(
        &tq,
        first_symbol,
        [tick(202, start_ns + 500_000_000, 619.0)],
    );
    assert!(tq.next().await.unwrap());

    let health = tq
        .record_ticks_health()
        .expect("record ticks health should be available");
    assert_eq!(health.total_appended_rows, 3);
    assert_eq!(health.symbols[0].total_appended_rows, 2);
    assert_eq!(health.symbols[1].total_appended_rows, 1);
    assert!(health.symbols[0].gap_detected);
    assert!(!health.symbols[1].gap_detected);

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let first_initial = cache
        .load_series(TickDataSeriesRequest::new(
            first_symbol,
            start_ns,
            start_ns + 1,
        ))
        .unwrap();
    let first_gap_tail = cache
        .load_series(TickDataSeriesRequest::new(
            first_symbol,
            start_ns + 500_000_000,
            start_ns + 500_000_001,
        ))
        .unwrap();
    let second = cache
        .load_series(TickDataSeriesRequest::new(
            second_symbol,
            start_ns,
            start_ns + 1,
        ))
        .unwrap();
    assert_eq!(
        first_initial.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![200]
    );
    assert_eq!(
        first_gap_tail.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![202]
    );
    assert_eq!(
        second.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![300]
    );
}

#[tokio::test]
async fn facade_record_ticks_rescans_after_a_write_failure() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let start_ns = 1_713_660_000_000_000_000_i64;
    let mut tq = Tq::from_api(manual_tq_api());

    tq.record_ticks(&cache_dir, [symbol]).await.unwrap();
    seed_record_tick_chart_rows(&tq, symbol, [tick(200, start_ns, 618.0)]);
    assert!(tq.next().await.unwrap());

    let invalid_rows = (201_i64..329)
        .map(|id| {
            let datetime = if id == 201 {
                start_ns - 1
            } else {
                start_ns + id
            };
            tick(id, datetime, 619.0)
        })
        .collect::<Vec<_>>();
    seed_record_tick_chart_rows(&tq, symbol, invalid_rows);
    let error = tq.next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("live tick cache writer tick datetime moved backwards")
    );

    seed_record_tick_chart_rows(&tq, symbol, [tick(201, start_ns + 1, 619.0)]);
    assert!(tq.next().await.unwrap());

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let series = cache
        .load_series(TickDataSeriesRequest::new(symbol, start_ns, start_ns + 2))
        .unwrap();
    assert_eq!(
        series.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![200, 201]
    );
}

#[tokio::test]
async fn facade_record_ticks_buffers_a_contiguous_tail_until_drop() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let start_ns = 1_713_660_000_000_000_000_i64;
    let end_ns = start_ns + 500_000_001;

    {
        let mut tq = Tq::from_api(manual_tq_api());
        tq.record_ticks(&cache_dir, [symbol]).await.unwrap();

        seed_record_tick_chart_rows(&tq, symbol, [tick(200, start_ns, 618.0)]);
        assert!(tq.next().await.unwrap());
        seed_record_tick_chart_rows(&tq, symbol, [tick(201, end_ns - 1, 619.0)]);
        assert!(tq.next().await.unwrap());

        let health = tq
            .record_ticks_health()
            .expect("record ticks health should be available");
        assert_eq!(health.total_appended_rows, 1);

        let cache = BacktestTickCache::open(&cache_dir).unwrap();
        let coverage = cache.coverage(symbol, start_ns, end_ns).unwrap();
        assert_eq!(coverage.cached_ranges, vec![(start_ns, start_ns + 1)]);
        assert_eq!(coverage.missing_ranges, vec![(start_ns + 1, end_ns)]);
    }

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let series = cache
        .load_series(TickDataSeriesRequest::new(symbol, start_ns, end_ns))
        .unwrap();
    assert_eq!(
        series.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![200, 201]
    );
}

#[tokio::test]
async fn facade_record_ticks_health_reports_live_tick_gaps() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let mut tq = Tq::from_api(manual_tq_api());

    tq.record_ticks(&cache_dir, [symbol]).await.unwrap();

    seed_record_tick_chart_rows(&tq, symbol, [tick(200, 1_713_660_000_000_000_000, 618.0)]);
    assert!(tq.next().await.unwrap());

    seed_record_tick_chart_rows(&tq, symbol, [tick(202, 1_713_660_000_500_000_000, 619.0)]);
    assert!(tq.next().await.unwrap());

    let health = tq
        .record_ticks_health()
        .expect("record ticks health should be available");
    assert_eq!(health.flush_count, 2);
    assert_eq!(health.total_appended_rows, 2);
    assert!(health.gap_detected);
    assert_eq!(health.symbols[0].last_seen_id, Some(202));
    assert!(health.symbols[0].gap_detected);

    let last_flush = health
        .last_flush
        .as_ref()
        .expect("latest flush report should be retained");
    assert_eq!(last_flush.appended_rows, 1);
    assert!(last_flush.gap_detected);
    assert_eq!(last_flush.symbols[0].last_seen_id, Some(202));
}

#[tokio::test]
async fn facade_record_ticks_health_can_seed_gap_warmup_policy() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let start_ns = 1_713_660_000_000_000_000_i64;
    let end_ns = 1_713_660_000_500_000_001_i64;
    let mut tq = Tq::from_api(manual_tq_api());

    tq.record_ticks(&cache_dir, [symbol]).await.unwrap();
    seed_record_tick_chart_rows(&tq, symbol, [tick(200, start_ns, 618.0)]);
    assert!(tq.next().await.unwrap());
    seed_record_tick_chart_rows(&tq, symbol, [tick(202, end_ns - 1, 619.0)]);
    assert!(tq.next().await.unwrap());

    let policy = tq
        .recorded_market_cache_policy()
        .expect("active recording should expose a cache policy");
    assert_eq!(policy.cache_dir(), cache_dir.as_path());
    assert_eq!(policy.tick_symbols(), &[symbol.to_string()]);

    let report = Tq::futures()
        .market_cache(policy)
        .backtest(start_ns, end_ns)
        .cache_only()
        .warmup()
        .await
        .unwrap();

    assert_eq!(report.symbols_total, 1);
    assert_eq!(report.symbols_missing, 1);
    assert_eq!(
        report.symbols[0].before.missing_ranges,
        vec![(start_ns + 1, end_ns - 1)]
    );
}

#[tokio::test]
async fn facade_market_cache_policy_starts_live_tick_recording() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let mut tq = Tq::from_api(manual_tq_api());

    let report = tq
        .start_market_cache(MarketCachePolicy::new(&cache_dir).record_ticks([symbol]))
        .await
        .unwrap()
        .expect("non-empty market cache policy should start recording");
    assert_eq!(report.symbols, vec![symbol.to_string()]);
    assert_eq!(
        tq.record_ticks_report(),
        Some(&RecordTicksReport {
            cache_dir: cache_dir.clone(),
            symbols: vec![symbol.to_string()],
            data_length: 10_000,
        })
    );

    seed_ready_record_tick_chart(&tq, symbol);
    assert!(tq.next().await.unwrap());
    let health = tq
        .record_ticks_health()
        .expect("record ticks health should be available");
    assert_eq!(health.total_appended_rows, 2);
    assert!(!health.gap_detected);

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let series = cache
        .load_series(TickDataSeriesRequest::new(
            symbol,
            1_713_660_000_000_000_000,
            1_713_660_000_500_000_001,
        ))
        .unwrap();
    assert_eq!(
        series.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![200, 201]
    );
}

#[tokio::test]
async fn facade_market_cache_policy_records_static_universe() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let mut tq = Tq::from_api(manual_tq_api());

    let report = tq
        .start_market_cache(
            MarketCachePolicy::new(&cache_dir)
                .record_universe(format!("symbol:{symbol}"))
                .unwrap(),
        )
        .await
        .unwrap()
        .expect("static universe market cache policy should start recording");

    assert_eq!(report.symbols, vec![symbol.to_string()]);
}

#[tokio::test]
async fn facade_market_cache_policy_supplies_backtest_cache_inputs() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let mut tq = Tq::futures()
        .market_cache(MarketCachePolicy::new(&cache_dir).record_ticks([symbol]))
        .backtest(1_000, 3_000)
        .cache_only()
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 100.0);
    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().last_price, 101.0);
    assert!(!tq.next().await.unwrap());
}

#[tokio::test]
async fn facade_market_cache_policy_universe_supplies_backtest_cache_inputs() {
    let symbol = "SHFE.rb2501";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let prepared = Tq::futures()
        .market_cache(
            MarketCachePolicy::new(&cache_dir)
                .record_universe(format!("symbol:{symbol}"))
                .unwrap(),
        )
        .backtest(1_000, 3_000)
        .cache_only()
        .prepare()
        .await
        .unwrap();

    assert_eq!(prepared.data_report().resolved_symbols, 1);
    assert!(!prepared.data_report().remote_used);
}

#[test]
fn facade_backtest_builder_inspects_and_purges_explicit_symbol_cache() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let builder = Tq::futures()
        .backtest(1_000, 5_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol);

    let statuses = builder.inspect_cache().unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].symbol, symbol);
    assert!(!statuses[0].is_complete());
    assert_eq!(statuses[0].cached_ranges, vec![(1_000, 3_000)]);
    assert_eq!(statuses[0].missing_ranges, vec![(3_000, 5_000)]);
    assert!(statuses[0].series_path_exists);

    let purged = builder.purge_cache_symbols().unwrap();
    assert_eq!(purged.len(), 1);
    assert!(purged[0].removed);
    assert!(purged[0].removed_files > 0);
}

#[tokio::test]
async fn facade_backtest_refresh_purges_symbol_cache_before_remote_fill() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    cache
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let prepared = Tq::futures()
        .auth("demo-user", "demo-pass")
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .refresh()
        .prepare()
        .await
        .unwrap();

    assert!(prepared.data_report().remote_used);
    let coverage = cache.coverage(symbol, 1_000, 3_000).unwrap();
    assert_eq!(coverage.cached_ranges, Vec::<(i64, i64)>::new());
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

#[tokio::test]
async fn facade_backtest_warmup_skips_complete_cache_without_auth() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();

    let report = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .remote_on_miss()
        .warmup()
        .await
        .unwrap();

    assert_eq!(report.symbols_total, 1);
    assert_eq!(report.symbols_skipped, 1);
    assert_eq!(report.symbols_missing, 0);
    assert_eq!(report.symbols_filled, 0);
    assert_eq!(report.rows_written, 0);
    assert!(!report.remote_used);
    assert_eq!(report.symbols[0].symbol, symbol);
    assert_eq!(
        report.symbols[0].action,
        BacktestCacheWarmupAction::SkippedComplete
    );
    assert!(report.symbols[0].after.is_complete());
}

#[tokio::test]
async fn facade_backtest_warmup_emits_a_cache_inspected_remote_fill_plan() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    BacktestTickCache::open(&cache_dir)
        .unwrap()
        .store_ticks(
            symbol,
            1_000,
            3_000,
            [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
        )
        .unwrap();
    let observed_events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&observed_events);

    let report = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .remote_on_miss()
        .on_remote_fill_telemetry(move |event| {
            callback_events.lock().unwrap().push(event.clone());
        })
        .warmup()
        .await
        .unwrap();

    let events = observed_events.lock().unwrap().clone();
    assert!(!report.remote_used);
    assert_eq!(events.len(), 2);
    let inspection = &events[0];
    assert_eq!(inspection.phase(), BacktestRemoteFillPhase::Inspecting);
    assert_eq!(inspection.physical_symbol(), Some(symbol));
    assert_eq!(inspection.requested_range(), Some((1_000, 3_000)));
    let inspection_progress = inspection.inspection_progress().unwrap();
    assert_eq!(inspection_progress.total_ranges(), 1);
    assert_eq!(inspection_progress.checked_ranges(), 1);
    assert_eq!(inspection_progress.complete_ranges(), 1);
    assert_eq!(inspection_progress.incomplete_ranges(), 0);

    let plan = events[1].plan().unwrap();
    assert_eq!(events[1].phase(), BacktestRemoteFillPhase::PlanReady);
    assert!(events[1].inspection_progress().is_none());
    assert_eq!(plan.requested_range(), (1_000, 3_000));
    assert_eq!(plan.logical_symbols(), &[symbol.to_string()]);
    assert_eq!(plan.logical_batches(), 0);
    assert_eq!(plan.physical_symbols().len(), 1);
    assert_eq!(plan.physical_symbols()[0].physical_symbol(), symbol);
    assert!(plan.physical_symbols()[0].missing_ranges().is_empty());
}

#[tokio::test]
async fn facade_backtest_warmup_reports_every_parallel_cache_inspection() {
    let symbols = ["DCE.m2601", "SHFE.rb2601"];
    let cache_dir = temp_cache_dir();
    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    for symbol in symbols {
        cache
            .store_ticks(
                symbol,
                1_000,
                3_000,
                [tick(1, 1_000, 100.0), tick(2, 2_000, 101.0)],
            )
            .unwrap();
    }
    let observed_events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&observed_events);

    let report = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbols[0])
        .symbol(symbols[1])
        .remote_on_miss()
        .on_remote_fill_telemetry(move |event| {
            callback_events.lock().unwrap().push(event.clone());
        })
        .warmup()
        .await
        .unwrap();

    assert_eq!(report.symbols_total, symbols.len());
    assert_eq!(report.symbols_skipped, symbols.len());
    let events = observed_events.lock().unwrap().clone();
    let mut inspection_counts = events
        .iter()
        .filter_map(|event| event.inspection_progress())
        .map(|inspection| inspection.checked_ranges())
        .collect::<Vec<_>>();
    inspection_counts.sort_unstable();
    assert_eq!(inspection_counts, vec![1, 2]);
    assert!(events.iter().all(|event| {
        event
            .inspection_progress()
            .is_none_or(|inspection| inspection.total_ranges() == symbols.len())
    }));
    let plan = events.last().and_then(|event| event.plan()).unwrap();
    assert_eq!(plan.physical_symbols().len(), symbols.len());
    assert!(!plan.requires_remote_fill());
}

#[tokio::test]
async fn facade_backtest_warmup_cache_only_reports_missing_ranges() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();

    let report = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache_only()
        .warmup()
        .await
        .unwrap();

    assert_eq!(report.symbols_total, 1);
    assert_eq!(report.symbols_skipped, 0);
    assert_eq!(report.symbols_missing, 1);
    assert_eq!(report.symbols_filled, 0);
    assert!(!report.remote_used);
    assert_eq!(
        report.symbols[0].action,
        BacktestCacheWarmupAction::MissingCacheOnly
    );
    assert_eq!(
        report.symbols[0].before.missing_ranges,
        vec![(1_000, 3_000)]
    );
    assert_eq!(report.symbols[0].after.missing_ranges, vec![(1_000, 3_000)]);
}

#[tokio::test]
async fn facade_backtest_warmup_reports_missing_canonical_minutes_in_remote_fill_plan() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let observed_events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&observed_events);

    let report = Tq::futures()
        .backtest(0, 60_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .cache_only()
        .kline(symbol, Duration::from_secs(60), 1)
        .unwrap()
        .on_remote_fill_telemetry(move |event| {
            callback_events.lock().unwrap().push(event.clone());
        })
        .warmup()
        .await
        .unwrap();

    let events = observed_events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].phase(), BacktestRemoteFillPhase::PlanReady);
    let plan = events[0].plan().unwrap();
    assert_eq!(plan.logical_symbols(), &[symbol.to_string()]);
    assert_eq!(plan.logical_batches(), 1);
    assert!(plan.requires_remote_fill());
    assert!(matches!(
        plan.physical_symbols(),
        [minute] if minute.physical_symbol() == symbol
            && minute.requested_ranges() == [(0, 60_000_000_000)]
            && minute.missing_ranges() == [(0, 60_000_000_000)]
    ));
    assert!(matches!(
        &report.minute_kline_symbols[..],
        [minute] if minute.symbol == symbol
            && minute.action == BacktestCacheWarmupAction::MissingCacheOnly
    ));
}

#[tokio::test]
async fn facade_backtest_warmup_keeps_tick_and_canonical_minute_batches_distinct() {
    let symbol = "SHFE.rb2601";
    let cache_dir = temp_cache_dir();
    let observed_events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&observed_events);

    Tq::futures()
        .backtest(0, 60_000_000_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache_only()
        .kline(symbol, Duration::from_secs(60), 1)
        .unwrap()
        .on_remote_fill_telemetry(move |event| {
            callback_events.lock().unwrap().push(event.clone());
        })
        .warmup()
        .await
        .unwrap();

    let events = observed_events.lock().unwrap().clone();
    let plan = events.last().and_then(|event| event.plan()).unwrap();
    assert_eq!(plan.logical_batches(), 2);
    assert!(plan.requires_remote_fill());
}

#[tokio::test]
async fn facade_backtest_warmup_remote_on_miss_requires_auth_when_missing() {
    let cache_dir = temp_cache_dir();

    let error = Tq::futures()
        .backtest(1_000, 3_000)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol("SHFE.rb2601")
        .remote_on_miss()
        .warmup()
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("remote backtest cache fill requires auth"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TQ_AUTH_USER/TQ_AUTH_PASS and remote backtest service"]
async fn facade_backtest_remote_on_miss_live_smoke() {
    let user = std::env::var("TQ_AUTH_USER").unwrap();
    let pass = std::env::var("TQ_AUTH_PASS").unwrap();
    let cache_dir = temp_cache_dir();
    let symbol = "KQ.i@SHFE.au";
    let start_ns = 1_781_182_800_000_000_000;
    let end_ns = 1_781_182_860_000_000_000;

    let mut tq = Tq::futures()
        .auth(user, pass)
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .cache(BacktestCachePolicy::RemoteOnMiss)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(symbol).await.unwrap();

    assert!(tq.next().await.unwrap());
    assert!(quote.load().unwrap().last_price.is_finite());
    while tq.next().await.unwrap() {}

    let cache = BacktestTickCache::open(&cache_dir).unwrap();
    let status = cache.inspect(symbol, start_ns, end_ns).unwrap();
    assert!(status.is_complete(), "cache status: {status:?}");

    let prepared = Tq::futures()
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .remote_on_miss()
        .prepare()
        .await
        .unwrap();
    assert!(!prepared.data_report().remote_used);

    let mut cached = prepared.connect().await.unwrap();
    assert!(cached.next().await.unwrap());
}

#[tokio::test]
#[ignore = "requires TQ_AUTH_USER/TQ_AUTH_PASS and remote backtest service"]
async fn facade_backtest_warmup_remote_on_miss_live_smoke() {
    let user = std::env::var("TQ_AUTH_USER").unwrap();
    let pass = std::env::var("TQ_AUTH_PASS").unwrap();
    let cache_dir = temp_cache_dir();
    let symbol = "KQ.i@SHFE.au";
    let start_ns = 1_781_182_800_000_000_000;
    let end_ns = 1_781_182_860_000_000_000;

    let first = Tq::futures()
        .auth(user, pass)
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .remote_on_miss()
        .warmup()
        .await
        .unwrap();
    assert!(first.remote_used);
    assert_eq!(first.symbols_filled, 1);
    assert!(first.symbols[0].after.is_complete());

    let second = Tq::futures()
        .backtest(start_ns, end_ns)
        .cache_dir(&cache_dir)
        .unwrap()
        .symbol(symbol)
        .remote_on_miss()
        .warmup()
        .await
        .unwrap();
    assert!(!second.remote_used);
    assert_eq!(second.symbols_skipped, 1);
}

#[tokio::test]
async fn facade_local_backtest_accepts_instrument_specs_for_klines() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::kline(
            "fixture",
            "SHFE.rb2501",
            1_000,
            Some(1_000),
            60_000_000_000,
            Kline {
                id: 1,
                datetime: 1_000,
                open: 100.0,
                high: 105.0,
                low: 99.0,
                close: 102.0,
                volume: 10,
                ..Kline::default()
            },
        )
        .unwrap(),
    ]);

    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .instrument_spec(instrument_spec("SHFE.rb2501", 0.5, 10))
        .connect()
        .await
        .unwrap();
    let quote = tq.quote("SHFE.rb2501").await.unwrap();

    assert!(tq.next().await.unwrap());
    let quote = quote.load().unwrap();
    assert_eq!(quote.last_price, 102.0);
    assert_eq!(quote.ask_price1, 102.5);
    assert_eq!(quote.bid_price1, 101.5);
}

#[tokio::test]
async fn facade_local_backtest_target_pos_uses_underlying_for_continuous_replay_symbol() {
    let alias = "KQ.m@SHFE.rb";
    let underlying = "SHFE.rb2501";
    let replay = ReplayMarketSource::new(vec![
        quote_event(alias, 1_000, 100.0, 10, 99.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
        quote_event(alias, 2_000, 101.0, 10, 100.0, 8)
            .with_underlying_symbol(underlying)
            .unwrap(),
    ]);

    let mut tq = Tq::futures()
        .replay_backtest(replay)
        .connect()
        .await
        .unwrap();
    let quote = tq.quote(alias).await.unwrap();
    let target = tq.target_pos(LOCAL_BACKTEST_ACCOUNT_ID, alias).unwrap();
    target.set(1).unwrap();

    assert!(tq.next().await.unwrap());
    assert_eq!(quote.load().unwrap().underlying_symbol, underlying);
    assert_eq!(
        tq.position(LOCAL_BACKTEST_ACCOUNT_ID, alias)
            .load()
            .unwrap()
            .pos_long,
        1
    );
    assert_eq!(
        tq.position(LOCAL_BACKTEST_ACCOUNT_ID, underlying)
            .load()
            .unwrap()
            .pos_long,
        1
    );

    assert!(tq.next().await.unwrap());
    assert!(!tq.next().await.unwrap());

    let summary = tq.backtest_summary().unwrap();
    assert_eq!(summary.trades().len(), 1);
    assert_eq!(summary.trades()[0].exchange_id, "SHFE");
    assert_eq!(summary.trades()[0].instrument_id, "rb2501");
    let metrics = tq.backtest_performance_metrics().unwrap();
    assert_eq!(metrics.open_trade_count(), 1);
    assert_eq!(metrics.close_trade_count(), 0);
    assert_eq!(metrics.start_balance(), metrics.end_balance());
    let report = tq.backtest_performance_report(2).unwrap();
    assert_eq!(
        report.metrics().open_trade_count(),
        metrics.open_trade_count()
    );
    assert_eq!(
        report.metrics().balance_return_rate(),
        metrics.balance_return_rate()
    );
    assert_eq!(report.daily_balance_returns().len(), 1);
    assert_eq!(report.rolling_balance_sharpe_ratios().len(), 1);
    assert_eq!(target.execution_report().trades.len(), 1);

    let (event_cursor, events) = target.execution_events_since(0);
    assert!(!events.is_empty());
    assert!(target.execution_events_since(event_cursor).1.is_empty());
    let (trade_cursor, trades) = target.execution_trades_since(0);
    assert_eq!(trades.len(), 1);
    assert!(target.execution_trades_since(trade_cursor).1.is_empty());
}

#[test]
fn advanced_namespaces_keep_curated_underlying_access() {
    let _session = tqsdk::advanced::session::SessionClientBuilder::new("demo-user", "demo-pass")
        .futures_market();
    let _ = std::any::type_name::<tqsdk::advanced::session::InstrumentSpec>();
    let _ = std::any::type_name::<tqsdk::advanced::session::SymbolInfo>();
    let _data = tqsdk::advanced::data::DataClient::new();
    let _split = tqsdk::advanced::task::VolumeSplitPolicy::new(1, 2).unwrap();

    let _ = std::any::type_name::<tqsdk::advanced::runtime::RuntimeReader>();
}

#[test]
fn facade_root_exports_are_curated() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");
    let top_level_pub_uses = top_level_pub_use_statements(&source);

    for (crate_name, symbol) in [
        ("tqsdk_data", "DataClient"),
        ("tqsdk_session", "SessionClient"),
        ("tqsdk_session", "SessionClientBuilder"),
        ("tqsdk_task", "TargetPosTask"),
        ("tqsdk_task", "TaskHost"),
        ("tqsdk_wait", "TqApi"),
        ("tqsdk_wait", "TqApiBuilder"),
    ] {
        assert!(
            !top_level_pub_uses
                .iter()
                .any(|statement| pub_use_exports_symbol(statement, crate_name, symbol)),
            "facade root exports lower-level symbol directly: {crate_name}::{symbol}"
        );
    }

    for wildcard_export in [
        "pub use tqsdk_core::*",
        "pub use tqsdk_data::*",
        "pub use tqsdk_session::*",
        "pub use tqsdk_task::*",
        "pub use tqsdk_wait::*",
    ] {
        assert!(
            !source.contains(wildcard_export),
            "advanced namespace must be curated, found wildcard: {wildcard_export}"
        );
    }
}

fn top_level_pub_use_statements(source: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut depth = 0usize;
    let mut statement_start = None;

    for (index, ch) in source.char_indices() {
        if let Some(start) = statement_start {
            if ch == ';' {
                statements.push(&source[start..=index]);
                statement_start = None;
            }
            continue;
        }

        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0
                    && source[index..].starts_with("pub use")
                    && line_prefix_is_whitespace(source, index)
                {
                    statement_start = Some(index);
                }
            }
        }
    }

    statements
}

fn pub_use_exports_symbol(statement: &str, crate_name: &str, symbol: &str) -> bool {
    let normalized = statement
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != ';')
        .collect::<String>();
    let Some(exports) = normalized.strip_prefix(&format!("pubuse{crate_name}::")) else {
        return false;
    };

    if let Some(grouped_exports) = exports
        .strip_prefix('{')
        .and_then(|exports| exports.strip_suffix('}'))
    {
        return grouped_exports
            .split(',')
            .map(strip_alias)
            .any(|exported| exported == symbol);
    }

    strip_alias(exports) == symbol
}

fn line_prefix_is_whitespace(source: &str, index: usize) -> bool {
    source[..index]
        .rsplit_once('\n')
        .map_or(&source[..index], |(_previous, prefix)| prefix)
        .trim()
        .is_empty()
}

fn strip_alias(export: &str) -> &str {
    export
        .split_once("as")
        .map_or(export, |(symbol, _alias)| symbol)
}

fn instrument_spec(
    symbol: &str,
    price_tick: f64,
    volume_multiple: i64,
) -> tqsdk::advanced::session::InstrumentSpec {
    let (exchange_id, product_id) =
        symbol
            .split_once('.')
            .map_or(("", symbol), |(exchange, instrument)| {
                let product_len = instrument
                    .chars()
                    .take_while(|ch| ch.is_ascii_alphabetic())
                    .count();
                (exchange, &instrument[..product_len])
            });
    tqsdk::advanced::session::InstrumentSpec {
        symbol: Symbol::new(symbol),
        exchange_id: exchange_id.to_string(),
        product_id: product_id.to_string(),
        class: tqsdk::advanced::session::InstrumentClass::Future,
        price_tick,
        volume_multiple,
        expire_datetime_secs: None,
        underlying_symbol: None,
    }
}

fn quote_event(
    symbol: &str,
    datetime: i64,
    ask_price1: f64,
    ask_volume1: i64,
    bid_price1: f64,
    bid_volume1: i64,
) -> ReplayMarketEvent {
    ReplayMarketEvent::quote(
        "fixture",
        symbol,
        datetime,
        Some(datetime),
        Quote {
            datetime: "2026-05-15 09:30:00.000000".to_string(),
            last_price: ask_price1,
            ask_price1,
            ask_volume1,
            bid_price1,
            bid_volume1,
            ..Quote::default()
        },
    )
    .unwrap()
}

#[test]
fn facade_exposes_stock_backtest_builder_without_connecting() {
    let _backtest = Tq::stock()
        .auth("demo-user", "demo-pass")
        .backtest(0, 1)
        .disabled_cache();
    assert!(format!("{:?}", Tq::stock()).contains("market_kind: Stock"));

    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub fn stock() -> TqBuilder",
        "FacadeMarketKind::Stock => wait_builder.stock_backtest",
        "FacadeMarketKind::Stock => builder.stock_market",
    ] {
        assert!(
            source.contains(required_surface),
            "stock facade surface is missing: {required_surface}"
        );
    }
}

#[tokio::test]
async fn cache_backed_stock_backtest_is_rejected_before_any_history_fill() {
    let result = Tq::stock()
        .backtest(1_000, 2_000)
        .cache_only()
        .symbol("SSE.600000")
        .prepare()
        .await;
    let error = match result {
        Ok(_) => {
            panic!("cache-backed stock backtests must not enter the futures-only history path")
        }
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("cache-backed backtest history currently supports futures only"),
        "unexpected error: {error}"
    );
}

#[test]
fn facade_result_accepts_session_errors() {
    let error = tqsdk_session::SessionFacadeError::InvalidState("facade contract");
    let _: tqsdk::Error = error.into();
}

#[test]
fn facade_exposes_tqkq_target_helpers_instead_of_literal_account_ids() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub async fn tqkq_account_id(&self) -> Result<String>",
        "pub async fn target_pos_tqkq(&mut self, symbol: &str) -> Result<TargetPos>",
        "pub async fn target_pos_tqkq_numbered(",
        "tqkq_login_command()",
        "tqkq_login_command_numbered(number)",
    ] {
        assert!(
            source.contains(required_surface),
            "missing resolved TQKQ facade helper: {required_surface}"
        );
    }
}

#[test]
fn facade_exposes_default_account_login_helpers() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for required_surface in [
        "pub fn default_account_id(&self) -> Result<&str>",
        "pub fn account_default(&self) -> Result<tqsdk_wait::AccountRef>",
        "pub fn position_default(&self, symbol: &str) -> Result<tqsdk_wait::PositionRef>",
        "pub fn target_pos_default(&mut self, symbol: &str) -> Result<TargetPos>",
        "pub async fn login_trade_account(",
        "pub async fn login_tqkq_account(&mut self) -> Result<tqsdk_wait::AccountRef>",
        "pub fn tqkq_sim(mut self) -> Self",
        "pub fn trade_account_env(self) -> Result<Self>",
    ] {
        assert!(
            source.contains(required_surface),
            "missing default account facade helper: {required_surface}"
        );
    }
}

#[test]
fn facade_exposes_market_relay_builder_method() {
    let builder = tqsdk::Tq::futures()
        .auth("demo-user", "demo-pass")
        .market_relay("ws://127.0.0.1:7788/market")
        .trade_target_tqkq();

    let debug = format!("{builder:?}");
    assert!(debug.contains("market_url"));
}

#[test]
fn target_pos_wrapper_uses_sync_intent_api_and_no_direct_wait() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read facade source");

    for expected_surface in [
        "pub fn set(&self, volume: i64) -> Result<()>",
        "pub fn close(&self) -> Result<()>",
        "pub fn is_finished(&self) -> bool",
        "pub fn last_error(&self) -> Option<tqsdk_task::TaskError>",
        "pub fn execution_report(&self) -> tqsdk_task::TargetPosTaskExecutionReport",
    ] {
        assert!(
            source.contains(expected_surface),
            "missing explicit target-position wrapper surface: {expected_surface}"
        );
    }

    for removed_surface in [
        "pub async fn set(&mut self",
        "pub async fn close(&mut self",
        "pub async fn wait_target_reached",
    ] {
        assert!(
            !source.contains(removed_surface),
            "misleading async target-position surface remains: {removed_surface}"
        );
    }
}

#[test]
fn default_facade_contract_example_exists() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s33_default_facade.rs"
    );
    let source = std::fs::read_to_string(path).expect("read default facade example");

    for required in [
        "use tqsdk::prelude::*;",
        "Tq::futures()",
        ".auth_env()?",
        ".trade_target_tqkq()",
        "target_pos_tqkq(\"SHFE.au2602\").await?",
        "while tq.next().await?",
        "target.set(1)?",
    ] {
        assert!(
            source.contains(required),
            "default facade example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn replay_backtest_contract_example_exposes_custom_replay_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s38_facade_local_backtest.rs"
    );
    let source = std::fs::read_to_string(path).expect("read local backtest facade example");

    for required in [
        "ReplayMarketSource::new(vec![])",
        ".replay_backtest(replay)",
        ".quote_symbol(\"SHFE.au2510\")",
        ".price_tick(\"SHFE.au2510\", 0.02)",
    ] {
        assert!(
            source.contains(required),
            "local replay backtest example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn backtest_contract_example_exposes_cache_backed_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s43_facade_backtest_history_cache.rs"
    );
    let source = std::fs::read_to_string(path).expect("read cache-backed backtest example");

    for required in [
        "BacktestTickCache::open",
        "MinuteKlineCache::open",
        "MinuteKlineCacheSnapshot::cst_v1()",
        ".backtest(START_NS, END_NS)",
        ".cache_dir(&cache_dir)?",
        ".cache_only()",
        ".universe(format!(\"symbol:{SYMBOL}\"))?",
        "while tq.next().await?",
    ] {
        assert!(
            source.contains(required),
            "cache-backed backtest example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn record_ticks_contract_example_exposes_live_cache_recording_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s46_facade_record_ticks.rs"
    );
    let source = std::fs::read_to_string(path).expect("read record ticks facade example");

    for required in [
        "TQ_RUN_LIVE_RECORD_TICKS",
        ".auth_env()?",
        ".record_ticks(\".tqsdk/backtest_ticks\", [SYMBOL]).await?",
        "while tq.next().await?",
    ] {
        assert!(
            source.contains(required),
            "record ticks example missing required flow fragment: {required}"
        );
    }
}

#[test]
fn market_cache_policy_contract_example_exposes_shared_cache_flow() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/api_contract_s47_facade_market_cache_policy.rs"
    );
    let source = std::fs::read_to_string(path).expect("read market cache policy facade example");

    for required in [
        "MarketCachePolicy::new(CACHE_DIR).record_universe(format!(\"symbol:{SYMBOL}\"))?",
        ".market_cache(cache.clone())",
        ".connect()",
        "record_ticks_health()",
        "recorded_market_cache_policy()",
        ".market_cache(cache)",
        ".backtest(",
        ".cache_only()",
    ] {
        assert!(
            source.contains(required),
            "market cache policy example missing required flow fragment: {required}"
        );
    }
}

async fn collect_remote_index_klines(
    user: &str,
    pass: &str,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
    durations: &[Duration],
) -> BTreeMap<i64, Vec<Kline>> {
    let mut api = tqsdk_wait::TqApiBuilder::new(user.to_owned(), pass.to_owned())
        .futures_backtest(start_ns, end_ns)
        .unwrap()
        .backtest_cache_fill_mode()
        .build()
        .await
        .unwrap_or_else(|error| panic!("remote Kline connection failed for {symbol}: {error}"));
    let mut charts = Vec::with_capacity(durations.len());
    for duration in durations {
        let duration_ns = i64::try_from(duration.as_nanos()).unwrap();
        let chart = api
            .kline(symbol, *duration, 10_000)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "remote {}ns Kline subscription failed for {symbol}: {error}",
                    duration_ns
                )
            });
        charts.push((duration_ns, chart));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let charts_ready = charts.iter().all(|(_, chart)| chart.is_ready().unwrap());
        if charts_ready && server_backtest_history_complete(&api) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "remote Kline history did not finish for {symbol}"
        );
        api.step_until(Some(deadline))
            .await
            .unwrap_or_else(|error| panic!("remote Kline polling failed for {symbol}: {error}"));
    }

    charts
        .into_iter()
        .map(|(duration_ns, chart)| {
            let rows = chart
                .rows()
                .unwrap()
                .into_iter()
                .filter(|row| {
                    row.datetime >= start_ns
                        && row
                            .datetime
                            .checked_add(duration_ns)
                            .is_some_and(|bar_end_ns| bar_end_ns <= end_ns)
                })
                .collect();
            (duration_ns, rows)
        })
        .collect()
}

fn server_backtest_history_complete(api: &tqsdk_wait::TqApi) -> bool {
    api.session()
        .reader()
        .read_market_state()
        .get_path(&["mdhis_more_data"])
        .and_then(|value| value.as_bool())
        == Some(false)
}

fn aggregate_final_minutes(
    minutes: &[Kline],
    duration_ns: i64,
    end_ns: i64,
) -> BTreeMap<i64, Kline> {
    let mut aggregator =
        MinuteKlineAggregator::new(duration_ns, MinuteKlineSessionTemplate::cst_trading_day())
            .unwrap();
    let mut aggregated = BTreeMap::new();
    for minute in minutes {
        let Some(update) = aggregator.update(minute).unwrap() else {
            continue;
        };
        if update
            .updated
            .datetime
            .checked_add(duration_ns)
            .is_some_and(|bar_end_ns| bar_end_ns <= end_ns)
        {
            aggregated.insert(update.updated.datetime, update.updated);
        }
    }
    aggregated
}

fn assert_local_aggregate_matches_remote(
    symbol: &str,
    duration_ns: i64,
    local: &BTreeMap<i64, Kline>,
    remote: &[Kline],
) {
    let remote_by_datetime = remote
        .iter()
        .map(|row| (row.datetime, row.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        remote_by_datetime.len(),
        remote.len(),
        "remote {duration_ns}ns Kline stream has duplicate bar timestamps for {symbol}"
    );

    let local_only = local
        .keys()
        .filter(|datetime| !remote_by_datetime.contains_key(datetime))
        .copied()
        .collect::<Vec<_>>();
    let remote_only = remote_by_datetime
        .keys()
        .filter(|datetime| !local.contains_key(datetime))
        .copied()
        .collect::<Vec<_>>();
    let mismatches = local
        .iter()
        .filter_map(|(datetime, local_row)| {
            let remote_row = remote_by_datetime.get(datetime)?;
            (!same_kline_ohlcv_oi(local_row, remote_row))
                .then(|| format!("{datetime}: local={local_row:?}, remote={remote_row:?}"))
        })
        .collect::<Vec<_>>();

    eprintln!(
        "canonical-minute acceptance: symbol={symbol} duration_ns={duration_ns} local={} remote={} local_only={} remote_only={} mismatches={}",
        local.len(),
        remote_by_datetime.len(),
        local_only.len(),
        remote_only.len(),
        mismatches.len(),
    );
    assert!(
        !local.is_empty(),
        "local {duration_ns}ns aggregation produced no complete bars for {symbol}"
    );
    assert!(
        local_only.is_empty() && remote_only.is_empty(),
        "{symbol} {duration_ns}ns bar timestamps differ: local_only={:?}, remote_only={:?}",
        &local_only[..local_only.len().min(3)],
        &remote_only[..remote_only.len().min(3)],
    );
    assert!(
        mismatches.is_empty(),
        "{symbol} {duration_ns}ns local aggregation differs from remote rows: {}",
        mismatches
            .into_iter()
            .take(3)
            .collect::<Vec<_>>()
            .join(" | "),
    );
}

fn same_kline_ohlcv_oi(left: &Kline, right: &Kline) -> bool {
    same_kline_price(left.open, right.open)
        && same_kline_price(left.high, right.high)
        && same_kline_price(left.low, right.low)
        && same_kline_price(left.close, right.close)
        && left.volume == right.volume
        && left.open_oi == right.open_oi
        && left.close_oi == right.close_oi
}

fn same_kline_price(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}

fn temp_cache_dir() -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = NEXT_TEMP_CACHE_DIR_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "tqsdk-facade-contract-cache-{}-{unique}-{sequence}",
        std::process::id(),
    ))
}

struct EnvVarGuard {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(name);
        // Tests hold a process-local mutex while this override is active.
        unsafe {
            std::env::set_var(name, value);
        }
        Self { name, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }
}

fn manual_tq_api() -> tqsdk_wait::TqApi {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();

    let handle = RuntimeHandle::with_adapters(adapters);
    tqsdk_wait::TqApi::new(ManualSession::from_runtime(handle).into_client())
}

fn seed_ready_record_tick_chart(tq: &Tq, symbol: &str) {
    seed_record_tick_chart_rows(
        tq,
        symbol,
        [
            Tick {
                id: 200,
                datetime: 1_713_660_000_000_000_000_i64,
                last_price: 618.0,
                average: 618.2,
                highest: 619.0,
                lowest: 617.5,
                ask_price1: 618.2,
                ask_volume1: 4,
                bid_price1: 618.0,
                bid_volume1: 5,
                volume: 12,
                amount: 7416.0,
                open_interest: 101,
                ..Tick::default()
            },
            Tick {
                id: 201,
                datetime: 1_713_660_000_500_000_000_i64,
                last_price: 618.5,
                average: 618.3,
                highest: 619.2,
                lowest: 617.5,
                ask_price1: 618.6,
                ask_volume1: 3,
                bid_price1: 618.4,
                bid_volume1: 6,
                volume: 15,
                amount: 9277.5,
                open_interest: 102,
                ..Tick::default()
            },
        ],
    );
}

fn seed_record_tick_chart_rows(tq: &Tq, symbol: &str, rows: impl IntoIterator<Item = Tick>) {
    let rows = rows.into_iter().collect::<Vec<_>>();
    let left_id = rows.first().map_or(0, |row| row.id);
    let right_id = rows.last().map_or(-1, |row| row.id);
    let chart_id = format!("wait-tick-{}-10000", sanitize_chart_token(symbol));
    let data = rows
        .iter()
        .map(|row| (row.id.to_string(), tick_row_value(row)))
        .collect::<Map<_, _>>();
    tq.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                "ins_list": symbol,
                                "duration": 0,
                            },
                            "left_id": left_id,
                            "right_id": right_id,
                            "more_data": false,
                            "ready": true,
                        }
                    },
                    "ticks": {
                        symbol: {
                            "data": data
                        }
                    }
                }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed ready record tick chart should produce a commit");
}

fn tick_row_value(row: &Tick) -> Value {
    json!({
        "id": row.id,
        "datetime": row.datetime,
        "last_price": row.last_price,
        "average": row.average,
        "highest": row.highest,
        "lowest": row.lowest,
        "ask_price1": row.ask_price1,
        "ask_volume1": row.ask_volume1,
        "bid_price1": row.bid_price1,
        "bid_volume1": row.bid_volume1,
        "volume": row.volume,
        "amount": row.amount,
        "open_interest": row.open_interest,
    })
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ask_price1: last_price + 0.5,
        ask_volume1: 1,
        bid_price1: last_price - 0.5,
        bid_volume1: 1,
        ..Tick::default()
    }
}

fn minute_kline(id: i64, datetime: i64, close: f64) -> Kline {
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
