use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    BacktestTickCache, HistorySeriesCache, MinuteKlineCache, MinuteKlineCacheSnapshot,
};
use tqsdk_task::{
    BacktestMarketStream, HistoryBacktestKlineRequest, HistoryBacktestMinuteKlineSource,
    HistoryBacktestMinuteKlineUnderlyingSegment, HistoryBacktestProjectedReplayRequest,
    HistoryBacktestReplayRequest, HistoryBacktestReplayStream, HistoryBacktestSyntheticKlineSource,
    HistoryBacktestTickSource, HistoryTickReplayStream, MinuteKlineSessionTemplate,
    ReplayMarketPayload,
};

const MINUTE_NS: i64 = 60_000_000_000;
const SUB_MINUTE_NS: i64 = 15_000_000_000;

#[tokio::test]
async fn history_backtest_replay_tick_only_matches_tick_stream_order() {
    let dir = temp_dir("tick-only-order");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 4_000, [tick(2, 2_000, 102.0, 2)])
        .unwrap();
    cache
        .store_ticks("DCE.i2601", 1_000, 4_000, [tick(1, 1_000, 101.0, 1)])
        .unwrap();

    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache: HistorySeriesCache::open(&dir).unwrap(),
        start_ns: 1_000,
        end_ns: 4_000,
        tick_symbols: vec!["SHFE.rb2601".to_string(), "DCE.i2601".to_string()],
        native_klines: Vec::new(),
        synthetic_klines: Vec::new(),
    })
    .unwrap();

    let first = stream.next_event().await.unwrap().unwrap();
    let second = stream.next_event().await.unwrap().unwrap();

    assert_eq!(first.symbol(), "DCE.i2601");
    assert_eq!(second.symbol(), "SHFE.rb2601");
    assert!(matches!(first.payload(), ReplayMarketPayload::Tick(_)));
    assert!(stream.next_event().await.unwrap().is_none());

    let mut tick_stream = HistoryTickReplayStream::new(
        HistorySeriesCache::open(&dir).unwrap(),
        [
            tqsdk_data::TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 4_000),
            tqsdk_data::TickDataSeriesRequest::new("DCE.i2601", 1_000, 4_000),
        ],
    )
    .unwrap();
    assert_eq!(
        tick_stream.next_event().await.unwrap().unwrap().symbol(),
        "DCE.i2601"
    );
    assert_eq!(
        tick_stream.next_event().await.unwrap().unwrap().symbol(),
        "SHFE.rb2601"
    );
}

#[tokio::test]
async fn history_backtest_replay_projects_shared_physical_ticks_under_main_contract() {
    let dir = temp_dir("projected-main-contract");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let main = "KQ.m@SHFE.au";
    let physical = "SHFE.au2608";
    cache
        .store_ticks(physical, 1_000, 2_000, [tick(1, 1_000, 500.0, 1)])
        .unwrap();

    let mut stream =
        HistoryBacktestReplayStream::new_projected(HistoryBacktestProjectedReplayRequest {
            cache: HistorySeriesCache::open(&dir).unwrap(),
            start_ns: 1_000,
            end_ns: 2_000,
            tick_sources: vec![
                HistoryBacktestTickSource {
                    replay_symbol: main.to_string(),
                    cache_symbol: physical.to_string(),
                    start_ns: 1_000,
                    end_ns: 2_000,
                },
                HistoryBacktestTickSource {
                    replay_symbol: physical.to_string(),
                    cache_symbol: physical.to_string(),
                    start_ns: 1_000,
                    end_ns: 2_000,
                },
            ],
            native_klines: Vec::new(),
            synthetic_kline_sources: Vec::new(),
            minute_kline_sources: Vec::new(),
        })
        .unwrap();

    let mut events = Vec::new();
    while let Some(event) = stream.next_event().await.unwrap() {
        events.push(event);
    }

    assert_eq!(events.len(), 2);
    let main_event = events
        .iter()
        .find(|event| event.symbol() == main)
        .expect("main-contract replay event");
    assert_eq!(main_event.underlying_symbol(), Some(physical));
    let physical_event = events
        .iter()
        .find(|event| event.symbol() == physical)
        .expect("physical-contract replay event");
    assert_eq!(physical_event.underlying_symbol(), None);
}

#[tokio::test]
async fn history_backtest_replay_synthesizes_main_contract_kline_from_physical_ticks() {
    let dir = temp_dir("projected-main-contract-synthetic");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let main = "KQ.m@SHFE.au";
    let physical = "SHFE.au2608";
    cache
        .store_ticks(physical, 1_000, 2_000, [tick(1, 1_000, 500.0, 1)])
        .unwrap();

    let mut stream =
        HistoryBacktestReplayStream::new_projected(HistoryBacktestProjectedReplayRequest {
            cache: HistorySeriesCache::open(&dir).unwrap(),
            start_ns: 1_000,
            end_ns: 2_000,
            tick_sources: Vec::new(),
            native_klines: Vec::new(),
            synthetic_kline_sources: vec![HistoryBacktestSyntheticKlineSource {
                tick_source: HistoryBacktestTickSource {
                    replay_symbol: main.to_string(),
                    cache_symbol: physical.to_string(),
                    start_ns: 1_000,
                    end_ns: 2_000,
                },
                duration_ns: SUB_MINUTE_NS,
            }],
            minute_kline_sources: Vec::new(),
        })
        .unwrap();

    let event = stream.next_event().await.unwrap().unwrap();
    assert_eq!(event.symbol(), main);
    assert_eq!(event.underlying_symbol(), Some(physical));
    assert!(matches!(
        event.payload(),
        ReplayMarketPayload::Kline {
            duration_ns: SUB_MINUTE_NS,
            row
        } if row.close == 500.0
    ));
}

#[tokio::test]
async fn projected_synthetic_klines_prime_volume_without_replaying_pre_request_ticks() {
    let dir = temp_dir("projected-synthetic-priming");
    let cache = BacktestTickCache::open(&dir).unwrap();
    let trading_day = chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
    let day_range = tqsdk_data::backtest_tick_trading_day_range(trading_day).unwrap();
    let request_start_ns = day_range.start_ns + SUB_MINUTE_NS;
    let request_end_ns = request_start_ns + 2_000_000_000;
    cache
        .store_ticks(
            "SHFE.rb2601",
            day_range.start_ns,
            request_end_ns,
            [
                tick(1, day_range.start_ns + 1_000_000_000, 100.0, 5),
                tick(2, request_start_ns + 1_000_000_000, 101.0, 9),
            ],
        )
        .unwrap();

    let mut stream =
        HistoryBacktestReplayStream::new_projected(HistoryBacktestProjectedReplayRequest {
            cache: HistorySeriesCache::open(&dir).unwrap(),
            start_ns: request_start_ns,
            end_ns: request_end_ns,
            tick_sources: Vec::new(),
            native_klines: Vec::new(),
            synthetic_kline_sources: vec![HistoryBacktestSyntheticKlineSource {
                tick_source: HistoryBacktestTickSource {
                    replay_symbol: "SHFE.rb2601".to_string(),
                    cache_symbol: "SHFE.rb2601".to_string(),
                    start_ns: day_range.start_ns,
                    end_ns: request_end_ns,
                },
                duration_ns: SUB_MINUTE_NS,
            }],
            minute_kline_sources: Vec::new(),
        })
        .unwrap();

    let event = stream.next_event().await.unwrap().unwrap();
    assert_eq!(event.event_time_ns(), request_start_ns + 1_000_000_000);
    assert!(matches!(
        event.payload(),
        ReplayMarketPayload::Kline {
            duration_ns: SUB_MINUTE_NS,
            row,
        } if row.open == 100.0 && row.close == 101.0 && row.volume == 4
    ));
    assert!(stream.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn history_backtest_replay_emits_sub_minute_synthetic_klines_from_ticks() {
    let dir = temp_dir("synthetic-fifteen");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks(
            "SHFE.rb2601",
            1_000,
            3_000,
            [tick(1, 1_000, 101.0, 10), tick(2, 2_000, 102.0, 12)],
        )
        .unwrap();

    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache: HistorySeriesCache::open(&dir).unwrap(),
        start_ns: 1_000,
        end_ns: 3_000,
        tick_symbols: Vec::new(),
        native_klines: Vec::new(),
        synthetic_klines: vec![HistoryBacktestKlineRequest {
            symbol: "SHFE.rb2601".to_string(),
            duration_ns: SUB_MINUTE_NS,
        }],
    })
    .unwrap();

    let first = stream.next_event().await.unwrap().unwrap();
    let second = stream.next_event().await.unwrap().unwrap();

    assert_eq!(first.source(), "history-cache-synth-kline");
    assert_eq!(first.event_time_ns(), 1_000);
    assert!(matches!(
        first.payload(),
        ReplayMarketPayload::Kline {
            duration_ns: SUB_MINUTE_NS,
            row
        } if row.close == 101.0
    ));
    assert_eq!(second.event_time_ns(), 2_000);
    assert!(matches!(
        second.payload(),
        ReplayMarketPayload::Kline { row, .. } if row.close == 102.0 && row.volume == 12
    ));
    assert!(stream.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn history_backtest_replay_emits_native_klines_above_one_minute() {
    let dir = temp_dir("native-sixty-one");
    let history = HistorySeriesCache::open(&dir).unwrap();
    history
        .write_kline_range(
            "SHFE.rb2601",
            61_000_000_000,
            0,
            122_000_000_000,
            &[kline(1, 0, 100.0, 105.0, 99.0, 104.0)],
        )
        .unwrap();

    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache: HistorySeriesCache::open(&dir).unwrap(),
        start_ns: 0,
        end_ns: 122_000_000_000,
        tick_symbols: Vec::new(),
        native_klines: vec![HistoryBacktestKlineRequest {
            symbol: "SHFE.rb2601".to_string(),
            duration_ns: 61_000_000_000,
        }],
        synthetic_klines: Vec::new(),
    })
    .unwrap();

    let open = stream.next_event().await.unwrap().unwrap();
    let close = stream.next_event().await.unwrap().unwrap();

    assert_eq!(open.source(), "history-cache-native-kline-open");
    assert_eq!(open.event_time_ns(), 0);
    assert!(matches!(
        open.payload(),
        ReplayMarketPayload::Kline { row, .. } if row.open == 100.0 && row.close == 100.0 && row.volume == 0
    ));
    assert_eq!(close.source(), "history-cache-native-kline-close");
    assert_eq!(close.event_time_ns(), 61_000_000_000);
    assert!(matches!(
        close.payload(),
        ReplayMarketPayload::Kline { row, .. } if row.close == 104.0 && row.volume == 10
    ));
    assert!(stream.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn history_backtest_replay_streams_canonical_and_aggregated_minute_klines() {
    let dir = temp_dir("canonical-minute");
    let minute_cache = MinuteKlineCache::open(&dir).unwrap();
    let snapshot =
        MinuteKlineCacheSnapshot::new(1, "calendar-fixture", "cst-trading-day-v1").unwrap();
    let start_ns = 0;
    let end_ns = 6 * MINUTE_NS;
    let rows = (0..5)
        .map(|index| {
            kline(
                index + 1,
                index * MINUTE_NS,
                100.0 + index as f64,
                101.0 + index as f64,
                99.0 + index as f64,
                100.5 + index as f64,
            )
        })
        .collect::<Vec<_>>();
    minute_cache
        .store_final_range("SHFE.rb2601", start_ns, end_ns, &snapshot, &rows)
        .unwrap();

    let source = |duration_ns| HistoryBacktestMinuteKlineSource {
        cache: minute_cache.clone(),
        snapshot: snapshot.clone(),
        replay_symbol: "SHFE.rb2601".to_string(),
        cache_symbol: "SHFE.rb2601".to_string(),
        start_ns,
        end_ns,
        duration_ns,
        session: MinuteKlineSessionTemplate::cst_trading_day(),
        underlying_segments: Vec::new(),
    };
    let mut stream =
        HistoryBacktestReplayStream::new_projected(HistoryBacktestProjectedReplayRequest {
            cache: HistorySeriesCache::open(&dir).unwrap(),
            start_ns,
            end_ns,
            tick_sources: Vec::new(),
            native_klines: Vec::new(),
            synthetic_kline_sources: Vec::new(),
            minute_kline_sources: vec![source(MINUTE_NS), source(5 * MINUTE_NS)],
        })
        .unwrap();

    let mut canonical = Vec::new();
    let mut aggregate = Vec::new();
    while let Some(event) = stream.next_event().await.unwrap() {
        match event.payload() {
            ReplayMarketPayload::Kline {
                duration_ns, row, ..
            } if *duration_ns == MINUTE_NS => canonical.push((
                event.source().to_string(),
                event.event_time_ns(),
                row.clone(),
            )),
            ReplayMarketPayload::Kline {
                duration_ns, row, ..
            } if *duration_ns == 5 * MINUTE_NS => aggregate.push((
                event.source().to_string(),
                event.event_time_ns(),
                row.clone(),
            )),
            _ => {}
        }
    }

    assert_eq!(canonical.len(), 10);
    assert_eq!(canonical[0].0, "history-cache-minute-kline-open");
    assert_eq!(canonical[0].1, start_ns);
    assert_eq!(canonical[0].2.volume, 0);
    assert_eq!(canonical[1].0, "history-cache-minute-kline-close");
    assert_eq!(canonical[1].1, start_ns + MINUTE_NS);
    assert_eq!(canonical[1].2.close, 100.5);

    assert_eq!(aggregate.len(), 6);
    assert_eq!(aggregate[0].0, "history-cache-minute-kline-aggregate-open");
    assert_eq!(aggregate[0].1, start_ns);
    assert_eq!(aggregate[0].2.volume, 0);
    assert_eq!(aggregate[1].1, start_ns + MINUTE_NS);
    assert_eq!(aggregate[1].2.volume, 10);
    assert_eq!(aggregate.last().unwrap().1, start_ns + 5 * MINUTE_NS);
    assert_eq!(aggregate.last().unwrap().2.volume, 50);
    assert_eq!(aggregate.last().unwrap().2.high, 105.0);
    assert_eq!(aggregate.last().unwrap().2.low, 99.0);
}

#[tokio::test]
async fn history_backtest_replay_applies_continuous_mapping_to_logical_minute_cache() {
    let dir = temp_dir("logical-main-minute");
    let minute_cache = MinuteKlineCache::open(&dir).unwrap();
    let snapshot =
        MinuteKlineCacheSnapshot::new(1, "calendar-fixture", "cst-trading-day-v1").unwrap();
    let symbol = "KQ.m@SHFE.au";
    let start_ns = 0;
    let end_ns = 3 * MINUTE_NS;
    minute_cache
        .store_final_range(
            symbol,
            start_ns,
            end_ns,
            &snapshot,
            &[
                kline(1, start_ns, 500.0, 501.0, 499.0, 500.5),
                kline(2, start_ns + MINUTE_NS, 600.0, 601.0, 599.0, 600.5),
            ],
        )
        .unwrap();

    let mut stream =
        HistoryBacktestReplayStream::new_projected(HistoryBacktestProjectedReplayRequest {
            cache: HistorySeriesCache::open(&dir).unwrap(),
            start_ns,
            end_ns,
            tick_sources: Vec::new(),
            native_klines: Vec::new(),
            synthetic_kline_sources: Vec::new(),
            minute_kline_sources: vec![HistoryBacktestMinuteKlineSource {
                cache: minute_cache,
                snapshot,
                replay_symbol: symbol.to_string(),
                cache_symbol: symbol.to_string(),
                start_ns,
                end_ns,
                duration_ns: MINUTE_NS,
                session: MinuteKlineSessionTemplate::cst_trading_day(),
                underlying_segments: vec![
                    HistoryBacktestMinuteKlineUnderlyingSegment {
                        start_ns,
                        end_ns: start_ns + MINUTE_NS,
                        underlying_symbol: "SHFE.au2608".to_string(),
                    },
                    HistoryBacktestMinuteKlineUnderlyingSegment {
                        start_ns: start_ns + MINUTE_NS,
                        end_ns,
                        underlying_symbol: "SHFE.au2610".to_string(),
                    },
                ],
            }],
        })
        .unwrap();

    let first_open = stream.next_event().await.unwrap().unwrap();
    let first_close = stream.next_event().await.unwrap().unwrap();
    let second_open = stream.next_event().await.unwrap().unwrap();
    assert_eq!(first_open.underlying_symbol(), Some("SHFE.au2608"));
    assert_eq!(first_close.underlying_symbol(), Some("SHFE.au2608"));
    assert_eq!(second_open.underlying_symbol(), Some("SHFE.au2610"));
}

#[tokio::test]
async fn history_backtest_replay_merges_tick_synthetic_and_native_events_by_time() {
    let dir = temp_dir("mixed-order");
    let tick_cache = BacktestTickCache::open(&dir).unwrap();
    tick_cache
        .store_ticks(
            "SHFE.rb2601",
            0,
            122_000_000_000,
            [tick(1, 2_000, 101.0, 10)],
        )
        .unwrap();
    let history = HistorySeriesCache::open(&dir).unwrap();
    history
        .write_kline_range(
            "DCE.i2601",
            61_000_000_000,
            0,
            122_000_000_000,
            &[kline(1, 0, 200.0, 205.0, 199.0, 204.0)],
        )
        .unwrap();

    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache: HistorySeriesCache::open(&dir).unwrap(),
        start_ns: 0,
        end_ns: 122_000_000_000,
        tick_symbols: vec!["SHFE.rb2601".to_string()],
        native_klines: vec![HistoryBacktestKlineRequest {
            symbol: "DCE.i2601".to_string(),
            duration_ns: 61_000_000_000,
        }],
        synthetic_klines: vec![HistoryBacktestKlineRequest {
            symbol: "SHFE.rb2601".to_string(),
            duration_ns: SUB_MINUTE_NS,
        }],
    })
    .unwrap();

    let mut times = Vec::new();
    while let Some(event) = stream.next_event().await.unwrap() {
        times.push(event.event_time_ns());
    }

    assert_eq!(times, vec![0, 2_000, 2_000, 61_000_000_000]);
}

#[tokio::test]
async fn history_backtest_replay_does_not_persist_synthetic_klines() {
    let dir = temp_dir("synthetic-not-persisted");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 3_000, [tick(1, 1_000, 101.0, 10)])
        .unwrap();

    let mut stream = HistoryBacktestReplayStream::new(HistoryBacktestReplayRequest {
        cache: HistorySeriesCache::open(&dir).unwrap(),
        start_ns: 1_000,
        end_ns: 3_000,
        tick_symbols: Vec::new(),
        native_klines: Vec::new(),
        synthetic_klines: vec![HistoryBacktestKlineRequest {
            symbol: "SHFE.rb2601".to_string(),
            duration_ns: SUB_MINUTE_NS,
        }],
    })
    .unwrap();
    assert!(stream.next_event().await.unwrap().is_some());

    let coverage = HistorySeriesCache::open(&dir)
        .unwrap()
        .kline_coverage("SHFE.rb2601", SUB_MINUTE_NS, 1_000, 3_000)
        .unwrap();
    assert_eq!(coverage.missing_ranges, vec![(1_000, 3_000)]);
}

fn tick(id: i64, datetime: i64, last_price: f64, volume: i64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        volume,
        ..Tick::default()
    }
}

fn kline(id: i64, datetime: i64, open: f64, high: f64, low: f64, close: f64) -> Kline {
    Kline {
        id,
        datetime,
        open,
        high,
        low,
        close,
        volume: 10,
        open_oi: 20,
        close_oi: 21,
        ..Kline::default()
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "tqsdk-history-backtest-replay-{name}-{}-{unique}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
