use tqsdk_core::{Kline, Tick};
use tqsdk_data::{BacktestTickCache, HistorySeriesCache};
use tqsdk_task::{
    BacktestMarketStream, HistoryBacktestKlineRequest, HistoryBacktestProjectedReplayRequest,
    HistoryBacktestReplayRequest, HistoryBacktestReplayStream, HistoryBacktestSyntheticKlineSource,
    HistoryBacktestTickSource, HistoryTickReplayStream, ReplayMarketPayload,
};

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
                duration_ns: 60_000_000_000,
            }],
        })
        .unwrap();

    let event = stream.next_event().await.unwrap().unwrap();
    assert_eq!(event.symbol(), main);
    assert_eq!(event.underlying_symbol(), Some(physical));
    assert!(matches!(
        event.payload(),
        ReplayMarketPayload::Kline {
            duration_ns: 60_000_000_000,
            row
        } if row.close == 500.0
    ));
}

#[tokio::test]
async fn history_backtest_replay_emits_sixty_second_synthetic_klines_from_ticks() {
    let dir = temp_dir("synthetic-sixty");
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
            duration_ns: 60_000_000_000,
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
            duration_ns: 60_000_000_000,
            row
        } if row.close == 101.0
    ));
    assert_eq!(second.event_time_ns(), 2_000);
    assert!(matches!(
        second.payload(),
        ReplayMarketPayload::Kline { row, .. } if row.close == 102.0 && row.volume == 2
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
            duration_ns: 60_000_000_000,
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
            duration_ns: 60_000_000_000,
        }],
    })
    .unwrap();
    assert!(stream.next_event().await.unwrap().is_some());

    let coverage = HistorySeriesCache::open(&dir)
        .unwrap()
        .kline_coverage("SHFE.rb2601", 60_000_000_000, 1_000, 3_000)
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
