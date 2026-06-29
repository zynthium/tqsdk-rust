use tqsdk_core::Tick;
use tqsdk_data::{BacktestTickCache, HistorySeriesCache, TickDataSeriesRequest};
use tqsdk_task::{BacktestMarketStream, HistoryTickReplayStream, ReplayMarketPayload};

#[tokio::test]
async fn history_tick_replay_merges_symbols_by_datetime_and_tick_id() {
    let dir = temp_dir("history-tick-replay");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .store_ticks("SHFE.rb2601", 1_000, 4_000, [tick(2, 2_000, 102.0)])
        .unwrap();
    cache
        .store_ticks("DCE.i2601", 1_000, 4_000, [tick(1, 1_000, 101.0)])
        .unwrap();

    let mut stream = HistoryTickReplayStream::new(
        HistorySeriesCache::open(&dir).unwrap(),
        [
            TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 4_000),
            TickDataSeriesRequest::new("DCE.i2601", 1_000, 4_000),
        ],
    )
    .unwrap();

    let first = stream.next_event().await.unwrap().unwrap();
    let second = stream.next_event().await.unwrap().unwrap();
    assert_eq!(first.symbol(), "DCE.i2601");
    assert_eq!(second.symbol(), "SHFE.rb2601");
    assert!(matches!(first.payload(), ReplayMarketPayload::Tick(_)));
    assert!(stream.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn history_tick_replay_requires_complete_cache_coverage() {
    let dir = temp_dir("history-tick-replay-incomplete");
    let cache = BacktestTickCache::open(&dir).unwrap();
    cache
        .append_partial_ticks("SHFE.rb2601", [tick(1, 1_000, 101.0)])
        .unwrap();

    let result = HistoryTickReplayStream::new(
        HistorySeriesCache::open(&dir).unwrap(),
        [TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 4_000)],
    );

    assert!(result.is_err());
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ..Tick::default()
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "tqsdk-history-tick-replay-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
