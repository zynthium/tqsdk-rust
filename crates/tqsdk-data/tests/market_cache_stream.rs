#![cfg(feature = "stream")]

use futures::stream;
use tqsdk_core::{
    CommitResult, CommitScope, Kline, ProtocolDomain, Quote, Revision, SharedCommitResult, Tick,
};
use tqsdk_data::{
    MarketCachePayload, MarketCacheReader, MarketCacheStreamWriter, MarketCacheWriter,
};
use tqsdk_stream::{KlineWindow, MarketEvent, TickWindow, ValueUpdate};

#[tokio::test(flavor = "current_thread")]
async fn market_cache_stream_writer_pipes_quote_events() {
    let path = std::env::temp_dir().join("tqsdk-market-cache-stream-test.jsonl");
    let _ = std::fs::remove_file(&path);
    let writer = MarketCacheWriter::create(&path).unwrap();
    let mut cache = MarketCacheStreamWriter::new("live", writer).unwrap();
    let quote = Quote {
        instrument_id: "SHFE.au2602".to_string(),
        last_price: 480.0,
        ..Quote::default()
    };

    let written = cache
        .pipe_market_events(
            stream::iter([Ok(MarketEvent::Quote(ValueUpdate {
                commit: market_commit(7),
                value: quote,
            }))]),
            Some(1),
        )
        .await
        .unwrap();

    assert_eq!(written, 1);
    let events: Vec<_> = MarketCacheReader::open(&path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source, "live");
    assert_eq!(events[0].symbol, "SHFE.au2602");
    assert!(matches!(events[0].payload, MarketCachePayload::Quote(_)));

    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "current_thread")]
async fn market_cache_stream_writer_records_latest_kline_and_tick_window_rows() {
    let path = std::env::temp_dir().join("tqsdk-market-cache-window-stream-test.jsonl");
    let _ = std::fs::remove_file(&path);
    let writer = MarketCacheWriter::create(&path).unwrap();
    let mut cache = MarketCacheStreamWriter::new("live", writer).unwrap();
    let kline_window = KlineWindow::new(
        "SHFE.au2602".to_string(),
        60_000_000_000,
        2,
        "kline-SHFE.au2602".to_string(),
        vec![
            Kline {
                id: 1,
                datetime: 1_000,
                close: 479.0,
                ..Kline::default()
            },
            Kline {
                id: 2,
                datetime: 2_000,
                close: 480.0,
                ..Kline::default()
            },
        ],
    );
    let tick_window = TickWindow::new(
        "SHFE.au2602".to_string(),
        2,
        "tick-SHFE.au2602".to_string(),
        vec![
            Tick {
                id: 10,
                datetime: 3_000,
                last_price: 480.0,
                ..Tick::default()
            },
            Tick {
                id: 11,
                datetime: 4_000,
                last_price: 481.0,
                ..Tick::default()
            },
        ],
    );

    let written = cache
        .pipe_market_events(
            stream::iter([
                Ok(MarketEvent::KlineWindow(ValueUpdate {
                    commit: market_commit(8),
                    value: kline_window,
                })),
                Ok(MarketEvent::TickWindow(ValueUpdate {
                    commit: market_commit(9),
                    value: tick_window,
                })),
            ]),
            Some(2),
        )
        .await
        .unwrap();

    assert_eq!(written, 2);
    let events: Vec<_> = MarketCacheReader::open(&path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0].payload,
        MarketCachePayload::Kline {
            duration_ns: 60_000_000_000,
            ref row
        } if row.id == 2 && row.datetime == 2_000
    ));
    assert!(matches!(
        events[1].payload,
        MarketCachePayload::Tick(ref row) if row.id == 11 && row.datetime == 4_000
    ));

    let _ = std::fs::remove_file(&path);
}

fn market_commit(revision: u64) -> SharedCommitResult {
    CommitResult::new(
        Revision::new(revision),
        vec![ProtocolDomain::Market],
        Default::default(),
        Vec::new(),
        CommitScope::RealtimeUpdate,
    )
    .into()
}
