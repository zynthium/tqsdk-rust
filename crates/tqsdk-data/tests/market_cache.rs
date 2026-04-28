use std::io::Cursor;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheEvent, MarketCacheIndex, MarketCacheLock, MarketCachePayload,
    MarketCachePayloadKind, MarketCacheQueue, MarketCacheReader, MarketCacheReplay,
    MarketCacheWriter,
};

#[test]
fn market_cache_event_constructors_preserve_standard_payloads() {
    let mut quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    quote.datetime = "2026-04-27 10:00:00.000000".into();

    let quote_event =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(900), quote).unwrap();
    assert_eq!(quote_event.source, "live");
    assert_eq!(quote_event.symbol, "SHFE.au2602");
    assert_eq!(quote_event.event_time_ns(), 900);
    match quote_event.payload {
        MarketCachePayload::Quote(payload) => assert_eq!(payload.last_price, 480.5),
        _ => panic!("expected quote payload"),
    }

    let kline = Kline {
        datetime: 2_000,
        close: 481.0,
        ..Kline::default()
    };
    let kline_event = MarketCacheEvent::kline(
        "history",
        "SHFE.au2602",
        2_100,
        Some(2_000),
        60_000_000_000,
        kline,
    )
    .unwrap();
    assert_eq!(kline_event.event_time_ns(), 2_000);
    match kline_event.payload {
        MarketCachePayload::Kline { duration_ns, row } => {
            assert_eq!(duration_ns, 60_000_000_000);
            assert_eq!(row.close, 481.0);
        }
        _ => panic!("expected kline payload"),
    }

    let tick = Tick {
        datetime: 3_000,
        last_price: 482.0,
        ..Tick::default()
    };
    let tick_event = MarketCacheEvent::tick("history", "SHFE.au2602", 3_100, None, tick).unwrap();
    assert_eq!(tick_event.event_time_ns(), 3_100);
    match tick_event.payload {
        MarketCachePayload::Tick(payload) => assert_eq!(payload.last_price, 482.0),
        _ => panic!("expected tick payload"),
    }
}

#[test]
fn market_cache_event_rejects_invalid_identity_and_times() {
    assert!(MarketCacheEvent::quote("live", "", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("", "SHFE.au2602", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("live", "SHFE.au2602", -1, None, Quote::default()).is_err());
    assert!(
        MarketCacheEvent::kline("history", "SHFE.au2602", 1, None, 0, Kline::default()).is_err()
    );
}

#[test]
fn market_cache_writer_and_reader_roundtrip_jsonl_events() {
    let quote = Quote {
        last_price: 481.0,
        ..Quote::default()
    };
    let event = MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(900), quote).unwrap();

    let mut bytes = Vec::new();
    {
        let mut writer = MarketCacheWriter::new(&mut bytes);
        writer.write_event(&event).unwrap();
        writer.flush().unwrap();
    }

    let decoded: Vec<_> = MarketCacheReader::new(Cursor::new(bytes))
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].source, event.source);
    assert_eq!(decoded[0].symbol, event.symbol);
    assert_eq!(decoded[0].received_at_ns, event.received_at_ns);
    assert_eq!(decoded[0].exchange_time_ns, event.exchange_time_ns);
    match &decoded[0].payload {
        MarketCachePayload::Quote(payload) => assert_eq!(payload.last_price, 481.0),
        _ => panic!("expected quote payload"),
    }
}

#[test]
fn market_cache_replay_orders_events_by_event_time_then_receive_time() {
    let late_received_early_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 2_000, Some(1_000), Quote::default())
            .unwrap();
    let early_received_late_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(3_000), Quote::default())
            .unwrap();
    let no_exchange_time =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_500, None, Quote::default()).unwrap();

    let replay = MarketCacheReplay::new(vec![
        early_received_late_exchange,
        no_exchange_time,
        late_received_early_exchange,
    ]);
    let ordered: Vec<_> = replay.collect();
    let order_keys: Vec<_> = ordered
        .iter()
        .map(|event| (event.event_time_ns(), event.received_at_ns))
        .collect();

    assert_eq!(
        order_keys,
        vec![(1_000, 2_000), (1_500, 1_500), (3_000, 1_000)]
    );
}

#[test]
fn market_cache_index_groups_events_by_source_symbol_and_payload_kind() {
    let events = [
        quote_event("live", "SHFE.au2602", 2_000, Some(1_000), 480.0),
        quote_event("live", "SHFE.au2602", 3_000, Some(1_500), 481.0),
        MarketCacheEvent::tick(
            "history",
            "SHFE.au2602",
            4_000,
            Some(2_000),
            Tick {
                datetime: 2_000,
                last_price: 482.0,
                ..Tick::default()
            },
        )
        .unwrap(),
    ];

    let index = MarketCacheIndex::from_events(events.iter());

    assert_eq!(index.total_events(), 3);
    let quote_entry = index
        .entry("live", "SHFE.au2602", MarketCachePayloadKind::Quote)
        .unwrap();
    assert_eq!(quote_entry.events, 2);
    assert_eq!(quote_entry.min_event_time_ns, 1_000);
    assert_eq!(quote_entry.max_event_time_ns, 1_500);
    assert!(
        index
            .entry("history", "SHFE.au2602", MarketCachePayloadKind::Tick)
            .is_some()
    );
}

#[test]
fn market_cache_queue_drains_to_writer_after_success() {
    let queue_path = temp_path("market-cache-queue.jsonl");
    let cache_path = temp_path("market-cache-drain.jsonl");
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&cache_path);

    let queue = MarketCacheQueue::open(&queue_path).unwrap();
    queue
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_000),
            480.0,
        ))
        .unwrap();
    queue
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            3_000,
            Some(1_500),
            481.0,
        ))
        .unwrap();

    let mut writer = MarketCacheWriter::create(&cache_path).unwrap();
    let report = queue.drain_to_writer(&mut writer).unwrap();

    assert_eq!(report.read_events, 2);
    assert_eq!(report.written_events, 2);
    assert!(queue.is_empty().unwrap());

    let drained = MarketCacheReader::open(&cache_path)
        .unwrap()
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(drained.len(), 2);
}

#[test]
fn market_cache_lock_blocks_second_holder_until_released() {
    let lock_path = temp_path("market-cache.lock");
    let _ = std::fs::remove_file(&lock_path);

    let first = MarketCacheLock::acquire(&lock_path).unwrap();
    assert!(MarketCacheLock::acquire(&lock_path).is_err());

    drop(first);
    let second = MarketCacheLock::acquire(&lock_path).unwrap();
    assert_eq!(second.path(), lock_path.as_path());
}

#[test]
fn market_cache_compaction_filters_by_event_time_and_builds_index() {
    let mut input = Vec::new();
    {
        let mut writer = MarketCacheWriter::new(&mut input);
        writer
            .write_event(&quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0))
            .unwrap();
        writer
            .write_event(&quote_event(
                "live",
                "SHFE.au2602",
                2_000,
                Some(1_500),
                480.0,
            ))
            .unwrap();
        writer
            .write_event(&quote_event(
                "history",
                "DCE.m2601",
                3_000,
                Some(2_500),
                3_100.0,
            ))
            .unwrap();
        writer.flush().unwrap();
    }

    let mut compacted = Vec::new();
    let report = {
        let mut output = MarketCacheWriter::new(&mut compacted);
        let report = MarketCacheCompaction::new()
            .retain_event_time_from(1_000)
            .retain_symbol("SHFE.au2602")
            .compact_reader_to_writer(MarketCacheReader::new(Cursor::new(input)), &mut output)
            .unwrap();
        output.flush().unwrap();
        report
    };

    assert_eq!(report.read_events, 3);
    assert_eq!(report.written_events, 1);
    assert_eq!(report.dropped_events, 2);
    assert_eq!(report.index.total_events(), 1);

    let events = MarketCacheReader::new(Cursor::new(compacted))
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_time_ns(), 1_500);
    assert_eq!(events[0].symbol, "SHFE.au2602");
}

fn quote_event(
    source: &str,
    symbol: &str,
    received_at_ns: i64,
    exchange_time_ns: Option<i64>,
    last_price: f64,
) -> MarketCacheEvent {
    MarketCacheEvent::quote(
        source,
        symbol,
        received_at_ns,
        exchange_time_ns,
        Quote {
            last_price,
            ..Quote::default()
        },
    )
    .unwrap()
}

fn temp_path(file_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-data-{}-{nanos}-{file_name}",
        std::process::id()
    ))
}
