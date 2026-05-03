use std::io::Cursor;

use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_data::{
    MarketCacheEvent, MarketCachePayload, MarketCacheReader, MarketCacheReplay, MarketCacheWriter,
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
