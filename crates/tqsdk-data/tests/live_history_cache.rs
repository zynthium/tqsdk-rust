#![cfg(feature = "stream")]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use tqsdk_core::{
    CommitResult, CommitScope, Kline, ProtocolDomain, Quote, Revision, SharedCommitResult, Tick,
};
use tqsdk_data::{HistorySeriesCache, LiveHistoryCacheOptions, LiveHistoryCacheWriter};
use tqsdk_stream::{KlineWindow, MarketEvent, TickWindow, ValueUpdate};

#[test]
fn kline_live_writer_skips_mutable_tail_and_writes_completed_bar_after_window_advances() {
    let dir = temp_dir("live-kline");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    let mut writer = LiveHistoryCacheWriter::new(cache.clone(), LiveHistoryCacheOptions::default());

    let first = writer
        .write_kline_window(&kline_window(vec![kline(1, 10, 1.0), kline(2, 20, 2.0)]))
        .unwrap();

    assert_eq!(first.rows_seen, 2);
    assert_eq!(first.rows_written, 1);
    assert!(first.skipped_mutable_tail);
    assert_eq!(
        cache
            .read_latest_kline_rows("SHFE.au2602", 60_000_000_000, 10)
            .unwrap()
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1]
    );

    let second = writer
        .write_kline_window(&kline_window(vec![kline(2, 20, 22.0), kline(3, 30, 3.0)]))
        .unwrap();

    assert_eq!(second.rows_seen, 2);
    assert_eq!(second.rows_written, 1);
    assert!(second.skipped_mutable_tail);
    let rows = cache
        .read_latest_kline_rows("SHFE.au2602", 60_000_000_000, 10)
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(rows[1].close, 22.0);
}

#[test]
fn tick_live_writer_dedups_repeated_windows() {
    let dir = temp_dir("live-tick");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    let mut writer = LiveHistoryCacheWriter::new(cache.clone(), LiveHistoryCacheOptions::default());
    let window = tick_window(vec![tick(10, 100, 10.0), tick(11, 110, 11.0)]);

    let first = writer.write_tick_window(&window).unwrap();
    let second = writer.write_tick_window(&window).unwrap();

    assert_eq!(first.rows_seen, 2);
    assert_eq!(first.rows_written, 2);
    assert!(!first.skipped_mutable_tail);
    assert_eq!(second.rows_seen, 2);
    assert_eq!(second.rows_written, 2);
    assert!(!second.skipped_mutable_tail);
    let rows = cache.read_latest_tick_rows("SHFE.au2602", 10).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![10, 11]
    );
}

#[test]
fn live_writer_market_event_routes_windows_and_ignores_quotes() {
    let dir = temp_dir("live-event");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    let mut writer = LiveHistoryCacheWriter::new(cache.clone(), LiveHistoryCacheOptions::default());

    let quote = writer
        .write_market_event(MarketEvent::Quote(ValueUpdate {
            commit: market_commit(1),
            value: Quote {
                instrument_id: "SHFE.au2602".to_string(),
                last_price: 10.0,
                ..Quote::default()
            },
        }))
        .unwrap();
    let kline = writer
        .write_market_event(MarketEvent::KlineWindow(ValueUpdate {
            commit: market_commit(2),
            value: kline_window(vec![kline(1, 10, 1.0), kline(2, 20, 2.0)]),
        }))
        .unwrap();
    let tick = writer
        .write_market_event(MarketEvent::TickWindow(ValueUpdate {
            commit: market_commit(3),
            value: tick_window(vec![tick(10, 100, 10.0)]),
        }))
        .unwrap();

    assert_eq!(quote.rows_seen, 0);
    assert_eq!(quote.rows_written, 0);
    assert_eq!(kline.rows_written, 1);
    assert_eq!(tick.rows_written, 1);
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("tqsdk-data-live-history-cache-{name}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    canonical_or_original(&dir)
}

fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn kline_window(rows: Vec<Kline>) -> KlineWindow {
    KlineWindow::new(
        "SHFE.au2602".to_string(),
        60_000_000_000,
        rows.len(),
        "kline-SHFE.au2602".to_string(),
        rows,
    )
}

fn tick_window(rows: Vec<Tick>) -> TickWindow {
    TickWindow::new(
        "SHFE.au2602".to_string(),
        rows.len(),
        "tick-SHFE.au2602".to_string(),
        rows,
    )
}

fn kline(id: i64, datetime: i64, close: f64) -> Kline {
    Kline {
        id,
        datetime,
        open: close - 1.0,
        high: close + 1.0,
        low: close - 2.0,
        close,
        volume: id * 10,
        open_oi: id * 100,
        close_oi: id * 100 + 1,
        ..Kline::default()
    }
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        highest: last_price + 1.0,
        lowest: last_price - 1.0,
        average: last_price + 0.5,
        volume: id * 10,
        amount: last_price * 10.0,
        open_interest: id * 100,
        bid_price1: last_price - 0.1,
        bid_volume1: 1,
        ask_price1: last_price + 0.1,
        ask_volume1: 2,
        bid_price2: last_price - 0.2,
        bid_volume2: 3,
        ask_price2: last_price + 0.2,
        ask_volume2: 4,
        bid_price3: last_price - 0.3,
        bid_volume3: 5,
        ask_price3: last_price + 0.3,
        ask_volume3: 6,
        bid_price4: last_price - 0.4,
        bid_volume4: 7,
        ask_price4: last_price + 0.4,
        ask_volume4: 8,
        bid_price5: last_price - 0.5,
        bid_volume5: 9,
        ask_price5: last_price + 0.5,
        ask_volume5: 10,
        ..Tick::default()
    }
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
