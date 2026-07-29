use chrono::{TimeZone, Utc};
use tqsdk_core::Kline;
use tqsdk_data::{backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range};
use tqsdk_task::{MinuteKlineAggregator, MinuteKlineSessionTemplate, MinuteKlineSessionWindow};

const MINUTE_NS: i64 = 60_000_000_000;

#[test]
fn aggregate_opens_once_then_updates_after_each_closed_minute() {
    let start = utc_ns(2026, 1, 5, 1, 0);
    let mut aggregate =
        MinuteKlineAggregator::new(5 * MINUTE_NS, MinuteKlineSessionTemplate::cst_trading_day())
            .unwrap();

    let first = aggregate
        .update(&kline(1, start, 100.0, 103.0, 99.0, 102.0, 3))
        .unwrap()
        .unwrap();
    assert_eq!(first.opened.unwrap().datetime, start);
    assert_eq!(first.updated.open, 100.0);
    assert_eq!(first.updated.close, 102.0);
    assert_eq!(first.updated.volume, 3);
    assert_eq!(first.event_time_ns, start + MINUTE_NS);

    let second = aggregate
        .update(&kline(2, start + MINUTE_NS, 102.0, 105.0, 101.0, 104.0, 4))
        .unwrap()
        .unwrap();
    assert!(second.opened.is_none());
    assert_eq!(second.updated.datetime, start);
    assert_eq!(second.updated.high, 105.0);
    assert_eq!(second.updated.low, 99.0);
    assert_eq!(second.updated.close, 104.0);
    assert_eq!(second.updated.volume, 7);
}

#[test]
fn aggregate_resets_at_configured_session_break() {
    let timestamp = utc_ns(2026, 1, 5, 1, 0);
    let day = backtest_tick_trading_day_for_timestamp_ns(timestamp).unwrap();
    let range = backtest_tick_trading_day_range(day).unwrap();
    let template = MinuteKlineSessionTemplate::new(
        "fixture-session-v1",
        vec![
            MinuteKlineSessionWindow::new(0, 20 * MINUTE_NS).unwrap(),
            MinuteKlineSessionWindow::new(30 * MINUTE_NS, 50 * MINUTE_NS).unwrap(),
        ],
    )
    .unwrap();
    let mut aggregate = MinuteKlineAggregator::new(5 * MINUTE_NS, template).unwrap();

    let first_time = range.start_ns + 5 * MINUTE_NS;
    let second_session_time = range.start_ns + 30 * MINUTE_NS;
    let first = aggregate
        .update(&kline(1, first_time, 100.0, 100.0, 100.0, 100.0, 1))
        .unwrap()
        .unwrap();
    let second = aggregate
        .update(&kline(
            2,
            second_session_time,
            200.0,
            200.0,
            200.0,
            200.0,
            1,
        ))
        .unwrap()
        .unwrap();

    assert_eq!(first.opened.unwrap().datetime, first_time);
    assert_eq!(second.opened.unwrap().datetime, second_session_time);
    assert_eq!(second.updated.open, 200.0);
}

#[test]
fn aggregate_rejects_non_multiple_or_non_minute_input() {
    assert!(
        MinuteKlineAggregator::new(
            90 * 1_000_000_000,
            MinuteKlineSessionTemplate::cst_trading_day(),
        )
        .is_err()
    );
    let mut aggregate =
        MinuteKlineAggregator::new(5 * MINUTE_NS, MinuteKlineSessionTemplate::cst_trading_day())
            .unwrap();
    assert!(
        aggregate
            .update(&kline(
                1,
                utc_ns(2026, 1, 5, 1, 0) + 1,
                1.0,
                1.0,
                1.0,
                1.0,
                1
            ))
            .is_err()
    );
}

fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

fn kline(id: i64, datetime: i64, open: f64, high: f64, low: f64, close: f64, volume: i64) -> Kline {
    Kline {
        id,
        datetime,
        open,
        high,
        low,
        close,
        volume,
        open_oi: id,
        close_oi: id + 1,
        ..Kline::default()
    }
}
