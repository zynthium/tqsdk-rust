use chrono::{TimeZone, Utc};
use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    KlineSessionTemplate, KlineSessionWindow, MinuteKlineAggregator, TickKlineAggregator,
    backtest_tick_trading_day_for_timestamp_ns, backtest_tick_trading_day_range,
};

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
const HOUR_NS: i64 = 60 * MINUTE_NS;

#[test]
fn tick_first_bar_includes_the_first_cumulative_volume() {
    let session_start = trading_day_start();
    let mut aggregate = TickKlineAggregator::new(
        "KQ.i@SHFE.au",
        15 * SECOND_NS,
        KlineSessionTemplate::cst_trading_day(),
    )
    .unwrap();

    let first = aggregate
        .update(&tick(1, session_start + 1, 100.0, 3, 10))
        .unwrap()
        .unwrap();
    let second = aggregate
        .update(&tick(2, session_start + 2, 101.0, 8, 11))
        .unwrap()
        .unwrap();

    assert_eq!(first.updated.volume, 3);
    assert_eq!(second.updated.volume, 8);
    assert_eq!(second.updated.open, 100.0);
    assert_eq!(second.updated.close, 101.0);
}

#[test]
fn tick_mid_day_warmup_preserves_the_cumulative_volume_baseline() {
    let session_start = trading_day_start();
    let mut aggregate = TickKlineAggregator::new(
        "KQ.i@SHFE.au",
        15 * SECOND_NS,
        KlineSessionTemplate::cst_trading_day(),
    )
    .unwrap();

    aggregate
        .update(&tick(1, session_start + SECOND_NS, 100.0, 5, 10))
        .unwrap();
    let requested_bar = aggregate
        .update(&tick(
            2,
            session_start + 15 * SECOND_NS + SECOND_NS,
            101.0,
            9,
            11,
        ))
        .unwrap()
        .unwrap();

    assert_eq!(
        requested_bar.updated.datetime,
        session_start + 15 * SECOND_NS
    );
    assert_eq!(requested_bar.updated.volume, 4);
}

#[test]
fn tick_sessions_close_at_break_and_reset_only_on_a_new_trading_day() {
    let session_start = trading_day_start();
    let template = short_break_template();
    let mut aggregate = TickKlineAggregator::new("KQ.i@SHFE.au", 15 * SECOND_NS, template).unwrap();

    aggregate
        .update(&tick(1, session_start + 19 * SECOND_NS, 100.0, 5, 10))
        .unwrap();
    let after_break = aggregate
        .update(&tick(
            2,
            session_start + 30 * SECOND_NS + SECOND_NS,
            101.0,
            8,
            11,
        ))
        .unwrap()
        .unwrap();
    let closed = after_break.closed.unwrap();
    let opened = after_break.opened.unwrap();
    assert_eq!(closed.datetime, session_start + 15 * SECOND_NS);
    assert_eq!(closed.volume, 5);
    assert_eq!(opened.datetime, session_start + 30 * SECOND_NS);
    assert_eq!(opened.open, 101.0);
    assert_eq!(opened.open_oi, 10);
    assert_eq!(after_break.updated.volume, 3);
    assert_eq!(after_break.updated.close_oi, 11);

    let first_day = backtest_tick_trading_day_for_timestamp_ns(session_start).unwrap();
    let first_range = backtest_tick_trading_day_range(first_day).unwrap();
    let second_day = backtest_tick_trading_day_for_timestamp_ns(first_range.end_ns + 1).unwrap();
    let second_range = backtest_tick_trading_day_range(second_day).unwrap();
    let next_day = aggregate
        .update(&tick(3, second_range.start_ns + SECOND_NS, 102.0, 3, 13))
        .unwrap()
        .unwrap();
    let opened = next_day.opened.unwrap();
    assert_eq!(opened.open, 102.0);
    assert_eq!(opened.open_oi, 11);
    assert_eq!(next_day.updated.volume, 3);
    assert_eq!(next_day.updated.close_oi, 13);
}

#[test]
fn tick_cumulative_volume_is_carried_when_it_continues_across_a_trading_day() {
    let session_start = trading_day_start();
    let mut aggregate = TickKlineAggregator::new(
        "KQ.i@SHFE.au",
        15 * SECOND_NS,
        KlineSessionTemplate::cst_trading_day(),
    )
    .unwrap();

    aggregate
        .update(&tick(1, session_start + SECOND_NS, 100.0, 100, 10))
        .unwrap();

    let first_day = backtest_tick_trading_day_for_timestamp_ns(session_start).unwrap();
    let first_range = backtest_tick_trading_day_range(first_day).unwrap();
    let second_day = backtest_tick_trading_day_for_timestamp_ns(first_range.end_ns + 1).unwrap();
    let second_range = backtest_tick_trading_day_range(second_day).unwrap();
    let next_day = aggregate
        .update(&tick(2, second_range.start_ns + SECOND_NS, 101.0, 105, 11))
        .unwrap()
        .unwrap();

    assert_eq!(next_day.updated.open, 101.0);
    assert_eq!(next_day.updated.open_oi, 10);
    assert_eq!(next_day.updated.volume, 5);
    assert_eq!(next_day.updated.close_oi, 11);
}

#[test]
fn tick_truncated_session_bar_can_be_finished_without_crossing_the_break() {
    let session_start = trading_day_start();
    let mut aggregate =
        TickKlineAggregator::new("KQ.i@SHFE.au", 15 * SECOND_NS, short_break_template()).unwrap();
    aggregate
        .update(&tick(1, session_start + 19 * SECOND_NS, 100.0, 5, 10))
        .unwrap();

    let closed = aggregate
        .finish_closed_through(session_start + 20 * SECOND_NS)
        .unwrap();
    assert_eq!(closed.datetime, session_start + 15 * SECOND_NS);
    assert_eq!(closed.volume, 5);
}

#[test]
fn tick_official_session_windows_use_the_prior_natural_day_across_weekends() {
    let monday_nine_cst = Utc
        .with_ymd_and_hms(2026, 1, 5, 1, 0, 0)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let template = KlineSessionTemplate::new(
        "official-day-window",
        vec![KlineSessionWindow::new(15 * HOUR_NS, 16 * HOUR_NS).unwrap()],
    )
    .unwrap();
    let position = template.locate(monday_nine_cst).unwrap().unwrap();
    assert_eq!(position.window_start_ns, monday_nine_cst);

    let mut aggregate = TickKlineAggregator::new("KQ.i@SHFE.au", 15 * SECOND_NS, template).unwrap();
    let update = aggregate
        .update(&tick(1, monday_nine_cst + SECOND_NS, 100.0, 3, 10))
        .unwrap()
        .unwrap();
    assert_eq!(update.updated.datetime, monday_nine_cst);
    assert_eq!(update.updated.volume, 3);
}

#[test]
fn tick_boundary_assignment_matches_the_official_chart() {
    let session_start = trading_day_start();
    let mut aggregate = TickKlineAggregator::new(
        "KQ.i@SHFE.au",
        15 * SECOND_NS,
        KlineSessionTemplate::cst_trading_day(),
    )
    .unwrap();

    aggregate
        .update(&tick(1, session_start + SECOND_NS, 100.0, 10, 1))
        .unwrap();
    aggregate
        .update(&tick(2, session_start + 14 * SECOND_NS, 101.0, 15, 2))
        .unwrap();
    let exact_boundary = aggregate
        .update(&tick(3, session_start + 15 * SECOND_NS, 102.0, 20, 3))
        .unwrap()
        .unwrap();
    let closed = exact_boundary.closed.unwrap();
    assert_eq!(closed.open, 100.0);
    assert_eq!(closed.close, 102.0);
    assert_eq!(closed.volume, 20);
    assert_eq!(closed.close_oi, 3);
    let opened = exact_boundary.opened.unwrap();
    assert_eq!(opened.open, 102.0);
    assert_eq!(opened.volume, 0);
    assert_eq!(exact_boundary.updated.volume, 0);

    aggregate
        .update(&tick(4, session_start + 29 * SECOND_NS, 103.0, 25, 4))
        .unwrap();
    let after_boundary = aggregate
        .update(&tick(
            5,
            session_start + 30 * SECOND_NS + SECOND_NS,
            104.0,
            29,
            5,
        ))
        .unwrap()
        .unwrap();
    let closed = after_boundary.closed.unwrap();
    assert_eq!(closed.open, 102.0);
    assert_eq!(closed.close, 103.0);
    assert_eq!(closed.volume, 5);
    let opened = after_boundary.opened.unwrap();
    assert_eq!(opened.open, 103.0);
    assert_eq!(opened.open_oi, 4);
    assert_eq!(after_boundary.updated.open, 103.0);
    assert_eq!(after_boundary.updated.close, 104.0);
    assert_eq!(after_boundary.updated.volume, 4);
    assert_eq!(after_boundary.updated.close_oi, 5);
}

#[test]
fn minute_aggregation_uses_only_closed_minutes_and_preserves_the_grid_across_gaps() {
    let session_start = trading_day_start();
    let mut aggregate =
        MinuteKlineAggregator::new(5 * MINUTE_NS, KlineSessionTemplate::cst_trading_day()).unwrap();
    let mut final_update = None;
    for index in 0..5 {
        final_update = aggregate
            .update(&kline(
                index,
                session_start + index * MINUTE_NS,
                100.0 + index as f64,
                101.0 + index as f64,
                99.0 + index as f64,
                100.5 + index as f64,
                index + 1,
            ))
            .unwrap();
    }
    let final_update = final_update.unwrap();
    assert_eq!(final_update.updated.open, 100.0);
    assert_eq!(final_update.updated.high, 105.0);
    assert_eq!(final_update.updated.low, 99.0);
    assert_eq!(final_update.updated.close, 104.5);
    assert_eq!(final_update.updated.volume, 15);
    assert_eq!(final_update.updated.open_oi, 0);
    assert_eq!(final_update.updated.close_oi, 5);
    let closed = final_update.closed.unwrap();
    assert_eq!(closed.datetime, final_update.updated.datetime);
    assert_eq!(closed.open, final_update.updated.open);
    assert_eq!(closed.high, final_update.updated.high);
    assert_eq!(closed.low, final_update.updated.low);
    assert_eq!(closed.close, final_update.updated.close);
    assert_eq!(closed.volume, final_update.updated.volume);

    let template = KlineSessionTemplate::new(
        "fifteen-minute-break",
        vec![
            KlineSessionWindow::new(0, 10 * MINUTE_NS).unwrap(),
            KlineSessionWindow::new(20 * MINUTE_NS, 40 * MINUTE_NS).unwrap(),
        ],
    )
    .unwrap();
    let mut broken = MinuteKlineAggregator::new(15 * MINUTE_NS, template).unwrap();
    for index in 0..10 {
        broken
            .update(&kline(
                index,
                session_start + index * MINUTE_NS,
                100.0,
                100.0,
                100.0,
                100.0,
                1,
            ))
            .unwrap();
    }
    let after_break = broken
        .update(&kline(
            11,
            session_start + 20 * MINUTE_NS,
            200.0,
            200.0,
            200.0,
            200.0,
            1,
        ))
        .unwrap()
        .unwrap();
    assert_eq!(
        after_break.opened.unwrap().datetime,
        session_start + 15 * MINUTE_NS
    );
    assert_eq!(after_break.updated.open, 200.0);
}

#[test]
fn minute_hourly_aggregation_keeps_the_trading_day_grid_across_a_break() {
    let session_start = trading_day_start();
    let template = KlineSessionTemplate::new(
        "hourly-break-grid",
        vec![
            KlineSessionWindow::new(0, 15 * MINUTE_NS).unwrap(),
            KlineSessionWindow::new(30 * MINUTE_NS, 90 * MINUTE_NS).unwrap(),
        ],
    )
    .unwrap();
    let mut aggregate = MinuteKlineAggregator::new(HOUR_NS, template).unwrap();

    for index in 0..15 {
        aggregate
            .update(&kline(
                index,
                session_start + index * MINUTE_NS,
                100.0 + index as f64,
                100.0 + index as f64,
                100.0 + index as f64,
                100.0 + index as f64,
                1,
            ))
            .unwrap();
    }

    let after_break = aggregate
        .update(&kline(
            30,
            session_start + 30 * MINUTE_NS,
            200.0,
            200.0,
            200.0,
            200.0,
            1,
        ))
        .unwrap()
        .unwrap();
    assert!(after_break.opened.is_none());
    assert!(after_break.closed.is_none());
    assert_eq!(after_break.updated.datetime, session_start);

    let mut final_update = after_break;
    for index in 31..60 {
        final_update = aggregate
            .update(&kline(
                index,
                session_start + index * MINUTE_NS,
                200.0 + index as f64,
                200.0 + index as f64,
                200.0 + index as f64,
                200.0 + index as f64,
                1,
            ))
            .unwrap()
            .unwrap();
    }
    let closed = final_update.closed.unwrap();
    assert_eq!(closed.datetime, session_start);
    assert_eq!(closed.volume, 45);

    let next = aggregate
        .update(&kline(
            60,
            session_start + HOUR_NS,
            300.0,
            300.0,
            300.0,
            300.0,
            1,
        ))
        .unwrap()
        .unwrap();
    assert_eq!(next.opened.unwrap().datetime, session_start + HOUR_NS);
}

#[test]
fn synthetic_ids_are_bar_start_timestamps_across_session_breaks() {
    let session_start = trading_day_start();
    let mut aggregate =
        TickKlineAggregator::new("KQ.i@SHFE.au", 15 * SECOND_NS, short_break_template()).unwrap();
    let first = aggregate
        .update(&tick(1, session_start + SECOND_NS, 100.0, 1, 10))
        .unwrap()
        .unwrap();
    let second = aggregate
        .update(&tick(
            2,
            session_start + 30 * SECOND_NS + SECOND_NS,
            101.0,
            2,
            11,
        ))
        .unwrap()
        .unwrap();

    assert_eq!(first.updated.id, first.updated.datetime);
    let opened = second.opened.unwrap();
    assert_eq!(opened.id, opened.datetime);
    assert_ne!(first.updated.id, opened.id);
}

fn short_break_template() -> KlineSessionTemplate {
    KlineSessionTemplate::new(
        "short-break-v1",
        vec![
            KlineSessionWindow::new(0, 20 * SECOND_NS).unwrap(),
            KlineSessionWindow::new(30 * SECOND_NS, 50 * SECOND_NS).unwrap(),
        ],
    )
    .unwrap()
}

fn trading_day_start() -> i64 {
    let timestamp = Utc
        .with_ymd_and_hms(2026, 1, 5, 1, 0, 0)
        .single()
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    let day = backtest_tick_trading_day_for_timestamp_ns(timestamp).unwrap();
    backtest_tick_trading_day_range(day).unwrap().start_ns
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
