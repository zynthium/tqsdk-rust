use std::time::Duration;

use chrono::{DateTime, Datelike, Days, FixedOffset, NaiveDate, NaiveTime, Utc};
use tqsdk_core::{Quote, TradingTime};

use crate::calendar::TradingDayCalendar;

pub(super) fn shanghai_now() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&china_tz())
}

pub(super) fn china_tz() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("UTC+8 fixed offset should be valid")
}

pub(super) fn effective_step_elapsed(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    quote: Option<&Quote>,
    calendar: Option<&TradingDayCalendar>,
) -> Duration {
    quote
        .and_then(|quote| trading_time_elapsed_between(start, end, &quote.trading_time, calendar))
        .unwrap_or_else(|| wall_elapsed(start, end))
}

pub(super) fn trading_time_elapsed_between(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    trading_time: &TradingTime,
    calendar: Option<&TradingDayCalendar>,
) -> Option<Duration> {
    if end <= start {
        return Some(Duration::ZERO);
    }

    let min_date = start
        .date_naive()
        .checked_sub_days(Days::new(1))
        .unwrap_or(start.date_naive());
    let max_date = end.date_naive();
    let mut date = min_date;
    let mut total = Duration::ZERO;
    let mut saw_valid_window = has_valid_trading_window(trading_time);

    while date <= max_date {
        let next_date = date.checked_add_days(Days::new(1))?;
        let day_open = is_trading_day(date, calendar);
        let next_day_open = is_trading_day(next_date, calendar);
        let night_open = day_open && next_day_open;

        if day_open {
            let (elapsed, saw_valid) =
                trading_windows_overlap(start, end, date, &trading_time.day, false);
            total = total.saturating_add(elapsed);
            saw_valid_window |= saw_valid;
        }
        if night_open {
            let (elapsed, saw_valid) =
                trading_windows_overlap(start, end, date, &trading_time.night, true);
            total = total.saturating_add(elapsed);
            saw_valid_window |= saw_valid;
        }

        date = next_date;
    }

    saw_valid_window.then_some(total)
}

fn has_valid_trading_window(trading_time: &TradingTime) -> bool {
    trading_time
        .day
        .iter()
        .any(|window| parse_trading_window(window, false).is_some())
        || trading_time
            .night
            .iter()
            .any(|window| parse_trading_window(window, true).is_some())
}

fn trading_windows_overlap(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    date: NaiveDate,
    windows: &[Vec<String>],
    allow_cross_midnight: bool,
) -> (Duration, bool) {
    let mut total = Duration::ZERO;
    let mut saw_valid_window = false;

    for window in windows {
        let Some((window_start, window_end, crosses_midnight)) =
            parse_trading_window(window, allow_cross_midnight)
        else {
            continue;
        };
        saw_valid_window = true;

        let interval_start = date
            .and_time(window_start)
            .and_local_timezone(china_tz())
            .single();
        let interval_end_date = if crosses_midnight {
            date.checked_add_days(Days::new(1))
        } else {
            Some(date)
        };
        let interval_end = interval_end_date.and_then(|interval_end_date| {
            interval_end_date
                .and_time(window_end)
                .and_local_timezone(china_tz())
                .single()
        });

        let (Some(interval_start), Some(interval_end)) = (interval_start, interval_end) else {
            continue;
        };
        if interval_end <= interval_start {
            continue;
        }

        let overlap_start = start.max(interval_start);
        let overlap_end = end.min(interval_end);
        total = total.saturating_add(wall_elapsed(overlap_start, overlap_end));
    }

    (total, saw_valid_window)
}

fn parse_trading_window(
    window: &[String],
    allow_cross_midnight: bool,
) -> Option<(NaiveTime, NaiveTime, bool)> {
    let start = NaiveTime::parse_from_str(window.first()?, "%H:%M:%S").ok()?;
    let end = NaiveTime::parse_from_str(window.get(1)?, "%H:%M:%S").ok()?;
    if start == end {
        return None;
    }
    let crosses_midnight = allow_cross_midnight && end < start;
    if !crosses_midnight && end <= start {
        return None;
    }
    Some((start, end, crosses_midnight))
}

fn wall_elapsed(start: DateTime<FixedOffset>, end: DateTime<FixedOffset>) -> Duration {
    end.signed_duration_since(start)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn is_weekday(date: NaiveDate) -> bool {
    date.weekday().number_from_monday() <= 5
}

fn is_trading_day(date: NaiveDate, calendar: Option<&TradingDayCalendar>) -> bool {
    calendar
        .and_then(|calendar| calendar.day_status(date))
        .unwrap_or_else(|| is_weekday(date))
}
