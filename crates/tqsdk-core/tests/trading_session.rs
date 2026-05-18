use std::time::Duration;

use tqsdk_core::{TradingSessionPhase, TradingSessionSchedule, TradingSessionSegment};

fn segment(start_secs: u64, end_secs: u64) -> TradingSessionSegment {
    TradingSessionSegment::new(
        Duration::from_secs(start_secs),
        Duration::from_secs(end_secs),
    )
    .expect("segment should be valid")
}

#[test]
fn status_at_reports_open_and_countdown() {
    let schedule = TradingSessionSchedule::from_segments([
        segment(9 * 3600, 10 * 3600 + 15 * 60),
        segment(13 * 3600 + 30 * 60, 15 * 3600),
    ])
    .with_pre_close_window(Duration::from_secs(5 * 60));

    let status = schedule.status_at(Duration::from_secs(9 * 3600 + 30 * 60));

    assert_eq!(status.phase, TradingSessionPhase::Open);
    assert_eq!(status.countdown, Some(Duration::from_secs(45 * 60)));
}

#[test]
fn status_at_reports_pre_close_inside_window() {
    let schedule = TradingSessionSchedule::from_segments([
        segment(9 * 3600, 10 * 3600 + 15 * 60),
        segment(13 * 3600 + 30 * 60, 15 * 3600),
    ])
    .with_pre_close_window(Duration::from_secs(5 * 60));

    let status = schedule.status_at(Duration::from_secs(10 * 3600 + 13 * 60));

    assert_eq!(status.phase, TradingSessionPhase::PreClose);
    assert_eq!(status.countdown, Some(Duration::from_secs(2 * 60)));
}

#[test]
fn status_at_reports_closed_and_rollover_to_next_open() {
    let schedule = TradingSessionSchedule::from_segments([
        segment(9 * 3600, 10 * 3600 + 15 * 60),
        segment(13 * 3600 + 30 * 60, 15 * 3600),
    ]);

    let status = schedule.status_at(Duration::from_secs(11 * 3600));

    assert_eq!(status.phase, TradingSessionPhase::Closed);
    assert_eq!(
        status.countdown,
        Some(Duration::from_secs(2 * 3600 + 30 * 60))
    );
}

#[test]
fn status_at_handles_wraparound_sessions_and_empty_schedules() {
    let overnight = TradingSessionSchedule::from_segments([segment(21 * 3600, 3600)])
        .with_pre_close_window(Duration::from_secs(30 * 60));

    let open = overnight.status_at(Duration::from_secs(23 * 3600 + 30 * 60));
    assert_eq!(open.phase, TradingSessionPhase::Open);
    assert_eq!(open.countdown, Some(Duration::from_secs(90 * 60)));

    let rollover = overnight.status_at(Duration::from_secs(2 * 3600));
    assert_eq!(rollover.phase, TradingSessionPhase::Closed);
    assert_eq!(rollover.countdown, Some(Duration::from_secs(19 * 3600)));

    let empty = TradingSessionSchedule::from_segments(std::iter::empty::<TradingSessionSegment>());
    let closed = empty.status_at(Duration::from_secs(12 * 3600));
    assert_eq!(closed.phase, TradingSessionPhase::Closed);
    assert_eq!(closed.countdown, None);
}

#[test]
fn segment_rejects_empty_window() {
    assert!(
        TradingSessionSegment::new(Duration::from_secs(9 * 3600), Duration::from_secs(9 * 3600))
            .is_none()
    );
}
