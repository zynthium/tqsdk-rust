use std::time::Duration;

use tqsdk_core::{ContractError, RetryHint, SessionPhase};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{
    StreamErrorKind, StreamFacadeError, StreamHealthStatus, StreamReconnectOutcome,
    StreamSessionPhase,
};

mod support;

#[test]
fn stream_errors_expose_stable_kind_and_retry_hint() {
    let lagged = StreamFacadeError::Lagged { skipped: 7 };
    let diagnostic = lagged.diagnostic();
    assert_eq!(diagnostic.kind, StreamErrorKind::Lagged);
    assert_eq!(diagnostic.retry_hint, RetryHint::DoNotRetry);
    assert_eq!(diagnostic.lagged_commits, Some(7));

    let session = StreamFacadeError::Session(SessionFacadeError::from(ContractError::transport(
        "websocket recv failed",
    )));
    let diagnostic = session.diagnostic();
    assert_eq!(diagnostic.kind, StreamErrorKind::Transport);
    assert_eq!(diagnostic.retry_hint, RetryHint::RetryAfterReconnect);
    assert!(session.is_retryable());
}

#[tokio::test(flavor = "current_thread")]
async fn stream_health_status_summarizes_operational_state() {
    let stream = support::core_seed::seeded_stream();
    assert_eq!(
        stream.health().unwrap().status(),
        StreamHealthStatus::Starting
    );

    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Running);
    assert_eq!(
        stream.health().unwrap().status(),
        StreamHealthStatus::Healthy
    );

    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Reconnecting);
    support::core_seed::seed_session_reconnect_commit(&stream, "transport-error");
    let health = stream.health().unwrap();
    assert_eq!(health.session_phase, Some(StreamSessionPhase::Reconnecting));
    assert_eq!(health.status(), StreamHealthStatus::Recovering);

    let _commits = stream.commit_stream().unwrap();
    stream.close_driver_for_test();
    assert_eq!(
        stream.health().unwrap().status(),
        StreamHealthStatus::Closed
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_monitor_waits_for_recovery_without_manual_polling() {
    let stream = support::core_seed::seeded_stream();
    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Reconnecting);
    support::core_seed::seed_session_reconnect_commit(&stream, "transport-error");

    let recovery = async {
        tokio::task::yield_now().await;
        support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Running);
    };

    let monitor = stream
        .reconnect_monitor()
        .timeout(Duration::from_millis(100));
    let (report, ()) = tokio::join!(monitor.wait(), recovery);
    let report = report.unwrap();

    assert_eq!(report.outcome(), StreamReconnectOutcome::Recovered);
    assert_eq!(
        report.health().session_phase,
        Some(StreamSessionPhase::Running)
    );
    assert_eq!(report.last_reconnect().unwrap().attempt, 1);
    assert!(report.observed_commits() >= 1);
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_monitor_reports_exhausted_reconnect_without_manual_polling() {
    let stream = support::core_seed::seeded_stream();
    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Reconnecting);
    support::core_seed::seed_session_reconnect_commit_with_exhausted(
        &stream,
        "transport-error",
        true,
    );

    let report = stream.reconnect_monitor().wait().await.unwrap();

    assert_eq!(report.outcome(), StreamReconnectOutcome::Exhausted);
    assert_eq!(
        report.health().session_phase,
        Some(StreamSessionPhase::Reconnecting)
    );
    assert!(report.last_reconnect().unwrap().exhausted);
    assert_eq!(report.observed_commits(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_monitor_reports_timeout_without_manual_polling() {
    let stream = support::core_seed::seeded_stream();
    support::core_seed::seed_session_phase_commit(&stream, SessionPhase::Reconnecting);
    support::core_seed::seed_session_reconnect_commit(&stream, "transport-error");

    let report = stream
        .reconnect_monitor()
        .timeout(Duration::from_millis(1))
        .wait()
        .await
        .unwrap();

    assert_eq!(report.outcome(), StreamReconnectOutcome::TimedOut);
    assert_eq!(
        report.health().session_phase,
        Some(StreamSessionPhase::Reconnecting)
    );
    assert_eq!(report.observed_commits(), 0);
}
