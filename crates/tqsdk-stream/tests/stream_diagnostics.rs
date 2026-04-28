use tqsdk_core::{ContractError, RetryHint, SessionPhase};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{StreamErrorKind, StreamFacadeError, StreamHealthStatus, StreamSessionPhase};

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
