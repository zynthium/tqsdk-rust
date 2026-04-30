use std::time::Duration;

use tqsdk_core::{ContractError, RetryHint, SessionPhase};
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{
    StreamErrorKind, StreamFacadeError, StreamHealthStatus, StreamReconnectOutcome,
    StreamRetryDecision, StreamRetryGiveUpReason, StreamRetryPolicy, StreamSessionPhase,
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

#[test]
fn stream_retry_policy_classifies_transport_http_and_non_retryable_errors() {
    let policy = StreamRetryPolicy::new()
        .max_attempts(3)
        .base_delay(Duration::from_millis(10))
        .max_delay(Duration::from_millis(25));

    let transport = StreamFacadeError::Session(SessionFacadeError::from(ContractError::transport(
        "websocket recv failed",
    )));
    assert_eq!(
        policy.decide(1, &transport),
        StreamRetryDecision::RetryAfterReconnect {
            failed_attempt: 1,
            delay: Duration::from_millis(10)
        }
    );

    let http = StreamFacadeError::Session(SessionFacadeError::from(ContractError::http(
        "query timeout",
    )));
    assert_eq!(
        policy.decide(2, &http),
        StreamRetryDecision::RetryWithBackoff {
            failed_attempt: 2,
            delay: Duration::from_millis(20)
        }
    );

    assert_eq!(
        policy.decide(1, &StreamFacadeError::Lagged { skipped: 1 }),
        StreamRetryDecision::GiveUp {
            failed_attempt: 1,
            reason: StreamRetryGiveUpReason::NotRetryable
        }
    );
    let auth = StreamFacadeError::Session(SessionFacadeError::from(ContractError::auth(
        "bad password",
    )));
    assert_eq!(
        policy.decide(3, &auth),
        StreamRetryDecision::GiveUp {
            failed_attempt: 3,
            reason: StreamRetryGiveUpReason::NotRetryable
        }
    );
    assert_eq!(
        policy.decide(3, &transport),
        StreamRetryDecision::GiveUp {
            failed_attempt: 3,
            reason: StreamRetryGiveUpReason::AttemptsExhausted
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stream_retry_policy_runs_retryable_operation_without_manual_backoff_loop() {
    let policy = StreamRetryPolicy::new()
        .max_attempts(3)
        .base_delay(Duration::ZERO);
    let mut attempts = 0;

    let report = policy
        .run(|attempt| {
            attempts = attempt;
            async move {
                if attempt < 3 {
                    Err(StreamFacadeError::Session(SessionFacadeError::from(
                        ContractError::http("query timeout"),
                    )))
                } else {
                    Ok("ok")
                }
            }
        })
        .await
        .expect("retryable operation should eventually succeed");

    assert_eq!(report.value(), &"ok");
    assert_eq!(report.attempts(), 3);
    assert_eq!(report.retry_count(), 2);
    assert_eq!(attempts, 3);
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
