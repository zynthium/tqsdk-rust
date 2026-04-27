use tqsdk_core::{ContractError, RetryHint};
use tqsdk_session::{SessionErrorKind, SessionFacadeError};

#[test]
fn session_errors_expose_kind_retry_hint_and_message() {
    let transport = SessionFacadeError::from(ContractError::transport("socket closed"));
    let diagnostic = transport.diagnostic();
    assert_eq!(diagnostic.kind, SessionErrorKind::Transport);
    assert_eq!(diagnostic.retry_hint, RetryHint::RetryAfterReconnect);
    assert_eq!(diagnostic.message, "transport error: socket closed");
    assert!(transport.is_retryable());

    let invalid = SessionFacadeError::InvalidState("query route disabled");
    let diagnostic = invalid.diagnostic();
    assert_eq!(diagnostic.kind, SessionErrorKind::InvalidState);
    assert_eq!(diagnostic.retry_hint, RetryHint::DoNotRetry);
    assert!(!invalid.is_retryable());
}
