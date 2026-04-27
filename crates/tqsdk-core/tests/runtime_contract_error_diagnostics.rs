use tqsdk_core::{ContractError, ContractErrorKind, RetryHint};

#[test]
fn contract_errors_expose_stable_kind_and_retry_hint() {
    assert_eq!(
        ContractError::transport("websocket recv failed").kind(),
        ContractErrorKind::Transport
    );
    assert_eq!(
        ContractError::transport("websocket recv failed").retry_hint(),
        RetryHint::RetryAfterReconnect
    );

    assert_eq!(
        ContractError::auth("bad password").kind(),
        ContractErrorKind::Auth
    );
    assert_eq!(
        ContractError::auth("bad password").retry_hint(),
        RetryHint::DoNotRetry
    );

    assert_eq!(
        ContractError::validation("invalid symbol").kind(),
        ContractErrorKind::Validation
    );
    assert_eq!(
        ContractError::validation("invalid symbol").retry_hint(),
        RetryHint::DoNotRetry
    );

    assert_eq!(
        ContractError::http("query timeout").retry_hint(),
        RetryHint::RetryWithBackoff
    );
    assert_eq!(
        ContractError::UnsupportedCommand("market").retry_hint(),
        RetryHint::DoNotRetry
    );
}
