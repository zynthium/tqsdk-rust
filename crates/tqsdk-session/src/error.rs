#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-session`.
pub type Result<T> = std::result::Result<T, SessionFacadeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
    InvalidState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionErrorDiagnostic {
    pub kind: SessionErrorKind,
    pub retry_hint: tqsdk_core::RetryHint,
    pub message: String,
}

/// Errors returned by the shared session/direct-query facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionFacadeError {
    Core(tqsdk_core::ContractError),
    InvalidState(&'static str),
}

impl From<tqsdk_core::ContractError> for SessionFacadeError {
    fn from(error: tqsdk_core::ContractError) -> Self {
        Self::Core(error)
    }
}

impl SessionFacadeError {
    #[must_use]
    pub fn diagnostic(&self) -> SessionErrorDiagnostic {
        match self {
            Self::Core(error) => SessionErrorDiagnostic {
                kind: match error.kind() {
                    tqsdk_core::ContractErrorKind::Validation => SessionErrorKind::Validation,
                    tqsdk_core::ContractErrorKind::Auth => SessionErrorKind::Auth,
                    tqsdk_core::ContractErrorKind::Transport => SessionErrorKind::Transport,
                    tqsdk_core::ContractErrorKind::Http => SessionErrorKind::Http,
                    tqsdk_core::ContractErrorKind::Adapter => SessionErrorKind::Adapter,
                    tqsdk_core::ContractErrorKind::UnsupportedCommand => {
                        SessionErrorKind::UnsupportedCommand
                    }
                    tqsdk_core::ContractErrorKind::UnsupportedInput => {
                        SessionErrorKind::UnsupportedInput
                    }
                },
                retry_hint: error.retry_hint(),
                message: error.to_string(),
            },
            Self::InvalidState(message) => SessionErrorDiagnostic {
                kind: SessionErrorKind::InvalidState,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("invalid session facade state: {message}"),
            },
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self.diagnostic().retry_hint,
            tqsdk_core::RetryHint::DoNotRetry
        )
    }
}

impl Display for SessionFacadeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => write!(f, "{error}"),
            Self::InvalidState(message) => write!(f, "invalid session facade state: {message}"),
        }
    }
}

impl std::error::Error for SessionFacadeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            Self::InvalidState(_) => None,
        }
    }
}
