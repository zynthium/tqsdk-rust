#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-stream`.
pub type Result<T> = std::result::Result<T, StreamFacadeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Io,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
    InvalidState,
    MissingValue,
    Lagged,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamErrorDiagnostic {
    pub kind: StreamErrorKind,
    pub retry_hint: tqsdk_core::RetryHint,
    pub message: String,
    pub lagged_commits: Option<u64>,
}

impl StreamErrorDiagnostic {
    fn from_contract(error: &tqsdk_core::ContractError) -> Self {
        Self {
            kind: match error.kind() {
                tqsdk_core::ContractErrorKind::Validation => StreamErrorKind::Validation,
                tqsdk_core::ContractErrorKind::Auth => StreamErrorKind::Auth,
                tqsdk_core::ContractErrorKind::Transport => StreamErrorKind::Transport,
                tqsdk_core::ContractErrorKind::Http => StreamErrorKind::Http,
                tqsdk_core::ContractErrorKind::Adapter => StreamErrorKind::Adapter,
                tqsdk_core::ContractErrorKind::UnsupportedCommand => {
                    StreamErrorKind::UnsupportedCommand
                }
                tqsdk_core::ContractErrorKind::UnsupportedInput => {
                    StreamErrorKind::UnsupportedInput
                }
            },
            retry_hint: error.retry_hint(),
            message: error.to_string(),
            lagged_commits: None,
        }
    }

    fn from_session(diagnostic: tqsdk_session::SessionErrorDiagnostic) -> Self {
        Self {
            kind: match diagnostic.kind {
                tqsdk_session::SessionErrorKind::Validation => StreamErrorKind::Validation,
                tqsdk_session::SessionErrorKind::Auth => StreamErrorKind::Auth,
                tqsdk_session::SessionErrorKind::Transport => StreamErrorKind::Transport,
                tqsdk_session::SessionErrorKind::Http => StreamErrorKind::Http,
                tqsdk_session::SessionErrorKind::Adapter => StreamErrorKind::Adapter,
                tqsdk_session::SessionErrorKind::UnsupportedCommand => {
                    StreamErrorKind::UnsupportedCommand
                }
                tqsdk_session::SessionErrorKind::UnsupportedInput => {
                    StreamErrorKind::UnsupportedInput
                }
                tqsdk_session::SessionErrorKind::InvalidState => StreamErrorKind::InvalidState,
            },
            retry_hint: diagnostic.retry_hint,
            message: diagnostic.message,
            lagged_commits: None,
        }
    }
}

/// Errors returned by the stream facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFacadeError {
    Contract(tqsdk_core::ContractError),
    Session(tqsdk_session::SessionFacadeError),
    MissingValue {
        path: tqsdk_core::StatePath,
    },
    Lagged {
        skipped: u64,
    },
    Closed,
    Io {
        operation: &'static str,
        message: String,
    },
    InvalidState(&'static str),
}

impl From<tqsdk_core::ContractError> for StreamFacadeError {
    fn from(error: tqsdk_core::ContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<tqsdk_session::SessionFacadeError> for StreamFacadeError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

impl StreamFacadeError {
    #[must_use]
    pub fn diagnostic(&self) -> StreamErrorDiagnostic {
        match self {
            Self::Contract(error) => StreamErrorDiagnostic::from_contract(error),
            Self::Session(error) => StreamErrorDiagnostic::from_session(error.diagnostic()),
            Self::MissingValue { path } => StreamErrorDiagnostic {
                kind: StreamErrorKind::MissingValue,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("stream value missing at path {}", path.segments().join("/")),
                lagged_commits: None,
            },
            Self::Lagged { skipped } => StreamErrorDiagnostic {
                kind: StreamErrorKind::Lagged,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("stream receiver lagged and skipped {skipped} commit(s)"),
                lagged_commits: Some(*skipped),
            },
            Self::Closed => StreamErrorDiagnostic {
                kind: StreamErrorKind::Closed,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: "stream driver closed".to_string(),
                lagged_commits: None,
            },
            Self::Io { operation, message } => StreamErrorDiagnostic {
                kind: StreamErrorKind::Io,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("{operation}: {message}"),
                lagged_commits: None,
            },
            Self::InvalidState(message) => StreamErrorDiagnostic {
                kind: StreamErrorKind::InvalidState,
                retry_hint: tqsdk_core::RetryHint::DoNotRetry,
                message: format!("invalid stream facade state: {message}"),
                lagged_commits: None,
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

impl Display for StreamFacadeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => write!(f, "{error}"),
            Self::Session(error) => write!(f, "{error}"),
            Self::MissingValue { path } => write!(
                f,
                "stream value missing at path {}",
                path.segments().join("/")
            ),
            Self::Lagged { skipped } => {
                write!(f, "stream receiver lagged and skipped {skipped} commit(s)")
            }
            Self::Closed => write!(f, "stream driver closed"),
            Self::Io { operation, message } => write!(f, "{operation}: {message}"),
            Self::InvalidState(message) => write!(f, "invalid stream facade state: {message}"),
        }
    }
}

impl std::error::Error for StreamFacadeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Session(error) => Some(error),
            Self::MissingValue { .. }
            | Self::Lagged { .. }
            | Self::Closed
            | Self::Io { .. }
            | Self::InvalidState(_) => None,
        }
    }
}
