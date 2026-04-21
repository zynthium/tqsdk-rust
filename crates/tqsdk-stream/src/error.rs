#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-stream`.
pub type Result<T> = std::result::Result<T, StreamFacadeError>;

/// Errors returned by the stream facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFacadeError {
    Session(tqsdk_session::SessionFacadeError),
    Lagged { skipped: u64 },
    Closed,
    InvalidState(&'static str),
}

impl From<tqsdk_session::SessionFacadeError> for StreamFacadeError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

impl Display for StreamFacadeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(error) => write!(f, "{error}"),
            Self::Lagged { skipped } => {
                write!(f, "stream receiver lagged and skipped {skipped} commit(s)")
            }
            Self::Closed => write!(f, "stream driver closed"),
            Self::InvalidState(message) => write!(f, "invalid stream facade state: {message}"),
        }
    }
}

impl std::error::Error for StreamFacadeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            Self::Lagged { .. } | Self::Closed | Self::InvalidState(_) => None,
        }
    }
}
