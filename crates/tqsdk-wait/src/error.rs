#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-wait`.
pub type Result<T> = std::result::Result<T, WaitFacadeError>;

/// Errors returned by the wait-style facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitFacadeError {
    Session(tqsdk_session::SessionFacadeError),
    Core(tqsdk_core::ContractError),
    ConcurrentWaitUpdate,
    InvalidState(&'static str),
}

impl From<tqsdk_session::SessionFacadeError> for WaitFacadeError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

impl From<tqsdk_core::ContractError> for WaitFacadeError {
    fn from(error: tqsdk_core::ContractError) -> Self {
        Self::Core(error)
    }
}

impl Display for WaitFacadeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(error) => write!(f, "{error}"),
            Self::Core(error) => write!(f, "{error}"),
            Self::ConcurrentWaitUpdate => write!(f, "concurrent wait_update is not allowed"),
            Self::InvalidState(message) => write!(f, "invalid wait facade state: {message}"),
        }
    }
}

impl std::error::Error for WaitFacadeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            Self::Core(error) => Some(error),
            Self::ConcurrentWaitUpdate | Self::InvalidState(_) => None,
        }
    }
}
