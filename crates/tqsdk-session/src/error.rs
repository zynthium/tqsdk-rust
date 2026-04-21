#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-session`.
pub type Result<T> = std::result::Result<T, SessionFacadeError>;

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
