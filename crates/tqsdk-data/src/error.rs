#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};
use std::time::Duration;

/// Result alias for `tqsdk-data`.
pub type Result<T> = std::result::Result<T, DataError>;

/// Errors returned by research/offline data helpers.
#[derive(Debug)]
pub enum DataError {
    Session(tqsdk_session::SessionFacadeError),
    PermissionDenied(String),
    Validation(String),
    InvalidState(&'static str),
    InvalidResponse(String),
    Timeout(Duration),
    Http(reqwest::Error),
}

impl From<tqsdk_session::SessionFacadeError> for DataError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

impl From<reqwest::Error> for DataError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl Display for DataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(error) => write!(f, "{error}"),
            Self::PermissionDenied(message) => write!(f, "{message}"),
            Self::Validation(message) => write!(f, "invalid data query input: {message}"),
            Self::InvalidState(message) => write!(f, "invalid data client state: {message}"),
            Self::InvalidResponse(message) => {
                write!(f, "invalid data service response: {message}")
            }
            Self::Timeout(timeout) => {
                write!(f, "data request timed out after {timeout:?}")
            }
            Self::Http(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            Self::Http(error) => Some(error),
            Self::PermissionDenied(_)
            | Self::Validation(_)
            | Self::InvalidState(_)
            | Self::InvalidResponse(_)
            | Self::Timeout(_) => None,
        }
    }
}
