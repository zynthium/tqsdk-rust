#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-data`.
pub type Result<T> = std::result::Result<T, DataError>;

/// Errors returned by research/offline data helpers.
#[derive(Debug)]
pub enum DataError {
    Session(tqsdk_session::SessionFacadeError),
    Validation(String),
    InvalidResponse(String),
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
            Self::Validation(message) => write!(f, "invalid data query input: {message}"),
            Self::InvalidResponse(message) => {
                write!(f, "invalid data service response: {message}")
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
            Self::Validation(_) | Self::InvalidResponse(_) => None,
        }
    }
}
