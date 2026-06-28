#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;

pub type RelayResult<T> = Result<T, RelayError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayError {
    InvalidConfig(String),
    InvalidProtocol(String),
    UnsupportedCommand(String),
    Capacity(String),
    Transport(String),
    Internal(String),
}

impl RelayError {
    #[must_use]
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    #[must_use]
    pub fn invalid_protocol(message: impl Into<String>) -> Self {
        Self::InvalidProtocol(message.into())
    }

    #[must_use]
    pub fn unsupported_command(aid: impl Into<String>) -> Self {
        Self::UnsupportedCommand(aid.into())
    }
}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid relay config: {message}"),
            Self::InvalidProtocol(message) => write!(f, "invalid relay protocol: {message}"),
            Self::UnsupportedCommand(aid) => write!(f, "unsupported relay market command: {aid}"),
            Self::Capacity(message) => write!(f, "relay capacity error: {message}"),
            Self::Transport(message) => write!(f, "relay transport error: {message}"),
            Self::Internal(message) => write!(f, "relay internal error: {message}"),
        }
    }
}

impl From<tqsdk_data::DataError> for RelayError {
    fn from(error: tqsdk_data::DataError) -> Self {
        Self::invalid_config(error.to_string())
    }
}

impl std::error::Error for RelayError {}
