use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractError {
    Validation(String),
    Auth(String),
    Transport(String),
    Http(String),
    Adapter(String),
    UnsupportedCommand(&'static str),
    UnsupportedInput(&'static str),
}

impl ContractError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport(message.into())
    }

    pub fn http(message: impl Into<String>) -> Self {
        Self::Http(message.into())
    }
}

impl Display for ContractError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "validation error: {message}"),
            Self::Auth(message) => write!(f, "auth error: {message}"),
            Self::Transport(message) => write!(f, "transport error: {message}"),
            Self::Http(message) => write!(f, "http error: {message}"),
            Self::Adapter(message) => write!(f, "adapter error: {message}"),
            Self::UnsupportedCommand(kind) => write!(f, "unsupported command: {kind}"),
            Self::UnsupportedInput(kind) => write!(f, "unsupported input: {kind}"),
        }
    }
}

impl std::error::Error for ContractError {}

pub type Result<T> = std::result::Result<T, ContractError>;
