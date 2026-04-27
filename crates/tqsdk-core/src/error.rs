use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHint {
    DoNotRetry,
    RetryWithBackoff,
    RetryAfterReconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractErrorKind {
    Validation,
    Auth,
    Transport,
    Http,
    Adapter,
    UnsupportedCommand,
    UnsupportedInput,
}

/// Error categories emitted by the runtime contract.
///
/// The variants are intentionally coarse-grained so higher layers can
/// distinguish validation failures, authentication problems, transport faults,
/// HTTP faults, and adapter issues without inheriting any facade semantics.
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
    /// Constructs a validation error for malformed commands, state, or usage.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    /// Constructs an authentication or authorization error.
    pub fn auth(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    /// Constructs a transport-layer error.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport(message.into())
    }

    /// Constructs an HTTP execution error.
    pub fn http(message: impl Into<String>) -> Self {
        Self::Http(message.into())
    }

    #[must_use]
    pub fn kind(&self) -> ContractErrorKind {
        match self {
            Self::Validation(_) => ContractErrorKind::Validation,
            Self::Auth(_) => ContractErrorKind::Auth,
            Self::Transport(_) => ContractErrorKind::Transport,
            Self::Http(_) => ContractErrorKind::Http,
            Self::Adapter(_) => ContractErrorKind::Adapter,
            Self::UnsupportedCommand(_) => ContractErrorKind::UnsupportedCommand,
            Self::UnsupportedInput(_) => ContractErrorKind::UnsupportedInput,
        }
    }

    #[must_use]
    pub fn retry_hint(&self) -> RetryHint {
        match self {
            Self::Transport(_) => RetryHint::RetryAfterReconnect,
            Self::Http(_) => RetryHint::RetryWithBackoff,
            Self::Validation(_)
            | Self::Auth(_)
            | Self::Adapter(_)
            | Self::UnsupportedCommand(_)
            | Self::UnsupportedInput(_) => RetryHint::DoNotRetry,
        }
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

/// Standard result alias for the runtime contract crate.
pub type Result<T> = std::result::Result<T, ContractError>;
