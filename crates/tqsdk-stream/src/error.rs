#![cfg_attr(not(test), forbid(unsafe_code))]

use std::{
    fmt::{Display, Formatter},
    future::Future,
    time::Duration,
};

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

/// Retry policy for stream-facing fallible operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

/// Retry decision derived from a [`StreamFacadeError`] diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRetryDecision {
    RetryWithBackoff {
        failed_attempt: u32,
        delay: Duration,
    },
    RetryAfterReconnect {
        failed_attempt: u32,
        delay: Duration,
    },
    GiveUp {
        failed_attempt: u32,
        reason: StreamRetryGiveUpReason,
    },
}

/// Reason a [`StreamRetryPolicy`] stopped retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRetryGiveUpReason {
    NotRetryable,
    AttemptsExhausted,
}

/// Report returned by [`StreamRetryPolicy::run`] after a successful operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRetryReport<T> {
    value: T,
    attempts: u32,
    retry_count: u32,
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

impl Default for StreamRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl StreamRetryPolicy {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn try_max_attempts(mut self, max_attempts: u32) -> Result<Self> {
        if max_attempts == 0 {
            return Err(StreamFacadeError::InvalidState(
                "stream retry max attempts must be greater than zero",
            ));
        }
        self.max_attempts = max_attempts;
        Ok(self)
    }

    #[must_use]
    pub fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    #[must_use]
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.max_attempts
    }

    #[must_use]
    pub fn decide(&self, failed_attempt: u32, error: &StreamFacadeError) -> StreamRetryDecision {
        let retry_hint = error.diagnostic().retry_hint;
        if retry_hint == tqsdk_core::RetryHint::DoNotRetry {
            return StreamRetryDecision::GiveUp {
                failed_attempt,
                reason: StreamRetryGiveUpReason::NotRetryable,
            };
        }

        if failed_attempt >= self.max_attempts {
            return StreamRetryDecision::GiveUp {
                failed_attempt,
                reason: StreamRetryGiveUpReason::AttemptsExhausted,
            };
        }

        let delay = self.delay_for_attempt(failed_attempt);
        match retry_hint {
            tqsdk_core::RetryHint::RetryWithBackoff => StreamRetryDecision::RetryWithBackoff {
                failed_attempt,
                delay,
            },
            tqsdk_core::RetryHint::RetryAfterReconnect => {
                StreamRetryDecision::RetryAfterReconnect {
                    failed_attempt,
                    delay,
                }
            }
            tqsdk_core::RetryHint::DoNotRetry => unreachable!("DoNotRetry returned early"),
        }
    }

    pub async fn run<T, F, Fut>(self, mut operation: F) -> Result<StreamRetryReport<T>>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt = 1;
        let mut retry_count = 0;
        loop {
            match operation(attempt).await {
                Ok(value) => {
                    return Ok(StreamRetryReport {
                        value,
                        attempts: attempt,
                        retry_count,
                    });
                }
                Err(error) => match self.decide(attempt, &error) {
                    StreamRetryDecision::RetryWithBackoff { delay, .. }
                    | StreamRetryDecision::RetryAfterReconnect { delay, .. } => {
                        retry_count += 1;
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        attempt += 1;
                    }
                    StreamRetryDecision::GiveUp { .. } => return Err(error),
                },
            }
        }
    }

    fn delay_for_attempt(&self, failed_attempt: u32) -> Duration {
        let multiplier = 1_u32
            .checked_shl(failed_attempt.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.base_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

impl StreamRetryDecision {
    #[must_use]
    pub fn should_retry(self) -> bool {
        matches!(
            self,
            Self::RetryWithBackoff { .. } | Self::RetryAfterReconnect { .. }
        )
    }

    #[must_use]
    pub fn failed_attempt(self) -> u32 {
        match self {
            Self::RetryWithBackoff { failed_attempt, .. }
            | Self::RetryAfterReconnect { failed_attempt, .. }
            | Self::GiveUp { failed_attempt, .. } => failed_attempt,
        }
    }

    #[must_use]
    pub fn delay(self) -> Option<Duration> {
        match self {
            Self::RetryWithBackoff { delay, .. } | Self::RetryAfterReconnect { delay, .. } => {
                Some(delay)
            }
            Self::GiveUp { .. } => None,
        }
    }
}

impl<T> StreamRetryReport<T> {
    #[must_use]
    pub fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub fn into_value(self) -> T {
        self.value
    }

    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    #[must_use]
    pub fn retry_count(&self) -> u32 {
        self.retry_count
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
