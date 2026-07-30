#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use crate::backtest_history::BacktestHistoryRequestId;
use crate::history_series_cache::HistorySeriesCacheMiss;

/// Result alias for `tqsdk-data`.
pub type Result<T> = std::result::Result<T, DataError>;

/// Errors returned by research/offline data helpers.
#[derive(Debug)]
pub enum DataError {
    Session(tqsdk_session::SessionFacadeError),
    FeatureDisabled(&'static str),
    RemoteBacktestHistoryFillUnavailable,
    PermissionDenied(String),
    CacheMiss(Box<HistorySeriesCacheMiss>),
    CacheBusy {
        cache_dir: PathBuf,
        operation: &'static str,
    },
    Validation(String),
    InvalidState(&'static str),
    InvalidResponse(String),
    Timeout(Duration),
    CollectLimitExceeded {
        limit_bytes: usize,
        attempted_bytes: usize,
    },
    RequestFailed {
        request_id: BacktestHistoryRequestId,
        message: String,
        emitted_rows: usize,
    },
    #[cfg(feature = "services")]
    Http(reqwest::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl From<tqsdk_session::SessionFacadeError> for DataError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

#[cfg(feature = "services")]
impl From<reqwest::Error> for DataError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for DataError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for DataError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl Display for DataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(error) => write!(f, "{error}"),
            Self::FeatureDisabled(feature) => {
                write!(
                    f,
                    "this operation requires the tqsdk-data feature {feature:?}"
                )
            }
            Self::RemoteBacktestHistoryFillUnavailable => write!(
                f,
                "remote backtest history fill requires tqsdk-data features \"live\" and \"services\""
            ),
            Self::PermissionDenied(message) => write!(f, "{message}"),
            Self::CacheMiss(miss) => write!(
                f,
                "history series cache miss for {} duration {} in [{}, {}): {:?}",
                miss.symbol,
                miss.duration_ns,
                miss.start_datetime_ns,
                miss.end_datetime_ns,
                miss.missing_ranges
            ),
            Self::CacheBusy {
                cache_dir,
                operation,
            } => write!(
                f,
                "history cache at {} is busy with {operation}",
                cache_dir.display()
            ),
            Self::Validation(message) => write!(f, "invalid data query input: {message}"),
            Self::InvalidState(message) => write!(f, "invalid data client state: {message}"),
            Self::InvalidResponse(message) => {
                write!(f, "invalid data service response: {message}")
            }
            Self::Timeout(timeout) => {
                write!(f, "data request timed out after {timeout:?}")
            }
            Self::CollectLimitExceeded {
                limit_bytes,
                attempted_bytes,
            } => write!(
                f,
                "backtest history collection would use {attempted_bytes} bytes, exceeding its {limit_bytes}-byte limit"
            ),
            Self::RequestFailed {
                request_id,
                message,
                emitted_rows,
            } => write!(
                f,
                "backtest history request {request_id} failed after emitting {emitted_rows} rows: {message}"
            ),
            #[cfg(feature = "services")]
            Self::Http(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Json(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            #[cfg(feature = "services")]
            Self::Http(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::FeatureDisabled(_)
            | Self::RemoteBacktestHistoryFillUnavailable
            | Self::PermissionDenied(_)
            | Self::CacheMiss(_)
            | Self::CacheBusy { .. }
            | Self::Validation(_)
            | Self::InvalidState(_)
            | Self::InvalidResponse(_)
            | Self::Timeout(_)
            | Self::CollectLimitExceeded { .. }
            | Self::RequestFailed { .. } => None,
        }
    }
}
