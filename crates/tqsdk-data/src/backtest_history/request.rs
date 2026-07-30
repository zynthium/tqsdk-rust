use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{DataError, Result};

use super::BacktestHistoryClient;
use super::fill::{ServerHistorySourceFactory, default_server_history_source_factory};

/// Stable identifier supplied by the caller to associate request events.
pub type BacktestHistoryRequestId = u64;

/// Whether a query may fill incomplete durable cache coverage from the official
/// server-backtest source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryPolicy {
    /// Read only durable cache coverage and report a miss when it is incomplete.
    CacheOnly,
    /// Use durable cache coverage first and fill only missing ranges remotely.
    RemoteOnMiss,
}

/// The history rows requested from the cache-backed backtest query service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryKind {
    /// Raw Tick rows.
    Tick,
    /// Kline rows at the requested duration.
    Kline { duration: Duration },
}

/// One half-open backtest history request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryRequest {
    request_id: BacktestHistoryRequestId,
    symbol: String,
    kind: BacktestHistoryKind,
    start_ns: i64,
    end_ns: i64,
    provisional_as_of_ns: Option<i64>,
}

impl BacktestHistoryRequest {
    /// Creates a Tick request over `[start_ns, end_ns)`.
    #[must_use]
    pub fn tick(
        request_id: BacktestHistoryRequestId,
        symbol: impl Into<String>,
        start_ns: i64,
        end_ns: i64,
    ) -> Self {
        Self {
            request_id,
            symbol: symbol.into(),
            kind: BacktestHistoryKind::Tick,
            start_ns,
            end_ns,
            provisional_as_of_ns: None,
        }
    }

    /// Creates a Kline request over `[start_ns, end_ns)`.
    #[must_use]
    pub fn kline(
        request_id: BacktestHistoryRequestId,
        symbol: impl Into<String>,
        duration: Duration,
        start_ns: i64,
        end_ns: i64,
    ) -> Self {
        Self {
            request_id,
            symbol: symbol.into(),
            kind: BacktestHistoryKind::Kline { duration },
            start_ns,
            end_ns,
            provisional_as_of_ns: None,
        }
    }

    /// Opts this Tick or sub-minute request into an explicit provisional view.
    #[must_use]
    pub fn with_provisional_as_of_ns(mut self, as_of_ns: i64) -> Self {
        self.provisional_as_of_ns = Some(as_of_ns);
        self
    }

    pub(crate) fn validate(&self) -> Result<ValidatedBacktestHistoryRequest> {
        if self.symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "backtest history symbol must not be empty".to_string(),
            ));
        }
        if self.start_ns >= self.end_ns {
            return Err(DataError::Validation(format!(
                "backtest history range must satisfy start_ns < end_ns: [{}, {})",
                self.start_ns, self.end_ns
            )));
        }
        let duration_ns = match self.kind {
            BacktestHistoryKind::Tick => None,
            BacktestHistoryKind::Kline { duration } => {
                let value = i64::try_from(duration.as_nanos()).map_err(|_| {
                    DataError::Validation(
                        "backtest history Kline duration exceeds i64 nanoseconds".to_string(),
                    )
                })?;
                if value <= 0 {
                    return Err(DataError::Validation(
                        "backtest history Kline duration must be positive".to_string(),
                    ));
                }
                Some(value)
            }
        };
        if self
            .provisional_as_of_ns
            .is_some_and(|value| value < self.start_ns || value > self.end_ns)
        {
            return Err(DataError::Validation(
                "provisional_as_of_ns must be inside the requested range".to_string(),
            ));
        }
        Ok(ValidatedBacktestHistoryRequest {
            request_id: self.request_id,
            symbol: self.symbol.clone(),
            kind: self.kind,
            duration_ns,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
            provisional_as_of_ns: self.provisional_as_of_ns,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ValidatedBacktestHistoryRequest {
    pub(crate) request_id: BacktestHistoryRequestId,
    pub(crate) symbol: String,
    pub(crate) kind: BacktestHistoryKind,
    pub(crate) duration_ns: Option<i64>,
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
    pub(crate) provisional_as_of_ns: Option<i64>,
}

/// Credentials used only when a cache miss actually needs a remote fill.
///
/// The type deliberately does not implement [`std::fmt::Debug`] so passwords
/// cannot be leaked by normal diagnostics.
pub struct BacktestHistoryCredentials {
    user: String,
    pass: String,
}

impl BacktestHistoryCredentials {
    /// Creates a credential pair. Empty values are rejected when loaded.
    #[must_use]
    pub fn new(user: impl Into<String>, pass: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            pass: pass.into(),
        }
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if self.user.trim().is_empty() {
            return Err(DataError::Validation(
                "backtest history authentication user must not be empty".to_string(),
            ));
        }
        if self.pass.is_empty() {
            return Err(DataError::Validation(
                "backtest history authentication password must not be empty".to_string(),
            ));
        }
        Ok(self)
    }

    #[allow(dead_code)]
    pub(crate) fn into_parts(self) -> (String, String) {
        (self.user, self.pass)
    }
}

/// Lazy source of credentials for remote cache fills.
pub trait BacktestHistoryAuthProvider: Send + Sync {
    /// Loads credentials only after the query planner has established that a
    /// remote fill is necessary.
    fn load<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>>;
}

impl<T> BacktestHistoryAuthProvider for Arc<T>
where
    T: BacktestHistoryAuthProvider + ?Sized,
{
    fn load<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>> {
        self.as_ref().load()
    }
}

struct EnvironmentBacktestHistoryAuthProvider;

impl BacktestHistoryAuthProvider for EnvironmentBacktestHistoryAuthProvider {
    fn load<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<BacktestHistoryCredentials>> + Send + 'a>> {
        Box::pin(async {
            let user = std::env::var("TQ_AUTH_USER").map_err(|_| {
                DataError::Validation(
                    "TQ_AUTH_USER is required for remote backtest history fill".into(),
                )
            })?;
            let pass = std::env::var("TQ_AUTH_PASS").map_err(|_| {
                DataError::Validation(
                    "TQ_AUTH_PASS is required for remote backtest history fill".into(),
                )
            })?;
            BacktestHistoryCredentials::new(user, pass).validate()
        })
    }
}

/// Configures a [`BacktestHistoryClient`].
pub struct BacktestHistoryClientBuilder {
    cache_dir: PathBuf,
    policy: BacktestHistoryPolicy,
    logical_concurrency: usize,
    blocking_workers: usize,
    per_symbol_buffer_bytes: usize,
    collect_limit_bytes: usize,
    auth_provider: Option<Arc<dyn BacktestHistoryAuthProvider>>,
}

impl BacktestHistoryClientBuilder {
    pub(crate) fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            policy: BacktestHistoryPolicy::RemoteOnMiss,
            logical_concurrency: DEFAULT_LOGICAL_CONCURRENCY,
            blocking_workers: default_blocking_workers(),
            per_symbol_buffer_bytes: DEFAULT_PER_SYMBOL_BUFFER_BYTES,
            collect_limit_bytes: DEFAULT_COLLECT_LIMIT_BYTES,
            auth_provider: None,
        }
    }

    /// Selects whether cache misses may be filled from the official source.
    #[must_use]
    pub fn policy(mut self, policy: BacktestHistoryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Uses `TQ_AUTH_USER` and `TQ_AUTH_PASS` only if a remote fill is needed.
    #[must_use]
    pub fn auth_env(mut self) -> Self {
        self.auth_provider = Some(Arc::new(EnvironmentBacktestHistoryAuthProvider));
        self
    }

    /// Uses an application-provided, asynchronous credential source.
    #[must_use]
    pub fn auth_provider<P>(mut self, provider: P) -> Self
    where
        P: BacktestHistoryAuthProvider + 'static,
    {
        self.auth_provider = Some(Arc::new(provider));
        self
    }

    /// Limits concurrently active logical requests.
    #[must_use]
    pub fn logical_concurrency(mut self, value: usize) -> Self {
        self.logical_concurrency = value;
        self
    }

    /// Limits blocking cache scan workers.
    #[must_use]
    pub fn blocking_workers(mut self, value: usize) -> Self {
        self.blocking_workers = value;
        self
    }

    /// Limits shared Tick/minute buffering for one logical symbol.
    #[must_use]
    pub fn per_symbol_buffer_bytes(mut self, value: usize) -> Self {
        self.per_symbol_buffer_bytes = value;
        self
    }

    /// Sets the default maximum memory used by [`super::BacktestHistoryRun::collect`].
    #[must_use]
    pub fn collect_limit_bytes(mut self, value: usize) -> Self {
        self.collect_limit_bytes = value;
        self
    }

    /// Validates configuration and constructs a cache-backed query client.
    pub fn build(self) -> Result<BacktestHistoryClient> {
        validate_nonzero("logical_concurrency", self.logical_concurrency)?;
        validate_nonzero("blocking_workers", self.blocking_workers)?;
        validate_nonzero("per_symbol_buffer_bytes", self.per_symbol_buffer_bytes)?;
        validate_nonzero("collect_limit_bytes", self.collect_limit_bytes)?;
        Ok(BacktestHistoryClient::from_config(
            BacktestHistoryClientConfig {
                cache_dir: self.cache_dir,
                policy: self.policy,
                logical_concurrency: self.logical_concurrency,
                blocking_workers: self.blocking_workers,
                per_symbol_buffer_bytes: self.per_symbol_buffer_bytes,
                collect_limit_bytes: self.collect_limit_bytes,
                auth_provider: self.auth_provider,
                source_factory: default_server_history_source_factory(),
            },
        ))
    }
}

fn validate_nonzero(name: &str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(DataError::Validation(format!(
            "backtest history {name} must be greater than zero"
        )));
    }
    Ok(())
}

pub(crate) const DEFAULT_LOGICAL_CONCURRENCY: usize = 32;
pub(crate) const DEFAULT_PER_SYMBOL_BUFFER_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const DEFAULT_COLLECT_LIMIT_BYTES: usize = 512 * 1024 * 1024;

pub(crate) fn default_blocking_workers() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, 8)
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct BacktestHistoryClientConfig {
    pub(crate) cache_dir: PathBuf,
    pub(crate) policy: BacktestHistoryPolicy,
    pub(crate) logical_concurrency: usize,
    pub(crate) blocking_workers: usize,
    pub(crate) per_symbol_buffer_bytes: usize,
    pub(crate) collect_limit_bytes: usize,
    pub(crate) auth_provider: Option<Arc<dyn BacktestHistoryAuthProvider>>,
    pub(crate) source_factory: Arc<dyn ServerHistorySourceFactory>,
}
