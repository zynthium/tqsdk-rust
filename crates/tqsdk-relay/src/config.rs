#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use crate::cache::MarketCacheLimits;
use crate::error::{RelayError, RelayResult};
use crate::universe_expression::{
    SnapshotUniverseDispatch, UniverseExpression, UniverseSpec, parse_snapshot_universe_compatible,
};
use crate::upstream::{UpstreamTickChart, upstream_subscription_ins_list_chars};

const SECONDS_PER_DAY: u64 = 86_400;
const UPSTREAM_TICK_CHART_ID_PREFIX: &str = "relay-upstream-tick";
pub const DEFAULT_UPSTREAM_TICK_VIEW_WIDTH: usize = 10_000;
const ENV_UPSTREAM_MARKET_URL: &str = "TQSDK_RELAY_UPSTREAM_MARKET_URL";
const ENV_DOWNSTREAM_LISTEN: &str = "TQSDK_RELAY_DOWNSTREAM_LISTEN";
const ENV_METRICS_LISTEN: &str = "TQSDK_RELAY_METRICS_LISTEN";
const ENV_FUTURES_UNIVERSE: &str = "TQSDK_RELAY_FUTURES_UNIVERSE";
const ENV_FUTURES_UNIVERSE_FILES: &str = "TQSDK_RELAY_FUTURES_UNIVERSE_FILES";
const ENV_FUTURES_UNIVERSE_REFRESH_AT: &str = "TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_AT";
const ENV_FUTURES_METADATA_BATCH_SIZE: &str = "TQSDK_RELAY_FUTURES_METADATA_BATCH_SIZE";
const ENV_UPSTREAM_INS_LIST_WARN_CHARS: &str = "TQSDK_RELAY_UPSTREAM_INS_LIST_WARN_CHARS";
const ENV_UPSTREAM_INS_LIST_MAX_CHARS: &str = "TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS";
const ENV_UPSTREAM_TICK_VIEW_WIDTH: &str = "TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH";
const ENV_TICK_RING_CAPACITY: &str = "TQSDK_RELAY_TICK_RING_CAPACITY";
const ENV_KLINE_RING_CAPACITY: &str = "TQSDK_RELAY_KLINE_RING_CAPACITY";
const ENV_OUTBOUND_CHANNEL_CAPACITY: &str = "TQSDK_RELAY_OUTBOUND_CHANNEL_CAPACITY";
const ENV_OUTBOUND_BYTE_CAPACITY: &str = "TQSDK_RELAY_OUTBOUND_BYTE_CAPACITY";
const ENV_MARKET_CACHE_MAX_SYMBOLS: &str = "TQSDK_RELAY_MARKET_CACHE_MAX_SYMBOLS";
const ENV_MARKET_CACHE_MAX_BYTES: &str = "TQSDK_RELAY_MARKET_CACHE_MAX_BYTES";
const ENV_ROLLING_CACHE_DIR: &str = "TQSDK_RELAY_ROLLING_CACHE_DIR";
const ENV_ROLLING_CACHE_SESSION_HASH: &str = "TQSDK_RELAY_ROLLING_CACHE_SESSION_HASH";
const ENV_ROLLING_CACHE_ALGORITHM_VERSION: &str = "TQSDK_RELAY_ROLLING_CACHE_ALGORITHM_VERSION";
const ENV_HISTORY_ROOT: &str = "TQSDK_RELAY_HISTORY_ROOT";
const ENV_HISTORY_CACHE_DIR: &str = "TQSDK_RELAY_HISTORY_CACHE_DIR";
const ENV_DRY_RUN: &str = "TQSDK_RELAY_DRY_RUN";
const ENV_AUTH_USER: &str = "TQ_AUTH_USER";
const ENV_AUTH_PASS: &str = "TQ_AUTH_PASS";
pub const DEFAULT_FUTURES_METADATA_BATCH_SIZE: usize = 500;
pub const DEFAULT_OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuturesUniverseRefreshSchedule {
    refresh_at: DailyRefreshTime,
}

impl Default for FuturesUniverseRefreshSchedule {
    fn default() -> Self {
        Self {
            refresh_at: DailyRefreshTime::from_hms(8, 30, 0).expect("valid default refresh time"),
        }
    }
}

impl FuturesUniverseRefreshSchedule {
    pub fn daily(refresh_at: DailyRefreshTime) -> Self {
        Self { refresh_at }
    }

    #[must_use]
    pub const fn refresh_at(self) -> DailyRefreshTime {
        self.refresh_at
    }

    #[must_use]
    pub fn delay_from_seconds_after_midnight(
        self,
        current_seconds_after_midnight: u32,
    ) -> Duration {
        next_daily_refresh_delay(current_seconds_after_midnight, self.refresh_at)
    }

    fn validate(self) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DailyRefreshTime {
    seconds_after_midnight: u32,
}

impl DailyRefreshTime {
    pub fn from_hms(hour: u32, minute: u32, second: u32) -> RelayResult<Self> {
        if hour >= 24 || minute >= 60 || second >= 60 {
            return Err(RelayError::invalid_config(
                "daily refresh time must be HH:MM[:SS]",
            ));
        }
        Ok(Self {
            seconds_after_midnight: hour * 3600 + minute * 60 + second,
        })
    }

    pub fn parse(value: &str) -> RelayResult<Self> {
        let parts: Vec<&str> = value.trim().split(':').collect();
        if !(2..=3).contains(&parts.len()) {
            return Err(RelayError::invalid_config(
                "daily refresh time must be HH:MM[:SS]",
            ));
        }
        let hour = parse_time_part(parts[0])?;
        let minute = parse_time_part(parts[1])?;
        let second = if let Some(second) = parts.get(2) {
            parse_time_part(second)?
        } else {
            0
        };
        Self::from_hms(hour, minute, second)
    }

    #[must_use]
    pub const fn seconds_after_midnight(self) -> u32 {
        self.seconds_after_midnight
    }
}

pub fn next_daily_refresh_delay(
    current_seconds_after_midnight: u32,
    refresh_at: DailyRefreshTime,
) -> Duration {
    let current = u64::from(current_seconds_after_midnight) % SECONDS_PER_DAY;
    let target = u64::from(refresh_at.seconds_after_midnight());
    let delay = if current < target {
        target - current
    } else {
        SECONDS_PER_DAY - (current - target)
    };
    Duration::from_secs(delay)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamInsListLimits {
    pub warn_chars: Option<usize>,
    pub max_chars: Option<usize>,
}

impl Default for UpstreamInsListLimits {
    fn default() -> Self {
        Self {
            warn_chars: Some(32_000),
            max_chars: None,
        }
    }
}

impl UpstreamInsListLimits {
    pub fn validate(&self) -> RelayResult<()> {
        if self.warn_chars == Some(0) {
            return Err(RelayError::invalid_config(
                "upstream_ins_list_limits.warn_chars must be greater than zero",
            ));
        }
        if self.max_chars == Some(0) {
            return Err(RelayError::invalid_config(
                "upstream_ins_list_limits.max_chars must be greater than zero",
            ));
        }
        Ok(())
    }

    pub fn validate_ins_list_chars(&self, chars: usize) -> RelayResult<()> {
        if let Some(max_chars) = self.max_chars
            && chars > max_chars
        {
            return Err(RelayError::invalid_config(format!(
                "upstream subscription ins_list length {chars} exceeds hard limit {max_chars} chars"
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn over_warn(&self, chars: usize) -> bool {
        self.warn_chars.is_some_and(|warn_chars| chars > warn_chars)
    }

    #[must_use]
    pub fn suggested_shards(&self, chars: usize) -> Option<usize> {
        let limit = self.max_chars.or(self.warn_chars)?;
        if chars <= limit {
            return None;
        }
        Some(chars.div_ceil(limit).max(1))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayConfig {
    pub upstream_market_url: String,
    pub upstream_auth_user: Option<String>,
    pub upstream_auth_pass: Option<String>,
    pub downstream_listen: String,
    pub metrics_listen: String,
    pub futures_universe_refresh: FuturesUniverseRefreshSchedule,
    pub futures_metadata_batch_size: usize,
    pub futures_universe_expression: Option<UniverseExpression>,
    pub upstream_ins_list_limits: UpstreamInsListLimits,
    pub upstream_tick_view_width: usize,
    pub tick_ring_capacity: usize,
    pub kline_ring_capacity: usize,
    pub outbound_channel_capacity: usize,
    pub disk_cache_dir: Option<PathBuf>,
    pub bootstrap: BootstrapConfig,
    pub best_effort_duration_tag: bool,
    pub dry_run: bool,
}

impl fmt::Debug for RelayConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upstream_auth_pass = self.upstream_auth_pass.as_ref().map(|_| "<redacted>");

        f.debug_struct("RelayConfig")
            .field("upstream_market_url", &self.upstream_market_url)
            .field("upstream_auth_user", &self.upstream_auth_user)
            .field("upstream_auth_pass", &upstream_auth_pass)
            .field("downstream_listen", &self.downstream_listen)
            .field("metrics_listen", &self.metrics_listen)
            .field("futures_universe_refresh", &self.futures_universe_refresh)
            .field(
                "futures_metadata_batch_size",
                &self.futures_metadata_batch_size,
            )
            .field(
                "futures_universe_expression",
                &self.futures_universe_expression,
            )
            .field("upstream_ins_list_limits", &self.upstream_ins_list_limits)
            .field("upstream_tick_view_width", &self.upstream_tick_view_width)
            .field("tick_ring_capacity", &self.tick_ring_capacity)
            .field("kline_ring_capacity", &self.kline_ring_capacity)
            .field("outbound_channel_capacity", &self.outbound_channel_capacity)
            .field("disk_cache_dir", &self.disk_cache_dir)
            .field("bootstrap", &self.bootstrap)
            .field("best_effort_duration_tag", &self.best_effort_duration_tag)
            .field("dry_run", &self.dry_run)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub max_concurrent_remote_charts: usize,
    pub min_remote_request_interval: Duration,
    pub per_series_cooldown: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            upstream_market_url: "wss://openmd.shinnytech.com/t/md/front/mobile".to_string(),
            upstream_auth_user: None,
            upstream_auth_pass: None,
            downstream_listen: "127.0.0.1:7788".to_string(),
            metrics_listen: "127.0.0.1:7789".to_string(),
            futures_universe_refresh: FuturesUniverseRefreshSchedule::default(),
            futures_metadata_batch_size: DEFAULT_FUTURES_METADATA_BATCH_SIZE,
            futures_universe_expression: None,
            upstream_ins_list_limits: UpstreamInsListLimits::default(),
            upstream_tick_view_width: DEFAULT_UPSTREAM_TICK_VIEW_WIDTH,
            tick_ring_capacity: 200_000,
            kline_ring_capacity: 10_000,
            outbound_channel_capacity: DEFAULT_OUTBOUND_CHANNEL_CAPACITY,
            disk_cache_dir: None,
            bootstrap: BootstrapConfig::default(),
            best_effort_duration_tag: true,
            dry_run: false,
        }
    }
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            max_concurrent_remote_charts: 4,
            min_remote_request_interval: Duration::from_millis(250),
            per_series_cooldown: Duration::from_secs(30),
        }
    }
}

impl RelayConfig {
    pub fn from_env() -> RelayResult<Self> {
        Self::from_env_vars(|key| std::env::var(key).ok())
    }

    pub fn from_env_vars(mut get: impl FnMut(&str) -> Option<String>) -> RelayResult<Self> {
        let mut config = Self::default();
        if let Some(value) = get(ENV_UPSTREAM_MARKET_URL) {
            config.upstream_market_url = value;
        }
        if let Some(value) = get(ENV_DOWNSTREAM_LISTEN) {
            config.downstream_listen = value;
        }
        if let Some(value) = get(ENV_METRICS_LISTEN) {
            config.metrics_listen = value;
        }
        if let Some(value) = get(ENV_AUTH_USER) {
            config.upstream_auth_user = Some(value.trim().to_string());
        }
        if let Some(value) = get(ENV_AUTH_PASS) {
            config.upstream_auth_pass = Some(value.trim().to_string());
        }
        if let Some(value) = get(ENV_DRY_RUN) {
            config.dry_run = parse_bool_env(ENV_DRY_RUN, &value)?;
        }
        if let Some(value) = get(ENV_FUTURES_UNIVERSE_REFRESH_AT) {
            let refresh_at = DailyRefreshTime::parse(&value).map_err(|_| {
                RelayError::invalid_config(format!(
                    "{ENV_FUTURES_UNIVERSE_REFRESH_AT} must be HH:MM[:SS]"
                ))
            })?;
            config.futures_universe_refresh = FuturesUniverseRefreshSchedule::daily(refresh_at);
        }
        if let Some(value) = get(ENV_FUTURES_METADATA_BATCH_SIZE) {
            config.futures_metadata_batch_size =
                parse_positive_usize_env(ENV_FUTURES_METADATA_BATCH_SIZE, &value)?;
        }
        if let Some(value) = get(ENV_UPSTREAM_INS_LIST_WARN_CHARS) {
            config.upstream_ins_list_limits.warn_chars = Some(parse_positive_usize_env(
                ENV_UPSTREAM_INS_LIST_WARN_CHARS,
                &value,
            )?);
        }
        if let Some(value) = get(ENV_UPSTREAM_INS_LIST_MAX_CHARS) {
            config.upstream_ins_list_limits.max_chars = Some(parse_positive_usize_env(
                ENV_UPSTREAM_INS_LIST_MAX_CHARS,
                &value,
            )?);
        }
        if let Some(value) = get(ENV_UPSTREAM_TICK_VIEW_WIDTH) {
            config.upstream_tick_view_width =
                parse_positive_usize_env(ENV_UPSTREAM_TICK_VIEW_WIDTH, &value)?;
        }
        if let Some(value) = get(ENV_TICK_RING_CAPACITY) {
            config.tick_ring_capacity = parse_positive_usize_env(ENV_TICK_RING_CAPACITY, &value)?;
        }
        if let Some(value) = get(ENV_KLINE_RING_CAPACITY) {
            config.kline_ring_capacity = parse_positive_usize_env(ENV_KLINE_RING_CAPACITY, &value)?;
        }
        if let Some(value) = get(ENV_OUTBOUND_CHANNEL_CAPACITY) {
            config.outbound_channel_capacity =
                parse_positive_usize_env(ENV_OUTBOUND_CHANNEL_CAPACITY, &value)?;
        }
        if let Some(value) = get(ENV_FUTURES_UNIVERSE) {
            config.futures_universe_expression = Some(UniverseExpression::parse(&value)?);
        }
        config.validate()?;
        Ok(config)
    }

    pub fn upstream_tick_charts_for_symbols<'a, I>(
        &self,
        symbols: I,
    ) -> RelayResult<Vec<UpstreamTickChart>>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut symbols: Vec<String> = symbols
            .into_iter()
            .map(str::trim)
            .filter(|symbol| !symbol.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        symbols.sort();
        symbols.dedup();
        if symbols.is_empty() {
            return Ok(Vec::new());
        }
        let charts = symbols
            .iter()
            .map(|symbol| {
                UpstreamTickChart::new(
                    upstream_tick_chart_id(symbol, self.upstream_tick_view_width),
                    [symbol.as_str()],
                    self.upstream_tick_view_width,
                )
            })
            .collect::<RelayResult<Vec<_>>>()?;
        self.upstream_ins_list_limits
            .validate_ins_list_chars(upstream_subscription_ins_list_chars(&charts))?;
        Ok(charts)
    }

    #[must_use]
    pub fn has_upstream_futures_source(&self) -> bool {
        self.futures_universe_expression.is_some()
    }

    #[must_use]
    pub fn refreshes_futures_universe(&self) -> bool {
        self.futures_universe_expression.is_some()
    }

    pub fn validate(&self) -> RelayResult<()> {
        if self.upstream_market_url.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "upstream_market_url must not be empty",
            ));
        }
        if self.downstream_listen.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "downstream_listen must not be empty",
            ));
        }
        if self.metrics_listen.trim().is_empty() {
            return Err(RelayError::invalid_config(
                "metrics_listen must not be empty",
            ));
        }
        self.futures_universe_refresh.validate();
        self.upstream_ins_list_limits.validate()?;
        if self.futures_metadata_batch_size == 0 {
            return Err(RelayError::invalid_config(
                "futures_metadata_batch_size must be greater than zero",
            ));
        }
        if self.upstream_tick_view_width == 0 {
            return Err(RelayError::invalid_config(
                "upstream_tick_view_width must be greater than zero",
            ));
        }
        if self.tick_ring_capacity == 0 {
            return Err(RelayError::invalid_config(
                "tick_ring_capacity must be greater than zero",
            ));
        }
        if self.kline_ring_capacity == 0 {
            return Err(RelayError::invalid_config(
                "kline_ring_capacity must be greater than zero",
            ));
        }
        if self.outbound_channel_capacity == 0 {
            return Err(RelayError::invalid_config(
                "outbound_channel_capacity must be greater than zero",
            ));
        }
        if self.bootstrap.max_concurrent_remote_charts == 0 {
            return Err(RelayError::invalid_config(
                "bootstrap.max_concurrent_remote_charts must be greater than zero",
            ));
        }
        if self.bootstrap.min_remote_request_interval == Duration::ZERO {
            return Err(RelayError::invalid_config(
                "bootstrap.min_remote_request_interval must be greater than zero",
            ));
        }
        if self.bootstrap.per_series_cooldown == Duration::ZERO {
            return Err(RelayError::invalid_config(
                "bootstrap.per_series_cooldown must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Additive process-level resource boundaries.
///
/// This lives beside [`RelayConfig`] so new limits do not break callers that
/// still use exhaustive `RelayConfig` literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayResourceLimits {
    pub outbound_byte_capacity: usize,
    pub market_cache: MarketCacheLimits,
}

impl Default for RelayResourceLimits {
    fn default() -> Self {
        Self::defaults()
    }
}

impl RelayResourceLimits {
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            outbound_byte_capacity: 8 * 1024 * 1024,
            market_cache: MarketCacheLimits::defaults(),
        }
    }

    fn validate(self, tick_capacity: usize) -> RelayResult<()> {
        if self.outbound_byte_capacity == 0 {
            return Err(RelayError::invalid_config(
                "outbound_byte_capacity must be greater than zero",
            ));
        }
        if self.market_cache.max_symbols == 0 {
            return Err(RelayError::invalid_config(
                "market_cache.max_symbols must be greater than zero",
            ));
        }
        let minimum = MarketCacheLimits::minimum_retained_bytes(tick_capacity);
        if self.market_cache.max_retained_bytes < minimum {
            return Err(RelayError::invalid_config(format!(
                "market_cache.max_retained_bytes must fit one tick ring ({minimum} bytes)"
            )));
        }
        Ok(())
    }
}

pub const DEFAULT_ROLLING_CACHE_CAPACITY: usize = 10_000;

/// Durable, process-level rolling market-view cache configuration.
///
/// Its root is intentionally disjoint from CacheOnly history roots. It is a
/// relay restart accelerator, never a canonical history authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingCacheConfig {
    root: PathBuf,
    session_hash: String,
    aggregation_algorithm_version: u32,
}

impl RollingCacheConfig {
    pub fn new(
        root: impl Into<PathBuf>,
        session_hash: impl Into<String>,
        aggregation_algorithm_version: u32,
    ) -> RelayResult<Self> {
        let value = Self {
            root: root.into(),
            session_hash: session_hash.into(),
            aggregation_algorithm_version,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    #[must_use]
    pub fn capacity(&self) -> NonZeroUsize {
        NonZeroUsize::new(DEFAULT_ROLLING_CACHE_CAPACITY).expect("constant is nonzero")
    }

    #[must_use]
    pub fn session_hash(&self) -> &str {
        &self.session_hash
    }

    #[must_use]
    pub const fn aggregation_algorithm_version(&self) -> u32 {
        self.aggregation_algorithm_version
    }

    fn validate(&self) -> RelayResult<()> {
        if !self.root.is_absolute() {
            return Err(RelayError::invalid_config(
                "rolling cache root must be an absolute existing directory",
            ));
        }
        let metadata = fs::symlink_metadata(&self.root).map_err(|error| {
            RelayError::invalid_config(format!(
                "rolling cache root {} is unavailable: {error}",
                self.root.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(RelayError::invalid_config(
                "rolling cache root must be an existing non-symlink directory",
            ));
        }
        if self.session_hash.is_empty() || self.session_hash.len() > 4096 {
            return Err(RelayError::invalid_config(
                "rolling cache session hash must be nonempty and bounded",
            ));
        }
        if self.aggregation_algorithm_version == 0 {
            return Err(RelayError::invalid_config(
                "rolling cache aggregation algorithm version must be positive",
            ));
        }
        Ok(())
    }
}

/// Additive process-level configuration for Universe V2 sources.
///
/// [`RelayConfig`] keeps its original exhaustive public field set for downstream source
/// compatibility. New typed Universe inputs live in this private-field wrapper instead.
#[derive(Clone, PartialEq, Eq)]
pub struct RelayRuntimeConfig {
    relay: RelayConfig,
    resource_limits: RelayResourceLimits,
    futures_universe_spec: Option<UniverseSpec>,
    futures_universe_symbol_files: Vec<PathBuf>,
    rolling_cache: Option<RollingCacheConfig>,
}

impl fmt::Debug for RelayRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayRuntimeConfig")
            .field("relay", &self.relay)
            .field("resource_limits", &self.resource_limits)
            .field("futures_universe_spec", &self.futures_universe_spec)
            .field(
                "futures_universe_symbol_files",
                &self.futures_universe_symbol_files,
            )
            .field("rolling_cache", &self.rolling_cache)
            .finish()
    }
}

impl Default for RelayRuntimeConfig {
    fn default() -> Self {
        Self::new(RelayConfig::default())
    }
}

impl From<RelayConfig> for RelayRuntimeConfig {
    fn from(relay: RelayConfig) -> Self {
        Self::new(relay)
    }
}

impl RelayRuntimeConfig {
    #[must_use]
    pub const fn new(relay: RelayConfig) -> Self {
        Self {
            relay,
            resource_limits: RelayResourceLimits::defaults(),
            futures_universe_spec: None,
            futures_universe_symbol_files: Vec::new(),
            rolling_cache: None,
        }
    }

    pub fn from_env() -> RelayResult<Self> {
        Self::from_env_vars(|key| std::env::var(key).ok())
    }

    pub fn from_env_vars(mut get: impl FnMut(&str) -> Option<String>) -> RelayResult<Self> {
        let universe = get(ENV_FUTURES_UNIVERSE);
        let universe_files = get(ENV_FUTURES_UNIVERSE_FILES);
        let outbound_byte_capacity = get(ENV_OUTBOUND_BYTE_CAPACITY);
        let market_cache_max_symbols = get(ENV_MARKET_CACHE_MAX_SYMBOLS);
        let market_cache_max_bytes = get(ENV_MARKET_CACHE_MAX_BYTES);
        let rolling_cache_dir = get(ENV_ROLLING_CACHE_DIR);
        let rolling_cache_session_hash = get(ENV_ROLLING_CACHE_SESSION_HASH);
        let rolling_cache_algorithm_version = get(ENV_ROLLING_CACHE_ALGORITHM_VERSION);
        let history_root = get(ENV_HISTORY_ROOT);
        let history_cache_dir = get(ENV_HISTORY_CACHE_DIR);
        let relay = RelayConfig::from_env_vars(|key| {
            if matches!(key, ENV_FUTURES_UNIVERSE | ENV_FUTURES_UNIVERSE_FILES) {
                None
            } else {
                get(key)
            }
        })?;
        let mut config = Self::new(relay);
        if let Some(value) = outbound_byte_capacity {
            config.resource_limits.outbound_byte_capacity =
                parse_positive_usize_env(ENV_OUTBOUND_BYTE_CAPACITY, &value)?;
        }
        if let Some(value) = market_cache_max_symbols {
            config.resource_limits.market_cache.max_symbols =
                parse_positive_usize_env(ENV_MARKET_CACHE_MAX_SYMBOLS, &value)?;
        }
        if let Some(value) = market_cache_max_bytes {
            config.resource_limits.market_cache.max_retained_bytes =
                parse_positive_usize_env(ENV_MARKET_CACHE_MAX_BYTES, &value)?;
        }
        if let Some(universe) = universe {
            config.set_futures_universe(&universe)?;
        }
        if let Some(universe_files) = universe_files {
            config.futures_universe_symbol_files = std::env::split_paths(&universe_files).collect();
        }
        match (rolling_cache_dir, rolling_cache_session_hash) {
            (None, None) => {}
            (Some(root), Some(session_hash)) => {
                let algorithm_version = rolling_cache_algorithm_version
                    .as_deref()
                    .map(|value| {
                        parse_positive_usize_env(ENV_ROLLING_CACHE_ALGORITHM_VERSION, value)
                            .and_then(|value| {
                                u32::try_from(value).map_err(|_| {
                                    RelayError::invalid_config(format!(
                                        "{ENV_ROLLING_CACHE_ALGORITHM_VERSION} exceeds u32"
                                    ))
                                })
                            })
                    })
                    .transpose()?
                    .unwrap_or(1);
                config.rolling_cache = Some(RollingCacheConfig::new(
                    root,
                    session_hash,
                    algorithm_version,
                )?);
            }
            _ => {
                return Err(RelayError::invalid_config(format!(
                    "{ENV_ROLLING_CACHE_DIR} and {ENV_ROLLING_CACHE_SESSION_HASH} must be configured together"
                )));
            }
        }
        if let Some(rolling) = config.rolling_cache.as_ref() {
            for history_root in [history_root, history_cache_dir].into_iter().flatten() {
                reject_overlapping_roots(rolling.root(), PathBuf::from(history_root).as_path())?;
            }
        }
        config.validate()?;
        Ok(config)
    }

    #[must_use]
    pub const fn relay_config(&self) -> &RelayConfig {
        &self.relay
    }

    #[must_use]
    pub const fn resource_limits(&self) -> RelayResourceLimits {
        self.resource_limits
    }

    #[must_use]
    pub fn rolling_cache(&self) -> Option<&RollingCacheConfig> {
        self.rolling_cache.as_ref()
    }

    pub fn with_rolling_cache(mut self, rolling_cache: RollingCacheConfig) -> RelayResult<Self> {
        rolling_cache.validate()?;
        self.rolling_cache = Some(rolling_cache);
        self.validate()?;
        Ok(self)
    }

    pub fn with_resource_limits(
        mut self,
        resource_limits: RelayResourceLimits,
    ) -> RelayResult<Self> {
        resource_limits.validate(self.relay.tick_ring_capacity)?;
        self.resource_limits = resource_limits;
        Ok(self)
    }

    #[must_use]
    pub fn into_relay_config(self) -> RelayConfig {
        self.relay
    }

    #[must_use]
    pub const fn futures_universe_spec(&self) -> Option<&UniverseSpec> {
        self.futures_universe_spec.as_ref()
    }

    #[must_use]
    pub fn futures_universe_symbol_files(&self) -> &[PathBuf] {
        &self.futures_universe_symbol_files
    }

    pub fn with_futures_universe(mut self, expression: impl AsRef<str>) -> RelayResult<Self> {
        self.set_futures_universe(expression.as_ref())?;
        Ok(self)
    }

    pub fn with_futures_universe_spec(mut self, spec: UniverseSpec) -> RelayResult<Self> {
        if spec.mode() != crate::universe_expression::UniverseMode::Snapshot {
            return Err(RelayError::invalid_config(
                "futures universe spec is a snapshot-only entry point",
            ));
        }
        self.relay.futures_universe_expression = None;
        self.futures_universe_spec = Some(spec);
        Ok(self)
    }

    #[must_use]
    pub fn universe_symbol_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.futures_universe_symbol_files.push(path.into());
        self
    }

    #[must_use]
    pub fn universe_symbol_files<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.futures_universe_symbol_files
            .extend(paths.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn has_upstream_futures_source(&self) -> bool {
        self.relay.has_upstream_futures_source()
            || self.futures_universe_spec.is_some()
            || !self.futures_universe_symbol_files.is_empty()
    }

    #[must_use]
    pub fn refreshes_futures_universe(&self) -> bool {
        self.has_upstream_futures_source()
    }

    pub fn validate(&self) -> RelayResult<()> {
        self.relay.validate()?;
        self.resource_limits
            .validate(self.relay.tick_ring_capacity)?;
        if self
            .futures_universe_spec
            .as_ref()
            .is_some_and(|spec| spec.mode() != crate::universe_expression::UniverseMode::Snapshot)
        {
            return Err(RelayError::invalid_config(
                "futures universe spec is a snapshot-only entry point",
            ));
        }
        if let Some(rolling_cache) = self.rolling_cache.as_ref() {
            rolling_cache.validate()?;
        }
        Ok(())
    }

    fn set_futures_universe(&mut self, expression: &str) -> RelayResult<()> {
        match parse_snapshot_universe_compatible(expression)
            .map_err(|error| RelayError::invalid_config(error.to_string()))?
        {
            SnapshotUniverseDispatch::Legacy { expression, .. } => {
                self.relay.futures_universe_expression = Some(expression);
                self.futures_universe_spec = None;
            }
            SnapshotUniverseDispatch::V2 { spec, .. } => {
                self.relay.futures_universe_expression = None;
                self.futures_universe_spec = Some(spec);
            }
            _ => unreachable!("snapshot universe dispatch is non-exhaustive"),
        }
        Ok(())
    }
}

fn reject_overlapping_roots(
    rolling_root: &PathBuf,
    history_root: &std::path::Path,
) -> RelayResult<()> {
    let rolling = fs::canonicalize(rolling_root).map_err(|error| {
        RelayError::invalid_config(format!(
            "rolling cache root {} cannot be canonicalized: {error}",
            rolling_root.display()
        ))
    })?;
    let history = fs::canonicalize(history_root).map_err(|error| {
        RelayError::invalid_config(format!(
            "history root {} cannot be canonicalized: {error}",
            history_root.display()
        ))
    })?;
    if rolling == history || rolling.starts_with(&history) || history.starts_with(&rolling) {
        return Err(RelayError::invalid_config(format!(
            "rolling cache root {} overlaps history root {}",
            rolling.display(),
            history.display()
        )));
    }
    Ok(())
}

fn upstream_tick_chart_id(symbol: &str, view_width: usize) -> String {
    format!(
        "{UPSTREAM_TICK_CHART_ID_PREFIX}-{}-{view_width}",
        sanitize_chart_token(symbol)
    )
}

fn sanitize_chart_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn parse_time_part(value: &str) -> RelayResult<u32> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(RelayError::invalid_config(
            "daily refresh time must be HH:MM[:SS]",
        ));
    }
    value
        .parse::<u32>()
        .map_err(|_| RelayError::invalid_config("daily refresh time must be HH:MM[:SS]"))
}

fn parse_positive_usize_env(name: &str, value: &str) -> RelayResult<usize> {
    let parsed = value.trim().parse::<usize>().map_err(|err| {
        RelayError::invalid_config(format!("{name} must be positive integer chars: {err}"))
    })?;
    if parsed == 0 {
        return Err(RelayError::invalid_config(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_bool_env(name: &str, value: &str) -> RelayResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(RelayError::invalid_config(format!(
            "{name} must be boolean: use true/false or 1/0"
        ))),
    }
}
