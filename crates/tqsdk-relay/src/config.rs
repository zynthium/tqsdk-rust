#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::{RelayError, RelayResult};
use crate::universe::{FuturesProductCode, FuturesProductFilter};
use crate::upstream::UpstreamTickChart;

const SECONDS_PER_DAY: u64 = 86_400;
const UPSTREAM_TICK_CHART_ID: &str = "relay-upstream-all-futures-ticks";
const UPSTREAM_TICK_VIEW_WIDTH: usize = 10_000;
const ENV_UPSTREAM_MARKET_URL: &str = "TQSDK_RELAY_UPSTREAM_MARKET_URL";
const ENV_DOWNSTREAM_LISTEN: &str = "TQSDK_RELAY_DOWNSTREAM_LISTEN";
const ENV_METRICS_LISTEN: &str = "TQSDK_RELAY_METRICS_LISTEN";
const ENV_FUTURES_SYMBOLS: &str = "TQSDK_RELAY_FUTURES_SYMBOLS";
const ENV_FUTURES_SYMBOLS_FILE: &str = "TQSDK_RELAY_FUTURES_SYMBOLS_FILE";
const ENV_FUTURES_PRODUCTS: &str = "TQSDK_RELAY_FUTURES_PRODUCTS";
const ENV_FUTURES_UNIVERSE_REFRESH_AT: &str = "TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_AT";
const ENV_FUTURES_UNIVERSE_REFRESH_SECS: &str = "TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_SECS";
const ENV_FUTURES_METADATA_BATCH_SIZE: &str = "TQSDK_RELAY_FUTURES_METADATA_BATCH_SIZE";
const ENV_UPSTREAM_INS_LIST_WARN_CHARS: &str = "TQSDK_RELAY_UPSTREAM_INS_LIST_WARN_CHARS";
const ENV_UPSTREAM_INS_LIST_MAX_CHARS: &str = "TQSDK_RELAY_UPSTREAM_INS_LIST_MAX_CHARS";
const ENV_DRY_RUN: &str = "TQSDK_RELAY_DRY_RUN";
const ENV_AUTH_USER: &str = "TQ_AUTH_USER";
const ENV_AUTH_PASS: &str = "TQ_AUTH_PASS";
pub const DEFAULT_FUTURES_METADATA_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuturesUniverseRefreshSchedule {
    Daily(DailyRefreshTime),
    Interval(Duration),
}

impl Default for FuturesUniverseRefreshSchedule {
    fn default() -> Self {
        Self::Daily(DailyRefreshTime::from_hms(8, 30, 0).expect("valid default refresh time"))
    }
}

impl FuturesUniverseRefreshSchedule {
    #[must_use]
    pub fn delay_from_seconds_after_midnight(
        self,
        current_seconds_after_midnight: u32,
    ) -> Duration {
        match self {
            Self::Daily(refresh_at) => {
                next_daily_refresh_delay(current_seconds_after_midnight, refresh_at)
            }
            Self::Interval(interval) => interval,
        }
    }

    fn validate(self) -> RelayResult<()> {
        if let Self::Interval(interval) = self
            && interval == Duration::ZERO
        {
            return Err(RelayError::invalid_config(
                "futures_universe_refresh interval must be greater than zero",
            ));
        }
        Ok(())
    }
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
            let suggested = self.suggested_shards(chars).unwrap_or(1);
            return Err(RelayError::invalid_config(format!(
                "upstream ins_list length {chars} exceeds hard limit {max_chars} chars; suggest splitting into at least {suggested} relay instances"
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
    pub futures_symbols: Vec<String>,
    pub futures_product_filter: FuturesProductFilter,
    pub upstream_ins_list_limits: UpstreamInsListLimits,
    pub tick_ring_capacity: usize,
    pub kline_ring_capacity: usize,
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
            .field("futures_symbols", &self.futures_symbols)
            .field("futures_product_filter", &self.futures_product_filter)
            .field("upstream_ins_list_limits", &self.upstream_ins_list_limits)
            .field("tick_ring_capacity", &self.tick_ring_capacity)
            .field("kline_ring_capacity", &self.kline_ring_capacity)
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
            futures_symbols: Vec::new(),
            futures_product_filter: FuturesProductFilter::None,
            upstream_ins_list_limits: UpstreamInsListLimits::default(),
            tick_ring_capacity: 200_000,
            kline_ring_capacity: 10_000,
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
        let refresh_at = get(ENV_FUTURES_UNIVERSE_REFRESH_AT);
        let refresh_secs = get(ENV_FUTURES_UNIVERSE_REFRESH_SECS);
        if refresh_at.is_some() && refresh_secs.is_some() {
            return Err(RelayError::invalid_config(format!(
                "set only one of {ENV_FUTURES_UNIVERSE_REFRESH_AT} or {ENV_FUTURES_UNIVERSE_REFRESH_SECS}"
            )));
        }
        if let Some(value) = refresh_at {
            let refresh_at = DailyRefreshTime::parse(&value).map_err(|_| {
                RelayError::invalid_config(format!(
                    "{ENV_FUTURES_UNIVERSE_REFRESH_AT} must be HH:MM[:SS]"
                ))
            })?;
            config.futures_universe_refresh = FuturesUniverseRefreshSchedule::Daily(refresh_at);
        }
        if let Some(value) = refresh_secs {
            let seconds = value.trim().parse::<u64>().map_err(|err| {
                RelayError::invalid_config(format!(
                    "{ENV_FUTURES_UNIVERSE_REFRESH_SECS} must be seconds: {err}"
                ))
            })?;
            config.futures_universe_refresh =
                FuturesUniverseRefreshSchedule::Interval(Duration::from_secs(seconds));
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
        let inline_futures_symbols = get(ENV_FUTURES_SYMBOLS);
        let futures_symbols_file = get(ENV_FUTURES_SYMBOLS_FILE);
        let futures_products = get(ENV_FUTURES_PRODUCTS);
        let configured_universe_sources = usize::from(inline_futures_symbols.is_some())
            + usize::from(futures_symbols_file.is_some())
            + usize::from(futures_products.is_some());
        if inline_futures_symbols.is_some()
            && futures_symbols_file.is_some()
            && futures_products.is_none()
        {
            return Err(RelayError::invalid_config(format!(
                "set only one of {ENV_FUTURES_SYMBOLS} or {ENV_FUTURES_SYMBOLS_FILE}"
            )));
        }
        if configured_universe_sources > 1 {
            return Err(RelayError::invalid_config(
                "set only one futures universe source",
            ));
        }
        if let Some(value) = inline_futures_symbols {
            config.futures_symbols = parse_futures_symbols(&value);
        }
        if let Some(path) = futures_symbols_file {
            let contents = std::fs::read_to_string(&path).map_err(|err| {
                RelayError::invalid_config(format!(
                    "failed to read {ENV_FUTURES_SYMBOLS_FILE} {path}: {err}"
                ))
            })?;
            config.futures_symbols = parse_futures_symbols(&contents);
        }
        if let Some(value) = futures_products {
            config.futures_product_filter = parse_futures_product_filter(&value)?;
        }
        config.validate()?;
        Ok(config)
    }

    pub fn upstream_tick_chart(&self) -> RelayResult<Option<UpstreamTickChart>> {
        self.upstream_tick_chart_for_symbols(self.futures_symbols.iter().map(String::as_str))
    }

    pub fn upstream_tick_chart_for_symbols<'a, I>(
        &self,
        symbols: I,
    ) -> RelayResult<Option<UpstreamTickChart>>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let symbols: Vec<&str> = symbols.into_iter().collect();
        if symbols.is_empty() {
            return Ok(None);
        }
        let chart =
            UpstreamTickChart::new(UPSTREAM_TICK_CHART_ID, symbols, UPSTREAM_TICK_VIEW_WIDTH)?;
        self.upstream_ins_list_limits
            .validate_ins_list_chars(chart.ins_list_chars())?;
        Ok(Some(chart))
    }

    #[must_use]
    pub fn has_upstream_futures_source(&self) -> bool {
        !self.futures_symbols.is_empty()
            || self.futures_product_filter != FuturesProductFilter::None
    }

    #[must_use]
    pub fn refreshes_futures_universe(&self) -> bool {
        self.futures_product_filter != FuturesProductFilter::None
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
        self.futures_universe_refresh.validate()?;
        self.upstream_ins_list_limits.validate()?;
        if self.futures_metadata_batch_size == 0 {
            return Err(RelayError::invalid_config(
                "futures_metadata_batch_size must be greater than zero",
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
        if self
            .futures_symbols
            .iter()
            .any(|symbol| symbol.trim().is_empty())
        {
            return Err(RelayError::invalid_config(
                "futures_symbols must not contain empty symbols",
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

fn parse_futures_symbols(value: &str) -> Vec<String> {
    value
        .lines()
        .flat_map(|line| line.split(','))
        .map(|symbol| symbol.trim().to_string())
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

fn parse_futures_product_filter(value: &str) -> RelayResult<FuturesProductFilter> {
    let products: Vec<String> = value
        .lines()
        .flat_map(|line| line.split(','))
        .map(|product| product.trim().to_string())
        .collect();
    if products.len() == 1 && (products[0] == "*" || products[0].eq_ignore_ascii_case("all")) {
        return Ok(FuturesProductFilter::All);
    }
    if products.iter().any(String::is_empty) {
        return Err(RelayError::invalid_config(
            "futures_products must not contain empty entries",
        ));
    }
    products
        .into_iter()
        .map(|product| FuturesProductCode::parse(&product))
        .collect::<RelayResult<Vec<_>>>()
        .map(FuturesProductFilter::Products)
}
