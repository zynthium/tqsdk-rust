#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::{RelayError, RelayResult};
use crate::universe::{FuturesProductCode, FuturesProductFilter};
use crate::upstream::UpstreamTickChart;

const UPSTREAM_TICK_CHART_ID: &str = "relay-upstream-all-futures-ticks";
const UPSTREAM_TICK_VIEW_WIDTH: usize = 10_000;
const ENV_UPSTREAM_MARKET_URL: &str = "TQSDK_RELAY_UPSTREAM_MARKET_URL";
const ENV_DOWNSTREAM_LISTEN: &str = "TQSDK_RELAY_DOWNSTREAM_LISTEN";
const ENV_METRICS_LISTEN: &str = "TQSDK_RELAY_METRICS_LISTEN";
const ENV_FUTURES_SYMBOLS: &str = "TQSDK_RELAY_FUTURES_SYMBOLS";
const ENV_FUTURES_SYMBOLS_FILE: &str = "TQSDK_RELAY_FUTURES_SYMBOLS_FILE";
const ENV_FUTURES_PRODUCTS: &str = "TQSDK_RELAY_FUTURES_PRODUCTS";
const ENV_FUTURES_UNIVERSE_REFRESH_SECS: &str = "TQSDK_RELAY_FUTURES_UNIVERSE_REFRESH_SECS";
const ENV_AUTH_USER: &str = "TQ_AUTH_USER";
const ENV_AUTH_PASS: &str = "TQ_AUTH_PASS";

#[derive(Clone, PartialEq, Eq)]
pub struct RelayConfig {
    pub upstream_market_url: String,
    pub upstream_auth_user: Option<String>,
    pub upstream_auth_pass: Option<String>,
    pub downstream_listen: String,
    pub metrics_listen: String,
    pub futures_universe_refresh: Duration,
    pub futures_symbols: Vec<String>,
    pub futures_product_filter: FuturesProductFilter,
    pub tick_ring_capacity: usize,
    pub kline_ring_capacity: usize,
    pub disk_cache_dir: Option<PathBuf>,
    pub bootstrap: BootstrapConfig,
    pub best_effort_duration_tag: bool,
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
            .field("futures_symbols", &self.futures_symbols)
            .field("futures_product_filter", &self.futures_product_filter)
            .field("tick_ring_capacity", &self.tick_ring_capacity)
            .field("kline_ring_capacity", &self.kline_ring_capacity)
            .field("disk_cache_dir", &self.disk_cache_dir)
            .field("bootstrap", &self.bootstrap)
            .field("best_effort_duration_tag", &self.best_effort_duration_tag)
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
            futures_universe_refresh: Duration::from_secs(86_400),
            futures_symbols: Vec::new(),
            futures_product_filter: FuturesProductFilter::None,
            tick_ring_capacity: 200_000,
            kline_ring_capacity: 10_000,
            disk_cache_dir: None,
            bootstrap: BootstrapConfig::default(),
            best_effort_duration_tag: true,
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
        if let Some(value) = get(ENV_FUTURES_UNIVERSE_REFRESH_SECS) {
            let seconds = value.trim().parse::<u64>().map_err(|err| {
                RelayError::invalid_config(format!(
                    "{ENV_FUTURES_UNIVERSE_REFRESH_SECS} must be seconds: {err}"
                ))
            })?;
            config.futures_universe_refresh = Duration::from_secs(seconds);
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
        UpstreamTickChart::new(UPSTREAM_TICK_CHART_ID, symbols, UPSTREAM_TICK_VIEW_WIDTH).map(Some)
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
        if self.futures_universe_refresh == Duration::ZERO {
            return Err(RelayError::invalid_config(
                "futures_universe_refresh must be greater than zero",
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
