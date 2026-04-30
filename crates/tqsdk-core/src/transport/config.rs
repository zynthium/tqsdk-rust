use std::time::Duration;

use crate::ids::{AccountId, ProtocolDomain};

const DEFAULT_AUTH_URL: &str = "https://auth.shinnytech.com";

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_env_or_default(name: &str, default: &str) -> String {
    read_optional_env(name).unwrap_or_else(|| default.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    pub auth_url: Option<String>,
    pub market_url: Option<String>,
    pub trade_url: Option<String>,
    pub query_url: Option<String>,
    pub replay_url: Option<String>,
    pub schema_url: Option<String>,
}

impl EndpointConfig {
    pub fn new(auth_url: impl Into<String>) -> Self {
        Self {
            auth_url: Some(auth_url.into()),
            market_url: None,
            trade_url: None,
            query_url: None,
            replay_url: None,
            schema_url: None,
        }
    }

    pub fn from_env() -> Self {
        Self {
            auth_url: Some(read_env_or_default("TQ_AUTH_URL", DEFAULT_AUTH_URL)),
            market_url: read_optional_env("TQ_MD_URL"),
            trade_url: read_optional_env("TQ_TD_URL"),
            query_url: None,
            replay_url: None,
            schema_url: None,
        }
    }

    pub fn with_market_url(mut self, market_url: impl Into<String>) -> Self {
        self.market_url = Some(market_url.into());
        self
    }

    pub fn with_trade_url(mut self, trade_url: impl Into<String>) -> Self {
        self.trade_url = Some(trade_url.into());
        self
    }

    pub fn with_query_url(mut self, query_url: impl Into<String>) -> Self {
        self.query_url = Some(query_url.into());
        self
    }

    pub fn with_replay_url(mut self, replay_url: impl Into<String>) -> Self {
        self.replay_url = Some(replay_url.into());
        self
    }

    pub fn with_schema_url(mut self, schema_url: impl Into<String>) -> Self {
        self.schema_url = Some(schema_url.into());
        self
    }
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    pub interval: Duration,
    pub timeout: Duration,
}

impl HeartbeatPolicy {
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout }
    }
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: Option<u32>,
}

impl ReconnectPolicy {
    pub fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        max_attempts: Option<u32>,
    ) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts,
        }
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            max_attempts: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub endpoints: EndpointConfig,
    pub heartbeat: HeartbeatPolicy,
    pub reconnect: ReconnectPolicy,
    pub market_target: MarketSessionTarget,
    pub trade_targets: Vec<TradeSessionTarget>,
    pub enabled_domains: Vec<ProtocolDomain>,
}

impl SessionConfig {
    pub fn new(endpoints: EndpointConfig) -> Self {
        Self {
            endpoints,
            heartbeat: HeartbeatPolicy::default(),
            reconnect: ReconnectPolicy::default(),
            market_target: MarketSessionTarget::default(),
            trade_targets: Vec::new(),
            enabled_domains: Vec::new(),
        }
    }

    pub fn with_heartbeat(mut self, heartbeat: HeartbeatPolicy) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    pub fn with_reconnect(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn with_market_target(mut self, market_target: MarketSessionTarget) -> Self {
        self.market_target = market_target;
        self
    }

    pub fn add_trade_target(mut self, trade_target: TradeSessionTarget) -> Self {
        self.trade_targets.push(trade_target);
        self
    }

    pub fn enable_domain(mut self, domain: ProtocolDomain) -> Self {
        if !self.enabled_domains.contains(&domain) {
            self.enabled_domains.push(domain);
        }
        self
    }

    pub fn enabled_domains(&self) -> &[ProtocolDomain] {
        &self.enabled_domains
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSessionTarget {
    pub stock: bool,
    pub backtest: bool,
}

impl MarketSessionTarget {
    pub const fn new(stock: bool, backtest: bool) -> Self {
        Self { stock, backtest }
    }

    pub const fn stock_live() -> Self {
        Self::new(true, false)
    }

    pub const fn futures_live() -> Self {
        Self::new(false, false)
    }

    pub const fn stock_backtest() -> Self {
        Self::new(true, true)
    }

    pub const fn futures_backtest() -> Self {
        Self::new(false, true)
    }
}

impl Default for MarketSessionTarget {
    fn default() -> Self {
        Self::stock_live()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeSessionTarget {
    pub broker_id: String,
    pub account_id: AccountId,
    pub trade_url: Option<String>,
    pub auth_derived: Option<AuthDerivedTradeTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDerivedTradeTarget {
    TqKqFuture { number: Option<u8> },
    TqKqStock { number: Option<u8> },
}

impl TradeSessionTarget {
    pub fn new(broker_id: impl Into<String>, account_id: AccountId) -> Self {
        Self {
            broker_id: broker_id.into(),
            account_id,
            trade_url: None,
            auth_derived: None,
        }
    }

    pub fn with_trade_url(mut self, trade_url: impl Into<String>) -> Self {
        self.trade_url = Some(trade_url.into());
        self
    }

    #[must_use]
    pub fn tqkq() -> Self {
        Self {
            broker_id: "快期模拟".to_string(),
            account_id: AccountId::new(String::new()),
            trade_url: None,
            auth_derived: Some(AuthDerivedTradeTarget::TqKqFuture { number: None }),
        }
    }

    #[must_use]
    pub fn tqkq_numbered(number: u8) -> Self {
        Self {
            broker_id: "快期模拟".to_string(),
            account_id: AccountId::new(String::new()),
            trade_url: None,
            auth_derived: Some(AuthDerivedTradeTarget::TqKqFuture {
                number: Some(number),
            }),
        }
    }

    #[must_use]
    pub fn tqkq_stock() -> Self {
        Self {
            broker_id: "快期股票模拟".to_string(),
            account_id: AccountId::new(String::new()),
            trade_url: None,
            auth_derived: Some(AuthDerivedTradeTarget::TqKqStock { number: None }),
        }
    }

    #[must_use]
    pub fn tqkq_stock_numbered(number: u8) -> Self {
        Self {
            broker_id: "快期股票模拟".to_string(),
            account_id: AccountId::new(String::new()),
            trade_url: None,
            auth_derived: Some(AuthDerivedTradeTarget::TqKqStock {
                number: Some(number),
            }),
        }
    }

    #[must_use]
    pub fn is_auth_derived(&self) -> bool {
        self.auth_derived.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::MarketSessionTarget;

    #[test]
    fn market_session_target_named_constructors_are_explicit() {
        assert_eq!(
            MarketSessionTarget::stock_live(),
            MarketSessionTarget::new(true, false)
        );
        assert_eq!(
            MarketSessionTarget::futures_live(),
            MarketSessionTarget::new(false, false)
        );
        assert_eq!(
            MarketSessionTarget::stock_backtest(),
            MarketSessionTarget::new(true, true)
        );
        assert_eq!(
            MarketSessionTarget::futures_backtest(),
            MarketSessionTarget::new(false, true)
        );
    }
}
