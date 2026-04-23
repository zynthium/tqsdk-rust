#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{
    AccountId, AdapterRegistry, EndpointConfig, MarketSessionTarget, ProtocolDomain, RuntimeHandle,
    SessionConfig, TradeSessionTarget,
};

use crate::{
    client::{SessionClient, SessionClientContext},
    config::SessionFacadeConfig,
    error::Result,
};

const DEFAULT_SCHEMA_BASE_URL: &str = "https://files.shinnytech.com";

/// Builder for a shared [`SessionClient`] substrate.
///
/// This builder owns endpoint selection and enabled protocol domains, but it
/// deliberately stays below any wait/stream facade style.
#[derive(Debug, Clone)]
pub struct SessionClientBuilder {
    auth_user: String,
    auth_pass: String,
    endpoints: EndpointConfig,
    query_enabled: bool,
    facade_config: SessionFacadeConfig,
    market_target: MarketSessionTarget,
    trade_targets: Vec<TradeSessionTarget>,
}

impl SessionClientBuilder {
    #[must_use]
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        let endpoints = EndpointConfig::from_env();
        let endpoints = if endpoints.schema_url.is_some() {
            endpoints
        } else {
            endpoints.with_schema_url(DEFAULT_SCHEMA_BASE_URL)
        };
        Self {
            auth_user: auth_user.into(),
            auth_pass: auth_pass.into(),
            endpoints,
            query_enabled: false,
            facade_config: SessionFacadeConfig::default(),
            market_target: MarketSessionTarget::stock_live(),
            trade_targets: Vec::new(),
        }
    }

    #[must_use]
    pub fn facade_config(mut self, facade_config: SessionFacadeConfig) -> Self {
        self.facade_config = facade_config;
        self
    }

    #[must_use]
    pub fn facade_config_ref(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }

    #[must_use]
    pub fn query_enabled(&self) -> bool {
        self.query_enabled
    }

    #[must_use]
    pub fn query_url(mut self, query_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_query_url(query_url);
        self.query_enabled = true;
        self
    }

    #[must_use]
    pub fn enable_query(mut self) -> Self {
        self.query_enabled = true;
        self
    }

    #[must_use]
    pub fn schema_url(mut self, schema_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_schema_url(schema_url);
        self
    }

    #[must_use]
    pub fn replay_url(mut self, replay_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_replay_url(replay_url);
        self
    }

    #[must_use]
    pub fn market_target(mut self, stock: bool, backtest: bool) -> Self {
        self.market_target = MarketSessionTarget::new(stock, backtest);
        self
    }

    #[must_use]
    pub fn stock_market(self) -> Self {
        Self {
            market_target: MarketSessionTarget::stock_live(),
            ..self
        }
    }

    #[must_use]
    pub fn futures_market(self) -> Self {
        Self {
            market_target: MarketSessionTarget::futures_live(),
            ..self
        }
    }

    #[must_use]
    pub fn stock_backtest_market(self) -> Self {
        Self {
            market_target: MarketSessionTarget::stock_backtest(),
            ..self
        }
    }

    #[must_use]
    pub fn futures_backtest_market(self) -> Self {
        Self {
            market_target: MarketSessionTarget::futures_backtest(),
            ..self
        }
    }

    #[must_use]
    pub fn trade_target(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(TradeSessionTarget::new(
            broker_id,
            AccountId::new(account_id.into()),
        ));
        self
    }

    #[must_use]
    pub fn trade_target_with_url(
        mut self,
        broker_id: impl Into<String>,
        account_id: impl Into<String>,
        trade_url: impl Into<String>,
    ) -> Self {
        self.trade_targets.push(
            TradeSessionTarget::new(broker_id, AccountId::new(account_id.into()))
                .with_trade_url(trade_url),
        );
        self
    }

    #[must_use]
    pub fn endpoints(&self) -> &EndpointConfig {
        &self.endpoints
    }

    #[must_use]
    pub fn market_target_ref(&self) -> &MarketSessionTarget {
        &self.market_target
    }

    #[must_use]
    pub fn trade_targets_ref(&self) -> &[TradeSessionTarget] {
        &self.trade_targets
    }

    pub fn build(self) -> Result<SessionClient> {
        let Self {
            auth_user,
            auth_pass,
            endpoints,
            query_enabled,
            facade_config,
            market_target,
            trade_targets,
        } = self;
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        let context = SessionClientContext::new(auth_user, auth_pass, endpoints);
        let config = session_config(
            context.endpoints.clone(),
            query_enabled,
            market_target,
            &trade_targets,
        );
        SessionClient::new_live(handle, facade_config, context, config, trade_targets)
    }
}

fn session_config(
    endpoints: EndpointConfig,
    query_enabled: bool,
    market_target: MarketSessionTarget,
    trade_targets: &[TradeSessionTarget],
) -> SessionConfig {
    let mut config = SessionConfig::new(endpoints).with_market_target(market_target);
    config = config
        .enable_domain(ProtocolDomain::Market)
        .enable_domain(ProtocolDomain::System);

    if query_enabled || config.endpoints.query_url.is_some() {
        config = config.enable_domain(ProtocolDomain::Query);
    }
    if config.endpoints.schema_url.is_some() {
        config = config.enable_domain(ProtocolDomain::Schema);
    }
    if config.endpoints.replay_url.is_some() {
        config = config.enable_domain(ProtocolDomain::Replay);
    }
    if !trade_targets.is_empty() {
        config = config.enable_domain(ProtocolDomain::Trade);
        for target in trade_targets {
            config = config.add_trade_target(target.clone());
        }
    }

    config
}
