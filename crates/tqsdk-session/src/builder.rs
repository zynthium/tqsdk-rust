#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{
    AccountId, AdapterRegistry, AuthDerivedTradeTarget, EndpointConfig, MarketSessionTarget,
    ProtocolDomain, RuntimeHandle, SessionConfig, TradeSessionTarget,
};

#[cfg(feature = "services")]
use crate::services::SessionServiceEndpoints;
use crate::{
    client::{SessionClient, SessionClientContext},
    error::Result,
};

const DEFAULT_SCHEMA_BASE_URL: &str = "https://files.shinnytech.com";

/// Builder for a shared [`SessionClient`] substrate.
///
/// This builder owns endpoint selection and enabled protocol domains, but it
/// deliberately stays below any wait/fan-out facade style.
#[derive(Debug, Clone)]
pub struct SessionClientBuilder {
    auth_user: String,
    auth_pass: String,
    endpoints: EndpointConfig,
    #[cfg(feature = "services")]
    service_endpoints: SessionServiceEndpoints,
    query_enabled: bool,
    market_target: MarketSessionTarget,
    trade_targets: Vec<TradeSessionTarget>,
    commit_log_retention: Option<usize>,
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
            #[cfg(feature = "services")]
            service_endpoints: SessionServiceEndpoints::default(),
            query_enabled: false,
            market_target: MarketSessionTarget::stock_live(),
            trade_targets: Vec::new(),
            commit_log_retention: None,
        }
    }

    /// Overrides the target retained runtime commit count for this session.
    ///
    /// Values below one are clamped to one. The default runtime retention is
    /// preserved when this method is not called. An active lagging cursor can
    /// temporarily keep more commits so required revisions are not truncated.
    #[must_use]
    pub fn commit_log_retention(mut self, max_entries: usize) -> Self {
        self.commit_log_retention = Some(max_entries.max(1));
        self
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
    pub fn market_url(mut self, market_url: impl Into<String>) -> Self {
        self.endpoints = self.endpoints.with_market_url(market_url);
        self
    }

    #[must_use]
    pub fn market_relay(self, relay_url: impl Into<String>) -> Self {
        self.market_url(relay_url)
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
    #[cfg(feature = "services")]
    pub fn holiday_url(mut self, holiday_url: impl Into<String>) -> Self {
        self.service_endpoints = self.service_endpoints.with_holiday_url(holiday_url);
        self
    }

    #[must_use]
    #[deprecated(
        note = "use stock_market, futures_market, stock_backtest_market, or futures_backtest_market instead"
    )]
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
    pub fn trade_target_tqkq(mut self) -> Self {
        self.trade_targets.push(TradeSessionTarget::tqkq());
        self
    }

    #[must_use]
    pub fn trade_target_tqkq_numbered(mut self, number: u8) -> Self {
        self.trade_targets
            .push(TradeSessionTarget::tqkq_numbered(number));
        self
    }

    #[must_use]
    pub fn trade_target_tqkq_stock(mut self) -> Self {
        self.trade_targets.push(TradeSessionTarget::tqkq_stock());
        self
    }

    #[must_use]
    pub fn trade_target_tqkq_stock_numbered(mut self, number: u8) -> Self {
        self.trade_targets
            .push(TradeSessionTarget::tqkq_stock_numbered(number));
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
            #[cfg(feature = "services")]
            service_endpoints,
            query_enabled,
            market_target,
            trade_targets,
            commit_log_retention,
        } = self;
        validate_trade_targets(&trade_targets)?;
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = match commit_log_retention {
            Some(max_entries) => {
                RuntimeHandle::with_adapters_and_commit_log_retention(adapters, max_entries)
            }
            None => RuntimeHandle::with_adapters(adapters),
        };
        let config = session_config(
            endpoints.clone(),
            query_enabled,
            market_target,
            &trade_targets,
        );
        #[cfg(feature = "services")]
        let context = SessionClientContext::new_with_service_endpoints(
            auth_user,
            auth_pass,
            endpoints,
            service_endpoints,
        );
        #[cfg(not(feature = "services"))]
        let context = SessionClientContext::new(auth_user, auth_pass, endpoints);
        SessionClient::new_live(handle, context, config, trade_targets)
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __tqsdk_impl_session_builder_forwarders {
    () => {
        #[must_use]
        pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
            Self::from_session_builder($crate::SessionClientBuilder::new(auth_user, auth_pass))
        }

        #[must_use]
        #[deprecated(
            note = "use stock_market, futures_market, stock_backtest_market, or futures_backtest_market instead"
        )]
        #[allow(deprecated)]
        pub fn market_target(mut self, stock: bool, backtest: bool) -> Self {
            self.inner = self.inner.market_target(stock, backtest);
            self
        }

        #[must_use]
        pub fn stock_market(mut self) -> Self {
            self.inner = self.inner.stock_market();
            self
        }

        #[must_use]
        pub fn futures_market(mut self) -> Self {
            self.inner = self.inner.futures_market();
            self
        }

        #[must_use]
        pub fn stock_backtest_market(mut self) -> Self {
            self.inner = self.inner.stock_backtest_market();
            self
        }

        #[must_use]
        pub fn futures_backtest_market(mut self) -> Self {
            self.inner = self.inner.futures_backtest_market();
            self
        }

        #[must_use]
        pub fn trade_target(
            mut self,
            broker_id: impl Into<String>,
            account_id: impl Into<String>,
        ) -> Self {
            self.inner = self.inner.trade_target(broker_id, account_id);
            self
        }

        #[must_use]
        pub fn trade_target_tqkq(mut self) -> Self {
            self.inner = self.inner.trade_target_tqkq();
            self
        }

        #[must_use]
        pub fn trade_target_tqkq_numbered(mut self, number: u8) -> Self {
            self.inner = self.inner.trade_target_tqkq_numbered(number);
            self
        }

        #[must_use]
        pub fn trade_target_tqkq_stock(mut self) -> Self {
            self.inner = self.inner.trade_target_tqkq_stock();
            self
        }

        #[must_use]
        pub fn trade_target_tqkq_stock_numbered(mut self, number: u8) -> Self {
            self.inner = self.inner.trade_target_tqkq_stock_numbered(number);
            self
        }

        #[must_use]
        pub fn trade_target_with_url(
            mut self,
            broker_id: impl Into<String>,
            account_id: impl Into<String>,
            trade_url: impl Into<String>,
        ) -> Self {
            self.inner = self
                .inner
                .trade_target_with_url(broker_id, account_id, trade_url);
            self
        }

        #[must_use]
        pub fn replay_url(mut self, replay_url: impl Into<String>) -> Self {
            self.inner = self.inner.replay_url(replay_url);
            self
        }

        #[must_use]
        #[cfg(feature = "services")]
        pub fn holiday_url(mut self, holiday_url: impl Into<String>) -> Self {
            self.inner = self.inner.holiday_url(holiday_url);
            self
        }

        #[must_use]
        pub fn market_url(mut self, market_url: impl Into<String>) -> Self {
            self.inner = self.inner.market_url(market_url);
            self
        }

        #[must_use]
        pub fn market_relay(mut self, relay_url: impl Into<String>) -> Self {
            self.inner = self.inner.market_relay(relay_url);
            self
        }
    };
}

fn validate_trade_targets(trade_targets: &[TradeSessionTarget]) -> Result<()> {
    for target in trade_targets {
        match target.auth_derived {
            Some(AuthDerivedTradeTarget::TqKqFuture {
                number: Some(number),
            })
            | Some(AuthDerivedTradeTarget::TqKqStock {
                number: Some(number),
            }) => validate_tqkq_number(number)?,
            Some(
                AuthDerivedTradeTarget::TqKqFuture { number: None }
                | AuthDerivedTradeTarget::TqKqStock { number: None },
            )
            | None => {}
        }
    }

    Ok(())
}

fn validate_tqkq_number(number: u8) -> Result<()> {
    if (1..=99).contains(&number) {
        Ok(())
    } else {
        Err(tqsdk_core::ContractError::validation(format!(
            "TqKq assistant account number must be within 1..=99, got {number}"
        ))
        .into())
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tqsdk_core::{CommitScope, InputPayload, IoEvent, ProtocolDomain, Revision, RuntimeInput};

    use super::SessionClientBuilder;

    #[test]
    fn configured_commit_log_retention_bounds_built_session() {
        let session = SessionClientBuilder::new("test-user", "test-pass")
            .commit_log_retention(2)
            .build()
            .unwrap();

        for last_price in [601.0, 602.0, 603.0] {
            session
                .handle()
                .ingest(
                    RuntimeInput::Io(IoEvent {
                        route: "market.shared".to_string(),
                        domains: vec![ProtocolDomain::Market],
                        payload: InputPayload::Json(json!({
                            "aid": "rtn_data",
                            "data": [{
                                "quotes": {
                                    "SHFE.au2602": {"last_price": last_price}
                                }
                            }]
                        })),
                    }),
                    vec![],
                    CommitScope::RealtimeUpdate,
                )
                .unwrap();
        }

        let reader = session.reader();
        let mut stale_cursor = session.handle().cursor_from(Revision::new(1));
        let mut retained_cursor = session.handle().cursor_from(Revision::new(2));
        assert!(reader.next(&mut stale_cursor).is_none());
        assert_eq!(
            reader.next(&mut retained_cursor).unwrap().revision,
            Revision::new(2)
        );
        assert_eq!(
            reader.next(&mut retained_cursor).unwrap().revision,
            Revision::new(3)
        );
    }
}
