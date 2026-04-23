#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::api::TqApi;

/// Builder for the single-owner wait facade.
///
/// This is a thin convenience layer over [`tqsdk_session::SessionClientBuilder`]
/// for users who want Python-style `wait_update()` consumption.
#[derive(Debug, Clone)]
pub struct TqApiBuilder {
    inner: tqsdk_session::SessionClientBuilder,
}

impl TqApiBuilder {
    #[must_use]
    pub fn from_session_builder(inner: tqsdk_session::SessionClientBuilder) -> Self {
        Self { inner }
    }

    #[must_use]
    pub fn new(auth_user: impl Into<String>, auth_pass: impl Into<String>) -> Self {
        Self::from_session_builder(tqsdk_session::SessionClientBuilder::new(
            auth_user, auth_pass,
        ))
    }

    #[must_use]
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

    pub async fn build(self) -> crate::error::Result<TqApi> {
        let session = self.inner.build()?;
        Ok(TqApi::new(session))
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::MarketSessionTarget;

    use super::TqApiBuilder;

    #[test]
    fn from_session_builder_preserves_underlying_session_configuration() {
        let session_builder = tqsdk_session::SessionClientBuilder::new("demo-user", "demo-pass")
            .enable_query()
            .schema_url("https://schema.example.com/latest.json")
            .market_target(true, false)
            .trade_target("9999", "sim")
            .replay_url("replay-driver");

        let builder = TqApiBuilder::from_session_builder(session_builder);

        assert!(builder.inner.query_enabled());
        assert_eq!(
            builder.inner.endpoints().schema_url.as_deref(),
            Some("https://schema.example.com/latest.json")
        );
        assert_eq!(
            builder.inner.market_target_ref(),
            &MarketSessionTarget::stock_live()
        );
        assert_eq!(builder.inner.trade_targets_ref().len(), 1);
        assert_eq!(builder.inner.trade_targets_ref()[0].broker_id, "9999");
        assert_eq!(
            builder.inner.trade_targets_ref()[0].account_id.as_str(),
            "sim"
        );
        assert_eq!(
            builder.inner.endpoints().replay_url.as_deref(),
            Some("replay-driver")
        );
    }

    #[test]
    fn market_trade_and_replay_methods_forward_to_inner_session_builder() {
        let builder = TqApiBuilder::new("demo-user", "demo-pass")
            .market_target(true, true)
            .trade_target("9999", "sim")
            .trade_target_with_url("simnow", "paper", "wss://trade.example/ws")
            .replay_url("replay-driver");

        assert_eq!(
            builder.inner.market_target_ref(),
            &MarketSessionTarget::stock_backtest()
        );
        assert_eq!(builder.inner.trade_targets_ref().len(), 2);
        assert_eq!(builder.inner.trade_targets_ref()[0].broker_id, "9999");
        assert_eq!(
            builder.inner.trade_targets_ref()[0].account_id.as_str(),
            "sim"
        );
        assert_eq!(builder.inner.trade_targets_ref()[0].trade_url, None);
        assert_eq!(builder.inner.trade_targets_ref()[1].broker_id, "simnow");
        assert_eq!(
            builder.inner.trade_targets_ref()[1].account_id.as_str(),
            "paper"
        );
        assert_eq!(
            builder.inner.trade_targets_ref()[1].trade_url.as_deref(),
            Some("wss://trade.example/ws")
        );
        assert_eq!(
            builder.inner.endpoints().replay_url.as_deref(),
            Some("replay-driver")
        );
    }

    #[test]
    fn tqkq_trade_methods_forward_to_inner_session_builder() {
        let builder = TqApiBuilder::new("demo-user", "demo-pass")
            .trade_target_tqkq()
            .trade_target_tqkq_numbered(7);

        assert_eq!(builder.inner.trade_targets_ref().len(), 2);
        assert_eq!(builder.inner.trade_targets_ref()[0].broker_id, "快期模拟");
        assert!(builder.inner.trade_targets_ref()[0].is_auth_derived());
        assert_eq!(builder.inner.trade_targets_ref()[1].broker_id, "快期模拟");
        assert_eq!(
            builder.inner.trade_targets_ref()[1].auth_derived,
            Some(tqsdk_core::AuthDerivedTradeTarget::TqKqFuture { number: Some(7) })
        );
    }

    #[test]
    fn tqkq_stock_trade_methods_forward_to_inner_session_builder() {
        let builder = TqApiBuilder::new("demo-user", "demo-pass")
            .trade_target_tqkq_stock()
            .trade_target_tqkq_stock_numbered(7);

        assert_eq!(builder.inner.trade_targets_ref().len(), 2);
        assert_eq!(
            builder.inner.trade_targets_ref()[0].broker_id,
            "快期股票模拟"
        );
        assert!(builder.inner.trade_targets_ref()[0].is_auth_derived());
        assert_eq!(
            builder.inner.trade_targets_ref()[1].broker_id,
            "快期股票模拟"
        );
        assert_eq!(
            builder.inner.trade_targets_ref()[1].auth_derived,
            Some(tqsdk_core::AuthDerivedTradeTarget::TqKqStock { number: Some(7) })
        );
    }

    #[test]
    fn named_market_target_shortcuts_forward_to_inner_session_builder() {
        let futures_live = TqApiBuilder::new("demo-user", "demo-pass").futures_market();
        assert_eq!(
            futures_live.inner.market_target_ref(),
            &MarketSessionTarget::futures_live()
        );

        let stock_backtest = TqApiBuilder::new("demo-user", "demo-pass").stock_backtest_market();
        assert_eq!(
            stock_backtest.inner.market_target_ref(),
            &MarketSessionTarget::stock_backtest()
        );

        let futures_backtest =
            TqApiBuilder::new("demo-user", "demo-pass").futures_backtest_market();
        assert_eq!(
            futures_backtest.inner.market_target_ref(),
            &MarketSessionTarget::futures_backtest()
        );
    }
}
