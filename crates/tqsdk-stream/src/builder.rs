#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::api::TqStream;

/// Builder for the async stream facade.
///
/// This is a thin convenience layer over [`tqsdk_session::SessionClientBuilder`]
/// for users who want Rust-native commit stream consumption.
#[derive(Debug, Clone)]
pub struct TqStreamBuilder {
    inner: tqsdk_session::SessionClientBuilder,
    commit_channel_capacity: usize,
}

impl TqStreamBuilder {
    #[must_use]
    pub fn from_session_builder(inner: tqsdk_session::SessionClientBuilder) -> Self {
        Self {
            inner,
            commit_channel_capacity: crate::api::DEFAULT_COMMIT_CHANNEL_CAPACITY,
        }
    }

    tqsdk_session::__tqsdk_impl_session_builder_forwarders!();

    pub fn commit_channel_capacity(mut self, capacity: usize) -> crate::error::Result<Self> {
        if capacity == 0 {
            return Err(crate::StreamFacadeError::InvalidState(
                "commit channel capacity must be greater than zero",
            ));
        }
        self.commit_channel_capacity = capacity;
        Ok(self)
    }

    /// Sizes the root commit fan-out ring from the expected number of
    /// independent commit consumers.
    ///
    /// The capacity is `max(1024, expected_consumers * 8)`. Use
    /// [`Self::commit_channel_capacity`] when the workload already has a known
    /// ring-size target.
    pub fn expected_commit_consumers(
        mut self,
        expected_consumers: usize,
    ) -> crate::error::Result<Self> {
        self.commit_channel_capacity =
            crate::api::commit_channel_capacity_for_consumers(expected_consumers)?;
        Ok(self)
    }

    pub async fn build(self) -> crate::error::Result<TqStream> {
        let session = self.inner.build()?;
        Ok(TqStream::new_with_capacity(
            session,
            self.commit_channel_capacity,
        ))
    }
}

#[cfg(test)]
mod tests {
    use tqsdk_core::MarketSessionTarget;

    use super::TqStreamBuilder;

    #[test]
    fn from_session_builder_preserves_underlying_session_configuration() {
        let session_builder = tqsdk_session::SessionClientBuilder::new("demo-user", "demo-pass")
            .enable_query()
            .schema_url("https://schema.example.com/latest.json")
            .stock_market()
            .trade_target("9999", "sim")
            .replay_url("replay-driver");

        let builder = TqStreamBuilder::from_session_builder(session_builder);

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
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
            .stock_backtest_market()
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
    fn market_relay_forwards_to_inner_session_builder() {
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
            .market_relay("ws://127.0.0.1:7788/market");

        assert_eq!(
            builder.inner.endpoints().market_url.as_deref(),
            Some("ws://127.0.0.1:7788/market")
        );
    }

    #[test]
    fn tqkq_trade_methods_forward_to_inner_session_builder() {
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
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
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
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
        let futures_live = TqStreamBuilder::new("demo-user", "demo-pass").futures_market();
        assert_eq!(
            futures_live.inner.market_target_ref(),
            &MarketSessionTarget::futures_live()
        );

        let stock_backtest = TqStreamBuilder::new("demo-user", "demo-pass").stock_backtest_market();
        assert_eq!(
            stock_backtest.inner.market_target_ref(),
            &MarketSessionTarget::stock_backtest()
        );

        let futures_backtest =
            TqStreamBuilder::new("demo-user", "demo-pass").futures_backtest_market();
        assert_eq!(
            futures_backtest.inner.market_target_ref(),
            &MarketSessionTarget::futures_backtest()
        );
    }

    #[test]
    fn commit_channel_capacity_accepts_positive_capacity() {
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
            .commit_channel_capacity(2048)
            .unwrap();

        assert_eq!(builder.commit_channel_capacity, 2048);
    }

    #[test]
    fn commit_channel_capacity_rejects_zero_capacity() {
        let err = TqStreamBuilder::new("demo-user", "demo-pass")
            .commit_channel_capacity(0)
            .unwrap_err();

        assert_eq!(err.diagnostic().kind, crate::StreamErrorKind::InvalidState);
    }

    #[test]
    fn expected_commit_consumers_keeps_default_floor_for_small_counts() {
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
            .expected_commit_consumers(16)
            .unwrap();

        assert_eq!(
            builder.commit_channel_capacity,
            crate::api::DEFAULT_COMMIT_CHANNEL_CAPACITY
        );
    }

    #[test]
    fn expected_commit_consumers_scales_capacity_for_many_consumers() {
        let builder = TqStreamBuilder::new("demo-user", "demo-pass")
            .expected_commit_consumers(512)
            .unwrap();

        assert_eq!(builder.commit_channel_capacity, 4096);
    }

    #[test]
    fn expected_commit_consumers_rejects_zero_consumers() {
        let err = TqStreamBuilder::new("demo-user", "demo-pass")
            .expected_commit_consumers(0)
            .unwrap_err();

        assert_eq!(err.diagnostic().kind, crate::StreamErrorKind::InvalidState);
    }
}
