use std::time::Duration;

use tqsdk_core::{TradeDirection, TradeOffset};

use crate::{Result, TaskHost};

use super::allocation::AccountGroup;
use super::report::AccountFailurePolicy;
use super::submit::submit_multi_account_order;
use super::ticket::MultiAccountOrderTicket;

/// Builder for one multi-account task order.
pub struct MultiAccountOrderBuilder<'a> {
    pub(super) host: &'a mut TaskHost,
    pub(super) accounts: AccountGroup,
    pub(super) group_id: Option<String>,
    pub(super) max_unhedged: Option<Duration>,
    pub(super) failure_policy: AccountFailurePolicy,
}

/// Draft multi-account order after side and offset are selected.
pub struct MultiAccountOrderDraft<'a> {
    pub(super) builder: MultiAccountOrderBuilder<'a>,
    pub(super) symbol: String,
    pub(super) direction: TradeDirection,
    pub(super) offset: TradeOffset,
    pub(super) total_volume: i64,
    pub(super) limit_price: Option<f64>,
}

impl<'a> MultiAccountOrderBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, accounts: AccountGroup) -> Self {
        Self {
            host,
            accounts,
            group_id: None,
            max_unhedged: None,
            failure_policy: AccountFailurePolicy::ReportExposure,
        }
    }

    #[must_use]
    pub fn client_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    #[must_use]
    pub fn max_unhedged(mut self, duration: Duration) -> Self {
        self.max_unhedged = Some(duration);
        self
    }

    #[must_use]
    pub fn on_account_failed(mut self, policy: AccountFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    #[must_use]
    pub fn buy_open(
        self,
        symbol: impl AsRef<str>,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, TradeOffset::Open, total_volume)
    }

    #[must_use]
    pub fn sell_open(
        self,
        symbol: impl AsRef<str>,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        self.intent(
            symbol,
            TradeDirection::Sell,
            TradeOffset::Open,
            total_volume,
        )
    }

    fn intent(
        self,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: TradeOffset,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        MultiAccountOrderDraft {
            builder: self,
            symbol: symbol.as_ref().to_owned(),
            direction,
            offset,
            total_volume,
            limit_price: None,
        }
    }
}

impl MultiAccountOrderDraft<'_> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.limit_price = Some(price);
        self
    }

    pub async fn send_once(self) -> Result<MultiAccountOrderTicket> {
        submit_multi_account_order(self).await
    }
}
