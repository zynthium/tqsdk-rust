#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::{ClientOrderId, OrderTicket};

use crate::{Result, TaskHost};

/// Snapshot of a task-level order request before it is submitted.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskOrderIntent {
    pub account_id: String,
    pub symbol: String,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub volume: i64,
    pub limit_price: Option<f64>,
}

/// Typed order entrypoint owned by [`TaskHost`].
pub struct TaskOrderBuilder<'a> {
    host: &'a mut TaskHost,
    account_id: String,
}

/// Draft order request that can be enriched before submission.
pub struct TaskOrderDraft<'a> {
    host: &'a mut TaskHost,
    intent: TaskOrderIntent,
}

impl<'a> TaskOrderBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, account_id: String) -> Self {
        Self { host, account_id }
    }

    #[must_use]
    pub fn buy_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, Some(TradeOffset::Open), volume)
    }

    #[must_use]
    pub fn sell_open(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(
            symbol,
            TradeDirection::Sell,
            Some(TradeOffset::Open),
            volume,
        )
    }

    #[must_use]
    pub fn buy_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(
            symbol,
            TradeDirection::Buy,
            Some(TradeOffset::Close),
            volume,
        )
    }

    #[must_use]
    pub fn sell_close(self, symbol: impl AsRef<str>, volume: i64) -> TaskOrderDraft<'a> {
        self.intent(
            symbol,
            TradeDirection::Sell,
            Some(TradeOffset::Close),
            volume,
        )
    }

    fn intent(
        self,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
    ) -> TaskOrderDraft<'a> {
        TaskOrderDraft {
            host: self.host,
            intent: TaskOrderIntent {
                account_id: self.account_id,
                symbol: symbol.as_ref().to_owned(),
                direction,
                offset,
                volume,
                limit_price: None,
            },
        }
    }
}

impl TaskOrderDraft<'_> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.intent.limit_price = Some(price);
        self
    }

    #[must_use]
    pub fn intent(&self) -> &TaskOrderIntent {
        &self.intent
    }

    pub async fn send_once(self, client_order_id: impl Into<ClientOrderId>) -> Result<OrderTicket> {
        self.host
            .submit_task_order_once(self.intent, client_order_id.into())
            .await
    }
}
