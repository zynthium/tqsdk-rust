use std::collections::HashMap;

use tqsdk_core::{Order, Position, Trade};

use crate::{Result, TaskError};

use super::broker::FakeBrokerConnectionStatus;

/// Result of one fake strategy test step.
#[derive(Debug, Clone, Default)]
pub struct StrategyTestReport {
    pub(super) orders: Vec<Order>,
    pub(super) trades: Vec<Trade>,
    pub(super) positions: HashMap<(String, String), Position>,
    pub(super) pending_orders: usize,
    pub(super) broker_connection_status: FakeBrokerConnectionStatus,
}

impl StrategyTestReport {
    #[must_use]
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }

    #[must_use]
    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<Position> {
        self.positions
            .get(&(account_id.as_ref().to_owned(), symbol.as_ref().to_owned()))
            .cloned()
            .ok_or(TaskError::InvalidState("strategy test position not ready"))
    }

    #[must_use]
    pub fn pending_orders(&self) -> usize {
        self.pending_orders
    }

    #[must_use]
    pub fn broker_connection_status(&self) -> FakeBrokerConnectionStatus {
        self.broker_connection_status
    }
}
