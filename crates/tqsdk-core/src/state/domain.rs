use std::sync::RwLockReadGuard;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Account, AccountId, Order, OrderId, Position, Quote, Result, Revision, Symbol, TradingStatus,
};

use super::{
    StateReadView,
    read::{decode_value_at_path, get_at_path},
};

#[derive(Clone, Copy)]
pub struct MarketStateView<'a> {
    source: MarketStateSource<'a>,
}

#[derive(Clone, Copy)]
enum MarketStateSource<'a> {
    Snapshot(StateReadView<'a>),
    Partitions {
        revision: Revision,
        quotes: &'a Value,
        trading_status: &'a Value,
    },
}

impl<'a> MarketStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self {
            source: MarketStateSource::Snapshot(read),
        }
    }

    pub(crate) fn from_partitions(
        revision: Revision,
        quotes: &'a Value,
        trading_status: &'a Value,
    ) -> Self {
        Self {
            source: MarketStateSource::Partitions {
                revision,
                quotes,
                trading_status,
            },
        }
    }

    pub fn revision(&self) -> Revision {
        match self.source {
            MarketStateSource::Snapshot(read) => read.revision(),
            MarketStateSource::Partitions { revision, .. } => revision,
        }
    }

    pub fn quote(&self, symbol: &Symbol) -> Result<Option<Quote>> {
        match self.source {
            MarketStateSource::Snapshot(read) => read.decode_path(&["quotes", symbol.as_str()]),
            MarketStateSource::Partitions { quotes, .. } => {
                decode_partition_path(quotes, &[symbol.as_str()], &["quotes", symbol.as_str()])
            }
        }
    }

    pub fn trading_status(&self, symbol: &Symbol) -> Result<Option<TradingStatus>> {
        match self.source {
            MarketStateSource::Snapshot(read) => {
                read.decode_path(&["trading_status", symbol.as_str()])
            }
            MarketStateSource::Partitions { trading_status, .. } => decode_partition_path(
                trading_status,
                &[symbol.as_str()],
                &["trading_status", symbol.as_str()],
            ),
        }
    }
}

pub struct MarketStateReadGuard<'a> {
    revision: Revision,
    quotes: RwLockReadGuard<'a, Value>,
    trading_status: RwLockReadGuard<'a, Value>,
}

impl<'a> MarketStateReadGuard<'a> {
    pub(crate) fn new(
        revision: Revision,
        quotes: RwLockReadGuard<'a, Value>,
        trading_status: RwLockReadGuard<'a, Value>,
    ) -> Self {
        Self {
            revision,
            quotes,
            trading_status,
        }
    }

    pub fn view(&self) -> MarketStateView<'_> {
        MarketStateView::from_partitions(self.revision, &self.quotes, &self.trading_status)
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn quote(&self, symbol: &Symbol) -> Result<Option<Quote>> {
        self.view().quote(symbol)
    }

    pub fn trading_status(&self, symbol: &Symbol) -> Result<Option<TradingStatus>> {
        self.view().trading_status(symbol)
    }
}

#[derive(Clone, Copy)]
pub struct TradeStateView<'a> {
    source: TradeStateSource<'a>,
}

#[derive(Clone, Copy)]
enum TradeStateSource<'a> {
    Snapshot(StateReadView<'a>),
    Partition {
        revision: Revision,
        trade: &'a Value,
    },
}

impl<'a> TradeStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self {
            source: TradeStateSource::Snapshot(read),
        }
    }

    pub(crate) fn from_partition(revision: Revision, trade: &'a Value) -> Self {
        Self {
            source: TradeStateSource::Partition { revision, trade },
        }
    }

    pub fn revision(&self) -> Revision {
        match self.source {
            TradeStateSource::Snapshot(read) => read.revision(),
            TradeStateSource::Partition { revision, .. } => revision,
        }
    }

    pub fn account(&self, account_id: &AccountId) -> Result<Option<Account>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "accounts", "CNY"])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "accounts", "CNY"],
                &["trade", account_id.as_str(), "accounts", "CNY"],
            ),
        }
    }

    pub fn position(&self, account_id: &AccountId, symbol: &Symbol) -> Result<Option<Position>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "positions", symbol.as_str()])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "positions", symbol.as_str()],
                &["trade", account_id.as_str(), "positions", symbol.as_str()],
            ),
        }
    }

    pub fn order(&self, account_id: &AccountId, order_id: &OrderId) -> Result<Option<Order>> {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                read.decode_path(&["trade", account_id.as_str(), "orders", order_id.as_str()])
            }
            TradeStateSource::Partition { trade, .. } => decode_partition_path(
                trade,
                &[account_id.as_str(), "orders", order_id.as_str()],
                &["trade", account_id.as_str(), "orders", order_id.as_str()],
            ),
        }
    }

    /// Decodes a value from a `trade`-partition relative path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.source {
            TradeStateSource::Snapshot(read) => {
                let mut full_path = Vec::with_capacity(path.len() + 1);
                full_path.push("trade");
                full_path.extend_from_slice(path);
                read.decode_path(&full_path)
            }
            TradeStateSource::Partition { trade, .. } => {
                let mut display_path = Vec::with_capacity(path.len() + 1);
                display_path.push("trade");
                display_path.extend_from_slice(path);
                decode_partition_path(trade, path, &display_path)
            }
        }
    }
}

pub struct TradeStateReadGuard<'a> {
    revision: Revision,
    trade: RwLockReadGuard<'a, Value>,
}

impl<'a> TradeStateReadGuard<'a> {
    pub(crate) fn new(revision: Revision, trade: RwLockReadGuard<'a, Value>) -> Self {
        Self { revision, trade }
    }

    pub fn view(&self) -> TradeStateView<'_> {
        TradeStateView::from_partition(self.revision, &self.trade)
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn account(&self, account_id: &AccountId) -> Result<Option<Account>> {
        self.view().account(account_id)
    }

    pub fn position(&self, account_id: &AccountId, symbol: &Symbol) -> Result<Option<Position>> {
        self.view().position(account_id, symbol)
    }

    pub fn order(&self, account_id: &AccountId, order_id: &OrderId) -> Result<Option<Order>> {
        self.view().order(account_id, order_id)
    }

    /// Decodes a value from a `trade`-partition relative path.
    pub fn decode_path<T>(&self, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.view().decode_path(path)
    }

    /// Returns a raw `Value` reference at a partition-relative path.
    pub(crate) fn get_path(&self, path: &[&str]) -> Option<&Value> {
        get_at_path(&self.trade, path.iter().copied())
    }
}

fn decode_partition_path<T>(
    partition: &Value,
    lookup_path: &[&str],
    display_path: &[&str],
) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let Some(value) = get_at_path(partition, lookup_path.iter().copied()) else {
        return Ok(None);
    };

    decode_value_at_path(value, display_path).map(Some)
}
