use crate::{
    Account, AccountId, Order, OrderId, Position, Quote, Result, Revision, Symbol, TradingStatus,
};

use super::StateReadView;

#[derive(Clone, Copy)]
pub struct MarketStateView<'a> {
    read: StateReadView<'a>,
}

impl<'a> MarketStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self { read }
    }

    pub fn revision(&self) -> Revision {
        self.read.revision()
    }

    pub fn quote(&self, symbol: &Symbol) -> Result<Option<Quote>> {
        self.read.decode_path(&["quotes", symbol.as_str()])
    }

    pub fn trading_status(&self, symbol: &Symbol) -> Result<Option<TradingStatus>> {
        self.read.decode_path(&["trading_status", symbol.as_str()])
    }
}

#[derive(Clone, Copy)]
pub struct TradeStateView<'a> {
    read: StateReadView<'a>,
}

impl<'a> TradeStateView<'a> {
    pub(crate) fn new(read: StateReadView<'a>) -> Self {
        Self { read }
    }

    pub fn revision(&self) -> Revision {
        self.read.revision()
    }

    pub fn account(&self, account_id: &AccountId) -> Result<Option<Account>> {
        self.read
            .decode_path(&["trade", account_id.as_str(), "accounts", "CNY"])
    }

    pub fn position(&self, account_id: &AccountId, symbol: &Symbol) -> Result<Option<Position>> {
        self.read
            .decode_path(&["trade", account_id.as_str(), "positions", symbol.as_str()])
    }

    pub fn order(&self, account_id: &AccountId, order_id: &OrderId) -> Result<Option<Order>> {
        self.read
            .decode_path(&["trade", account_id.as_str(), "orders", order_id.as_str()])
    }
}
