use tqsdk_core::{
    Account, AccountId, ObjectKey, Order, OrderId, Position, StatePath, Symbol, Trade, TradeId,
};

use crate::{api::TqApi, change::ChangeTrackedRef};

#[derive(Debug, Clone)]
pub struct AccountRef {
    account_id: AccountId,
}

impl AccountRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Account> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "account not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Account>> {
        api.driver
            .reader
            .read()
            .decode_path::<Account>(&["trade", self.account_id.as_str(), "accounts", "CNY"])
            .map_err(Into::into)
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for AccountRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Account {
            account_id: self.account_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["trade", self.account_id.as_str(), "accounts", "CNY"])
    }
}

#[derive(Debug, Clone)]
pub struct PositionRef {
    account_id: AccountId,
    symbol: Symbol,
}

impl PositionRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Position> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "position not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Position>> {
        api.driver
            .reader
            .read()
            .decode_path::<Position>(&[
                "trade",
                self.account_id.as_str(),
                "positions",
                self.symbol.as_str(),
            ])
            .map_err(Into::into)
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for PositionRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Position {
            account_id: self.account_id.clone(),
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "positions",
            self.symbol.as_str(),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct OrderRef {
    account_id: AccountId,
    order_id: OrderId,
}

impl OrderRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Order>> {
        api.driver
            .reader
            .read()
            .decode_path::<Order>(&[
                "trade",
                self.account_id.as_str(),
                "orders",
                self.order_id.as_str(),
            ])
            .map_err(Into::into)
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Order> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "order not ready",
            ))
    }
}

impl ChangeTrackedRef for OrderRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Order {
            account_id: self.account_id.clone(),
            order_id: self.order_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "orders",
            self.order_id.as_str(),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct TradeRef {
    account_id: AccountId,
    trade_id: TradeId,
}

impl TradeRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, trade_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            trade_id: TradeId::new(trade_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Trade> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "trade not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Trade>> {
        api.driver
            .reader
            .read()
            .decode_path::<Trade>(&[
                "trade",
                self.account_id.as_str(),
                "trades",
                self.trade_id.as_str(),
            ])
            .map_err(Into::into)
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for TradeRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Trade {
            account_id: self.account_id.clone(),
            trade_id: self.trade_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "trades",
            self.trade_id.as_str(),
        ])
    }
}
