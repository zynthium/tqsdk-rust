use serde::de::DeserializeOwned;
use tqsdk_core::{
    AccountId, ObjectKey, OrderId, SecurityAccount, SecurityOrder, SecurityPosition, SecurityTrade,
    StatePath, Symbol, TradeId,
};

use crate::{api::TqApi, change::ChangeTrackedRef};

fn decode_optional<T: DeserializeOwned>(
    api: &TqApi,
    path: &[&str],
) -> crate::error::Result<Option<T>> {
    api.driver
        .reader
        .read()
        .decode_path::<T>(path)
        .map_err(Into::into)
}

/// Lightweight handle to a stock-shaped `trade/{account_id}/accounts/CNY`.
#[derive(Debug, Clone)]
pub struct SecurityAccountRef {
    account_id: AccountId,
}

impl SecurityAccountRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<SecurityAccount> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "security account not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<SecurityAccount>> {
        decode_optional(api, &["trade", self.account_id.as_str(), "accounts", "CNY"])
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for SecurityAccountRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Account {
            account_id: self.account_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["trade", self.account_id.as_str(), "accounts", "CNY"])
    }
}

/// Lightweight handle to a stock-shaped `trade/{account_id}/positions/{symbol}`.
#[derive(Debug, Clone)]
pub struct SecurityPositionRef {
    account_id: AccountId,
    symbol: Symbol,
}

impl SecurityPositionRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<SecurityPosition> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "security position not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<SecurityPosition>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "positions",
                self.symbol.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for SecurityPositionRef {
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

/// Lightweight handle to a stock-shaped `trade/{account_id}/orders/{order_id}`.
#[derive(Debug, Clone)]
pub struct SecurityOrderRef {
    account_id: AccountId,
    order_id: OrderId,
}

impl SecurityOrderRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<SecurityOrder> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "security order not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<SecurityOrder>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "orders",
                self.order_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for SecurityOrderRef {
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

/// Lightweight handle to a stock-shaped `trade/{account_id}/trades/{trade_id}`.
#[derive(Debug, Clone)]
pub struct SecurityTradeRef {
    account_id: AccountId,
    trade_id: TradeId,
}

impl SecurityTradeRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, trade_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            trade_id: TradeId::new(trade_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<SecurityTrade> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "security trade not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<SecurityTrade>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "trades",
                self.trade_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for SecurityTradeRef {
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
