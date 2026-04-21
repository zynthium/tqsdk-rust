use serde::de::DeserializeOwned;
use tqsdk_core::{
    Account, AccountId, ObjectKey, Order, OrderId, Position, PreInsertOrder, RiskManagementData,
    RiskManagementRule, SettlementInfo, StatePath, Symbol, Trade, TradeId,
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

/// Lightweight handle to `trade/{account_id}/accounts/CNY`.
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
        decode_optional(api, &["trade", self.account_id.as_str(), "accounts", "CNY"])
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

/// Lightweight handle to `trade/{account_id}/positions/{symbol}`.
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

/// Lightweight handle to `trade/{account_id}/pre_insert_orders/{order_id}`.
#[derive(Debug, Clone)]
pub struct PreInsertOrderRef {
    account_id: AccountId,
    order_id: OrderId,
}

impl PreInsertOrderRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<PreInsertOrder> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "pre-insert order not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<PreInsertOrder>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "pre_insert_orders",
                self.order_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for PreInsertOrderRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::PreInsertOrder {
            account_id: self.account_id.clone(),
            order_id: self.order_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "pre_insert_orders",
            self.order_id.as_str(),
        ])
    }
}

/// Lightweight handle to `trade/{account_id}/orders/{order_id}`.
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

/// Lightweight handle to `trade/{account_id}/trades/{trade_id}`.
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

/// Lightweight handle to `trade/{account_id}/risk_management_rule/{exchange_id}`.
#[derive(Debug, Clone)]
pub struct RiskManagementRuleRef {
    account_id: AccountId,
    exchange_id: String,
}

impl RiskManagementRuleRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, exchange_id: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            exchange_id: exchange_id.into(),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<RiskManagementRule> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "risk management rule not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<RiskManagementRule>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "risk_management_rule",
                self.exchange_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for RiskManagementRuleRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::RiskManagementRule {
            account_id: self.account_id.clone(),
            exchange_id: self.exchange_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "risk_management_rule",
            self.exchange_id.as_str(),
        ])
    }
}

/// Lightweight handle to `trade/{account_id}/risk_management_data/{symbol}`.
#[derive(Debug, Clone)]
pub struct RiskManagementDataRef {
    account_id: AccountId,
    symbol: Symbol,
}

impl RiskManagementDataRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<RiskManagementData> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "risk management data not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<RiskManagementData>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "risk_management_data",
                self.symbol.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for RiskManagementDataRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::RiskManagementData {
            account_id: self.account_id.clone(),
            symbol: self.symbol.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "risk_management_data",
            self.symbol.as_str(),
        ])
    }
}

/// Lightweight handle to `trade/{account_id}/his_settlements/{trading_day}`.
#[derive(Debug, Clone)]
pub struct SettlementInfoRef {
    account_id: AccountId,
    trading_day: String,
}

impl SettlementInfoRef {
    #[must_use]
    pub fn new(account_id: impl Into<String>, trading_day: impl Into<String>) -> Self {
        Self {
            account_id: AccountId::new(account_id.into()),
            trading_day: trading_day.into(),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<SettlementInfo> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "settlement info not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<SettlementInfo>> {
        decode_optional(
            api,
            &[
                "trade",
                self.account_id.as_str(),
                "his_settlements",
                self.trading_day.as_str(),
            ],
        )
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
    }
}

impl ChangeTrackedRef for SettlementInfoRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Settlement {
            account_id: self.account_id.clone(),
            trading_day: self.trading_day.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new([
            "trade",
            self.account_id.as_str(),
            "his_settlements",
            self.trading_day.as_str(),
        ])
    }
}
