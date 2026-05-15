use serde::de::DeserializeOwned;
use tqsdk_core::{
    Account, AccountId, ObjectKey, Order, OrderId, OrderLifecycle, Position, PreInsertOrder,
    RiskManagementData, RiskManagementRule, SettlementInfo, StatePath, Symbol, Trade, TradeId,
};

use crate::{api::TqApi, change::ChangeTrackedRef, step::WaitReadHandle};

fn decode_optional<T: DeserializeOwned>(
    reader: &WaitReadHandle,
    path: &[&str],
) -> crate::error::Result<Option<T>> {
    reader
        .reader()
        .read_trade_state()
        .decode_path::<T>(path)
        .map_err(Into::into)
}

/// Lightweight handle to `trade/{account_id}/accounts/CNY`.
#[derive(Clone)]
pub struct AccountRef {
    reader: WaitReadHandle,
    account_id: AccountId,
}

impl AccountRef {
    pub(crate) fn new(reader: WaitReadHandle, account_id: impl Into<String>) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<Account> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "account not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Account>> {
        decode_optional(&self.reader, &[self.account_id.as_str(), "accounts", "CNY"])
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct PositionRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    symbol: Symbol,
}

impl PositionRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<Position> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "position not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Position>> {
        decode_optional(
            &self.reader,
            &[self.account_id.as_str(), "positions", self.symbol.as_str()],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct PreInsertOrderRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    order_id: OrderId,
}

impl PreInsertOrderRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        order_id: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<PreInsertOrder> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "pre-insert order not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<PreInsertOrder>> {
        decode_optional(
            &self.reader,
            &[
                self.account_id.as_str(),
                "pre_insert_orders",
                self.order_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct OrderRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    order_id: OrderId,
}

impl std::fmt::Debug for OrderRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrderRef")
            .field("account_id", &self.account_id)
            .field("order_id", &self.order_id)
            .finish_non_exhaustive()
    }
}

impl OrderRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        order_id: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            order_id: OrderId::new(order_id.into()),
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        self.account_id.as_str()
    }

    #[must_use]
    pub fn order_id(&self) -> &str {
        self.order_id.as_str()
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Order>> {
        decode_optional(
            &self.reader,
            &[self.account_id.as_str(), "orders", self.order_id.as_str()],
        )
    }

    pub fn load(&self) -> crate::error::Result<Order> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "order not ready",
            ))
    }

    pub async fn cancel(&self, api: &mut TqApi) -> crate::error::Result<()> {
        api.cancel_order(self.account_id.as_str(), self.order_id.as_str())
            .await
    }

    pub async fn cancel_remaining(&self, api: &mut TqApi) -> crate::error::Result<()> {
        if let Some(order) = self.snapshot()?
            && (order.volume_left <= 0 || order.lifecycle.is_terminal())
        {
            return Ok(());
        }

        self.cancel(api).await
    }

    pub async fn wait_partially_filled(&self, api: &mut TqApi) -> crate::error::Result<Order> {
        self.wait_partially_filled_with_deadline(api, None).await
    }

    pub async fn wait_partially_filled_until(
        &self,
        api: &mut TqApi,
        deadline: tokio::time::Instant,
    ) -> crate::error::Result<Order> {
        self.wait_partially_filled_with_deadline(api, Some(deadline))
            .await
    }

    pub async fn wait_terminal(&self, api: &mut TqApi) -> crate::error::Result<Order> {
        self.wait_terminal_with_deadline(api, None).await
    }

    pub async fn wait_terminal_until(
        &self,
        api: &mut TqApi,
        deadline: tokio::time::Instant,
    ) -> crate::error::Result<Order> {
        self.wait_terminal_with_deadline(api, Some(deadline)).await
    }

    async fn wait_partially_filled_with_deadline(
        &self,
        api: &mut TqApi,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<Order> {
        loop {
            if let Some(order) = self.snapshot()? {
                if is_partially_filled(&order) {
                    return Ok(order);
                }
                if order.lifecycle.is_terminal() {
                    return Err(crate::error::WaitFacadeError::InvalidState(
                        "order reached terminal state before partial fill",
                    ));
                }
            }

            if !api.wait_update(deadline).await? {
                return Err(crate::error::WaitFacadeError::InvalidState(
                    "order partial fill not ready",
                ));
            }
        }
    }

    async fn wait_terminal_with_deadline(
        &self,
        api: &mut TqApi,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<Order> {
        loop {
            if let Some(order) = self.snapshot()?
                && order.lifecycle.is_terminal()
            {
                return Ok(order);
            }

            if !api.wait_update(deadline).await? {
                return Err(crate::error::WaitFacadeError::InvalidState(
                    "order terminal state not ready",
                ));
            }
        }
    }
}

fn is_partially_filled(order: &Order) -> bool {
    order.lifecycle == OrderLifecycle::PartiallyFilled
        || (order.volume_origin > 0
            && order.volume_left > 0
            && order.volume_left < order.volume_origin)
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
#[derive(Clone)]
pub struct TradeRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    trade_id: TradeId,
}

impl TradeRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        trade_id: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            trade_id: TradeId::new(trade_id.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<Trade> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "trade not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Trade>> {
        decode_optional(
            &self.reader,
            &[self.account_id.as_str(), "trades", self.trade_id.as_str()],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct RiskManagementRuleRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    exchange_id: String,
}

impl RiskManagementRuleRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        exchange_id: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            exchange_id: exchange_id.into(),
        }
    }

    pub fn load(&self) -> crate::error::Result<RiskManagementRule> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "risk management rule not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<RiskManagementRule>> {
        decode_optional(
            &self.reader,
            &[
                self.account_id.as_str(),
                "risk_management_rule",
                self.exchange_id.as_str(),
            ],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct RiskManagementDataRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    symbol: Symbol,
}

impl RiskManagementDataRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            symbol: Symbol::new(symbol.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<RiskManagementData> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "risk management data not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<RiskManagementData>> {
        decode_optional(
            &self.reader,
            &[
                self.account_id.as_str(),
                "risk_management_data",
                self.symbol.as_str(),
            ],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
#[derive(Clone)]
pub struct SettlementInfoRef {
    reader: WaitReadHandle,
    account_id: AccountId,
    trading_day: String,
}

impl SettlementInfoRef {
    pub(crate) fn new(
        reader: WaitReadHandle,
        account_id: impl Into<String>,
        trading_day: impl Into<String>,
    ) -> Self {
        Self {
            reader,
            account_id: AccountId::new(account_id.into()),
            trading_day: trading_day.into(),
        }
    }

    pub fn load(&self) -> crate::error::Result<SettlementInfo> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "settlement info not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<SettlementInfo>> {
        decode_optional(
            &self.reader,
            &[
                self.account_id.as_str(),
                "his_settlements",
                self.trading_day.as_str(),
            ],
        )
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
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
