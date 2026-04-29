#![cfg_attr(not(test), forbid(unsafe_code))]

#[cfg(feature = "services")]
use chrono::NaiveDate;
use serde_json::Value;
use tqsdk_core::{Order, TradeDirection, TradeOffset, TradingCalendarDay};
use tqsdk_wait::{ClientOrderId, OrderTicket};

use crate::Result;
use crate::TaskError;
use crate::account_group::{AccountGroup, AccountGroupBuilder, MultiAccountOrderBuilder};
use crate::calendar::TradingDayCalendar;
use crate::execution_group::ExecutionGroupBuilder;
use crate::order::{TaskOrderBuilder, TaskOrderIntent};
use crate::registry::TaskId;
use crate::risk::{RiskDecision, RiskEngine};
use crate::scheduler::{TargetPosSchedulerBuilder, process_schedulers_wait_update};
use crate::shared::{
    SharedQuoteSubscriptions, SharedTargetPosSchedulerStore, SharedTargetPosStore,
    SharedTaskRegistry, SharedTradingCalendar,
};
use crate::strategy::StrategyHostBuilder;
use crate::target_pos::{TargetPosBuilder, process_target_tasks_wait_update};
use crate::testing::StrategyTestRuntime;

/// Single-owner task host built on a wait-style API.
pub struct TaskHost {
    api: tqsdk_wait::TqApi,
    registry: SharedTaskRegistry,
    target_tasks: SharedTargetPosStore,
    schedulers: SharedTargetPosSchedulerStore,
    quote_subscriptions: SharedQuoteSubscriptions,
    trading_calendar: SharedTradingCalendar,
    risk: Option<RiskEngine>,
    pub(crate) strategy_test: Option<StrategyTestRuntime>,
}

impl TaskHost {
    #[must_use]
    pub fn new(api: tqsdk_wait::TqApi) -> Self {
        Self {
            api,
            registry: SharedTaskRegistry::default(),
            target_tasks: SharedTargetPosStore::default(),
            schedulers: SharedTargetPosSchedulerStore::default(),
            quote_subscriptions: SharedQuoteSubscriptions::default(),
            trading_calendar: SharedTradingCalendar::default(),
            risk: None,
            strategy_test: None,
        }
    }

    #[must_use]
    pub fn api(&self) -> &tqsdk_wait::TqApi {
        &self.api
    }

    #[must_use]
    pub fn api_mut(&mut self) -> &mut tqsdk_wait::TqApi {
        &mut self.api
    }

    #[must_use]
    pub fn into_api(self) -> tqsdk_wait::TqApi {
        self.api
    }

    #[must_use]
    pub fn with_risk(mut self, risk: RiskEngine) -> Self {
        self.risk = Some(risk);
        self
    }

    pub fn set_risk(&mut self, risk: RiskEngine) {
        self.risk = Some(risk);
    }

    #[must_use]
    pub fn risk(&self) -> Option<&RiskEngine> {
        self.risk.as_ref()
    }

    #[must_use]
    pub fn trading_calendar(&self) -> TradingDayCalendar {
        self.trading_calendar.snapshot()
    }

    pub fn set_trading_calendar(&mut self, calendar: TradingDayCalendar) {
        self.trading_calendar.replace(calendar);
    }

    pub fn extend_trading_calendar(
        &mut self,
        days: impl IntoIterator<Item = TradingCalendarDay>,
    ) -> Result<()> {
        self.trading_calendar.extend(days)
    }

    #[cfg(feature = "services")]
    pub async fn refresh_trading_calendar(
        &mut self,
        start_dt: NaiveDate,
        end_dt: NaiveDate,
    ) -> Result<usize> {
        let days = self
            .api
            .session()
            .get_trading_calendar(start_dt, end_dt)
            .await?;
        let count = days.len();
        self.extend_trading_calendar(days)?;
        Ok(count)
    }

    pub async fn wait_update(&mut self, deadline: Option<tokio::time::Instant>) -> Result<bool> {
        let updated = self.api.wait_update(deadline).await?;
        process_target_tasks_wait_update(&self.target_tasks, &mut self.api).await;
        process_schedulers_wait_update(&self.schedulers, &mut self.api).await;
        Ok(updated)
    }

    #[must_use]
    pub fn orders(&mut self, account_id: impl AsRef<str>) -> TaskOrderBuilder<'_> {
        TaskOrderBuilder::new(self, account_id.as_ref().to_owned())
    }

    #[must_use]
    pub fn strategy(self) -> StrategyHostBuilder {
        StrategyHostBuilder::new(self)
    }

    #[must_use]
    pub fn execution_group(&mut self, account_id: impl AsRef<str>) -> ExecutionGroupBuilder<'_> {
        ExecutionGroupBuilder::new(self, account_id.as_ref().to_owned())
    }

    #[must_use]
    pub fn account_group(&self) -> AccountGroupBuilder {
        AccountGroup::builder()
    }

    #[must_use]
    pub fn multi_account_order(&mut self, accounts: AccountGroup) -> MultiAccountOrderBuilder<'_> {
        MultiAccountOrderBuilder::new(self, accounts)
    }

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder {
        TargetPosBuilder::new(
            self.registry.clone(),
            self.target_tasks.clone(),
            self.quote_subscriptions.clone(),
            account_id.as_ref().to_owned(),
            symbol.as_ref().to_owned(),
        )
    }

    #[must_use]
    pub fn target_pos_scheduler(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosSchedulerBuilder {
        TargetPosSchedulerBuilder::new(
            self.registry.clone(),
            self.target_tasks.clone(),
            self.schedulers.clone(),
            self.quote_subscriptions.clone(),
            self.trading_calendar.clone(),
            account_id.as_ref().to_owned(),
            symbol.as_ref().to_owned(),
        )
    }

    pub async fn insert_order_guarded(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
        limit_price: Option<Value>,
    ) -> Result<tqsdk_wait::OrderRef> {
        let account_id = account_id.as_ref().to_owned();
        let symbol = symbol.as_ref().to_owned();

        self.registry
            .with(|registry| registry.check_manual_order_allowed(&account_id, &symbol))?;

        let intent = TaskOrderIntent {
            account_id: account_id.clone(),
            symbol: symbol.clone(),
            direction,
            offset,
            volume,
            limit_price: limit_price.as_ref().and_then(Value::as_f64),
        };
        self.check_risk(&intent)?;

        let order: tqsdk_wait::OrderRef = self
            .api
            .insert_order(&account_id, &symbol, direction, offset, volume, limit_price)
            .await?;
        self.record_submitted_order(&intent)?;
        Ok(order)
    }

    pub(crate) async fn submit_task_order_once(
        &mut self,
        intent: TaskOrderIntent,
        client_order_id: ClientOrderId,
    ) -> Result<OrderTicket> {
        self.preflight_task_order(&intent)?;
        self.submit_prechecked_task_order_once(intent, client_order_id)
            .await
    }

    pub(crate) fn preflight_task_order(&self, intent: &TaskOrderIntent) -> Result<()> {
        self.preflight_task_orders(std::slice::from_ref(intent))
    }

    pub(crate) fn preflight_task_orders(&self, intents: &[TaskOrderIntent]) -> Result<()> {
        for intent in intents {
            validate_task_order_intent(intent)?;
            self.registry.with(|registry| {
                registry.check_manual_order_allowed(&intent.account_id, &intent.symbol)
            })?;
        }

        let Some(risk) = &self.risk else {
            return Ok(());
        };
        let mut risk = risk.clone();
        for intent in intents {
            match risk.check(&self.api, intent)? {
                RiskDecision::Accepted => risk.record_accepted_order(intent)?,
                RiskDecision::Rejected(rejection) => {
                    return Err(TaskError::RiskRejected(rejection));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn submit_prechecked_task_order_once(
        &mut self,
        intent: TaskOrderIntent,
        client_order_id: impl Into<ClientOrderId>,
    ) -> Result<OrderTicket> {
        validate_task_order_intent(&intent)?;
        self.check_risk(&intent)?;
        let offset = intent.offset.ok_or(TaskError::Unsupported(
            "task orders require explicit offset",
        ))?;
        let limit_price = intent
            .limit_price
            .ok_or(TaskError::InvalidState("limit price is required"))?;

        let ticket: OrderTicket = self
            .api
            .limit_order(intent.account_id.clone(), intent.symbol.clone())
            .client_intent(client_order_id)
            .side(intent.direction, offset, intent.volume)
            .at(limit_price)
            .send_once()
            .await?;
        if ticket.was_submitted() {
            self.record_submitted_order(&intent)?;
        }
        Ok(ticket)
    }

    fn check_risk(&self, intent: &TaskOrderIntent) -> Result<()> {
        let Some(risk) = &self.risk else {
            return Ok(());
        };
        match risk.check(&self.api, intent)? {
            RiskDecision::Accepted => Ok(()),
            RiskDecision::Rejected(rejection) => Err(TaskError::RiskRejected(rejection)),
        }
    }

    fn record_submitted_order(&mut self, intent: &TaskOrderIntent) -> Result<()> {
        let Some(risk) = &mut self.risk else {
            return Ok(());
        };
        risk.record_accepted_order(intent)
    }

    pub async fn cancel_order_guarded(
        &mut self,
        account_id: impl AsRef<str>,
        order_id: impl AsRef<str>,
    ) -> Result<()> {
        let account_id = account_id.as_ref().to_owned();
        let order_id = order_id.as_ref().to_owned();
        let order = self
            .api
            .get_order(&account_id, &order_id)
            .snapshot(&self.api)?
            .ok_or_else(|| TaskError::OrderNotReady {
                account_id: account_id.clone(),
                order_id: order_id.clone(),
            })?;
        let symbol = order_symbol(&order).ok_or_else(|| TaskError::OrderNotReady {
            account_id: account_id.clone(),
            order_id: order_id.clone(),
        })?;
        let exchange_id =
            order_exchange_id(&order, &symbol).ok_or_else(|| TaskError::OrderNotReady {
                account_id: account_id.clone(),
                order_id: order_id.clone(),
            })?;

        self.registry
            .with(|registry| registry.check_manual_order_allowed(&account_id, &symbol))?;
        self.check_order_operation_risk(&account_id, exchange_id)?;

        self.api
            .cancel_order(&account_id, &order_id)
            .await
            .map_err(TaskError::from)?;
        self.record_order_operation(&account_id, exchange_id)?;
        Ok(())
    }

    fn check_order_operation_risk(&self, account_id: &str, exchange_id: &str) -> Result<()> {
        let Some(risk) = &self.risk else {
            return Ok(());
        };
        match risk.check_order_operation(account_id, exchange_id)? {
            RiskDecision::Accepted => Ok(()),
            RiskDecision::Rejected(rejection) => Err(TaskError::RiskRejected(rejection)),
        }
    }

    fn record_order_operation(&mut self, account_id: &str, exchange_id: &str) -> Result<()> {
        let Some(risk) = &mut self.risk else {
            return Ok(());
        };
        risk.record_order_operation(account_id, exchange_id)
    }

    #[doc(hidden)]
    pub fn register_target_owner_for_test(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<u64> {
        self.registry
            .with_mut(|registry| registry.register_target_task(account_id, symbol))
            .map(|task| task.id.0)
    }

    #[doc(hidden)]
    pub fn register_scheduler_owner_for_test(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<u64> {
        self.registry
            .with_mut(|registry| registry.register_scheduler(account_id, symbol))
            .map(|task| task.id.0)
    }

    #[doc(hidden)]
    pub fn check_manual_order_allowed_for_test(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        self.registry
            .with(|registry| registry.check_manual_order_allowed(account_id, symbol))
    }

    #[doc(hidden)]
    pub fn unregister_task_for_test(&mut self, task_id: u64) -> bool {
        self.registry
            .with_mut(|registry| registry.unregister_task(TaskId(task_id)))
    }
}

fn validate_task_order_intent(intent: &TaskOrderIntent) -> Result<()> {
    if intent.volume <= 0 {
        return Err(TaskError::InvalidState("order volume must be positive"));
    }
    if intent.offset.is_none() {
        return Err(TaskError::Unsupported(
            "task orders require explicit offset",
        ));
    }
    let limit_price = intent
        .limit_price
        .ok_or(TaskError::InvalidState("limit price is required"))?;
    if !limit_price.is_finite() {
        return Err(TaskError::InvalidState("limit price must be finite"));
    }
    Ok(())
}

fn order_symbol(order: &Order) -> Option<String> {
    if order.instrument_id.is_empty() {
        return None;
    }

    if order.instrument_id.contains('.') {
        return Some(order.instrument_id.clone());
    }

    if order.exchange_id.is_empty() {
        return None;
    }

    Some(format!("{}.{}", order.exchange_id, order.instrument_id))
}

fn order_exchange_id<'a>(order: &'a Order, symbol: &'a str) -> Option<&'a str> {
    if !order.exchange_id.is_empty() {
        return Some(order.exchange_id.as_str());
    }
    symbol
        .split_once('.')
        .map(|(exchange_id, _)| exchange_id)
        .filter(|exchange_id| !exchange_id.is_empty())
}
