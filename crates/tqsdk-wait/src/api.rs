#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tqsdk_core::{
    AccountId, MarketChartCommand, MarketCommand, OrderId, RuntimeCommand, Symbol, TradeCommand,
    TradeDirection, TradeInsertOrderCommand, TradeOffset, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition,
};
use tqsdk_session::SessionClient;

use crate::change::{ChangeTrackedRef, matches_any, matches_fields};
use crate::driver::{WaitDriver, WaitGuard};
use crate::refs::{
    AccountRef, KlineSerialRef, OrderRef, PositionRef, QuoteRef, TickSerialRef, TradeRef,
    TradingStatusRef,
};

pub struct TqApi {
    pub(crate) driver: WaitDriver,
}

impl TqApi {
    pub fn new(session: SessionClient) -> Self {
        let handle = session.handle().clone();
        Self::new_for_test(handle, session)
    }

    #[doc(hidden)]
    pub fn new_for_test(handle: tqsdk_core::RuntimeHandle, session: SessionClient) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();
        let runtime = session.runtime_clone();

        Self {
            driver: WaitDriver {
                session,
                reader,
                cursor,
                runtime,
                deferred_commits: VecDeque::new(),
                last_commit: None,
                waiting: AtomicBool::new(false),
                next_order_seq: AtomicU64::new(1),
            },
        }
    }

    pub async fn wait_update(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<bool> {
        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit);
            return Ok(true);
        }

        loop {
            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                self.driver.last_commit = Some(commit);
                return Ok(true);
            }

            let flushed = self
                .driver
                .session
                .flush_outbound()
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if flushed {
                continue;
            }

            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                self.driver.last_commit = Some(commit);
                return Ok(true);
            }

            let drove_pending = self
                .driver
                .session
                .drive_pending_once()
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if drove_pending {
                continue;
            }

            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                self.driver.last_commit = Some(commit);
                return Ok(true);
            }

            let drove_route = self
                .driver
                .session
                .drive_route_once(deadline)
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if !drove_route {
                return Ok(false);
            }
        }
    }

    pub fn last_commit(&self) -> Option<&tqsdk_core::CommitResult> {
        self.driver.last_commit.as_ref()
    }

    pub fn quote_ref(&self, symbol: &str) -> QuoteRef {
        QuoteRef::new(symbol)
    }

    pub fn get_account(&self, account_id: &str) -> AccountRef {
        AccountRef::new(account_id)
    }

    pub fn get_position(&self, account_id: &str, symbol: &str) -> PositionRef {
        PositionRef::new(account_id, symbol)
    }

    pub fn get_order(&self, account_id: &str, order_id: &str) -> OrderRef {
        OrderRef::new(account_id, order_id)
    }

    pub fn get_trade(&self, account_id: &str, trade_id: &str) -> TradeRef {
        TradeRef::new(account_id, trade_id)
    }

    pub async fn get_quote(&mut self, symbol: &str) -> crate::error::Result<QuoteRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(self.quote_ref(symbol))
    }

    pub async fn get_trading_status(
        &mut self,
        symbol: &str,
    ) -> crate::error::Result<TradingStatusRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(
                MarketCommand::SubscribeTradingStatus {
                    symbols: vec![Symbol::new(symbol)],
                },
            ))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(TradingStatusRef::new(symbol))
    }

    pub async fn get_kline_serial(
        &mut self,
        symbol: &str,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<KlineSerialRef> {
        let duration_ns =
            (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos());
        let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{data_length}");

        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        let serial = KlineSerialRef {
            symbol: symbol.to_string(),
            duration_ns,
            view_width: data_length,
            chart_id,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api))
            .await?;

        Ok(serial)
    }

    pub async fn get_tick_serial(
        &mut self,
        symbol: &str,
        data_length: usize,
    ) -> crate::error::Result<TickSerialRef> {
        let chart_id = format!("wait-tick-{symbol}-{data_length}");

        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns: 0,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        let serial = TickSerialRef {
            symbol: symbol.to_string(),
            view_width: data_length,
            chart_id,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api))
            .await?;

        Ok(serial)
    }

    pub async fn insert_order(
        &mut self,
        account_id: &str,
        symbol: &str,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
        limit_price: Option<Value>,
    ) -> crate::error::Result<OrderRef> {
        let order_seq = self.driver.next_order_seq.fetch_add(1, Ordering::Relaxed);
        let order_id = OrderId::new(format!("wait-order-{order_seq}"));
        let (price_type, limit_price, time_condition) = map_wait_order_price(limit_price);

        self.driver
            .session
            .submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
                TradeInsertOrderCommand {
                    account_id: AccountId::new(account_id),
                    order_id: order_id.clone(),
                    symbol: Symbol::new(symbol),
                    direction,
                    offset,
                    volume,
                    price_type,
                    limit_price,
                    time_condition,
                    volume_condition: TradeVolumeCondition::Any,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(self.get_order(account_id, order_id.as_str()))
    }

    pub async fn cancel_order(
        &mut self,
        account_id: &str,
        order_id: &str,
    ) -> crate::error::Result<()> {
        self.driver
            .session
            .submit(RuntimeCommand::Trade(TradeCommand::CancelOrder {
                account_id: AccountId::new(account_id),
                order_id: OrderId::new(order_id),
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(())
    }

    pub async fn confirm_settlement(&mut self, account_id: &str) -> crate::error::Result<()> {
        self.driver
            .session
            .submit(RuntimeCommand::Trade(TradeCommand::ConfirmSettlement {
                account_id: AccountId::new(account_id),
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(())
    }

    pub fn is_changing(&self, target: &impl ChangeTrackedRef) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_any(&commit.changes, target)))
    }

    pub fn is_changing_fields(
        &self,
        target: &impl ChangeTrackedRef,
        fields: &[&str],
    ) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_fields(&commit.changes, target, fields)))
    }

    async fn wait_until_ready_for_test<F>(&mut self, mut ready: F) -> crate::error::Result<()>
    where
        F: FnMut(&Self) -> crate::error::Result<bool>,
    {
        if ready(self)? {
            return Ok(());
        }

        let previous_last_commit = self.driver.last_commit.clone();
        let mut replay = Vec::new();

        while !ready(self)? {
            if !self.wait_update(None).await? {
                for commit in replay.into_iter().rev() {
                    self.driver.deferred_commits.push_front(commit);
                }
                self.driver.last_commit = previous_last_commit;
                return Err(crate::error::WaitFacadeError::InvalidState(
                    "object not ready",
                ));
            }

            if let Some(commit) = self.driver.last_commit.clone() {
                replay.push(commit);
            }
        }

        for commit in replay.into_iter().rev() {
            self.driver.deferred_commits.push_front(commit);
        }
        self.driver.last_commit = previous_last_commit;

        Ok(())
    }

    #[doc(hidden)]
    pub fn begin_wait_for_test(&self) -> crate::error::Result<WaitGuard<'_>> {
        self.driver.begin_wait()
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.driver.runtime.handle()
    }

    #[doc(hidden)]
    pub fn push_deferred_commit_for_test(&mut self, commit: tqsdk_core::CommitResult) {
        self.driver.deferred_commits.push_back(commit);
    }
}

fn map_wait_order_price(
    limit_price: Option<Value>,
) -> (TradePriceType, Option<Value>, TradeTimeCondition) {
    match limit_price {
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("BEST") => {
            (TradePriceType::Best, None, TradeTimeCondition::Ioc)
        }
        Some(Value::String(mode)) if mode.eq_ignore_ascii_case("FIVELEVEL") => {
            (TradePriceType::FiveLevel, None, TradeTimeCondition::Ioc)
        }
        Some(limit_price) => (
            TradePriceType::Limit,
            Some(limit_price),
            TradeTimeCondition::Gfd,
        ),
        None => (TradePriceType::Any, None, TradeTimeCondition::Ioc),
    }
}
