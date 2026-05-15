#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tqsdk_core::{
    AccountId, MarketChartCommand, MarketCommand, OrderId, RuntimeCommand, Symbol,
    TradeAccountType, TradeCommand, TradeDirection, TradeInsertOrderCommand, TradeLoginCommand,
    TradeOffset, TradeVolumeCondition,
};
use tqsdk_session::SessionClient;

use crate::driver::{WaitDriver, WaitGuard};
use crate::price::OrderPrice;
use crate::refs::{
    AccountRef, KlineHandle, NotificationRef, OrderRef, PositionRef, PreInsertOrderRef, QuoteRef,
    RiskManagementDataRef, RiskManagementRuleRef, SecurityAccountRef, SecurityOrderRef,
    SecurityPositionRef, SecurityTradeRef, SettlementInfoRef, TickHandle, TradeRef,
    TradingStatusRef,
};
use crate::step::{WaitReadHandle, WaitStep};

/// Single-owner wait facade over a shared [`tqsdk_session::SessionClient`].
///
/// [`TqApi`] drives the underlying session one commit at a time through
/// [`TqApi::wait_update`], while exposing lightweight references into the
/// projected state tree.
pub struct TqApi {
    pub(crate) driver: WaitDriver,
}

pub(crate) struct WaitInsertOrderRequest {
    pub(crate) account_id: String,
    pub(crate) symbol: String,
    pub(crate) order_id: OrderId,
    pub(crate) direction: TradeDirection,
    pub(crate) offset: Option<TradeOffset>,
    pub(crate) volume: i64,
    pub(crate) limit_price: OrderPrice,
}

impl TqApi {
    #[must_use]
    pub fn new(session: SessionClient) -> Self {
        let handle = session.handle().clone();
        Self::from_runtime_parts(handle, session)
    }

    #[must_use]
    fn from_runtime_parts(handle: tqsdk_core::RuntimeHandle, session: SessionClient) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();

        Self {
            driver: WaitDriver {
                session,
                reader,
                cursor,
                deferred_commits: VecDeque::new(),
                last_commit: None,
                waiting: AtomicBool::new(false),
                next_order_seq: AtomicU64::new(1),
                serial_charts: Default::default(),
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

            let progress = self
                .driver
                .session
                .progress_once(deadline)
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if !progress.is_progress() {
                return Ok(false);
            }
        }
    }

    pub async fn step(&mut self) -> crate::error::Result<Option<WaitStep>> {
        self.step_until(None).await
    }

    pub async fn step_until(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<Option<WaitStep>> {
        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit.clone());
            return Ok(Some(WaitStep::new(
                commit,
                current_dt_from_reader(&self.driver.reader),
            )));
        }

        loop {
            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                self.driver.last_commit = Some(commit.clone());
                return Ok(Some(WaitStep::new(
                    commit,
                    current_dt_from_reader(&self.driver.reader),
                )));
            }

            let progress = self
                .driver
                .session
                .progress_once(deadline)
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            if !progress.is_progress() {
                return Ok(None);
            }
        }
    }

    #[must_use]
    pub fn last_commit(&self) -> Option<&tqsdk_core::CommitResult> {
        self.driver.last_commit.as_deref()
    }

    #[must_use]
    pub fn session(&self) -> &SessionClient {
        &self.driver.session
    }

    #[must_use]
    pub fn into_session(self) -> SessionClient {
        self.driver.session
    }

    #[must_use]
    pub fn account(&self, account_id: &str) -> AccountRef {
        AccountRef::new(self.read_handle(), account_id)
    }

    #[must_use]
    pub fn position(&self, account_id: &str, symbol: &str) -> PositionRef {
        PositionRef::new(self.read_handle(), account_id, symbol)
    }

    #[must_use]
    pub fn order(&self, account_id: &str, order_id: &str) -> OrderRef {
        OrderRef::new(self.read_handle(), account_id, order_id)
    }

    #[must_use]
    pub fn pre_insert_order(&self, account_id: &str, order_id: &str) -> PreInsertOrderRef {
        PreInsertOrderRef::new(self.read_handle(), account_id, order_id)
    }

    #[must_use]
    pub fn trade(&self, account_id: &str, trade_id: &str) -> TradeRef {
        TradeRef::new(self.read_handle(), account_id, trade_id)
    }

    #[must_use]
    pub fn risk_management_rule(
        &self,
        account_id: &str,
        exchange_id: &str,
    ) -> RiskManagementRuleRef {
        RiskManagementRuleRef::new(self.read_handle(), account_id, exchange_id)
    }

    #[must_use]
    pub fn risk_management_data(&self, account_id: &str, symbol: &str) -> RiskManagementDataRef {
        RiskManagementDataRef::new(self.read_handle(), account_id, symbol)
    }

    #[must_use]
    pub fn settlement_info(&self, account_id: &str, trading_day: &str) -> SettlementInfoRef {
        SettlementInfoRef::new(self.read_handle(), account_id, trading_day)
    }

    #[must_use]
    pub fn notification(&self, notification_id: &str) -> NotificationRef {
        NotificationRef::new(self.read_handle(), notification_id)
    }

    #[must_use]
    pub fn security_account(&self, account_id: &str) -> SecurityAccountRef {
        SecurityAccountRef::new(self.read_handle(), account_id)
    }

    #[must_use]
    pub fn security_position(&self, account_id: &str, symbol: &str) -> SecurityPositionRef {
        SecurityPositionRef::new(self.read_handle(), account_id, symbol)
    }

    #[must_use]
    pub fn security_order(&self, account_id: &str, order_id: &str) -> SecurityOrderRef {
        SecurityOrderRef::new(self.read_handle(), account_id, order_id)
    }

    #[must_use]
    pub fn security_trade(&self, account_id: &str, trade_id: &str) -> SecurityTradeRef {
        SecurityTradeRef::new(self.read_handle(), account_id, trade_id)
    }

    pub async fn quote(&mut self, symbol: &str) -> crate::error::Result<QuoteRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(QuoteRef::new(self.read_handle(), symbol))
    }

    pub fn startup_recovery(&mut self) -> crate::recovery::WaitStartupRecovery<'_> {
        crate::recovery::WaitStartupRecovery::new(self)
    }

    pub async fn trading_status(&mut self, symbol: &str) -> crate::error::Result<TradingStatusRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(
                MarketCommand::SubscribeTradingStatus {
                    symbols: vec![Symbol::new(symbol)],
                },
            ))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(TradingStatusRef::new(self.read_handle(), symbol))
    }

    pub async fn kline(
        &mut self,
        symbol: &str,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<KlineHandle> {
        let data_length = normalize_serial_data_length(data_length)?;
        let duration_ns = duration_to_ns(duration)?;
        let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{data_length}");

        if !self.driver.serial_charts.contains(&chart_id) {
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
            self.driver.serial_charts.insert(chart_id.clone());
        }

        let serial = KlineHandle::new(
            self.read_handle(),
            symbol.to_string(),
            duration_ns,
            data_length,
            chart_id,
        );
        self.wait_until_ready(|| serial.is_ready()).await?;

        Ok(serial)
    }

    pub async fn tick(
        &mut self,
        symbol: &str,
        data_length: usize,
    ) -> crate::error::Result<TickHandle> {
        let data_length = normalize_serial_data_length(data_length)?;
        let chart_id = format!("wait-tick-{symbol}-{data_length}");

        if !self.driver.serial_charts.contains(&chart_id) {
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
            self.driver.serial_charts.insert(chart_id.clone());
        }

        let serial = TickHandle::new(
            self.read_handle(),
            symbol.to_string(),
            data_length,
            chart_id,
        );
        self.wait_until_ready(|| serial.is_ready()).await?;

        Ok(serial)
    }

    pub async fn login_trade_account(
        &mut self,
        broker_id: &str,
        account_id: &str,
        password: &str,
        account_type: TradeAccountType,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<AccountRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Trade(TradeCommand::Login(
                TradeLoginCommand {
                    account_id: AccountId::new(account_id),
                    broker_id: broker_id.to_owned(),
                    password: password.to_owned(),
                    account_type,
                    front_broker: None,
                    front_url: None,
                    client_app_id: None,
                    client_system_info: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        let account = self.account(account_id);
        self.wait_until_ready_until(|| account.is_ready(), deadline, "trade account not ready")
            .await?;

        Ok(account)
    }

    pub async fn insert_order(
        &mut self,
        account_id: &str,
        symbol: &str,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
        limit_price: OrderPrice,
    ) -> crate::error::Result<OrderRef> {
        let order_seq = self.driver.next_order_seq.fetch_add(1, Ordering::Relaxed);
        let order_id = OrderId::new(format!("wait-order-{order_seq}"));
        self.submit_insert_order(WaitInsertOrderRequest {
            account_id: account_id.to_owned(),
            symbol: symbol.to_owned(),
            order_id: order_id.clone(),
            direction,
            offset,
            volume,
            limit_price,
        })
        .await?;

        Ok(self.order(account_id, order_id.as_str()))
    }

    pub fn limit_order(
        &mut self,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> crate::order_intent::LimitOrderIntent<'_> {
        crate::order_intent::LimitOrderIntent::new(self, account_id, symbol)
    }

    pub(crate) async fn submit_insert_order(
        &mut self,
        request: WaitInsertOrderRequest,
    ) -> crate::error::Result<tqsdk_core::CommandId> {
        let (price_type, limit_price, time_condition) = request.limit_price.into_command_parts();

        let command_id = self
            .driver
            .session
            .submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
                TradeInsertOrderCommand {
                    account_id: AccountId::new(request.account_id),
                    order_id: request.order_id,
                    symbol: Symbol::new(request.symbol),
                    direction: request.direction,
                    offset: request.offset,
                    volume: request.volume,
                    price_type,
                    limit_price,
                    time_condition,
                    volume_condition: TradeVolumeCondition::Any,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(command_id)
    }

    pub async fn insert_limit_order(
        &mut self,
        account_id: &str,
        symbol: &str,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
        limit_price: f64,
    ) -> crate::error::Result<OrderRef> {
        self.insert_order(
            account_id,
            symbol,
            direction,
            offset,
            volume,
            OrderPrice::limit(limit_price)?,
        )
        .await
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

    async fn wait_until_ready<F>(&mut self, mut ready: F) -> crate::error::Result<()>
    where
        F: FnMut() -> crate::error::Result<bool>,
    {
        self.wait_until_ready_until(&mut ready, None, "object not ready")
            .await
    }

    async fn wait_until_ready_until<F>(
        &mut self,
        mut ready: F,
        deadline: Option<tokio::time::Instant>,
        not_ready_message: &'static str,
    ) -> crate::error::Result<()>
    where
        F: FnMut() -> crate::error::Result<bool>,
    {
        if ready()? {
            return Ok(());
        }

        let previous_last_commit = self.driver.last_commit.clone();
        let mut replay = Vec::new();

        while !ready()? {
            if !self.wait_update(deadline).await? {
                for commit in replay.into_iter().rev() {
                    self.driver.deferred_commits.push_front(commit);
                }
                self.driver.last_commit = previous_last_commit;
                return Err(crate::error::WaitFacadeError::InvalidState(
                    not_ready_message,
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

    pub(crate) fn begin_fixture_wait(&self) -> crate::error::Result<WaitGuard<'_>> {
        self.driver.begin_wait()
    }

    pub(crate) fn push_fixture_deferred_commit(&mut self, commit: tqsdk_core::SharedCommitResult) {
        self.driver.deferred_commits.push_back(commit);
    }

    pub(crate) fn read_handle(&self) -> WaitReadHandle {
        WaitReadHandle::new(self.driver.reader.clone())
    }
}

fn current_dt_from_reader(reader: &tqsdk_core::RuntimeReader) -> Option<i64> {
    let guard = reader.read();

    guard
        .get_path(&["_tqsdk_backtest", "current_dt"])
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            let replay = guard.get_path(&["replay"])?;
            replay
                .pointer("/cursor/dt")
                .and_then(serde_json::Value::as_i64)
                .or_else(|| {
                    replay.as_object()?.values().find_map(|session| {
                        session
                            .pointer("/cursor/dt")
                            .and_then(serde_json::Value::as_i64)
                    })
                })
        })
}

fn normalize_serial_data_length(data_length: usize) -> crate::error::Result<usize> {
    if data_length == 0 {
        return Err(crate::error::WaitFacadeError::InvalidState(
            "serial data_length must be greater than zero",
        ));
    }

    Ok(data_length.min(10_000))
}

fn duration_to_ns(duration: Duration) -> crate::error::Result<i64> {
    if duration.is_zero() {
        return Err(crate::error::WaitFacadeError::InvalidState(
            "kline duration must be positive",
        ));
    }

    let secs = i64::try_from(duration.as_secs())
        .map_err(|_| crate::error::WaitFacadeError::InvalidState("kline duration is too large"))?;
    secs.checked_mul(1_000_000_000)
        .and_then(|ns| ns.checked_add(i64::from(duration.subsec_nanos())))
        .ok_or(crate::error::WaitFacadeError::InvalidState(
            "kline duration is too large",
        ))
}
