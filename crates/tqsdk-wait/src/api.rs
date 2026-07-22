#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tqsdk_core::{
    AccountId, MarketChartCommand, OrderId, RuntimeCommand, Symbol, TradeAccountType, TradeCommand,
    TradeDirection, TradeInsertOrderCommand, TradeLoginCommand, TradeOffset, TradeVolumeCondition,
};
use tqsdk_session::SessionClient;

use crate::backtest::{
    BacktestCommitAction, BacktestPump, BacktestPumpMode, BacktestSyntheticCommit, TqBacktest,
};
use crate::driver::{WaitDriver, WaitGuard};
use crate::price::OrderPrice;
use crate::refs::{
    AccountRef, KlineHandle, MultiKlineHandle, NotificationRef, OrderRef, PositionRef,
    PreInsertOrderRef, QuoteRef, QuoteSet, RiskManagementDataRef, RiskManagementRuleRef,
    SecurityAccountRef, SecurityOrderRef, SecurityPositionRef, SecurityTradeRef, SettlementInfoRef,
    TickHandle, TradeRef, TradingStatusRef,
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
        Self::new_with_backtest(session, None)
    }

    #[must_use]
    pub(crate) fn new_with_backtest(session: SessionClient, backtest: Option<TqBacktest>) -> Self {
        Self::new_with_backtest_mode(session, backtest, BacktestPumpMode::Strategy)
    }

    #[must_use]
    pub(crate) fn new_with_backtest_mode(
        session: SessionClient,
        backtest: Option<TqBacktest>,
        backtest_pump_mode: BacktestPumpMode,
    ) -> Self {
        let handle = session.handle().clone();
        Self::from_runtime_parts(handle, session, backtest, backtest_pump_mode)
    }

    #[must_use]
    fn from_runtime_parts(
        handle: tqsdk_core::RuntimeHandle,
        session: SessionClient,
        backtest: Option<TqBacktest>,
        backtest_pump_mode: BacktestPumpMode,
    ) -> Self {
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
                quote_subscriptions: Default::default(),
                trading_status_subscriptions: Default::default(),
                serial_charts: Default::default(),
                backtest_pump: backtest.as_ref().map(|_| match backtest_pump_mode {
                    BacktestPumpMode::Strategy => BacktestPump::new(),
                    BacktestPumpMode::CacheFill => BacktestPump::new_cache_fill(),
                }),
                backtest,
                backtest_finished: false,
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
                match handle_backtest_reader_commit(
                    self.driver.backtest.clone(),
                    self.driver.backtest_pump.as_mut(),
                    self.driver.session.clone(),
                    self.driver.reader.clone(),
                    commit,
                )
                .await?
                {
                    Some(HandledBacktestCommit::Commit(commit)) => {
                        self.driver.last_commit = Some(commit);
                        return Ok(true);
                    }
                    Some(HandledBacktestCommit::Synthetic(synthetic)) => {
                        consume_returned_synthetic_commit(
                            &self.driver.reader,
                            &mut self.driver.cursor,
                            &synthetic.commit,
                        );
                        self.driver.last_commit = Some(synthetic.commit);
                        return Ok(true);
                    }
                    None => continue,
                }
            }

            if let Some(synthetic) = emit_pending_backtest_commit(
                self.driver.backtest.clone(),
                self.driver.backtest_pump.as_mut(),
                self.driver.session.clone(),
                self.driver.reader.clone(),
            )
            .await?
            {
                consume_returned_synthetic_commit(
                    &self.driver.reader,
                    &mut self.driver.cursor,
                    &synthetic.commit,
                );
                self.driver.last_commit = Some(synthetic.commit);
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
        if self.driver.backtest_finished {
            return Ok(None);
        }

        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit.clone());
            let step = WaitStep::new(commit, current_dt_from_reader(&self.driver.reader));
            if backtest_reached_end(self.driver.backtest.as_ref(), &step) {
                self.driver.backtest_finished = true;
            }
            return Ok(Some(step));
        }

        loop {
            if let Some(commit) = self.driver.reader.next(&mut self.driver.cursor) {
                match handle_backtest_reader_commit(
                    self.driver.backtest.clone(),
                    self.driver.backtest_pump.as_mut(),
                    self.driver.session.clone(),
                    self.driver.reader.clone(),
                    commit,
                )
                .await?
                {
                    Some(HandledBacktestCommit::Commit(commit)) => {
                        self.driver.last_commit = Some(commit.clone());
                        let step =
                            WaitStep::new(commit, current_dt_from_reader(&self.driver.reader));
                        if backtest_reached_end(self.driver.backtest.as_ref(), &step) {
                            self.driver.backtest_finished = true;
                        }
                        return Ok(Some(step));
                    }
                    Some(HandledBacktestCommit::Synthetic(synthetic)) => {
                        consume_returned_synthetic_commit(
                            &self.driver.reader,
                            &mut self.driver.cursor,
                            &synthetic.commit,
                        );
                        self.driver.last_commit = Some(synthetic.commit.clone());
                        let step = WaitStep::new(
                            synthetic.commit,
                            synthetic
                                .current_dt
                                .or_else(|| current_dt_from_reader(&self.driver.reader)),
                        );
                        if backtest_reached_end(self.driver.backtest.as_ref(), &step) {
                            self.driver.backtest_finished = true;
                        }
                        return Ok(Some(step));
                    }
                    None => continue,
                }
            }

            if let Some(synthetic) = emit_pending_backtest_commit(
                self.driver.backtest.clone(),
                self.driver.backtest_pump.as_mut(),
                self.driver.session.clone(),
                self.driver.reader.clone(),
            )
            .await?
            {
                consume_returned_synthetic_commit(
                    &self.driver.reader,
                    &mut self.driver.cursor,
                    &synthetic.commit,
                );
                self.driver.last_commit = Some(synthetic.commit.clone());
                let step = WaitStep::new(
                    synthetic.commit,
                    synthetic
                        .current_dt
                        .or_else(|| current_dt_from_reader(&self.driver.reader)),
                );
                if backtest_reached_end(self.driver.backtest.as_ref(), &step) {
                    self.driver.backtest_finished = true;
                }
                return Ok(Some(step));
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

    /// Return the latest committed update as a [`WaitStep`].
    ///
    /// This is primarily an internal facade bridge for consumers that need the
    /// authoritative change set without advancing the session a second time.
    #[doc(hidden)]
    #[must_use]
    pub fn last_step(&self) -> Option<WaitStep> {
        self.driver
            .last_commit
            .clone()
            .map(|commit| WaitStep::new(commit, current_dt_from_reader(&self.driver.reader)))
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
        let quote = QuoteRef::new(self.read_handle(), symbol);

        if self.driver.quote_subscriptions.insert(symbol.to_owned()) {
            self.driver.session.ensure_quotes([symbol]).await?;
        }

        Ok(quote)
    }

    pub async fn quotes<I, S>(&mut self, symbols: I) -> crate::error::Result<QuoteSet>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut refs = BTreeMap::new();
        let mut new_symbols = Vec::new();

        for symbol in symbols {
            let symbol = symbol.as_ref();
            refs.insert(
                symbol.to_string(),
                QuoteRef::new(self.read_handle(), symbol),
            );
            if self.driver.quote_subscriptions.insert(symbol.to_owned()) {
                new_symbols.push(symbol.to_string());
            }
        }

        if !new_symbols.is_empty() {
            self.driver
                .session
                .ensure_quotes(new_symbols.iter().map(String::as_str))
                .await?;
        }

        Ok(QuoteSet::new(refs))
    }

    pub fn startup_recovery(&mut self) -> crate::recovery::WaitStartupRecovery<'_> {
        crate::recovery::WaitStartupRecovery::new(self)
    }

    pub async fn trading_status(&mut self, symbol: &str) -> crate::error::Result<TradingStatusRef> {
        let trading_status = TradingStatusRef::new(self.read_handle(), symbol);

        if self
            .driver
            .trading_status_subscriptions
            .insert(symbol.to_owned())
        {
            self.driver.session.ensure_trading_status([symbol]).await?;
        }

        Ok(trading_status)
    }

    pub async fn kline(
        &mut self,
        symbol: &str,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<KlineHandle> {
        validate_single_serial_symbol(
            symbol,
            "kline accepts one symbol; use kline_multi for multi-contract kline serials",
        )?;
        let data_length = normalize_serial_data_length(data_length)?;
        let duration_ns = duration_to_ns(duration)?;
        let chart_id = format!(
            "wait-kline-{}-{duration_ns}-{data_length}",
            sanitize_chart_token(symbol)
        );

        if !self.driver.serial_charts.contains(&chart_id) {
            self.driver
                .session
                .ensure_chart(MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                })
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            self.driver.serial_charts.insert(chart_id.clone());
        }

        Ok(KlineHandle::new(
            self.read_handle(),
            symbol.to_string(),
            duration_ns,
            data_length,
            chart_id,
        ))
    }

    pub async fn kline_multi<I, S>(
        &mut self,
        symbols: I,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<MultiKlineHandle>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = normalize_multi_kline_symbols(symbols)?;
        let data_length = normalize_serial_data_length(data_length)?;
        let duration_ns = duration_to_ns(duration)?;
        let chart_id = format!(
            "wait-kline-multi-{}-{duration_ns}-{data_length}",
            symbols
                .iter()
                .map(|symbol| sanitize_chart_token(symbol))
                .collect::<Vec<_>>()
                .join("_")
        );
        let request_view_width = if symbols.len() == 1 {
            data_length
        } else {
            MAX_SERIAL_DATA_LENGTH
        };

        if !self.driver.serial_charts.contains(&chart_id) {
            self.driver
                .session
                .ensure_chart(MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: symbols.iter().map(Symbol::new).collect(),
                    duration_ns,
                    view_width: request_view_width,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                })
                .await
                .map_err(crate::error::WaitFacadeError::Session)?;
            self.driver.serial_charts.insert(chart_id.clone());
        }

        Ok(MultiKlineHandle::new(
            self.read_handle(),
            symbols,
            duration_ns,
            data_length,
            chart_id,
        ))
    }

    pub async fn kline_ready(
        &mut self,
        symbol: &str,
        duration: Duration,
        data_length: usize,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<KlineHandle> {
        let serial = self.kline(symbol, duration, data_length).await?;
        self.wait_until_ready_until(|| serial.is_ready(), deadline, "serial chart not ready")
            .await?;
        Ok(serial)
    }

    pub async fn tick(
        &mut self,
        symbol: &str,
        data_length: usize,
    ) -> crate::error::Result<TickHandle> {
        validate_single_serial_symbol(
            symbol,
            "tick serials accept one symbol; multi-contract tick serials are not supported",
        )?;
        let data_length = normalize_serial_data_length(data_length)?;
        let chart_id = format!("wait-tick-{}-{data_length}", sanitize_chart_token(symbol));

        if !self.driver.serial_charts.contains(&chart_id) {
            if let Some(backtest) = self.driver.backtest.clone() {
                let Some(pump) = self.driver.backtest_pump.as_mut() else {
                    return Err(crate::error::WaitFacadeError::InvalidState(
                        "backtest pump not initialized",
                    ));
                };
                pump.ensure_tick_serial(
                    &self.driver.session,
                    &backtest,
                    symbol,
                    data_length,
                    &chart_id,
                )
                .await?;
            } else {
                self.driver
                    .session
                    .ensure_chart(MarketChartCommand {
                        chart_id: chart_id.clone(),
                        symbols: vec![Symbol::new(symbol)],
                        duration_ns: 0,
                        view_width: data_length,
                        left_kline_id: None,
                        focus_datetime_ns: None,
                        focus_position: None,
                    })
                    .await
                    .map_err(crate::error::WaitFacadeError::Session)?;
            }
            self.driver.serial_charts.insert(chart_id.clone());
        }

        Ok(TickHandle::new(
            self.read_handle(),
            symbol.to_string(),
            data_length,
            chart_id,
        ))
    }

    pub async fn tick_ready(
        &mut self,
        symbol: &str,
        data_length: usize,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<TickHandle> {
        let serial = self.tick(symbol, data_length).await?;
        self.wait_until_ready_until(|| serial.is_ready(), deadline, "serial chart not ready")
            .await?;
        Ok(serial)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn backtest_tick_serial_exhausted(&self, handle: &TickHandle) -> Option<bool> {
        self.driver
            .backtest_pump
            .as_ref()?
            .tick_serial_exhausted(&handle.chart_id)
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

async fn emit_pending_backtest_commit(
    backtest: Option<TqBacktest>,
    pump: Option<&mut BacktestPump>,
    session: SessionClient,
    reader: tqsdk_core::RuntimeReader,
) -> crate::error::Result<Option<BacktestSyntheticCommit>> {
    let Some(backtest) = backtest else {
        return Ok(None);
    };
    let Some(pump) = pump else {
        return Ok(None);
    };

    pump.emit_pending_tick(&session, &reader, &backtest).await
}

async fn handle_backtest_reader_commit(
    backtest: Option<TqBacktest>,
    pump: Option<&mut BacktestPump>,
    session: SessionClient,
    reader: tqsdk_core::RuntimeReader,
    commit: tqsdk_core::SharedCommitResult,
) -> crate::error::Result<Option<HandledBacktestCommit>> {
    let Some(backtest) = backtest else {
        return Ok(Some(HandledBacktestCommit::Commit(commit)));
    };
    let Some(pump) = pump else {
        return Ok(Some(HandledBacktestCommit::Commit(commit)));
    };

    match pump
        .handle_commit(commit, &session, &reader, &backtest)
        .await?
    {
        BacktestCommitAction::Expose(commit) => Ok(Some(HandledBacktestCommit::Commit(commit))),
        BacktestCommitAction::Synthetic(commit) => {
            Ok(Some(HandledBacktestCommit::Synthetic(commit)))
        }
        BacktestCommitAction::Suppressed => Ok(None),
    }
}

enum HandledBacktestCommit {
    Commit(tqsdk_core::SharedCommitResult),
    Synthetic(BacktestSyntheticCommit),
}

fn consume_returned_synthetic_commit(
    reader: &tqsdk_core::RuntimeReader,
    cursor: &mut tqsdk_core::UpdateCursor,
    synthetic: &tqsdk_core::CommitResult,
) {
    if cursor.next_revision() == synthetic.revision {
        let consumed = reader.next(cursor);
        debug_assert!(
            consumed
                .as_ref()
                .is_some_and(|commit| commit.revision == synthetic.revision),
            "returned synthetic commit should be the next unread commit",
        );
    }
}

fn backtest_reached_end(backtest: Option<&TqBacktest>, step: &WaitStep) -> bool {
    if let Some(backtest) = backtest
        && let Some(current_dt) = step.current_dt()
    {
        return current_dt >= backtest.end_datetime_ns();
    }
    false
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

const MAX_SERIAL_DATA_LENGTH: usize = 10_000;

fn normalize_serial_data_length(data_length: usize) -> crate::error::Result<usize> {
    if data_length == 0 {
        return Err(crate::error::WaitFacadeError::InvalidState(
            "serial data_length must be greater than zero",
        ));
    }

    Ok(data_length.min(MAX_SERIAL_DATA_LENGTH))
}

fn validate_single_serial_symbol(symbol: &str, message: &'static str) -> crate::error::Result<()> {
    if symbol.split(',').count() > 1 {
        return Err(crate::error::WaitFacadeError::InvalidState(message));
    }
    if symbol.trim().is_empty() {
        return Err(crate::error::WaitFacadeError::InvalidState(
            "serial symbol must not be empty",
        ));
    }
    Ok(())
}

fn normalize_multi_kline_symbols<I, S>(symbols: I) -> crate::error::Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = symbol.as_ref().trim();
        if symbol.is_empty() {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "kline_multi requires non-empty symbols",
            ));
        }
        if symbol.contains(',') {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "kline_multi expects separate symbols, not comma-joined items",
            ));
        }
        if normalized.iter().any(|existing| existing == symbol) {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "kline_multi symbols must be unique",
            ));
        }
        normalized.push(symbol.to_string());
    }
    if normalized.is_empty() {
        return Err(crate::error::WaitFacadeError::InvalidState(
            "kline_multi requires at least one symbol",
        ));
    }
    Ok(normalized)
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
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
