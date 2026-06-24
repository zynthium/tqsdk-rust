#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    Account, CommitScope, InputPayload, IoEvent, Kline, Order, Position, ProtocolDomain, Quote,
    RuntimeInput, Tick, Trade, TradeDirection, TradeOffset,
};

use crate::replay::{
    ReplayMarketEvent, ReplayMarketPayload, ReplayMarketPayloadKind, ReplayMarketSource,
    ReplayStepMeta,
};
use crate::sim::{TqSim, TqSimStepReport};
use crate::strategy::StrategyHostBuilder;
use crate::testing::StrategyTestHarness;
use crate::{Result, StrategyContext, StrategyHost, TaskError, TaskHost};

const DEFAULT_TRADING_DAYS_PER_YEAR: f64 = 250.0;

/// Local Python-compatible strategy backtest over task-owned replay market events.
pub struct StrategyBacktestBuilder {
    replay: ReplayMarketSource,
    sim: TqSim,
    quotes: Vec<String>,
    price_ticks: HashMap<String, f64>,
    default_price_tick: Option<f64>,
}

/// Local Python-compatible strategy backtest host.
pub struct StrategyBacktest {
    replay: ReplayMarketSource,
    strategy: StrategyHost,
    sim: TqSim,
    tracked_symbols: Vec<String>,
    price_ticks: HashMap<String, f64>,
    quote_metadata_price_ticks: HashMap<String, f64>,
    default_price_tick: Option<f64>,
    summary: StrategyBacktestSummary,
}

/// Metadata for the market event that produced a backtest context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyBacktestEvent {
    source: String,
    symbol: String,
    received_at_ns: i64,
    event_time_ns: i64,
    underlying_symbol: Option<String>,
}

/// Lightweight local backtest summary.
#[derive(Debug, Clone)]
pub struct StrategyBacktestSummary {
    event_count: usize,
    quote_count: usize,
    tick_count: usize,
    kline_count: usize,
    balance_points: Vec<StrategyBacktestBalancePoint>,
    equity_points: Vec<StrategyBacktestEquityPoint>,
    closed_profit_points: Vec<StrategyBacktestClosedProfitPoint>,
    orders: Vec<Order>,
    trades: Vec<Trade>,
    initial_account: Account,
    final_account: Account,
    final_positions: Vec<Position>,
    peak_balance: f64,
    max_balance_drawdown: f64,
    max_balance_drawdown_rate: f64,
    peak_equity: f64,
    max_equity_drawdown: f64,
    max_equity_drawdown_rate: f64,
}

/// Cash-balance point recorded by local backtest summary.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestBalancePoint {
    event_count: usize,
    event_time_ns: Option<i64>,
    balance: f64,
    return_rate: f64,
    drawdown: f64,
    drawdown_rate: f64,
}

/// Mark-to-market equity point recorded by local backtest summary.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestEquityPoint {
    event_count: usize,
    event_time_ns: Option<i64>,
    equity: f64,
    return_rate: f64,
    drawdown: f64,
    drawdown_rate: f64,
}

/// Realized close-profit observation recorded by local backtest summary.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestClosedProfitPoint {
    event_count: usize,
    event_time_ns: Option<i64>,
    trade_count: usize,
    profit: f64,
}

/// End-of-day mark-to-market equity return derived from replay observations.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestDailyEquityReturn {
    date: NaiveDate,
    equity: f64,
    return_rate: f64,
}

/// End-of-day cash-balance return derived from replay observations.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestDailyBalanceReturn {
    date: NaiveDate,
    balance: f64,
    return_rate: f64,
}

/// Explicit daily return window for exchange/trading-day grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyBacktestDailyReturnWindow {
    date: NaiveDate,
    start_time_ns: i64,
    end_time_ns: i64,
}

/// Strategy context plus local sim controls for the current backtest step.
pub struct StrategyBacktestContext<'a> {
    event: StrategyBacktestEvent,
    context: StrategyContext<'a>,
    sim: &'a mut TqSim,
    summary: &'a mut StrategyBacktestSummary,
    tracked_symbols: &'a [String],
}

impl StrategyBacktest {
    #[must_use]
    pub fn builder(replay: ReplayMarketSource) -> StrategyBacktestBuilder {
        StrategyBacktestBuilder::new(replay)
    }

    pub async fn next(&mut self) -> Result<Option<StrategyBacktestContext<'_>>> {
        let Some(event) = self.replay.next() else {
            return Ok(None);
        };
        let backtest_event = StrategyBacktestEvent::from_replay_event(&event);
        let payload_kind = event.payload_kind();
        match event.payload() {
            ReplayMarketPayload::Quote(quote) => {
                let quote =
                    quote_with_replay_underlying((**quote).clone(), event.underlying_symbol());
                self.ingest_quote(event.symbol(), &quote)?;
            }
            ReplayMarketPayload::Tick(tick) => {
                let quote =
                    quote_with_replay_underlying(quote_from_tick(tick), event.underlying_symbol());
                self.ingest_quote(event.symbol(), &quote)?;
            }
            ReplayMarketPayload::Kline { row, .. } => {
                let price_tick = self.price_tick(event.symbol())?;
                let checkpoints = kline_quote_checkpoints(row, price_tick);
                for quote in checkpoints {
                    let quote = quote_with_replay_underlying(quote, event.underlying_symbol());
                    self.ingest_quote(event.symbol(), &quote)?;
                }
            }
        }
        self.summary.record_payload(payload_kind);
        self.summary.record_account_snapshot(
            &self.sim,
            &self.tracked_symbols,
            Some(backtest_event.event_time_ns()),
        );

        let context = self.strategy.next_once().await?;
        Ok(Some(StrategyBacktestContext {
            event: backtest_event,
            context,
            sim: &mut self.sim,
            summary: &mut self.summary,
            tracked_symbols: &self.tracked_symbols,
        }))
    }

    #[must_use]
    pub fn sim(&self) -> &TqSim {
        &self.sim
    }

    #[must_use]
    pub fn sim_mut(&mut self) -> &mut TqSim {
        &mut self.sim
    }

    #[must_use]
    pub fn strategy(&self) -> &StrategyHost {
        &self.strategy
    }

    #[must_use]
    pub fn strategy_mut(&mut self) -> &mut StrategyHost {
        &mut self.strategy
    }

    #[must_use]
    pub fn summary(&self) -> StrategyBacktestSummary {
        let mut summary = self.summary.clone();
        summary.record_account_snapshot(&self.sim, &self.tracked_symbols, None);
        summary
    }

    fn ingest_quote(&mut self, symbol: &str, quote: &Quote) -> Result<()> {
        self.remember_quote_metadata(symbol, quote);
        ingest_quote_event(self.strategy.task_host(), symbol, quote)?;
        let report = self.sim.update_quote(symbol.to_owned(), quote.clone());
        self.sim
            .ingest_step_report(self.strategy.task_host(), &report)?;
        Ok(())
    }

    fn price_tick(&self, symbol: &str) -> Result<f64> {
        self.price_ticks
            .get(symbol)
            .copied()
            .or_else(|| self.quote_metadata_price_ticks.get(symbol).copied())
            .or(self.default_price_tick)
            .ok_or(TaskError::Unsupported(
                "StrategyBacktest kline event requires price_tick(symbol, tick), replayed quote price_tick metadata, or default_price_tick(tick)",
            ))
    }

    fn remember_quote_metadata(&mut self, symbol: &str, quote: &Quote) {
        if quote.price_tick.is_finite() && quote.price_tick > 0.0 {
            self.quote_metadata_price_ticks
                .entry(symbol.to_owned())
                .or_insert(quote.price_tick);
        }
    }
}

impl StrategyBacktestBuilder {
    #[must_use]
    pub fn new(replay: ReplayMarketSource) -> Self {
        Self {
            replay,
            sim: TqSim::new(),
            quotes: Vec::new(),
            price_ticks: HashMap::new(),
            default_price_tick: None,
        }
    }

    #[must_use]
    pub fn sim(mut self, sim: TqSim) -> Self {
        self.sim = sim;
        self
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>) -> Self {
        let symbol = symbol.as_ref();
        if !self.quotes.iter().any(|existing| existing == symbol) {
            self.quotes.push(symbol.to_owned());
        }
        self
    }

    #[must_use]
    pub fn price_tick(mut self, symbol: impl AsRef<str>, price_tick: f64) -> Self {
        self.price_ticks
            .insert(symbol.as_ref().to_owned(), price_tick);
        self
    }

    #[must_use]
    pub fn instrument_spec(mut self, spec: tqsdk_session::InstrumentSpec) -> Self {
        let symbol = spec.symbol.as_str().to_owned();
        self.price_ticks
            .entry(symbol.clone())
            .or_insert(spec.price_tick);
        self.sim
            .set_contract_multiplier(symbol, spec.volume_multiple as f64);
        self
    }

    #[must_use]
    pub fn instrument_specs(
        mut self,
        specs: impl IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    ) -> Self {
        for spec in specs {
            self = self.instrument_spec(spec);
        }
        self
    }

    /// Set fallback price tick for kline quote synthesis.
    ///
    /// Per-symbol [`StrategyBacktestBuilder::price_tick`] overrides this fallback.
    #[must_use]
    pub fn default_price_tick(mut self, price_tick: f64) -> Self {
        self.default_price_tick = Some(price_tick);
        self
    }

    pub async fn build(self) -> Result<StrategyBacktest> {
        let Self {
            replay,
            sim,
            mut quotes,
            price_ticks,
            default_price_tick,
        } = self;
        validate_price_ticks(&price_ticks)?;
        validate_default_price_tick(default_price_tick)?;
        for symbol in replay.symbols() {
            if !quotes.iter().any(|existing| existing == symbol) {
                quotes.push(symbol.to_owned());
            }
        }
        let harness = StrategyTestHarness::new().build()?;
        let host = harness.into_task_host();
        let mut sim = sim;
        for quote in &quotes {
            sim.ensure_position(quote);
        }
        sim.seed_runtime(&host)?;
        let mut builder = StrategyHostBuilder::new(host).account(sim.account_id());
        for quote in &quotes {
            builder = builder.quote(quote);
        }
        let mut strategy = builder.build().await?;
        drain_initial_commits(&mut strategy).await?;
        let tracked_symbols = quotes;
        let summary = StrategyBacktestSummary::from_sim(&sim, &tracked_symbols);
        Ok(StrategyBacktest {
            replay,
            strategy,
            sim,
            tracked_symbols,
            price_ticks,
            quote_metadata_price_ticks: HashMap::new(),
            default_price_tick,
            summary,
        })
    }
}

impl StrategyBacktestSummary {
    fn from_sim(sim: &TqSim, symbols: &[String]) -> Self {
        let initial_account = sim.account();
        let initial_balance = initial_account.balance;
        let initial_equity = account_equity(&initial_account);
        let mut summary = Self {
            event_count: 0,
            quote_count: 0,
            tick_count: 0,
            kline_count: 0,
            balance_points: Vec::new(),
            equity_points: Vec::new(),
            closed_profit_points: Vec::new(),
            orders: Vec::new(),
            trades: Vec::new(),
            initial_account: initial_account.clone(),
            final_account: initial_account,
            final_positions: Vec::new(),
            peak_balance: initial_balance,
            max_balance_drawdown: 0.0,
            max_balance_drawdown_rate: 0.0,
            peak_equity: initial_equity,
            max_equity_drawdown: 0.0,
            max_equity_drawdown_rate: 0.0,
        };
        summary.record_account_snapshot(sim, symbols, None);
        summary
    }

    fn record_payload(&mut self, kind: ReplayMarketPayloadKind) {
        self.event_count += 1;
        match kind {
            ReplayMarketPayloadKind::Quote => self.quote_count += 1,
            ReplayMarketPayloadKind::Kline => self.kline_count += 1,
            ReplayMarketPayloadKind::Tick => self.tick_count += 1,
        }
    }

    fn refresh_from_sim(&mut self, sim: &TqSim, symbols: &[String]) {
        self.orders = sim.orders();
        self.trades = sim.trades();
        self.final_account = sim.account();
        self.final_positions = self
            .orders
            .iter()
            .map(|order| format!("{}.{}", order.exchange_id, order.instrument_id))
            .chain(symbols.iter().cloned())
            .chain(
                self.trades
                    .iter()
                    .map(|trade| format!("{}.{}", trade.exchange_id, trade.instrument_id)),
            )
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|symbol| sim.position(symbol))
            .collect();
    }

    fn record_account_snapshot(
        &mut self,
        sim: &TqSim,
        symbols: &[String],
        event_time_ns: Option<i64>,
    ) {
        let previous_close_profit = self.final_account.close_profit;
        let previous_trade_count = self.trades.len();
        self.refresh_from_sim(sim, symbols);
        self.record_closed_profit_point(previous_close_profit, previous_trade_count, event_time_ns);
        self.record_balance_point(event_time_ns);
        self.record_equity_point(event_time_ns);
    }

    fn record_closed_profit_point(
        &mut self,
        previous_close_profit: f64,
        previous_trade_count: usize,
        event_time_ns: Option<i64>,
    ) {
        let profit = self.final_account.close_profit - previous_close_profit;
        if !profit.is_finite() {
            return;
        }
        let trade_count = self.trades[previous_trade_count..]
            .iter()
            .filter(|trade| is_close_trade(trade))
            .count();
        if trade_count == 0 && profit == 0.0 {
            return;
        }
        self.closed_profit_points
            .push(StrategyBacktestClosedProfitPoint {
                event_count: self.event_count,
                event_time_ns,
                trade_count,
                profit,
            });
    }

    fn record_balance_point(&mut self, event_time_ns: Option<i64>) {
        let balance = self.final_account.balance;
        if self.balance_points.last().is_some_and(|point| {
            point.balance == balance && same_observation_bucket(point.event_time_ns, event_time_ns)
        }) {
            return;
        }
        if balance.is_finite() && (!self.peak_balance.is_finite() || balance > self.peak_balance) {
            self.peak_balance = balance;
        }
        let point = StrategyBacktestBalancePoint::new(
            self.event_count,
            event_time_ns,
            balance,
            self.initial_account.balance,
            self.peak_balance,
        );
        if point.drawdown.is_finite() && point.drawdown > self.max_balance_drawdown {
            self.max_balance_drawdown = point.drawdown;
            self.max_balance_drawdown_rate = point.drawdown_rate;
        }
        self.balance_points.push(point);
    }

    fn record_equity_point(&mut self, event_time_ns: Option<i64>) {
        let equity = account_equity(&self.final_account);
        if self.equity_points.last().is_some_and(|point| {
            point.equity == equity && same_observation_bucket(point.event_time_ns, event_time_ns)
        }) {
            return;
        }
        if equity.is_finite() && (!self.peak_equity.is_finite() || equity > self.peak_equity) {
            self.peak_equity = equity;
        }
        let point = StrategyBacktestEquityPoint::new(
            self.event_count,
            event_time_ns,
            equity,
            account_equity(&self.initial_account),
            self.peak_equity,
        );
        if point.drawdown.is_finite() && point.drawdown > self.max_equity_drawdown {
            self.max_equity_drawdown = point.drawdown;
            self.max_equity_drawdown_rate = point.drawdown_rate;
        }
        self.equity_points.push(point);
    }

    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    #[must_use]
    pub fn quote_count(&self) -> usize {
        self.quote_count
    }

    #[must_use]
    pub fn tick_count(&self) -> usize {
        self.tick_count
    }

    #[must_use]
    pub fn kline_count(&self) -> usize {
        self.kline_count
    }

    #[must_use]
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }

    #[must_use]
    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    #[must_use]
    pub fn trade_log(&self) -> &[Trade] {
        &self.trades
    }

    #[must_use]
    pub fn buy_trade_count(&self) -> usize {
        self.trades
            .iter()
            .filter(|trade| trade.direction == Some(TradeDirection::Buy))
            .count()
    }

    #[must_use]
    pub fn sell_trade_count(&self) -> usize {
        self.trades
            .iter()
            .filter(|trade| trade.direction == Some(TradeDirection::Sell))
            .count()
    }

    #[must_use]
    pub fn open_trade_count(&self) -> usize {
        self.trades
            .iter()
            .filter(|trade| trade.offset == Some(TradeOffset::Open))
            .count()
    }

    #[must_use]
    pub fn close_trade_count(&self) -> usize {
        self.trades
            .iter()
            .filter(|trade| is_close_trade(trade))
            .count()
    }

    #[must_use]
    pub fn balance_points(&self) -> &[StrategyBacktestBalancePoint] {
        &self.balance_points
    }

    #[must_use]
    pub fn equity_points(&self) -> &[StrategyBacktestEquityPoint] {
        &self.equity_points
    }

    #[must_use]
    pub fn closed_profit_points(&self) -> &[StrategyBacktestClosedProfitPoint] {
        &self.closed_profit_points
    }

    #[must_use]
    pub fn initial_account(&self) -> &Account {
        &self.initial_account
    }

    #[must_use]
    pub fn final_account(&self) -> &Account {
        &self.final_account
    }

    #[must_use]
    pub fn final_positions(&self) -> &[Position] {
        &self.final_positions
    }

    #[must_use]
    pub fn balance_change(&self) -> f64 {
        self.final_account.balance - self.initial_account.balance
    }

    #[must_use]
    pub fn balance_return_rate(&self) -> f64 {
        rate_or_nan(self.balance_change(), self.initial_account.balance)
    }

    #[must_use]
    pub fn initial_equity(&self) -> f64 {
        account_equity(&self.initial_account)
    }

    #[must_use]
    pub fn final_equity(&self) -> f64 {
        account_equity(&self.final_account)
    }

    #[must_use]
    pub fn equity_change(&self) -> f64 {
        self.final_equity() - self.initial_equity()
    }

    #[must_use]
    pub fn equity_return_rate(&self) -> f64 {
        rate_or_nan(self.equity_change(), self.initial_equity())
    }

    #[must_use]
    pub fn realized_profit(&self) -> f64 {
        self.final_account.close_profit - self.initial_account.close_profit
    }

    #[must_use]
    pub fn total_commission(&self) -> f64 {
        self.final_account.commission - self.initial_account.commission
    }

    #[must_use]
    pub fn net_realized_profit(&self) -> f64 {
        self.realized_profit() - self.total_commission()
    }

    #[must_use]
    pub fn closed_trade_count(&self) -> usize {
        self.closed_profit_points
            .iter()
            .map(StrategyBacktestClosedProfitPoint::trade_count)
            .sum()
    }

    #[must_use]
    pub fn closed_profit_observation_count(&self) -> usize {
        self.closed_profit_points.len()
    }

    #[must_use]
    pub fn winning_closed_profit_observation_count(&self) -> usize {
        self.closed_profit_points
            .iter()
            .filter(|point| point.profit >= 0.0)
            .count()
    }

    #[must_use]
    pub fn losing_closed_profit_observation_count(&self) -> usize {
        self.closed_profit_points
            .iter()
            .filter(|point| point.profit < 0.0)
            .count()
    }

    #[must_use]
    pub fn winning_rate(&self) -> f64 {
        let wins = self.winning_closed_profit_observation_count();
        let losses = self.losing_closed_profit_observation_count();
        rate_or_nan(wins as f64, (wins + losses) as f64)
    }

    #[must_use]
    pub fn gross_profit(&self) -> f64 {
        self.closed_profit_points
            .iter()
            .filter_map(|point| (point.profit > 0.0).then_some(point.profit))
            .sum()
    }

    #[must_use]
    pub fn gross_loss(&self) -> f64 {
        self.closed_profit_points
            .iter()
            .filter_map(|point| (point.profit < 0.0).then_some(-point.profit))
            .sum()
    }

    #[must_use]
    pub fn profit_loss_ratio(&self) -> f64 {
        let gross_profit = self.gross_profit();
        let gross_loss = self.gross_loss();
        if gross_loss == 0.0 {
            if gross_profit > 0.0 {
                f64::INFINITY
            } else {
                f64::NAN
            }
        } else {
            gross_profit / gross_loss
        }
    }

    #[must_use]
    pub fn daily_balance_returns(&self) -> Vec<StrategyBacktestDailyBalanceReturn> {
        let mut daily_balance = BTreeMap::new();
        for point in &self.balance_points {
            let Some(event_time_ns) = point.event_time_ns else {
                continue;
            };
            let Some(date) = utc_date_from_timestamp_ns(event_time_ns) else {
                continue;
            };
            daily_balance.insert(date, point.balance);
        }

        let mut previous_balance = self.initial_account.balance;
        daily_balance
            .into_iter()
            .map(|(date, balance)| {
                let return_rate = rate_or_nan(balance - previous_balance, previous_balance);
                previous_balance = balance;
                StrategyBacktestDailyBalanceReturn {
                    date,
                    balance,
                    return_rate,
                }
            })
            .collect()
    }

    /// Derive cash-balance returns with caller-provided trading-day windows.
    ///
    /// Windows are evaluated in caller order. Invalid empty windows are skipped.
    /// A valid window with no observations carries the previous balance forward
    /// and therefore contributes a zero-return day.
    #[must_use]
    pub fn daily_balance_returns_for_windows(
        &self,
        windows: &[StrategyBacktestDailyReturnWindow],
    ) -> Vec<StrategyBacktestDailyBalanceReturn> {
        let mut previous_balance = self.initial_account.balance;
        windows
            .iter()
            .filter(|window| window.is_valid())
            .map(|window| {
                let balance = last_balance_in_window(&self.balance_points, window)
                    .unwrap_or(previous_balance);
                let return_rate = rate_or_nan(balance - previous_balance, previous_balance);
                previous_balance = balance;
                StrategyBacktestDailyBalanceReturn {
                    date: window.date,
                    balance,
                    return_rate,
                }
            })
            .collect()
    }

    #[must_use]
    pub fn balance_trading_day_count(&self) -> usize {
        self.daily_balance_returns().len()
    }

    #[must_use]
    pub fn profitable_balance_day_count(&self) -> usize {
        count_return_days(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            |return_rate| return_rate > 0.0,
        )
    }

    #[must_use]
    pub fn losing_balance_day_count(&self) -> usize {
        count_return_days(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            |return_rate| return_rate < 0.0,
        )
    }

    #[must_use]
    pub fn max_consecutive_profitable_balance_days(&self) -> usize {
        max_consecutive_return_days(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            |return_rate| return_rate > 0.0,
        )
    }

    #[must_use]
    pub fn max_consecutive_losing_balance_days(&self) -> usize {
        max_consecutive_return_days(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            |return_rate| return_rate < 0.0,
        )
    }

    #[must_use]
    pub fn daily_equity_returns(&self) -> Vec<StrategyBacktestDailyEquityReturn> {
        let mut daily_equity = BTreeMap::new();
        for point in &self.equity_points {
            let Some(event_time_ns) = point.event_time_ns else {
                continue;
            };
            let Some(date) = utc_date_from_timestamp_ns(event_time_ns) else {
                continue;
            };
            daily_equity.insert(date, point.equity);
        }

        let mut previous_equity = self.initial_equity();
        daily_equity
            .into_iter()
            .map(|(date, equity)| {
                let return_rate = rate_or_nan(equity - previous_equity, previous_equity);
                previous_equity = equity;
                StrategyBacktestDailyEquityReturn {
                    date,
                    equity,
                    return_rate,
                }
            })
            .collect()
    }

    /// Derive mark-to-market equity returns with caller-provided trading-day windows.
    ///
    /// Windows are evaluated in caller order. Invalid empty windows are skipped.
    /// A valid window with no observations carries the previous equity forward
    /// and therefore contributes a zero-return day.
    #[must_use]
    pub fn daily_equity_returns_for_windows(
        &self,
        windows: &[StrategyBacktestDailyReturnWindow],
    ) -> Vec<StrategyBacktestDailyEquityReturn> {
        let mut previous_equity = self.initial_equity();
        windows
            .iter()
            .filter(|window| window.is_valid())
            .map(|window| {
                let equity =
                    last_equity_in_window(&self.equity_points, window).unwrap_or(previous_equity);
                let return_rate = rate_or_nan(equity - previous_equity, previous_equity);
                previous_equity = equity;
                StrategyBacktestDailyEquityReturn {
                    date: window.date,
                    equity,
                    return_rate,
                }
            })
            .collect()
    }

    #[must_use]
    pub fn equity_trading_day_count(&self) -> usize {
        self.daily_equity_returns().len()
    }

    #[must_use]
    pub fn profitable_equity_day_count(&self) -> usize {
        count_return_days(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            |return_rate| return_rate > 0.0,
        )
    }

    #[must_use]
    pub fn losing_equity_day_count(&self) -> usize {
        count_return_days(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            |return_rate| return_rate < 0.0,
        )
    }

    #[must_use]
    pub fn max_consecutive_profitable_equity_days(&self) -> usize {
        max_consecutive_return_days(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            |return_rate| return_rate > 0.0,
        )
    }

    #[must_use]
    pub fn max_consecutive_losing_equity_days(&self) -> usize {
        max_consecutive_return_days(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            |return_rate| return_rate < 0.0,
        )
    }

    #[must_use]
    pub fn annualized_balance_return_rate(&self) -> f64 {
        annualized_return_rate(
            self.balance_return_rate(),
            self.daily_balance_returns().len(),
        )
    }

    #[must_use]
    pub fn annualized_equity_return_rate(&self) -> f64 {
        annualized_return_rate(self.equity_return_rate(), self.daily_equity_returns().len())
    }

    #[must_use]
    pub fn annualized_daily_balance_sharpe_ratio(&self) -> f64 {
        self.annualized_daily_balance_sharpe_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_balance_sharpe_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_sharpe_ratio(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn annualized_daily_equity_sharpe_ratio(&self) -> f64 {
        self.annualized_daily_equity_sharpe_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_equity_sharpe_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_sharpe_ratio(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn annualized_daily_sharpe_ratio(&self) -> f64 {
        self.annualized_daily_equity_sharpe_ratio()
    }

    #[must_use]
    pub fn annualized_daily_sharpe_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        self.annualized_daily_equity_sharpe_ratio_with_risk_free_rate(annual_risk_free_rate)
    }

    #[must_use]
    pub fn annualized_daily_balance_sortino_ratio(&self) -> f64 {
        self.annualized_daily_balance_sortino_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_balance_sortino_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_sortino_ratio(
            self.daily_balance_returns()
                .iter()
                .map(StrategyBacktestDailyBalanceReturn::return_rate),
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn annualized_daily_equity_sortino_ratio(&self) -> f64 {
        self.annualized_daily_equity_sortino_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_equity_sortino_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_sortino_ratio(
            self.daily_equity_returns()
                .iter()
                .map(StrategyBacktestDailyEquityReturn::return_rate),
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn annualized_daily_balance_calmar_ratio(&self) -> f64 {
        self.annualized_daily_balance_calmar_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_balance_calmar_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_calmar_ratio(
            self.annualized_balance_return_rate(),
            self.max_balance_drawdown_rate,
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn annualized_daily_equity_calmar_ratio(&self) -> f64 {
        self.annualized_daily_equity_calmar_ratio_with_risk_free_rate(0.0)
    }

    #[must_use]
    pub fn annualized_daily_equity_calmar_ratio_with_risk_free_rate(
        &self,
        annual_risk_free_rate: f64,
    ) -> f64 {
        annualized_calmar_ratio(
            self.annualized_equity_return_rate(),
            self.max_equity_drawdown_rate,
            annual_risk_free_rate,
        )
    }

    #[must_use]
    pub fn peak_balance(&self) -> f64 {
        self.peak_balance
    }

    #[must_use]
    pub fn max_balance_drawdown(&self) -> f64 {
        self.max_balance_drawdown
    }

    #[must_use]
    pub fn max_balance_drawdown_rate(&self) -> f64 {
        self.max_balance_drawdown_rate
    }

    #[must_use]
    pub fn peak_equity(&self) -> f64 {
        self.peak_equity
    }

    #[must_use]
    pub fn max_equity_drawdown(&self) -> f64 {
        self.max_equity_drawdown
    }

    #[must_use]
    pub fn max_equity_drawdown_rate(&self) -> f64 {
        self.max_equity_drawdown_rate
    }
}

impl StrategyBacktestBalancePoint {
    fn new(
        event_count: usize,
        event_time_ns: Option<i64>,
        balance: f64,
        initial_balance: f64,
        peak_balance: f64,
    ) -> Self {
        let drawdown = if balance.is_finite() && peak_balance.is_finite() {
            (peak_balance - balance).max(0.0)
        } else {
            f64::NAN
        };
        Self {
            event_count,
            event_time_ns,
            balance,
            return_rate: rate_or_nan(balance - initial_balance, initial_balance),
            drawdown,
            drawdown_rate: rate_or_nan(drawdown, peak_balance),
        }
    }

    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    #[must_use]
    pub fn event_time_ns(&self) -> Option<i64> {
        self.event_time_ns
    }

    #[must_use]
    pub fn balance(&self) -> f64 {
        self.balance
    }

    #[must_use]
    pub fn return_rate(&self) -> f64 {
        self.return_rate
    }

    #[must_use]
    pub fn drawdown(&self) -> f64 {
        self.drawdown
    }

    #[must_use]
    pub fn drawdown_rate(&self) -> f64 {
        self.drawdown_rate
    }
}

impl StrategyBacktestEquityPoint {
    fn new(
        event_count: usize,
        event_time_ns: Option<i64>,
        equity: f64,
        initial_equity: f64,
        peak_equity: f64,
    ) -> Self {
        let drawdown = if equity.is_finite() && peak_equity.is_finite() {
            (peak_equity - equity).max(0.0)
        } else {
            f64::NAN
        };
        Self {
            event_count,
            event_time_ns,
            equity,
            return_rate: rate_or_nan(equity - initial_equity, initial_equity),
            drawdown,
            drawdown_rate: rate_or_nan(drawdown, peak_equity),
        }
    }

    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    #[must_use]
    pub fn event_time_ns(&self) -> Option<i64> {
        self.event_time_ns
    }

    #[must_use]
    pub fn equity(&self) -> f64 {
        self.equity
    }

    #[must_use]
    pub fn return_rate(&self) -> f64 {
        self.return_rate
    }

    #[must_use]
    pub fn drawdown(&self) -> f64 {
        self.drawdown
    }

    #[must_use]
    pub fn drawdown_rate(&self) -> f64 {
        self.drawdown_rate
    }
}

impl StrategyBacktestClosedProfitPoint {
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    #[must_use]
    pub fn event_time_ns(&self) -> Option<i64> {
        self.event_time_ns
    }

    #[must_use]
    pub fn trade_count(&self) -> usize {
        self.trade_count
    }

    #[must_use]
    pub fn profit(&self) -> f64 {
        self.profit
    }
}

impl StrategyBacktestDailyEquityReturn {
    #[must_use]
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    #[must_use]
    pub fn equity(&self) -> f64 {
        self.equity
    }

    #[must_use]
    pub fn return_rate(&self) -> f64 {
        self.return_rate
    }
}

impl StrategyBacktestDailyBalanceReturn {
    #[must_use]
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    #[must_use]
    pub fn balance(&self) -> f64 {
        self.balance
    }

    #[must_use]
    pub fn return_rate(&self) -> f64 {
        self.return_rate
    }
}

impl StrategyBacktestDailyReturnWindow {
    #[must_use]
    pub fn new(date: NaiveDate, start_time_ns: i64, end_time_ns: i64) -> Self {
        Self {
            date,
            start_time_ns,
            end_time_ns,
        }
    }

    #[must_use]
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    #[must_use]
    pub fn start_time_ns(&self) -> i64 {
        self.start_time_ns
    }

    #[must_use]
    pub fn end_time_ns(&self) -> i64 {
        self.end_time_ns
    }

    fn is_valid(&self) -> bool {
        self.start_time_ns < self.end_time_ns
    }

    fn contains(&self, event_time_ns: i64) -> bool {
        self.start_time_ns <= event_time_ns && event_time_ns < self.end_time_ns
    }
}

impl StrategyBacktestEvent {
    fn from_replay_event(event: &ReplayMarketEvent) -> Self {
        event.step_meta().into()
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn received_at_ns(&self) -> i64 {
        self.received_at_ns
    }

    #[must_use]
    pub fn event_time_ns(&self) -> i64 {
        self.event_time_ns
    }

    #[must_use]
    pub fn underlying_symbol(&self) -> Option<&str> {
        self.underlying_symbol.as_deref()
    }
}

impl From<ReplayStepMeta> for StrategyBacktestEvent {
    fn from(meta: ReplayStepMeta) -> Self {
        Self {
            source: meta.source,
            symbol: meta.symbol,
            received_at_ns: meta.received_at_ns,
            event_time_ns: meta.event_time_ns,
            underlying_symbol: meta.underlying_symbol,
        }
    }
}

impl StrategyBacktestContext<'_> {
    #[must_use]
    pub fn event(&self) -> &StrategyBacktestEvent {
        &self.event
    }

    pub fn quote(&self, symbol: impl AsRef<str>) -> Result<Quote> {
        self.context.quote(symbol)
    }

    pub fn account(&self, account_id: impl AsRef<str>) -> Result<tqsdk_core::Account> {
        self.context.account(account_id)
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<tqsdk_core::Position> {
        self.context.position(account_id, symbol)
    }

    #[must_use]
    pub fn orders(&mut self, account_id: impl AsRef<str>) -> crate::TaskOrderBuilder<'_> {
        self.context.orders(account_id)
    }

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> crate::TargetPosBuilder {
        self.context.target_pos(account_id, symbol)
    }

    #[must_use]
    pub fn task_host(&self) -> &TaskHost {
        self.context.task_host()
    }

    #[must_use]
    pub fn sim(&self) -> &TqSim {
        self.sim
    }

    pub fn finish_sim_step(&mut self) -> Result<TqSimStepReport> {
        let report = self.sim.process_host_orders(self.context.task_host())?;
        self.summary.record_account_snapshot(
            self.sim,
            self.tracked_symbols,
            Some(self.event.event_time_ns()),
        );
        Ok(report)
    }
}

async fn drain_initial_commits(strategy: &mut StrategyHost) -> Result<()> {
    let deadline = Some(tokio::time::Instant::now());
    while strategy.task_host_mut().wait_update(deadline).await? {}
    Ok(())
}

fn ingest_quote_event(host: &TaskHost, symbol: &str, quote: &Quote) -> Result<()> {
    host.api().session().handle().ingest(
        RuntimeInput::Io(IoEvent {
            route: "backtest".to_string(),
            domains: vec![ProtocolDomain::Market],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [quote_update(symbol, quote)]
            })),
        }),
        vec![],
        CommitScope::ReplayStep,
    )?;
    Ok(())
}

fn quote_update(symbol: &str, quote: &Quote) -> Value {
    let mut quote_value = Map::new();
    insert_string_if_present(&mut quote_value, "datetime", &quote.datetime);
    insert_f64_if_finite(&mut quote_value, "last_price", quote.last_price);
    insert_f64_if_finite(&mut quote_value, "highest", quote.highest);
    insert_f64_if_finite(&mut quote_value, "lowest", quote.lowest);
    insert_f64_if_finite(&mut quote_value, "open", quote.open);
    insert_f64_if_finite(&mut quote_value, "close", quote.close);
    insert_f64_if_finite(&mut quote_value, "average", quote.average);
    insert_f64_if_finite(&mut quote_value, "ask_price1", quote.ask_price1);
    insert_i64_if_nonzero(&mut quote_value, "ask_volume1", quote.ask_volume1);
    insert_f64_if_finite(&mut quote_value, "bid_price1", quote.bid_price1);
    insert_i64_if_nonzero(&mut quote_value, "bid_volume1", quote.bid_volume1);
    insert_i64_if_nonzero(&mut quote_value, "volume", quote.volume);
    insert_f64_if_finite(&mut quote_value, "amount", quote.amount);
    insert_i64_if_nonzero(&mut quote_value, "open_interest", quote.open_interest);
    insert_f64_if_finite(&mut quote_value, "price_tick", quote.price_tick);
    insert_i64_if_nonzero(&mut quote_value, "price_decs", quote.price_decs);
    insert_i64_if_nonzero(&mut quote_value, "volume_multiple", quote.volume_multiple);
    insert_i64_if_nonzero(&mut quote_value, "open_limit", quote.open_limit);
    insert_i64_if_nonzero(
        &mut quote_value,
        "max_limit_order_volume",
        quote.max_limit_order_volume,
    );
    insert_i64_if_nonzero(
        &mut quote_value,
        "max_market_order_volume",
        quote.max_market_order_volume,
    );
    insert_i64_if_nonzero(
        &mut quote_value,
        "min_limit_order_volume",
        quote.min_limit_order_volume,
    );
    insert_i64_if_nonzero(
        &mut quote_value,
        "min_market_order_volume",
        quote.min_market_order_volume,
    );
    insert_string_if_present(
        &mut quote_value,
        "underlying_symbol",
        &quote.underlying_symbol,
    );
    insert_f64_if_finite(&mut quote_value, "strike_price", quote.strike_price);
    insert_string_if_present(&mut quote_value, "ins_class", &quote.ins_class);
    insert_string_if_present(&mut quote_value, "instrument_id", &quote.instrument_id);
    insert_string_if_present(&mut quote_value, "instrument_name", &quote.instrument_name);
    insert_string_if_present(&mut quote_value, "exchange_id", &quote.exchange_id);
    insert_string_if_present(&mut quote_value, "product_id", &quote.product_id);
    insert_f64_if_finite(&mut quote_value, "margin", quote.margin);
    insert_f64_if_finite(&mut quote_value, "commission", quote.commission);

    json!({
        "quotes": {
            symbol: Value::Object(quote_value)
        }
    })
}

fn quote_from_tick(tick: &Tick) -> Quote {
    Quote {
        datetime: tick.datetime.to_string(),
        last_price: tick.last_price,
        average: tick.average,
        highest: tick.highest,
        lowest: tick.lowest,
        ask_price1: tick.ask_price1,
        ask_volume1: tick.ask_volume1,
        bid_price1: tick.bid_price1,
        bid_volume1: tick.bid_volume1,
        ask_price2: tick.ask_price2,
        ask_volume2: tick.ask_volume2,
        bid_price2: tick.bid_price2,
        bid_volume2: tick.bid_volume2,
        ask_price3: tick.ask_price3,
        ask_volume3: tick.ask_volume3,
        bid_price3: tick.bid_price3,
        bid_volume3: tick.bid_volume3,
        ask_price4: tick.ask_price4,
        ask_volume4: tick.ask_volume4,
        bid_price4: tick.bid_price4,
        bid_volume4: tick.bid_volume4,
        ask_price5: tick.ask_price5,
        ask_volume5: tick.ask_volume5,
        bid_price5: tick.bid_price5,
        bid_volume5: tick.bid_volume5,
        volume: tick.volume,
        amount: tick.amount,
        open_interest: tick.open_interest,
        ..Quote::default()
    }
}

fn quote_with_replay_underlying(mut quote: Quote, underlying_symbol: Option<&str>) -> Quote {
    if quote.underlying_symbol.is_empty() {
        if let Some(underlying_symbol) = underlying_symbol {
            quote.underlying_symbol = underlying_symbol.to_owned();
        }
    }
    quote
}

fn kline_quote_checkpoints(row: &Kline, price_tick: f64) -> [Quote; 4] {
    [
        quote_from_kline_checkpoint(row, row.open, price_tick),
        quote_from_kline_checkpoint(row, row.high, price_tick),
        quote_from_kline_checkpoint(row, row.low, price_tick),
        quote_from_kline_checkpoint(row, row.close, price_tick),
    ]
}

fn quote_from_kline_checkpoint(row: &Kline, price: f64, price_tick: f64) -> Quote {
    Quote {
        datetime: row.datetime.to_string(),
        last_price: price,
        highest: row.high,
        lowest: row.low,
        open: row.open,
        close: row.close,
        ask_price1: price + price_tick,
        ask_volume1: i64::MAX,
        bid_price1: price - price_tick,
        bid_volume1: i64::MAX,
        volume: row.volume,
        open_interest: row.close_oi,
        price_tick,
        ..Quote::default()
    }
}

fn validate_price_ticks(price_ticks: &HashMap<String, f64>) -> Result<()> {
    for (symbol, price_tick) in price_ticks {
        if !price_tick.is_finite() || *price_tick <= 0.0 {
            return Err(TaskError::InvalidState(if symbol.is_empty() {
                "StrategyBacktest price_tick must be finite and positive"
            } else {
                "StrategyBacktest price_tick(symbol, tick) must be finite and positive"
            }));
        }
    }
    Ok(())
}

fn validate_default_price_tick(price_tick: Option<f64>) -> Result<()> {
    if let Some(price_tick) = price_tick
        && (!price_tick.is_finite() || price_tick <= 0.0)
    {
        return Err(TaskError::InvalidState(
            "StrategyBacktest default_price_tick must be finite and positive",
        ));
    }
    Ok(())
}

fn rate_or_nan(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 || !denominator.is_finite() {
        f64::NAN
    } else {
        numerator / denominator
    }
}

fn count_return_days(
    returns: impl IntoIterator<Item = f64>,
    predicate: impl Fn(f64) -> bool,
) -> usize {
    returns
        .into_iter()
        .filter(|return_rate| return_rate.is_finite() && predicate(*return_rate))
        .count()
}

fn max_consecutive_return_days(
    returns: impl IntoIterator<Item = f64>,
    predicate: impl Fn(f64) -> bool,
) -> usize {
    let mut current = 0;
    let mut max_seen = 0;
    for return_rate in returns {
        if return_rate.is_finite() && predicate(return_rate) {
            current += 1;
            max_seen = max_seen.max(current);
        } else {
            current = 0;
        }
    }
    max_seen
}

fn last_balance_in_window(
    points: &[StrategyBacktestBalancePoint],
    window: &StrategyBacktestDailyReturnWindow,
) -> Option<f64> {
    points
        .iter()
        .filter_map(|point| {
            point
                .event_time_ns
                .filter(|event_time_ns| window.contains(*event_time_ns))
                .map(|event_time_ns| (event_time_ns, point.balance))
        })
        .max_by_key(|(event_time_ns, _)| *event_time_ns)
        .map(|(_, balance)| balance)
}

fn last_equity_in_window(
    points: &[StrategyBacktestEquityPoint],
    window: &StrategyBacktestDailyReturnWindow,
) -> Option<f64> {
    points
        .iter()
        .filter_map(|point| {
            point
                .event_time_ns
                .filter(|event_time_ns| window.contains(*event_time_ns))
                .map(|event_time_ns| (event_time_ns, point.equity))
        })
        .max_by_key(|(event_time_ns, _)| *event_time_ns)
        .map(|(_, equity)| equity)
}

fn account_equity(account: &Account) -> f64 {
    account.balance + account.float_profit
}

fn utc_date_from_timestamp_ns(timestamp_ns: i64) -> Option<NaiveDate> {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let nanos = timestamp_ns.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos).map(|datetime| datetime.date_naive())
}

fn same_observation_date(left: Option<i64>, right: Option<i64>) -> bool {
    match (
        left.and_then(utc_date_from_timestamp_ns),
        right.and_then(utc_date_from_timestamp_ns),
    ) {
        (Some(left), Some(right)) => left == right,
        (None, None) => true,
        _ => false,
    }
}

fn same_observation_bucket(previous: Option<i64>, current: Option<i64>) -> bool {
    current.is_none() || same_observation_date(previous, current)
}

fn annualized_return_rate(total_return_rate: f64, period_count: usize) -> f64 {
    if period_count == 0 || !total_return_rate.is_finite() {
        return f64::NAN;
    }
    let growth = 1.0 + total_return_rate;
    if growth < 0.0 {
        f64::NAN
    } else {
        growth.powf(DEFAULT_TRADING_DAYS_PER_YEAR / period_count as f64) - 1.0
    }
}

fn daily_risk_free_rate(annual_risk_free_rate: f64) -> f64 {
    if !annual_risk_free_rate.is_finite() || annual_risk_free_rate <= -1.0 {
        f64::NAN
    } else {
        (1.0 + annual_risk_free_rate).powf(1.0 / DEFAULT_TRADING_DAYS_PER_YEAR) - 1.0
    }
}

fn annualized_sharpe_ratio(
    returns: impl IntoIterator<Item = f64>,
    annual_risk_free_rate: f64,
) -> f64 {
    let returns = returns
        .into_iter()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < 2 {
        return f64::NAN;
    }
    let daily_rf = daily_risk_free_rate(annual_risk_free_rate);
    if !daily_rf.is_finite() {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev == 0.0 || !std_dev.is_finite() {
        f64::NAN
    } else {
        (mean - daily_rf) / std_dev * DEFAULT_TRADING_DAYS_PER_YEAR.sqrt()
    }
}

fn annualized_sortino_ratio(
    returns: impl IntoIterator<Item = f64>,
    annual_risk_free_rate: f64,
) -> f64 {
    let returns = returns
        .into_iter()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if returns.len() < 2 {
        return f64::NAN;
    }
    let daily_rf = daily_risk_free_rate(annual_risk_free_rate);
    if !daily_rf.is_finite() {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside_variance = returns
        .iter()
        .filter(|value| **value < daily_rf)
        .map(|value| {
            let diff = value - daily_rf;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    let downside_dev = downside_variance.sqrt();
    if downside_dev == 0.0 || !downside_dev.is_finite() {
        f64::NAN
    } else {
        (mean - daily_rf) / downside_dev * DEFAULT_TRADING_DAYS_PER_YEAR.sqrt()
    }
}

fn annualized_calmar_ratio(
    annualized_return_rate: f64,
    max_drawdown_rate: f64,
    annual_risk_free_rate: f64,
) -> f64 {
    if !annualized_return_rate.is_finite()
        || !max_drawdown_rate.is_finite()
        || !annual_risk_free_rate.is_finite()
        || annual_risk_free_rate <= -1.0
        || max_drawdown_rate <= 0.0
    {
        f64::NAN
    } else {
        (annualized_return_rate - annual_risk_free_rate) / max_drawdown_rate
    }
}

fn is_close_trade(trade: &Trade) -> bool {
    matches!(
        trade.offset,
        Some(TradeOffset::Close | TradeOffset::CloseToday)
    )
}

fn insert_string_if_present(value: &mut Map<String, Value>, key: &str, field: &str) {
    if !field.is_empty() {
        value.insert(key.to_string(), Value::from(field));
    }
}

fn insert_f64_if_finite(value: &mut Map<String, Value>, key: &str, field: f64) {
    if let Some(number) = Number::from_f64(field) {
        value.insert(key.to_string(), Value::Number(number));
    }
}

fn insert_i64_if_nonzero(value: &mut Map<String, Value>, key: &str, field: i64) {
    if field != 0 {
        value.insert(key.to_string(), Value::from(field));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kline_quote_checkpoints_include_open_high_low_close_order() {
        let row = Kline {
            datetime: 1_000,
            open: 101.0,
            high: 105.0,
            low: 97.0,
            close: 99.0,
            volume: 100,
            open_oi: 40,
            close_oi: 50,
            ..Kline::default()
        };

        let checkpoints = kline_quote_checkpoints(&row, 0.5);

        assert_eq!(checkpoints[0].last_price, 101.0);
        assert_eq!(checkpoints[0].ask_price1, 101.5);
        assert_eq!(checkpoints[0].bid_price1, 100.5);
        assert_eq!(checkpoints[1].last_price, 105.0);
        assert_eq!(checkpoints[2].last_price, 97.0);
        assert_eq!(checkpoints[3].last_price, 99.0);
        assert_eq!(checkpoints[3].ask_price1, 99.5);
        assert_eq!(checkpoints[3].bid_price1, 98.5);
    }
}
