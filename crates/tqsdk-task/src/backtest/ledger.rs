#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use tqsdk_core::{Account, Order, Position, Trade, TradeDirection, TradeOffset};

use crate::replay::ReplayMarketPayloadKind;

const DEFAULT_TRADING_DAYS_PER_YEAR: f64 = 250.0;

/// Lightweight local backtest summary.
#[derive(Debug, Clone)]
pub struct StrategyBacktestSummary {
    event_count: usize,
    quote_count: usize,
    tick_count: usize,
    kline_count: usize,
    start_event_time_ns: Option<i64>,
    end_event_time_ns: Option<i64>,
    balance_points: Vec<StrategyBacktestBalancePoint>,
    equity_points: Vec<StrategyBacktestEquityPoint>,
    risk_ratio_points: Vec<StrategyBacktestRiskRatioPoint>,
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

/// Account risk-ratio observation recorded by local backtest summary.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestRiskRatioPoint {
    event_count: usize,
    event_time_ns: Option<i64>,
    risk_ratio: f64,
}

/// End-of-day mark-to-market equity return derived from replay observations.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestDailyEquityReturn {
    date: NaiveDate,
    equity: f64,
    profit: f64,
    drawdown: f64,
    drawdown_rate: f64,
    return_rate: f64,
}

/// End-of-day cash-balance return derived from replay observations.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestDailyBalanceReturn {
    date: NaiveDate,
    balance: f64,
    profit: f64,
    drawdown: f64,
    drawdown_rate: f64,
    return_rate: f64,
}

/// Rolling annualized ratio point derived from daily backtest returns.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestRollingRatioPoint {
    date: NaiveDate,
    sample_count: usize,
    ratio: f64,
}

/// Aggregated balance-based performance metrics for local backtest summaries.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestPerformanceMetrics {
    start_date_utc: Option<NaiveDate>,
    end_date_utc: Option<NaiveDate>,
    start_balance: f64,
    end_balance: f64,
    balance_change: f64,
    balance_return_rate: f64,
    annualized_balance_return_rate: f64,
    balance_trading_day_count: usize,
    profitable_balance_day_count: usize,
    losing_balance_day_count: usize,
    max_consecutive_profitable_balance_days: usize,
    max_consecutive_losing_balance_days: usize,
    max_balance_drawdown: f64,
    max_balance_drawdown_rate: f64,
    total_commission: f64,
    open_trade_count: usize,
    close_trade_count: usize,
    average_risk_ratio: f64,
    realized_profit: f64,
    net_realized_profit: f64,
    winning_rate: f64,
    profit_loss_ratio: f64,
    annualized_daily_balance_sharpe_ratio: f64,
    annualized_daily_balance_sortino_ratio: f64,
    annualized_daily_balance_calmar_ratio: f64,
}

/// Typed performance report snapshot for local backtest summaries.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestPerformanceReport {
    metrics: StrategyBacktestPerformanceMetrics,
    daily_balance_returns: Vec<StrategyBacktestDailyBalanceReturn>,
    daily_equity_returns: Vec<StrategyBacktestDailyEquityReturn>,
    rolling_balance_sharpe_ratios: Vec<StrategyBacktestRollingRatioPoint>,
    rolling_balance_sortino_ratios: Vec<StrategyBacktestRollingRatioPoint>,
    rolling_balance_calmar_ratios: Vec<StrategyBacktestRollingRatioPoint>,
    rolling_equity_sharpe_ratios: Vec<StrategyBacktestRollingRatioPoint>,
    rolling_equity_sortino_ratios: Vec<StrategyBacktestRollingRatioPoint>,
    rolling_equity_calmar_ratios: Vec<StrategyBacktestRollingRatioPoint>,
}

/// Explicit daily return window for exchange/trading-day grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyBacktestDailyReturnWindow {
    date: NaiveDate,
    start_time_ns: i64,
    end_time_ns: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct BacktestLedgerSnapshot {
    event_time_ns: Option<i64>,
    account: Account,
    orders: Vec<Order>,
    trades: Vec<Trade>,
    positions: Vec<Position>,
}

impl BacktestLedgerSnapshot {
    pub(crate) fn new(
        event_time_ns: Option<i64>,
        account: Account,
        orders: Vec<Order>,
        trades: Vec<Trade>,
        positions: Vec<Position>,
    ) -> Self {
        Self {
            event_time_ns,
            account,
            orders,
            trades,
            positions,
        }
    }
}

impl StrategyBacktestSummary {
    pub(crate) fn from_snapshot(snapshot: BacktestLedgerSnapshot) -> Self {
        let initial_account = snapshot.account.clone();
        let initial_balance = initial_account.balance;
        let initial_equity = account_equity(&initial_account);
        let mut summary = Self {
            event_count: 0,
            quote_count: 0,
            tick_count: 0,
            kline_count: 0,
            start_event_time_ns: None,
            end_event_time_ns: None,
            balance_points: Vec::new(),
            equity_points: Vec::new(),
            risk_ratio_points: Vec::new(),
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
        summary.record_snapshot(snapshot);
        summary
    }

    pub(crate) fn record_payload(&mut self, kind: ReplayMarketPayloadKind) {
        self.event_count += 1;
        match kind {
            ReplayMarketPayloadKind::Quote => self.quote_count += 1,
            ReplayMarketPayloadKind::Kline => self.kline_count += 1,
            ReplayMarketPayloadKind::Tick => self.tick_count += 1,
        }
    }

    pub(crate) fn record_snapshot(&mut self, snapshot: BacktestLedgerSnapshot) {
        let event_time_ns = snapshot.event_time_ns;
        let previous_close_profit = self.final_account.close_profit;
        let previous_trade_count = self.trades.len();
        self.record_event_time(event_time_ns);
        self.orders = snapshot.orders;
        self.trades = snapshot.trades;
        self.final_account = snapshot.account;
        self.final_positions = snapshot.positions;
        self.record_closed_profit_point(previous_close_profit, previous_trade_count, event_time_ns);
        self.record_balance_point(event_time_ns);
        self.record_equity_point(event_time_ns);
        self.record_risk_ratio_point(event_time_ns);
    }

    fn record_event_time(&mut self, event_time_ns: Option<i64>) {
        let Some(event_time_ns) = event_time_ns else {
            return;
        };
        self.start_event_time_ns = Some(
            self.start_event_time_ns
                .map_or(event_time_ns, |start| start.min(event_time_ns)),
        );
        self.end_event_time_ns = Some(
            self.end_event_time_ns
                .map_or(event_time_ns, |end| end.max(event_time_ns)),
        );
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

    fn record_risk_ratio_point(&mut self, event_time_ns: Option<i64>) {
        let risk_ratio = self.final_account.risk_ratio;
        if self.risk_ratio_points.last().is_some_and(|point| {
            point.risk_ratio == risk_ratio
                && same_observation_bucket(point.event_time_ns, event_time_ns)
        }) {
            return;
        }
        self.risk_ratio_points.push(StrategyBacktestRiskRatioPoint {
            event_count: self.event_count,
            event_time_ns,
            risk_ratio,
        });
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
    pub fn start_event_time_ns(&self) -> Option<i64> {
        self.start_event_time_ns
    }

    #[must_use]
    pub fn end_event_time_ns(&self) -> Option<i64> {
        self.end_event_time_ns
    }

    #[must_use]
    pub fn start_event_date_utc(&self) -> Option<NaiveDate> {
        self.start_event_time_ns
            .and_then(utc_date_from_timestamp_ns)
    }

    #[must_use]
    pub fn end_event_date_utc(&self) -> Option<NaiveDate> {
        self.end_event_time_ns.and_then(utc_date_from_timestamp_ns)
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
    pub fn risk_ratio_points(&self) -> &[StrategyBacktestRiskRatioPoint] {
        &self.risk_ratio_points
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
    pub fn average_risk_ratio(&self) -> f64 {
        average_finite(self.risk_ratio_points.iter().map(|point| point.risk_ratio))
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
    pub fn performance_metrics(&self) -> StrategyBacktestPerformanceMetrics {
        StrategyBacktestPerformanceMetrics {
            start_date_utc: self.start_event_date_utc(),
            end_date_utc: self.end_event_date_utc(),
            start_balance: self.initial_account.balance,
            end_balance: self.final_account.balance,
            balance_change: self.balance_change(),
            balance_return_rate: self.balance_return_rate(),
            annualized_balance_return_rate: self.annualized_balance_return_rate(),
            balance_trading_day_count: self.balance_trading_day_count(),
            profitable_balance_day_count: self.profitable_balance_day_count(),
            losing_balance_day_count: self.losing_balance_day_count(),
            max_consecutive_profitable_balance_days: self.max_consecutive_profitable_balance_days(),
            max_consecutive_losing_balance_days: self.max_consecutive_losing_balance_days(),
            max_balance_drawdown: self.max_balance_drawdown,
            max_balance_drawdown_rate: self.max_balance_drawdown_rate,
            total_commission: self.total_commission(),
            open_trade_count: self.open_trade_count(),
            close_trade_count: self.close_trade_count(),
            average_risk_ratio: self.average_risk_ratio(),
            realized_profit: self.realized_profit(),
            net_realized_profit: self.net_realized_profit(),
            winning_rate: self.winning_rate(),
            profit_loss_ratio: self.profit_loss_ratio(),
            annualized_daily_balance_sharpe_ratio: self.annualized_daily_balance_sharpe_ratio(),
            annualized_daily_balance_sortino_ratio: self.annualized_daily_balance_sortino_ratio(),
            annualized_daily_balance_calmar_ratio: self.annualized_daily_balance_calmar_ratio(),
        }
    }

    #[must_use]
    pub fn performance_report(
        &self,
        rolling_window_len: usize,
    ) -> StrategyBacktestPerformanceReport {
        StrategyBacktestPerformanceReport {
            metrics: self.performance_metrics(),
            daily_balance_returns: self.daily_balance_returns(),
            daily_equity_returns: self.daily_equity_returns(),
            rolling_balance_sharpe_ratios: self
                .rolling_daily_balance_sharpe_ratios(rolling_window_len),
            rolling_balance_sortino_ratios: self
                .rolling_daily_balance_sortino_ratios(rolling_window_len),
            rolling_balance_calmar_ratios: self
                .rolling_daily_balance_calmar_ratios(rolling_window_len),
            rolling_equity_sharpe_ratios: self
                .rolling_daily_equity_sharpe_ratios(rolling_window_len),
            rolling_equity_sortino_ratios: self
                .rolling_daily_equity_sortino_ratios(rolling_window_len),
            rolling_equity_calmar_ratios: self
                .rolling_daily_equity_calmar_ratios(rolling_window_len),
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
        let mut peak_balance = self.initial_account.balance;
        daily_balance
            .into_iter()
            .map(|(date, balance)| {
                let profit = balance - previous_balance;
                let return_rate = rate_or_nan(profit, previous_balance);
                update_peak(&mut peak_balance, balance);
                let (drawdown, drawdown_rate) = drawdown_from_peak(balance, peak_balance);
                previous_balance = balance;
                StrategyBacktestDailyBalanceReturn {
                    date,
                    balance,
                    profit,
                    drawdown,
                    drawdown_rate,
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
        let mut peak_balance = self.initial_account.balance;
        windows
            .iter()
            .filter(|window| window.is_valid())
            .map(|window| {
                let balance = last_balance_in_window(&self.balance_points, window)
                    .unwrap_or(previous_balance);
                let profit = balance - previous_balance;
                let return_rate = rate_or_nan(profit, previous_balance);
                update_peak(&mut peak_balance, balance);
                let (drawdown, drawdown_rate) = drawdown_from_peak(balance, peak_balance);
                previous_balance = balance;
                StrategyBacktestDailyBalanceReturn {
                    date: window.date,
                    balance,
                    profit,
                    drawdown,
                    drawdown_rate,
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
        let mut peak_equity = self.initial_equity();
        daily_equity
            .into_iter()
            .map(|(date, equity)| {
                let profit = equity - previous_equity;
                let return_rate = rate_or_nan(profit, previous_equity);
                update_peak(&mut peak_equity, equity);
                let (drawdown, drawdown_rate) = drawdown_from_peak(equity, peak_equity);
                previous_equity = equity;
                StrategyBacktestDailyEquityReturn {
                    date,
                    equity,
                    profit,
                    drawdown,
                    drawdown_rate,
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
        let mut peak_equity = self.initial_equity();
        windows
            .iter()
            .filter(|window| window.is_valid())
            .map(|window| {
                let equity =
                    last_equity_in_window(&self.equity_points, window).unwrap_or(previous_equity);
                let profit = equity - previous_equity;
                let return_rate = rate_or_nan(profit, previous_equity);
                update_peak(&mut peak_equity, equity);
                let (drawdown, drawdown_rate) = drawdown_from_peak(equity, peak_equity);
                previous_equity = equity;
                StrategyBacktestDailyEquityReturn {
                    date: window.date,
                    equity,
                    profit,
                    drawdown,
                    drawdown_rate,
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
    pub fn rolling_daily_balance_sharpe_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_balance_returns();
        rolling_balance_ratio_points(&daily, window_len, |window| {
            annualized_sharpe_ratio(window.iter().map(|day| day.return_rate), 0.0)
        })
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
    pub fn rolling_daily_equity_sharpe_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_equity_returns();
        rolling_equity_ratio_points(&daily, window_len, |window| {
            annualized_sharpe_ratio(window.iter().map(|day| day.return_rate), 0.0)
        })
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
    pub fn rolling_daily_balance_sortino_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_balance_returns();
        rolling_balance_ratio_points(&daily, window_len, |window| {
            annualized_sortino_ratio(window.iter().map(|day| day.return_rate), 0.0)
        })
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
    pub fn rolling_daily_equity_sortino_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_equity_returns();
        rolling_equity_ratio_points(&daily, window_len, |window| {
            annualized_sortino_ratio(window.iter().map(|day| day.return_rate), 0.0)
        })
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
    pub fn rolling_daily_balance_calmar_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_balance_returns();
        rolling_balance_ratio_points(&daily, window_len, |window| {
            annualized_calmar_ratio(
                annualized_return_rate(
                    compounded_return_rate(window.iter().map(|day| day.return_rate)),
                    window.len(),
                ),
                max_rolling_drawdown_rate(window.iter().map(|day| day.drawdown_rate)),
                0.0,
            )
        })
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
    pub fn rolling_daily_equity_calmar_ratios(
        &self,
        window_len: usize,
    ) -> Vec<StrategyBacktestRollingRatioPoint> {
        let daily = self.daily_equity_returns();
        rolling_equity_ratio_points(&daily, window_len, |window| {
            annualized_calmar_ratio(
                annualized_return_rate(
                    compounded_return_rate(window.iter().map(|day| day.return_rate)),
                    window.len(),
                ),
                max_rolling_drawdown_rate(window.iter().map(|day| day.drawdown_rate)),
                0.0,
            )
        })
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

impl StrategyBacktestRiskRatioPoint {
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_count
    }

    #[must_use]
    pub fn event_time_ns(&self) -> Option<i64> {
        self.event_time_ns
    }

    #[must_use]
    pub fn risk_ratio(&self) -> f64 {
        self.risk_ratio
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
    pub fn profit(&self) -> f64 {
        self.profit
    }

    #[must_use]
    pub fn drawdown(&self) -> f64 {
        self.drawdown
    }

    #[must_use]
    pub fn drawdown_rate(&self) -> f64 {
        self.drawdown_rate
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
    pub fn profit(&self) -> f64 {
        self.profit
    }

    #[must_use]
    pub fn drawdown(&self) -> f64 {
        self.drawdown
    }

    #[must_use]
    pub fn drawdown_rate(&self) -> f64 {
        self.drawdown_rate
    }

    #[must_use]
    pub fn return_rate(&self) -> f64 {
        self.return_rate
    }
}

impl StrategyBacktestRollingRatioPoint {
    #[must_use]
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.sample_count
    }

    #[must_use]
    pub fn ratio(&self) -> f64 {
        self.ratio
    }
}

impl StrategyBacktestPerformanceMetrics {
    #[must_use]
    pub fn start_date_utc(&self) -> Option<NaiveDate> {
        self.start_date_utc
    }

    #[must_use]
    pub fn end_date_utc(&self) -> Option<NaiveDate> {
        self.end_date_utc
    }

    #[must_use]
    pub fn start_balance(&self) -> f64 {
        self.start_balance
    }

    #[must_use]
    pub fn end_balance(&self) -> f64 {
        self.end_balance
    }

    #[must_use]
    pub fn balance_change(&self) -> f64 {
        self.balance_change
    }

    #[must_use]
    pub fn balance_return_rate(&self) -> f64 {
        self.balance_return_rate
    }

    #[must_use]
    pub fn annualized_balance_return_rate(&self) -> f64 {
        self.annualized_balance_return_rate
    }

    #[must_use]
    pub fn balance_trading_day_count(&self) -> usize {
        self.balance_trading_day_count
    }

    #[must_use]
    pub fn profitable_balance_day_count(&self) -> usize {
        self.profitable_balance_day_count
    }

    #[must_use]
    pub fn losing_balance_day_count(&self) -> usize {
        self.losing_balance_day_count
    }

    #[must_use]
    pub fn max_consecutive_profitable_balance_days(&self) -> usize {
        self.max_consecutive_profitable_balance_days
    }

    #[must_use]
    pub fn max_consecutive_losing_balance_days(&self) -> usize {
        self.max_consecutive_losing_balance_days
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
    pub fn total_commission(&self) -> f64 {
        self.total_commission
    }

    #[must_use]
    pub fn open_trade_count(&self) -> usize {
        self.open_trade_count
    }

    #[must_use]
    pub fn close_trade_count(&self) -> usize {
        self.close_trade_count
    }

    #[must_use]
    pub fn average_risk_ratio(&self) -> f64 {
        self.average_risk_ratio
    }

    #[must_use]
    pub fn realized_profit(&self) -> f64 {
        self.realized_profit
    }

    #[must_use]
    pub fn net_realized_profit(&self) -> f64 {
        self.net_realized_profit
    }

    #[must_use]
    pub fn winning_rate(&self) -> f64 {
        self.winning_rate
    }

    #[must_use]
    pub fn profit_loss_ratio(&self) -> f64 {
        self.profit_loss_ratio
    }

    #[must_use]
    pub fn annualized_daily_balance_sharpe_ratio(&self) -> f64 {
        self.annualized_daily_balance_sharpe_ratio
    }

    #[must_use]
    pub fn annualized_daily_balance_sortino_ratio(&self) -> f64 {
        self.annualized_daily_balance_sortino_ratio
    }

    #[must_use]
    pub fn annualized_daily_balance_calmar_ratio(&self) -> f64 {
        self.annualized_daily_balance_calmar_ratio
    }
}

impl StrategyBacktestPerformanceReport {
    #[must_use]
    pub fn metrics(&self) -> &StrategyBacktestPerformanceMetrics {
        &self.metrics
    }

    #[must_use]
    pub fn daily_balance_returns(&self) -> &[StrategyBacktestDailyBalanceReturn] {
        &self.daily_balance_returns
    }

    #[must_use]
    pub fn daily_equity_returns(&self) -> &[StrategyBacktestDailyEquityReturn] {
        &self.daily_equity_returns
    }

    #[must_use]
    pub fn rolling_balance_sharpe_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_balance_sharpe_ratios
    }

    #[must_use]
    pub fn rolling_balance_sortino_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_balance_sortino_ratios
    }

    #[must_use]
    pub fn rolling_balance_calmar_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_balance_calmar_ratios
    }

    #[must_use]
    pub fn rolling_equity_sharpe_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_equity_sharpe_ratios
    }

    #[must_use]
    pub fn rolling_equity_sortino_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_equity_sortino_ratios
    }

    #[must_use]
    pub fn rolling_equity_calmar_ratios(&self) -> &[StrategyBacktestRollingRatioPoint] {
        &self.rolling_equity_calmar_ratios
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
fn rate_or_nan(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 || !denominator.is_finite() {
        f64::NAN
    } else {
        numerator / denominator
    }
}

fn update_peak(peak: &mut f64, value: f64) {
    if value.is_finite() && (!peak.is_finite() || value > *peak) {
        *peak = value;
    }
}

fn drawdown_from_peak(value: f64, peak: f64) -> (f64, f64) {
    let drawdown = if value.is_finite() && peak.is_finite() {
        (peak - value).max(0.0)
    } else {
        f64::NAN
    };
    (drawdown, rate_or_nan(drawdown, peak))
}

fn rolling_equity_ratio_points(
    daily: &[StrategyBacktestDailyEquityReturn],
    window_len: usize,
    ratio: impl Fn(&[StrategyBacktestDailyEquityReturn]) -> f64,
) -> Vec<StrategyBacktestRollingRatioPoint> {
    if window_len == 0 {
        return Vec::new();
    }
    daily
        .iter()
        .enumerate()
        .map(|(index, day)| {
            let sample_count = (index + 1).min(window_len);
            let ratio = if index + 1 < window_len {
                f64::NAN
            } else {
                ratio(&daily[index + 1 - window_len..=index])
            };
            StrategyBacktestRollingRatioPoint {
                date: day.date,
                sample_count,
                ratio,
            }
        })
        .collect()
}

fn rolling_balance_ratio_points(
    daily: &[StrategyBacktestDailyBalanceReturn],
    window_len: usize,
    ratio: impl Fn(&[StrategyBacktestDailyBalanceReturn]) -> f64,
) -> Vec<StrategyBacktestRollingRatioPoint> {
    if window_len == 0 {
        return Vec::new();
    }
    daily
        .iter()
        .enumerate()
        .map(|(index, day)| {
            let sample_count = (index + 1).min(window_len);
            let ratio = if index + 1 < window_len {
                f64::NAN
            } else {
                ratio(&daily[index + 1 - window_len..=index])
            };
            StrategyBacktestRollingRatioPoint {
                date: day.date,
                sample_count,
                ratio,
            }
        })
        .collect()
}

fn compounded_return_rate(returns: impl IntoIterator<Item = f64>) -> f64 {
    let mut growth = 1.0;
    for return_rate in returns {
        if !return_rate.is_finite() {
            return f64::NAN;
        }
        growth *= 1.0 + return_rate;
    }
    growth - 1.0
}

fn max_rolling_drawdown_rate(drawdown_rates: impl IntoIterator<Item = f64>) -> f64 {
    drawdown_rates
        .into_iter()
        .filter(|drawdown_rate| drawdown_rate.is_finite())
        .fold(0.0_f64, f64::max)
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

fn average_finite(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in values {
        if value.is_finite() {
            sum += value;
            count += 1;
        }
    }
    if count == 0 {
        f64::NAN
    } else {
        sum / count as f64
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn account(balance: f64, float_profit: f64, close_profit: f64, risk_ratio: f64) -> Account {
        Account {
            balance,
            float_profit,
            close_profit,
            risk_ratio,
            ..Account::default()
        }
    }

    fn snapshot(
        event_time_ns: Option<i64>,
        account: Account,
        orders: Vec<Order>,
        trades: Vec<Trade>,
        positions: Vec<Position>,
    ) -> BacktestLedgerSnapshot {
        BacktestLedgerSnapshot::new(event_time_ns, account, orders, trades, positions)
    }

    #[test]
    fn ledger_records_owned_snapshots_without_sim_dependency() {
        let mut summary = StrategyBacktestSummary::from_snapshot(snapshot(
            None,
            account(100.0, 0.0, 0.0, 0.1),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));

        summary.record_payload(ReplayMarketPayloadKind::Quote);
        summary.record_snapshot(snapshot(
            Some(1_000_000_000),
            account(110.0, 5.0, 0.0, 0.2),
            vec![Order {
                exchange_id: "SHFE".to_string(),
                instrument_id: "cu2401".to_string(),
                ..Order::default()
            }],
            Vec::new(),
            vec![Position {
                exchange_id: "SHFE".to_string(),
                instrument_id: "cu2401".to_string(),
                ..Position::default()
            }],
        ));

        assert_eq!(summary.event_count(), 1);
        assert_eq!(summary.quote_count(), 1);
        assert_eq!(summary.initial_account().balance, 100.0);
        assert_eq!(summary.final_account().balance, 110.0);
        assert_eq!(summary.final_equity(), 115.0);
        assert_eq!(summary.balance_points().len(), 2);
        assert_eq!(summary.balance_points()[1].balance(), 110.0);
        assert_eq!(summary.equity_points()[1].equity(), 115.0);
        assert_eq!(summary.risk_ratio_points()[1].risk_ratio(), 0.2);
        assert_eq!(summary.orders().len(), 1);
        assert_eq!(summary.final_positions().len(), 1);
    }

    #[test]
    fn ledger_records_closed_profit_observation_from_snapshot_delta() {
        let mut summary = StrategyBacktestSummary::from_snapshot(snapshot(
            None,
            account(100.0, 0.0, 0.0, 0.0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));

        summary.record_payload(ReplayMarketPayloadKind::Tick);
        summary.record_snapshot(snapshot(
            Some(2_000_000_000),
            account(125.0, 0.0, 25.0, 0.0),
            Vec::new(),
            vec![Trade {
                exchange_id: "SHFE".to_string(),
                instrument_id: "cu2401".to_string(),
                direction: Some(TradeDirection::Sell),
                offset: Some(TradeOffset::Close),
                ..Trade::default()
            }],
            Vec::new(),
        ));

        assert_eq!(summary.closed_profit_observation_count(), 1);
        assert_eq!(summary.closed_profit_points()[0].event_count(), 1);
        assert_eq!(summary.closed_profit_points()[0].trade_count(), 1);
        assert_eq!(summary.closed_profit_points()[0].profit(), 25.0);
        assert_eq!(summary.close_trade_count(), 1);
        assert_eq!(summary.closed_trade_count(), 1);
        assert_eq!(summary.gross_profit(), 25.0);
        assert_eq!(summary.gross_loss(), 0.0);
    }
}
