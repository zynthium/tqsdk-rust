#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    Account, CommitScope, InputPayload, IoEvent, Kline, Order, Position, ProtocolDomain, Quote,
    RuntimeInput, Tick, Trade,
};

use crate::replay::{
    ReplayMarketEvent, ReplayMarketPayload, ReplayMarketPayloadKind, ReplayMarketSource,
    ReplayStepMeta,
};
use crate::sim::{TqSim, TqSimStepReport};
use crate::strategy::StrategyHostBuilder;
use crate::testing::StrategyTestHarness;
use crate::{Result, StrategyContext, StrategyHost, TaskError, TaskHost};

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
}

/// Lightweight local backtest summary.
#[derive(Debug, Clone)]
pub struct StrategyBacktestSummary {
    event_count: usize,
    quote_count: usize,
    tick_count: usize,
    kline_count: usize,
    balance_points: Vec<StrategyBacktestBalancePoint>,
    orders: Vec<Order>,
    trades: Vec<Trade>,
    initial_account: Account,
    final_account: Account,
    final_positions: Vec<Position>,
    peak_balance: f64,
    max_balance_drawdown: f64,
    max_balance_drawdown_rate: f64,
}

/// Cash-balance point recorded by local backtest summary.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyBacktestBalancePoint {
    event_count: usize,
    balance: f64,
    return_rate: f64,
    drawdown: f64,
    drawdown_rate: f64,
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
                self.ingest_quote(event.symbol(), quote)?;
            }
            ReplayMarketPayload::Tick(tick) => {
                let quote = quote_from_tick(tick);
                self.ingest_quote(event.symbol(), &quote)?;
            }
            ReplayMarketPayload::Kline { row, .. } => {
                let price_tick = self.price_tick(event.symbol())?;
                let checkpoints = kline_quote_checkpoints(row, price_tick);
                for quote in checkpoints {
                    self.ingest_quote(event.symbol(), &quote)?;
                }
            }
        }
        self.summary.record_payload(payload_kind);
        self.summary
            .record_account_snapshot(&self.sim, &self.tracked_symbols);

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
        summary.record_account_snapshot(&self.sim, &self.tracked_symbols);
        summary
    }

    fn ingest_quote(&mut self, symbol: &str, quote: &Quote) -> Result<()> {
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
            .or(self.default_price_tick)
            .ok_or(TaskError::Unsupported(
                "StrategyBacktest kline event requires price_tick(symbol, tick) or default_price_tick(tick)",
            ))
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
            default_price_tick,
            summary,
        })
    }
}

impl StrategyBacktestSummary {
    fn from_sim(sim: &TqSim, symbols: &[String]) -> Self {
        let initial_account = sim.account();
        let initial_balance = initial_account.balance;
        let mut summary = Self {
            event_count: 0,
            quote_count: 0,
            tick_count: 0,
            kline_count: 0,
            balance_points: Vec::new(),
            orders: Vec::new(),
            trades: Vec::new(),
            initial_account: initial_account.clone(),
            final_account: initial_account,
            final_positions: Vec::new(),
            peak_balance: initial_balance,
            max_balance_drawdown: 0.0,
            max_balance_drawdown_rate: 0.0,
        };
        summary.record_account_snapshot(sim, symbols);
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

    fn record_account_snapshot(&mut self, sim: &TqSim, symbols: &[String]) {
        self.refresh_from_sim(sim, symbols);
        self.record_balance_point();
    }

    fn record_balance_point(&mut self) {
        let balance = self.final_account.balance;
        if self
            .balance_points
            .last()
            .is_some_and(|point| point.balance == balance)
        {
            return;
        }
        if balance.is_finite() && (!self.peak_balance.is_finite() || balance > self.peak_balance) {
            self.peak_balance = balance;
        }
        let point = StrategyBacktestBalancePoint::new(
            self.event_count,
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
    pub fn balance_points(&self) -> &[StrategyBacktestBalancePoint] {
        &self.balance_points
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
}

impl StrategyBacktestBalancePoint {
    fn new(event_count: usize, balance: f64, initial_balance: f64, peak_balance: f64) -> Self {
        let drawdown = if balance.is_finite() && peak_balance.is_finite() {
            (peak_balance - balance).max(0.0)
        } else {
            f64::NAN
        };
        Self {
            event_count,
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
}

impl From<ReplayStepMeta> for StrategyBacktestEvent {
    fn from(meta: ReplayStepMeta) -> Self {
        Self {
            source: meta.source,
            symbol: meta.symbol,
            received_at_ns: meta.received_at_ns,
            event_time_ns: meta.event_time_ns,
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
        self.summary
            .record_account_snapshot(self.sim, self.tracked_symbols);
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

fn kline_quote_checkpoints(row: &Kline, price_tick: f64) -> [Quote; 3] {
    [
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
