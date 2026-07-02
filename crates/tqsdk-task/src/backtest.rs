#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashMap};

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote, RuntimeInput, Tick,
};

use crate::backtest_stream::{BacktestMarketStream, ReplayMarketStream};
use crate::replay::{ReplayMarketEvent, ReplayMarketPayload, ReplayMarketSource, ReplayStepMeta};
use crate::sim::{TqSim, TqSimStepReport};
use crate::strategy::StrategyHostBuilder;
use crate::testing::StrategyTestHarness;
use crate::{Result, StrategyContext, StrategyHost, TaskError, TaskHost};

mod ledger;

pub use ledger::{
    StrategyBacktestBalancePoint, StrategyBacktestClosedProfitPoint,
    StrategyBacktestDailyBalanceReturn, StrategyBacktestDailyEquityReturn,
    StrategyBacktestDailyReturnWindow, StrategyBacktestEquityPoint,
    StrategyBacktestPerformanceMetrics, StrategyBacktestPerformanceReport,
    StrategyBacktestRiskRatioPoint, StrategyBacktestRollingRatioPoint, StrategyBacktestSummary,
};

use ledger::BacktestLedgerSnapshot;

/// Local Python-compatible strategy backtest over task-owned replay market events.
pub struct StrategyBacktestBuilder {
    replay: Box<dyn BacktestMarketStream>,
    replay_symbols: Vec<String>,
    sim: TqSim,
    quotes: Vec<String>,
    price_ticks: HashMap<String, f64>,
    default_price_tick: Option<f64>,
}

/// Local Python-compatible strategy backtest host.
pub struct StrategyBacktest {
    replay: Box<dyn BacktestMarketStream>,
    pending_event: Option<ReplayMarketEvent>,
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
        let replay_symbols = replay.symbols().map(str::to_string).collect();
        StrategyBacktestBuilder::new(Box::new(ReplayMarketStream::new(replay)))
            .with_replay_symbols(replay_symbols)
    }

    #[must_use]
    pub fn builder_from_stream(stream: Box<dyn BacktestMarketStream>) -> StrategyBacktestBuilder {
        StrategyBacktestBuilder::new(stream)
    }

    pub async fn next(&mut self) -> Result<Option<StrategyBacktestContext<'_>>> {
        let Some(event) = self.next_replay_event().await? else {
            self.drain_pending_task_updates().await?;
            return Ok(None);
        };
        let event_time_ns = event.event_time_ns();
        let backtest_event = StrategyBacktestEvent::from_replay_event(&event);
        self.ingest_replay_event(&event)?;

        loop {
            let Some(next_event) = self.replay.next_event().await? else {
                break;
            };
            if next_event.event_time_ns() != event_time_ns {
                self.pending_event = Some(next_event);
                break;
            }
            self.ingest_replay_event(&next_event)?;
        }

        self.summary.record_snapshot(ledger_snapshot_from_sim(
            &self.sim,
            &self.tracked_symbols,
            Some(event_time_ns),
        ));

        let context = self.strategy.next_once().await?;
        Ok(Some(StrategyBacktestContext {
            event: backtest_event,
            context,
            sim: &mut self.sim,
            summary: &mut self.summary,
            tracked_symbols: &self.tracked_symbols,
        }))
    }

    async fn next_replay_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        if self.pending_event.is_some() {
            return Ok(self.pending_event.take());
        }
        self.replay.next_event().await
    }

    fn ingest_replay_event(&mut self, event: &ReplayMarketEvent) -> Result<()> {
        let event_time_ns = event.event_time_ns();
        match event.payload() {
            ReplayMarketPayload::Quote(quote) => {
                let quote =
                    quote_with_replay_underlying((**quote).clone(), event.underlying_symbol());
                self.ingest_quote(event.symbol(), &quote, event_time_ns)?;
            }
            ReplayMarketPayload::Tick(tick) => {
                let quote =
                    quote_with_replay_underlying(quote_from_tick(tick), event.underlying_symbol());
                self.ingest_quote(event.symbol(), &quote, event_time_ns)?;
            }
            ReplayMarketPayload::Kline { row, .. } => {
                let price_tick = self.price_tick(event.symbol())?;
                let checkpoints = kline_quote_checkpoints(row, price_tick);
                for quote in checkpoints {
                    let quote = quote_with_replay_underlying(quote, event.underlying_symbol());
                    self.ingest_quote(event.symbol(), &quote, event_time_ns)?;
                }
            }
        }
        self.summary.record_payload(event.payload_kind());
        Ok(())
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
        summary.record_snapshot(ledger_snapshot_from_sim(
            &self.sim,
            &self.tracked_symbols,
            None,
        ));
        summary
    }

    fn ingest_quote(&mut self, symbol: &str, quote: &Quote, event_time_ns: i64) -> Result<()> {
        self.remember_quote_metadata(symbol, quote);
        ingest_quote_event(self.strategy.task_host(), symbol, quote)?;
        let report = self
            .sim
            .update_quote_at(symbol.to_owned(), quote.clone(), event_time_ns);
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

    async fn drain_pending_task_updates(&mut self) -> Result<()> {
        while self
            .strategy
            .task_host_mut()
            .wait_update(Some(tokio::time::Instant::now()))
            .await?
        {}
        Ok(())
    }
}

impl StrategyBacktestBuilder {
    #[must_use]
    pub fn new(replay: Box<dyn BacktestMarketStream>) -> Self {
        Self {
            replay,
            replay_symbols: Vec::new(),
            sim: TqSim::new(),
            quotes: Vec::new(),
            price_ticks: HashMap::new(),
            default_price_tick: None,
        }
    }

    #[must_use]
    fn with_replay_symbols(mut self, symbols: Vec<String>) -> Self {
        self.replay_symbols = symbols;
        self
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
            replay_symbols,
            sim,
            mut quotes,
            price_ticks,
            default_price_tick,
        } = self;
        validate_price_ticks(&price_ticks)?;
        validate_default_price_tick(default_price_tick)?;
        for symbol in replay_symbols {
            if !quotes.iter().any(|existing| existing == &symbol) {
                quotes.push(symbol);
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
        let summary = StrategyBacktestSummary::from_snapshot(ledger_snapshot_from_sim(
            &sim,
            &tracked_symbols,
            None,
        ));
        Ok(StrategyBacktest {
            replay,
            pending_event: None,
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
        self.summary.record_snapshot(ledger_snapshot_from_sim(
            self.sim,
            self.tracked_symbols,
            Some(self.event.event_time_ns()),
        ));
        Ok(report)
    }
}

async fn drain_initial_commits(strategy: &mut StrategyHost) -> Result<()> {
    let deadline = Some(tokio::time::Instant::now());
    while strategy.task_host_mut().wait_update(deadline).await? {}
    Ok(())
}

fn ledger_snapshot_from_sim(
    sim: &TqSim,
    symbols: &[String],
    event_time_ns: Option<i64>,
) -> BacktestLedgerSnapshot {
    let orders = sim.orders();
    let trades = sim.trades();
    let positions = orders
        .iter()
        .map(|order| format!("{}.{}", order.exchange_id, order.instrument_id))
        .chain(symbols.iter().cloned())
        .chain(
            trades
                .iter()
                .map(|trade| format!("{}.{}", trade.exchange_id, trade.instrument_id)),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|symbol| sim.position(symbol))
        .collect();

    BacktestLedgerSnapshot::new(event_time_ns, sim.account(), orders, trades, positions)
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

    #[tokio::test]
    async fn strategy_backtest_batches_same_timestamp_ticks_into_one_step() {
        let replay = ReplayMarketSource::new(vec![
            tick_event("SHFE.a", 1, 1_000, 101.0),
            tick_event("DCE.b", 2, 1_000, 201.0),
            tick_event("SHFE.a", 3, 2_000, 102.0),
        ]);
        let mut backtest = StrategyBacktest::builder(replay)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        assert_eq!(first.event().event_time_ns(), 1_000);
        assert_eq!(first.quote("SHFE.a").unwrap().last_price, 101.0);
        assert_eq!(first.quote("DCE.b").unwrap().last_price, 201.0);
        drop(first);

        let second = backtest
            .next()
            .await
            .expect("second step should succeed")
            .expect("second step should exist");
        assert_eq!(second.event().event_time_ns(), 2_000);
        assert_eq!(second.quote("SHFE.a").unwrap().last_price, 102.0);
        drop(second);

        assert!(backtest.next().await.unwrap().is_none());
    }

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

    fn tick_event(symbol: &str, id: i64, datetime: i64, last_price: f64) -> ReplayMarketEvent {
        ReplayMarketEvent::tick(
            "test",
            symbol,
            datetime,
            Some(datetime),
            Tick {
                id,
                datetime,
                last_price,
                ask_price1: last_price + 0.5,
                ask_volume1: 1,
                bid_price1: last_price - 0.5,
                bid_volume1: 1,
                ..Tick::default()
            },
        )
        .expect("tick event should be valid")
    }
}
