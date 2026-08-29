#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use serde_json::{Number, Value};
use tqsdk_core::{
    FieldMutation, Kline, NormalizedMutation, Quote, ReplaySessionId, ReplayUniverseBatch,
    ReplayUniverseChange, Tick,
};
use tqsdk_data::{HistoricalUniverseTimeline, UniverseMemberChange, UniverseTimelineBatch};

use crate::backtest_stream::{BacktestMarketStream, ReplayMarketStream};
use crate::replay::{ReplayMarketEvent, ReplayMarketPayload, ReplayMarketSource, ReplayStepMeta};
use crate::replay_runtime::{
    BacktestTickSubscription, ReplayKlineSpec, ReplayTickSpec, backtest_market_mutations,
    backtest_tick_subscriptions, ingest_presorted_replay_market_mutations, seed_replay_serials,
};
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
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
    price_ticks: HashMap<String, f64>,
    default_price_tick: Option<f64>,
    historical_universe: Option<HistoricalUniverseTimeline>,
}

/// Local Python-compatible strategy backtest host.
pub struct StrategyBacktest {
    replay: Box<dyn BacktestMarketStream>,
    pending_event: Option<ReplayMarketEvent>,
    strategy: StrategyHost,
    sim: TqSim,
    tracked_symbols: Vec<String>,
    klines: Vec<ReplayKlineSpec>,
    price_ticks: HashMap<String, f64>,
    quote_metadata_price_ticks: HashMap<String, f64>,
    last_replay_quotes: HashMap<String, ReplayStepQuote>,
    tick_subscriptions: HashMap<String, BacktestTickSubscription>,
    default_price_tick: Option<f64>,
    summary: StrategyBacktestSummary,
    universe_batches: VecDeque<UniverseTimelineBatch>,
    active_universe: Option<BTreeSet<String>>,
    universe_session_id: ReplaySessionId,
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
    active_universe: Option<&'a BTreeSet<String>>,
}

#[derive(Debug, Default)]
struct ReplayStepBatch {
    market_mutations: Vec<NormalizedMutation>,
    replay_mutations: Vec<NormalizedMutation>,
    latest_quotes: Vec<(String, ReplayStepQuote)>,
    sim_report: TqSimStepReport,
}

#[derive(Debug)]
struct ReplayStepQuote {
    quote: Quote,
    datetime_ns: Option<i64>,
}

impl ReplayStepQuote {
    fn quote(quote: Quote) -> Self {
        Self {
            quote,
            datetime_ns: None,
        }
    }

    fn tick(quote: Quote, datetime_ns: i64) -> Self {
        Self {
            quote,
            datetime_ns: Some(datetime_ns),
        }
    }
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
        let mut batch = ReplayStepBatch::default();
        self.ingest_universe_batches_at(event_time_ns, &mut batch)?;
        self.ingest_replay_event(&event, &mut batch)?;
        let backtest_event = StrategyBacktestEvent::from_replay_event(event);

        loop {
            let Some(next_event) = self.next_stream_event().await? else {
                break;
            };
            if next_event.event_time_ns() != event_time_ns {
                self.pending_event = Some(next_event);
                break;
            }
            self.ingest_replay_event(&next_event, &mut batch)?;
        }
        let sim_report = self.ingest_replay_batch(batch)?;
        self.record_summary_step(event_time_ns, &sim_report);

        let context = self.strategy.next_once().await?;
        Ok(Some(StrategyBacktestContext {
            event: backtest_event,
            context,
            sim: &mut self.sim,
            summary: &mut self.summary,
            tracked_symbols: &self.tracked_symbols,
            active_universe: self.active_universe.as_ref(),
        }))
    }

    async fn next_replay_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        if self.pending_event.is_some() {
            return Ok(self.pending_event.take());
        }
        self.next_stream_event().await
    }

    async fn next_stream_event(&mut self) -> Result<Option<ReplayMarketEvent>> {
        if let Some(result) = self.replay.next_event_ready() {
            return result;
        }
        self.replay.next_event().await
    }

    fn ingest_replay_event(
        &mut self,
        event: &ReplayMarketEvent,
        batch: &mut ReplayStepBatch,
    ) -> Result<()> {
        if let Some(mut mutations) =
            backtest_market_mutations(event, &self.klines, &mut self.tick_subscriptions)
        {
            batch.market_mutations.append(&mut mutations);
        }
        let event_time_ns = event.event_time_ns();
        match event.payload() {
            ReplayMarketPayload::Quote(quote) => {
                let quote =
                    quote_with_replay_underlying((**quote).clone(), event.underlying_symbol());
                self.ingest_quote(
                    event.symbol(),
                    ReplayStepQuote::quote(quote),
                    event_time_ns,
                    batch,
                );
            }
            ReplayMarketPayload::Tick(tick) => {
                let quote =
                    quote_with_replay_underlying(quote_from_tick(tick), event.underlying_symbol());
                self.ingest_quote(
                    event.symbol(),
                    ReplayStepQuote::tick(quote, tick.datetime),
                    event_time_ns,
                    batch,
                );
            }
            ReplayMarketPayload::Kline { row, .. } => {
                let price_tick = self.price_tick(event.symbol())?;
                let checkpoints = kline_quote_checkpoints(row, price_tick);
                for quote in checkpoints {
                    let quote = quote_with_replay_underlying(quote, event.underlying_symbol());
                    self.ingest_quote(
                        event.symbol(),
                        ReplayStepQuote::quote(quote),
                        event_time_ns,
                        batch,
                    );
                }
            }
        }
        self.summary.record_payload(event.payload_kind());
        Ok(())
    }

    fn ingest_replay_batch(&mut self, batch: ReplayStepBatch) -> Result<TqSimStepReport> {
        let ReplayStepBatch {
            market_mutations,
            replay_mutations,
            latest_quotes,
            sim_report,
        } = batch;
        let quote_fields = self.quote_fields_for_replay_batch(latest_quotes);
        ingest_presorted_replay_market_mutations(
            self.strategy.task_host(),
            market_mutations,
            replay_mutations,
            quote_fields,
        )?;
        self.sim
            .ingest_step_report(self.strategy.task_host(), &sim_report)?;
        Ok(sim_report)
    }

    fn quote_fields_for_replay_batch(
        &mut self,
        quotes: Vec<(String, ReplayStepQuote)>,
    ) -> Vec<(String, Vec<FieldMutation>)> {
        quotes
            .into_iter()
            .filter_map(|(symbol, quote)| {
                let fields = quote_state_fields_since(self.last_replay_quotes.get(&symbol), &quote);
                self.last_replay_quotes.insert(symbol.clone(), quote);
                if fields.is_empty() {
                    None
                } else {
                    Some((symbol, fields))
                }
            })
            .collect()
    }

    fn ingest_universe_batches_at(
        &mut self,
        event_time_ns: i64,
        batch: &mut ReplayStepBatch,
    ) -> Result<()> {
        if self
            .universe_batches
            .front()
            .is_some_and(|universe| universe.effective_ns < event_time_ns)
        {
            return Err(TaskError::InvalidState(
                "historical universe transition has no market event at its effective timestamp",
            ));
        }
        while self
            .universe_batches
            .front()
            .is_some_and(|universe| universe.effective_ns == event_time_ns)
        {
            let universe = self
                .universe_batches
                .pop_front()
                .expect("front was checked above");
            if let Some(active_universe) = &mut self.active_universe {
                for change in &universe.changes {
                    match change {
                        UniverseMemberChange::Add { instrument, .. } => {
                            if !active_universe.insert(instrument.symbol()) {
                                return Err(TaskError::InvalidState(
                                    "historical universe adds an already-active instrument",
                                ));
                            }
                        }
                        UniverseMemberChange::Remove { instrument } => {
                            let symbol = instrument.symbol();
                            let position = self.sim.position(&symbol);
                            let has_position =
                                position.volume_long != 0 || position.volume_short != 0;
                            let has_open_order = self.sim.orders().iter().any(|order| {
                                order.volume_left > 0
                                    && format!("{}.{}", order.exchange_id, order.instrument_id)
                                        == symbol
                            });
                            if has_position || has_open_order {
                                return Err(TaskError::InvalidState(
                                    "historical universe removes an instrument with open state",
                                ));
                            }
                            if !active_universe.remove(&symbol) {
                                return Err(TaskError::InvalidState(
                                    "historical universe removes an inactive instrument",
                                ));
                            }
                        }
                    }
                }
            }
            let changes = universe
                .changes
                .into_iter()
                .map(|change| match change {
                    UniverseMemberChange::Add {
                        instrument,
                        provenance,
                    } => ReplayUniverseChange {
                        instrument: instrument.symbol(),
                        active: true,
                        readiness: Some("ready".to_string()),
                        provenance: Some(provenance),
                    },
                    UniverseMemberChange::Remove { instrument } => ReplayUniverseChange {
                        instrument: instrument.symbol(),
                        active: false,
                        readiness: None,
                        provenance: None,
                    },
                })
                .collect();
            batch.replay_mutations.push(
                ReplayUniverseBatch {
                    session_id: self.universe_session_id.clone(),
                    effective_ns: universe.effective_ns,
                    changes,
                }
                .into_normalized_mutation(),
            );
        }
        Ok(())
    }

    fn record_summary_step(&mut self, event_time_ns: i64, sim_report: &TqSimStepReport) {
        if sim_report.is_empty() {
            self.summary
                .record_unchanged_account_observation(Some(event_time_ns));
        } else {
            self.summary.record_snapshot(ledger_snapshot_from_sim(
                &self.sim,
                &self.tracked_symbols,
                Some(event_time_ns),
            ));
        }
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

    fn ingest_quote(
        &mut self,
        symbol: &str,
        quote: ReplayStepQuote,
        event_time_ns: i64,
        batch: &mut ReplayStepBatch,
    ) {
        self.remember_quote_metadata(symbol, &quote.quote);
        let report = self
            .sim
            .update_quote_ref_at(symbol, &quote.quote, event_time_ns);
        if let Some((_, existing)) = batch
            .latest_quotes
            .iter_mut()
            .find(|(existing, _)| existing == symbol)
        {
            *existing = quote;
        } else {
            batch.latest_quotes.push((symbol.to_owned(), quote));
        }
        batch.sim_report.extend(report);
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
            klines: Vec::new(),
            ticks: Vec::new(),
            price_ticks: HashMap::new(),
            default_price_tick: None,
            historical_universe: None,
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

    /// Attach a pinned historical membership timeline to this local backtest.
    #[must_use]
    pub fn historical_universe(mut self, timeline: HistoricalUniverseTimeline) -> Self {
        self.historical_universe = Some(timeline);
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
    pub fn kline(mut self, symbol: impl AsRef<str>, duration: Duration, view_width: usize) -> Self {
        let spec = ReplayKlineSpec {
            symbol: symbol.as_ref().to_owned(),
            duration_ns: duration_to_ns(duration),
            view_width,
        };
        if !self.klines.iter().any(|existing| existing == &spec) {
            self.klines.push(spec);
        }
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        let spec = ReplayTickSpec {
            symbol: symbol.as_ref().to_owned(),
            view_width,
        };
        if !self.ticks.iter().any(|existing| existing == &spec) {
            self.ticks.push(spec);
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
            klines,
            ticks,
            price_ticks,
            default_price_tick,
            historical_universe,
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
        seed_replay_serials(&host, &klines, &ticks)?;
        let tick_subscriptions = backtest_tick_subscriptions(&ticks);
        let mut builder = StrategyHostBuilder::new(host).account(sim.account_id());
        for quote in &quotes {
            builder = builder.quote(quote);
        }
        for spec in &klines {
            builder = builder.kline(
                &spec.symbol,
                Duration::from_nanos(spec.duration_ns as u64),
                spec.view_width,
            );
        }
        for spec in &ticks {
            builder = builder.tick(&spec.symbol, spec.view_width);
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
            klines,
            price_ticks,
            quote_metadata_price_ticks: HashMap::new(),
            last_replay_quotes: HashMap::new(),
            tick_subscriptions,
            default_price_tick,
            summary,
            active_universe: historical_universe.as_ref().map(|_| BTreeSet::new()),
            universe_batches: historical_universe
                .map(|timeline| timeline.batches.into())
                .unwrap_or_default(),
            universe_session_id: ReplaySessionId::new("local-backtest-universe"),
        })
    }
}

impl StrategyBacktestEvent {
    fn from_replay_event(event: ReplayMarketEvent) -> Self {
        event.into_step_meta().into()
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

    pub fn kline(
        &self,
        symbol: impl AsRef<str>,
        duration: Duration,
    ) -> Result<tqsdk_wait::KlineWindow> {
        self.context.kline(symbol, duration)
    }

    pub fn tick(&self, symbol: impl AsRef<str>) -> Result<tqsdk_wait::TickWindow> {
        self.context.tick(symbol)
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
        let active_universe = self.active_universe.as_ref();
        let report = self.sim.process_host_orders_with_guard(
            self.context.task_host(),
            |symbol, offset| {
                if offset == tqsdk_core::TradeOffset::Open
                    && active_universe.is_some_and(|members| !members.contains(symbol))
                {
                    return Err(TaskError::InvalidState(
                        "historical universe rejects opening an inactive instrument",
                    ));
                }
                Ok(())
            },
        )?;
        if report.is_empty() {
            self.summary
                .record_unchanged_account_observation(Some(self.event.event_time_ns()));
        } else {
            self.summary.record_snapshot(ledger_snapshot_from_sim(
                self.sim,
                self.tracked_symbols,
                Some(self.event.event_time_ns()),
            ));
        }
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

#[cfg(test)]
fn quote_state_fields(quote: &ReplayStepQuote) -> Vec<FieldMutation> {
    quote_state_fields_since(None, quote)
}

fn quote_state_fields_since(
    previous: Option<&ReplayStepQuote>,
    quote: &ReplayStepQuote,
) -> Vec<FieldMutation> {
    let mut fields = Vec::with_capacity(16);
    let quote_data = &quote.quote;
    let previous_quote = previous.map(|previous| &previous.quote);
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.amount),
        "amount",
        quote_data.amount,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.ask_price1),
        "ask_price1",
        quote_data.ask_price1,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.ask_volume1),
        "ask_volume1",
        quote_data.ask_volume1,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.average),
        "average",
        quote_data.average,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.bid_price1),
        "bid_price1",
        quote_data.bid_price1,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.bid_volume1),
        "bid_volume1",
        quote_data.bid_volume1,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.close),
        "close",
        quote_data.close,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.commission),
        "commission",
        quote_data.commission,
    );
    push_datetime_if_changed(&mut fields, previous, quote);
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.exchange_id.as_str()),
        "exchange_id",
        &quote_data.exchange_id,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.highest),
        "highest",
        quote_data.highest,
    );
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.ins_class.as_str()),
        "ins_class",
        &quote_data.ins_class,
    );
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.instrument_id.as_str()),
        "instrument_id",
        &quote_data.instrument_id,
    );
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.instrument_name.as_str()),
        "instrument_name",
        &quote_data.instrument_name,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.last_price),
        "last_price",
        quote_data.last_price,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.lowest),
        "lowest",
        quote_data.lowest,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.margin),
        "margin",
        quote_data.margin,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.max_limit_order_volume),
        "max_limit_order_volume",
        quote_data.max_limit_order_volume,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.max_market_order_volume),
        "max_market_order_volume",
        quote_data.max_market_order_volume,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.min_limit_order_volume),
        "min_limit_order_volume",
        quote_data.min_limit_order_volume,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.min_market_order_volume),
        "min_market_order_volume",
        quote_data.min_market_order_volume,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.open),
        "open",
        quote_data.open,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.open_interest),
        "open_interest",
        quote_data.open_interest,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.open_limit),
        "open_limit",
        quote_data.open_limit,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.price_decs),
        "price_decs",
        quote_data.price_decs,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.price_tick),
        "price_tick",
        quote_data.price_tick,
    );
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.product_id.as_str()),
        "product_id",
        &quote_data.product_id,
    );
    push_f64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.strike_price),
        "strike_price",
        quote_data.strike_price,
    );
    push_string_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.underlying_symbol.as_str()),
        "underlying_symbol",
        &quote_data.underlying_symbol,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.volume),
        "volume",
        quote_data.volume,
    );
    push_i64_if_changed(
        &mut fields,
        previous_quote.map(|quote| quote.volume_multiple),
        "volume_multiple",
        quote_data.volume_multiple,
    );

    fields
}

fn quote_from_tick(tick: &Tick) -> Quote {
    Quote {
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

fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}

fn push_datetime_if_changed(
    fields: &mut Vec<FieldMutation>,
    previous: Option<&ReplayStepQuote>,
    quote: &ReplayStepQuote,
) {
    if quote_datetime_matches(previous, quote) {
        return;
    }
    let Some(datetime) = quote_datetime_value(quote) else {
        return;
    };
    push_field(fields, "datetime", Value::from(datetime));
}

fn quote_datetime_value(quote: &ReplayStepQuote) -> Option<String> {
    if let Some(datetime_ns) = quote.datetime_ns {
        Some(datetime_ns.to_string())
    } else if quote.quote.datetime.is_empty() {
        None
    } else {
        Some(quote.quote.datetime.clone())
    }
}

fn quote_datetime_matches(previous: Option<&ReplayStepQuote>, quote: &ReplayStepQuote) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    match (previous.datetime_ns, quote.datetime_ns) {
        (Some(previous_ns), Some(current_ns)) => previous_ns == current_ns,
        (None, None) => {
            !quote.quote.datetime.is_empty() && previous.quote.datetime == quote.quote.datetime
        }
        (Some(previous_ns), None) => {
            !quote.quote.datetime.is_empty() && previous_ns.to_string() == quote.quote.datetime
        }
        (None, Some(current_ns)) => {
            !previous.quote.datetime.is_empty() && previous.quote.datetime == current_ns.to_string()
        }
    }
}

fn push_string_if_changed(
    fields: &mut Vec<FieldMutation>,
    previous: Option<&str>,
    key: &str,
    current: &str,
) {
    if current.is_empty() || previous == Some(current) {
        return;
    }
    push_field(fields, key, Value::from(current));
}

fn push_f64_if_changed(
    fields: &mut Vec<FieldMutation>,
    previous: Option<f64>,
    key: &str,
    current: f64,
) {
    let Some(number) = Number::from_f64(current) else {
        return;
    };
    if previous.is_some_and(|previous| Number::from_f64(previous).is_some() && previous == current)
    {
        return;
    }
    push_field(fields, key, Value::Number(number));
}

fn push_i64_if_changed(
    fields: &mut Vec<FieldMutation>,
    previous: Option<i64>,
    key: &str,
    current: i64,
) {
    if current == 0 || previous == Some(current) {
        return;
    }
    push_field(fields, key, Value::from(current));
}

fn push_field(fields: &mut Vec<FieldMutation>, key: &str, value: Value) {
    fields.push(FieldMutation {
        field: key.to_string(),
        value,
    });
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

    #[tokio::test]
    async fn strategy_backtest_keeps_latest_quote_for_duplicate_timestamp_symbol() {
        let replay = ReplayMarketSource::new(vec![
            tick_event("SHFE.a", 1, 1_000, 101.0),
            tick_event("SHFE.a", 2, 1_000, 102.0),
            tick_event("SHFE.a", 3, 2_000, 103.0),
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
        let first_quote = first.quote("SHFE.a").unwrap();
        assert_eq!(first_quote.last_price, 102.0);
        assert_eq!(first_quote.datetime, "1000");
        drop(first);

        let second = backtest
            .next()
            .await
            .expect("second step should succeed")
            .expect("second step should exist");
        assert_eq!(second.event().event_time_ns(), 2_000);
        assert_eq!(second.quote("SHFE.a").unwrap().last_price, 103.0);
        drop(second);

        assert!(backtest.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn strategy_backtest_updates_declared_tick_serial() {
        let replay = ReplayMarketSource::new(vec![tick_event("SHFE.a", 1, 1_000, 101.0)]);
        let mut backtest = StrategyBacktest::builder(replay)
            .tick("SHFE.a", 10)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        let ticks = first.tick("SHFE.a").expect("tick window should load");

        assert_eq!(ticks.symbol(), "SHFE.a");
        assert_eq!(ticks.view_width(), 10);
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks.last().expect("last tick").last_price, 101.0);
    }

    #[tokio::test]
    async fn strategy_backtest_bounds_declared_tick_window_and_reclaims_rows() {
        let replay = ReplayMarketSource::new(vec![
            tick_event("SHFE.a", 10, 1_000, 101.0),
            tick_event("SHFE.a", 11, 2_000, 102.0),
            tick_event("SHFE.a", 12, 3_000, 103.0),
        ]);
        let mut backtest = StrategyBacktest::builder(replay)
            .tick("SHFE.a", 2)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest.next().await.unwrap().expect("first step");
        assert_eq!(
            first
                .tick("SHFE.a")
                .unwrap()
                .iter()
                .map(|tick| tick.id)
                .collect::<Vec<_>>(),
            vec![10]
        );
        drop(first);

        let second = backtest.next().await.unwrap().expect("second step");
        assert_eq!(
            second
                .tick("SHFE.a")
                .unwrap()
                .iter()
                .map(|tick| tick.id)
                .collect::<Vec<_>>(),
            vec![10, 11]
        );
        drop(second);

        let third = backtest.next().await.unwrap().expect("third step");
        assert_eq!(
            third
                .tick("SHFE.a")
                .unwrap()
                .iter()
                .map(|tick| tick.id)
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
        let market = third
            .task_host()
            .api()
            .session()
            .reader()
            .read_market_state();
        assert!(
            market
                .get_path(&["ticks", "SHFE.a", "data", "10"])
                .is_none(),
            "rows outside the declared tick window must be removed"
        );
        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-2", "ready"])
                .and_then(Value::as_bool),
            Some(true),
            "seeded chart readiness must survive incremental bounds updates"
        );
        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-2", "state", "duration"])
                .and_then(Value::as_i64),
            Some(0),
            "seeded chart state must survive incremental bounds updates"
        );
    }

    #[tokio::test]
    async fn strategy_backtest_keeps_distinct_bounds_for_tick_views_of_one_symbol() {
        let replay = ReplayMarketSource::new(vec![
            tick_event("SHFE.a", 10, 1_000, 101.0),
            tick_event("SHFE.a", 11, 2_000, 102.0),
            tick_event("SHFE.a", 12, 3_000, 103.0),
        ]);
        let mut backtest = StrategyBacktest::builder(replay)
            .tick("SHFE.a", 1)
            .tick("SHFE.a", 2)
            .build()
            .await
            .expect("backtest should build");

        drop(backtest.next().await.unwrap().expect("first step"));
        drop(backtest.next().await.unwrap().expect("second step"));
        let third = backtest.next().await.unwrap().expect("third step");
        let market = third
            .task_host()
            .api()
            .session()
            .reader()
            .read_market_state();

        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-1", "left_id"])
                .and_then(Value::as_i64),
            Some(12)
        );
        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-1", "right_id"])
                .and_then(Value::as_i64),
            Some(12)
        );
        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-2", "left_id"])
                .and_then(Value::as_i64),
            Some(11)
        );
        assert_eq!(
            market
                .get_path(&["charts", "wait-tick-SHFE_a-2", "right_id"])
                .and_then(Value::as_i64),
            Some(12)
        );
        assert!(
            market
                .get_path(&["ticks", "SHFE.a", "data", "10"])
                .is_none(),
            "the largest declared window should reclaim rows outside its own capacity"
        );
        assert!(
            market
                .get_path(&["ticks", "SHFE.a", "data", "11"])
                .is_some()
        );
        assert!(
            market
                .get_path(&["ticks", "SHFE.a", "data", "12"])
                .is_some()
        );
    }

    #[tokio::test]
    async fn strategy_backtest_tick_and_quote_share_one_replay_commit() {
        let replay = ReplayMarketSource::new(vec![tick_event("SHFE.a", 7, 1_000, 101.0)]);
        let mut backtest = StrategyBacktest::builder(replay)
            .tick("SHFE.a", 10)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        let objects = &first
            .task_host()
            .api()
            .last_commit()
            .expect("step should expose its replay commit")
            .changes
            .object_hits;
        assert!(objects.iter().any(|object| {
            matches!(
                object,
                tqsdk_core::ObjectKey::Tick { symbol, tick_id }
                    if symbol.as_str() == "SHFE.a" && *tick_id == 7
            )
        }));
        assert!(objects.iter().any(|object| {
            matches!(
                object,
                tqsdk_core::ObjectKey::Quote { symbol } if symbol.as_str() == "SHFE.a"
            )
        }));
    }

    #[tokio::test]
    async fn strategy_backtest_skips_undeclared_tick_serial_state() {
        let replay = ReplayMarketSource::new(vec![tick_event("SHFE.a", 7, 1_000, 101.0)]);
        let mut backtest = StrategyBacktest::builder(replay)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        assert_eq!(first.quote("SHFE.a").unwrap().last_price, 101.0);
        let objects = &first
            .task_host()
            .api()
            .last_commit()
            .expect("step should expose its replay commit")
            .changes
            .object_hits;
        assert!(objects.iter().any(|object| {
            matches!(
                object,
                tqsdk_core::ObjectKey::Quote { symbol } if symbol.as_str() == "SHFE.a"
            )
        }));
        assert!(!objects.iter().any(|object| {
            matches!(
                object,
                tqsdk_core::ObjectKey::Tick { symbol, .. } if symbol.as_str() == "SHFE.a"
            )
        }));
    }

    #[tokio::test]
    async fn strategy_backtest_updates_declared_kline_serial() {
        let replay =
            ReplayMarketSource::new(vec![kline_event("SHFE.a", 1, 1_000, 60_000_000_000, 101.0)]);
        let mut backtest = StrategyBacktest::builder(replay)
            .kline("SHFE.a", Duration::from_secs(60), 10)
            .default_price_tick(0.5)
            .build()
            .await
            .expect("backtest should build");

        let first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        let klines = first
            .kline("SHFE.a", Duration::from_secs(60))
            .expect("kline window should load");

        assert_eq!(klines.symbol(), "SHFE.a");
        assert_eq!(klines.duration_ns(), 60_000_000_000);
        assert_eq!(klines.view_width(), 10);
        assert_eq!(klines.len(), 1);
        assert_eq!(klines.last().expect("last kline").close, 101.0);
    }

    #[tokio::test]
    async fn strategy_backtest_empty_sim_steps_keep_summary_counts_and_times() {
        let replay = ReplayMarketSource::new(vec![
            tick_event("SHFE.a", 1, 1_000, 101.0),
            tick_event("SHFE.a", 2, 2_000, 102.0),
        ]);
        let mut backtest = StrategyBacktest::builder(replay)
            .build()
            .await
            .expect("backtest should build");

        let mut first = backtest
            .next()
            .await
            .expect("first step should succeed")
            .expect("first step should exist");
        assert!(first.finish_sim_step().unwrap().is_empty());
        drop(first);

        let mut second = backtest
            .next()
            .await
            .expect("second step should succeed")
            .expect("second step should exist");
        assert!(second.finish_sim_step().unwrap().is_empty());
        drop(second);

        assert!(backtest.next().await.unwrap().is_none());
        let summary = backtest.summary();
        assert_eq!(summary.event_count(), 2);
        assert_eq!(summary.tick_count(), 2);
        assert_eq!(summary.start_event_time_ns(), Some(1_000));
        assert_eq!(summary.end_event_time_ns(), Some(2_000));
        assert_eq!(summary.final_positions().len(), 1);
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

    #[test]
    fn quote_state_fields_are_sorted_for_presorted_ingest() {
        let fields = quote_state_fields(&ReplayStepQuote::tick(
            Quote {
                datetime: "1000".to_string(),
                last_price: 100.0,
                highest: 101.0,
                lowest: 99.0,
                open: 100.0,
                close: 100.5,
                average: 100.25,
                ask_price1: 100.5,
                ask_volume1: 1,
                bid_price1: 99.5,
                bid_volume1: 2,
                volume: 10,
                amount: 1_000.0,
                open_interest: 20,
                price_tick: 0.5,
                price_decs: 1,
                volume_multiple: 10,
                open_limit: 100,
                max_limit_order_volume: 500,
                max_market_order_volume: 100,
                min_limit_order_volume: 1,
                min_market_order_volume: 1,
                underlying_symbol: "SHFE.rb2601".to_string(),
                strike_price: 10.0,
                ins_class: "FUTURE".to_string(),
                instrument_id: "rb2601".to_string(),
                instrument_name: "rb2601".to_string(),
                exchange_id: "SHFE".to_string(),
                product_id: "rb".to_string(),
                margin: 1_000.0,
                commission: 1.0,
                ..Quote::default()
            },
            1_000,
        ));

        assert!(fields.windows(2).all(|pair| pair[0].field <= pair[1].field));
    }

    #[test]
    fn quote_state_fields_since_keeps_only_changed_present_fields() {
        let previous = ReplayStepQuote::tick(
            Quote {
                last_price: 100.0,
                volume: 10,
                open_interest: 20,
                underlying_symbol: "SHFE.rb2601".to_string(),
                ..Quote::default()
            },
            1_000,
        );
        let current = ReplayStepQuote::tick(
            Quote {
                last_price: 101.0,
                volume: 11,
                open_interest: 0,
                underlying_symbol: "SHFE.rb2601".to_string(),
                ..Quote::default()
            },
            2_000,
        );

        let fields = quote_state_fields_since(Some(&previous), &current);
        let names = fields
            .iter()
            .map(|field| field.field.as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["datetime", "last_price", "volume"]);
        assert_eq!(fields[0].value, Value::from("2000"));
        assert_eq!(fields[1].value, Value::from(101.0));
        assert_eq!(fields[2].value, Value::from(11));
    }

    #[test]
    fn quote_state_fields_since_skips_unchanged_tick_quote() {
        let previous = ReplayStepQuote::tick(
            Quote {
                last_price: 100.0,
                volume: 10,
                underlying_symbol: "SHFE.rb2601".to_string(),
                ..Quote::default()
            },
            1_000,
        );
        let current = ReplayStepQuote::tick(
            Quote {
                last_price: 100.0,
                volume: 10,
                underlying_symbol: "SHFE.rb2601".to_string(),
                ..Quote::default()
            },
            1_000,
        );

        assert!(quote_state_fields_since(Some(&previous), &current).is_empty());
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

    fn kline_event(
        symbol: &str,
        id: i64,
        datetime: i64,
        duration_ns: i64,
        close: f64,
    ) -> ReplayMarketEvent {
        ReplayMarketEvent::kline(
            "test",
            symbol,
            datetime,
            Some(datetime),
            duration_ns,
            Kline {
                id,
                datetime,
                open: close,
                high: close,
                low: close,
                close,
                volume: 1,
                ..Kline::default()
            },
        )
        .expect("kline event should be valid")
    }
}
