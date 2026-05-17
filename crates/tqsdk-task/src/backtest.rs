#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{CommitScope, InputPayload, IoEvent, ProtocolDomain, Quote, RuntimeInput};
use tqsdk_data::{MarketCacheEvent, MarketCachePayload, MarketCacheReplay};

use crate::sim::{TqSim, TqSimStepReport};
use crate::strategy::StrategyHostBuilder;
use crate::testing::StrategyTestHarness;
use crate::{Result, StrategyContext, StrategyHost, TaskError, TaskHost};

/// Local Python-compatible strategy backtest over normalized market cache events.
pub struct StrategyBacktestBuilder {
    replay: MarketCacheReplay,
    sim: TqSim,
    quotes: Vec<String>,
}

/// Local Python-compatible strategy backtest host.
pub struct StrategyBacktest {
    replay: MarketCacheReplay,
    strategy: StrategyHost,
    sim: TqSim,
}

/// Metadata for the market event that produced a backtest context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyBacktestEvent {
    source: String,
    symbol: String,
    received_at_ns: i64,
    event_time_ns: i64,
}

/// Strategy context plus local sim controls for the current backtest step.
pub struct StrategyBacktestContext<'a> {
    event: StrategyBacktestEvent,
    context: StrategyContext<'a>,
    sim: &'a mut TqSim,
}

impl StrategyBacktest {
    #[must_use]
    pub fn builder(replay: MarketCacheReplay) -> StrategyBacktestBuilder {
        StrategyBacktestBuilder::new(replay)
    }

    pub async fn next(&mut self) -> Result<Option<StrategyBacktestContext<'_>>> {
        let Some(event) = self.replay.next() else {
            return Ok(None);
        };
        let backtest_event = StrategyBacktestEvent::from_cache_event(&event);
        match &event.payload {
            MarketCachePayload::Quote(quote) => {
                ingest_quote_event(self.strategy.task_host(), &event.symbol, quote)?;
                let report = self
                    .sim
                    .update_quote(event.symbol.clone(), (**quote).clone());
                self.sim
                    .ingest_step_report(self.strategy.task_host(), &report)?;
            }
            MarketCachePayload::Kline { .. } | MarketCachePayload::Tick(_) => {
                return Err(TaskError::Unsupported(
                    "StrategyBacktest currently supports quote events only",
                ));
            }
        }

        let context = self.strategy.next_once().await?;
        Ok(Some(StrategyBacktestContext {
            event: backtest_event,
            context,
            sim: &mut self.sim,
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
}

impl StrategyBacktestBuilder {
    #[must_use]
    pub fn new(replay: MarketCacheReplay) -> Self {
        Self {
            replay,
            sim: TqSim::new(),
            quotes: Vec::new(),
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

    pub async fn build(self) -> Result<StrategyBacktest> {
        let harness = StrategyTestHarness::new().build()?;
        let host = harness.into_task_host();
        let mut sim = self.sim;
        for quote in &self.quotes {
            sim.ensure_position(quote);
        }
        sim.seed_runtime(&host)?;
        let mut builder = StrategyHostBuilder::new(host).account(sim.account_id());
        for quote in &self.quotes {
            builder = builder.quote(quote);
        }
        let mut strategy = builder.build().await?;
        drain_initial_commits(&mut strategy).await?;
        Ok(StrategyBacktest {
            replay: self.replay,
            strategy,
            sim,
        })
    }
}

impl StrategyBacktestEvent {
    fn from_cache_event(event: &MarketCacheEvent) -> Self {
        Self {
            source: event.source.clone(),
            symbol: event.symbol.clone(),
            received_at_ns: event.received_at_ns,
            event_time_ns: event.event_time_ns(),
        }
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
    pub fn task_host(&self) -> &TaskHost {
        self.context.task_host()
    }

    #[must_use]
    pub fn sim(&self) -> &TqSim {
        self.sim
    }

    pub fn finish_sim_step(&mut self) -> Result<TqSimStepReport> {
        self.sim.process_host_orders(self.context.task_host())
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
    insert_f64_if_finite(&mut quote_value, "ask_price1", quote.ask_price1);
    insert_i64_if_nonzero(&mut quote_value, "ask_volume1", quote.ask_volume1);
    insert_f64_if_finite(&mut quote_value, "bid_price1", quote.bid_price1);
    insert_i64_if_nonzero(&mut quote_value, "bid_volume1", quote.bid_volume1);

    json!({
        "quotes": {
            symbol: Value::Object(quote_value)
        }
    })
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
