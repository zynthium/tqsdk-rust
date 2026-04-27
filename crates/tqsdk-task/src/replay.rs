#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote, RuntimeInput, Tick,
};
use tqsdk_data::{MarketCacheEvent, MarketCachePayload, MarketCacheReplay};

use crate::strategy::StrategyHostBuilder;
use crate::testing::{FakeBroker, FakeMarket, StrategyTestReport};
use crate::{Result, StrategyContext, StrategyHost, TaskError, TaskHost};

/// Offline strategy replay builder backed by ordered market cache events.
pub struct StrategyReplayBuilder {
    replay: MarketCacheReplay,
    market: FakeMarket,
    broker: FakeBroker,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
    checkpoint: StrategyReplayCheckpoint,
    speed: StrategyReplaySpeed,
}

/// Offline strategy replay host.
pub struct StrategyReplay {
    replay: MarketCacheReplay,
    strategy: StrategyHost,
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
    next_event_index: usize,
    replay_time_ns: Option<i64>,
    speed: StrategyReplaySpeed,
}

/// Metadata for the market event that produced a replay strategy context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyReplayEvent {
    source: String,
    symbol: String,
    received_at_ns: i64,
    event_time_ns: i64,
}

/// Resumable position in a [`StrategyReplay`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyReplayCheckpoint {
    next_event_index: usize,
    replay_time_ns: Option<i64>,
}

/// Replay pacing policy for [`StrategyReplay`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StrategyReplaySpeed {
    kind: StrategyReplaySpeedKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StrategyReplaySpeedKind {
    Fastest,
    Scaled { multiplier: f64 },
}

/// Strategy context plus the replay event that triggered it.
pub struct StrategyReplayContext<'a> {
    event: StrategyReplayEvent,
    checkpoint: StrategyReplayCheckpoint,
    context: StrategyContext<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayKlineSpec {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayTickSpec {
    symbol: String,
    view_width: usize,
}

impl StrategyReplay {
    #[must_use]
    pub fn builder(replay: MarketCacheReplay) -> StrategyReplayBuilder {
        StrategyReplayBuilder::new(replay)
    }

    pub async fn next(&mut self) -> Result<Option<StrategyReplayContext<'_>>> {
        let Some(event) = self.replay.next() else {
            return Ok(None);
        };
        let replay_event = StrategyReplayEvent::from_cache_event(&event);
        let event_time_ns = replay_event.event_time_ns();
        if let Some(delay) = self.speed.delay_between(self.replay_time_ns, event_time_ns) {
            tokio::time::sleep(delay).await;
        }
        ingest_market_cache_event(self.strategy.task_host(), &event, &self.klines, &self.ticks)?;
        let context = self.strategy.next_once().await?;
        self.next_event_index += 1;
        self.replay_time_ns = Some(event_time_ns);
        let checkpoint = StrategyReplayCheckpoint {
            next_event_index: self.next_event_index,
            replay_time_ns: self.replay_time_ns,
        };
        Ok(Some(StrategyReplayContext {
            event: replay_event,
            checkpoint,
            context,
        }))
    }

    #[must_use]
    pub fn replay_time_ns(&self) -> Option<i64> {
        self.replay_time_ns
    }

    #[must_use]
    pub fn checkpoint(&self) -> StrategyReplayCheckpoint {
        StrategyReplayCheckpoint {
            next_event_index: self.next_event_index,
            replay_time_ns: self.replay_time_ns,
        }
    }

    #[must_use]
    pub fn speed(&self) -> StrategyReplaySpeed {
        self.speed
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
    pub fn into_strategy(self) -> StrategyHost {
        self.strategy
    }
}

impl StrategyReplayBuilder {
    #[must_use]
    pub fn new(replay: MarketCacheReplay) -> Self {
        Self {
            replay,
            market: FakeMarket::new(),
            broker: FakeBroker::new(),
            accounts: Vec::new(),
            quotes: Vec::new(),
            klines: Vec::new(),
            ticks: Vec::new(),
            checkpoint: StrategyReplayCheckpoint::default(),
            speed: StrategyReplaySpeed::FASTEST,
        }
    }

    #[must_use]
    pub fn market(mut self, market: FakeMarket) -> Self {
        self.market = market;
        self
    }

    #[must_use]
    pub fn broker(mut self, broker: FakeBroker) -> Self {
        self.broker = broker;
        self
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>) -> Self {
        push_unique(&mut self.accounts, account_id.as_ref());
        self
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>) -> Self {
        push_unique(&mut self.quotes, symbol.as_ref());
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
    pub fn resume_from(mut self, checkpoint: StrategyReplayCheckpoint) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    #[must_use]
    pub fn speed(mut self, speed: StrategyReplaySpeed) -> Self {
        self.speed = speed;
        self
    }

    pub async fn build(self) -> Result<StrategyReplay> {
        let harness = crate::testing::StrategyTestHarness::new()
            .market(self.market)
            .broker(self.broker)
            .build()?;
        let host = harness.into_task_host();
        seed_replay_serials(&host, &self.klines, &self.ticks)?;
        let mut builder = StrategyHostBuilder::new(host);
        for account in &self.accounts {
            builder = builder.account(account);
        }
        for quote in &self.quotes {
            builder = builder.quote(quote);
        }
        for spec in &self.klines {
            builder = builder.kline(
                &spec.symbol,
                Duration::from_nanos(spec.duration_ns as u64),
                spec.view_width,
            );
        }
        for spec in &self.ticks {
            builder = builder.tick(&spec.symbol, spec.view_width);
        }
        let mut strategy = builder.build().await?;
        drain_initial_commits(&mut strategy).await?;
        let mut replay = self.replay;
        for _ in 0..self.checkpoint.next_event_index {
            if replay.next().is_none() {
                break;
            }
        }
        Ok(StrategyReplay {
            replay,
            strategy,
            klines: self.klines,
            ticks: self.ticks,
            next_event_index: self.checkpoint.next_event_index,
            replay_time_ns: self.checkpoint.replay_time_ns,
            speed: self.speed,
        })
    }
}

impl StrategyReplaySpeed {
    pub const FASTEST: Self = Self {
        kind: StrategyReplaySpeedKind::Fastest,
    };

    pub const REAL_TIME: Self = Self {
        kind: StrategyReplaySpeedKind::Scaled { multiplier: 1.0 },
    };

    pub fn scaled(multiplier: f64) -> Result<Self> {
        if !multiplier.is_finite() || multiplier <= 0.0 {
            return Err(TaskError::InvalidState(
                "strategy replay speed multiplier must be finite and positive",
            ));
        }
        Ok(Self {
            kind: StrategyReplaySpeedKind::Scaled { multiplier },
        })
    }

    fn delay_between(self, previous_time_ns: Option<i64>, event_time_ns: i64) -> Option<Duration> {
        match self.kind {
            StrategyReplaySpeedKind::Fastest => None,
            StrategyReplaySpeedKind::Scaled { multiplier } => {
                let previous_time_ns = previous_time_ns?;
                let delta_ns = event_time_ns.checked_sub(previous_time_ns)?;
                if delta_ns <= 0 {
                    return None;
                }
                let delay_secs = (delta_ns as f64 / 1_000_000_000.0) / multiplier;
                if delay_secs.is_finite() && delay_secs > 0.0 {
                    Some(Duration::from_secs_f64(delay_secs))
                } else {
                    None
                }
            }
        }
    }
}

impl Default for StrategyReplaySpeed {
    fn default() -> Self {
        Self::FASTEST
    }
}

impl StrategyReplayCheckpoint {
    #[must_use]
    pub const fn new(next_event_index: usize, replay_time_ns: Option<i64>) -> Self {
        Self {
            next_event_index,
            replay_time_ns,
        }
    }

    #[must_use]
    pub fn next_event_index(&self) -> usize {
        self.next_event_index
    }

    #[must_use]
    pub fn replay_time_ns(&self) -> Option<i64> {
        self.replay_time_ns
    }
}

impl StrategyReplayEvent {
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

impl StrategyReplayContext<'_> {
    #[must_use]
    pub fn event(&self) -> &StrategyReplayEvent {
        &self.event
    }

    #[must_use]
    pub fn replay_time_ns(&self) -> i64 {
        self.event.event_time_ns()
    }

    #[must_use]
    pub fn checkpoint(&self) -> StrategyReplayCheckpoint {
        self.checkpoint
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

    pub async fn finish_test_step(&mut self) -> Result<StrategyTestReport> {
        self.context.finish_test_step().await
    }
}

fn ingest_market_cache_event(
    host: &TaskHost,
    event: &MarketCacheEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    let body = match &event.payload {
        MarketCachePayload::Quote(quote) => quote_update(&event.symbol, quote),
        MarketCachePayload::Kline { duration_ns, row } => {
            kline_update(&event.symbol, *duration_ns, row, klines)
        }
        MarketCachePayload::Tick(tick) => tick_update(&event.symbol, tick, ticks),
    };

    host.api().handle_for_test().ingest(
        RuntimeInput::Io(IoEvent {
            route: "market-replay".to_string(),
            domains: vec![ProtocolDomain::Market],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": [body]
            })),
        }),
        vec![],
        CommitScope::ReplayStep,
    )?;
    Ok(())
}

fn seed_replay_serials(
    host: &TaskHost,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    for spec in klines {
        let chart_id = kline_chart_id(&spec.symbol, spec.duration_ns, spec.view_width);
        host.api().handle_for_test().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": spec.duration_ns
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "klines": {
                            spec.symbol.clone(): {
                                spec.duration_ns.to_string(): {
                                    "data": {
                                        "-1": {
                                            "id": -1,
                                            "datetime": -1
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    for spec in ticks {
        let chart_id = tick_chart_id(&spec.symbol, spec.view_width);
        host.api().handle_for_test().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": 0
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "ticks": {
                            spec.symbol.clone(): {
                                "data": {
                                    "-1": {
                                        "id": -1,
                                        "datetime": -1
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    Ok(())
}

async fn drain_initial_commits(strategy: &mut StrategyHost) -> Result<()> {
    let deadline = Some(tokio::time::Instant::now());
    while strategy.task_host_mut().wait_update(deadline).await? {}
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

fn kline_update(symbol: &str, duration_ns: i64, row: &Kline, klines: &[ReplayKlineSpec]) -> Value {
    let row_id = row.id.to_string();
    let mut charts = Map::new();
    for spec in klines
        .iter()
        .filter(|spec| spec.symbol == symbol && spec.duration_ns == duration_ns)
    {
        let chart_id = kline_chart_id(symbol, duration_ns, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": duration_ns
                },
                "left_id": row.id,
                "right_id": row.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    json!({
        "charts": Value::Object(charts),
        "klines": {
            symbol: {
                duration_ns.to_string(): {
                    "data": {
                        row_id: kline_value(row)
                    }
                }
            }
        }
    })
}

fn tick_update(symbol: &str, tick: &Tick, ticks: &[ReplayTickSpec]) -> Value {
    let row_id = tick.id.to_string();
    let mut charts = Map::new();
    for spec in ticks.iter().filter(|spec| spec.symbol == symbol) {
        let chart_id = tick_chart_id(symbol, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": 0
                },
                "left_id": tick.id,
                "right_id": tick.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    json!({
        "charts": Value::Object(charts),
        "ticks": {
            symbol: {
                "data": {
                    row_id: tick_value(tick)
                }
            }
        }
    })
}

fn kline_value(row: &Kline) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "open", row.open);
    insert_f64_if_finite(&mut value, "high", row.high);
    insert_f64_if_finite(&mut value, "low", row.low);
    insert_f64_if_finite(&mut value, "close", row.close);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_i64_if_nonzero(&mut value, "open_oi", row.open_oi);
    insert_i64_if_nonzero(&mut value, "close_oi", row.close_oi);
    Value::Object(value)
}

fn tick_value(row: &Tick) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "last_price", row.last_price);
    insert_f64_if_finite(&mut value, "average", row.average);
    insert_f64_if_finite(&mut value, "highest", row.highest);
    insert_f64_if_finite(&mut value, "lowest", row.lowest);
    insert_f64_if_finite(&mut value, "ask_price1", row.ask_price1);
    insert_i64_if_nonzero(&mut value, "ask_volume1", row.ask_volume1);
    insert_f64_if_finite(&mut value, "bid_price1", row.bid_price1);
    insert_i64_if_nonzero(&mut value, "bid_volume1", row.bid_volume1);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_f64_if_finite(&mut value, "amount", row.amount);
    insert_i64_if_nonzero(&mut value, "open_interest", row.open_interest);
    Value::Object(value)
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

fn kline_chart_id(symbol: &str, duration_ns: i64, view_width: usize) -> String {
    format!("wait-kline-{symbol}-{duration_ns}-{view_width}")
}

fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    format!("wait-tick-{symbol}-{view_width}")
}

fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}
