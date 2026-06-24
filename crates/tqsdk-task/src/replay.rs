#![cfg_attr(not(test), forbid(unsafe_code))]

use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, io};

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    CommitScope, InputPayload, IoEvent, Kline, ProtocolDomain, Quote, RuntimeInput, Tick,
};
use tqsdk_data::{KlineDataSeries, TickDataSeries};

use crate::strategy::StrategyHostBuilder;
use crate::testing::{FakeBroker, FakeMarket, StrategyTestReport};
use crate::{
    Result, StrategyContext, StrategyHost, StrategyUpdate, TargetPosBuilder, TaskError, TaskHost,
};

/// Normalized market payload for task-level replay and local backtest input.
#[derive(Debug, Clone)]
pub enum ReplayMarketPayload {
    Quote(Box<Quote>),
    Kline { duration_ns: i64, row: Kline },
    Tick(Tick),
}

/// Stable payload classifier for task-level replay summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplayMarketPayloadKind {
    Quote,
    Kline,
    Tick,
}

/// Normalized market event for task-level deterministic replay.
#[derive(Debug, Clone)]
pub struct ReplayMarketEvent {
    source: String,
    symbol: String,
    received_at_ns: i64,
    event_time_ns: i64,
    underlying_symbol: Option<String>,
    payload: ReplayMarketPayload,
}

/// Ordered task-level market replay source.
#[derive(Debug, Clone, Default)]
pub struct ReplayMarketSource {
    events: Vec<ReplayMarketEvent>,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayStepMeta {
    pub(crate) source: String,
    pub(crate) symbol: String,
    pub(crate) received_at_ns: i64,
    pub(crate) event_time_ns: i64,
    pub(crate) underlying_symbol: Option<String>,
}

/// Offline strategy replay builder backed by ordered replay market events.
pub struct StrategyReplayBuilder {
    replay: ReplayMarketSource,
    market: FakeMarket,
    broker: FakeBroker,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<ReplayKlineSpec>,
    ticks: Vec<ReplayTickSpec>,
    checkpoint: StrategyReplayCheckpoint,
    speed: StrategyReplaySpeed,
}

/// Builder that combines multiple normalized market event series into one replay.
#[derive(Debug, Clone, Default)]
pub struct StrategyReplaySourceBuilder {
    events: Vec<ReplayMarketEvent>,
}

/// Offline strategy replay host.
pub struct StrategyReplay {
    replay: ReplayMarketSource,
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
    underlying_symbol: Option<String>,
}

/// Resumable position in a [`StrategyReplay`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StrategyReplayCheckpoint {
    next_event_index: usize,
    replay_time_ns: Option<i64>,
}

/// JSON file-backed durable store for [`StrategyReplayCheckpoint`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyReplayCheckpointStore {
    path: PathBuf,
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
    pub fn builder(replay: ReplayMarketSource) -> StrategyReplayBuilder {
        StrategyReplayBuilder::new(replay)
    }

    #[must_use]
    pub fn source_builder() -> StrategyReplaySourceBuilder {
        StrategyReplaySourceBuilder::new()
    }

    pub async fn next(&mut self) -> Result<Option<StrategyReplayContext<'_>>> {
        let Some(event) = self.replay.next() else {
            return Ok(None);
        };
        let replay_event = StrategyReplayEvent::from_replay_event(&event);
        let event_time_ns = replay_event.event_time_ns();
        if let Some(delay) = self.speed.delay_between(self.replay_time_ns, event_time_ns) {
            tokio::time::sleep(delay).await;
        }
        ingest_replay_market_event(self.strategy.task_host(), &event, &self.klines, &self.ticks)?;
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

impl StrategyReplaySourceBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    #[must_use]
    pub fn event(mut self, event: ReplayMarketEvent) -> Self {
        self.events.push(event);
        self
    }

    #[must_use]
    pub fn events(mut self, events: impl IntoIterator<Item = ReplayMarketEvent>) -> Self {
        self.events.extend(events);
        self
    }

    pub fn kline_series(self, series: KlineDataSeries, source: impl AsRef<str>) -> Result<Self> {
        let symbol = series.symbol().to_owned();
        let duration_ns = series.duration_ns();
        self.kline_rows(symbol, duration_ns, series.into_rows(), source)
    }

    /// Append kline history series while replaying it under a caller-provided symbol.
    ///
    /// This is useful when underlying contract history should drive a synthetic
    /// replay symbol such as a continuous-contract code.
    pub fn kline_series_as(
        self,
        series: KlineDataSeries,
        replay_symbol: impl AsRef<str>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        let underlying_symbol = series.symbol().to_owned();
        let replay_symbol = replay_symbol.as_ref().to_owned();
        let duration_ns = series.duration_ns();
        if replay_symbol == underlying_symbol {
            self.kline_rows(replay_symbol, duration_ns, series.into_rows(), source)
        } else {
            self.kline_rows_with_underlying(
                replay_symbol,
                underlying_symbol,
                duration_ns,
                series.into_rows(),
                source,
            )
        }
    }

    /// Append owned kline rows under a replay symbol.
    ///
    /// `duration_ns` is the kline duration in nanoseconds.
    pub fn kline_rows(
        self,
        replay_symbol: impl AsRef<str>,
        duration_ns: i64,
        rows: impl IntoIterator<Item = Kline>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        self.kline_rows_inner(replay_symbol, None::<&str>, duration_ns, rows, source)
    }

    /// Append owned kline rows under a replay symbol with underlying metadata.
    ///
    /// This keeps the replay symbol stable while exposing the contract that
    /// supplied the rows through quote `underlying_symbol`.
    pub fn kline_rows_with_underlying(
        self,
        replay_symbol: impl AsRef<str>,
        underlying_symbol: impl AsRef<str>,
        duration_ns: i64,
        rows: impl IntoIterator<Item = Kline>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        self.kline_rows_inner(
            replay_symbol,
            Some(underlying_symbol.as_ref()),
            duration_ns,
            rows,
            source,
        )
    }

    fn kline_rows_inner(
        mut self,
        replay_symbol: impl AsRef<str>,
        underlying_symbol: Option<&str>,
        duration_ns: i64,
        rows: impl IntoIterator<Item = Kline>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        let replay_symbol = replay_symbol.as_ref();
        let source = source.as_ref();
        for row in rows {
            let mut event = ReplayMarketEvent::kline(
                source,
                replay_symbol,
                row.datetime,
                Some(row.datetime),
                duration_ns,
                row,
            )?;
            if let Some(underlying_symbol) = underlying_symbol {
                event = event.with_underlying_symbol(underlying_symbol)?;
            }
            self.events.push(event);
        }
        Ok(self)
    }

    pub fn tick_series(self, series: TickDataSeries, source: impl AsRef<str>) -> Result<Self> {
        let symbol = series.symbol().to_owned();
        self.tick_rows(symbol, series.into_rows(), source)
    }

    /// Append tick history series while replaying it under a caller-provided symbol.
    ///
    /// This is useful when underlying contract history should drive a synthetic
    /// replay symbol such as a continuous-contract code.
    pub fn tick_series_as(
        self,
        series: TickDataSeries,
        replay_symbol: impl AsRef<str>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        let underlying_symbol = series.symbol().to_owned();
        let replay_symbol = replay_symbol.as_ref().to_owned();
        if replay_symbol == underlying_symbol {
            self.tick_rows(replay_symbol, series.into_rows(), source)
        } else {
            self.tick_rows_with_underlying(
                replay_symbol,
                underlying_symbol,
                series.into_rows(),
                source,
            )
        }
    }

    /// Append owned tick rows under a replay symbol.
    pub fn tick_rows(
        self,
        replay_symbol: impl AsRef<str>,
        rows: impl IntoIterator<Item = Tick>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        self.tick_rows_inner(replay_symbol, None::<&str>, rows, source)
    }

    /// Append owned tick rows under a replay symbol with underlying metadata.
    ///
    /// This keeps the replay symbol stable while exposing the contract that
    /// supplied the rows through quote `underlying_symbol`.
    pub fn tick_rows_with_underlying(
        self,
        replay_symbol: impl AsRef<str>,
        underlying_symbol: impl AsRef<str>,
        rows: impl IntoIterator<Item = Tick>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        self.tick_rows_inner(
            replay_symbol,
            Some(underlying_symbol.as_ref()),
            rows,
            source,
        )
    }

    fn tick_rows_inner(
        mut self,
        replay_symbol: impl AsRef<str>,
        underlying_symbol: Option<&str>,
        rows: impl IntoIterator<Item = Tick>,
        source: impl AsRef<str>,
    ) -> Result<Self> {
        let replay_symbol = replay_symbol.as_ref();
        let source = source.as_ref();
        for row in rows {
            let mut event = ReplayMarketEvent::tick(
                source,
                replay_symbol,
                row.datetime,
                Some(row.datetime),
                row,
            )?;
            if let Some(underlying_symbol) = underlying_symbol {
                event = event.with_underlying_symbol(underlying_symbol)?;
            }
            self.events.push(event);
        }
        Ok(self)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[must_use]
    pub fn build(self) -> ReplayMarketSource {
        ReplayMarketSource::new(self.events)
    }
}

impl StrategyReplayBuilder {
    #[must_use]
    pub fn new(replay: ReplayMarketSource) -> Self {
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

    pub fn resume_from_store(mut self, store: &StrategyReplayCheckpointStore) -> Result<Self> {
        if let Some(checkpoint) = store.load()? {
            self.checkpoint = checkpoint;
        }
        Ok(self)
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

impl StrategyReplayCheckpointStore {
    #[must_use]
    pub fn json_file(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<StrategyReplayCheckpoint>> {
        match fs::read_to_string(&self.path) {
            Ok(content) => {
                let value = serde_json::from_str(&content).map_err(|error| {
                    invalid_checkpoint(format!("checkpoint JSON is invalid: {error}"))
                })?;
                StrategyReplayCheckpoint::from_json_value(&value).map(Some)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(checkpoint_io_error("read", &self.path, error)),
        }
    }

    pub fn save(&self, checkpoint: StrategyReplayCheckpoint) -> Result<()> {
        if let Some(parent) = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|error| checkpoint_io_error("create parent directory", parent, error))?;
        }

        let tmp_path = checkpoint_tmp_path(&self.path);
        fs::write(&tmp_path, checkpoint.to_json_string())
            .map_err(|error| checkpoint_io_error("write", &tmp_path, error))?;
        fs::rename(&tmp_path, &self.path)
            .map_err(|error| checkpoint_io_error("rename", &self.path, error))?;
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(checkpoint_io_error("remove", &self.path, error)),
        }
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

    fn to_json_string(self) -> String {
        let mut output = json!({
            "version": 1,
            "next_event_index": self.next_event_index,
            "replay_time_ns": self.replay_time_ns,
        })
        .to_string();
        output.push('\n');
        output
    }

    fn from_json_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| invalid_checkpoint("checkpoint root must be a JSON object"))?;

        let version = object
            .get("version")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid_checkpoint("checkpoint version must be an unsigned integer"))?;
        if version != 1 {
            return Err(invalid_checkpoint(format!(
                "unsupported checkpoint version {version}"
            )));
        }

        let next_event_index = object
            .get("next_event_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                invalid_checkpoint("checkpoint next_event_index must be an unsigned integer")
            })?;
        let next_event_index = usize::try_from(next_event_index).map_err(|_| {
            invalid_checkpoint("checkpoint next_event_index does not fit this platform")
        })?;

        let replay_time_ns = match object.get("replay_time_ns") {
            Some(Value::Null) | None => None,
            Some(value) => Some(
                value
                    .as_i64()
                    .ok_or_else(|| invalid_checkpoint("checkpoint replay_time_ns must be i64"))?,
            ),
        };

        Ok(Self {
            next_event_index,
            replay_time_ns,
        })
    }
}

impl StrategyReplayEvent {
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

impl From<ReplayStepMeta> for StrategyReplayEvent {
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

impl ReplayMarketPayload {
    #[must_use]
    pub fn kind(&self) -> ReplayMarketPayloadKind {
        match self {
            Self::Quote(_) => ReplayMarketPayloadKind::Quote,
            Self::Kline { .. } => ReplayMarketPayloadKind::Kline,
            Self::Tick(_) => ReplayMarketPayloadKind::Tick,
        }
    }
}

impl ReplayMarketEvent {
    pub(crate) fn step_meta(&self) -> ReplayStepMeta {
        ReplayStepMeta {
            source: self.source().to_owned(),
            symbol: self.symbol().to_owned(),
            received_at_ns: self.received_at_ns(),
            event_time_ns: self.event_time_ns(),
            underlying_symbol: self.underlying_symbol.clone(),
        }
    }

    pub fn quote(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        event_time_ns: Option<i64>,
        quote: Quote,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            event_time_ns,
            ReplayMarketPayload::Quote(Box::new(quote)),
        )
    }

    pub fn kline(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        event_time_ns: Option<i64>,
        duration_ns: i64,
        row: Kline,
    ) -> Result<Self> {
        if duration_ns <= 0 {
            return Err(TaskError::InvalidState(
                "replay kline duration must be positive",
            ));
        }
        Self::new(
            source,
            symbol,
            received_at_ns,
            event_time_ns,
            ReplayMarketPayload::Kline { duration_ns, row },
        )
    }

    pub fn tick(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        event_time_ns: Option<i64>,
        tick: Tick,
    ) -> Result<Self> {
        Self::new(
            source,
            symbol,
            received_at_ns,
            event_time_ns,
            ReplayMarketPayload::Tick(tick),
        )
    }

    fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        received_at_ns: i64,
        event_time_ns: Option<i64>,
        payload: ReplayMarketPayload,
    ) -> Result<Self> {
        let source = source.into();
        let symbol = symbol.into();
        if source.trim().is_empty() {
            return Err(TaskError::InvalidState(
                "replay market event source must not be empty",
            ));
        }
        if symbol.trim().is_empty() {
            return Err(TaskError::InvalidState(
                "replay market event symbol must not be empty",
            ));
        }
        if received_at_ns < 0 {
            return Err(TaskError::InvalidState(
                "replay market event received_at_ns must be non-negative",
            ));
        }
        if event_time_ns.is_some_and(|time| time < 0) {
            return Err(TaskError::InvalidState(
                "replay market event event_time_ns must be non-negative",
            ));
        }
        Ok(Self {
            source,
            symbol,
            received_at_ns,
            event_time_ns: event_time_ns.unwrap_or(received_at_ns),
            underlying_symbol: None,
            payload,
        })
    }

    pub fn with_underlying_symbol(mut self, underlying_symbol: impl AsRef<str>) -> Result<Self> {
        let underlying_symbol = underlying_symbol.as_ref();
        if underlying_symbol.trim().is_empty() {
            return Err(TaskError::InvalidState(
                "replay market event underlying_symbol must not be empty",
            ));
        }
        self.underlying_symbol = Some(underlying_symbol.to_owned());
        Ok(self)
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

    #[must_use]
    pub fn payload(&self) -> &ReplayMarketPayload {
        &self.payload
    }

    #[must_use]
    pub fn payload_kind(&self) -> ReplayMarketPayloadKind {
        self.payload.kind()
    }
}

impl ReplayMarketSource {
    #[must_use]
    pub fn new(mut events: Vec<ReplayMarketEvent>) -> Self {
        events.sort_by_key(|event| (event.event_time_ns(), event.received_at_ns));
        Self { events, index: 0 }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len().saturating_sub(self.index)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Symbols present in the remaining replay events.
    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        self.events[self.index..]
            .iter()
            .map(ReplayMarketEvent::symbol)
    }
}

impl Iterator for ReplayMarketSource {
    type Item = ReplayMarketEvent;

    fn next(&mut self) -> Option<Self::Item> {
        let event = self.events.get(self.index)?.clone();
        self.index += 1;
        Some(event)
    }
}

impl StrategyReplayContext<'_> {
    #[must_use]
    pub fn update(&self) -> StrategyUpdate {
        self.context.update()
    }

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

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder {
        self.context.target_pos(account_id, symbol)
    }

    #[must_use]
    pub fn risk(&self) -> Option<&crate::RiskEngine> {
        self.context.risk()
    }

    pub async fn finish_test_step(&mut self) -> Result<StrategyTestReport> {
        self.context.finish_test_step().await
    }
}

fn ingest_replay_market_event(
    host: &TaskHost,
    event: &ReplayMarketEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    let body = match &event.payload {
        ReplayMarketPayload::Quote(quote) => {
            quote_update(&event.symbol, quote, event.underlying_symbol())
        }
        ReplayMarketPayload::Kline { duration_ns, row } => kline_update(
            &event.symbol,
            *duration_ns,
            row,
            klines,
            event.underlying_symbol(),
        ),
        ReplayMarketPayload::Tick(tick) => {
            tick_update(&event.symbol, tick, ticks, event.underlying_symbol())
        }
    };

    host.api().session().handle().ingest(
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
        host.api().session().handle().ingest(
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
        host.api().session().handle().ingest(
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

fn quote_update(symbol: &str, quote: &Quote, underlying_symbol: Option<&str>) -> Value {
    let mut quote_value = Map::new();
    insert_string_if_present(&mut quote_value, "datetime", &quote.datetime);
    insert_f64_if_finite(&mut quote_value, "last_price", quote.last_price);
    insert_f64_if_finite(&mut quote_value, "ask_price1", quote.ask_price1);
    insert_i64_if_nonzero(&mut quote_value, "ask_volume1", quote.ask_volume1);
    insert_f64_if_finite(&mut quote_value, "bid_price1", quote.bid_price1);
    insert_i64_if_nonzero(&mut quote_value, "bid_volume1", quote.bid_volume1);
    insert_string_if_present(
        &mut quote_value,
        "underlying_symbol",
        effective_underlying_symbol(quote, underlying_symbol),
    );

    json!({
        "quotes": {
            symbol: Value::Object(quote_value)
        }
    })
}

fn kline_update(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    klines: &[ReplayKlineSpec],
    underlying_symbol: Option<&str>,
) -> Value {
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

    let mut body = json!({
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
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
}

fn tick_update(
    symbol: &str,
    tick: &Tick,
    ticks: &[ReplayTickSpec],
    underlying_symbol: Option<&str>,
) -> Value {
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

    let mut body = json!({
        "charts": Value::Object(charts),
        "ticks": {
            symbol: {
                "data": {
                    row_id: tick_value(tick)
                }
            }
        }
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
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

fn effective_underlying_symbol<'a>(
    quote: &'a Quote,
    event_underlying_symbol: Option<&'a str>,
) -> &'a str {
    if !quote.underlying_symbol.is_empty() {
        &quote.underlying_symbol
    } else {
        event_underlying_symbol.unwrap_or("")
    }
}

fn insert_quote_underlying_update(body: &mut Value, symbol: &str, underlying_symbol: Option<&str>) {
    let Some(underlying_symbol) = underlying_symbol else {
        return;
    };
    if underlying_symbol.is_empty() {
        return;
    }
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert(
        "quotes".to_string(),
        json!({
            symbol: {
                "underlying_symbol": underlying_symbol
            }
        }),
    );
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
    let symbol = sanitize_chart_token(symbol);
    format!("wait-kline-{symbol}-{duration_ns}-{view_width}")
}

fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    let symbol = sanitize_chart_token(symbol);
    format!("wait-tick-{symbol}-{view_width}")
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

fn checkpoint_tmp_path(path: &Path) -> PathBuf {
    let mut extension = path.extension().map_or_else(OsString::new, OsString::from);
    if !extension.is_empty() {
        extension.push(".");
    }
    extension.push("tmp");
    path.with_extension(extension)
}

fn checkpoint_io_error(operation: &'static str, path: &Path, error: io::Error) -> TaskError {
    TaskError::CheckpointIo {
        operation,
        path: path.display().to_string(),
        reason: error.to_string(),
    }
}

fn invalid_checkpoint(reason: impl Into<String>) -> TaskError {
    TaskError::InvalidCheckpoint {
        reason: reason.into(),
    }
}
