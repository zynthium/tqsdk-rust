#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{Account, Position, Quote};
use tqsdk_wait::{KlineWindow, TickWindow};

use crate::replay::{
    StrategyReplay, StrategyReplayBuilder, StrategyReplayCheckpoint, StrategyReplayContext,
    StrategyReplayEvent,
};
use crate::risk::RiskEngine;
use crate::strategy::{StrategyContext, StrategyHostBuilder, StrategyUpdate};
use crate::target_pos::TargetPosBuilder;
use crate::testing::{BuiltStrategyTestHarness, StrategyTestReport};
use crate::{Result, StrategyHost, TaskError, TaskHost, TaskOrderBuilder};

/// Strategy runtime adapter that hides live/test/replay construction differences.
pub enum StrategyEnvironment {
    TaskHost(StrategyHost),
    Replay(StrategyReplay),
}

/// Builder for a [`StrategyEnvironment`].
pub struct StrategyEnvironmentBuilder {
    source: StrategyEnvironmentSource,
    subscriptions: StrategyEnvironmentSubscriptions,
}

/// Source kind backing a [`StrategyEnvironment`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyEnvironmentKind {
    TaskHost,
    Replay,
}

/// Stable strategy context over task-host and replay-backed environments.
pub enum StrategyEnvironmentContext<'a> {
    TaskHost(StrategyContext<'a>),
    Replay(StrategyReplayContext<'a>),
}

/// Market/trade objects a strategy environment should prepare before running.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StrategyEnvironmentSubscriptions {
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<StrategyEnvironmentKlineSubscription>,
    ticks: Vec<StrategyEnvironmentTickSubscription>,
}

/// Kline serial requested by a strategy environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyEnvironmentKlineSubscription {
    symbol: String,
    duration: Duration,
    view_width: usize,
}

/// Tick serial requested by a strategy environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyEnvironmentTickSubscription {
    symbol: String,
    view_width: usize,
}

enum StrategyEnvironmentSource {
    TaskHost(Box<TaskHost>),
    Replay(Box<StrategyReplayBuilder>),
}

impl StrategyEnvironment {
    #[must_use]
    pub fn from_task_host(host: TaskHost) -> StrategyEnvironmentBuilder {
        StrategyEnvironmentBuilder::new(StrategyEnvironmentSource::TaskHost(Box::new(host)))
    }

    #[must_use]
    pub fn from_test_harness(harness: BuiltStrategyTestHarness) -> StrategyEnvironmentBuilder {
        Self::from_task_host(harness.into_task_host())
    }

    #[must_use]
    pub fn from_replay_builder(builder: StrategyReplayBuilder) -> StrategyEnvironmentBuilder {
        StrategyEnvironmentBuilder::new(StrategyEnvironmentSource::Replay(Box::new(builder)))
    }

    #[must_use]
    pub fn kind(&self) -> StrategyEnvironmentKind {
        match self {
            Self::TaskHost(_) => StrategyEnvironmentKind::TaskHost,
            Self::Replay(_) => StrategyEnvironmentKind::Replay,
        }
    }

    pub async fn next(&mut self) -> Result<Option<StrategyEnvironmentContext<'_>>> {
        match self {
            Self::TaskHost(strategy) => strategy
                .next(None)
                .await
                .map(|context| context.map(StrategyEnvironmentContext::TaskHost)),
            Self::Replay(replay) => replay
                .next()
                .await
                .map(|context| context.map(StrategyEnvironmentContext::Replay)),
        }
    }

    pub async fn next_once(&mut self) -> Result<StrategyEnvironmentContext<'_>> {
        self.next()
            .await?
            .ok_or(TaskError::InvalidState("strategy environment closed"))
    }
}

impl StrategyEnvironmentBuilder {
    fn new(source: StrategyEnvironmentSource) -> Self {
        Self {
            source,
            subscriptions: StrategyEnvironmentSubscriptions::new(),
        }
    }

    #[must_use]
    pub fn account(mut self, account_id: impl AsRef<str>) -> Self {
        self.subscriptions = self.subscriptions.account(account_id);
        self
    }

    #[must_use]
    pub fn quote(mut self, symbol: impl AsRef<str>) -> Self {
        self.subscriptions = self.subscriptions.quote(symbol);
        self
    }

    #[must_use]
    pub fn kline(mut self, symbol: impl AsRef<str>, duration: Duration, view_width: usize) -> Self {
        self.subscriptions = self.subscriptions.kline(symbol, duration, view_width);
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        self.subscriptions = self.subscriptions.tick(symbol, view_width);
        self
    }

    #[must_use]
    pub fn subscriptions(mut self, subscriptions: StrategyEnvironmentSubscriptions) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    pub async fn build(self) -> Result<StrategyEnvironment> {
        match self.source {
            StrategyEnvironmentSource::TaskHost(host) => {
                let mut builder = StrategyHostBuilder::new(*host);
                for account in &self.subscriptions.accounts {
                    builder = builder.account(account);
                }
                for quote in &self.subscriptions.quotes {
                    builder = builder.quote(quote);
                }
                for spec in &self.subscriptions.klines {
                    builder = builder.kline(&spec.symbol, spec.duration, spec.view_width);
                }
                for spec in &self.subscriptions.ticks {
                    builder = builder.tick(&spec.symbol, spec.view_width);
                }
                Ok(StrategyEnvironment::TaskHost(builder.build().await?))
            }
            StrategyEnvironmentSource::Replay(builder) => {
                let mut builder = *builder;
                for account in &self.subscriptions.accounts {
                    builder = builder.account(account);
                }
                for quote in &self.subscriptions.quotes {
                    builder = builder.quote(quote);
                }
                for spec in &self.subscriptions.klines {
                    builder = builder.kline(&spec.symbol, spec.duration, spec.view_width);
                }
                for spec in &self.subscriptions.ticks {
                    builder = builder.tick(&spec.symbol, spec.view_width);
                }
                Ok(StrategyEnvironment::Replay(builder.build().await?))
            }
        }
    }
}

impl StrategyEnvironmentSubscriptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        let spec = StrategyEnvironmentKlineSubscription {
            symbol: symbol.as_ref().to_owned(),
            duration,
            view_width,
        };
        if !self.klines.iter().any(|existing| existing == &spec) {
            self.klines.push(spec);
        }
        self
    }

    #[must_use]
    pub fn tick(mut self, symbol: impl AsRef<str>, view_width: usize) -> Self {
        let spec = StrategyEnvironmentTickSubscription {
            symbol: symbol.as_ref().to_owned(),
            view_width,
        };
        if !self.ticks.iter().any(|existing| existing == &spec) {
            self.ticks.push(spec);
        }
        self
    }

    #[must_use]
    pub fn account_ids(&self) -> &[String] {
        &self.accounts
    }

    #[must_use]
    pub fn quote_symbols(&self) -> &[String] {
        &self.quotes
    }

    #[must_use]
    pub fn klines(&self) -> &[StrategyEnvironmentKlineSubscription] {
        &self.klines
    }

    #[must_use]
    pub fn ticks(&self) -> &[StrategyEnvironmentTickSubscription] {
        &self.ticks
    }
}

impl StrategyEnvironmentKlineSubscription {
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }
}

impl StrategyEnvironmentTickSubscription {
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }
}

impl StrategyEnvironmentContext<'_> {
    #[must_use]
    pub fn kind(&self) -> StrategyEnvironmentKind {
        match self {
            Self::TaskHost(_) => StrategyEnvironmentKind::TaskHost,
            Self::Replay(_) => StrategyEnvironmentKind::Replay,
        }
    }

    #[must_use]
    pub fn update(&self) -> StrategyUpdate {
        match self {
            Self::TaskHost(context) => context.update(),
            Self::Replay(context) => context.update(),
        }
    }

    #[must_use]
    pub fn replay_event(&self) -> Option<&StrategyReplayEvent> {
        match self {
            Self::TaskHost(_) => None,
            Self::Replay(context) => Some(context.event()),
        }
    }

    #[must_use]
    pub fn replay_time_ns(&self) -> Option<i64> {
        match self {
            Self::TaskHost(_) => None,
            Self::Replay(context) => Some(context.replay_time_ns()),
        }
    }

    #[must_use]
    pub fn replay_checkpoint(&self) -> Option<StrategyReplayCheckpoint> {
        match self {
            Self::TaskHost(_) => None,
            Self::Replay(context) => Some(context.checkpoint()),
        }
    }

    pub fn quote(&self, symbol: impl AsRef<str>) -> Result<Quote> {
        match self {
            Self::TaskHost(context) => context.quote(symbol),
            Self::Replay(context) => context.quote(symbol),
        }
    }

    pub fn kline(&self, symbol: impl AsRef<str>, duration: Duration) -> Result<KlineWindow> {
        match self {
            Self::TaskHost(context) => context.kline(symbol, duration),
            Self::Replay(context) => context.kline(symbol, duration),
        }
    }

    pub fn tick(&self, symbol: impl AsRef<str>) -> Result<TickWindow> {
        match self {
            Self::TaskHost(context) => context.tick(symbol),
            Self::Replay(context) => context.tick(symbol),
        }
    }

    pub fn account(&self, account_id: impl AsRef<str>) -> Result<Account> {
        match self {
            Self::TaskHost(context) => context.account(account_id),
            Self::Replay(context) => context.account(account_id),
        }
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<Position> {
        match self {
            Self::TaskHost(context) => context.position(account_id, symbol),
            Self::Replay(context) => context.position(account_id, symbol),
        }
    }

    #[must_use]
    pub fn orders(&mut self, account_id: impl AsRef<str>) -> TaskOrderBuilder<'_> {
        match self {
            Self::TaskHost(context) => context.orders(account_id),
            Self::Replay(context) => context.orders(account_id),
        }
    }

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder {
        match self {
            Self::TaskHost(context) => context.target_pos(account_id, symbol),
            Self::Replay(context) => context.target_pos(account_id, symbol),
        }
    }

    #[must_use]
    pub fn risk(&self) -> Option<&RiskEngine> {
        match self {
            Self::TaskHost(context) => context.risk(),
            Self::Replay(context) => context.risk(),
        }
    }

    pub async fn finish_test_step(&mut self) -> Result<StrategyTestReport> {
        match self {
            Self::TaskHost(context) => context.finish_test_step().await,
            Self::Replay(context) => context.finish_test_step().await,
        }
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}
