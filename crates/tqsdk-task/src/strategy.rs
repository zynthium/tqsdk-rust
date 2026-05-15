#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{Account, Position, Quote};
use tqsdk_wait::{KlineHandle, KlineWindow, QuoteRef, TickHandle, TickWindow};

use crate::order::TaskOrderBuilder;
use crate::risk::RiskEngine;
use crate::target_pos::TargetPosBuilder;
use crate::testing::StrategyTestReport;
use crate::{Result, TaskError, TaskHost};

/// Builder for a single-owner strategy host.
pub struct StrategyHostBuilder {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
    klines: Vec<StrategyKlineSpec>,
    ticks: Vec<StrategyTickSpec>,
}

/// Single-owner strategy runtime built on [`TaskHost`].
pub struct StrategyHost {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
    quote_handles: Vec<StrategyQuoteHandle>,
    klines: Vec<StrategyKlineHandle>,
    ticks: Vec<StrategyTickHandle>,
}

/// Summary of one strategy update step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyUpdate {
    updated: bool,
}

/// Stable strategy context for one host-driven update step.
pub struct StrategyContext<'a> {
    host: &'a mut TaskHost,
    update: StrategyUpdate,
    quotes: &'a [StrategyQuoteHandle],
    klines: &'a [StrategyKlineHandle],
    ticks: &'a [StrategyTickHandle],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrategyKlineSpec {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrategyTickSpec {
    symbol: String,
    view_width: usize,
}

struct StrategyQuoteHandle {
    symbol: String,
    quote: QuoteRef,
}

struct StrategyKlineHandle {
    spec: StrategyKlineSpec,
    serial: KlineHandle,
}

struct StrategyTickHandle {
    spec: StrategyTickSpec,
    serial: TickHandle,
}

impl StrategyHost {
    #[must_use]
    pub fn builder(host: TaskHost) -> StrategyHostBuilder {
        StrategyHostBuilder::new(host)
    }

    pub async fn next(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Option<StrategyContext<'_>>> {
        let updated = self.host.wait_update(deadline).await?;
        Ok(Some(StrategyContext {
            host: &mut self.host,
            update: StrategyUpdate { updated },
            quotes: &self.quote_handles,
            klines: &self.klines,
            ticks: &self.ticks,
        }))
    }

    pub async fn next_once(&mut self) -> Result<StrategyContext<'_>> {
        self.next(None)
            .await?
            .ok_or(TaskError::InvalidState("strategy host closed"))
    }

    #[must_use]
    pub fn accounts(&self) -> &[String] {
        &self.accounts
    }

    #[must_use]
    pub fn quotes(&self) -> &[String] {
        &self.quotes
    }

    #[must_use]
    pub fn task_host(&self) -> &TaskHost {
        &self.host
    }

    #[must_use]
    pub fn task_host_mut(&mut self) -> &mut TaskHost {
        &mut self.host
    }

    #[must_use]
    pub fn into_task_host(self) -> TaskHost {
        self.host
    }
}

impl StrategyHostBuilder {
    #[must_use]
    pub fn new(host: TaskHost) -> Self {
        Self {
            host,
            accounts: Vec::new(),
            quotes: Vec::new(),
            klines: Vec::new(),
            ticks: Vec::new(),
        }
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
        let spec = StrategyKlineSpec {
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
        let spec = StrategyTickSpec {
            symbol: symbol.as_ref().to_owned(),
            view_width,
        };
        if !self.ticks.iter().any(|existing| existing == &spec) {
            self.ticks.push(spec);
        }
        self
    }

    pub async fn build(mut self) -> Result<StrategyHost> {
        let mut quote_handles = Vec::new();
        for symbol in &self.quotes {
            let quote = self.host.api_mut().quote(symbol).await?;
            quote_handles.push(StrategyQuoteHandle {
                symbol: symbol.clone(),
                quote,
            });
        }

        let mut kline_handles = Vec::new();
        for spec in &self.klines {
            let serial = self
                .host
                .api_mut()
                .kline(
                    &spec.symbol,
                    Duration::from_nanos(spec.duration_ns as u64),
                    spec.view_width,
                )
                .await?;
            kline_handles.push(StrategyKlineHandle {
                spec: spec.clone(),
                serial,
            });
        }

        let mut tick_handles = Vec::new();
        for spec in &self.ticks {
            let serial = self
                .host
                .api_mut()
                .tick(&spec.symbol, spec.view_width)
                .await?;
            tick_handles.push(StrategyTickHandle {
                spec: spec.clone(),
                serial,
            });
        }

        Ok(StrategyHost {
            host: self.host,
            accounts: self.accounts,
            quotes: self.quotes,
            quote_handles,
            klines: kline_handles,
            ticks: tick_handles,
        })
    }
}

impl StrategyUpdate {
    #[must_use]
    pub fn updated(&self) -> bool {
        self.updated
    }
}

impl StrategyContext<'_> {
    #[must_use]
    pub fn update(&self) -> StrategyUpdate {
        self.update
    }

    pub fn quote(&self, symbol: impl AsRef<str>) -> Result<Quote> {
        let symbol = symbol.as_ref();
        let Some(handle) = self.quotes.iter().find(|handle| handle.symbol == symbol) else {
            return Err(TaskError::InvalidState("strategy quote is not configured"));
        };
        handle.quote.load().map_err(Into::into)
    }

    pub fn kline(&self, symbol: impl AsRef<str>, duration: Duration) -> Result<KlineWindow> {
        let duration_ns = duration_to_ns(duration);
        let symbol = symbol.as_ref();
        let Some(handle) = self
            .klines
            .iter()
            .find(|handle| handle.spec.symbol == symbol && handle.spec.duration_ns == duration_ns)
        else {
            return Err(TaskError::InvalidState(
                "strategy kline serial is not configured",
            ));
        };
        handle.serial.window().map_err(Into::into)
    }

    pub fn tick(&self, symbol: impl AsRef<str>) -> Result<TickWindow> {
        let symbol = symbol.as_ref();
        let Some(handle) = self
            .ticks
            .iter()
            .find(|handle| handle.spec.symbol == symbol)
        else {
            return Err(TaskError::InvalidState(
                "strategy tick serial is not configured",
            ));
        };
        handle.serial.window().map_err(Into::into)
    }

    pub fn account(&self, account_id: impl AsRef<str>) -> Result<Account> {
        self.host
            .api()
            .account(account_id.as_ref())
            .load()
            .map_err(Into::into)
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<Position> {
        self.host
            .api()
            .position(account_id.as_ref(), symbol.as_ref())
            .load()
            .map_err(Into::into)
    }

    #[must_use]
    pub fn orders(&mut self, account_id: impl AsRef<str>) -> TaskOrderBuilder<'_> {
        self.host.orders(account_id)
    }

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder {
        self.host.target_pos(account_id, symbol)
    }

    #[must_use]
    pub fn risk(&self) -> Option<&RiskEngine> {
        self.host.risk()
    }

    #[must_use]
    pub fn task_host(&self) -> &TaskHost {
        self.host
    }

    #[must_use]
    pub fn task_host_mut(&mut self) -> &mut TaskHost {
        self.host
    }

    pub async fn finish_test_step(&mut self) -> Result<StrategyTestReport> {
        crate::testing::finish_test_step(self.host).await
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

fn duration_to_ns(duration: Duration) -> i64 {
    (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos())
}
