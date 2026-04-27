#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{Account, Position, Quote};

use crate::order::TaskOrderBuilder;
use crate::risk::RiskEngine;
use crate::testing::StrategyTestReport;
use crate::target_pos::TargetPosBuilder;
use crate::{Result, TaskError, TaskHost};

/// Builder for a single-owner strategy host.
pub struct StrategyHostBuilder {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
}

/// Single-owner strategy runtime built on [`TaskHost`].
pub struct StrategyHost {
    host: TaskHost,
    accounts: Vec<String>,
    quotes: Vec<String>,
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

    pub async fn build(mut self) -> Result<StrategyHost> {
        for symbol in &self.quotes {
            self.host.api_mut().get_quote(symbol).await?;
        }
        Ok(StrategyHost {
            host: self.host,
            accounts: self.accounts,
            quotes: self.quotes,
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
        self.host
            .api()
            .quote_ref(symbol.as_ref())
            .load(self.host.api())
            .map_err(Into::into)
    }

    pub fn account(&self, account_id: impl AsRef<str>) -> Result<Account> {
        self.host
            .api()
            .get_account(account_id.as_ref())
            .load(self.host.api())
            .map_err(Into::into)
    }

    pub fn position(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<Position> {
        self.host
            .api()
            .get_position(account_id.as_ref(), symbol.as_ref())
            .load(self.host.api())
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
