#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;

use tqsdk_core::{MarketChartCommand, MarketCommand, RuntimeCommand, Symbol};
use tqsdk_session::SessionClient;

use crate::change::{matches_any, matches_fields, ChangeTrackedRef};
use crate::driver::{WaitDriver, WaitGuard};
use crate::refs::{KlineSerialRef, QuoteRef, TickSerialRef, TradingStatusRef};

pub struct TqApi {
    pub(crate) driver: WaitDriver,
}

impl TqApi {
    pub fn new(session: SessionClient) -> Self {
        let handle = session.handle().clone();
        Self::new_for_test(handle, session)
    }

    #[doc(hidden)]
    pub fn new_for_test(handle: tqsdk_core::RuntimeHandle, session: SessionClient) -> Self {
        let reader = handle.reader();
        let cursor = reader.cursor();
        let runtime = session.runtime_clone();

        Self {
            driver: WaitDriver {
                session,
                reader,
                cursor,
                runtime,
                deferred_commits: VecDeque::new(),
                last_commit: None,
                waiting: AtomicBool::new(false),
                next_order_seq: AtomicU64::new(1),
            },
        }
    }

    pub async fn wait_update(
        &mut self,
        _deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<bool> {
        let _guard = WaitGuard::new(&self.driver.waiting)?;

        if let Some(commit) = self.driver.deferred_commits.pop_front() {
            self.driver.last_commit = Some(commit);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn last_commit(&self) -> Option<&tqsdk_core::CommitResult> {
        self.driver.last_commit.as_ref()
    }

    pub fn quote_ref(&self, symbol: &str) -> QuoteRef {
        QuoteRef::new(symbol)
    }

    pub async fn get_quote(&mut self, symbol: &str) -> crate::error::Result<QuoteRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(self.quote_ref(symbol))
    }

    pub async fn get_trading_status(
        &mut self,
        symbol: &str,
    ) -> crate::error::Result<TradingStatusRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(
                MarketCommand::SubscribeTradingStatus {
                    symbols: vec![Symbol::new(symbol)],
                },
            ))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        Ok(TradingStatusRef::new(symbol))
    }

    pub async fn get_kline_serial(
        &mut self,
        symbol: &str,
        duration: Duration,
        data_length: usize,
    ) -> crate::error::Result<KlineSerialRef> {
        let duration_ns =
            (duration.as_secs() as i64) * 1_000_000_000 + i64::from(duration.subsec_nanos());
        let chart_id = format!("wait-kline-{symbol}-{duration_ns}-{data_length}");

        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SetChart(
                MarketChartCommand {
                    chart_id: chart_id.clone(),
                    symbols: vec![Symbol::new(symbol)],
                    duration_ns,
                    view_width: data_length,
                    left_kline_id: None,
                    focus_datetime_ns: None,
                    focus_position: None,
                },
            )))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        let serial = KlineSerialRef {
            symbol: symbol.to_string(),
            duration_ns,
            view_width: data_length,
            chart_id,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api))
            .await?;

        Ok(serial)
    }

    pub async fn get_tick_serial(
        &mut self,
        symbol: &str,
        data_length: usize,
    ) -> crate::error::Result<TickSerialRef> {
        self.driver
            .session
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new(symbol)],
            }))
            .await
            .map_err(crate::error::WaitFacadeError::Session)?;

        let serial = TickSerialRef {
            symbol: symbol.to_string(),
            view_width: data_length,
        };
        self.wait_until_ready_for_test(|api| serial.is_ready(api))
            .await?;

        Ok(serial)
    }

    pub fn is_changing(&self, target: &impl ChangeTrackedRef) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_any(&commit.changes, target)))
    }

    pub fn is_changing_fields(
        &self,
        target: &impl ChangeTrackedRef,
        fields: &[&str],
    ) -> crate::error::Result<bool> {
        Ok(self
            .driver
            .last_commit
            .as_ref()
            .is_some_and(|commit| matches_fields(&commit.changes, target, fields)))
    }

    async fn wait_until_ready_for_test<F>(&mut self, mut ready: F) -> crate::error::Result<()>
    where
        F: FnMut(&Self) -> crate::error::Result<bool>,
    {
        if ready(self)? {
            return Ok(());
        }

        let mut replay = Vec::new();

        while !ready(self)? {
            if !self.wait_update(None).await? {
                for commit in replay.into_iter().rev() {
                    self.driver.deferred_commits.push_front(commit);
                }
                return Err(crate::error::WaitFacadeError::InvalidState(
                    "object not ready",
                ));
            }

            if let Some(commit) = self.driver.last_commit.clone() {
                replay.push(commit);
            }
        }

        for commit in replay.into_iter().rev() {
            self.driver.deferred_commits.push_front(commit);
        }

        Ok(())
    }

    #[doc(hidden)]
    pub fn begin_wait_for_test(&self) -> crate::error::Result<WaitGuard<'_>> {
        self.driver.begin_wait()
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.driver.runtime.handle()
    }

    #[doc(hidden)]
    pub fn push_deferred_commit_for_test(&mut self, commit: tqsdk_core::CommitResult) {
        self.driver.deferred_commits.push_back(commit);
    }
}
