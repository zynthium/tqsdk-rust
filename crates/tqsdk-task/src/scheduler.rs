#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::watch;

use crate::Result;
use crate::config::{OffsetPriority, PriceMode, TargetPosSchedulerConfig, VolumeSplitPolicy};
use crate::registry::TaskId;
use crate::shared::{
    SharedQuoteSubscriptions, SharedTargetPosSchedulerStore, SharedTargetPosStore,
    SharedTaskRegistry, SharedTradingCalendar, TaskStateCell,
};
use crate::target_pos::{TargetPosTaskExecutionEvent, TargetPosTaskTradeFill};

mod planner;
mod runner;
mod state;

#[cfg(test)]
use chrono::NaiveDate;
#[cfg(test)]
use planner::{china_tz, effective_step_elapsed, trading_time_elapsed_between};
pub(crate) use runner::process_schedulers_wait_update;
pub(crate) use state::TargetPosSchedulerStore;
#[cfg(test)]
use tqsdk_core::{Quote, TradingTime};

#[cfg(test)]
use crate::calendar::TradingDayCalendar;
#[cfg(test)]
use crate::target_pos::TargetPosBuilder;
use state::TargetPosSchedulerState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPosScheduleStep {
    pub interval: Duration,
    pub target_volume: i64,
    pub price_mode: Option<PriceMode>,
}

impl TargetPosScheduleStep {
    #[must_use]
    pub fn target(interval: Duration, target_volume: i64, price_mode: PriceMode) -> Self {
        Self {
            interval,
            target_volume,
            price_mode: Some(price_mode),
        }
    }

    #[must_use]
    pub fn pause(interval: Duration) -> Self {
        Self {
            interval,
            target_volume: 0,
            price_mode: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPosExecutionStep {
    pub step_index: usize,
    pub target_volume: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TargetPosStepOutcomeReport {
    pub step_index: usize,
    pub target_volume: i64,
    pub submitted_order_count: usize,
    pub cancel_request_count: usize,
    pub finished_order_count: usize,
    pub filled_volume: i64,
    pub filled_turnover: f64,
    pub trade_count: usize,
    pub target_reached: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TargetPosExecutionReport {
    pub applied_steps: Vec<TargetPosExecutionStep>,
    pub step_outcomes: Vec<TargetPosStepOutcomeReport>,
    pub trades: Vec<TargetPosSchedulerTradeFill>,
    pub submitted_order_count: usize,
    pub cancel_request_count: usize,
    pub finished_order_count: usize,
    pub filled_volume: i64,
    pub filled_turnover: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosSchedulerExecutionEvent {
    pub step_index: usize,
    pub event: TargetPosTaskExecutionEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosSchedulerTradeFill {
    pub step_index: usize,
    pub trade: TargetPosTaskTradeFill,
}

pub struct TargetPosSchedulerBuilder {
    registry: SharedTaskRegistry,
    target_tasks: SharedTargetPosStore,
    store: SharedTargetPosSchedulerStore,
    quote_subscriptions: SharedQuoteSubscriptions,
    trading_calendar: SharedTradingCalendar,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
}

#[derive(Clone)]
pub struct TargetPosScheduler {
    inner: Arc<TargetPosSchedulerInner>,
}

struct TargetPosSchedulerInner {
    registry: SharedTaskRegistry,
    target_tasks: SharedTargetPosStore,
    quote_subscriptions: SharedQuoteSubscriptions,
    trading_calendar: SharedTradingCalendar,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
    state: TaskStateCell<TargetPosSchedulerState>,
    finished_tx: watch::Sender<bool>,
    cancel_requested: AtomicBool,
    finished: AtomicBool,
}

impl TargetPosSchedulerBuilder {
    pub(crate) fn new(
        registry: SharedTaskRegistry,
        target_tasks: SharedTargetPosStore,
        store: SharedTargetPosSchedulerStore,
        quote_subscriptions: SharedQuoteSubscriptions,
        trading_calendar: SharedTradingCalendar,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            target_tasks,
            store,
            quote_subscriptions,
            trading_calendar,
            account_id,
            symbol,
            steps: Vec::new(),
            config: TargetPosSchedulerConfig::default(),
        }
    }

    pub fn steps(mut self, steps: Vec<TargetPosScheduleStep>) -> Self {
        self.steps = steps;
        self
    }

    pub fn offset_priority(mut self, priority: OffsetPriority) -> Self {
        self.config.set_offset_priority(priority);
        self
    }

    pub fn split_policy(mut self, policy: VolumeSplitPolicy) -> Self {
        self.config.set_split_policy(policy);
        self
    }

    pub fn build(self) -> Result<TargetPosScheduler> {
        let task = self
            .registry
            .with_mut(|registry| registry.register_scheduler(&self.account_id, &self.symbol))?;
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosSchedulerInner {
            registry: self.registry.clone(),
            target_tasks: self.target_tasks.clone(),
            quote_subscriptions: self.quote_subscriptions.clone(),
            trading_calendar: self.trading_calendar.clone(),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            steps: self.steps,
            config: self.config,
            state: TaskStateCell::default(),
            finished_tx,
            cancel_requested: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });

        if inner.steps.is_empty() {
            inner.finish();
            return Ok(TargetPosScheduler { inner });
        }

        self.store
            .with_mut(|store| store.register(Arc::clone(&inner)));

        Ok(TargetPosScheduler { inner })
    }
}

impl TargetPosScheduler {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[must_use]
    pub fn config(&self) -> &TargetPosSchedulerConfig {
        &self.inner.config
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.finished.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn execution_report(&self) -> TargetPosExecutionReport {
        self.inner.with_state(|state| state.report.clone())
    }

    #[must_use]
    pub fn execution_events(&self) -> Vec<TargetPosSchedulerExecutionEvent> {
        self.inner.with_state(|state| state.events.clone())
    }

    #[must_use]
    pub fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerExecutionEvent>) {
        self.inner.with_state(|state| {
            let end = state.events.len();
            let start = start.min(end);
            (end, state.events[start..].to_vec())
        })
    }

    #[must_use]
    pub fn execution_trades_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerTradeFill>) {
        self.inner.with_state(|state| {
            let end = state.report.trades.len();
            let start = start.min(end);
            (end, state.report.trades[start..].to_vec())
        })
    }

    #[must_use]
    pub fn last_error(&self) -> Option<crate::TaskError> {
        self.inner.with_state(|state| state.last_error.clone())
    }

    pub async fn cancel(&self) -> Result<()> {
        if self.is_finished() {
            return Ok(());
        }
        self.inner.cancel_requested.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub async fn wait_finished(&self) -> Result<()> {
        if self.is_finished() {
            return self.inner.failure_result();
        }

        let mut finished_rx = self.inner.finished_tx.subscribe();
        loop {
            if *finished_rx.borrow() {
                return self.inner.failure_result();
            }

            finished_rx.changed().await.map_err(|_| {
                crate::TaskError::InvalidState("target position scheduler finished channel closed")
            })?;
        }
    }
}

impl Drop for TargetPosSchedulerInner {
    fn drop(&mut self) {
        let active_task = self.state.get_mut().active_task.take();
        if let Some(task) = active_task {
            task.cancel_internal();
        }

        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .with_mut(|registry| registry.unregister_task(self.task_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tqsdk_core::adapter::MarketAdapter;
    use tqsdk_core::{AdapterRegistry, RuntimeHandle};
    use tqsdk_session::testing::ManualSession;
    use tqsdk_wait::TqApi;

    fn market_only_api() -> TqApi {
        let mut adapters = AdapterRegistry::new();
        adapters.register_adapter(MarketAdapter::default());
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = ManualSession::from_runtime(handle).into_client();
        TqApi::new(session)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_scheduler_records_error_when_internal_cancel_submission_fails() {
        let registry = SharedTaskRegistry::default();
        let target_tasks = SharedTargetPosStore::default();
        let schedulers = SharedTargetPosSchedulerStore::default();
        let quote_subscriptions = SharedQuoteSubscriptions::default();
        let trading_calendar = SharedTradingCalendar::default();
        let scheduler = TargetPosSchedulerBuilder::new(
            registry.clone(),
            target_tasks.clone(),
            schedulers,
            quote_subscriptions.clone(),
            trading_calendar,
            "sim".to_string(),
            "SHFE.rb2601".to_string(),
        )
        .steps(vec![TargetPosScheduleStep::target(
            Duration::from_secs(60),
            1,
            PriceMode::Active,
        )])
        .build()
        .expect("scheduler should build");
        let task = TargetPosBuilder::new(
            registry,
            target_tasks,
            quote_subscriptions,
            "sim".to_string(),
            "SHFE.rb2601".to_string(),
        )
        .build_internal()
        .expect("internal target task should build");
        let mut api = market_only_api();
        task.track_order_for_test(api.order("sim", "unit-order-1"));
        scheduler.inner.with_state_mut(|state| {
            state.active_task = Some(task);
        });
        scheduler
            .inner
            .cancel_requested
            .store(true, Ordering::SeqCst);

        scheduler.inner.process_wait_update(&mut api).await;

        assert!(scheduler.is_finished());
        assert!(matches!(
            scheduler.last_error(),
            Some(crate::TaskError::Wait(_))
        ));
    }

    #[test]
    fn trading_time_elapsed_counts_only_open_day_windows() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 22, 11, 25, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 13, 35, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![
                vec!["09:00:00".to_string(), "10:15:00".to_string()],
                vec!["10:30:00".to_string(), "11:30:00".to_string()],
                vec!["13:30:00".to_string(), "15:00:00".to_string()],
            ],
            night: Vec::new(),
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("day windows should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(10 * 60));
    }

    #[test]
    fn trading_time_elapsed_skips_weekend_and_closed_hours() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 24, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 27, 9, 5, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![vec!["09:00:00".to_string(), "15:00:00".to_string()]],
            night: vec![vec!["21:00:00".to_string(), "23:00:00".to_string()]],
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("day/night windows should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(5 * 60));
    }

    #[test]
    fn trading_time_elapsed_uses_calendar_closed_days_when_available() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 30, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 5, 1, 9, 5, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![vec!["09:00:00".to_string(), "15:00:00".to_string()]],
            night: vec![vec!["21:00:00".to_string(), "23:00:00".to_string()]],
        };
        let calendar = TradingDayCalendar::from_entries([
            (
                NaiveDate::from_ymd_opt(2026, 4, 30).expect("valid date"),
                true,
            ),
            (
                NaiveDate::from_ymd_opt(2026, 5, 1).expect("valid date"),
                false,
            ),
        ]);

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, Some(&calendar))
            .expect("day/night windows should produce elapsed");

        assert_eq!(elapsed, Duration::ZERO);
    }

    #[test]
    fn trading_time_elapsed_supports_cross_midnight_night_window() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 21, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 0, 35, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: Vec::new(),
            night: vec![vec!["21:00:00".to_string(), "01:00:00".to_string()]],
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("cross-midnight night window should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(100 * 60));
    }

    #[test]
    fn effective_step_elapsed_falls_back_to_wall_clock_without_valid_schedule() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 22, 11, 25, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 13, 35, 0)
            .single()
            .expect("valid datetime");
        let quote = Quote {
            trading_time: TradingTime::default(),
            ..Quote::default()
        };

        assert_eq!(
            effective_step_elapsed(start, end, Some(&quote), None),
            Duration::from_secs(130 * 60)
        );
    }
}
