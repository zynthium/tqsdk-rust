#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::Result;
use crate::config::{OffsetPriority, PriceMode, TargetPosSchedulerConfig, VolumeSplitPolicy};
use crate::registry::{TaskId, TaskRegistry};
use crate::target_pos::{
    TargetPosBuilder, TargetPosStore, TargetPosTask, TargetPosTaskExecutionEvent,
    TargetPosTaskTradeFill,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveStepPhase {
    Running,
    Cancelling,
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
    registry: Arc<Mutex<TaskRegistry>>,
    target_tasks: Arc<Mutex<TargetPosStore>>,
    store: Arc<Mutex<TargetPosSchedulerStore>>,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
}

#[derive(Clone)]
pub struct TargetPosScheduler {
    inner: Arc<TargetPosSchedulerInner>,
}

#[derive(Default)]
pub(crate) struct TargetPosSchedulerStore {
    schedulers: HashMap<TaskId, Weak<TargetPosSchedulerInner>>,
}

struct TargetPosSchedulerInner {
    registry: Arc<Mutex<TaskRegistry>>,
    target_tasks: Arc<Mutex<TargetPosStore>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
    next_step_index: Mutex<usize>,
    current_step_started_at: Mutex<Option<Instant>>,
    current_step_phase: Mutex<ActiveStepPhase>,
    active_task: Mutex<Option<TargetPosTask>>,
    active_task_report_len: Mutex<usize>,
    report: Mutex<TargetPosExecutionReport>,
    events: Mutex<Vec<TargetPosSchedulerExecutionEvent>>,
    last_error: Mutex<Option<crate::TaskError>>,
    finished_tx: watch::Sender<bool>,
    cancel_requested: AtomicBool,
    finished: AtomicBool,
}

impl TargetPosSchedulerBuilder {
    pub(crate) fn new(
        registry: Arc<Mutex<TaskRegistry>>,
        target_tasks: Arc<Mutex<TargetPosStore>>,
        store: Arc<Mutex<TargetPosSchedulerStore>>,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            target_tasks,
            store,
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
        self.config.offset_priority = priority;
        self
    }

    pub fn split_policy(mut self, policy: VolumeSplitPolicy) -> Self {
        self.config.split_policy = Some(policy);
        self
    }

    pub fn build(self) -> Result<TargetPosScheduler> {
        if let Some(policy) = self.config.split_policy {
            policy.validate()?;
        }
        let task = self
            .registry
            .lock()
            .expect("task registry lock poisoned")
            .register_scheduler(&self.account_id, &self.symbol)?;
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosSchedulerInner {
            registry: Arc::clone(&self.registry),
            target_tasks: Arc::clone(&self.target_tasks),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            steps: self.steps,
            config: self.config,
            next_step_index: Mutex::new(0),
            current_step_started_at: Mutex::new(None),
            current_step_phase: Mutex::new(ActiveStepPhase::Running),
            active_task: Mutex::new(None),
            active_task_report_len: Mutex::new(0),
            report: Mutex::new(TargetPosExecutionReport::default()),
            events: Mutex::new(Vec::new()),
            last_error: Mutex::new(None),
            finished_tx,
            cancel_requested: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });

        if inner.steps.is_empty() {
            inner.finish();
            return Ok(TargetPosScheduler { inner });
        }

        self.store
            .lock()
            .expect("scheduler store lock poisoned")
            .register(Arc::clone(&inner));

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
        self.inner
            .report
            .lock()
            .expect("scheduler report lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn execution_events(&self) -> Vec<TargetPosSchedulerExecutionEvent> {
        self.inner
            .events
            .lock()
            .expect("scheduler events lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerExecutionEvent>) {
        let events = self
            .inner
            .events
            .lock()
            .expect("scheduler events lock poisoned");
        let end = events.len();
        let start = start.min(end);
        (end, events[start..].to_vec())
    }

    #[must_use]
    pub fn execution_trades_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosSchedulerTradeFill>) {
        let report = self
            .inner
            .report
            .lock()
            .expect("scheduler report lock poisoned");
        let end = report.trades.len();
        let start = start.min(end);
        (end, report.trades[start..].to_vec())
    }

    #[must_use]
    pub fn last_error(&self) -> Option<crate::TaskError> {
        self.inner
            .last_error
            .lock()
            .expect("scheduler last error lock poisoned")
            .clone()
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

impl TargetPosSchedulerStore {
    fn register(&mut self, scheduler: Arc<TargetPosSchedulerInner>) {
        self.schedulers
            .insert(scheduler.task_id, Arc::downgrade(&scheduler));
    }

    fn live_schedulers(&mut self) -> Vec<Arc<TargetPosSchedulerInner>> {
        self.schedulers.retain(|_, weak| weak.strong_count() > 0);
        self.schedulers.values().filter_map(Weak::upgrade).collect()
    }
}

impl TargetPosExecutionReport {
    fn step_outcome_mut(&mut self, step_index: usize) -> &mut TargetPosStepOutcomeReport {
        let step_outcome = self
            .step_outcomes
            .get_mut(step_index)
            .expect("scheduler step outcome should be initialized before events are recorded");
        debug_assert_eq!(step_outcome.step_index, step_index);
        step_outcome
    }

    fn record_step_event(&mut self, step_index: usize, event: &TargetPosTaskExecutionEvent) {
        match event {
            TargetPosTaskExecutionEvent::InsertOrder { .. } => {
                self.submitted_order_count += 1;
                self.step_outcome_mut(step_index).submitted_order_count += 1;
            }
            TargetPosTaskExecutionEvent::CancelOrder { .. } => {
                self.cancel_request_count += 1;
                self.step_outcome_mut(step_index).cancel_request_count += 1;
            }
            TargetPosTaskExecutionEvent::OrderFinished { .. } => {
                self.finished_order_count += 1;
                self.step_outcome_mut(step_index).finished_order_count += 1;
            }
            TargetPosTaskExecutionEvent::Trade {
                trade_id,
                order_id,
                direction,
                offset,
                volume,
                price,
                trade_date_time,
            } => {
                self.filled_volume += *volume;
                self.filled_turnover += *price * *volume as f64;
                let step_outcome = self.step_outcome_mut(step_index);
                step_outcome.filled_volume += *volume;
                step_outcome.filled_turnover += *price * *volume as f64;
                step_outcome.trade_count += 1;
                self.trades.push(TargetPosSchedulerTradeFill {
                    step_index,
                    trade: TargetPosTaskTradeFill {
                        trade_id: trade_id.clone(),
                        order_id: order_id.clone(),
                        direction: direction.clone(),
                        offset: offset.clone(),
                        volume: *volume,
                        price: *price,
                        trade_date_time: *trade_date_time,
                    },
                });
            }
            TargetPosTaskExecutionEvent::TargetReached { .. } => {
                self.step_outcome_mut(step_index).target_reached = true;
            }
        }
    }
}

impl TargetPosSchedulerInner {
    async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
        if self.is_finished() {
            return;
        }

        if self.cancel_requested.load(Ordering::SeqCst) {
            if let Some(task) = self.active_task() {
                if let Err(error) = task.cancel_pending_orders(api).await {
                    self.finish_with_error(error);
                    return;
                }
                if let Some(step_index) = self.current_step_index() {
                    self.collect_active_task_events(step_index);
                }
                if task.has_live_orders(api) {
                    return;
                }
                task.cancel_internal();
            }
            self.finish();
            return;
        }

        loop {
            let Some(step_index) = self.current_step_index() else {
                self.finish();
                return;
            };
            self.ensure_step_started(step_index);
            if self.is_finished() {
                return;
            }

            let step = self.steps[step_index].clone();
            let phase = self.current_step_phase();
            let is_last_step = step_index + 1 == self.steps.len();

            if is_last_step {
                if let Some(task) = self.active_task() {
                    task.process_wait_update(api).await;
                    self.collect_active_task_events(step_index);
                    if let Some(error) = task_failure(&task) {
                        self.finish_with_error(error);
                        return;
                    }
                    if task.applied_target_volume() == Some(step.target_volume) {
                        self.finish();
                    }
                } else if step.price_mode.is_none() {
                    self.finish();
                }
                return;
            }

            if matches!(phase, ActiveStepPhase::Running) && !self.step_deadline_elapsed(step_index)
            {
                if let Some(task) = self.active_task() {
                    task.process_wait_update(api).await;
                    self.collect_active_task_events(step_index);
                    if let Some(error) = task_failure(&task) {
                        self.finish_with_error(error);
                    }
                }
                return;
            }

            self.mark_current_step_cancelling();

            if let Some(task) = self.active_task() {
                if let Err(error) = task.cancel_pending_orders(api).await {
                    self.finish_with_error(error);
                    return;
                }
                self.collect_active_task_events(step_index);
                if task.has_live_orders(api) {
                    return;
                }
                task.cancel_internal();
            }

            if !self.advance_step() {
                return;
            }
        }
    }

    fn ensure_step_started(&self, step_index: usize) {
        if self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned")
            .is_some()
        {
            return;
        }

        let step = self.steps[step_index].clone();
        *self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned") = Some(Instant::now());
        *self
            .current_step_phase
            .lock()
            .expect("scheduler step phase lock poisoned") = ActiveStepPhase::Running;
        *self
            .active_task_report_len
            .lock()
            .expect("scheduler active task report len lock poisoned") = 0;
        let mut report = self.report.lock().expect("scheduler report lock poisoned");
        debug_assert_eq!(report.applied_steps.len(), step_index);
        debug_assert_eq!(report.step_outcomes.len(), step_index);
        report.applied_steps.push(TargetPosExecutionStep {
            step_index,
            target_volume: step.target_volume,
        });
        report.step_outcomes.push(TargetPosStepOutcomeReport {
            step_index,
            target_volume: step.target_volume,
            ..TargetPosStepOutcomeReport::default()
        });
        drop(report);

        let Some(price_mode) = step.price_mode else {
            return;
        };

        match self.build_step_task(step.target_volume, price_mode) {
            Ok(task) => {
                *self
                    .active_task
                    .lock()
                    .expect("scheduler active task lock poisoned") = Some(task);
            }
            Err(error) => self.finish_with_error(error),
        }
    }

    fn build_step_task(&self, target_volume: i64, price_mode: PriceMode) -> Result<TargetPosTask> {
        let mut builder = TargetPosBuilder::new(
            Arc::clone(&self.registry),
            Arc::clone(&self.target_tasks),
            self.account_id.clone(),
            self.symbol.clone(),
        )
        .price_mode(price_mode)
        .offset_priority(self.config.offset_priority);
        if let Some(policy) = self.config.split_policy {
            builder = builder.split_policy(policy);
        }

        let task = builder.build_internal()?;
        task.set_target_volume(target_volume)?;
        Ok(task)
    }

    fn current_step_index(&self) -> Option<usize> {
        let next_step_index = *self
            .next_step_index
            .lock()
            .expect("scheduler next step lock poisoned");
        (next_step_index < self.steps.len()).then_some(next_step_index)
    }

    fn active_task(&self) -> Option<TargetPosTask> {
        self.active_task
            .lock()
            .expect("scheduler active task lock poisoned")
            .clone()
    }

    fn step_deadline_elapsed(&self, step_index: usize) -> bool {
        let started_at = *self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned");
        let Some(started_at) = started_at else {
            return false;
        };
        Instant::now().duration_since(started_at) >= self.steps[step_index].interval
    }

    fn current_step_phase(&self) -> ActiveStepPhase {
        *self
            .current_step_phase
            .lock()
            .expect("scheduler step phase lock poisoned")
    }

    fn mark_current_step_cancelling(&self) {
        *self
            .current_step_phase
            .lock()
            .expect("scheduler step phase lock poisoned") = ActiveStepPhase::Cancelling;
    }

    fn advance_step(&self) -> bool {
        *self
            .active_task
            .lock()
            .expect("scheduler active task lock poisoned") = None;
        *self
            .active_task_report_len
            .lock()
            .expect("scheduler active task report len lock poisoned") = 0;
        *self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned") = None;
        *self
            .current_step_phase
            .lock()
            .expect("scheduler step phase lock poisoned") = ActiveStepPhase::Running;

        let mut next_step_index = self
            .next_step_index
            .lock()
            .expect("scheduler next step lock poisoned");
        *next_step_index += 1;
        if *next_step_index >= self.steps.len() {
            drop(next_step_index);
            self.finish();
            return false;
        }
        true
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    fn cancel_active_task(&self) {
        if let Some(task) = self
            .active_task
            .lock()
            .expect("scheduler active task lock poisoned")
            .take()
        {
            task.cancel_internal();
        }
        *self
            .active_task_report_len
            .lock()
            .expect("scheduler active task report len lock poisoned") = 0;
        *self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned") = None;
    }

    fn collect_active_task_events(&self, step_index: usize) {
        let Some(task) = self.active_task() else {
            return;
        };
        let mut report_len = self
            .active_task_report_len
            .lock()
            .expect("scheduler active task report len lock poisoned");
        let (next_report_len, new_events) = task.execution_events_since(*report_len);
        if new_events.is_empty() {
            return;
        }

        let mut report = self.report.lock().expect("scheduler report lock poisoned");
        let mut events = self.events.lock().expect("scheduler events lock poisoned");
        for event in new_events {
            report.record_step_event(step_index, &event);
            events.push(TargetPosSchedulerExecutionEvent { step_index, event });
        }
        *report_len = next_report_len;
    }

    fn finish(&self) {
        self.cancel_active_task();
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(self.task_id);
    }

    fn finish_with_error(&self, error: crate::TaskError) {
        *self
            .last_error
            .lock()
            .expect("scheduler last error lock poisoned") = Some(error);
        self.finish();
    }

    fn failure_result(&self) -> Result<()> {
        if let Some(error) = self
            .last_error
            .lock()
            .expect("scheduler last error lock poisoned")
            .clone()
        {
            return Err(error);
        }
        Ok(())
    }
}

fn task_failure(task: &TargetPosTask) -> Option<crate::TaskError> {
    task.is_finished().then(|| task.last_error()).flatten()
}

impl Drop for TargetPosSchedulerInner {
    fn drop(&mut self) {
        if let Some(task) = self
            .active_task
            .lock()
            .expect("scheduler active task lock poisoned")
            .take()
        {
            task.cancel_internal();
        }

        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(self.task_id);
    }
}

pub(crate) async fn process_schedulers_wait_update(
    store: &Arc<Mutex<TargetPosSchedulerStore>>,
    api: &mut tqsdk_wait::TqApi,
) {
    let schedulers = store
        .lock()
        .expect("scheduler store lock poisoned")
        .live_schedulers();
    for scheduler in schedulers {
        scheduler.process_wait_update(api).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tqsdk_core::{AdapterRegistry, MarketAdapter, RuntimeHandle};
    use tqsdk_session::SessionClient;
    use tqsdk_wait::TqApi;

    fn market_only_api() -> TqApi {
        let mut adapters = AdapterRegistry::new();
        adapters.register_adapter(MarketAdapter::default());
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = SessionClient::new_for_test_with_handle(handle);
        TqApi::new(session)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_scheduler_records_error_when_internal_cancel_submission_fails() {
        let registry = Arc::new(Mutex::new(TaskRegistry::default()));
        let target_tasks = Arc::new(Mutex::new(TargetPosStore::default()));
        let schedulers = Arc::new(Mutex::new(TargetPosSchedulerStore::default()));
        let scheduler = TargetPosSchedulerBuilder::new(
            Arc::clone(&registry),
            Arc::clone(&target_tasks),
            Arc::clone(&schedulers),
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
            Arc::clone(&registry),
            Arc::clone(&target_tasks),
            "sim".to_string(),
            "SHFE.rb2601".to_string(),
        )
        .build_internal()
        .expect("internal target task should build");
        let mut api = market_only_api();
        task.track_order_for_test(api.get_order("sim", "unit-order-1"));
        *scheduler
            .inner
            .active_task
            .lock()
            .expect("scheduler active task lock poisoned") = Some(task);
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
}
