#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::Result;
use crate::config::{OffsetPriority, PriceMode, TargetPosSchedulerConfig, VolumeSplitPolicy};
use crate::registry::{TaskId, TaskRegistry};
use crate::target_pos::{TargetPosBuilder, TargetPosStore, TargetPosTask};

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetPosExecutionReport {
    pub applied_steps: Vec<TargetPosExecutionStep>,
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
    store: Arc<Mutex<TargetPosSchedulerStore>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
    next_step_index: Mutex<usize>,
    current_step_started_at: Mutex<Option<Instant>>,
    active_task: Mutex<Option<TargetPosTask>>,
    report: Mutex<TargetPosExecutionReport>,
    finished_tx: watch::Sender<bool>,
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
            store: Arc::clone(&self.store),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            steps: self.steps,
            config: self.config,
            next_step_index: Mutex::new(0),
            current_step_started_at: Mutex::new(None),
            active_task: Mutex::new(None),
            report: Mutex::new(TargetPosExecutionReport::default()),
            finished_tx,
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

    pub async fn cancel(&self) -> Result<()> {
        self.inner.finish();
        self.inner
            .store
            .lock()
            .expect("scheduler store lock poisoned")
            .unregister(self.inner.task_id);
        Ok(())
    }

    pub async fn wait_finished(&self) -> Result<()> {
        if self.is_finished() {
            return Ok(());
        }

        let mut finished_rx = self.inner.finished_tx.subscribe();
        loop {
            if *finished_rx.borrow() {
                return Ok(());
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

    fn unregister(&mut self, task_id: TaskId) {
        self.schedulers.remove(&task_id);
    }

    fn live_schedulers(&mut self) -> Vec<Arc<TargetPosSchedulerInner>> {
        self.schedulers.retain(|_, weak| weak.strong_count() > 0);
        self.schedulers.values().filter_map(Weak::upgrade).collect()
    }
}

impl TargetPosSchedulerInner {
    async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
        if self.is_finished() {
            return;
        }

        loop {
            let Some(step_index) = self.current_step_index() else {
                self.finish();
                return;
            };
            self.ensure_step_started(step_index);

            let step = self.steps[step_index].clone();
            if let Some(task) = self.active_task() {
                task.process_wait_update(api).await;
                if step_index + 1 == self.steps.len()
                    && task.applied_target_volume() == Some(step.target_volume)
                {
                    self.finish();
                    return;
                }
            } else if step.price_mode.is_none() && step_index + 1 == self.steps.len() {
                self.finish();
                return;
            }

            if !self.step_deadline_elapsed(step_index) {
                return;
            }

            self.cancel_active_task();
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
        self.report
            .lock()
            .expect("scheduler report lock poisoned")
            .applied_steps
            .push(TargetPosExecutionStep {
                step_index,
                target_volume: step.target_volume,
            });

        let Some(price_mode) = step.price_mode else {
            return;
        };

        let task = self.build_step_task(step.target_volume, price_mode);
        if let Ok(task) = task {
            *self
                .active_task
                .lock()
                .expect("scheduler active task lock poisoned") = Some(task);
        } else {
            self.finish();
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

    fn advance_step(&self) -> bool {
        *self
            .active_task
            .lock()
            .expect("scheduler active task lock poisoned") = None;
        *self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned") = None;

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
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned") = None;
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
