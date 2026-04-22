#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::Result;
use crate::config::{OffsetPriority, TargetPosSchedulerConfig, VolumeSplitPolicy};
use crate::registry::{TaskId, TaskRegistry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPosScheduleStep {
    pub interval: Duration,
    pub target_volume: i64,
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
    store: Arc<Mutex<TargetPosSchedulerStore>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
    next_step_index: Mutex<usize>,
    current_step_started_at: Mutex<Option<Instant>>,
    report: Mutex<TargetPosExecutionReport>,
    finished_tx: watch::Sender<bool>,
    finished: AtomicBool,
}

impl TargetPosSchedulerBuilder {
    pub(crate) fn new(
        registry: Arc<Mutex<TaskRegistry>>,
        store: Arc<Mutex<TargetPosSchedulerStore>>,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
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
        let task = self
            .registry
            .lock()
            .expect("task registry lock poisoned")
            .register_scheduler(&self.account_id, &self.symbol)?;
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosSchedulerInner {
            registry: Arc::clone(&self.registry),
            store: Arc::clone(&self.store),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            steps: self.steps,
            config: self.config,
            next_step_index: Mutex::new(0),
            current_step_started_at: Mutex::new(None),
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

    pub(crate) fn process_wait_update(&mut self) {
        self.schedulers.retain(|_, weak| {
            let Some(scheduler) = weak.upgrade() else {
                return false;
            };
            scheduler.process_wait_update();
            !scheduler.is_finished()
        });
    }
}

impl TargetPosSchedulerInner {
    fn process_wait_update(&self) {
        if self.is_finished() {
            return;
        }

        let now = Instant::now();
        let mut next_step_index = self
            .next_step_index
            .lock()
            .expect("scheduler next step lock poisoned");
        let mut current_step_started_at = self
            .current_step_started_at
            .lock()
            .expect("scheduler started-at lock poisoned");

        if let Some(started_at) = *current_step_started_at {
            let active_step = &self.steps[*next_step_index];
            if now.duration_since(started_at) < active_step.interval {
                return;
            }

            *next_step_index += 1;
            *current_step_started_at = None;
            if *next_step_index >= self.steps.len() {
                drop(current_step_started_at);
                drop(next_step_index);
                self.finish();
                return;
            }
        }

        let step_index = *next_step_index;
        let step = self.steps[step_index].clone();
        self.report
            .lock()
            .expect("scheduler report lock poisoned")
            .applied_steps
            .push(TargetPosExecutionStep {
                step_index,
                target_volume: step.target_volume,
            });

        if step_index + 1 == self.steps.len() {
            *next_step_index = self.steps.len();
            *current_step_started_at = None;
            drop(current_step_started_at);
            drop(next_step_index);
            self.finish();
            return;
        }

        *current_step_started_at = Some(now);
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    fn finish(&self) {
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
