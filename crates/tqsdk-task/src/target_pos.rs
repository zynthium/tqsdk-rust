#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::watch;

use crate::config::{OffsetPriority, PriceMode, TargetPosConfig, VolumeSplitPolicy};
use crate::registry::TaskId;
use crate::shared::{
    SharedQuoteSubscriptions, SharedTargetPosStore, SharedTaskRegistry, TaskStateCell,
};
use crate::{Result, TaskError};

mod executor;
mod machine;
mod planner;
mod report;
mod state;

pub use report::{
    TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport, TargetPosTaskOrderReport,
    TargetPosTaskReachedTarget, TargetPosTaskTradeFill,
};
pub(crate) use state::TargetPosStore;
use state::TargetPosTaskState;

/// Builder for a target position task.
pub struct TargetPosBuilder {
    registry: SharedTaskRegistry,
    store: SharedTargetPosStore,
    quote_subscriptions: SharedQuoteSubscriptions,
    account_id: String,
    symbol: String,
    config: TargetPosConfig,
}

/// Minimal target position task shell.
#[derive(Clone)]
pub struct TargetPosTask {
    inner: Arc<TargetPosTaskInner>,
}

struct TargetPosTaskInner {
    registry: SharedTaskRegistry,
    store: SharedTargetPosStore,
    quote_subscriptions: SharedQuoteSubscriptions,
    managed_by_host: bool,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    config: TargetPosConfig,
    state: TaskStateCell<TargetPosTaskState>,
    next_request_seq: AtomicU64,
    submitted_request_seq: AtomicU64,
    awaiting_progress: AtomicBool,
    reached_tx: watch::Sender<u64>,
    finished_tx: watch::Sender<bool>,
    cancel_requested: AtomicBool,
    finished: AtomicBool,
}

impl TargetPosBuilder {
    pub(crate) fn new(
        registry: SharedTaskRegistry,
        store: SharedTargetPosStore,
        quote_subscriptions: SharedQuoteSubscriptions,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            store,
            quote_subscriptions,
            account_id,
            symbol,
            config: TargetPosConfig::default(),
        }
    }

    pub fn price_mode(mut self, mode: PriceMode) -> Self {
        self.config.set_price_mode(mode);
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

    pub fn build(self) -> Result<TargetPosTask> {
        let task = self
            .registry
            .with_mut(|registry| registry.register_target_task(&self.account_id, &self.symbol))?;
        self.build_with_task_id(task.id, true)
    }

    pub(crate) fn build_internal(self) -> Result<TargetPosTask> {
        let task_id = self
            .registry
            .with_mut(|registry| registry.allocate_task_id());
        self.build_with_task_id(task_id, false)
    }

    fn build_with_task_id(self, task_id: TaskId, managed_by_host: bool) -> Result<TargetPosTask> {
        let (reached_tx, _) = watch::channel(0_u64);
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosTaskInner {
            registry: self.registry.clone(),
            store: self.store.clone(),
            quote_subscriptions: self.quote_subscriptions.clone(),
            managed_by_host,
            task_id,
            account_id: self.account_id,
            symbol: self.symbol,
            config: self.config,
            state: TaskStateCell::default(),
            next_request_seq: AtomicU64::new(0),
            submitted_request_seq: AtomicU64::new(0),
            awaiting_progress: AtomicBool::new(false),
            reached_tx,
            finished_tx,
            cancel_requested: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        if managed_by_host {
            self.store
                .with_mut(|store| store.register(Arc::clone(&inner)));
        }

        Ok(TargetPosTask { inner })
    }
}

impl TargetPosTask {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[must_use]
    pub fn config(&self) -> &TargetPosConfig {
        &self.inner.config
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.finished.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn current_target_volume(&self) -> Option<i64> {
        self.inner.with_state(|state| state.target_volume)
    }

    #[must_use]
    pub fn last_error(&self) -> Option<TaskError> {
        self.inner.with_state(|state| state.last_error.clone())
    }

    #[must_use]
    pub fn execution_report(&self) -> TargetPosTaskExecutionReport {
        self.inner.with_state(|state| state.report.clone())
    }

    #[must_use]
    pub fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosTaskExecutionEvent>) {
        self.inner.with_state(|state| {
            let end = state.report.events.len();
            let start = start.min(end);
            (end, state.report.events[start..].to_vec())
        })
    }

    #[must_use]
    pub fn execution_trades_since(&self, start: usize) -> (usize, Vec<TargetPosTaskTradeFill>) {
        self.inner.with_state(|state| {
            let end = state.report.trades.len();
            let start = start.min(end);
            (end, state.report.trades[start..].to_vec())
        })
    }

    pub fn set_target_volume(&self, volume: i64) -> Result<()> {
        if self.is_finished() {
            return Err(TaskError::InvalidState(
                "target position task already finished",
            ));
        }
        if self.inner.cancel_requested.load(Ordering::SeqCst) {
            return Err(TaskError::InvalidState(
                "target position task cancellation already requested",
            ));
        }

        let changed = self.inner.with_state_mut(|state| {
            if state.target_volume == Some(volume) {
                return false;
            }
            state.target_volume = Some(volume);
            state.last_error = None;
            state.submitted_net_position = None;
            true
        });
        if !changed {
            return Ok(());
        }
        self.inner.awaiting_progress.store(false, Ordering::SeqCst);
        self.inner.next_request_seq.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub async fn wait_target_reached(&self) -> Result<()> {
        let target_seq = self.inner.next_request_seq.load(Ordering::SeqCst);
        if target_seq == 0 {
            return Ok(());
        }

        let mut reached_rx = self.inner.reached_tx.subscribe();
        let mut finished_rx = self.inner.finished_tx.subscribe();
        loop {
            if *reached_rx.borrow() >= target_seq {
                return Ok(());
            }
            if *finished_rx.borrow() {
                return match self.inner.failure_result() {
                    Ok(()) => Err(TaskError::InvalidState(
                        "target position task finished before reaching target",
                    )),
                    Err(error) => Err(error),
                };
            }

            tokio::select! {
                changed = reached_rx.changed() => {
                    changed.map_err(|_| TaskError::InvalidState(
                        "target position task reached channel closed",
                    ))?;
                }
                changed = finished_rx.changed() => {
                    changed.map_err(|_| TaskError::InvalidState(
                        "target position task finished channel closed",
                    ))?;
                }
            }
        }
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
                TaskError::InvalidState("target position task finished channel closed")
            })?;
        }
    }

    pub async fn cancel(&self) -> Result<()> {
        if self.is_finished() {
            return self.inner.failure_result();
        }
        self.inner.cancel_requested.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
        self.inner.process_wait_update(api).await;
    }

    /// Return the latest target volume that has been applied by host processing.
    ///
    /// A newly requested target is only applied when the owning [`TaskHost`](crate::TaskHost)
    /// advances via `wait_update()`, so this can lag behind
    /// [`TargetPosTask::current_target_volume`].
    #[must_use]
    pub fn applied_target_volume(&self) -> Option<i64> {
        self.inner.with_state(|state| state.applied_target_volume)
    }

    pub(crate) fn cancel_internal(&self) {
        self.inner.finish();
    }

    pub(crate) async fn cancel_pending_orders(&self, api: &mut tqsdk_wait::TqApi) -> Result<()> {
        self.inner.cancel_pending_orders(api).await
    }

    pub(crate) fn has_live_orders(&self, api: &tqsdk_wait::TqApi) -> bool {
        self.inner.has_live_orders(api)
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn track_order_for_test(&self, order_ref: tqsdk_wait::OrderRef) {
        self.inner.track_order(order_ref);
    }
}

impl TargetPosTaskInner {
    fn with_state<R>(&self, f: impl FnOnce(&TargetPosTaskState) -> R) -> R {
        self.state.with(f)
    }

    fn with_state_mut<R>(&self, f: impl FnOnce(&mut TargetPosTaskState) -> R) -> R {
        self.state.with_mut(f)
    }

    fn finish(&self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .with_mut(|registry| registry.unregister_task(self.task_id));
        if self.managed_by_host {
            self.store.with_mut(|store| store.unregister(self.task_id));
        }
    }

    fn failure_result(&self) -> Result<()> {
        if let Some(error) = self.with_state(|state| state.last_error.clone()) {
            return Err(error);
        }
        Ok(())
    }
}

pub(crate) async fn process_target_tasks_wait_update(
    store: &SharedTargetPosStore,
    api: &mut tqsdk_wait::TqApi,
) {
    let tasks = store.with_mut(TargetPosStore::live_tasks);
    for task in tasks {
        task.process_wait_update(api).await;
    }
}

impl Drop for TargetPosTaskInner {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn cancel_requested_task_records_error_when_cancel_submission_fails() {
        let registry = SharedTaskRegistry::default();
        let store = SharedTargetPosStore::default();
        let quote_subscriptions = SharedQuoteSubscriptions::default();
        let task = TargetPosBuilder::new(
            registry,
            store,
            quote_subscriptions,
            "sim".to_string(),
            "SHFE.rb2601".to_string(),
        )
        .build_internal()
        .expect("internal task should build");
        let mut api = market_only_api();
        task.inner.track_order(api.get_order("sim", "unit-order-1"));
        task.inner.cancel_requested.store(true, Ordering::SeqCst);

        task.inner.process_wait_update(&mut api).await;

        assert!(task.is_finished());
        assert!(matches!(task.last_error(), Some(TaskError::Wait(_))));
    }
}
