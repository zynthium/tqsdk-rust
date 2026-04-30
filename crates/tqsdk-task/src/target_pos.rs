#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::watch;
use tqsdk_core::{ObjectKey, Order, Position, TradeDirection, TradeOffset};
use tqsdk_wait::OrderRef;

use crate::config::{OffsetPriority, PriceMode, TargetPosConfig, VolumeSplitPolicy};
use crate::plan::net_position;
use crate::registry::TaskId;
use crate::shared::{
    SharedQuoteSubscriptions, SharedTargetPosStore, SharedTaskRegistry, TaskStateCell,
};
use crate::{Result, TaskError};

mod executor;
mod planner;
mod report;
mod state;

pub use report::{
    TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport, TargetPosTaskOrderReport,
    TargetPosTaskReachedTarget, TargetPosTaskTradeFill,
};
pub(crate) use state::TargetPosStore;
use state::TargetPosTaskState;
use state::{DesiredBatch, LiveOrderHandling};

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

enum ProcessStep {
    Continue,
    Stop,
}

struct TargetPlanState {
    current_net_position: i64,
    desired_batch: Option<DesiredBatch>,
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
        self.config.price_mode = mode;
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
        if let Some(policy) = self.config.split_policy {
            policy.validate()?;
        }
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

    #[must_use]
    pub(crate) fn applied_target_volume(&self) -> Option<i64> {
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

    #[doc(hidden)]
    #[must_use]
    pub fn applied_target_volume_for_test(&self) -> Option<i64> {
        self.applied_target_volume()
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn track_order_for_test(&self, order_ref: OrderRef) {
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

    async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
        self.record_commit_trades(api);

        match self.process_cancel_requested(api).await {
            Ok(ProcessStep::Continue) => {}
            Ok(ProcessStep::Stop) => return,
            Err(error) => {
                self.finish_with_error(error);
                return;
            }
        }

        let current_seq = self.next_request_seq.load(Ordering::SeqCst);
        if current_seq == 0 || *self.reached_tx.borrow() >= current_seq {
            return;
        }

        let Some(target_volume) = self.with_state(|state| state.target_volume) else {
            return;
        };

        let TargetPlanState {
            current_net_position,
            mut desired_batch,
        } = match self
            .desired_batch_for_current_state(api, target_volume)
            .await
        {
            Ok(plan_state) => plan_state,
            Err(error) => {
                self.finish_with_error(error);
                return;
            }
        };
        desired_batch = match self
            .handle_live_orders(
                api,
                current_net_position,
                target_volume,
                desired_batch.as_ref(),
            )
            .await
        {
            Ok(LiveOrderHandling::NoLiveOrders) => desired_batch,
            Ok(LiveOrderHandling::Blocked) => return,
            Ok(LiveOrderHandling::SubmitMissing(missing_batch)) => Some(missing_batch),
            Err(error) => {
                self.finish_with_error(error);
                return;
            }
        };

        if self.awaiting_progress.load(Ordering::SeqCst)
            && self.submitted_request_seq.load(Ordering::SeqCst) >= current_seq
            && self.with_state(|state| state.submitted_net_position) == Some(current_net_position)
        {
            return;
        }

        if self.mark_reached_if_current_position_matches(
            current_seq,
            current_net_position,
            target_volume,
        ) {
            return;
        }

        let Some(desired_batch) = desired_batch else {
            return;
        };

        if let Err(error) = self
            .submit_desired_batch(api, current_seq, current_net_position, desired_batch)
            .await
        {
            self.finish_with_error(error);
        }
    }

    async fn process_cancel_requested(&self, api: &mut tqsdk_wait::TqApi) -> Result<ProcessStep> {
        if !self.cancel_requested.load(Ordering::SeqCst) {
            return Ok(ProcessStep::Continue);
        }

        self.cancel_pending_orders(api).await?;
        if self.has_live_orders(api) {
            return Ok(ProcessStep::Stop);
        }
        self.finish();
        Ok(ProcessStep::Stop)
    }

    async fn desired_batch_for_current_state(
        &self,
        api: &mut tqsdk_wait::TqApi,
        target_volume: i64,
    ) -> Result<TargetPlanState> {
        self.ensure_quote_subscription(api).await?;

        let current_position = current_position_snapshot(api, &self.account_id, &self.symbol);
        let current_net_position = net_position(&current_position);
        let quote = api.quote_ref(&self.symbol).snapshot(api).ok().flatten();
        let desired_batch = quote.as_ref().and_then(|quote| {
            planner::desired_batch_for_target(
                &self.symbol,
                &self.config,
                target_volume,
                &current_position,
                quote,
            )
        });

        Ok(TargetPlanState {
            current_net_position,
            desired_batch,
        })
    }

    fn mark_reached_if_current_position_matches(
        &self,
        current_seq: u64,
        current_net_position: i64,
        target_volume: i64,
    ) -> bool {
        if current_net_position != target_volume {
            return false;
        }

        self.mark_reached(current_seq, target_volume);
        true
    }

    async fn submit_desired_batch(
        &self,
        api: &mut tqsdk_wait::TqApi,
        current_seq: u64,
        current_net_position: i64,
        desired_batch: DesiredBatch,
    ) -> Result<()> {
        if desired_batch.orders.is_empty() {
            return Ok(());
        }

        self.submitted_request_seq
            .store(current_seq, Ordering::SeqCst);
        self.awaiting_progress.store(true, Ordering::SeqCst);
        self.with_state_mut(|state| {
            state.submitted_net_position = Some(current_net_position);
        });

        let mut inserted_any = false;
        for desired_order in desired_batch.orders {
            match executor::insert_desired_order(
                api,
                &self.account_id,
                &self.symbol,
                &desired_order,
            )
            .await
            {
                Ok(order_ref) => {
                    inserted_any = true;
                    let order_id = order_ref.order_id().to_string();
                    self.track_order(order_ref);
                    self.record_insert_order(
                        current_seq,
                        &order_id,
                        desired_order.direction,
                        desired_order.offset,
                        desired_order.volume,
                        desired_order.limit_price,
                    );
                }
                Err(error) if inserted_any => {
                    self.with_state_mut(|state| {
                        state.last_error = Some(error);
                    });
                    self.cancel_requested.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    fn mark_reached(&self, current_seq: u64, target_volume: i64) {
        self.with_state_mut(|state| {
            state.applied_target_volume = Some(target_volume);
            state
                .report
                .record_target_reached(current_seq, target_volume);
        });
        self.awaiting_progress.store(false, Ordering::SeqCst);
        self.reached_tx.send_replace(current_seq);
    }

    fn track_order(&self, order_ref: OrderRef) {
        self.with_state_mut(|state| {
            state
                .known_order_ids
                .insert(order_ref.order_id().to_string());
            state.tracked_orders.push(order_ref);
        });
    }

    fn record_insert_order(
        &self,
        request_seq: u64,
        order_id: &str,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        limit_price: f64,
    ) {
        self.with_state_mut(|state| {
            state.report.record_insert_order(
                request_seq,
                order_id,
                direction,
                offset,
                volume,
                limit_price,
            );
        });
    }

    fn record_cancel_order(&self, order_id: &str) {
        self.with_state_mut(|state| {
            state.report.record_cancel_order(order_id);
        });
    }

    fn record_commit_trades(&self, api: &tqsdk_wait::TqApi) {
        let Some(commit) = api.last_commit() else {
            return;
        };
        let trade_ids = commit
            .changes
            .object_hits
            .iter()
            .filter_map(|object| match object {
                ObjectKey::Trade {
                    account_id,
                    trade_id,
                } if account_id.as_str() == self.account_id => Some(trade_id.as_str().to_string()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if trade_ids.is_empty() {
            return;
        }

        let known_order_ids = self.with_state(|state| state.known_order_ids.clone());
        if known_order_ids.is_empty() {
            return;
        }

        for trade_id in trade_ids {
            let Some(trade) = api
                .get_trade(&self.account_id, &trade_id)
                .snapshot(api)
                .ok()
                .flatten()
            else {
                continue;
            };
            if !known_order_ids.contains(&trade.order_id) {
                continue;
            }

            self.with_state_mut(|state| {
                if state.seen_trade_ids.insert(trade.trade_id.clone()) {
                    state.report.record_trade(&trade);
                }
            });
        }
    }

    async fn ensure_quote_subscription(&self, api: &mut tqsdk_wait::TqApi) -> Result<()> {
        if api
            .quote_ref(&self.symbol)
            .snapshot(api)
            .ok()
            .flatten()
            .as_ref()
            .is_some_and(planner::quote_supports_pricing)
        {
            return Ok(());
        }

        if self.quote_subscriptions.contains(&self.symbol) {
            return Ok(());
        }

        api.get_quote(&self.symbol).await.map_err(TaskError::from)?;
        self.quote_subscriptions.insert(self.symbol.clone());
        Ok(())
    }

    fn has_live_orders(&self, api: &tqsdk_wait::TqApi) -> bool {
        self.prune_terminal_orders(api);
        self.with_state(|state| !state.tracked_orders.is_empty())
    }

    async fn handle_live_orders(
        &self,
        api: &mut tqsdk_wait::TqApi,
        current_net_position: i64,
        target_volume: i64,
        desired_batch: Option<&DesiredBatch>,
    ) -> Result<LiveOrderHandling> {
        if !self.has_live_orders(api) {
            return Ok(LiveOrderHandling::NoLiveOrders);
        }

        if current_net_position == target_volume {
            self.cancel_pending_orders(api).await?;
            return Ok(LiveOrderHandling::Blocked);
        }

        let unmaterialized_order_ids = self.unmaterialized_order_ids(api);
        if !unmaterialized_order_ids.is_empty() {
            if self.awaiting_same_submission(current_net_position) {
                return Ok(LiveOrderHandling::Blocked);
            }

            self.cancel_pending_orders_by_id(api, &unmaterialized_order_ids)
                .await?;
            return Ok(LiveOrderHandling::Blocked);
        }

        let live_orders = self.live_orders(api);
        if live_orders.is_empty() {
            return Ok(LiveOrderHandling::NoLiveOrders);
        }

        let Some(desired_batch) = desired_batch else {
            return Ok(LiveOrderHandling::Blocked);
        };

        let reconciliation = planner::reconcile_live_orders(&live_orders, desired_batch);
        if !reconciliation.stale_order_ids.is_empty() {
            self.cancel_pending_orders_by_id(api, &reconciliation.stale_order_ids)
                .await?;
            return Ok(LiveOrderHandling::Blocked);
        }

        if reconciliation.missing_batch.orders.is_empty() {
            Ok(LiveOrderHandling::Blocked)
        } else {
            Ok(LiveOrderHandling::SubmitMissing(
                reconciliation.missing_batch,
            ))
        }
    }

    async fn cancel_pending_orders(&self, api: &mut tqsdk_wait::TqApi) -> Result<()> {
        self.cancel_pending_orders_filtered(api, None).await
    }

    async fn cancel_pending_orders_by_id(
        &self,
        api: &mut tqsdk_wait::TqApi,
        order_ids: &HashSet<String>,
    ) -> Result<()> {
        self.cancel_pending_orders_filtered(api, Some(order_ids))
            .await
    }

    async fn cancel_pending_orders_filtered(
        &self,
        api: &mut tqsdk_wait::TqApi,
        order_ids: Option<&HashSet<String>>,
    ) -> Result<()> {
        self.prune_terminal_orders(api);

        let tracked_orders = self.with_state(|state| state.tracked_orders.clone());

        for order_ref in tracked_orders {
            let order_id = order_ref.order_id().to_string();
            if let Some(order_ids) = order_ids
                && !order_ids.contains(&order_id)
            {
                continue;
            }
            let should_cancel = {
                self.with_state_mut(|state| {
                    state.cancel_requested_order_ids.insert(order_id.clone())
                })
            };
            if !should_cancel {
                continue;
            }
            if let Err(error) = executor::cancel_order(api, order_ref.account_id(), &order_id).await
            {
                self.with_state_mut(|state| {
                    state.cancel_requested_order_ids.remove(&order_id);
                });
                return Err(error);
            }
            self.record_cancel_order(&order_id);
        }
        Ok(())
    }

    fn prune_terminal_orders(&self, api: &tqsdk_wait::TqApi) {
        let tracked_orders = self.with_state(|state| state.tracked_orders.clone());
        let finished_orders = tracked_orders
            .iter()
            .filter_map(|order_ref| {
                order_ref
                    .snapshot(api)
                    .ok()
                    .flatten()
                    .filter(order_is_terminal)
            })
            .collect::<Vec<_>>();
        if finished_orders.is_empty() {
            if self.with_state(|state| state.tracked_orders.is_empty()) {
                self.awaiting_progress.store(false, Ordering::SeqCst);
            }
            return;
        }

        let finished_order_ids = finished_orders
            .iter()
            .map(|order| order.order_id.clone())
            .collect::<HashSet<_>>();
        let no_tracked_orders_left = self.with_state_mut(|state| {
            for order in &finished_orders {
                state.report.record_order_finished(order);
            }
            state
                .tracked_orders
                .retain(|order_ref| !finished_order_ids.contains(order_ref.order_id()));
            state
                .cancel_requested_order_ids
                .retain(|order_id| !finished_order_ids.contains(order_id));
            state.tracked_orders.is_empty()
        });
        if no_tracked_orders_left {
            self.awaiting_progress.store(false, Ordering::SeqCst);
        }
    }

    fn live_orders(&self, api: &tqsdk_wait::TqApi) -> Vec<Order> {
        self.prune_terminal_orders(api);
        self.with_state(|state| state.tracked_orders.clone())
            .into_iter()
            .filter_map(|order_ref| order_ref.snapshot(api).ok().flatten())
            .filter(|order| !order_is_terminal(order))
            .collect()
    }

    fn unmaterialized_order_ids(&self, api: &tqsdk_wait::TqApi) -> HashSet<String> {
        self.prune_terminal_orders(api);
        self.with_state(|state| state.tracked_orders.clone())
            .into_iter()
            .filter(|order_ref| order_ref.snapshot(api).ok().flatten().is_none())
            .map(|order_ref| order_ref.order_id().to_string())
            .collect()
    }

    fn awaiting_same_submission(&self, current_net_position: i64) -> bool {
        self.awaiting_progress.load(Ordering::SeqCst)
            && self.with_state(|state| state.submitted_net_position) == Some(current_net_position)
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

    fn finish_with_error(&self, error: TaskError) {
        self.with_state_mut(|state| {
            state.last_error = Some(error);
        });
        self.finish();
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

fn current_position_snapshot(api: &tqsdk_wait::TqApi, account_id: &str, symbol: &str) -> Position {
    api.get_position(account_id, symbol)
        .snapshot(api)
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn order_is_terminal(order: &Order) -> bool {
    order.lifecycle.is_terminal()
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
