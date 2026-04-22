#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::json;
use tokio::sync::watch;
use tqsdk_core::{ObjectKey, Order, Position, Quote, Trade, TradeDirection, TradeOffset};
use tqsdk_wait::OrderRef;

use crate::config::{OffsetPriority, PriceMode, TargetPosConfig, VolumeSplitPolicy};
use crate::plan::{compute_plan, net_position};
use crate::registry::{TaskId, TaskRegistry};
use crate::{Result, TaskError};

/// Builder for a target position task.
pub struct TargetPosBuilder {
    registry: Arc<Mutex<TaskRegistry>>,
    store: Arc<Mutex<TargetPosStore>>,
    account_id: String,
    symbol: String,
    config: TargetPosConfig,
}

/// Minimal target position task shell.
#[derive(Clone)]
pub struct TargetPosTask {
    inner: Arc<TargetPosTaskInner>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TargetPosTaskExecutionEvent {
    InsertOrder {
        request_seq: u64,
        order_id: String,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        limit_price: f64,
    },
    CancelOrder {
        order_id: String,
    },
    OrderFinished {
        order_id: String,
        status: String,
        filled_volume: i64,
        remaining_volume: i64,
        last_msg: String,
    },
    Trade {
        trade_id: String,
        order_id: String,
        direction: String,
        offset: String,
        volume: i64,
        price: f64,
        trade_date_time: i64,
    },
    TargetReached {
        request_seq: u64,
        target_volume: i64,
    },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TargetPosTaskExecutionReport {
    pub events: Vec<TargetPosTaskExecutionEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct DesiredOrder {
    direction: TradeDirection,
    offset: TradeOffset,
    volume: i64,
    limit_price: f64,
}

#[derive(Default)]
pub(crate) struct TargetPosStore {
    tasks: HashMap<TaskId, Weak<TargetPosTaskInner>>,
}

struct TargetPosTaskInner {
    registry: Arc<Mutex<TaskRegistry>>,
    store: Arc<Mutex<TargetPosStore>>,
    managed_by_host: bool,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    config: TargetPosConfig,
    target_volume: Mutex<Option<i64>>,
    applied_target_volume: Mutex<Option<i64>>,
    last_error: Mutex<Option<TaskError>>,
    next_request_seq: AtomicU64,
    submitted_request_seq: AtomicU64,
    submitted_net_position: Mutex<Option<i64>>,
    awaiting_progress: AtomicBool,
    tracked_orders: Mutex<Vec<OrderRef>>,
    known_order_ids: Mutex<HashSet<String>>,
    cancel_requested_order_ids: Mutex<HashSet<String>>,
    seen_trade_ids: Mutex<HashSet<String>>,
    report: Mutex<TargetPosTaskExecutionReport>,
    reached_tx: watch::Sender<u64>,
    finished_tx: watch::Sender<bool>,
    cancel_requested: AtomicBool,
    finished: AtomicBool,
}

impl TargetPosBuilder {
    pub(crate) fn new(
        registry: Arc<Mutex<TaskRegistry>>,
        store: Arc<Mutex<TargetPosStore>>,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            store,
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
            .lock()
            .expect("task registry lock poisoned")
            .register_target_task(&self.account_id, &self.symbol)?;
        self.build_with_task_id(task.id, true)
    }

    pub(crate) fn build_internal(self) -> Result<TargetPosTask> {
        let task_id = self
            .registry
            .lock()
            .expect("task registry lock poisoned")
            .allocate_task_id();
        self.build_with_task_id(task_id, false)
    }

    fn build_with_task_id(self, task_id: TaskId, managed_by_host: bool) -> Result<TargetPosTask> {
        if let Some(policy) = self.config.split_policy {
            policy.validate()?;
        }
        let (reached_tx, _) = watch::channel(0_u64);
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosTaskInner {
            registry: Arc::clone(&self.registry),
            store: Arc::clone(&self.store),
            managed_by_host,
            task_id,
            account_id: self.account_id,
            symbol: self.symbol,
            config: self.config,
            target_volume: Mutex::new(None),
            applied_target_volume: Mutex::new(None),
            last_error: Mutex::new(None),
            next_request_seq: AtomicU64::new(0),
            submitted_request_seq: AtomicU64::new(0),
            submitted_net_position: Mutex::new(None),
            awaiting_progress: AtomicBool::new(false),
            tracked_orders: Mutex::new(Vec::new()),
            known_order_ids: Mutex::new(HashSet::new()),
            cancel_requested_order_ids: Mutex::new(HashSet::new()),
            seen_trade_ids: Mutex::new(HashSet::new()),
            report: Mutex::new(TargetPosTaskExecutionReport::default()),
            reached_tx,
            finished_tx,
            cancel_requested: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        });
        if managed_by_host {
            self.store
                .lock()
                .expect("target task store lock poisoned")
                .register(Arc::clone(&inner));
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
        *self
            .inner
            .target_volume
            .lock()
            .expect("target volume lock poisoned")
    }

    #[must_use]
    pub fn last_error(&self) -> Option<TaskError> {
        self.inner
            .last_error
            .lock()
            .expect("target task last error lock poisoned")
            .clone()
    }

    #[must_use]
    pub fn execution_report(&self) -> TargetPosTaskExecutionReport {
        self.inner
            .report
            .lock()
            .expect("target task execution report lock poisoned")
            .clone()
    }

    pub(crate) fn execution_events_since(
        &self,
        start: usize,
    ) -> (usize, Vec<TargetPosTaskExecutionEvent>) {
        let report = self
            .inner
            .report
            .lock()
            .expect("target task execution report lock poisoned");
        let end = report.events.len();
        let start = start.min(end);
        (end, report.events[start..].to_vec())
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

        *self
            .inner
            .target_volume
            .lock()
            .expect("target volume lock poisoned") = Some(volume);
        *self
            .inner
            .last_error
            .lock()
            .expect("target task last error lock poisoned") = None;
        *self
            .inner
            .submitted_net_position
            .lock()
            .expect("target task submitted net position lock poisoned") = None;
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
        *self
            .inner
            .applied_target_volume
            .lock()
            .expect("applied target volume lock poisoned")
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

impl TargetPosStore {
    fn register(&mut self, task: Arc<TargetPosTaskInner>) {
        self.tasks.insert(task.task_id, Arc::downgrade(&task));
    }

    fn unregister(&mut self, task_id: TaskId) {
        self.tasks.remove(&task_id);
    }

    fn live_tasks(&mut self) -> Vec<Arc<TargetPosTaskInner>> {
        self.tasks.retain(|_, weak| weak.strong_count() > 0);
        self.tasks.values().filter_map(Weak::upgrade).collect()
    }
}

impl TargetPosTaskInner {
    async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
        self.record_commit_trades(api);

        if self.cancel_requested.load(Ordering::SeqCst) {
            if let Err(error) = self.cancel_pending_orders(api).await {
                self.finish_with_error(error);
                return;
            }
            if self.has_live_orders(api) {
                return;
            }
            self.finish();
            return;
        }

        let current_seq = self.next_request_seq.load(Ordering::SeqCst);
        if current_seq == 0 || *self.reached_tx.borrow() >= current_seq {
            return;
        }

        let Some(target_volume) = self
            .target_volume
            .lock()
            .expect("target volume lock poisoned")
            .as_ref()
            .copied()
        else {
            return;
        };

        let current_position = current_position_snapshot(api, &self.account_id, &self.symbol);
        let current_net_position = net_position(&current_position);
        let desired_order = desired_order_for_target(api, self, target_volume, &current_position);
        let handled_live_orders = match self
            .handle_live_orders(
                api,
                current_net_position,
                target_volume,
                desired_order.as_ref(),
            )
            .await
        {
            Ok(handled) => handled,
            Err(error) => {
                self.finish_with_error(error);
                return;
            }
        };
        if handled_live_orders {
            return;
        }

        if self.awaiting_progress.load(Ordering::SeqCst)
            && self.submitted_request_seq.load(Ordering::SeqCst) >= current_seq
            && *self
                .submitted_net_position
                .lock()
                .expect("target task submitted net position lock poisoned")
                == Some(current_net_position)
        {
            return;
        }

        if current_net_position == target_volume {
            self.mark_reached(current_seq, target_volume);
            return;
        }

        let Some(desired_order) = desired_order else {
            return;
        };

        match api
            .insert_order(
                &self.account_id,
                &self.symbol,
                desired_order.direction,
                Some(desired_order.offset),
                desired_order.volume,
                Some(json!(desired_order.limit_price)),
            )
            .await
        {
            Ok(order_ref) => {
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
                self.submitted_request_seq
                    .store(current_seq, Ordering::SeqCst);
                self.awaiting_progress.store(true, Ordering::SeqCst);
                *self
                    .submitted_net_position
                    .lock()
                    .expect("target task submitted net position lock poisoned") =
                    Some(current_net_position);
            }
            Err(error) => self.finish_with_error(TaskError::from(error)),
        }
    }

    fn mark_reached(&self, current_seq: u64, target_volume: i64) {
        *self
            .applied_target_volume
            .lock()
            .expect("applied target volume lock poisoned") = Some(target_volume);
        self.awaiting_progress.store(false, Ordering::SeqCst);
        self.record_target_reached(current_seq, target_volume);
        self.reached_tx.send_replace(current_seq);
    }

    fn track_order(&self, order_ref: OrderRef) {
        self.known_order_ids
            .lock()
            .expect("target task known order ids lock poisoned")
            .insert(order_ref.order_id().to_string());
        self.tracked_orders
            .lock()
            .expect("target task tracked orders lock poisoned")
            .push(order_ref);
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
        self.report
            .lock()
            .expect("target task execution report lock poisoned")
            .events
            .push(TargetPosTaskExecutionEvent::InsertOrder {
                request_seq,
                order_id: order_id.to_string(),
                direction,
                offset,
                volume,
                limit_price,
            });
    }

    fn record_cancel_order(&self, order_id: &str) {
        self.report
            .lock()
            .expect("target task execution report lock poisoned")
            .events
            .push(TargetPosTaskExecutionEvent::CancelOrder {
                order_id: order_id.to_string(),
            });
    }

    fn record_order_finished(&self, order: &Order) {
        self.report
            .lock()
            .expect("target task execution report lock poisoned")
            .events
            .push(TargetPosTaskExecutionEvent::OrderFinished {
                order_id: order.order_id.clone(),
                status: order.status.clone(),
                filled_volume: order.volume_orign - order.volume_left,
                remaining_volume: order.volume_left,
                last_msg: order.last_msg.clone(),
            });
    }

    fn record_target_reached(&self, request_seq: u64, target_volume: i64) {
        self.report
            .lock()
            .expect("target task execution report lock poisoned")
            .events
            .push(TargetPosTaskExecutionEvent::TargetReached {
                request_seq,
                target_volume,
            });
    }

    fn record_trade(&self, trade: &Trade) {
        self.report
            .lock()
            .expect("target task execution report lock poisoned")
            .events
            .push(TargetPosTaskExecutionEvent::Trade {
                trade_id: trade.trade_id.clone(),
                order_id: trade.order_id.clone(),
                direction: trade.direction.clone(),
                offset: trade.offset.clone(),
                volume: trade.volume,
                price: trade.price,
                trade_date_time: trade.trade_date_time,
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

        let known_order_ids = self
            .known_order_ids
            .lock()
            .expect("target task known order ids lock poisoned")
            .clone();
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

            let inserted = self
                .seen_trade_ids
                .lock()
                .expect("target task seen trade ids lock poisoned")
                .insert(trade.trade_id.clone());
            if inserted {
                self.record_trade(&trade);
            }
        }
    }

    fn has_live_orders(&self, api: &tqsdk_wait::TqApi) -> bool {
        self.prune_terminal_orders(api);
        !self
            .tracked_orders
            .lock()
            .expect("target task tracked orders lock poisoned")
            .is_empty()
    }

    async fn handle_live_orders(
        &self,
        api: &mut tqsdk_wait::TqApi,
        current_net_position: i64,
        target_volume: i64,
        desired_order: Option<&DesiredOrder>,
    ) -> Result<bool> {
        let live_orders = self.live_orders(api);
        if live_orders.is_empty() {
            return Ok(false);
        }

        if should_cancel_for_replan(
            &live_orders,
            desired_order,
            current_net_position,
            target_volume,
        ) {
            self.cancel_pending_orders(api).await?;
        }
        Ok(true)
    }

    async fn cancel_pending_orders(&self, api: &mut tqsdk_wait::TqApi) -> Result<()> {
        self.prune_terminal_orders(api);

        let tracked_orders = self
            .tracked_orders
            .lock()
            .expect("target task tracked orders lock poisoned")
            .clone();

        for order_ref in tracked_orders {
            let order_id = order_ref.order_id().to_string();
            let should_cancel = {
                let mut cancel_requested_order_ids = self
                    .cancel_requested_order_ids
                    .lock()
                    .expect("target task cancel requested orders lock poisoned");
                if cancel_requested_order_ids.contains(&order_id) {
                    false
                } else {
                    cancel_requested_order_ids.insert(order_id.clone());
                    true
                }
            };
            if !should_cancel {
                continue;
            }
            if let Err(error) = api.cancel_order(order_ref.account_id(), &order_id).await {
                self.cancel_requested_order_ids
                    .lock()
                    .expect("target task cancel requested orders lock poisoned")
                    .remove(&order_id);
                return Err(TaskError::from(error));
            }
            self.record_cancel_order(&order_id);
        }
        Ok(())
    }

    fn prune_terminal_orders(&self, api: &tqsdk_wait::TqApi) {
        let mut tracked_orders = self
            .tracked_orders
            .lock()
            .expect("target task tracked orders lock poisoned");
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
        for order in &finished_orders {
            self.record_order_finished(order);
        }
        let finished_order_ids = finished_orders
            .into_iter()
            .map(|order| order.order_id)
            .collect::<HashSet<_>>();
        tracked_orders.retain(|order_ref| !finished_order_ids.contains(order_ref.order_id()));
        if tracked_orders.is_empty() {
            self.awaiting_progress.store(false, Ordering::SeqCst);
        }

        if !finished_order_ids.is_empty() {
            self.cancel_requested_order_ids
                .lock()
                .expect("target task cancel requested orders lock poisoned")
                .retain(|order_id| !finished_order_ids.contains(order_id));
        }
    }

    fn live_orders(&self, api: &tqsdk_wait::TqApi) -> Vec<Order> {
        self.prune_terminal_orders(api);
        self.tracked_orders
            .lock()
            .expect("target task tracked orders lock poisoned")
            .iter()
            .filter_map(|order_ref| order_ref.snapshot(api).ok().flatten())
            .filter(|order| !order_is_terminal(order))
            .collect()
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
        if self.managed_by_host {
            self.store
                .lock()
                .expect("target task store lock poisoned")
                .unregister(self.task_id);
        }
    }

    fn failure_result(&self) -> Result<()> {
        if let Some(error) = self
            .last_error
            .lock()
            .expect("target task last error lock poisoned")
            .clone()
        {
            return Err(error);
        }
        Ok(())
    }

    fn finish_with_error(&self, error: TaskError) {
        *self
            .last_error
            .lock()
            .expect("target task last error lock poisoned") = Some(error);
        self.finish();
    }
}

pub(crate) async fn process_target_tasks_wait_update(
    store: &Arc<Mutex<TargetPosStore>>,
    api: &mut tqsdk_wait::TqApi,
) {
    let tasks = store
        .lock()
        .expect("target task store lock poisoned")
        .live_tasks();
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

fn desired_order_for_target(
    api: &tqsdk_wait::TqApi,
    task: &TargetPosTaskInner,
    target_volume: i64,
    current_position: &Position,
) -> Option<DesiredOrder> {
    let quote = api.quote_ref(&task.symbol).snapshot(api).ok().flatten()?;
    let exchange_id = quote_exchange_id(&quote, &task.symbol);
    let order = compute_plan(
        &exchange_id,
        current_position,
        target_volume,
        task.config.offset_priority,
    )
    .into_iter()
    .flat_map(|batch| batch.orders.into_iter())
    .next()?;

    Some(DesiredOrder {
        direction: order.direction,
        offset: order.offset,
        volume: split_order_volume(order.volume, task.config.split_policy),
        limit_price: resolve_limit_price(&quote, order.direction, task.config.price_mode)?,
    })
}

fn should_cancel_for_replan(
    live_orders: &[Order],
    desired_order: Option<&DesiredOrder>,
    current_net_position: i64,
    target_volume: i64,
) -> bool {
    if live_orders.is_empty() {
        return false;
    }
    if current_net_position == target_volume {
        return true;
    }

    let Some(desired_order) = desired_order else {
        return false;
    };
    live_orders
        .iter()
        .any(|order| order_differs_from_desired(order, desired_order))
}

fn order_differs_from_desired(order: &Order, desired_order: &DesiredOrder) -> bool {
    order.direction != desired_order.direction.as_str()
        || order.offset != desired_order.offset.as_str()
        || order.volume_left != desired_order.volume
        || order.limit_price != desired_order.limit_price
}

fn quote_exchange_id(quote: &Quote, symbol: &str) -> String {
    if !quote.exchange_id.is_empty() {
        return quote.exchange_id.clone();
    }

    symbol
        .split_once('.')
        .map(|(exchange_id, _)| exchange_id.to_string())
        .unwrap_or_default()
}

fn resolve_limit_price(quote: &Quote, direction: TradeDirection, mode: PriceMode) -> Option<f64> {
    let active_price = match direction {
        TradeDirection::Buy => first_finite(quote.ask_price1, quote.bid_price1, quote.last_price),
        TradeDirection::Sell => first_finite(quote.bid_price1, quote.ask_price1, quote.last_price),
    };
    let passive_price = match direction {
        TradeDirection::Buy => first_finite(quote.bid_price1, quote.ask_price1, quote.last_price),
        TradeDirection::Sell => first_finite(quote.ask_price1, quote.bid_price1, quote.last_price),
    };

    let price = match mode {
        PriceMode::Active => active_price?,
        PriceMode::Passive => passive_price?,
    };

    Some(price)
}

fn first_finite(primary: f64, secondary: f64, fallback: f64) -> Option<f64> {
    if primary.is_finite() {
        Some(primary)
    } else if secondary.is_finite() {
        Some(secondary)
    } else {
        fallback.is_finite().then_some(fallback)
    }
}

fn split_order_volume(volume: i64, split_policy: Option<VolumeSplitPolicy>) -> i64 {
    match split_policy {
        None => volume,
        Some(policy) if volume < policy.max_volume => volume,
        Some(policy) => {
            let tail = volume - policy.max_volume;
            if tail > 0 && tail < policy.min_volume {
                volume - policy.min_volume
            } else {
                policy.max_volume
            }
        }
    }
}

fn order_is_terminal(order: &Order) -> bool {
    order.status == "FINISHED"
}

impl Drop for TargetPosTaskInner {
    fn drop(&mut self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(self.task_id);
        if self.managed_by_host {
            self.store
                .lock()
                .expect("target task store lock poisoned")
                .unregister(self.task_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tqsdk_core::{AdapterRegistry, MarketAdapter, RuntimeHandle};
    use tqsdk_session::{SessionClient, SessionFacadeConfig};
    use tqsdk_wait::TqApi;

    fn market_only_api() -> TqApi {
        let mut adapters = AdapterRegistry::new();
        adapters.register_adapter(MarketAdapter::default());
        let handle = RuntimeHandle::with_adapters(adapters);
        let session =
            SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
        TqApi::new(session)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancel_requested_task_records_error_when_cancel_submission_fails() {
        let registry = Arc::new(Mutex::new(TaskRegistry::default()));
        let store = Arc::new(Mutex::new(TargetPosStore::default()));
        let task = TargetPosBuilder::new(
            Arc::clone(&registry),
            Arc::clone(&store),
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
