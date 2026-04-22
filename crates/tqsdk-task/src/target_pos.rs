#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::json;
use tokio::sync::watch;
use tqsdk_core::{Position, Quote, TradeDirection};

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

#[derive(Default)]
pub(crate) struct TargetPosStore {
    tasks: HashMap<TaskId, Weak<TargetPosTaskInner>>,
}

struct TargetPosTaskInner {
    registry: Arc<Mutex<TaskRegistry>>,
    store: Arc<Mutex<TargetPosStore>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    config: TargetPosConfig,
    target_volume: Mutex<Option<i64>>,
    applied_target_volume: Mutex<Option<i64>>,
    last_error: Mutex<Option<TaskError>>,
    next_request_seq: AtomicU64,
    submitted_request_seq: AtomicU64,
    reached_tx: watch::Sender<u64>,
    finished_tx: watch::Sender<bool>,
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
        let (reached_tx, _) = watch::channel(0_u64);
        let (finished_tx, _) = watch::channel(false);

        let inner = Arc::new(TargetPosTaskInner {
            registry: Arc::clone(&self.registry),
            store: Arc::clone(&self.store),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            config: self.config,
            target_volume: Mutex::new(None),
            applied_target_volume: Mutex::new(None),
            last_error: Mutex::new(None),
            next_request_seq: AtomicU64::new(0),
            submitted_request_seq: AtomicU64::new(0),
            reached_tx,
            finished_tx,
            finished: AtomicBool::new(false),
        });
        self.store
            .lock()
            .expect("target task store lock poisoned")
            .register(Arc::clone(&inner));

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

    pub fn set_target_volume(&self, volume: i64) -> Result<()> {
        if self.is_finished() {
            return Err(TaskError::InvalidState(
                "target position task already finished",
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
        self.inner.finish();
        Ok(())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn applied_target_volume_for_test(&self) -> Option<i64> {
        *self
            .inner
            .applied_target_volume
            .lock()
            .expect("applied target volume lock poisoned")
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
        if current_net_position == target_volume {
            self.mark_reached(current_seq, target_volume);
            return;
        }

        if self.config.offset_priority != OffsetPriority::OpenOnly {
            self.mark_reached(current_seq, target_volume);
            return;
        }

        if self.submitted_request_seq.load(Ordering::SeqCst) >= current_seq {
            return;
        }

        let quote = match api.quote_ref(&self.symbol).snapshot(api).ok().flatten() {
            Some(quote) => quote,
            None => return,
        };
        let exchange_id = quote_exchange_id(&quote, &self.symbol);
        let Some(order) = compute_plan(
            &exchange_id,
            &current_position,
            target_volume,
            self.config.offset_priority,
        )
        .into_iter()
        .flat_map(|batch| batch.orders.into_iter())
        .next() else {
            return;
        };
        let Some(limit_price) =
            resolve_limit_price(&quote, order.direction, self.config.price_mode)
        else {
            return;
        };

        if api
            .insert_order(
                &self.account_id,
                &self.symbol,
                order.direction,
                Some(order.offset),
                order.volume,
                Some(json!(limit_price)),
            )
            .await
            .is_ok()
        {
            self.submitted_request_seq
                .store(current_seq, Ordering::SeqCst);
        }
    }

    fn mark_reached(&self, current_seq: u64, target_volume: i64) {
        *self
            .applied_target_volume
            .lock()
            .expect("applied target volume lock poisoned") = Some(target_volume);
        self.reached_tx.send_replace(current_seq);
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
        self.store
            .lock()
            .expect("target task store lock poisoned")
            .unregister(self.task_id);
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
        self.store
            .lock()
            .expect("target task store lock poisoned")
            .unregister(self.task_id);
    }
}
