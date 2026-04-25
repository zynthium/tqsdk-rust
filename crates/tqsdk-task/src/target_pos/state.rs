use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::OrderRef;

use crate::registry::TaskId;
use crate::TaskError;

use super::{TargetPosTaskExecutionReport, TargetPosTaskInner};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct DesiredOrder {
    pub(super) direction: TradeDirection,
    pub(super) offset: TradeOffset,
    pub(super) volume: i64,
    pub(super) limit_price: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct DesiredBatch {
    pub(super) orders: Vec<DesiredOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct LiveOrderReconciliation {
    pub(super) stale_order_ids: HashSet<String>,
    pub(super) missing_batch: DesiredBatch,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum LiveOrderHandling {
    NoLiveOrders,
    Blocked,
    SubmitMissing(DesiredBatch),
}

#[derive(Default)]
pub(crate) struct TargetPosStore {
    tasks: HashMap<TaskId, Weak<TargetPosTaskInner>>,
}

#[derive(Default)]
pub(super) struct TargetPosTaskState {
    pub(super) target_volume: Option<i64>,
    pub(super) applied_target_volume: Option<i64>,
    pub(super) last_error: Option<TaskError>,
    pub(super) submitted_net_position: Option<i64>,
    pub(super) tracked_orders: Vec<OrderRef>,
    pub(super) known_order_ids: HashSet<String>,
    pub(super) cancel_requested_order_ids: HashSet<String>,
    pub(super) seen_trade_ids: HashSet<String>,
    pub(super) report: TargetPosTaskExecutionReport,
}

impl TargetPosStore {
    pub(super) fn register(&mut self, task: Arc<TargetPosTaskInner>) {
        self.tasks.insert(task.task_id, Arc::downgrade(&task));
    }

    pub(super) fn unregister(&mut self, task_id: TaskId) {
        self.tasks.remove(&task_id);
    }

    pub(super) fn live_tasks(&mut self) -> Vec<Arc<TargetPosTaskInner>> {
        self.tasks.retain(|_, weak| weak.strong_count() > 0);
        self.tasks.values().filter_map(Weak::upgrade).collect()
    }
}
