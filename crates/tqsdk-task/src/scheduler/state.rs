use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use chrono::{DateTime, FixedOffset};

use crate::registry::TaskId;
use crate::target_pos::{TargetPosTask, TargetPosTaskExecutionEvent, TargetPosTaskTradeFill};

use super::{
    TargetPosExecutionReport, TargetPosSchedulerExecutionEvent, TargetPosSchedulerInner,
    TargetPosSchedulerTradeFill, TargetPosStepOutcomeReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActiveStepPhase {
    Running,
    Cancelling,
}

#[derive(Default)]
pub(crate) struct TargetPosSchedulerStore {
    schedulers: HashMap<TaskId, Weak<TargetPosSchedulerInner>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ActiveStepClock {
    pub(super) last_accounted_at: DateTime<FixedOffset>,
    pub(super) active_elapsed: Duration,
}

pub(super) struct TargetPosSchedulerState {
    pub(super) next_step_index: usize,
    pub(super) current_step_clock: Option<ActiveStepClock>,
    pub(super) current_step_phase: ActiveStepPhase,
    pub(super) active_task: Option<TargetPosTask>,
    pub(super) active_task_report_len: usize,
    pub(super) report: TargetPosExecutionReport,
    pub(super) events: Vec<TargetPosSchedulerExecutionEvent>,
    pub(super) last_error: Option<crate::TaskError>,
}

impl Default for TargetPosSchedulerState {
    fn default() -> Self {
        Self {
            next_step_index: 0,
            current_step_clock: None,
            current_step_phase: ActiveStepPhase::Running,
            active_task: None,
            active_task_report_len: 0,
            report: TargetPosExecutionReport::default(),
            events: Vec::new(),
            last_error: None,
        }
    }
}

impl TargetPosSchedulerStore {
    pub(super) fn register(&mut self, scheduler: Arc<TargetPosSchedulerInner>) {
        self.schedulers
            .insert(scheduler.task_id, Arc::downgrade(&scheduler));
    }

    pub(super) fn live_schedulers(&mut self) -> Vec<Arc<TargetPosSchedulerInner>> {
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

    pub(super) fn record_step_event(
        &mut self,
        step_index: usize,
        event: &TargetPosTaskExecutionEvent,
    ) {
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
    pub(super) fn with_state<R>(&self, f: impl FnOnce(&TargetPosSchedulerState) -> R) -> R {
        self.state.with(f)
    }

    pub(super) fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut TargetPosSchedulerState) -> R,
    ) -> R {
        self.state.with_mut(f)
    }
}
