use std::sync::atomic::Ordering;

use crate::Result;
use crate::config::PriceMode;
use crate::shared::SharedTargetPosSchedulerStore;
use crate::target_pos::{TargetPosBuilder, TargetPosTask};

use super::planner::{effective_step_elapsed, shanghai_now};
use super::state::{ActiveStepClock, ActiveStepPhase, TargetPosSchedulerStore};
use super::{
    TargetPosExecutionStep, TargetPosSchedulerExecutionEvent, TargetPosSchedulerInner,
    TargetPosStepOutcomeReport,
};

impl TargetPosSchedulerInner {
    pub(super) async fn process_wait_update(&self, api: &mut tqsdk_wait::TqApi) {
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

            if matches!(phase, ActiveStepPhase::Running) && step.price_mode.is_some() {
                if let Err(error) = self.ensure_quote_ref(api).await {
                    self.finish_with_error(error);
                    return;
                }
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
        let step = self.steps[step_index].clone();
        let started = self.with_state_mut(|state| {
            if state.current_step_clock.is_some() {
                return false;
            }

            state.current_step_clock = Some(ActiveStepClock {
                last_accounted_at: shanghai_now(),
                active_elapsed: std::time::Duration::ZERO,
            });
            state.current_step_phase = ActiveStepPhase::Running;
            state.active_task_report_len = 0;
            debug_assert_eq!(state.report.applied_steps.len(), step_index);
            debug_assert_eq!(state.report.step_outcomes.len(), step_index);
            state.report.applied_steps.push(TargetPosExecutionStep {
                step_index,
                target_volume: step.target_volume,
            });
            state.report.step_outcomes.push(TargetPosStepOutcomeReport {
                step_index,
                target_volume: step.target_volume,
                ..TargetPosStepOutcomeReport::default()
            });
            true
        });
        if !started {
            return;
        }

        let Some(price_mode) = step.price_mode else {
            return;
        };

        match self.build_step_task(step.target_volume, price_mode) {
            Ok(task) => {
                self.with_state_mut(|state| {
                    state.active_task = Some(task);
                });
            }
            Err(error) => self.finish_with_error(error),
        }
    }

    fn build_step_task(&self, target_volume: i64, price_mode: PriceMode) -> Result<TargetPosTask> {
        let mut builder = TargetPosBuilder::new(
            self.registry.clone(),
            self.target_tasks.clone(),
            self.quote_subscriptions.clone(),
            self.account_id.clone(),
            self.symbol.clone(),
        )
        .price_mode(price_mode)
        .offset_priority(self.config.offset_priority());
        if let Some(policy) = self.config.split_policy() {
            builder = builder.split_policy(policy);
        }

        let task = builder.build_internal()?;
        task.set_target_volume(target_volume)?;
        Ok(task)
    }

    fn current_step_index(&self) -> Option<usize> {
        let next_step_index = self.with_state(|state| state.next_step_index);
        (next_step_index < self.steps.len()).then_some(next_step_index)
    }

    fn active_task(&self) -> Option<TargetPosTask> {
        self.with_state(|state| state.active_task.clone())
    }

    async fn ensure_quote_ref(&self, api: &mut tqsdk_wait::TqApi) -> Result<()> {
        if self.with_state(|state| state.quote.is_some()) {
            return Ok(());
        }

        let quote = api
            .quote(&self.symbol)
            .await
            .map_err(crate::TaskError::from)?;
        self.with_state_mut(|state| {
            state.quote = Some(quote);
        });
        if !self.quote_subscriptions.contains(&self.symbol) {
            self.quote_subscriptions.insert(self.symbol.clone());
        }
        Ok(())
    }

    fn step_deadline_elapsed(&self, step_index: usize) -> bool {
        let quote = self
            .with_state(|state| state.quote.clone())
            .and_then(|quote| quote.snapshot().ok().flatten());
        let now = shanghai_now();
        let trading_calendar = self.trading_calendar.snapshot();
        self.with_state_mut(|state| {
            let Some(step_clock) = state.current_step_clock.as_mut() else {
                return false;
            };

            let elapsed = effective_step_elapsed(
                step_clock.last_accounted_at,
                now,
                quote.as_ref(),
                Some(&trading_calendar),
            );
            step_clock.last_accounted_at = now;
            step_clock.active_elapsed = step_clock.active_elapsed.saturating_add(elapsed);
            step_clock.active_elapsed >= self.steps[step_index].interval
        })
    }

    fn current_step_phase(&self) -> ActiveStepPhase {
        self.with_state(|state| state.current_step_phase)
    }

    fn mark_current_step_cancelling(&self) {
        self.with_state_mut(|state| {
            state.current_step_phase = ActiveStepPhase::Cancelling;
        });
    }

    fn advance_step(&self) -> bool {
        let should_finish = self.with_state_mut(|state| {
            state.active_task = None;
            state.active_task_report_len = 0;
            state.current_step_clock = None;
            state.current_step_phase = ActiveStepPhase::Running;
            state.next_step_index += 1;
            state.next_step_index >= self.steps.len()
        });
        if should_finish {
            self.finish();
            return false;
        }
        true
    }

    pub(super) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    pub(super) fn cancel_active_task(&self) {
        let task = self.with_state_mut(|state| {
            let task = state.active_task.take();
            state.active_task_report_len = 0;
            state.current_step_clock = None;
            task
        });
        if let Some(task) = task {
            task.cancel_internal();
        }
    }

    fn collect_active_task_events(&self, step_index: usize) {
        let Some((task, report_len)) = self.with_state(|state| {
            state
                .active_task
                .clone()
                .map(|task| (task, state.active_task_report_len))
        }) else {
            return;
        };
        let (next_report_len, new_events) = task.execution_events_since(report_len);
        if new_events.is_empty() {
            return;
        }

        self.with_state_mut(|state| {
            for event in new_events {
                state.report.record_step_event(step_index, &event);
                state
                    .events
                    .push(TargetPosSchedulerExecutionEvent { step_index, event });
            }
            state.active_task_report_len = next_report_len;
        });
    }

    pub(super) fn finish(&self) {
        self.cancel_active_task();
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.finished_tx.send_replace(true);
        self.registry
            .with_mut(|registry| registry.unregister_task(self.task_id));
    }

    fn finish_with_error(&self, error: crate::TaskError) {
        self.with_state_mut(|state| {
            state.last_error = Some(error);
        });
        self.finish();
    }

    pub(super) fn failure_result(&self) -> Result<()> {
        if let Some(error) = self.with_state(|state| state.last_error.clone()) {
            return Err(error);
        }
        Ok(())
    }
}

fn task_failure(task: &TargetPosTask) -> Option<crate::TaskError> {
    task.is_finished().then(|| task.last_error()).flatten()
}

pub(crate) async fn process_schedulers_wait_update(
    store: &SharedTargetPosSchedulerStore,
    api: &mut tqsdk_wait::TqApi,
) {
    let schedulers = store.with_mut(TargetPosSchedulerStore::live_schedulers);
    for scheduler in schedulers {
        scheduler.process_wait_update(api).await;
    }
}
