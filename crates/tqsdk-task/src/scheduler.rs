#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::{DateTime, Datelike, Days, FixedOffset, NaiveDate, NaiveTime, Utc};
use tokio::sync::watch;
use tqsdk_core::{Quote, TradingTime};

use crate::Result;
use crate::calendar::TradingDayCalendar;
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
    quote_subscriptions: Arc<Mutex<HashSet<String>>>,
    trading_calendar: Arc<Mutex<TradingDayCalendar>>,
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
    quote_subscriptions: Arc<Mutex<HashSet<String>>>,
    trading_calendar: Arc<Mutex<TradingDayCalendar>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    steps: Vec<TargetPosScheduleStep>,
    config: TargetPosSchedulerConfig,
    next_step_index: Mutex<usize>,
    current_step_clock: Mutex<Option<ActiveStepClock>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveStepClock {
    last_accounted_at: DateTime<FixedOffset>,
    active_elapsed: Duration,
}

impl TargetPosSchedulerBuilder {
    pub(crate) fn new(
        registry: Arc<Mutex<TaskRegistry>>,
        target_tasks: Arc<Mutex<TargetPosStore>>,
        store: Arc<Mutex<TargetPosSchedulerStore>>,
        quote_subscriptions: Arc<Mutex<HashSet<String>>>,
        trading_calendar: Arc<Mutex<TradingDayCalendar>>,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            target_tasks,
            store,
            quote_subscriptions,
            trading_calendar,
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
            quote_subscriptions: Arc::clone(&self.quote_subscriptions),
            trading_calendar: Arc::clone(&self.trading_calendar),
            task_id: task.id,
            account_id: self.account_id,
            symbol: self.symbol,
            steps: self.steps,
            config: self.config,
            next_step_index: Mutex::new(0),
            current_step_clock: Mutex::new(None),
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

            if matches!(phase, ActiveStepPhase::Running)
                && !self.step_deadline_elapsed(api, step_index)
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
            .current_step_clock
            .lock()
            .expect("scheduler step clock lock poisoned")
            .is_some()
        {
            return;
        }

        let step = self.steps[step_index].clone();
        *self
            .current_step_clock
            .lock()
            .expect("scheduler step clock lock poisoned") = Some(ActiveStepClock {
            last_accounted_at: shanghai_now(),
            active_elapsed: Duration::ZERO,
        });
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
            Arc::clone(&self.quote_subscriptions),
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

    fn step_deadline_elapsed(&self, api: &tqsdk_wait::TqApi, step_index: usize) -> bool {
        let quote = api.quote_ref(&self.symbol).snapshot(api).ok().flatten();
        let mut step_clock = self
            .current_step_clock
            .lock()
            .expect("scheduler step clock lock poisoned");
        let Some(step_clock) = step_clock.as_mut() else {
            return false;
        };

        let now = shanghai_now();
        let trading_calendar = self
            .trading_calendar
            .lock()
            .expect("trading calendar lock poisoned");
        let elapsed = effective_step_elapsed(
            step_clock.last_accounted_at,
            now,
            quote.as_ref(),
            Some(&trading_calendar),
        );
        step_clock.last_accounted_at = now;
        step_clock.active_elapsed = step_clock.active_elapsed.saturating_add(elapsed);
        step_clock.active_elapsed >= self.steps[step_index].interval
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
            .current_step_clock
            .lock()
            .expect("scheduler step clock lock poisoned") = None;
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
            .current_step_clock
            .lock()
            .expect("scheduler step clock lock poisoned") = None;
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

fn shanghai_now() -> DateTime<FixedOffset> {
    Utc::now().with_timezone(&china_tz())
}

fn china_tz() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("UTC+8 fixed offset should be valid")
}

fn effective_step_elapsed(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    quote: Option<&Quote>,
    calendar: Option<&TradingDayCalendar>,
) -> Duration {
    quote
        .and_then(|quote| trading_time_elapsed_between(start, end, &quote.trading_time, calendar))
        .unwrap_or_else(|| wall_elapsed(start, end))
}

fn trading_time_elapsed_between(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    trading_time: &TradingTime,
    calendar: Option<&TradingDayCalendar>,
) -> Option<Duration> {
    if end <= start {
        return Some(Duration::ZERO);
    }

    let min_date = start
        .date_naive()
        .checked_sub_days(Days::new(1))
        .unwrap_or(start.date_naive());
    let max_date = end.date_naive();
    let mut date = min_date;
    let mut total = Duration::ZERO;
    let mut saw_valid_window = has_valid_trading_window(trading_time);

    while date <= max_date {
        let next_date = date.checked_add_days(Days::new(1))?;
        let day_open = is_trading_day(date, calendar);
        let next_day_open = is_trading_day(next_date, calendar);
        let night_open = day_open && next_day_open;

        if day_open {
            let (elapsed, saw_valid) =
                trading_windows_overlap(start, end, date, &trading_time.day, false);
            total = total.saturating_add(elapsed);
            saw_valid_window |= saw_valid;
        }
        if night_open {
            let (elapsed, saw_valid) =
                trading_windows_overlap(start, end, date, &trading_time.night, true);
            total = total.saturating_add(elapsed);
            saw_valid_window |= saw_valid;
        }

        date = next_date;
    }

    saw_valid_window.then_some(total)
}

fn has_valid_trading_window(trading_time: &TradingTime) -> bool {
    trading_time
        .day
        .iter()
        .any(|window| parse_trading_window(window, false).is_some())
        || trading_time
            .night
            .iter()
            .any(|window| parse_trading_window(window, true).is_some())
}

fn trading_windows_overlap(
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    date: NaiveDate,
    windows: &[Vec<String>],
    allow_cross_midnight: bool,
) -> (Duration, bool) {
    let mut total = Duration::ZERO;
    let mut saw_valid_window = false;

    for window in windows {
        let Some((window_start, window_end, crosses_midnight)) =
            parse_trading_window(window, allow_cross_midnight)
        else {
            continue;
        };
        saw_valid_window = true;

        let interval_start = date
            .and_time(window_start)
            .and_local_timezone(china_tz())
            .single();
        let interval_end_date = if crosses_midnight {
            date.checked_add_days(Days::new(1))
        } else {
            Some(date)
        };
        let interval_end = interval_end_date.and_then(|interval_end_date| {
            interval_end_date
                .and_time(window_end)
                .and_local_timezone(china_tz())
                .single()
        });

        let (Some(interval_start), Some(interval_end)) = (interval_start, interval_end) else {
            continue;
        };
        if interval_end <= interval_start {
            continue;
        }

        let overlap_start = start.max(interval_start);
        let overlap_end = end.min(interval_end);
        total = total.saturating_add(wall_elapsed(overlap_start, overlap_end));
    }

    (total, saw_valid_window)
}

fn parse_trading_window(
    window: &[String],
    allow_cross_midnight: bool,
) -> Option<(NaiveTime, NaiveTime, bool)> {
    let start = NaiveTime::parse_from_str(window.first()?, "%H:%M:%S").ok()?;
    let end = NaiveTime::parse_from_str(window.get(1)?, "%H:%M:%S").ok()?;
    if start == end {
        return None;
    }
    let crosses_midnight = allow_cross_midnight && end < start;
    if !crosses_midnight && end <= start {
        return None;
    }
    Some((start, end, crosses_midnight))
}

fn wall_elapsed(start: DateTime<FixedOffset>, end: DateTime<FixedOffset>) -> Duration {
    end.signed_duration_since(start)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn is_weekday(date: NaiveDate) -> bool {
    date.weekday().number_from_monday() <= 5
}

fn is_trading_day(date: NaiveDate, calendar: Option<&TradingDayCalendar>) -> bool {
    calendar
        .and_then(|calendar| calendar.day_status(date))
        .unwrap_or_else(|| is_weekday(date))
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
    use chrono::TimeZone;
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
        let quote_subscriptions = Arc::new(Mutex::new(HashSet::new()));
        let scheduler = TargetPosSchedulerBuilder::new(
            Arc::clone(&registry),
            Arc::clone(&target_tasks),
            Arc::clone(&schedulers),
            Arc::clone(&quote_subscriptions),
            Arc::new(Mutex::new(TradingDayCalendar::default())),
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
            Arc::clone(&quote_subscriptions),
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

    #[test]
    fn trading_time_elapsed_counts_only_open_day_windows() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 22, 11, 25, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 13, 35, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![
                vec!["09:00:00".to_string(), "10:15:00".to_string()],
                vec!["10:30:00".to_string(), "11:30:00".to_string()],
                vec!["13:30:00".to_string(), "15:00:00".to_string()],
            ],
            night: Vec::new(),
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("day windows should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(10 * 60));
    }

    #[test]
    fn trading_time_elapsed_skips_weekend_and_closed_hours() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 24, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 27, 9, 5, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![vec!["09:00:00".to_string(), "15:00:00".to_string()]],
            night: vec![vec!["21:00:00".to_string(), "23:00:00".to_string()]],
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("day/night windows should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(5 * 60));
    }

    #[test]
    fn trading_time_elapsed_uses_calendar_closed_days_when_available() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 30, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 5, 1, 9, 5, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: vec![vec!["09:00:00".to_string(), "15:00:00".to_string()]],
            night: vec![vec!["21:00:00".to_string(), "23:00:00".to_string()]],
        };
        let calendar = TradingDayCalendar::from_entries([
            (
                NaiveDate::from_ymd_opt(2026, 4, 30).expect("valid date"),
                true,
            ),
            (
                NaiveDate::from_ymd_opt(2026, 5, 1).expect("valid date"),
                false,
            ),
        ]);

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, Some(&calendar))
            .expect("day/night windows should produce elapsed");

        assert_eq!(elapsed, Duration::ZERO);
    }

    #[test]
    fn trading_time_elapsed_supports_cross_midnight_night_window() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 21, 22, 55, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 0, 35, 0)
            .single()
            .expect("valid datetime");
        let trading_time = TradingTime {
            day: Vec::new(),
            night: vec![vec!["21:00:00".to_string(), "01:00:00".to_string()]],
        };

        let elapsed = trading_time_elapsed_between(start, end, &trading_time, None)
            .expect("cross-midnight night window should produce elapsed");

        assert_eq!(elapsed, Duration::from_secs(100 * 60));
    }

    #[test]
    fn effective_step_elapsed_falls_back_to_wall_clock_without_valid_schedule() {
        let tz = china_tz();
        let start = tz
            .with_ymd_and_hms(2026, 4, 22, 11, 25, 0)
            .single()
            .expect("valid datetime");
        let end = tz
            .with_ymd_and_hms(2026, 4, 22, 13, 35, 0)
            .single()
            .expect("valid datetime");
        let quote = Quote {
            trading_time: TradingTime::default(),
            ..Quote::default()
        };

        assert_eq!(
            effective_step_elapsed(start, end, Some(&quote), None),
            Duration::from_secs(130 * 60)
        );
    }
}
