#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tqsdk_core::TradingCalendarDay;

use crate::Result;
use crate::calendar::TradingDayCalendar;
use crate::registry::TaskRegistry;
use crate::scheduler::TargetPosSchedulerStore;
use crate::target_pos::TargetPosStore;

pub(crate) type SharedTaskRegistry = SharedTaskState<TaskRegistry>;
pub(crate) type SharedTargetPosStore = SharedTaskState<TargetPosStore>;
pub(crate) type SharedTargetPosSchedulerStore = SharedTaskState<TargetPosSchedulerStore>;

pub(crate) struct TaskStateCell<T> {
    inner: Mutex<T>,
}

impl<T: Default> Default for TaskStateCell<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(T::default()),
        }
    }
}

impl<T> TaskStateCell<T> {
    pub(crate) fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let state = self.inner.lock().expect("task state lock poisoned");
        f(&state)
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut state = self.inner.lock().expect("task state lock poisoned");
        f(&mut state)
    }

    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut().expect("task state lock poisoned")
    }
}

pub(crate) struct SharedTaskState<T> {
    inner: Arc<Mutex<T>>,
}

impl<T> Clone for SharedTaskState<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Default> Default for SharedTaskState<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(T::default())),
        }
    }
}

impl<T> SharedTaskState<T> {
    pub(crate) fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let state = self.inner.lock().expect("shared task state lock poisoned");
        f(&state)
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut state = self.inner.lock().expect("shared task state lock poisoned");
        f(&mut state)
    }
}

#[derive(Clone, Default)]
pub(crate) struct SharedQuoteSubscriptions {
    inner: Arc<Mutex<HashSet<String>>>,
}

impl SharedQuoteSubscriptions {
    pub(crate) fn contains(&self, symbol: &str) -> bool {
        self.inner
            .lock()
            .expect("quote subscriptions lock poisoned")
            .contains(symbol)
    }

    pub(crate) fn insert(&self, symbol: impl Into<String>) {
        self.inner
            .lock()
            .expect("quote subscriptions lock poisoned")
            .insert(symbol.into());
    }
}

#[derive(Clone, Default)]
pub(crate) struct SharedTradingCalendar {
    inner: Arc<Mutex<TradingDayCalendar>>,
}

impl SharedTradingCalendar {
    pub(crate) fn snapshot(&self) -> TradingDayCalendar {
        self.inner
            .lock()
            .expect("trading calendar lock poisoned")
            .clone()
    }

    pub(crate) fn replace(&self, calendar: TradingDayCalendar) {
        *self.inner.lock().expect("trading calendar lock poisoned") = calendar;
    }

    pub(crate) fn extend(&self, days: impl IntoIterator<Item = TradingCalendarDay>) -> Result<()> {
        self.inner
            .lock()
            .expect("trading calendar lock poisoned")
            .try_extend_days(days)
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn shared_task_state_read_and_update_round_trips_value() {
        let shared = SharedTaskState::<usize>::default();
        let cloned = shared.clone();

        shared.with_mut(|value| *value = 42);

        assert_eq!(shared.with(|value| *value), 42);
        assert_eq!(cloned.with(|value| *value), 42);
    }

    #[test]
    fn task_state_cell_get_mut_updates_before_shared_access() {
        let mut cell = TaskStateCell::<Vec<&'static str>>::default();

        cell.get_mut().push("seeded");
        cell.with_mut(|items| items.push("updated"));

        assert_eq!(cell.with(|items| items.clone()), vec!["seeded", "updated"]);
    }

    #[test]
    fn shared_quote_subscriptions_deduplicates_symbols() {
        let subscriptions = SharedQuoteSubscriptions::default();
        let cloned = subscriptions.clone();

        subscriptions.insert("SHFE.au2606");
        subscriptions.insert("SHFE.au2606");

        assert!(subscriptions.contains("SHFE.au2606"));
        assert!(cloned.contains("SHFE.au2606"));
        assert!(!subscriptions.contains("DCE.m2605"));
    }

    #[test]
    fn shared_trading_calendar_replaces_days_atomically() {
        let calendar = SharedTradingCalendar::default();
        calendar
            .extend([
                TradingCalendarDay {
                    date: "2026-05-01".to_string(),
                    trading: false,
                },
                TradingCalendarDay {
                    date: "2026-05-04".to_string(),
                    trading: true,
                },
            ])
            .expect("calendar extension should succeed");

        let may_1 = NaiveDate::from_ymd_opt(2026, 5, 1).expect("fixture date should exist");
        let may_4 = NaiveDate::from_ymd_opt(2026, 5, 4).expect("fixture date should exist");
        assert_eq!(calendar.snapshot().day_status(may_1), Some(false));
        assert_eq!(calendar.snapshot().day_status(may_4), Some(true));

        calendar.replace(TradingDayCalendar::from_entries([(may_1, true)]));

        let snapshot = calendar.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.day_status(may_1), Some(true));
        assert_eq!(snapshot.day_status(may_4), None);
    }
}
