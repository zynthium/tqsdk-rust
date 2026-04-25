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
