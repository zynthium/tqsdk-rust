#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tqsdk_core::TradingCalendarDay;

use crate::Result;
use crate::calendar::TradingDayCalendar;

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
