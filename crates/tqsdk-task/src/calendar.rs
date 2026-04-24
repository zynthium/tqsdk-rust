#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use chrono::NaiveDate;
use tqsdk_core::TradingCalendarDay;

use crate::{Result, TaskError};

/// Minimal trading-day cache used by task schedulers.
///
/// Missing dates intentionally fall back to the scheduler's weekday rule, so a
/// partial or unavailable calendar never blocks task progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradingDayCalendar {
    days: HashMap<NaiveDate, bool>,
}

impl TradingDayCalendar {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = (NaiveDate, bool)>) -> Self {
        let mut calendar = Self::new();
        calendar.extend_entries(entries);
        calendar
    }

    pub fn try_from_days(days: impl IntoIterator<Item = TradingCalendarDay>) -> Result<Self> {
        let mut calendar = Self::new();
        calendar.try_extend_days(days)?;
        Ok(calendar)
    }

    pub fn extend_entries(&mut self, entries: impl IntoIterator<Item = (NaiveDate, bool)>) {
        self.days.extend(entries);
    }

    pub fn try_extend_days(
        &mut self,
        days: impl IntoIterator<Item = TradingCalendarDay>,
    ) -> Result<()> {
        for day in days {
            let date = NaiveDate::parse_from_str(&day.date, "%Y-%m-%d").map_err(|_| {
                TaskError::InvalidCalendarDate {
                    date: day.date.clone(),
                }
            })?;
            self.days.insert(date, day.trading);
        }
        Ok(())
    }

    pub fn insert(&mut self, date: NaiveDate, trading: bool) -> Option<bool> {
        self.days.insert(date, trading)
    }

    #[must_use]
    pub fn day_status(&self, date: NaiveDate) -> Option<bool> {
        self.days.get(&date).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.days.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.days.is_empty()
    }
}
