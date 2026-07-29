//! Session-aware aggregation of canonical, closed 60-second Klines.

use tqsdk_core::Kline;

use crate::{Result, TaskError};

/// Canonical source period accepted by [`MinuteKlineAggregator`].
pub const CANONICAL_MINUTE_KLINE_NS: i64 = tqsdk_data::MINUTE_KLINE_DURATION_NS;

/// One open interval, measured from the owning CST trading-day start.
///
/// Empty `windows` on [`MinuteKlineSessionTemplate`] means the complete CST
/// trading day is a single session.  Explicit windows reset aggregation at
/// breaks, preventing a 5m/15m/etc. bar from spanning a lunch or night break.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinuteKlineSessionWindow {
    pub start_offset_ns: i64,
    pub end_offset_ns: i64,
}

impl MinuteKlineSessionWindow {
    pub fn new(start_offset_ns: i64, end_offset_ns: i64) -> Result<Self> {
        if start_offset_ns < 0 || end_offset_ns <= start_offset_ns {
            return Err(TaskError::InvalidState(
                "minute kline session window must have non-negative increasing offsets",
            ));
        }
        Ok(Self {
            start_offset_ns,
            end_offset_ns,
        })
    }
}

/// Session topology whose stable hash is persisted with minute-cache files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinuteKlineSessionTemplate {
    snapshot_hash: String,
    windows: Vec<MinuteKlineSessionWindow>,
}

impl MinuteKlineSessionTemplate {
    pub fn new(
        snapshot_hash: impl Into<String>,
        mut windows: Vec<MinuteKlineSessionWindow>,
    ) -> Result<Self> {
        let snapshot_hash = snapshot_hash.into();
        if snapshot_hash.is_empty() {
            return Err(TaskError::InvalidState(
                "minute kline session snapshot hash must not be empty",
            ));
        }
        windows.sort_by_key(|window| window.start_offset_ns);
        for pair in windows.windows(2) {
            if pair[0].end_offset_ns > pair[1].start_offset_ns {
                return Err(TaskError::InvalidState(
                    "minute kline session windows must not overlap",
                ));
            }
        }
        Ok(Self {
            snapshot_hash,
            windows,
        })
    }

    /// Default market topology: one session per repository-standard CST
    /// trading day.  Instruments with breaks should provide explicit windows.
    #[must_use]
    pub fn cst_trading_day() -> Self {
        Self {
            snapshot_hash: "cst-trading-day-v1".to_string(),
            windows: Vec::new(),
        }
    }

    #[must_use]
    pub fn snapshot_hash(&self) -> &str {
        self.snapshot_hash.as_str()
    }

    #[must_use]
    pub fn windows(&self) -> &[MinuteKlineSessionWindow] {
        self.windows.as_slice()
    }

    fn session_start_for(&self, timestamp_ns: i64) -> Result<Option<i64>> {
        let trading_day = tqsdk_data::backtest_tick_trading_day_for_timestamp_ns(timestamp_ns)
            .map_err(data_error_to_task)?;
        let trading_day =
            tqsdk_data::backtest_tick_trading_day_range(trading_day).map_err(data_error_to_task)?;
        if self.windows.is_empty() {
            return Ok(Some(trading_day.start_ns));
        }
        for window in &self.windows {
            let start_ns = trading_day
                .start_ns
                .checked_add(window.start_offset_ns)
                .ok_or(TaskError::InvalidState(
                    "minute kline session start timestamp overflow",
                ))?;
            let end_ns = trading_day
                .start_ns
                .checked_add(window.end_offset_ns)
                .ok_or(TaskError::InvalidState(
                    "minute kline session end timestamp overflow",
                ))?;
            if end_ns > trading_day.end_ns {
                return Err(TaskError::InvalidState(
                    "minute kline session window exceeds its CST trading day",
                ));
            }
            if timestamp_ns >= start_ns && timestamp_ns < end_ns {
                return Ok(Some(start_ns));
            }
        }
        Ok(None)
    }
}

/// One no-lookahead update after a canonical 60-second bar has closed.
#[derive(Debug, Clone)]
pub struct MinuteKlineAggregationUpdate {
    /// Emitted exactly once at an aggregated bar's session-aligned open.
    pub opened: Option<Kline>,
    /// State after incorporating the just-closed canonical minute.
    pub updated: Kline,
    /// Equal to `closed_minute.datetime + 60s`.
    pub event_time_ns: i64,
}

/// Stateful aggregator for `N × 60s` Klines.
///
/// The caller invokes [`Self::update`] only after a canonical minute is known
/// closed.  It can therefore emit a bar open immediately and then one partial
/// update per closed minute without using future OHLC values.
#[derive(Debug, Clone)]
pub struct MinuteKlineAggregator {
    duration_ns: i64,
    session: MinuteKlineSessionTemplate,
    current: Option<AggregateBar>,
}

#[derive(Debug, Clone)]
struct AggregateBar {
    session_start_ns: i64,
    row: Kline,
}

impl MinuteKlineAggregator {
    pub fn new(duration_ns: i64, session: MinuteKlineSessionTemplate) -> Result<Self> {
        if duration_ns <= CANONICAL_MINUTE_KLINE_NS || duration_ns % CANONICAL_MINUTE_KLINE_NS != 0
        {
            return Err(TaskError::InvalidState(
                "aggregated kline duration must be an integer multiple greater than 60 seconds",
            ));
        }
        Ok(Self {
            duration_ns,
            session,
            current: None,
        })
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    #[must_use]
    pub fn session(&self) -> &MinuteKlineSessionTemplate {
        &self.session
    }

    /// Apply one final 60-second Kline.
    pub fn update(
        &mut self,
        closed_minute: &Kline,
    ) -> Result<Option<MinuteKlineAggregationUpdate>> {
        if closed_minute.datetime.rem_euclid(CANONICAL_MINUTE_KLINE_NS) != 0 {
            return Err(TaskError::InvalidState(
                "canonical minute kline datetime is not aligned to 60 seconds",
            ));
        }
        let Some(session_start_ns) = self.session.session_start_for(closed_minute.datetime)? else {
            self.current = None;
            return Ok(None);
        };
        let offset_ns =
            closed_minute
                .datetime
                .checked_sub(session_start_ns)
                .ok_or(TaskError::InvalidState(
                    "canonical minute predates its session start",
                ))?;
        let bar_start_ns = session_start_ns
            .checked_add(offset_ns.div_euclid(self.duration_ns) * self.duration_ns)
            .ok_or(TaskError::InvalidState(
                "aggregated kline bar timestamp overflow",
            ))?;
        let starts_new_bar = self.current.as_ref().is_none_or(|bar| {
            bar.session_start_ns != session_start_ns || bar.row.datetime != bar_start_ns
        });
        let opened = starts_new_bar.then(|| Kline {
            id: bar_start_ns.div_euclid(self.duration_ns),
            datetime: bar_start_ns,
            open: closed_minute.open,
            high: closed_minute.open,
            low: closed_minute.open,
            close: closed_minute.open,
            volume: 0,
            open_oi: closed_minute.open_oi,
            close_oi: closed_minute.open_oi,
            epoch: closed_minute.epoch,
        });
        if let Some(opened) = opened.as_ref() {
            self.current = Some(AggregateBar {
                session_start_ns,
                row: opened.clone(),
            });
        }
        let current = self
            .current
            .as_mut()
            .expect("a bar is initialized before update");
        current.row.high = current.row.high.max(closed_minute.high);
        current.row.low = current.row.low.min(closed_minute.low);
        current.row.close = closed_minute.close;
        current.row.volume = current
            .row
            .volume
            .saturating_add(closed_minute.volume.max(0));
        current.row.close_oi = closed_minute.close_oi;
        current.row.epoch = closed_minute.epoch.or(current.row.epoch);
        let event_time_ns = closed_minute
            .datetime
            .checked_add(CANONICAL_MINUTE_KLINE_NS)
            .ok_or(TaskError::InvalidState(
                "canonical minute close timestamp overflow",
            ))?;
        Ok(Some(MinuteKlineAggregationUpdate {
            opened,
            updated: current.row.clone(),
            event_time_ns,
        }))
    }
}

fn data_error_to_task(error: tqsdk_data::DataError) -> TaskError {
    TaskError::External(error.to_string())
}
