use tqsdk_core::Kline;

use crate::{DataError, Result};

use super::{CANONICAL_MINUTE_KLINE_NS, KlineSessionPosition, KlineSessionTemplate};

/// One no-lookahead update from a closed canonical 60-second Kline.
#[derive(Debug, Clone)]
pub struct MinuteKlineAggregationUpdate {
    /// The newly opened session-aligned aggregate bar, if one begins here.
    pub opened: Option<Kline>,
    /// The aggregate bar after incorporating the closed source minute.
    pub updated: Kline,
    /// The aggregate bar finalized by this source minute, if any.
    pub closed: Option<Kline>,
    /// The close time of the source canonical minute.
    pub event_time_ns: i64,
}

/// Stateful `N × 60s` aggregator over final canonical minute rows.
///
/// Higher-period bars stay on the fixed 18:00 CST trading-day grid. Explicit
/// session windows gate which source minutes exist, but an intra-day break does
/// not restart a bar that spans that gap; this matches official server charts.
#[derive(Debug, Clone)]
pub struct MinuteKlineAggregator {
    duration_ns: i64,
    session: KlineSessionTemplate,
    current: Option<MinuteAggregateBar>,
}

#[derive(Debug, Clone)]
struct MinuteAggregateBar {
    trading_day_start_ns: i64,
    effective_bar_end_ns: i64,
    row: Kline,
}

impl MinuteKlineAggregator {
    /// Creates an aggregator for an integral number of canonical minutes.
    pub fn new(duration_ns: i64, session: KlineSessionTemplate) -> Result<Self> {
        if duration_ns <= CANONICAL_MINUTE_KLINE_NS || duration_ns % CANONICAL_MINUTE_KLINE_NS != 0
        {
            return Err(DataError::Validation(
                "aggregated kline duration must be an integer multiple greater than 60 seconds"
                    .to_string(),
            ));
        }
        Ok(Self {
            duration_ns,
            session,
            current: None,
        })
    }

    /// Returns the derived Kline duration in nanoseconds.
    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    /// Returns the session template used to align output bars.
    #[must_use]
    pub fn session(&self) -> &KlineSessionTemplate {
        &self.session
    }

    /// Incorporates one final, 60-second-aligned source Kline.
    pub fn update(
        &mut self,
        closed_minute: &Kline,
    ) -> Result<Option<MinuteKlineAggregationUpdate>> {
        if closed_minute.datetime < 0
            || closed_minute.datetime.rem_euclid(CANONICAL_MINUTE_KLINE_NS) != 0
        {
            return Err(DataError::Validation(
                "canonical minute kline datetime is not aligned to 60 seconds".to_string(),
            ));
        }
        let Some(position) = self.session.locate(closed_minute.datetime)? else {
            return Ok(None);
        };
        let event_time_ns = closed_minute
            .datetime
            .checked_add(CANONICAL_MINUTE_KLINE_NS)
            .ok_or_else(|| {
                DataError::Validation("canonical minute close timestamp overflow".to_string())
            })?;
        if event_time_ns > position.window_end_ns {
            return Err(DataError::Validation(
                "canonical minute kline crosses a configured session boundary".to_string(),
            ));
        }

        let (bar_start_ns, effective_bar_end_ns) =
            self.bar_bounds(closed_minute.datetime, position)?;
        let starts_new_bar = self.current.as_ref().is_none_or(|bar| {
            bar.trading_day_start_ns != position.trading_day_start_ns
                || bar.row.datetime != bar_start_ns
        });
        let mut closed = starts_new_bar
            .then(|| self.current.take().map(|bar| bar.row))
            .flatten();
        let opened = if starts_new_bar {
            let row = Kline {
                id: bar_start_ns,
                datetime: bar_start_ns,
                open: closed_minute.open,
                high: closed_minute.open,
                low: closed_minute.open,
                close: closed_minute.open,
                volume: 0,
                open_oi: closed_minute.open_oi,
                close_oi: closed_minute.open_oi,
                epoch: closed_minute.epoch,
            };
            self.current = Some(MinuteAggregateBar {
                trading_day_start_ns: position.trading_day_start_ns,
                effective_bar_end_ns,
                row: row.clone(),
            });
            Some(row)
        } else {
            None
        };

        let current = self
            .current
            .as_mut()
            .expect("valid canonical minute initializes an aggregate bar");
        current.row.high = current.row.high.max(closed_minute.high);
        current.row.low = current.row.low.min(closed_minute.low);
        current.row.close = closed_minute.close;
        current.row.volume = current
            .row
            .volume
            .saturating_add(closed_minute.volume.max(0));
        current.row.close_oi = closed_minute.close_oi;
        current.row.epoch = closed_minute.epoch.or(current.row.epoch);
        let updated = current.row.clone();
        if event_time_ns >= current.effective_bar_end_ns && closed.is_none() {
            closed = self.current.take().map(|bar| bar.row);
        }

        Ok(Some(MinuteKlineAggregationUpdate {
            opened,
            updated,
            closed,
            event_time_ns,
        }))
    }

    /// Finalizes the active bar when its trading-day-grid end is inside the
    /// scanned source range.
    pub fn finish_closed_through(&mut self, source_end_ns: i64) -> Option<Kline> {
        self.current
            .as_ref()
            .is_some_and(|bar| bar.effective_bar_end_ns <= source_end_ns)
            .then(|| self.current.take().map(|bar| bar.row))
            .flatten()
    }

    fn bar_bounds(&self, timestamp_ns: i64, position: KlineSessionPosition) -> Result<(i64, i64)> {
        let trading_day_offset_ns = timestamp_ns
            .checked_sub(position.trading_day_start_ns)
            .ok_or_else(|| {
                DataError::Validation("minute predates its trading-day grid".to_string())
            })?;
        let bucket_offset_ns = trading_day_offset_ns
            .div_euclid(self.duration_ns)
            .checked_mul(self.duration_ns)
            .ok_or_else(|| {
                DataError::Validation("minute kline bucket offset overflow".to_string())
            })?;
        let bar_start_ns = position
            .trading_day_start_ns
            .checked_add(bucket_offset_ns)
            .ok_or_else(|| {
                DataError::Validation("aggregated kline bar timestamp overflow".to_string())
            })?;
        let nominal_end_ns = bar_start_ns.checked_add(self.duration_ns).ok_or_else(|| {
            DataError::Validation("aggregated kline bar end overflow".to_string())
        })?;
        Ok((
            bar_start_ns,
            nominal_end_ns.min(position.trading_day_end_ns),
        ))
    }
}
