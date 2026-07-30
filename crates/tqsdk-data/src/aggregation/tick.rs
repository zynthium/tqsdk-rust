use tqsdk_core::{Kline, Tick};

use crate::{DataError, Result};

use super::{CANONICAL_MINUTE_KLINE_NS, KlineSessionPosition, KlineSessionTemplate};

/// One no-lookahead update produced from a Tick row.
#[derive(Debug, Clone)]
pub struct TickKlineAggregationUpdate {
    /// The newly opened session-aligned bar, if this Tick starts one.
    pub opened: Option<Kline>,
    /// The current bar after incorporating this Tick.
    pub updated: Kline,
    /// The preceding bar when this Tick advances to a new bar, session, or day.
    pub closed: Option<Kline>,
    /// The Tick timestamp used for replay ordering.
    pub event_time_ns: i64,
}

/// Stateful Tick-to-sub-minute Kline aggregator.
#[derive(Debug, Clone)]
pub struct TickKlineAggregator {
    symbol: String,
    duration_ns: i64,
    session: KlineSessionTemplate,
    current: Option<TickAggregateBar>,
    trading_day_start_ns: Option<i64>,
    previous_cumulative_volume: i64,
}

#[derive(Debug, Clone)]
struct TickAggregateBar {
    trading_day_start_ns: i64,
    window_start_ns: i64,
    effective_bar_end_ns: i64,
    row: Kline,
}

impl TickKlineAggregator {
    /// Creates a session-aware aggregator for `0 < duration < 60s`.
    pub fn new(
        symbol: impl Into<String>,
        duration_ns: i64,
        session: KlineSessionTemplate,
    ) -> Result<Self> {
        let symbol = symbol.into();
        if symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "tick kline symbol must not be empty".to_string(),
            ));
        }
        if duration_ns <= 0 || duration_ns >= CANONICAL_MINUTE_KLINE_NS {
            return Err(DataError::Validation(
                "tick kline duration must satisfy 0 < duration < 60 seconds".to_string(),
            ));
        }
        Ok(Self {
            symbol,
            duration_ns,
            session,
            current: None,
            trading_day_start_ns: None,
            previous_cumulative_volume: 0,
        })
    }

    /// Returns the logical symbol used in output rows.
    #[must_use]
    pub fn symbol(&self) -> &str {
        self.symbol.as_str()
    }

    /// Returns the derived Kline duration in nanoseconds.
    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    /// Returns the session template used to align bars.
    #[must_use]
    pub fn session(&self) -> &KlineSessionTemplate {
        &self.session
    }

    /// Incorporates one Tick. Invalid timestamps/prices and configured breaks
    /// are intentionally ignored rather than producing a bar.
    pub fn update(&mut self, tick: &Tick) -> Result<Option<TickKlineAggregationUpdate>> {
        if tick.datetime < 0 || !tick.last_price.is_finite() {
            return Ok(None);
        }
        let Some(position) = self.session.locate(tick.datetime)? else {
            return Ok(None);
        };

        let mut closed = None;
        if self.trading_day_start_ns != Some(position.trading_day_start_ns) {
            closed = self.current.take().map(|bar| bar.row);
            self.trading_day_start_ns = Some(position.trading_day_start_ns);
            self.previous_cumulative_volume = 0;
        }

        let (bar_start_ns, effective_bar_end_ns) = self.bar_bounds(tick.datetime, position)?;
        let starts_new_bar = self.current.as_ref().is_none_or(|bar| {
            bar.trading_day_start_ns != position.trading_day_start_ns
                || bar.window_start_ns != position.window_start_ns
                || bar.row.datetime != bar_start_ns
        });
        let opened = if starts_new_bar {
            if closed.is_none() {
                closed = self.current.take().map(|bar| bar.row);
            } else {
                self.current = None;
            }
            let row = Kline {
                id: bar_start_ns,
                datetime: bar_start_ns,
                open: tick.last_price,
                high: tick.last_price,
                low: tick.last_price,
                close: tick.last_price,
                volume: 0,
                open_oi: tick.open_interest,
                close_oi: tick.open_interest,
                ..Kline::default()
            };
            self.current = Some(TickAggregateBar {
                trading_day_start_ns: position.trading_day_start_ns,
                window_start_ns: position.window_start_ns,
                effective_bar_end_ns,
                row: row.clone(),
            });
            Some(row)
        } else {
            None
        };

        let volume_delta = if tick.volume >= self.previous_cumulative_volume {
            tick.volume.saturating_sub(self.previous_cumulative_volume)
        } else {
            0
        };
        let current = self
            .current
            .as_mut()
            .expect("valid Tick initializes a bar before aggregation");
        current.row.high = current.row.high.max(tick.last_price);
        current.row.low = current.row.low.min(tick.last_price);
        current.row.close = tick.last_price;
        current.row.volume = current.row.volume.saturating_add(volume_delta);
        current.row.close_oi = tick.open_interest;
        self.previous_cumulative_volume = tick.volume;

        Ok(Some(TickKlineAggregationUpdate {
            opened,
            updated: current.row.clone(),
            closed,
            event_time_ns: tick.datetime,
        }))
    }

    /// Finalizes the currently open bar when its effective session-aligned end
    /// is inside the scanned source range.
    pub fn finish_closed_through(&mut self, source_end_ns: i64) -> Option<Kline> {
        self.current
            .as_ref()
            .is_some_and(|bar| bar.effective_bar_end_ns <= source_end_ns)
            .then(|| self.current.take().map(|bar| bar.row))
            .flatten()
    }

    fn bar_bounds(&self, timestamp_ns: i64, position: KlineSessionPosition) -> Result<(i64, i64)> {
        let window_offset_ns = timestamp_ns
            .checked_sub(position.window_start_ns)
            .ok_or_else(|| DataError::Validation("tick predates its session window".to_string()))?;
        let bucket_offset_ns = window_offset_ns
            .div_euclid(self.duration_ns)
            .checked_mul(self.duration_ns)
            .ok_or_else(|| {
                DataError::Validation("tick kline bucket offset overflow".to_string())
            })?;
        let bar_start_ns = position
            .window_start_ns
            .checked_add(bucket_offset_ns)
            .ok_or_else(|| {
                DataError::Validation("tick kline bar timestamp overflow".to_string())
            })?;
        let nominal_end_ns = bar_start_ns
            .checked_add(self.duration_ns)
            .ok_or_else(|| DataError::Validation("tick kline bar end overflow".to_string()))?;
        Ok((bar_start_ns, nominal_end_ns.min(position.window_end_ns)))
    }
}
