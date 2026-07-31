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
    previous_tick_open_interest: Option<i64>,
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
            previous_tick_open_interest: None,
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
        let opening_open_interest = self
            .previous_tick_open_interest
            .unwrap_or(tick.open_interest);
        self.previous_tick_open_interest = Some(tick.open_interest);
        let Some(position) = self.session.locate(tick.datetime)? else {
            return Ok(None);
        };

        let mut closed = None;
        if self.trading_day_start_ns != Some(position.trading_day_start_ns) {
            closed = self.current.take().map(|bar| bar.row);
            self.trading_day_start_ns = Some(position.trading_day_start_ns);
        }

        let (bar_start_ns, effective_bar_end_ns) = self.bar_bounds(tick.datetime, position)?;
        let starts_new_bar = self.current.as_ref().is_none_or(|bar| {
            bar.trading_day_start_ns != position.trading_day_start_ns
                || bar.window_start_ns != position.window_start_ns
                || bar.row.datetime != bar_start_ns
        });
        let mut opened = None;
        let mut boundary_tick_closed_preceding_bar = false;
        if starts_new_bar {
            let preceding = self.current.take();
            let continues_session = preceding.as_ref().is_some_and(|bar| {
                bar.trading_day_start_ns == position.trading_day_start_ns
                    && bar.window_start_ns == position.window_start_ns
            });

            if continues_session && tick.datetime == bar_start_ns {
                let mut preceding = preceding.expect("continuing session has a preceding bar");
                Self::apply_tick(&mut preceding.row, tick, self.volume_delta(tick.volume));
                closed = Some(preceding.row);
                self.previous_cumulative_volume = tick.volume;
                let row = Self::new_tick_bar(bar_start_ns, tick, tick.open_interest);
                opened = Some(row.clone());
                self.current = Some(TickAggregateBar {
                    trading_day_start_ns: position.trading_day_start_ns,
                    window_start_ns: position.window_start_ns,
                    effective_bar_end_ns,
                    row,
                });
                boundary_tick_closed_preceding_bar = true;
            } else {
                let row = if continues_session {
                    Self::new_carry_bar(
                        bar_start_ns,
                        &preceding
                            .as_ref()
                            .expect("continuing session has a preceding bar")
                            .row,
                    )
                } else {
                    Self::new_tick_bar(bar_start_ns, tick, opening_open_interest)
                };
                if closed.is_none() {
                    closed = preceding.map(|bar| bar.row);
                }
                opened = Some(row.clone());
                self.current = Some(TickAggregateBar {
                    trading_day_start_ns: position.trading_day_start_ns,
                    window_start_ns: position.window_start_ns,
                    effective_bar_end_ns,
                    row,
                });
            }
        }

        if !boundary_tick_closed_preceding_bar {
            let volume_delta = self.volume_delta(tick.volume);
            let current = self
                .current
                .as_mut()
                .expect("valid Tick initializes a bar before aggregation");
            Self::apply_tick(&mut current.row, tick, volume_delta);
            self.previous_cumulative_volume = tick.volume;
        }
        let updated = self
            .current
            .as_ref()
            .expect("valid Tick initializes a bar before aggregation")
            .row
            .clone();

        Ok(Some(TickKlineAggregationUpdate {
            opened,
            updated,
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

    fn volume_delta(&self, volume: i64) -> i64 {
        if volume >= self.previous_cumulative_volume {
            volume.saturating_sub(self.previous_cumulative_volume)
        } else {
            // Continuous-index Tick streams can carry their cumulative volume
            // across weekends and holidays. A lower counter is the reliable
            // signal that the exchange actually reset it.
            volume.max(0)
        }
    }

    fn new_tick_bar(bar_start_ns: i64, tick: &Tick, open_oi: i64) -> Kline {
        Kline {
            id: bar_start_ns,
            datetime: bar_start_ns,
            open: tick.last_price,
            high: tick.last_price,
            low: tick.last_price,
            close: tick.last_price,
            volume: 0,
            open_oi,
            close_oi: tick.open_interest,
            ..Kline::default()
        }
    }

    fn new_carry_bar(bar_start_ns: i64, preceding: &Kline) -> Kline {
        Kline {
            id: bar_start_ns,
            datetime: bar_start_ns,
            open: preceding.close,
            high: preceding.close,
            low: preceding.close,
            close: preceding.close,
            volume: 0,
            open_oi: preceding.close_oi,
            close_oi: preceding.close_oi,
            ..Kline::default()
        }
    }

    fn apply_tick(row: &mut Kline, tick: &Tick, volume_delta: i64) {
        row.high = row.high.max(tick.last_price);
        row.low = row.low.min(tick.last_price);
        row.close = tick.last_price;
        row.volume = row.volume.saturating_add(volume_delta);
        row.close_oi = tick.open_interest;
    }
}
