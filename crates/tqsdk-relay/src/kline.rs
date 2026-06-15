#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::error::{RelayError, RelayResult};
use crate::protocol::{RelayKlineRow, RelayTickRow};

#[derive(Debug, Clone)]
pub struct KlineSynthesis {
    symbol: String,
    duration_ns: i64,
    current: Option<MutableKline>,
    next_id: i64,
    last_tick_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct MutableKline {
    row: RelayKlineRow,
    first_volume: i64,
    last_volume: i64,
}

impl KlineSynthesis {
    #[must_use]
    pub fn new(symbol: impl Into<String>, duration_ns: i64) -> Self {
        Self::try_new(symbol, duration_ns)
            .expect("KlineSynthesis::new requires positive duration_ns")
    }

    pub fn try_new(symbol: impl Into<String>, duration_ns: i64) -> RelayResult<Self> {
        if duration_ns <= 0 {
            return Err(RelayError::invalid_config(
                "kline duration_ns must be greater than zero",
            ));
        }
        Ok(Self {
            symbol: symbol.into(),
            duration_ns,
            current: None,
            next_id: 0,
            last_tick_id: None,
        })
    }

    pub fn push_tick(&mut self, tick: RelayTickRow) -> RelayResult<Vec<RelayKlineRow>> {
        let start = window_start(tick.datetime, self.duration_ns);
        let mut completed = Vec::new();

        if self.last_tick_id.is_some_and(|last_id| tick.id < last_id) {
            return Ok(completed);
        }
        if self
            .current
            .as_ref()
            .is_some_and(|current| start < current.row.datetime)
        {
            return Ok(completed);
        }
        self.last_tick_id = Some(tick.id);

        match self.current.take() {
            None => {
                self.current = Some(self.new_bar(start, &tick));
            }
            Some(mut current) if current.row.datetime == start => {
                merge_tick(&mut current, &tick);
                self.current = Some(current);
            }
            Some(current) => {
                completed.push(finalize(current));
                self.current = Some(self.new_bar(start, &tick));
            }
        }

        Ok(completed)
    }

    #[must_use]
    pub fn current_bar(&self) -> Option<RelayKlineRow> {
        self.current
            .as_ref()
            .map(|current| finalize(current.clone()))
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    fn new_bar(&mut self, start: i64, tick: &RelayTickRow) -> MutableKline {
        let row = RelayKlineRow {
            id: self.next_id,
            datetime: start,
            open: tick.last_price,
            high: tick.last_price,
            low: tick.last_price,
            close: tick.last_price,
            volume: 0,
            open_oi: tick.open_interest,
            close_oi: tick.open_interest,
        };
        self.next_id += 1;
        MutableKline {
            row,
            first_volume: tick.volume,
            last_volume: tick.volume,
        }
    }
}

fn window_start(datetime: i64, duration_ns: i64) -> i64 {
    datetime.div_euclid(duration_ns) * duration_ns
}

fn merge_tick(current: &mut MutableKline, tick: &RelayTickRow) {
    current.row.high = current.row.high.max(tick.last_price);
    current.row.low = current.row.low.min(tick.last_price);
    current.row.close = tick.last_price;
    current.row.close_oi = tick.open_interest;
    current.last_volume = tick.volume;
}

fn finalize(mut current: MutableKline) -> RelayKlineRow {
    current.row.volume = current.last_volume.saturating_sub(current.first_volume);
    current.row
}
