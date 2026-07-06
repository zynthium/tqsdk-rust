use tqsdk_core::{Kline, Tick};

use crate::{Result, TaskError};

const MAX_SYNTH_KLINE_DURATION_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone)]
pub(crate) struct KlineSynthesizer {
    symbol: String,
    duration_ns: i64,
    current: Option<SynthBar>,
    previous_cumulative_volume: Option<i64>,
}

#[derive(Debug, Clone)]
struct SynthBar {
    baseline_volume: i64,
    row: Kline,
}

impl KlineSynthesizer {
    pub(crate) fn new(symbol: impl Into<String>, duration_ns: i64) -> Result<Self> {
        let symbol = symbol.into();
        if symbol.is_empty() {
            return Err(TaskError::InvalidState(
                "synthetic kline symbol must not be empty",
            ));
        }
        if duration_ns <= 0 {
            return Err(TaskError::InvalidState(
                "synthetic kline duration must be positive",
            ));
        }
        if duration_ns > MAX_SYNTH_KLINE_DURATION_NS {
            return Err(TaskError::InvalidState(
                "synthetic kline duration must be 60 seconds or less",
            ));
        }
        Ok(Self {
            symbol,
            duration_ns,
            current: None,
            previous_cumulative_volume: None,
        })
    }

    #[must_use]
    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub(crate) fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    pub(crate) fn update(&mut self, tick: &Tick) -> Option<Kline> {
        if tick.datetime < 0 || !tick.last_price.is_finite() {
            return None;
        }
        let bar_start = tick.datetime.div_euclid(self.duration_ns) * self.duration_ns;
        let needs_new_bar = self
            .current
            .as_ref()
            .is_none_or(|bar| bar.row.datetime != bar_start);
        if needs_new_bar {
            let baseline_volume = self.previous_cumulative_volume.unwrap_or(tick.volume);
            self.current = Some(SynthBar {
                baseline_volume,
                row: Kline {
                    id: bar_start.div_euclid(self.duration_ns),
                    datetime: bar_start,
                    open: tick.last_price,
                    high: tick.last_price,
                    low: tick.last_price,
                    close: tick.last_price,
                    volume: 0,
                    open_oi: tick.open_interest,
                    close_oi: tick.open_interest,
                    ..Kline::default()
                },
            });
        }

        let bar = self
            .current
            .as_mut()
            .expect("current bar must exist after initialization");
        bar.row.high = bar.row.high.max(tick.last_price);
        bar.row.low = bar.row.low.min(tick.last_price);
        bar.row.close = tick.last_price;
        bar.row.close_oi = tick.open_interest;
        bar.row.volume = non_negative_delta(tick.volume, bar.baseline_volume);
        self.previous_cumulative_volume = Some(tick.volume);
        Some(bar.row.clone())
    }
}

fn non_negative_delta(current: i64, baseline: i64) -> i64 {
    current.checked_sub(baseline).unwrap_or(0).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kline_synth_starts_first_bar_from_first_tick() {
        let mut synth = KlineSynthesizer::new("SHFE.rb2601", 1_000).unwrap();
        let row = synth.update(&tick(1, 1_234, 101.0, 10, 20)).unwrap();

        assert_eq!(synth.symbol(), "SHFE.rb2601");
        assert_eq!(synth.duration_ns(), 1_000);
        assert_eq!(row.id, 1);
        assert_eq!(row.datetime, 1_000);
        assert_eq!(row.open, 101.0);
        assert_eq!(row.high, 101.0);
        assert_eq!(row.low, 101.0);
        assert_eq!(row.close, 101.0);
        assert_eq!(row.volume, 0);
        assert_eq!(row.open_oi, 20);
        assert_eq!(row.close_oi, 20);
    }

    #[test]
    fn kline_synth_updates_high_low_close_and_volume() {
        let mut synth = KlineSynthesizer::new("SHFE.rb2601", 1_000).unwrap();
        synth.update(&tick(1, 1_100, 101.0, 10, 20)).unwrap();
        synth.update(&tick(2, 1_200, 99.0, 13, 21)).unwrap();
        let row = synth.update(&tick(3, 1_300, 103.0, 18, 22)).unwrap();

        assert_eq!(row.open, 101.0);
        assert_eq!(row.high, 103.0);
        assert_eq!(row.low, 99.0);
        assert_eq!(row.close, 103.0);
        assert_eq!(row.volume, 8);
        assert_eq!(row.open_oi, 20);
        assert_eq!(row.close_oi, 22);
    }

    #[test]
    fn kline_synth_rolls_on_exact_boundary() {
        let mut synth = KlineSynthesizer::new("SHFE.rb2601", 1_000).unwrap();
        synth.update(&tick(1, 1_999, 101.0, 10, 20)).unwrap();
        let row = synth.update(&tick(2, 2_000, 102.0, 13, 21)).unwrap();

        assert_eq!(row.id, 2);
        assert_eq!(row.datetime, 2_000);
        assert_eq!(row.open, 102.0);
        assert_eq!(row.volume, 3);
    }

    #[test]
    fn kline_synth_accepts_exactly_sixty_seconds() {
        assert!(KlineSynthesizer::new("SHFE.rb2601", 60_000_000_000).is_ok());
    }

    #[test]
    fn kline_synth_rejects_above_sixty_seconds() {
        assert!(KlineSynthesizer::new("SHFE.rb2601", 60_000_000_001).is_err());
    }

    #[test]
    fn kline_synth_keeps_volume_non_negative_after_reset() {
        let mut synth = KlineSynthesizer::new("SHFE.rb2601", 1_000).unwrap();
        synth.update(&tick(1, 1_100, 101.0, 10, 20)).unwrap();
        let row = synth.update(&tick(2, 1_200, 102.0, 8, 21)).unwrap();

        assert_eq!(row.volume, 0);
    }

    fn tick(id: i64, datetime: i64, last_price: f64, volume: i64, open_interest: i64) -> Tick {
        Tick {
            id,
            datetime,
            last_price,
            volume,
            open_interest,
            ..Tick::default()
        }
    }
}
