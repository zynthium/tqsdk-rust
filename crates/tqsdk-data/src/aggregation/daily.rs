use tqsdk_core::Kline;

use crate::daily_kline_cache::DAILY_KLINE_DURATION_NS;
use crate::{DataError, Result};

/// Stateful local aggregation over final native daily Klines.
///
/// Native daily timestamps establish one stable phase. This preserves the
/// provider's day boundary without assuming UTC midnight; release golden
/// packets verify that phase against official `2d..28d` charts.
#[derive(Debug, Clone)]
pub struct DailyKlineAggregator {
    duration_ns: i64,
    bucket_origin_ns: Option<i64>,
    current: Option<Kline>,
}

impl DailyKlineAggregator {
    /// Creates an aggregator for an integer `N × 1d` duration, where `N >= 2`.
    pub fn new(duration_ns: i64) -> Result<Self> {
        if duration_ns <= DAILY_KLINE_DURATION_NS || duration_ns % DAILY_KLINE_DURATION_NS != 0 {
            return Err(DataError::Validation(
                "aggregated daily kline duration must be an integer multiple of 1d greater than 1d"
                    .to_string(),
            ));
        }
        Ok(Self {
            duration_ns,
            bucket_origin_ns: None,
            current: None,
        })
    }

    /// Incorporates one final native daily row and returns a completed prior
    /// bar when this row starts a new `N × 1d` bucket.
    pub fn update(&mut self, daily: &Kline) -> Result<Option<Kline>> {
        let day_phase = daily.datetime.rem_euclid(DAILY_KLINE_DURATION_NS);
        let bucket_origin_ns = match self.bucket_origin_ns {
            Some(origin) if origin.rem_euclid(DAILY_KLINE_DURATION_NS) != day_phase => {
                return Err(DataError::InvalidResponse(
                    "native daily kline timestamps changed day-boundary phase".to_string(),
                ));
            }
            Some(origin) => origin,
            None => {
                self.bucket_origin_ns = Some(daily.datetime);
                daily.datetime
            }
        };
        let bucket_start = daily
            .datetime
            .checked_sub(bucket_origin_ns)
            .and_then(|value| value.checked_div_euclid(self.duration_ns))
            .and_then(|value| value.checked_mul(self.duration_ns))
            .and_then(|value| value.checked_add(bucket_origin_ns))
            .ok_or_else(|| DataError::Validation("daily kline bucket overflow".to_string()))?;
        let starts_new = self
            .current
            .as_ref()
            .is_none_or(|current| current.datetime != bucket_start);
        let closed = starts_new.then(|| self.current.take()).flatten();
        if starts_new {
            self.current = Some(Kline {
                id: bucket_start,
                datetime: bucket_start,
                open: daily.open,
                high: daily.high,
                low: daily.low,
                close: daily.close,
                volume: daily.volume.max(0),
                open_oi: daily.open_oi,
                close_oi: daily.close_oi,
                epoch: daily.epoch,
            });
        } else if let Some(current) = self.current.as_mut() {
            current.high = current.high.max(daily.high);
            current.low = current.low.min(daily.low);
            current.close = daily.close;
            current.volume = current.volume.saturating_add(daily.volume.max(0));
            current.close_oi = daily.close_oi;
            current.epoch = daily.epoch.or(current.epoch);
        }
        Ok(closed)
    }

    /// Returns final bucket only after its nominal `N × 1d` interval ends.
    pub fn finish_closed_through(&mut self, end_ns: i64) -> Result<Option<Kline>> {
        let Some(current) = self.current.as_ref() else {
            return Ok(None);
        };
        let bar_end = current
            .datetime
            .checked_add(self.duration_ns)
            .ok_or_else(|| DataError::Validation("daily kline bar end overflow".to_string()))?;
        Ok((bar_end <= end_ns).then(|| self.current.take()).flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_day_buckets_start_at_the_first_native_daily_timestamp() {
        let day = DAILY_KLINE_DURATION_NS;
        let phase = 8 * 60 * 60 * 1_000_000_000;
        let first = kline(day + phase, 101.0);
        let second = kline(2 * day + phase, 102.0);
        let third = kline(3 * day + phase, 103.0);
        let mut aggregator = DailyKlineAggregator::new(2 * day).unwrap();

        assert!(aggregator.update(&first).unwrap().is_none());
        assert!(aggregator.update(&second).unwrap().is_none());
        let closed = aggregator.update(&third).unwrap().unwrap();

        assert_eq!(closed.datetime, first.datetime);
        assert_eq!(closed.open, 101.0);
        assert_eq!(closed.close, 102.0);
    }

    fn kline(datetime: i64, close: f64) -> Kline {
        Kline {
            id: datetime,
            datetime,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1,
            open_oi: 1,
            close_oi: 1,
            epoch: None,
        }
    }
}
