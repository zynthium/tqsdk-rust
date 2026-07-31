use serde::{Deserialize, Serialize};

use crate::{DataError, Result};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 60 * 60 * NANOS_PER_SECOND;
const CST_SESSION_ANCHOR_UTC_NS: i64 = 10 * 60 * 60 * NANOS_PER_SECOND;

/// One half-open trading session, expressed from the fixed CST 18:00
/// prior-natural-day anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineSessionWindow {
    pub start_offset_ns: i64,
    pub end_offset_ns: i64,
}

impl KlineSessionWindow {
    /// Validates and creates a single session window.
    pub fn new(start_offset_ns: i64, end_offset_ns: i64) -> Result<Self> {
        if start_offset_ns < 0 || end_offset_ns <= start_offset_ns {
            return Err(DataError::Validation(
                "kline session window must have non-negative increasing offsets".to_string(),
            ));
        }
        Ok(Self {
            start_offset_ns,
            end_offset_ns,
        })
    }
}

/// Persistable session topology associated with one metadata snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineSessionTemplate {
    snapshot_hash: String,
    windows: Vec<KlineSessionWindow>,
}

impl KlineSessionTemplate {
    /// Creates an ordered, non-overlapping session topology.
    pub fn new(snapshot_hash: impl Into<String>, windows: Vec<KlineSessionWindow>) -> Result<Self> {
        let snapshot_hash = snapshot_hash.into();
        if snapshot_hash.is_empty() {
            return Err(DataError::Validation(
                "kline session snapshot hash must not be empty".to_string(),
            ));
        }
        let mut previous_end = None;
        for window in &windows {
            if window.start_offset_ns < 0 || window.end_offset_ns <= window.start_offset_ns {
                return Err(DataError::Validation(
                    "kline session window must have non-negative increasing offsets".to_string(),
                ));
            }
            if previous_end.is_some_and(|end| window.start_offset_ns < end) {
                return Err(DataError::Validation(
                    "kline session windows must be ordered and non-overlapping".to_string(),
                ));
            }
            previous_end = Some(window.end_offset_ns);
        }
        Ok(Self {
            snapshot_hash,
            windows,
        })
    }

    /// Uses the complete canonical CST trading day as one session.
    #[must_use]
    pub fn cst_trading_day() -> Self {
        Self {
            snapshot_hash: "cst-trading-day-v1".to_string(),
            windows: Vec::new(),
        }
    }

    /// Returns the hash of the calendar/session metadata snapshot.
    #[must_use]
    pub fn snapshot_hash(&self) -> &str {
        self.snapshot_hash.as_str()
    }

    /// Returns explicit windows. An empty list means the full trading day.
    #[must_use]
    pub fn windows(&self) -> &[KlineSessionWindow] {
        self.windows.as_slice()
    }

    /// Locates a timestamp in its canonical session cycle and session window.
    ///
    /// The cycle is always the 24 hours beginning at 18:00 CST on the prior
    /// natural day. It deliberately differs from the Tick cache's partition
    /// range, which crosses weekends and holidays for storage purposes.
    /// Timestamps inside a configured break return `Ok(None)`.
    pub fn locate(&self, timestamp_ns: i64) -> Result<Option<KlineSessionPosition>> {
        let (trading_day_start_ns, trading_day_end_ns) = self.cycle_bounds(timestamp_ns)?;
        if self.windows.is_empty() {
            return Ok(Some(KlineSessionPosition {
                trading_day_start_ns,
                trading_day_end_ns,
                window_start_ns: trading_day_start_ns,
                window_end_ns: trading_day_end_ns,
            }));
        }
        for window in &self.windows {
            let window_start_ns = trading_day_start_ns
                .checked_add(window.start_offset_ns)
                .ok_or_else(|| {
                    DataError::Validation(
                        "kline session window start timestamp overflow".to_string(),
                    )
                })?;
            let window_end_ns = trading_day_start_ns
                .checked_add(window.end_offset_ns)
                .ok_or_else(|| {
                    DataError::Validation("kline session window end timestamp overflow".to_string())
                })?;
            if window_end_ns > trading_day_end_ns {
                return Err(DataError::Validation(
                    "kline session window exceeds its canonical trading day".to_string(),
                ));
            }
            if timestamp_ns >= window_start_ns && timestamp_ns < window_end_ns {
                return Ok(Some(KlineSessionPosition {
                    trading_day_start_ns,
                    trading_day_end_ns,
                    window_start_ns,
                    window_end_ns,
                }));
            }
        }
        Ok(None)
    }

    /// Resolves the canonical 18:00 CST session cycle even when `timestamp_ns`
    /// lies in a configured trading break.
    pub(crate) fn cycle_bounds(&self, timestamp_ns: i64) -> Result<(i64, i64)> {
        canonical_session_cycle_bounds(timestamp_ns)
    }
}

fn canonical_session_cycle_bounds(timestamp_ns: i64) -> Result<(i64, i64)> {
    let shifted = timestamp_ns
        .checked_sub(CST_SESSION_ANCHOR_UTC_NS)
        .ok_or_else(|| DataError::Validation("kline session timestamp underflow".to_string()))?;
    let trading_day_start_ns = shifted
        .div_euclid(NANOS_PER_DAY)
        .checked_mul(NANOS_PER_DAY)
        .and_then(|day_start_ns| day_start_ns.checked_add(CST_SESSION_ANCHOR_UTC_NS))
        .ok_or_else(|| DataError::Validation("kline session timestamp overflow".to_string()))?;
    let trading_day_end_ns = trading_day_start_ns
        .checked_add(NANOS_PER_DAY)
        .ok_or_else(|| DataError::Validation("kline session end timestamp overflow".to_string()))?;
    Ok((trading_day_start_ns, trading_day_end_ns))
}

/// Resolved trading-day and session bounds for one input row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineSessionPosition {
    pub trading_day_start_ns: i64,
    pub trading_day_end_ns: i64,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
}
