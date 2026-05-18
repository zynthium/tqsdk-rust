#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

const TRADING_DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// A trading-session window within a single local trading day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingSessionSegment {
    start: Duration,
    end: Duration,
}

impl TradingSessionSegment {
    /// Creates a segment from local-day offsets.
    ///
    /// `end < start` represents a session that wraps across midnight.
    /// Empty windows are rejected.
    #[must_use]
    pub fn new(start: Duration, end: Duration) -> Option<Self> {
        (start != end && start < TRADING_DAY && end <= TRADING_DAY).then_some(Self { start, end })
    }

    #[must_use]
    pub fn start(&self) -> Duration {
        self.start
    }

    #[must_use]
    pub fn end(&self) -> Duration {
        self.end
    }

    fn contains(&self, now: Duration) -> bool {
        if self.wraps_midnight() {
            now >= self.start || now < self.end
        } else {
            self.start <= now && now < self.end
        }
    }

    fn distance_to_start(&self, now: Duration) -> Duration {
        if self.start >= now {
            self.start - now
        } else {
            TRADING_DAY - now + self.start
        }
    }

    fn distance_to_end(&self, now: Duration) -> Duration {
        if self.wraps_midnight() && now >= self.start {
            TRADING_DAY - now + self.end
        } else {
            self.end - now
        }
    }

    fn wraps_midnight(&self) -> bool {
        self.end <= self.start
    }
}

/// Deterministic trading-session schedule over a local 24-hour cycle.
#[derive(Debug, Clone)]
pub struct TradingSessionSchedule {
    segments: Vec<TradingSessionSegment>,
    pre_close_window: Duration,
}

impl TradingSessionSchedule {
    /// Builds a schedule from already parsed local-day session segments.
    pub fn from_segments(segments: impl IntoIterator<Item = TradingSessionSegment>) -> Self {
        let mut segments = segments.into_iter().collect::<Vec<_>>();
        segments.sort_by_key(|segment| (segment.start, segment.end));
        Self {
            segments,
            pre_close_window: Duration::ZERO,
        }
    }

    /// Marks an open session as pre-close when it is this close to its boundary.
    #[must_use]
    pub fn with_pre_close_window(mut self, window: Duration) -> Self {
        self.pre_close_window = window;
        self
    }

    #[must_use]
    pub fn status_at(&self, now: Duration) -> TradingSessionStatus {
        let now = normalize_day_offset(now);
        if let Some(countdown) = self
            .segments
            .iter()
            .filter(|segment| segment.contains(now))
            .map(|segment| segment.distance_to_end(now))
            .min()
        {
            let phase =
                if self.pre_close_window > Duration::ZERO && countdown <= self.pre_close_window {
                    TradingSessionPhase::PreClose
                } else {
                    TradingSessionPhase::Open
                };
            return TradingSessionStatus {
                phase,
                countdown: Some(countdown),
            };
        }

        TradingSessionStatus {
            phase: TradingSessionPhase::Closed,
            countdown: self
                .segments
                .iter()
                .map(|segment| segment.distance_to_start(now))
                .min(),
        }
    }
}

/// Current trading-session phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingSessionPhase {
    Open,
    PreClose,
    Closed,
}

/// Status at a point in the local trading-day cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingSessionStatus {
    pub phase: TradingSessionPhase,
    /// Time until the current session closes, or until the next session opens.
    pub countdown: Option<Duration>,
}

fn normalize_day_offset(offset: Duration) -> Duration {
    let secs = offset.as_secs() % TRADING_DAY.as_secs();
    let nanos = offset.subsec_nanos();
    Duration::new(secs, nanos)
}
