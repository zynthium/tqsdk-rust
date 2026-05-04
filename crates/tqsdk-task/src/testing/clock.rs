use std::time::Duration;

/// Deterministic clock used by fake strategy tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategyTestClock {
    next_time_ns: i64,
    step_ns: i64,
}

impl Default for StrategyTestClock {
    fn default() -> Self {
        Self::new(Self::DEFAULT_START_TIME_NS).step_by_ns(Self::DEFAULT_STEP_NS)
    }
}

impl StrategyTestClock {
    pub const DEFAULT_START_TIME_NS: i64 = 1_777_222_800_000_000_000;
    pub const DEFAULT_STEP_NS: i64 = 100_000_000;

    #[must_use]
    pub fn new(start_time_ns: i64) -> Self {
        Self {
            next_time_ns: start_time_ns,
            step_ns: Self::DEFAULT_STEP_NS,
        }
    }

    #[must_use]
    pub fn fixed(time_ns: i64) -> Self {
        Self {
            next_time_ns: time_ns,
            step_ns: 0,
        }
    }

    #[must_use]
    pub fn step_by(mut self, step: Duration) -> Self {
        self.step_ns = duration_to_ns(step);
        self
    }

    #[must_use]
    pub fn step_by_ns(mut self, step_ns: i64) -> Self {
        self.step_ns = step_ns.max(0);
        self
    }

    #[must_use]
    pub fn now_ns(&self) -> i64 {
        self.next_time_ns
    }

    pub(super) fn next_timestamp_ns(&mut self) -> i64 {
        let timestamp = self.next_time_ns;
        self.next_time_ns = self.next_time_ns.saturating_add(self.step_ns);
        timestamp
    }
}

fn duration_to_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}
