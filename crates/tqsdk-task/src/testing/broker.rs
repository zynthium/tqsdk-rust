/// Deterministic fake broker policy for strategy tests.
#[derive(Debug, Clone)]
pub struct FakeBroker {
    pub(super) policy: FakeBrokerPolicy,
    pub(super) latency_steps: usize,
    pub(super) disconnect_steps: usize,
}

/// Order handling policy used by [`FakeBroker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeBrokerPolicy {
    FillAll,
    RejectAll { reason: String },
    PartialFill { volume: i64 },
    PartialFills { volumes: Vec<i64> },
}

/// Fake broker connection status observed during a strategy test step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FakeBrokerConnectionStatus {
    #[default]
    Connected,
    Disconnected,
    Reconnected,
}

impl Default for FakeBroker {
    fn default() -> Self {
        Self {
            policy: FakeBrokerPolicy::FillAll,
            latency_steps: 0,
            disconnect_steps: 0,
        }
    }
}

impl FakeBroker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn fill_all(mut self) -> Self {
        self.policy = FakeBrokerPolicy::FillAll;
        self
    }

    #[must_use]
    pub fn reject_all(mut self, reason: impl Into<String>) -> Self {
        self.policy = FakeBrokerPolicy::RejectAll {
            reason: reason.into(),
        };
        self
    }

    #[must_use]
    pub fn partial_fill(mut self, volume: i64) -> Self {
        self.policy = FakeBrokerPolicy::PartialFill { volume };
        self
    }

    #[must_use]
    pub fn partial_fills(mut self, volumes: impl IntoIterator<Item = i64>) -> Self {
        self.policy = FakeBrokerPolicy::PartialFills {
            volumes: volumes.into_iter().collect(),
        };
        self
    }

    #[must_use]
    pub fn latency_steps(mut self, steps: usize) -> Self {
        self.latency_steps = steps;
        self
    }

    #[must_use]
    pub fn disconnect_for_steps(mut self, steps: usize) -> Self {
        self.disconnect_steps = steps;
        self
    }
}
