use tqsdk_core::{AdapterRegistry, RuntimeHandle};
use tqsdk_session::testing::ManualSession;
use tqsdk_wait::TqApi;

use crate::{Result, TaskHost};

use super::broker::FakeBroker;
use super::clock::StrategyTestClock;
use super::market::{FakeMarket, seed_market};
use super::runtime::StrategyTestRuntime;

/// Builder entrypoint for deterministic strategy tests.
pub struct StrategyTestHarness {
    market: FakeMarket,
    broker: FakeBroker,
    clock: StrategyTestClock,
}

/// Compatibility alias for callers that prefer an explicit builder type name.
pub type StrategyTestHarnessBuilder = StrategyTestHarness;

/// Built fake test harness.
pub struct BuiltStrategyTestHarness {
    host: TaskHost,
}

impl StrategyTestHarness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            market: FakeMarket::new(),
            broker: FakeBroker::new(),
            clock: StrategyTestClock::default(),
        }
    }

    #[must_use]
    pub fn market(mut self, market: FakeMarket) -> Self {
        self.market = market;
        self
    }

    #[must_use]
    pub fn broker(mut self, broker: FakeBroker) -> Self {
        self.broker = broker;
        self
    }

    #[must_use]
    pub fn clock(mut self, clock: StrategyTestClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn build(self) -> Result<BuiltStrategyTestHarness> {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = ManualSession::from_runtime(handle).into_client();
        let mut host = TaskHost::new(TqApi::new(session));
        let positions = seed_market(&host, &self.market)?;
        host.strategy_test = Some(StrategyTestRuntime::new(self.broker, positions, self.clock));

        Ok(BuiltStrategyTestHarness { host })
    }
}

impl Default for StrategyTestHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltStrategyTestHarness {
    #[must_use]
    pub fn into_task_host(self) -> TaskHost {
        self.host
    }
}
