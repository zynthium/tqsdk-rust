#![cfg_attr(not(test), forbid(unsafe_code))]

mod broker;
mod clock;
mod harness;
mod market;
mod report;
mod runtime;

pub use broker::{FakeBroker, FakeBrokerConnectionStatus, FakeBrokerPolicy};
pub use clock::StrategyTestClock;
#[allow(deprecated)]
pub use harness::{BuiltStrategyTestHarness, StrategyTestHarness, StrategyTestHarnessBuilder};
pub use market::FakeMarket;
pub use report::StrategyTestReport;

pub(crate) use runtime::{StrategyTestRuntime, finish_test_step};
