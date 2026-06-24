#![cfg_attr(not(test), forbid(unsafe_code))]
//! Task and execution tooling built on [`tqsdk_wait`].
//!
//! This crate hosts task ownership, guarded command entrypoints, and host-driven
//! execution helpers built on the wait-style facade.
//!
//! # Example
//!
//! ```
//! let split = tqsdk_task::VolumeSplitPolicy::new(1, 10).unwrap();
//! assert_eq!(split.min_volume(), 1);
//! assert_eq!(split.max_volume(), 10);
//! ```

mod account_group;
mod backtest;
mod calendar;
mod config;
mod deployment;
mod environment;
mod error;
mod execution_group;
mod host;
mod order;
mod order_projection;
mod plan;
mod registry;
mod replay;
mod risk;
mod scheduler;
mod shared;
mod sim;
mod strategy;
mod target_pos;
pub mod testing;
mod trading_desk;

pub use account_group::{
    AccountAllocation, AccountAllocationPlan, AccountFailurePolicy, AccountGroup,
    AccountGroupBuilder, AllocatedAccountOrder, MultiAccountOrderBuilder, MultiAccountOrderDraft,
    MultiAccountOrderGroupReport, MultiAccountOrderLegTicket, MultiAccountOrderOutcome,
    MultiAccountOrderReport, MultiAccountOrderState, MultiAccountOrderStatus,
    MultiAccountOrderTicket, Ratio,
};
pub use backtest::{
    StrategyBacktest, StrategyBacktestBalancePoint, StrategyBacktestBuilder,
    StrategyBacktestContext, StrategyBacktestEquityPoint, StrategyBacktestEvent,
    StrategyBacktestSummary,
};
pub use calendar::TradingDayCalendar;
pub use config::{
    OffsetPriority, PriceMode, TargetPosConfig, TargetPosSchedulerConfig, VolumeSplitPolicy,
};
pub use deployment::{
    StrategyDeployment, StrategyDeploymentBuilder, StrategyDeploymentConfig,
    StrategyEnvironmentProvider, StrategyLifecycle, StrategyMarketMode, StrategyRetryPolicy,
    StrategyRunReport, StrategyRunStopReason, StrategyShutdownReport, StrategyShutdownSignal,
    StrategyStepFuture, StrategySupervisor, StrategySupervisorHealth,
    StrategySupervisorHealthStatus, StrategySupervisorMetrics, StrategySupervisorReport,
    StrategySupervisorStopReason, StrategyTelemetryEvent, StrategyTelemetryEventKind,
    StrategyTelemetryReporter,
};
pub use environment::{
    StrategyEnvironment, StrategyEnvironmentBuilder, StrategyEnvironmentContext,
    StrategyEnvironmentKind, StrategyEnvironmentKlineSubscription,
    StrategyEnvironmentSubscriptions, StrategyEnvironmentTickSubscription,
};
pub use error::{Result, TaskError, TaskKind};
pub use execution_group::{
    ExecutionExposure, ExecutionGroupBuilder, ExecutionGroupOutcome, ExecutionGroupReport,
    ExecutionGroupStatus, ExecutionGroupTicket, ExecutionLegIntent, ExecutionLegReport,
    ExecutionLegState, ExecutionLegTicket, HedgePolicy,
};
pub use host::TaskHost;
pub use order::{TaskOrderBuilder, TaskOrderDraft, TaskOrderIntent};
pub use replay::{
    ReplayMarketEvent, ReplayMarketPayload, ReplayMarketPayloadKind, ReplayMarketSource,
    StrategyReplay, StrategyReplayBuilder, StrategyReplayCheckpoint, StrategyReplayCheckpointStore,
    StrategyReplayContext, StrategyReplayEvent, StrategyReplaySourceBuilder, StrategyReplaySpeed,
};
pub use risk::{RiskCheckReport, RiskDecision, RiskEngine, RiskProjectionReport, RiskRejection};
pub use scheduler::{
    TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep, TargetPosScheduler,
    TargetPosSchedulerBuilder, TargetPosSchedulerExecutionEvent, TargetPosSchedulerTradeFill,
    TargetPosStepOutcomeReport,
};
pub use sim::{LOCAL_BACKTEST_ACCOUNT_ID, TqSim, TqSimOrderRequest, TqSimStepReport};
pub use strategy::{StrategyContext, StrategyHost, StrategyHostBuilder, StrategyUpdate};
pub use target_pos::{
    TargetPosBuilder, TargetPosTask, TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport,
    TargetPosTaskOrderReport, TargetPosTaskReachedTarget, TargetPosTaskTradeFill,
};
pub use trading_desk::{
    TradingDeskMarketEvent, TradingDeskOrderState, TradingDeskOrderStatusReport,
    TradingDeskOrderTicket, TradingDeskPrecheckedOrder, TradingDeskProfile,
    TradingDeskProfileBuilder, TradingLatencyCycle, TradingLatencyProbe, TradingLatencyReport,
};
