#![cfg_attr(not(test), forbid(unsafe_code))]
//! Task and execution tooling built on [`tqsdk_wait`].
//!
//! This crate hosts task ownership, guarded command entrypoints, and host-driven
//! execution helpers built on the wait-style facade.

mod account_group;
mod calendar;
mod config;
mod deployment;
mod environment;
mod error;
mod execution_group;
mod host;
mod order;
mod plan;
mod registry;
mod replay;
mod risk;
mod scheduler;
mod shared;
mod strategy;
mod target_pos;
pub mod testing;

pub use account_group::{
    AccountAllocation, AccountAllocationPlan, AccountFailurePolicy, AccountGroup,
    AccountGroupBuilder, AllocatedAccountOrder, MultiAccountOrderBuilder, MultiAccountOrderDraft,
    MultiAccountOrderLegTicket, MultiAccountOrderOutcome, MultiAccountOrderReport,
    MultiAccountOrderState, MultiAccountOrderStatus, MultiAccountOrderTicket, Ratio,
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
    StrategySupervisorStopReason,
};
pub use environment::{
    StrategyEnvironment, StrategyEnvironmentBuilder, StrategyEnvironmentContext,
    StrategyEnvironmentKind, StrategyEnvironmentKlineSubscription,
    StrategyEnvironmentSubscriptions, StrategyEnvironmentTickSubscription,
};
pub use error::{Result, TaskError, TaskKind};
pub use execution_group::{
    ExecutionExposure, ExecutionGroupBuilder, ExecutionGroupOutcome, ExecutionGroupStatus,
    ExecutionGroupTicket, ExecutionLegIntent, ExecutionLegReport, ExecutionLegState,
    ExecutionLegTicket, HedgePolicy,
};
pub use host::TaskHost;
pub use order::{TaskOrderBuilder, TaskOrderDraft, TaskOrderIntent};
pub use replay::{
    StrategyReplay, StrategyReplayBuilder, StrategyReplayCheckpoint, StrategyReplayCheckpointStore,
    StrategyReplayContext, StrategyReplayEvent, StrategyReplaySourceBuilder, StrategyReplaySpeed,
};
pub use risk::{RiskDecision, RiskEngine, RiskRejection};
pub use scheduler::{
    TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep, TargetPosScheduler,
    TargetPosSchedulerBuilder, TargetPosSchedulerExecutionEvent, TargetPosSchedulerTradeFill,
    TargetPosStepOutcomeReport,
};
pub use strategy::{StrategyContext, StrategyHost, StrategyHostBuilder, StrategyUpdate};
pub use target_pos::{
    TargetPosBuilder, TargetPosTask, TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport,
    TargetPosTaskOrderReport, TargetPosTaskReachedTarget, TargetPosTaskTradeFill,
};
