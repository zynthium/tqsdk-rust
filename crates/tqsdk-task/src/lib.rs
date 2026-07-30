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
pub mod backtest;
mod backtest_stream;
mod calendar;
mod config;
pub mod deployment;
pub mod environment;
mod error;
mod execution_group;
mod history_backtest_replay;
mod history_tick_replay;
mod host;
mod minute_kline_aggregate;
mod order;
mod order_projection;
mod plan;
mod registry;
pub mod replay;
mod replay_runtime;
mod risk;
mod scheduler;
mod shared;
pub mod sim;
mod strategy;
mod target_pos;
pub mod testing;
pub mod trading_desk;

/// Multi-leg and multi-account order foundations.
///
/// These are advanced order-group interfaces. Ordinary task usage should start
/// with [`TaskHost`], [`TargetPosTask`], or [`RiskEngine`].
pub mod order_groups {
    pub use crate::account_group::{
        AccountAllocation, AccountAllocationPlan, AccountFailurePolicy, AccountGroup,
        AccountGroupBuilder, AllocatedAccountOrder, MultiAccountOrderBuilder,
        MultiAccountOrderDraft, MultiAccountOrderGroupReport, MultiAccountOrderLegTicket,
        MultiAccountOrderOutcome, MultiAccountOrderReport, MultiAccountOrderState,
        MultiAccountOrderStatus, MultiAccountOrderTicket, Ratio,
    };
    pub use crate::execution_group::{
        ExecutionExposure, ExecutionGroupBuilder, ExecutionGroupOutcome, ExecutionGroupReport,
        ExecutionGroupStatus, ExecutionGroupTicket, ExecutionLegIntent, ExecutionLegReport,
        ExecutionLegState, ExecutionLegTicket, HedgePolicy,
    };
}

pub use backtest::{
    StrategyBacktest, StrategyBacktestBalancePoint, StrategyBacktestBuilder,
    StrategyBacktestClosedProfitPoint, StrategyBacktestContext, StrategyBacktestDailyBalanceReturn,
    StrategyBacktestDailyEquityReturn, StrategyBacktestDailyReturnWindow,
    StrategyBacktestEquityPoint, StrategyBacktestEvent, StrategyBacktestPerformanceMetrics,
    StrategyBacktestPerformanceReport, StrategyBacktestRiskRatioPoint,
    StrategyBacktestRollingRatioPoint, StrategyBacktestSummary,
};
pub use backtest_stream::{BacktestMarketStream, ReplayMarketStream};
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
pub use history_backtest_replay::{
    HistoryBacktestKlineRequest, HistoryBacktestMinuteKlineSource,
    HistoryBacktestMinuteKlineUnderlyingSegment, HistoryBacktestProjectedReplayRequest,
    HistoryBacktestReplayRequest, HistoryBacktestReplayStream, HistoryBacktestSyntheticKlineSource,
    HistoryBacktestTickSource,
};
pub use history_tick_replay::HistoryTickReplayStream;
pub use host::TaskHost;
pub use minute_kline_aggregate::{
    CANONICAL_MINUTE_KLINE_NS, MinuteKlineAggregationUpdate, MinuteKlineAggregator,
    MinuteKlineSessionTemplate, MinuteKlineSessionWindow,
};
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
