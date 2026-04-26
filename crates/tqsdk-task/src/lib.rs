#![cfg_attr(not(test), forbid(unsafe_code))]
//! Task and execution tooling built on [`tqsdk_wait`].
//!
//! This crate hosts task ownership, guarded command entrypoints, and host-driven
//! execution helpers built on the wait-style facade.

mod calendar;
mod config;
mod error;
mod execution_group;
mod host;
mod order;
mod plan;
mod registry;
mod risk;
mod scheduler;
mod shared;
mod target_pos;

pub use calendar::TradingDayCalendar;
pub use config::{
    OffsetPriority, PriceMode, TargetPosConfig, TargetPosSchedulerConfig, VolumeSplitPolicy,
};
pub use error::{Result, TaskError, TaskKind};
pub use execution_group::{
    ExecutionGroupBuilder, ExecutionGroupTicket, ExecutionLegIntent, ExecutionLegTicket,
    HedgePolicy,
};
pub use host::TaskHost;
pub use order::{TaskOrderBuilder, TaskOrderDraft, TaskOrderIntent};
pub use risk::{RiskDecision, RiskEngine, RiskRejection};
pub use scheduler::{
    TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep, TargetPosScheduler,
    TargetPosSchedulerBuilder, TargetPosSchedulerExecutionEvent, TargetPosSchedulerTradeFill,
    TargetPosStepOutcomeReport,
};
pub use target_pos::{
    TargetPosBuilder, TargetPosTask, TargetPosTaskExecutionEvent, TargetPosTaskExecutionReport,
    TargetPosTaskOrderReport, TargetPosTaskReachedTarget, TargetPosTaskTradeFill,
};
