#![cfg_attr(not(test), forbid(unsafe_code))]
//! Task and execution tooling built on [`tqsdk_wait`].
//!
//! This crate hosts task ownership, guarded command entrypoints, and host-driven
//! execution helpers built on the wait-style facade.

mod config;
mod error;
mod host;
mod plan;
mod registry;
mod scheduler;
mod target_pos;

pub use config::{
    OffsetPriority, PriceMode, TargetPosConfig, TargetPosSchedulerConfig, VolumeSplitPolicy,
};
pub use error::{Result, TaskError, TaskKind};
pub use host::TaskHost;
pub use scheduler::{
    TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep, TargetPosScheduler,
    TargetPosSchedulerBuilder,
};
pub use target_pos::{TargetPosBuilder, TargetPosTask};
