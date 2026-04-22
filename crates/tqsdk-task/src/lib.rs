#![cfg_attr(not(test), forbid(unsafe_code))]
//! Task and execution tooling built on [`tqsdk_wait`].
//!
//! This crate is the future home of `TargetPosTask`, schedulers, and task
//! ownership. The current milestone only scaffolds the host shell and the
//! internal registry boundary.

mod error;
mod host;
mod registry;
mod target_pos;

pub use error::{Result, TaskError, TaskKind};
pub use host::TaskHost;
pub use target_pos::{TargetPosBuilder, TargetPosTask};
