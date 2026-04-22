#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

/// Result alias for `tqsdk-task`.
pub type Result<T> = std::result::Result<T, TaskError>;

/// High-level task kinds that can own an account-symbol pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    TargetPos,
    Scheduler,
}

/// Errors returned by task-level ownership and execution helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    OwnershipConflict {
        account_id: String,
        symbol: String,
        active_task_kind: TaskKind,
    },
    ManualOrderBlocked {
        account_id: String,
        symbol: String,
        active_task_kind: TaskKind,
    },
    InvalidState(&'static str),
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OwnershipConflict {
                account_id,
                symbol,
                active_task_kind,
            } => write!(
                f,
                "task ownership conflict on account={account_id} symbol={symbol} active_task_kind={active_task_kind:?}"
            ),
            Self::ManualOrderBlocked {
                account_id,
                symbol,
                active_task_kind,
            } => write!(
                f,
                "manual order blocked by active task on account={account_id} symbol={symbol} active_task_kind={active_task_kind:?}"
            ),
            Self::InvalidState(message) => write!(f, "invalid task state: {message}"),
        }
    }
}

impl std::error::Error for TaskError {}
