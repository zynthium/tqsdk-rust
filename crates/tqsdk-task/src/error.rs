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
    Wait(tqsdk_wait::WaitFacadeError),
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
    OrderNotReady {
        account_id: String,
        order_id: String,
    },
    InvalidState(&'static str),
}

impl From<tqsdk_wait::WaitFacadeError> for TaskError {
    fn from(error: tqsdk_wait::WaitFacadeError) -> Self {
        Self::Wait(error)
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wait(error) => write!(f, "{error}"),
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
            Self::OrderNotReady {
                account_id,
                order_id,
            } => write!(
                f,
                "order snapshot not ready for guarded command on account={account_id} order_id={order_id}"
            ),
            Self::InvalidState(message) => write!(f, "invalid task state: {message}"),
        }
    }
}

impl std::error::Error for TaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Wait(error) => Some(error),
            Self::OwnershipConflict { .. }
            | Self::ManualOrderBlocked { .. }
            | Self::OrderNotReady { .. }
            | Self::InvalidState(_) => None,
        }
    }
}
