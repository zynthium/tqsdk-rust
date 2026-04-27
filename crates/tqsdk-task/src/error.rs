#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt::{Display, Formatter};

use crate::risk::RiskRejection;

/// Result alias for `tqsdk-task`.
pub type Result<T> = std::result::Result<T, TaskError>;

/// High-level task kinds that can own an account-symbol pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    TargetPos,
    Scheduler,
}

/// Errors returned by task-level ownership and execution helpers.
#[derive(Debug, Clone, PartialEq)]
pub enum TaskError {
    Core(tqsdk_core::ContractError),
    Wait(tqsdk_wait::WaitFacadeError),
    Session(tqsdk_session::SessionFacadeError),
    RiskRejected(RiskRejection),
    ExecutionGroupPartialSubmit {
        group_id: String,
        submitted_legs: usize,
        total_legs: usize,
        reason: &'static str,
    },
    MultiAccountPartialSubmit {
        group_id: String,
        submitted_accounts: usize,
        total_accounts: usize,
        reason: &'static str,
    },
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
    InvalidCalendarDate {
        date: String,
    },
    Unsupported(&'static str),
    InvalidState(&'static str),
}

impl From<tqsdk_core::ContractError> for TaskError {
    fn from(error: tqsdk_core::ContractError) -> Self {
        Self::Core(error)
    }
}

impl From<tqsdk_wait::WaitFacadeError> for TaskError {
    fn from(error: tqsdk_wait::WaitFacadeError) -> Self {
        Self::Wait(error)
    }
}

impl From<tqsdk_session::SessionFacadeError> for TaskError {
    fn from(error: tqsdk_session::SessionFacadeError) -> Self {
        Self::Session(error)
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => write!(f, "{error}"),
            Self::Wait(error) => write!(f, "{error}"),
            Self::Session(error) => write!(f, "{error}"),
            Self::RiskRejected(rejection) => write!(f, "risk rejected order: {rejection:?}"),
            Self::ExecutionGroupPartialSubmit {
                group_id,
                submitted_legs,
                total_legs,
                reason,
            } => write!(
                f,
                "execution group partial submit group_id={group_id} submitted_legs={submitted_legs} total_legs={total_legs}: {reason}"
            ),
            Self::MultiAccountPartialSubmit {
                group_id,
                submitted_accounts,
                total_accounts,
                reason,
            } => write!(
                f,
                "multi-account partial submit group_id={group_id} submitted_accounts={submitted_accounts} total_accounts={total_accounts}: {reason}"
            ),
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
            Self::InvalidCalendarDate { date } => {
                write!(f, "invalid trading calendar date: {date}")
            }
            Self::Unsupported(message) => write!(f, "unsupported task operation: {message}"),
            Self::InvalidState(message) => write!(f, "invalid task state: {message}"),
        }
    }
}

impl std::error::Error for TaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            Self::Wait(error) => Some(error),
            Self::Session(error) => Some(error),
            Self::RiskRejected(_) => None,
            Self::ExecutionGroupPartialSubmit { .. } | Self::MultiAccountPartialSubmit { .. } => {
                None
            }
            Self::OwnershipConflict { .. }
            | Self::ManualOrderBlocked { .. }
            | Self::OrderNotReady { .. }
            | Self::InvalidCalendarDate { .. }
            | Self::Unsupported(_)
            | Self::InvalidState(_) => None,
        }
    }
}
