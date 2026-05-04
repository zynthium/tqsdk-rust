#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::Value;
use tqsdk_core::{
    AccountId, CommandId, CommandStatus, Order, OrderId, OrderLifecycle, StateReadView,
};
use tqsdk_wait::OrderTicketState;

use crate::{Result, TaskError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderVolumeProgress {
    pub(crate) filled_volume: i64,
    pub(crate) volume_left: i64,
}

pub(crate) fn ticket_state_from_view(
    view: StateReadView<'_>,
    account_id: &str,
    order_id: &str,
    command_id: Option<CommandId>,
) -> Result<OrderTicketState> {
    let account_id = AccountId::new(account_id.to_owned());
    let order_id = OrderId::new(order_id.to_owned());
    let order = view.trade_state().order(&account_id, &order_id)?;
    let command_status = command_status_from_view(view, command_id)?;

    match order {
        Some(order) => Ok(ticket_state_from_order(command_id, order)),
        None => Ok(ticket_state_from_command(command_id, command_status)),
    }
}

pub(crate) fn order_from_ticket_state(state: &OrderTicketState) -> Option<&Order> {
    match state {
        OrderTicketState::Live { order, .. }
        | OrderTicketState::Filled { order, .. }
        | OrderTicketState::Cancelled {
            order: Some(order), ..
        }
        | OrderTicketState::Rejected {
            order: Some(order), ..
        }
        | OrderTicketState::Failed {
            order: Some(order), ..
        } => Some(order),
        OrderTicketState::Unknown { .. }
        | OrderTicketState::CommandPending { .. }
        | OrderTicketState::Cancelled { order: None, .. }
        | OrderTicketState::Rejected { order: None, .. }
        | OrderTicketState::Failed { order: None, .. } => None,
    }
}

pub(crate) fn order_volume_progress(order: &Order) -> OrderVolumeProgress {
    let volume_left = order.volume_left;
    let filled_volume = (order.volume_origin - volume_left).max(0);
    OrderVolumeProgress {
        filled_volume,
        volume_left,
    }
}

pub(crate) fn fallback_volume_progress(requested_volume: i64) -> OrderVolumeProgress {
    OrderVolumeProgress {
        filled_volume: 0,
        volume_left: requested_volume,
    }
}

fn command_status_from_view(
    view: StateReadView<'_>,
    command_id: Option<CommandId>,
) -> Result<Option<CommandStatus>> {
    let Some(command_id) = command_id else {
        return Ok(None);
    };
    let command_segment = command_id.get().to_string();
    let Some(command) =
        view.decode_path::<Value>(&["runtime", "commands", command_segment.as_str()])?
    else {
        return Ok(None);
    };
    let Some(status) = command.get("status").and_then(Value::as_str) else {
        return Ok(None);
    };

    status
        .parse()
        .map(Some)
        .map_err(|()| TaskError::InvalidState("unknown command status"))
}

fn ticket_state_from_order(command_id: Option<CommandId>, order: Order) -> OrderTicketState {
    match order.lifecycle {
        OrderLifecycle::Filled => OrderTicketState::Filled { command_id, order },
        OrderLifecycle::Cancelled => OrderTicketState::Cancelled {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Rejected => OrderTicketState::Rejected {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Failed => OrderTicketState::Failed {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Unknown
        | OrderLifecycle::Submitting
        | OrderLifecycle::Sent
        | OrderLifecycle::Accepted
        | OrderLifecycle::PartiallyFilled
        | OrderLifecycle::Cancelling => OrderTicketState::Live { command_id, order },
    }
}

fn ticket_state_from_command(
    command_id: Option<CommandId>,
    command_status: Option<CommandStatus>,
) -> OrderTicketState {
    match (command_id, command_status) {
        (Some(command_id), Some(CommandStatus::Rejected)) => OrderTicketState::Rejected {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Cancelled)) => OrderTicketState::Cancelled {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Failed)) => OrderTicketState::Failed {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Completed)) => OrderTicketState::Unknown {
            command_id: Some(command_id),
        },
        (Some(command_id), Some(status)) => OrderTicketState::CommandPending { command_id, status },
        (None, Some(_)) => OrderTicketState::Unknown { command_id: None },
        (command_id, None) => OrderTicketState::Unknown { command_id },
    }
}
