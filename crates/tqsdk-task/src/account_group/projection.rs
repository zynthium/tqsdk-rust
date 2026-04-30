use tqsdk_core::{
    AccountId, CommandId, CommandStatus, Order, OrderId, OrderLifecycle, StateReadView,
};
use tqsdk_wait::OrderTicketState;

use crate::{Result, TaskError};

use super::report::{MultiAccountOrderOutcome, MultiAccountOrderReport, MultiAccountOrderState};
use super::ticket::MultiAccountOrderLegTicket;

pub(super) fn account_report_from_view(
    view: StateReadView<'_>,
    order: &MultiAccountOrderLegTicket,
) -> Result<MultiAccountOrderReport> {
    let state = ticket_state_from_view(view, order)?;
    let (state, filled_volume, volume_left) = match state {
        OrderTicketState::Unknown { .. } => {
            (MultiAccountOrderState::Unknown, 0, order.intent.volume)
        }
        OrderTicketState::CommandPending { .. } => (
            MultiAccountOrderState::CommandPending,
            0,
            order.intent.volume,
        ),
        OrderTicketState::Live { order, .. } => live_account_order_state(&order),
        OrderTicketState::Filled { order, .. } => {
            let volume_left = order.volume_left;
            let filled = (order.volume_origin - volume_left).max(0);
            (MultiAccountOrderState::Filled, filled, volume_left)
        }
        OrderTicketState::Cancelled {
            order: ticket_order,
            ..
        } => terminal_optional_order_state(
            ticket_order.as_ref(),
            MultiAccountOrderState::Cancelled,
            order.intent.volume,
        ),
        OrderTicketState::Rejected {
            order: ticket_order,
            ..
        } => terminal_optional_order_state(
            ticket_order.as_ref(),
            MultiAccountOrderState::Rejected,
            order.intent.volume,
        ),
        OrderTicketState::Failed {
            order: ticket_order,
            ..
        } => terminal_optional_order_state(
            ticket_order.as_ref(),
            MultiAccountOrderState::Failed,
            order.intent.volume,
        ),
    };

    Ok(MultiAccountOrderReport {
        account_id: order.account_id.clone(),
        client_order_id: order.client_order_id.clone(),
        symbol: order.intent.symbol.clone(),
        requested_volume: order.intent.volume,
        filled_volume,
        volume_left,
        state,
    })
}

fn ticket_state_from_view(
    view: StateReadView<'_>,
    order: &MultiAccountOrderLegTicket,
) -> Result<OrderTicketState> {
    let order_ref = order.ticket.order();
    let account_id = AccountId::new(order_ref.account_id().to_owned());
    let order_id = OrderId::new(order_ref.order_id().to_owned());
    let order_snapshot = view.trade_state().order(&account_id, &order_id)?;
    let command_status = command_status_from_view(view, order.ticket.command_id())?;

    match order_snapshot {
        Some(order_snapshot) => Ok(ticket_state_from_order(
            order.ticket.command_id(),
            order_snapshot,
        )),
        None => Ok(ticket_state_from_command(
            order.ticket.command_id(),
            command_status,
        )),
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
        view.decode_path::<serde_json::Value>(&["runtime", "commands", command_segment.as_str()])?
    else {
        return Ok(None);
    };
    let Some(status) = command.get("status").and_then(serde_json::Value::as_str) else {
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

fn live_account_order_state(order: &Order) -> (MultiAccountOrderState, i64, i64) {
    let volume_left = order.volume_left;
    let filled = (order.volume_origin - volume_left).max(0);
    if filled > 0 {
        (
            MultiAccountOrderState::PartiallyFilled {
                filled_volume: filled,
                volume_left,
            },
            filled,
            volume_left,
        )
    } else {
        (MultiAccountOrderState::Live, 0, volume_left)
    }
}

fn terminal_optional_order_state(
    order: Option<&Order>,
    fallback: MultiAccountOrderState,
    requested_volume: i64,
) -> (MultiAccountOrderState, i64, i64) {
    let Some(order) = order else {
        return (fallback, 0, requested_volume);
    };
    let volume_left = order.volume_left;
    let filled = (order.volume_origin - volume_left).max(0);
    let state = match (order.lifecycle, filled > 0, volume_left == 0) {
        (OrderLifecycle::Filled, _, _) => MultiAccountOrderState::Filled,
        (_, true, false) => MultiAccountOrderState::PartiallyFilled {
            filled_volume: filled,
            volume_left,
        },
        _ => fallback,
    };
    (state, filled, volume_left)
}

pub(super) fn outcome_from_reports(
    accounts: &[MultiAccountOrderReport],
) -> Option<MultiAccountOrderOutcome> {
    if accounts.iter().any(is_pending_state) {
        return None;
    }

    let all_filled = accounts
        .iter()
        .all(|account| matches!(account.state, MultiAccountOrderState::Filled));
    if all_filled {
        return Some(MultiAccountOrderOutcome::AllFilled {
            accounts: accounts.to_vec(),
        });
    }

    let any_filled = accounts.iter().any(|account| account.filled_volume > 0);
    if any_filled {
        return Some(needs_attention_from_reports(accounts));
    }

    if accounts
        .iter()
        .any(|account| matches!(account.state, MultiAccountOrderState::Failed))
    {
        return Some(MultiAccountOrderOutcome::Failed {
            accounts: accounts.to_vec(),
        });
    }
    if accounts
        .iter()
        .any(|account| matches!(account.state, MultiAccountOrderState::Rejected))
    {
        return Some(MultiAccountOrderOutcome::Rejected {
            accounts: accounts.to_vec(),
        });
    }
    if accounts
        .iter()
        .any(|account| matches!(account.state, MultiAccountOrderState::Cancelled))
    {
        return Some(MultiAccountOrderOutcome::Cancelled {
            accounts: accounts.to_vec(),
        });
    }
    None
}

pub(super) fn needs_attention_from_reports(
    accounts: &[MultiAccountOrderReport],
) -> MultiAccountOrderOutcome {
    let filled_accounts = accounts
        .iter()
        .filter(|account| account.filled_volume > 0)
        .map(|account| account.account_id.clone())
        .collect();
    let unfilled_accounts = accounts
        .iter()
        .filter(|account| account.filled_volume < account.requested_volume)
        .map(|account| account.account_id.clone())
        .collect();
    MultiAccountOrderOutcome::NeedsAttention {
        filled_accounts,
        unfilled_accounts,
        accounts: accounts.to_vec(),
    }
}

pub(super) fn has_open_account_exposure(accounts: &[MultiAccountOrderReport]) -> bool {
    let has_filled = accounts.iter().any(|account| account.filled_volume > 0);
    let has_unfilled = accounts.iter().any(|account| {
        account.volume_left > 0
            && !matches!(
                account.state,
                MultiAccountOrderState::Rejected
                    | MultiAccountOrderState::Failed
                    | MultiAccountOrderState::Cancelled
            )
    });
    has_filled && has_unfilled
}

fn is_pending_state(account: &MultiAccountOrderReport) -> bool {
    matches!(
        account.state,
        MultiAccountOrderState::Unknown
            | MultiAccountOrderState::CommandPending
            | MultiAccountOrderState::Live
    )
}
