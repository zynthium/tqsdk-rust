use tqsdk_core::{Order, OrderLifecycle, StateReadView};
use tqsdk_wait::OrderTicketState;

use crate::Result;
use crate::order_projection::{
    fallback_volume_progress, order_volume_progress,
    ticket_state_from_view as projected_ticket_state_from_view,
};

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
    projected_ticket_state_from_view(
        view,
        order_ref.account_id(),
        order_ref.order_id(),
        order.ticket.command_id(),
    )
}

fn live_account_order_state(order: &Order) -> (MultiAccountOrderState, i64, i64) {
    let progress = order_volume_progress(order);
    if progress.filled_volume > 0 {
        (
            MultiAccountOrderState::PartiallyFilled {
                filled_volume: progress.filled_volume,
                volume_left: progress.volume_left,
            },
            progress.filled_volume,
            progress.volume_left,
        )
    } else {
        (MultiAccountOrderState::Live, 0, progress.volume_left)
    }
}

fn terminal_optional_order_state(
    order: Option<&Order>,
    fallback: MultiAccountOrderState,
    requested_volume: i64,
) -> (MultiAccountOrderState, i64, i64) {
    let Some(order) = order else {
        let progress = fallback_volume_progress(requested_volume);
        return (fallback, progress.filled_volume, progress.volume_left);
    };
    let progress = order_volume_progress(order);
    let state = match (
        order.lifecycle,
        progress.filled_volume > 0,
        progress.volume_left == 0,
    ) {
        (OrderLifecycle::Filled, _, _) => MultiAccountOrderState::Filled,
        (_, true, false) => MultiAccountOrderState::PartiallyFilled {
            filled_volume: progress.filled_volume,
            volume_left: progress.volume_left,
        },
        _ => fallback,
    };
    (state, progress.filled_volume, progress.volume_left)
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
