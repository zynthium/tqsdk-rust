use crate::{Result, TaskError, TaskOrderIntent};

use super::builder::{MultiAccountOrderBuilder, MultiAccountOrderDraft};
use super::report::AccountFailurePolicy;
use super::ticket::{MultiAccountOrderLegTicket, MultiAccountOrderTicket};

pub(super) async fn submit_multi_account_order(
    draft: MultiAccountOrderDraft<'_>,
) -> Result<MultiAccountOrderTicket> {
    let MultiAccountOrderDraft {
        builder,
        symbol,
        direction,
        offset,
        total_volume,
        limit_price,
    } = draft;
    let MultiAccountOrderBuilder {
        host,
        accounts,
        group_id,
        max_unhedged,
        failure_policy,
    } = builder;

    let group_id =
        group_id
            .filter(|value| !value.trim().is_empty())
            .ok_or(TaskError::InvalidState(
                "multi-account group id is required",
            ))?;
    if failure_policy == AccountFailurePolicy::FlattenFilledAccounts {
        return Err(TaskError::Unsupported(
            "automatic multi-account flatten policy is not implemented",
        ));
    }
    let limit_price = limit_price.ok_or(TaskError::InvalidState("limit price is required"))?;
    let allocation_plan = accounts.allocate(total_volume)?;
    let mut intents = Vec::new();
    for allocation in allocation_plan.allocations() {
        intents.push(TaskOrderIntent {
            account_id: allocation.account_id().to_owned(),
            symbol: symbol.clone(),
            direction,
            offset: Some(offset),
            volume: allocation.volume(),
            limit_price: Some(limit_price),
        });
    }
    let preflight_intents = intents
        .iter()
        .enumerate()
        .map(|(index, intent)| (intent, format!("{group_id}:acct:{index}")))
        .collect::<Vec<_>>();
    host.preflight_new_task_orders(
        preflight_intents
            .iter()
            .map(|(intent, client_order_id)| (*intent, client_order_id.as_str())),
    )?;

    let mut orders = Vec::new();
    let total_accounts = intents.len();
    for (index, intent) in intents.into_iter().enumerate() {
        let client_order_id = format!("{group_id}:acct:{index}");
        match host
            .submit_prechecked_task_order_once(intent.clone(), client_order_id.clone())
            .await
        {
            Ok(ticket) => orders.push(MultiAccountOrderLegTicket {
                account_id: intent.account_id.clone(),
                client_order_id,
                intent,
                ticket,
            }),
            Err(error) if orders.is_empty() => return Err(error),
            Err(_) => {
                return Err(TaskError::MultiAccountPartialSubmit {
                    group_id,
                    submitted_accounts: orders.len(),
                    total_accounts,
                    reason: "account submit failed after group preflight",
                });
            }
        }
    }

    Ok(MultiAccountOrderTicket {
        group_id,
        max_unhedged,
        failure_policy,
        orders,
    })
}
