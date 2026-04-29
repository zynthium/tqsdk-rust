#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
use std::time::Duration;

use tqsdk_core::{
    AccountId, CommandId, CommandStatus, Order, OrderId, OrderLifecycle, Revision, StateReadView,
    TradeDirection, TradeOffset,
};
use tqsdk_wait::{OrderTicket, OrderTicketState};

use crate::{Result, TaskError, TaskHost, TaskOrderIntent};

/// Positive rational weight for one account in an account group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ratio {
    numerator: u32,
    denominator: u32,
}

/// One account and its allocation ratio inside an account group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAllocation {
    account_id: String,
    ratio: Ratio,
}

/// Typed account group for task-layer multi-account execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountGroup {
    accounts: Vec<AccountAllocation>,
    min_volume_per_account: i64,
}

/// Builder for [`AccountGroup`].
#[derive(Debug, Default)]
pub struct AccountGroupBuilder {
    accounts: Vec<AccountAllocation>,
    min_volume_per_account: i64,
}

/// Planned order size for one account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedAccountOrder {
    account_id: String,
    volume: i64,
}

/// Deterministic account allocation plan for one total order volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAllocationPlan {
    allocations: Vec<AllocatedAccountOrder>,
}

/// Failure policy for a multi-account order when account outcomes diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountFailurePolicy {
    ReportExposure,
    FlattenFilledAccounts,
}

/// Builder for one multi-account task order.
pub struct MultiAccountOrderBuilder<'a> {
    host: &'a mut TaskHost,
    accounts: AccountGroup,
    group_id: Option<String>,
    max_unhedged: Option<Duration>,
    failure_policy: AccountFailurePolicy,
}

/// Draft multi-account order after side and offset are selected.
pub struct MultiAccountOrderDraft<'a> {
    builder: MultiAccountOrderBuilder<'a>,
    symbol: String,
    direction: TradeDirection,
    offset: TradeOffset,
    total_volume: i64,
    limit_price: Option<f64>,
}

/// Ticket returned after submitting or recovering a multi-account order.
#[derive(Debug, Clone)]
pub struct MultiAccountOrderTicket {
    group_id: String,
    max_unhedged: Option<Duration>,
    failure_policy: AccountFailurePolicy,
    orders: Vec<MultiAccountOrderLegTicket>,
}

/// Revision-bound report for one multi-account order group.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiAccountOrderGroupReport {
    revision: Revision,
    group_id: String,
    status: MultiAccountOrderStatus,
}

/// Submitted or recovered ticket for one account allocation.
#[derive(Debug, Clone)]
pub struct MultiAccountOrderLegTicket {
    account_id: String,
    client_order_id: String,
    intent: TaskOrderIntent,
    ticket: OrderTicket,
}

/// State of one account allocation projected from its wait-layer order ticket.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiAccountOrderState {
    Unknown,
    CommandPending,
    Live,
    Filled,
    PartiallyFilled {
        filled_volume: i64,
        volume_left: i64,
    },
    Cancelled,
    Rejected,
    Failed,
}

/// Stable report for one account allocation.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiAccountOrderReport {
    pub account_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub requested_volume: i64,
    pub filled_volume: i64,
    pub volume_left: i64,
    pub state: MultiAccountOrderState,
}

/// Current multi-account order status.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiAccountOrderStatus {
    Pending {
        accounts: Vec<MultiAccountOrderReport>,
    },
    Finished(MultiAccountOrderOutcome),
}

/// Terminal multi-account order outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum MultiAccountOrderOutcome {
    AllFilled {
        accounts: Vec<MultiAccountOrderReport>,
    },
    Cancelled {
        accounts: Vec<MultiAccountOrderReport>,
    },
    Rejected {
        accounts: Vec<MultiAccountOrderReport>,
    },
    Failed {
        accounts: Vec<MultiAccountOrderReport>,
    },
    NeedsAttention {
        filled_accounts: Vec<String>,
        unfilled_accounts: Vec<String>,
        accounts: Vec<MultiAccountOrderReport>,
    },
}

impl MultiAccountOrderGroupReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn status(&self) -> &MultiAccountOrderStatus {
        &self.status
    }

    #[must_use]
    pub fn accounts(&self) -> &[MultiAccountOrderReport] {
        match &self.status {
            MultiAccountOrderStatus::Pending { accounts } => accounts,
            MultiAccountOrderStatus::Finished(outcome) => outcome.accounts(),
        }
    }
}

impl MultiAccountOrderOutcome {
    #[must_use]
    pub fn accounts(&self) -> &[MultiAccountOrderReport] {
        match self {
            Self::AllFilled { accounts }
            | Self::Cancelled { accounts }
            | Self::Rejected { accounts }
            | Self::Failed { accounts }
            | Self::NeedsAttention { accounts, .. } => accounts,
        }
    }
}

impl Ratio {
    pub fn new(numerator: u32, denominator: u32) -> Result<Self> {
        if numerator == 0 {
            return Err(TaskError::InvalidState(
                "account allocation ratio numerator must be positive",
            ));
        }
        if denominator == 0 {
            return Err(TaskError::InvalidState(
                "account allocation ratio denominator must be positive",
            ));
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    #[must_use]
    pub fn numerator(&self) -> u32 {
        self.numerator
    }

    #[must_use]
    pub fn denominator(&self) -> u32 {
        self.denominator
    }
}

impl AccountAllocation {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn ratio(&self) -> Ratio {
        self.ratio
    }
}

impl AccountGroup {
    #[must_use]
    pub fn builder() -> AccountGroupBuilder {
        AccountGroupBuilder::default()
    }

    #[must_use]
    pub fn accounts(&self) -> &[AccountAllocation] {
        &self.accounts
    }

    pub fn allocate(&self, total_volume: i64) -> Result<AccountAllocationPlan> {
        if total_volume <= 0 {
            return Err(TaskError::InvalidState("total volume must be positive"));
        }
        if self.accounts.is_empty() {
            return Err(TaskError::InvalidState("account group cannot be empty"));
        }
        if self.min_volume_per_account > 0
            && total_volume < self.min_volume_per_account * self.accounts.len() as i64
        {
            return Err(TaskError::InvalidState(
                "total volume cannot satisfy account minimum volume",
            ));
        }

        let common_denominator: u128 = self
            .accounts
            .iter()
            .map(|allocation| allocation.ratio.denominator as u128)
            .product();
        let weights: Vec<u128> = self
            .accounts
            .iter()
            .map(|allocation| {
                allocation.ratio.numerator as u128
                    * (common_denominator / allocation.ratio.denominator as u128)
            })
            .collect();
        let total_weight: u128 = weights.iter().sum();
        let mut rows: Vec<(usize, i64, u128)> = self
            .accounts
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let weighted = total_volume as u128 * weights[index];
                let whole = (weighted / total_weight) as i64;
                let remainder = weighted % total_weight;
                (index, whole, remainder)
            })
            .collect();

        let allocated: i64 = rows.iter().map(|(_, volume, _)| *volume).sum();
        let remaining = (total_volume - allocated).max(0) as usize;
        rows.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
        for row in rows.iter_mut().take(remaining) {
            row.1 += 1;
        }
        rows.sort_by_key(|row| row.0);

        if self.min_volume_per_account > 0
            && rows
                .iter()
                .any(|(_, volume, _)| *volume < self.min_volume_per_account)
        {
            return Err(TaskError::InvalidState(
                "total volume cannot satisfy account minimum volume",
            ));
        }

        Ok(AccountAllocationPlan {
            allocations: rows
                .into_iter()
                .map(|(index, volume, _)| AllocatedAccountOrder {
                    account_id: self.accounts[index].account_id.clone(),
                    volume,
                })
                .collect(),
        })
    }
}

impl AccountGroupBuilder {
    #[must_use]
    pub fn add(mut self, account_id: impl Into<String>, ratio: Ratio) -> Self {
        self.accounts.push(AccountAllocation {
            account_id: account_id.into(),
            ratio,
        });
        self
    }

    #[must_use]
    pub fn min_volume_per_account(mut self, min_volume: i64) -> Self {
        self.min_volume_per_account = min_volume;
        self
    }

    pub fn build(self) -> Result<AccountGroup> {
        if self.accounts.is_empty() {
            return Err(TaskError::InvalidState("account group cannot be empty"));
        }
        if self.min_volume_per_account < 0 {
            return Err(TaskError::InvalidState(
                "account minimum volume cannot be negative",
            ));
        }
        let mut seen = HashSet::new();
        for account in &self.accounts {
            if account.account_id.is_empty() {
                return Err(TaskError::InvalidState("account id cannot be empty"));
            }
            if !seen.insert(account.account_id.as_str()) {
                return Err(TaskError::InvalidState(
                    "duplicate account id in account group",
                ));
            }
        }
        Ok(AccountGroup {
            accounts: self.accounts,
            min_volume_per_account: self.min_volume_per_account,
        })
    }
}

impl AccountAllocationPlan {
    #[must_use]
    pub fn allocations(&self) -> &[AllocatedAccountOrder] {
        &self.allocations
    }
}

impl AllocatedAccountOrder {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn volume(&self) -> i64 {
        self.volume
    }
}

impl<'a> MultiAccountOrderBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, accounts: AccountGroup) -> Self {
        Self {
            host,
            accounts,
            group_id: None,
            max_unhedged: None,
            failure_policy: AccountFailurePolicy::ReportExposure,
        }
    }

    #[must_use]
    pub fn client_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    #[must_use]
    pub fn max_unhedged(mut self, duration: Duration) -> Self {
        self.max_unhedged = Some(duration);
        self
    }

    #[must_use]
    pub fn on_account_failed(mut self, policy: AccountFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    #[must_use]
    pub fn buy_open(
        self,
        symbol: impl AsRef<str>,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        self.intent(symbol, TradeDirection::Buy, TradeOffset::Open, total_volume)
    }

    #[must_use]
    pub fn sell_open(
        self,
        symbol: impl AsRef<str>,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        self.intent(
            symbol,
            TradeDirection::Sell,
            TradeOffset::Open,
            total_volume,
        )
    }

    fn intent(
        self,
        symbol: impl AsRef<str>,
        direction: TradeDirection,
        offset: TradeOffset,
        total_volume: i64,
    ) -> MultiAccountOrderDraft<'a> {
        MultiAccountOrderDraft {
            builder: self,
            symbol: symbol.as_ref().to_owned(),
            direction,
            offset,
            total_volume,
            limit_price: None,
        }
    }
}

impl MultiAccountOrderDraft<'_> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.limit_price = Some(price);
        self
    }

    pub async fn send_once(self) -> Result<MultiAccountOrderTicket> {
        submit_multi_account_order(self).await
    }
}

impl MultiAccountOrderTicket {
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn max_unhedged(&self) -> Option<Duration> {
        self.max_unhedged
    }

    #[must_use]
    pub fn failure_policy(&self) -> AccountFailurePolicy {
        self.failure_policy
    }

    #[must_use]
    pub fn orders(&self) -> &[MultiAccountOrderLegTicket] {
        &self.orders
    }

    pub fn status(&self, api: &tqsdk_wait::TqApi) -> Result<MultiAccountOrderStatus> {
        let accounts = self.account_reports(api)?;
        Ok(match outcome_from_reports(&accounts) {
            Some(outcome) => MultiAccountOrderStatus::Finished(outcome),
            None => MultiAccountOrderStatus::Pending { accounts },
        })
    }

    pub fn report(&self, api: &tqsdk_wait::TqApi) -> Result<MultiAccountOrderGroupReport> {
        let snapshot = api.session().reader().read();
        let revision = snapshot.revision();
        let accounts = self.account_reports_from_view(snapshot.view())?;
        let status = match outcome_from_reports(&accounts) {
            Some(outcome) => MultiAccountOrderStatus::Finished(outcome),
            None => MultiAccountOrderStatus::Pending { accounts },
        };
        Ok(MultiAccountOrderGroupReport {
            revision,
            group_id: self.group_id.clone(),
            status,
        })
    }

    pub fn outcome(&self, api: &tqsdk_wait::TqApi) -> Result<Option<MultiAccountOrderOutcome>> {
        let accounts = self.account_reports(api)?;
        Ok(outcome_from_reports(&accounts))
    }

    pub async fn wait_finished(
        &self,
        host: &mut TaskHost,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<MultiAccountOrderOutcome> {
        let mut exposure_started_at = None;
        loop {
            let accounts = self.account_reports(host.api())?;
            if let Some(outcome) = outcome_from_reports(&accounts) {
                return Ok(outcome);
            }

            let exposure_deadline = if let Some(max_unhedged) = self
                .max_unhedged
                .filter(|_| has_open_account_exposure(&accounts))
            {
                let started_at = *exposure_started_at.get_or_insert_with(tokio::time::Instant::now);
                let exposure_deadline = started_at + max_unhedged;
                if tokio::time::Instant::now() >= exposure_deadline {
                    return Ok(needs_attention_from_reports(&accounts));
                }
                Some(exposure_deadline)
            } else {
                exposure_started_at = None;
                None
            };

            let wait_deadline = match (deadline, exposure_deadline) {
                (Some(deadline), Some(exposure_deadline)) => {
                    Some(earlier_deadline(deadline, exposure_deadline))
                }
                (Some(deadline), None) => Some(deadline),
                (None, Some(exposure_deadline)) => Some(exposure_deadline),
                (None, None) => None,
            };

            if !host.wait_update(wait_deadline).await? {
                if let Some(wait_deadline) = wait_deadline
                    && tokio::time::Instant::now() < wait_deadline
                {
                    tokio::time::sleep_until(wait_deadline).await;
                }
                let accounts = self.account_reports(host.api())?;
                return Ok(needs_attention_from_reports(&accounts));
            }
        }
    }

    fn account_reports(&self, api: &tqsdk_wait::TqApi) -> Result<Vec<MultiAccountOrderReport>> {
        let snapshot = api.session().reader().read();
        self.account_reports_from_view(snapshot.view())
    }

    fn account_reports_from_view(
        &self,
        view: StateReadView<'_>,
    ) -> Result<Vec<MultiAccountOrderReport>> {
        self.orders
            .iter()
            .map(|order| account_report_from_view(view, order))
            .collect()
    }
}

fn earlier_deadline(
    left: tokio::time::Instant,
    right: tokio::time::Instant,
) -> tokio::time::Instant {
    if left <= right { left } else { right }
}

impl MultiAccountOrderLegTicket {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn intent(&self) -> &TaskOrderIntent {
        &self.intent
    }

    #[must_use]
    pub fn ticket(&self) -> &OrderTicket {
        &self.ticket
    }
}

async fn submit_multi_account_order(
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
    host.preflight_task_orders(&intents)?;

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

fn account_report_from_view(
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
            let filled = (order.volume_orign - volume_left).max(0);
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
    let filled = (order.volume_orign - volume_left).max(0);
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
    let filled = (order.volume_orign - volume_left).max(0);
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

fn outcome_from_reports(accounts: &[MultiAccountOrderReport]) -> Option<MultiAccountOrderOutcome> {
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

fn needs_attention_from_reports(accounts: &[MultiAccountOrderReport]) -> MultiAccountOrderOutcome {
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

fn has_open_account_exposure(accounts: &[MultiAccountOrderReport]) -> bool {
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
