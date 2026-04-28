#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{
    AccountId, CommandId, CommandStatus, Order, OrderId, OrderLifecycle, Revision, StateReadView,
    TradeDirection, TradeOffset,
};
use tqsdk_wait::{OrderTicket, OrderTicketState};

use crate::{Result, TaskError, TaskHost, TaskOrderIntent};

/// Policy to apply when an execution group has desynchronized leg results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgePolicy {
    ReportExposure,
    FlattenFilledLegs,
}

/// A leg intent with its deterministic group-scoped client order id.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionLegIntent {
    pub client_order_id: String,
    pub intent: TaskOrderIntent,
}

/// Submitted or recovered ticket for one execution-group leg.
#[derive(Debug, Clone)]
pub struct ExecutionLegTicket {
    client_order_id: String,
    intent: TaskOrderIntent,
    ticket: OrderTicket,
}

impl ExecutionLegTicket {
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

/// Builder for a task-layer execution group.
pub struct ExecutionGroupBuilder<'a> {
    host: &'a mut TaskHost,
    account_id: String,
    group_id: Option<String>,
    max_unhedged: Option<Duration>,
    hedge_policy: HedgePolicy,
    legs: Vec<TaskOrderIntent>,
}

/// Builder for one execution-group leg before side/offset selection.
pub struct ExecutionLegBuilder<'a> {
    group: ExecutionGroupBuilder<'a>,
    symbol: String,
}

/// Draft execution-group leg after side/offset selection.
pub struct ExecutionLegDraft<'a> {
    group: ExecutionGroupBuilder<'a>,
    intent: TaskOrderIntent,
}

/// Ticket returned after submitting or recovering an execution group.
#[derive(Debug, Clone)]
pub struct ExecutionGroupTicket {
    group_id: String,
    account_id: String,
    hedge_policy: HedgePolicy,
    max_unhedged: Option<Duration>,
    legs: Vec<ExecutionLegTicket>,
}

/// Revision-bound report for an execution group.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionGroupReport {
    revision: Revision,
    group_id: String,
    account_id: String,
    status: ExecutionGroupStatus,
}

/// State of one execution-group leg projected from its wait-layer order ticket.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionLegState {
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

/// Stable report for one execution-group leg.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionLegReport {
    pub client_order_id: String,
    pub account_id: String,
    pub symbol: String,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub requested_volume: i64,
    pub filled_volume: i64,
    pub volume_left: i64,
    pub state: ExecutionLegState,
}

/// Exposure summary when a group reaches a mixed terminal state.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionExposure {
    pub filled_symbols: Vec<String>,
    pub unfilled_symbols: Vec<String>,
}

/// Current group status.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionGroupStatus {
    Pending { legs: Vec<ExecutionLegReport> },
    Finished(ExecutionGroupOutcome),
}

/// Terminal group outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionGroupOutcome {
    AllFilled {
        legs: Vec<ExecutionLegReport>,
    },
    Cancelled {
        legs: Vec<ExecutionLegReport>,
    },
    Rejected {
        legs: Vec<ExecutionLegReport>,
    },
    Failed {
        legs: Vec<ExecutionLegReport>,
    },
    NeedsHedge {
        exposure: ExecutionExposure,
        legs: Vec<ExecutionLegReport>,
    },
}

impl ExecutionGroupReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn status(&self) -> &ExecutionGroupStatus {
        &self.status
    }

    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegReport] {
        match &self.status {
            ExecutionGroupStatus::Pending { legs } => legs,
            ExecutionGroupStatus::Finished(outcome) => outcome.legs(),
        }
    }
}

impl ExecutionGroupOutcome {
    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegReport] {
        match self {
            Self::AllFilled { legs }
            | Self::Cancelled { legs }
            | Self::Rejected { legs }
            | Self::Failed { legs }
            | Self::NeedsHedge { legs, .. } => legs,
        }
    }
}

impl<'a> ExecutionGroupBuilder<'a> {
    pub(crate) fn new(host: &'a mut TaskHost, account_id: String) -> Self {
        Self {
            host,
            account_id,
            group_id: None,
            max_unhedged: None,
            hedge_policy: HedgePolicy::ReportExposure,
            legs: Vec::new(),
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
    pub fn on_leg_failed(mut self, policy: HedgePolicy) -> Self {
        self.hedge_policy = policy;
        self
    }

    #[must_use]
    pub fn leg(self, symbol: impl AsRef<str>) -> ExecutionLegBuilder<'a> {
        ExecutionLegBuilder {
            group: self,
            symbol: symbol.as_ref().to_owned(),
        }
    }

    pub async fn send_once(self) -> Result<ExecutionGroupTicket> {
        submit_group(self).await
    }
}

impl<'a> ExecutionLegBuilder<'a> {
    #[must_use]
    pub fn buy_open(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Buy, Some(TradeOffset::Open), volume)
    }

    #[must_use]
    pub fn sell_open(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Sell, Some(TradeOffset::Open), volume)
    }

    #[must_use]
    pub fn buy_close(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Buy, Some(TradeOffset::Close), volume)
    }

    #[must_use]
    pub fn sell_close(self, volume: i64) -> ExecutionLegDraft<'a> {
        self.intent(TradeDirection::Sell, Some(TradeOffset::Close), volume)
    }

    fn intent(
        self,
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
    ) -> ExecutionLegDraft<'a> {
        ExecutionLegDraft {
            intent: TaskOrderIntent {
                account_id: self.group.account_id.clone(),
                symbol: self.symbol,
                direction,
                offset,
                volume,
                limit_price: None,
            },
            group: self.group,
        }
    }
}

impl<'a> ExecutionLegDraft<'a> {
    #[must_use]
    pub fn limit(mut self, price: f64) -> Self {
        self.intent.limit_price = Some(price);
        self
    }

    #[must_use]
    pub fn leg(mut self, symbol: impl AsRef<str>) -> ExecutionLegBuilder<'a> {
        self.group.legs.push(self.intent);
        self.group.leg(symbol)
    }

    pub async fn send_once(mut self) -> Result<ExecutionGroupTicket> {
        self.group.legs.push(self.intent);
        submit_group(self.group).await
    }
}

impl ExecutionGroupTicket {
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn hedge_policy(&self) -> HedgePolicy {
        self.hedge_policy
    }

    #[must_use]
    pub fn max_unhedged(&self) -> Option<Duration> {
        self.max_unhedged
    }

    #[must_use]
    pub fn legs(&self) -> &[ExecutionLegTicket] {
        &self.legs
    }

    pub fn status(&self, api: &tqsdk_wait::TqApi) -> Result<ExecutionGroupStatus> {
        let legs = self.leg_reports(api)?;
        Ok(match outcome_from_reports(&legs) {
            Some(outcome) => ExecutionGroupStatus::Finished(outcome),
            None => ExecutionGroupStatus::Pending { legs },
        })
    }

    pub fn report(&self, api: &tqsdk_wait::TqApi) -> Result<ExecutionGroupReport> {
        let snapshot = api.session().reader().read();
        let revision = snapshot.revision();
        let legs = self.leg_reports_from_view(snapshot.view())?;
        let status = match outcome_from_reports(&legs) {
            Some(outcome) => ExecutionGroupStatus::Finished(outcome),
            None => ExecutionGroupStatus::Pending { legs },
        };
        Ok(ExecutionGroupReport {
            revision,
            group_id: self.group_id.clone(),
            account_id: self.account_id.clone(),
            status,
        })
    }

    pub fn outcome(&self, api: &tqsdk_wait::TqApi) -> Result<Option<ExecutionGroupOutcome>> {
        let legs = self.leg_reports(api)?;
        Ok(outcome_from_reports(&legs))
    }

    pub async fn wait_finished(
        &self,
        host: &mut TaskHost,
        deadline: tokio::time::Instant,
    ) -> Result<ExecutionGroupOutcome> {
        let mut exposure_started_at = None;
        loop {
            let legs = self.leg_reports(host.api())?;
            if let Some(outcome) = outcome_from_reports(&legs) {
                return Ok(outcome);
            }

            let exposure_deadline = if let Some(max_unhedged) =
                self.max_unhedged.filter(|_| has_open_exposure(&legs))
            {
                let started_at = *exposure_started_at.get_or_insert_with(tokio::time::Instant::now);
                let exposure_deadline = started_at + max_unhedged;
                if tokio::time::Instant::now() >= exposure_deadline {
                    return Ok(ExecutionGroupOutcome::NeedsHedge {
                        exposure: exposure_from_reports(&legs),
                        legs,
                    });
                }
                Some(exposure_deadline)
            } else {
                exposure_started_at = None;
                None
            };

            let wait_deadline = exposure_deadline
                .map(|exposure_deadline| earlier_deadline(deadline, exposure_deadline))
                .unwrap_or(deadline);

            if !host.wait_update(Some(wait_deadline)).await? {
                if tokio::time::Instant::now() < wait_deadline {
                    tokio::time::sleep_until(wait_deadline).await;
                }
                let legs = self.leg_reports(host.api())?;
                return Ok(ExecutionGroupOutcome::NeedsHedge {
                    exposure: exposure_from_reports(&legs),
                    legs,
                });
            }
        }
    }

    fn leg_reports(&self, api: &tqsdk_wait::TqApi) -> Result<Vec<ExecutionLegReport>> {
        let snapshot = api.session().reader().read();
        self.leg_reports_from_view(snapshot.view())
    }

    fn leg_reports_from_view(&self, view: StateReadView<'_>) -> Result<Vec<ExecutionLegReport>> {
        self.legs
            .iter()
            .map(|leg| leg_report_from_view(view, leg))
            .collect()
    }
}

fn earlier_deadline(
    left: tokio::time::Instant,
    right: tokio::time::Instant,
) -> tokio::time::Instant {
    if left <= right { left } else { right }
}

async fn submit_group(mut builder: ExecutionGroupBuilder<'_>) -> Result<ExecutionGroupTicket> {
    let group_id = builder
        .group_id
        .take()
        .ok_or(TaskError::InvalidState("execution group id is required"))?;
    if group_id.trim().is_empty() {
        return Err(TaskError::InvalidState(
            "execution group id must not be empty",
        ));
    }
    if builder.legs.len() < 2 {
        return Err(TaskError::InvalidState(
            "execution group requires at least two legs",
        ));
    }
    if builder.hedge_policy == HedgePolicy::FlattenFilledLegs {
        return Err(TaskError::Unsupported(
            "automatic hedge policy is not implemented",
        ));
    }

    let leg_intents = builder
        .legs
        .into_iter()
        .enumerate()
        .map(|(index, intent)| ExecutionLegIntent {
            client_order_id: format!("{group_id}:leg:{index}"),
            intent,
        })
        .collect::<Vec<_>>();

    for leg in &leg_intents {
        builder.host.preflight_task_order(&leg.intent)?;
    }

    let mut submitted = Vec::with_capacity(leg_intents.len());
    let total_legs = leg_intents.len();
    for leg in leg_intents {
        match builder
            .host
            .submit_prechecked_task_order_once(leg.intent.clone(), leg.client_order_id.as_str())
            .await
        {
            Ok(ticket) => submitted.push(ExecutionLegTicket {
                client_order_id: leg.client_order_id,
                intent: leg.intent,
                ticket,
            }),
            Err(error) if submitted.is_empty() => return Err(error),
            Err(_) => {
                return Err(TaskError::ExecutionGroupPartialSubmit {
                    group_id,
                    submitted_legs: submitted.len(),
                    total_legs,
                    reason: "leg submit failed after group preflight",
                });
            }
        }
    }

    Ok(ExecutionGroupTicket {
        group_id,
        account_id: builder.account_id,
        hedge_policy: builder.hedge_policy,
        max_unhedged: builder.max_unhedged,
        legs: submitted,
    })
}

fn leg_report_from_view(
    view: StateReadView<'_>,
    leg: &ExecutionLegTicket,
) -> Result<ExecutionLegReport> {
    let state = ticket_state_from_view(view, leg)?;
    let (state, filled_volume, volume_left) = match state {
        OrderTicketState::Unknown { .. } => (ExecutionLegState::Unknown, 0, leg.intent.volume),
        OrderTicketState::CommandPending { .. } => {
            (ExecutionLegState::CommandPending, 0, leg.intent.volume)
        }
        OrderTicketState::Live { order, .. } => live_leg_state(&order),
        OrderTicketState::Filled { order, .. } => {
            let volume_left = order.volume_left;
            let filled = (order.volume_orign - volume_left).max(0);
            (ExecutionLegState::Filled, filled, volume_left)
        }
        OrderTicketState::Cancelled { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Cancelled,
            leg.intent.volume,
        ),
        OrderTicketState::Rejected { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Rejected,
            leg.intent.volume,
        ),
        OrderTicketState::Failed { order, .. } => terminal_optional_order_state(
            order.as_ref(),
            ExecutionLegState::Failed,
            leg.intent.volume,
        ),
    };

    Ok(ExecutionLegReport {
        client_order_id: leg.client_order_id.clone(),
        account_id: leg.intent.account_id.clone(),
        symbol: leg.intent.symbol.clone(),
        direction: leg.intent.direction,
        offset: leg.intent.offset,
        requested_volume: leg.intent.volume,
        filled_volume,
        volume_left,
        state,
    })
}

fn ticket_state_from_view(
    view: StateReadView<'_>,
    leg: &ExecutionLegTicket,
) -> Result<OrderTicketState> {
    let order_ref = leg.ticket.order();
    let account_id = AccountId::new(order_ref.account_id().to_owned());
    let order_id = OrderId::new(order_ref.order_id().to_owned());
    let order = view.trade_state().order(&account_id, &order_id)?;
    let command_status = command_status_from_view(view, leg.ticket.command_id())?;

    match order {
        Some(order) => Ok(ticket_state_from_order(leg.ticket.command_id(), order)),
        None => Ok(ticket_state_from_command(
            leg.ticket.command_id(),
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

fn live_leg_state(order: &Order) -> (ExecutionLegState, i64, i64) {
    let volume_left = order.volume_left;
    let filled = (order.volume_orign - volume_left).max(0);
    if filled > 0 {
        (
            ExecutionLegState::PartiallyFilled {
                filled_volume: filled,
                volume_left,
            },
            filled,
            volume_left,
        )
    } else {
        (ExecutionLegState::Live, 0, volume_left)
    }
}

fn terminal_optional_order_state(
    order: Option<&Order>,
    fallback: ExecutionLegState,
    requested_volume: i64,
) -> (ExecutionLegState, i64, i64) {
    let Some(order) = order else {
        return (fallback, 0, requested_volume);
    };
    let volume_left = order.volume_left;
    let filled = (order.volume_orign - volume_left).max(0);
    let state = match (order.lifecycle, filled > 0, volume_left == 0) {
        (OrderLifecycle::Filled, _, _) => ExecutionLegState::Filled,
        (_, true, false) => ExecutionLegState::PartiallyFilled {
            filled_volume: filled,
            volume_left,
        },
        _ => fallback,
    };
    (state, filled, volume_left)
}

fn outcome_from_reports(legs: &[ExecutionLegReport]) -> Option<ExecutionGroupOutcome> {
    if legs.iter().any(is_pending_state) {
        return None;
    }

    let any_filled = legs.iter().any(|leg| leg.filled_volume > 0);
    let all_filled = legs
        .iter()
        .all(|leg| matches!(leg.state, ExecutionLegState::Filled));
    if all_filled {
        return Some(ExecutionGroupOutcome::AllFilled {
            legs: legs.to_vec(),
        });
    }

    if any_filled {
        return Some(ExecutionGroupOutcome::NeedsHedge {
            exposure: exposure_from_reports(legs),
            legs: legs.to_vec(),
        });
    }

    if legs
        .iter()
        .any(|leg| matches!(leg.state, ExecutionLegState::Failed))
    {
        return Some(ExecutionGroupOutcome::Failed {
            legs: legs.to_vec(),
        });
    }
    if legs
        .iter()
        .any(|leg| matches!(leg.state, ExecutionLegState::Rejected))
    {
        return Some(ExecutionGroupOutcome::Rejected {
            legs: legs.to_vec(),
        });
    }
    if legs
        .iter()
        .any(|leg| matches!(leg.state, ExecutionLegState::Cancelled))
    {
        return Some(ExecutionGroupOutcome::Cancelled {
            legs: legs.to_vec(),
        });
    }
    None
}

fn is_pending_state(leg: &ExecutionLegReport) -> bool {
    matches!(
        leg.state,
        ExecutionLegState::Unknown | ExecutionLegState::CommandPending | ExecutionLegState::Live
    )
}

fn has_open_exposure(legs: &[ExecutionLegReport]) -> bool {
    let has_filled = legs.iter().any(|leg| leg.filled_volume > 0);
    let has_unfilled = legs.iter().any(|leg| {
        leg.volume_left > 0
            && !matches!(
                leg.state,
                ExecutionLegState::Rejected
                    | ExecutionLegState::Failed
                    | ExecutionLegState::Cancelled
            )
    });
    has_filled && has_unfilled
}

fn exposure_from_reports(legs: &[ExecutionLegReport]) -> ExecutionExposure {
    let filled_symbols = legs
        .iter()
        .filter(|leg| leg.filled_volume > 0)
        .map(|leg| leg.symbol.clone())
        .collect();
    let unfilled_symbols = legs
        .iter()
        .filter(|leg| leg.filled_volume < leg.requested_volume)
        .map(|leg| leg.symbol.clone())
        .collect();
    ExecutionExposure {
        filled_symbols,
        unfilled_symbols,
    }
}
