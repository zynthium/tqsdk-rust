#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::OrderTicket;

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
