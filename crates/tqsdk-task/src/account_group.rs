#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;
use std::time::Duration;

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::OrderTicket;

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

/// Submitted or recovered ticket for one account allocation.
#[derive(Debug, Clone)]
pub struct MultiAccountOrderLegTicket {
    account_id: String,
    client_order_id: String,
    intent: TaskOrderIntent,
    ticket: OrderTicket,
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

        let mut allocated: i64 = rows.iter().map(|(_, volume, _)| *volume).sum();
        rows.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
        for row in &mut rows {
            if allocated >= total_volume {
                break;
            }
            row.1 += 1;
            allocated += 1;
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
    for intent in &intents {
        host.preflight_task_order(intent)?;
    }

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
