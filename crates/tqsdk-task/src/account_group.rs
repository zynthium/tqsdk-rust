#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;

use crate::{Result, TaskError};

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
