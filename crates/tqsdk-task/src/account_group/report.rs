use tqsdk_core::Revision;

/// Failure policy for a multi-account order when account outcomes diverge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountFailurePolicy {
    ReportExposure,
    FlattenFilledAccounts,
}

/// Revision-bound report for one multi-account order group.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiAccountOrderGroupReport {
    pub(super) revision: Revision,
    pub(super) group_id: String,
    pub(super) status: MultiAccountOrderStatus,
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
