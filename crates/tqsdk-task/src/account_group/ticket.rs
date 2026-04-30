use std::time::Duration;

use tqsdk_core::StateReadView;
use tqsdk_wait::OrderTicket;

use crate::{Result, TaskHost, TaskOrderIntent};

use super::projection::{
    account_report_from_view, has_open_account_exposure, needs_attention_from_reports,
    outcome_from_reports,
};
use super::report::{
    AccountFailurePolicy, MultiAccountOrderGroupReport, MultiAccountOrderOutcome,
    MultiAccountOrderReport, MultiAccountOrderStatus,
};

/// Ticket returned after submitting or recovering a multi-account order.
#[derive(Debug, Clone)]
pub struct MultiAccountOrderTicket {
    pub(super) group_id: String,
    pub(super) max_unhedged: Option<Duration>,
    pub(super) failure_policy: AccountFailurePolicy,
    pub(super) orders: Vec<MultiAccountOrderLegTicket>,
}

/// Submitted or recovered ticket for one account allocation.
#[derive(Debug, Clone)]
pub struct MultiAccountOrderLegTicket {
    pub(super) account_id: String,
    pub(super) client_order_id: String,
    pub(super) intent: TaskOrderIntent,
    pub(super) ticket: OrderTicket,
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
