//! Contract: durable authority installs account policy and admits stable client
//! order id before caller-owned network submission begins.

use std::{error::Error, path::PathBuf};

use tqsdk_hard_risk::{
    HardRiskAuthority, HardRiskDirection, HardRiskOffset, HardRiskOrderRequest, HardRiskPolicy,
    HardRiskPolicyExpectation, HardRiskPrepareOutcome, HardRiskRuntimeReceipt, HardRiskScope,
    HardRiskSubmissionOutcome,
};

fn main() -> Result<(), Box<dyn Error>> {
    let database_path = std::env::var_os("TQ_HARD_RISK_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("tqsdk-hard-risk-contract.sqlite"));
    let authority = HardRiskAuthority::open(database_path)?;
    let request = HardRiskOrderRequest::new(
        HardRiskScope::new("live:user-7:route-a", "TQSIM", "2026-09-08"),
        "stable-client-order-001",
        "SHFE.au2602",
        HardRiskDirection::Buy,
        HardRiskOffset::Open,
        1,
    );
    let policy = HardRiskPolicy::new("desk-hard-limit", "2026-09-08.1")
        .max_order_attempts(100)
        .max_open_volume(20);
    let policy_scope = request.scope().policy_scope();
    let expectation = authority
        .active_policy(&policy_scope)?
        .map(|current| HardRiskPolicyExpectation::Current(current.revision().clone()))
        .unwrap_or(HardRiskPolicyExpectation::Absent);
    authority.install_policy(policy_scope, policy, expectation)?;

    match authority.prepare(request)? {
        HardRiskPrepareOutcome::Prepared(record) => {
            println!(
                "prepared durable client id={}",
                record.request().client_order_id()
            );
            match authority.begin_submission(&record.identity(), "contract-process")? {
                HardRiskSubmissionOutcome::Granted(permit) => {
                    // Production adapter sends only `admitted`, including its
                    // stable client id. The non-clone permit is consumed once.
                    let outcome = authority.submit_once(*permit, |admitted| {
                        Ok::<_, std::convert::Infallible>(HardRiskRuntimeReceipt::for_request(
                            admitted,
                            "contract-runtime",
                            "contract-command-001",
                        ))
                    })?;
                    println!("submission durable state={:?}", outcome.record().state());
                }
                HardRiskSubmissionOutcome::Existing(existing) => {
                    println!("submission already durable state={:?}", existing.state());
                }
            }
        }
        HardRiskPrepareOutcome::Existing(record) => {
            println!("existing durable state={:?}", record.state());
        }
    }
    Ok(())
}
