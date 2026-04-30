#![cfg_attr(not(test), forbid(unsafe_code))]

mod allocation;
mod builder;
mod projection;
mod report;
mod submit;
mod ticket;

pub use allocation::{
    AccountAllocation, AccountAllocationPlan, AccountGroup, AccountGroupBuilder,
    AllocatedAccountOrder, Ratio,
};
pub use builder::{MultiAccountOrderBuilder, MultiAccountOrderDraft};
pub use report::{
    AccountFailurePolicy, MultiAccountOrderGroupReport, MultiAccountOrderOutcome,
    MultiAccountOrderReport, MultiAccountOrderState, MultiAccountOrderStatus,
};
pub use ticket::{MultiAccountOrderLegTicket, MultiAccountOrderTicket};
