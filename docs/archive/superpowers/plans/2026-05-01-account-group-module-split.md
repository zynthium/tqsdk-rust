# Account Group Module Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `crates/tqsdk-task/src/account_group.rs` into focused internal modules without changing the public multi-account task API.

**Architecture:** This is a source-compatible internal refactor of the `tqsdk-task` account-group foundation. `crate::account_group::*` remains the module surface consumed by `crates/tqsdk-task/src/lib.rs`, `TaskHost`, docs, examples, and tests. The split keeps ratio allocation, order builder/draft, public report/outcome types, submitted ticket lifecycle, submit/preflight flow, and projection/outcome helpers separate while preserving the same wait-layer `OrderTicket`, session intent ledger, all-account preflight, and `NeedsAttention` semantics.

**Tech Stack:** Rust modules, `tqsdk-wait` order tickets, task-layer risk/preflight helpers, existing `account_group` integration tests, source-level guardrail test, `cargo check/test/clippy`.

---

## Scope

In scope:

- Keep `crates/tqsdk-task/src/account_group.rs` as the module root.
- Create child modules under `crates/tqsdk-task/src/account_group/`.
- Move existing definitions without changing public type names, method names, return types, or root crate re-exports.
- Add a source-level guardrail test to keep the file from regressing to a large mixed-responsibility module.
- Update review/plan documents after verification.

Out of scope:

- Changing multi-account order behavior.
- Implementing `AccountFailurePolicy::FlattenFilledAccounts`.
- Changing allocation rounding semantics.
- Changing `TaskHost::account_group()` or `TaskHost::multi_account_order()` signatures.
- Adding account-group resume/audit or automatic hedge/flatten policy.
- Changing `docs/architecture/api-task.md` or `crates/tqsdk-task/README.md`, because this refactor does not change the architecture contract.

## File Structure

- Modify: `crates/tqsdk-task/src/account_group.rs`
  - Root module only.
  - Declares child modules and re-exports the same public types currently exported from `crate::account_group`.
- Create: `crates/tqsdk-task/src/account_group/allocation.rs`
  - `Ratio`
  - `AccountAllocation`
  - `AccountGroup`
  - `AccountGroupBuilder`
  - `AllocatedAccountOrder`
  - `AccountAllocationPlan`
  - allocation and validation methods
- Create: `crates/tqsdk-task/src/account_group/report.rs`
  - `AccountFailurePolicy`
  - `MultiAccountOrderState`
  - `MultiAccountOrderReport`
  - `MultiAccountOrderStatus`
  - `MultiAccountOrderOutcome`
  - `MultiAccountOrderGroupReport`
- Create: `crates/tqsdk-task/src/account_group/builder.rs`
  - `MultiAccountOrderBuilder`
  - `MultiAccountOrderDraft`
  - builder/draft methods
- Create: `crates/tqsdk-task/src/account_group/ticket.rs`
  - `MultiAccountOrderTicket`
  - `MultiAccountOrderLegTicket`
  - status/report/outcome/wait helpers
- Create: `crates/tqsdk-task/src/account_group/submit.rs`
  - `submit_multi_account_order`
- Create: `crates/tqsdk-task/src/account_group/projection.rs`
  - `account_report_from_view`
  - `outcome_from_reports`
  - `needs_attention_from_reports`
  - `has_open_account_exposure`
  - ticket/order state projection helpers
- Modify: `crates/tqsdk-task/tests/account_group.rs`
  - Add source-level module split guardrail.
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
  - Mark the `account_group.rs` module-directory split complete after verification.
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - Close the remaining module split item after verification.
- Modify: `docs/superpowers/plans/2026-05-01-account-group-module-split.md`
  - Check off executed steps and record verification.

## Task 1: Add Account Group Split Guardrail Test

**Files:**
- Modify: `crates/tqsdk-task/tests/account_group.rs`

- [x] **Step 1: Write the failing structure test**

Add this test near the top-level helper functions:

```rust
#[test]
fn account_group_is_split_into_focused_modules() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let account_group_root = root.join("src/account_group.rs");
    let account_group_dir = root.join("src/account_group");

    for module in [
        "allocation.rs",
        "report.rs",
        "builder.rs",
        "ticket.rs",
        "submit.rs",
        "projection.rs",
    ] {
        assert!(
            account_group_dir.join(module).exists(),
            "account_group module {module} should exist under src/account_group/"
        );
    }

    let source =
        std::fs::read_to_string(&account_group_root).expect("account_group root should be readable");
    for module_decl in [
        "mod allocation;",
        "mod builder;",
        "mod projection;",
        "mod report;",
        "mod submit;",
        "mod ticket;",
    ] {
        assert!(
            source.contains(module_decl),
            "account_group root should declare {module_decl}"
        );
    }

    assert!(
        !source.contains("pub struct AccountGroupBuilder"),
        "allocation builder should live in src/account_group/allocation.rs"
    );
    assert!(
        !source.contains("async fn submit_multi_account_order"),
        "submit flow should live in src/account_group/submit.rs"
    );
    assert!(
        !source.contains("fn account_report_from_view"),
        "projection helpers should live in src/account_group/projection.rs"
    );
}
```

- [x] **Step 2: Run the structure test and verify RED**

Run:

```bash
cargo test -p tqsdk-task --test account_group account_group_is_split_into_focused_modules
```

Expected before implementation:

```text
FAILED account_group_is_split_into_focused_modules
```

The failure should report at least one missing module under `src/account_group/`.

Observed RED: failed because `src/account_group/allocation.rs` did not exist.

## Task 2: Create Module Root and Re-exports

**Files:**
- Modify: `crates/tqsdk-task/src/account_group.rs`
- Create: `crates/tqsdk-task/src/account_group/allocation.rs`
- Create: `crates/tqsdk-task/src/account_group/report.rs`
- Create: `crates/tqsdk-task/src/account_group/builder.rs`
- Create: `crates/tqsdk-task/src/account_group/ticket.rs`
- Create: `crates/tqsdk-task/src/account_group/submit.rs`
- Create: `crates/tqsdk-task/src/account_group/projection.rs`

- [x] **Step 1: Replace root file with module declarations and re-exports**

After moving the definitions in Tasks 3-6, `crates/tqsdk-task/src/account_group.rs` should contain:

```rust
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
```

- [x] **Step 2: Keep crate-level public exports unchanged**

Do not edit the `pub use account_group::{ ... }` block in `crates/tqsdk-task/src/lib.rs` except if rustfmt reflows it.

Run:

```bash
cargo check -p tqsdk-task
```

Expected after Tasks 3-6 complete:

```text
Finished `dev` profile ...
```

## Task 3: Move Allocation and Report Types

**Files:**
- Create: `crates/tqsdk-task/src/account_group/allocation.rs`
- Create: `crates/tqsdk-task/src/account_group/report.rs`
- Modify: `crates/tqsdk-task/src/account_group.rs`

- [x] **Step 1: Move allocation definitions**

Move these existing definitions from `account_group.rs` into `account_group/allocation.rs`:

- `Ratio`
- `AccountAllocation`
- `AccountGroup`
- `AccountGroupBuilder`
- `AllocatedAccountOrder`
- `AccountAllocationPlan`
- impl blocks for those types

Required imports in `allocation.rs`:

```rust
use std::collections::HashSet;

use crate::{Result, TaskError};
```

Fields that remain externally private may stay private inside `allocation.rs`.

- [x] **Step 2: Move report and outcome definitions**

Move these existing definitions from `account_group.rs` into `account_group/report.rs`:

- `AccountFailurePolicy`
- `MultiAccountOrderGroupReport`
- `MultiAccountOrderState`
- `MultiAccountOrderReport`
- `MultiAccountOrderStatus`
- `MultiAccountOrderOutcome`
- impl blocks for `MultiAccountOrderGroupReport` and `MultiAccountOrderOutcome`

Required imports in `report.rs`:

```rust
use tqsdk_core::Revision;
```

Make `MultiAccountOrderGroupReport` fields `pub(super)` so `ticket.rs` can construct reports without exposing fields publicly.

## Task 4: Move Builder, Ticket, and Submit Flow

**Files:**
- Create: `crates/tqsdk-task/src/account_group/builder.rs`
- Create: `crates/tqsdk-task/src/account_group/ticket.rs`
- Create: `crates/tqsdk-task/src/account_group/submit.rs`
- Modify: `crates/tqsdk-task/src/account_group.rs`

- [x] **Step 1: Move multi-account builder and draft**

Move these existing definitions from `account_group.rs` into `account_group/builder.rs`:

- `MultiAccountOrderBuilder`
- `MultiAccountOrderDraft`
- impl blocks for `MultiAccountOrderBuilder`
- impl block for `MultiAccountOrderDraft`

Required imports in `builder.rs`:

```rust
use std::time::Duration;

use tqsdk_core::{TradeDirection, TradeOffset};

use crate::{Result, TaskHost};

use super::allocation::AccountGroup;
use super::report::AccountFailurePolicy;
use super::submit::submit_multi_account_order;
use super::ticket::MultiAccountOrderTicket;
```

Use `pub(super)` fields on `MultiAccountOrderBuilder` and `MultiAccountOrderDraft` so `submit.rs` can destructure them without exposing fields publicly.

- [x] **Step 2: Move ticket lifecycle methods**

Move these existing definitions from `account_group.rs` into `account_group/ticket.rs`:

- `MultiAccountOrderTicket`
- `MultiAccountOrderLegTicket`
- impl blocks for both ticket types
- private `earlier_deadline`

Required imports in `ticket.rs`:

```rust
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
```

Use `pub(super)` fields on ticket structs for `submit.rs` and `projection.rs`.

- [x] **Step 3: Move submit flow**

Move `submit_multi_account_order` into `account_group/submit.rs`.

Required imports in `submit.rs`:

```rust
use crate::{Result, TaskError, TaskOrderIntent};

use super::builder::{MultiAccountOrderBuilder, MultiAccountOrderDraft};
use super::report::AccountFailurePolicy;
use super::ticket::{MultiAccountOrderLegTicket, MultiAccountOrderTicket};
```

Keep `submit_multi_account_order` as:

```rust
pub(super) async fn submit_multi_account_order(
    draft: MultiAccountOrderDraft<'_>,
) -> Result<MultiAccountOrderTicket> {
    /* move the existing function body unchanged */
}
```

## Task 5: Move Projection and Outcome Helpers

**Files:**
- Create: `crates/tqsdk-task/src/account_group/projection.rs`
- Modify: `crates/tqsdk-task/src/account_group.rs`

- [x] **Step 1: Move projection helpers**

Move these existing helper functions from `account_group.rs` into `account_group/projection.rs`:

- `account_report_from_view`
- `ticket_state_from_view`
- `command_status_from_view`
- `ticket_state_from_order`
- `ticket_state_from_command`
- `live_account_order_state`
- `terminal_optional_order_state`
- `outcome_from_reports`
- `needs_attention_from_reports`
- `has_open_account_exposure`
- `is_pending_state`

Required imports in `projection.rs`:

```rust
use tqsdk_core::{
    AccountId, CommandId, CommandStatus, Order, OrderId, OrderLifecycle, StateReadView,
};
use tqsdk_wait::OrderTicketState;

use crate::{Result, TaskError};

use super::report::{MultiAccountOrderOutcome, MultiAccountOrderReport, MultiAccountOrderState};
use super::ticket::MultiAccountOrderLegTicket;
```

Export the helpers needed by `ticket.rs` as `pub(super)`.

## Task 6: Verify Behavior and Docs

**Files:**
- Modify: `crates/tqsdk-task/tests/account_group.rs`
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/superpowers/plans/2026-05-01-account-group-module-split.md`

- [x] **Step 1: Run focused account-group checks**

Run:

```bash
cargo test -p tqsdk-task --test account_group
cargo check -p tqsdk-task
```

Expected:

```text
test result: ok
Finished `dev` profile ...
```

Observed focused verification:

```text
cargo test -p tqsdk-task --test account_group
cargo check -p tqsdk-task
```

- [x] **Step 2: Update review and umbrella plan docs**

In `docs/reviews/comprehensive-review-2026-04-30.md`:

- Add `account_group.rs` to the completed summary as split into `account_group/` modules.
- Change the `account_group.rs` maintainability table row to mention completion.
- Remove the remaining independent module-split item.

In `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`:

- Add this child plan to the 2026-05-01 continuation completed list after verification.
- Change the remaining module split sentence to state that no module-directory split items from this review remain.

- [x] **Step 3: Run full verification**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --examples
git diff --check
```

Expected:

```text
Finished `dev` profile ...
test result: ok
```

Observed full verification:

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --examples
git diff --check
```

- [x] **Step 4: Commit**

Run:

```bash
git add crates/tqsdk-task/src/account_group.rs crates/tqsdk-task/src/account_group crates/tqsdk-task/tests/account_group.rs docs/reviews/comprehensive-review-2026-04-30.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-05-01-account-group-module-split.md
git commit -m "refactor: split task account group modules"
```

Expected:

```text
[main <sha>] refactor: split task account group modules
```

## Self-Review

- Spec coverage: Covers the final remaining comprehensive-review module split item and keeps the public `AccountGroup` / `MultiAccountOrder*` API intact.
- Placeholder scan: No `TBD`, `TODO`, or vague implementation placeholders are present; each task names exact files, moved definitions, and commands.
- Type consistency: Module imports match the existing type names in `account_group.rs`; public re-export names match `crates/tqsdk-task/src/lib.rs`.
