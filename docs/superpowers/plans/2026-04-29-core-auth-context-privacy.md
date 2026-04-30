# Core AuthContext Privacy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Privatize `tqsdk_core::AuthContext` fields while preserving the existing constructor and accessor API.

**Architecture:** This is a focused source-breaking `tqsdk-core` public API narrowing batch. `AuthContext` remains the core auth/session contract returned by `AuthProvider`; only direct field construction and direct field reads are removed. Do not change runtime state commits, auth refresh semantics, session ownership, or any facade boundary in this plan.

**Tech Stack:** Rust, Cargo integration tests, existing architecture docs under `docs/architecture`.

---

## Files

- Modify: `crates/tqsdk-core/src/auth.rs`
- Create: `crates/tqsdk-core/tests/runtime_contract_auth_context.rs`
- Modify: `docs/architecture/runtime-core/session-auth.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Update planning artifact if present in the main planning workspace: `docs/public-api-disposition-matrix.md`
- Do not modify in this focused plan: `crates/tqsdk-core/src/lib.rs`
- Do not modify in this focused plan: `crates/tqsdk-core/src/commands.rs`

## Deferred Out Of This Child Plan

- Do not internalize `AggregatedCommit`, `AggregatedCursor`, `AggregatedRuntimeReader`, `AggregatedSnapshotReadGuard`, `StateSourceId`, or `OutboundEnvelope` here. Those belong in a separate core surface narrowing child plan because `RuntimeHandle::drain_outbound()` currently exposes `OutboundEnvelope`.
- Do not restructure `TradePreInsertOrderCommand` here. The disposition matrix marks it `split-plan`, so it needs a separate compatibility plan before any code change.

## Task 1: Characterize Current AuthContext Contract

- [x] **Step 1: Confirm direct field usage is not required by workspace code**

Run:

```bash
rg -n "AuthContext \{" crates docs README.md
rg -n "\.(access_token|auth_id|features)[[:space:]]*([,.;})]|$)" crates docs README.md
```

Expected:

```text
No production or test code outside `crates/tqsdk-core/src/auth.rs` should require direct `AuthContext` struct literals or direct field reads; callers should already use `new`, `access_token()`, `auth_id()`, `features()`, `with_auth_id`, or `with_feature`.
```

- [x] **Step 2: Run the focused auth/session contract baseline**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_session -- --nocapture
cargo test -p tqsdk-session auth -- --nocapture
```

Expected:

```text
The existing auth/session tests pass before field privacy changes.
```

## Task 2: Add An Explicit AuthContext Accessor Contract Test

- [x] **Step 1: Add compile-fail documentation to `crates/tqsdk-core/src/auth.rs`**

Add this doc comment immediately above `pub struct AuthContext`:

```rust
/// Authentication result returned by [`AuthProvider`].
///
/// Use the constructor and accessor methods as the public contract:
///
/// ```
/// # use tqsdk_core::{AuthContext, AuthId};
/// let auth = AuthContext::new("access-token").with_auth_id(AuthId::new("auth-1"));
/// assert_eq!(auth.access_token(), "access-token");
/// assert_eq!(auth.auth_id().map(AuthId::as_str), Some("auth-1"));
/// ```
///
/// Direct field construction is not part of the public contract:
///
/// ```compile_fail
/// # use tqsdk_core::AuthContext;
/// let _auth = AuthContext {
///     access_token: String::from("access-token"),
///     auth_id: None,
///     features: Vec::new(),
/// };
/// ```
```

- [x] **Step 2: Run doctests and verify the compile-fail test fails before implementation**

Run:

```bash
cargo test -p tqsdk-core --doc
```

Expected before implementation:

```text
The new `compile_fail` doctest fails because direct field construction still compiles while the fields are public.
```

- [x] **Step 3: Create `crates/tqsdk-core/tests/runtime_contract_auth_context.rs`**

Add:

```rust
use tqsdk_core::{AuthContext, AuthId};

#[test]
fn auth_context_constructor_and_accessors_are_the_public_contract() {
    let auth = AuthContext::new("access-token")
        .with_auth_id(AuthId::new("auth-1"))
        .with_feature("trade")
        .with_feature("query");

    assert_eq!(auth.access_token(), "access-token");
    assert_eq!(auth.auth_id().map(AuthId::as_str), Some("auth-1"));
    assert_eq!(auth.features(), &["trade".to_string(), "query".to_string()]);

    let debug = format!("{auth:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("access-token"));
}
```

- [x] **Step 4: Run the new integration test before implementation**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_auth_context
```

Expected:

```text
The test passes before implementation because it documents the accessor API that must remain stable after fields become private.
```

## Task 3: Privatize AuthContext Fields

- [x] **Step 1: Replace the struct fields in `crates/tqsdk-core/src/auth.rs`**

Change:

```rust
#[derive(Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub access_token: String,
    pub auth_id: Option<AuthId>,
    pub features: Vec<String>,
}
```

To:

```rust
#[derive(Clone, PartialEq, Eq)]
pub struct AuthContext {
    access_token: String,
    auth_id: Option<AuthId>,
    features: Vec<String>,
}
```

- [x] **Step 2: Keep the public constructor and accessors unchanged**

Verify this impl block still contains exactly these public methods:

```rust
impl AuthContext {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            auth_id: None,
            features: Vec::new(),
        }
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn auth_id(&self) -> Option<&AuthId> {
        self.auth_id.as_ref()
    }

    pub fn features(&self) -> &[String] {
        &self.features
    }

    pub fn with_auth_id(mut self, auth_id: AuthId) -> Self {
        self.auth_id = Some(auth_id);
        self
    }

    pub fn with_feature(mut self, feature: impl Into<String>) -> Self {
        self.features.push(feature.into());
        self
    }
}
```

- [x] **Step 3: Run focused compile and test checks**

Run:

```bash
cargo test -p tqsdk-core --doc
cargo test -p tqsdk-core --test runtime_contract_auth_context
cargo test -p tqsdk-core --test runtime_contract_session -- --nocapture
cargo test -p tqsdk-session auth -- --nocapture
```

Expected:

```text
The doctest now passes because direct field construction no longer compiles; all focused auth/session checks pass without adding new field setters or direct mutable accessors.
```

## Task 4: Sync AuthContext Documentation

- [x] **Step 1: Update `docs/architecture/runtime-core/session-auth.md`**

Replace the `AuthContext` snippet with:

```rust
pub struct AuthContext { /* fields private */ }

impl AuthContext {
    pub fn new(access_token: impl Into<String>) -> Self;
    pub fn access_token(&self) -> &str;
    pub fn auth_id(&self) -> Option<&AuthId>;
    pub fn features(&self) -> &[String];
    pub fn with_auth_id(self, auth_id: AuthId) -> Self;
    pub fn with_feature(self, feature: impl Into<String>) -> Self;
}
```

Keep the surrounding constraints unchanged: auth results must still enter runtime state and remain observable through `RuntimeReader`.

- [x] **Step 2: Update the planning copy of `docs/public-api-disposition-matrix.md`**

If `docs/public-api-disposition-matrix.md` exists in the main planning workspace, update the `AuthContext public fields` row disposition to `internalize` and replace the follow-up text with:

```text
Direct fields are removed from the public contract by `docs/superpowers/plans/2026-04-29-core-auth-context-privacy.md`; constructor and accessor APIs remain public.
```

Do not create `docs/public-api-disposition-matrix.md` inside `.worktrees/audit-guardrails` if it is absent there; that would mix a large untracked audit artifact into this focused code commit. Do not reclassify unrelated `tqsdk-core` rows in this plan.

## Task 5: Verify Core And Workspace Compatibility

- [x] **Step 1: Run the parent Task 6 core contract commands**

Run:

```bash
cargo test -p tqsdk-core -q --test runtime_contract_v1_capability
cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface
cargo test -p tqsdk-core
cargo check --workspace --examples
```

Expected:

```text
Core contract tests and workspace examples pass after the source-breaking field privacy change.
```

- [x] **Step 2: Run the dependent session crate tests**

Run:

```bash
cargo test -p tqsdk-session
```

Expected:

```text
The session crate still compiles and passes using only `AuthContext` constructor/accessor APIs.
```

## Task 6: Commit

- [x] **Step 1: Stage the focused AuthContext privacy batch**

Run:

```bash
git add crates/tqsdk-core/src/auth.rs crates/tqsdk-core/tests/runtime_contract_auth_context.rs docs/architecture/runtime-core/session-auth.md
```

- [x] **Step 2: Commit**

Run:

```bash
git commit -m "refactor(core): privatize auth context fields"
```

Expected:

```text
A focused commit exists for `AuthContext` field privacy only. No aggregation, outbound envelope, or trade command shape changes are included.
```

Output:

- RED verified before implementation: `cargo test -p tqsdk-core --doc` failed because the new `compile_fail` doctest compiled successfully while fields were still public.
- GREEN verification before commit:
  - `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
  - `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
  - `cargo test -p tqsdk-core`
  - `cargo check --workspace --examples`
  - `cargo test -p tqsdk-session`
- Committed in worktree `.worktrees/audit-guardrails` as `418f7ee refactor(core): privatize auth context fields`.
