# Project Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the review findings around task order idempotency with risk gates, history cache merge corruption handling, and high-complexity checker/parser code.

**Architecture:** Keep the existing crate boundaries: wait/session remain the owners of the order intent ledger, task remains the risk/task facade, and data owns offline history cache integrity. The fixes should be small and behavior-preserving except for rejecting corrupted cache merge inputs and allowing valid idempotent task retries. Refactors must not move public API ownership or change runtime commit semantics.

**Tech Stack:** Rust edition 2024, Cargo workspace, Tokio tests, existing `tqsdk-session` intent ledger, `tqsdk-task::RiskEngine`, `tqsdk-data::HistorySeriesCache`.

---

## File Structure

- Modify `crates/tqsdk-task/src/host.rs`
  - Add an internal `submit_task_order_intent_once` helper that validates intent fields, asks the wait/session intent ledger to reuse existing tickets first via `LimitOrderIntent::send_once`, and records risk only when the wait ticket reports a fresh submission.
  - Remove the second risk check from the prechecked path so batch preflight remains the single source of risk validation for a batch.
- Modify `crates/tqsdk-task/tests/risk_orders.rs`
  - Add tests proving repeated task order `send_once` with the same `client_order_id` bypasses already-consumed local daily/rate risk and does not dispatch again.
- Modify `crates/tqsdk-task/tests/execution_group.rs`
  - Add a group retry test with daily open count risk enabled.
- Modify `crates/tqsdk-task/tests/account_group.rs`
  - Add a multi-account retry test with daily open count risk enabled.
- Modify `crates/tqsdk-data/src/history_series_cache/storage.rs`
  - Make `MappedSeriesFile::write_rows_to` return an error when the requested row count exceeds the mapped file size.
- Modify `crates/tqsdk-data/src/history_series_cache.rs`
  - Validate each segment row count against its filename range before merge.
- Modify `crates/tqsdk-data/tests/history_series_cache.rs`
  - Add tests for corrupted adjacent segments during merge.
- Modify `crates/tqsdk-task/src/risk.rs`
  - Split `RiskEngine::check_report_from_views` into small private helpers without changing external behavior.
- Modify `crates/tqsdk-session/src/services.rs`
  - Split `query_symbol_ranking` and `query_edb_data` into request/parse/fill helpers without changing public signatures.

---

### Task 1: Preserve Task `send_once` Idempotency Under Risk

**Files:**
- Modify: `crates/tqsdk-task/src/host.rs`
- Test: `crates/tqsdk-task/tests/risk_orders.rs`
- Test: `crates/tqsdk-task/tests/execution_group.rs`
- Test: `crates/tqsdk-task/tests/account_group.rs`

- [ ] **Step 1: Add failing tests for single-order retries with consumed risk**

Append these tests to `crates/tqsdk-task/tests/risk_orders.rs` near the existing daily volume and rate-limit tests:

```rust
#[tokio::test(flavor = "current_thread")]
async fn task_order_retry_reuses_existing_intent_after_daily_open_volume_limit_is_consumed() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().daily_open_volume_limit(2, ["SHFE.rb2601"]));

    let first = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3_660.0)
        .send_once("risk-daily-volume-retry")
        .await
        .unwrap();
    assert!(first.was_submitted());
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    let retry = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3_660.0)
        .send_once("risk-daily-volume-retry")
        .await
        .unwrap();

    assert_eq!(retry.client_order_id(), "risk-daily-volume-retry");
    assert!(!retry.was_submitted());
    assert_eq!(retry.command_id(), first.command_id());
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task_order_retry_reuses_existing_intent_after_order_rate_limit_is_consumed() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().order_rate_limit_per_second(1, ["SHFE"]));

    let first = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-rate-retry")
        .await
        .unwrap();
    assert!(first.was_submitted());
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    let retry = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-rate-retry")
        .await
        .unwrap();

    assert_eq!(retry.client_order_id(), "risk-rate-retry");
    assert!(!retry.was_submitted());
    assert_eq!(retry.command_id(), first.command_id());
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders task_order_retry_reuses_existing_intent_after_daily_open_volume_limit_is_consumed -- --nocapture
cargo test -p tqsdk-task --test risk_orders task_order_retry_reuses_existing_intent_after_order_rate_limit_is_consumed -- --nocapture
```

Expected before the fix: both tests fail with `TaskError::RiskRejected(...)` on the retry.

- [ ] **Step 3: Add failing tests for batch/group retries with consumed risk**

Append this test to `crates/tqsdk-task/tests/execution_group.rs` near `execution_group_send_once_reuses_existing_leg_intents_on_retry`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn execution_group_retry_reuses_existing_leg_intents_after_daily_open_count_is_consumed() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().daily_open_count_limit(2, ["SHFE.au2602"]));

    let first = host
        .execution_group("sim")
        .client_group_id("spread-risk-retry-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();
    assert!(first.legs().iter().all(|leg| leg.ticket().was_submitted()));
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        2
    );

    let retry = host
        .execution_group("sim")
        .client_group_id("spread-risk-retry-001")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(retry.group_id(), "spread-risk-retry-001");
    assert!(retry.legs().iter().all(|leg| !leg.ticket().was_submitted()));
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
```

Append this test to `crates/tqsdk-task/tests/account_group.rs` near `multi_account_order_retry_reuses_existing_account_intents`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn multi_account_retry_reuses_existing_account_intents_after_daily_open_count_is_consumed() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().daily_open_count_limit(1, ["SHFE.au2602"]));
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let first = host
        .multi_account_order(accounts.clone())
        .client_group_id("alloc-risk-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();
    assert!(first.orders().iter().all(|order| order.ticket().was_submitted()));
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        2
    );

    let retry = host
        .multi_account_order(accounts)
        .client_group_id("alloc-risk-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();

    assert!(
        retry
            .orders()
            .iter()
            .all(|order| !order.ticket().was_submitted())
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
```

- [ ] **Step 4: Run the group/account failing tests**

Run:

```bash
cargo test -p tqsdk-task --test execution_group execution_group_retry_reuses_existing_leg_intents_after_daily_open_count_is_consumed -- --nocapture
cargo test -p tqsdk-task --test account_group multi_account_retry_reuses_existing_account_intents_after_daily_open_count_is_consumed -- --nocapture
```

Expected before the fix: both tests fail with `TaskError::RiskRejected(...)` during retry preflight.

- [ ] **Step 5: Refactor task order submission so risk is only consumed for new submissions**

In `crates/tqsdk-task/src/host.rs`, replace the relevant task-order methods with this shape. Keep the existing surrounding methods unchanged.

```rust
    pub(crate) async fn submit_task_order_once(
        &mut self,
        intent: TaskOrderIntent,
        client_order_id: ClientOrderId,
    ) -> Result<OrderTicket> {
        self.preflight_task_order(&intent)?;
        self.submit_prechecked_task_order_once(intent, client_order_id)
            .await
    }

    pub(crate) fn preflight_task_order(&self, intent: &TaskOrderIntent) -> Result<()> {
        self.preflight_task_orders(std::slice::from_ref(intent))
    }

    pub(crate) fn preflight_task_orders(&self, intents: &[TaskOrderIntent]) -> Result<()> {
        for intent in intents {
            validate_task_order_intent(intent)?;
            self.registry.with(|registry| {
                registry.check_manual_order_allowed(&intent.account_id, &intent.symbol)
            })?;
        }

        let Some(risk) = &self.risk else {
            return Ok(());
        };
        let mut risk = risk.clone();
        for intent in intents {
            match risk.check(&self.api, intent)? {
                RiskDecision::Accepted => risk.record_accepted_order(intent)?,
                RiskDecision::Rejected(rejection) => {
                    return Err(TaskError::RiskRejected(rejection));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn submit_prechecked_task_order_once(
        &mut self,
        intent: TaskOrderIntent,
        client_order_id: impl Into<ClientOrderId>,
    ) -> Result<OrderTicket> {
        self.submit_task_order_intent_once(intent, client_order_id.into())
            .await
    }

    async fn submit_task_order_intent_once(
        &mut self,
        intent: TaskOrderIntent,
        client_order_id: ClientOrderId,
    ) -> Result<OrderTicket> {
        validate_task_order_intent(&intent)?;
        let offset = intent.offset.ok_or(TaskError::Unsupported(
            "task orders require explicit offset",
        ))?;
        let limit_price = intent
            .limit_price
            .ok_or(TaskError::InvalidState("limit price is required"))?;

        let ticket: OrderTicket = self
            .api
            .limit_order(intent.account_id.clone(), intent.symbol.clone())
            .client_intent(client_order_id)
            .side(intent.direction, offset, intent.volume)
            .at(limit_price)
            .send_once()
            .await?;
        if ticket.was_submitted() {
            self.record_submitted_order(&intent)?;
        }
        Ok(ticket)
    }
```

Do not remove `check_risk`; the legacy guarded insert path still uses it.

- [ ] **Step 6: Run focused tests for the task fix**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders task_order_retry_reuses_existing_intent_after_daily_open_volume_limit_is_consumed -- --nocapture
cargo test -p tqsdk-task --test risk_orders task_order_retry_reuses_existing_intent_after_order_rate_limit_is_consumed -- --nocapture
cargo test -p tqsdk-task --test execution_group execution_group_retry_reuses_existing_leg_intents_after_daily_open_count_is_consumed -- --nocapture
cargo test -p tqsdk-task --test account_group multi_account_retry_reuses_existing_account_intents_after_daily_open_count_is_consumed -- --nocapture
```

Expected after the fix: all pass.

- [ ] **Step 7: Run regression tests for existing risk behavior**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders
cargo test -p tqsdk-task --test execution_group
cargo test -p tqsdk-task --test account_group
```

Expected: all pass. Existing rejection tests must still reject first-time orders that exceed risk limits.

- [ ] **Step 8: Commit task idempotency fix**

Run:

```bash
git add crates/tqsdk-task/src/host.rs crates/tqsdk-task/tests/risk_orders.rs crates/tqsdk-task/tests/execution_group.rs crates/tqsdk-task/tests/account_group.rs
git commit -m "fix(task): preserve send_once idempotency under risk gates"
```

---

### Task 2: Reject Corrupted History Cache Segments During Merge

**Files:**
- Modify: `crates/tqsdk-data/src/history_series_cache/storage.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Test: `crates/tqsdk-data/tests/history_series_cache.rs`

- [ ] **Step 1: Add failing merge corruption tests**

Append these tests to `crates/tqsdk-data/tests/history_series_cache.rs` near `corrupted_cache_file_returns_typed_error`:

```rust
#[test]
fn merge_adjacent_files_rejects_segment_shorter_than_filename_range() {
    let dir = temp_dir("merge-short-segment");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0), kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(3, 120_000_000_000, 3.0)],
        )
        .unwrap();
    std::fs::rename(
        dir.join("SHFE.au2602.60000000000.1.3"),
        dir.join("SHFE.au2602.60000000000.1.4"),
    )
    .unwrap();

    let err = cache
        .merge_adjacent_files("SHFE.au2602", 60_000_000_000)
        .unwrap_err();

    assert!(matches!(err, DataError::InvalidResponse(message)
        if message.contains("history series cache range does not match row count")));
    assert!(dir.join("SHFE.au2602.60000000000.1.4").exists());
    assert!(dir.join("SHFE.au2602.60000000000.3.4").exists());
    assert!(!dir.join("SHFE.au2602.60000000000.1.4.merge").exists());
}

#[test]
fn merge_adjacent_files_rejects_copy_count_larger_than_mapped_segment() {
    let dir = temp_dir("merge-copy-overflow");
    let cache = HistorySeriesCache::open(&dir).unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(1, 0, 1.0)],
        )
        .unwrap();
    cache
        .write_kline_segment(
            "SHFE.au2602",
            60_000_000_000,
            &[kline(2, 60_000_000_000, 2.0)],
        )
        .unwrap();
    std::fs::rename(
        dir.join("SHFE.au2602.60000000000.1.2"),
        dir.join("SHFE.au2602.60000000000.1.3"),
    )
    .unwrap();

    let err = cache
        .merge_adjacent_files("SHFE.au2602", 60_000_000_000)
        .unwrap_err();

    assert!(matches!(err, DataError::InvalidResponse(message)
        if message.contains("history series cache range does not match row count")
            || message.contains("history series merge requested more rows than segment contains")));
}
```

If the exact temp merge filename assertion is brittle because `merge_temp_path` includes a generated suffix, remove only the `*.merge` assertion and keep the source-file assertions.

- [ ] **Step 2: Run the failing data tests**

Run:

```bash
cargo test -p tqsdk-data --test history_series_cache merge_adjacent_files_rejects_segment_shorter_than_filename_range -- --nocapture
cargo test -p tqsdk-data --test history_series_cache merge_adjacent_files_rejects_copy_count_larger_than_mapped_segment -- --nocapture
```

Expected before the fix: at least one test fails because merge succeeds or produces the wrong error.

- [ ] **Step 3: Expose a row count accessor on mapped cache files**

In `crates/tqsdk-data/src/history_series_cache/storage.rs`, add this method inside `impl MappedSeriesFile`:

```rust
    #[must_use]
    pub(super) fn row_count(&self) -> usize {
        self.row_count
    }
```

- [ ] **Step 4: Make `write_rows_to` reject short mappings**

In `crates/tqsdk-data/src/history_series_cache/storage.rs`, replace `write_rows_to` with:

```rust
    pub(super) fn write_rows_to(&self, rows_to_copy: i64, writer: &mut impl Write) -> Result<()> {
        let rows_to_copy = usize::try_from(rows_to_copy.max(0)).map_err(|_| {
            DataError::InvalidResponse("history series merge row count overflow".to_string())
        })?;
        if rows_to_copy > self.row_count {
            return Err(DataError::InvalidResponse(
                "history series merge requested more rows than segment contains".to_string(),
            ));
        }
        let bytes_to_copy = rows_to_copy
            .checked_mul(self.layout.row_size())
            .ok_or_else(|| {
                DataError::InvalidResponse("history series merge byte count overflow".to_string())
            })?;
        if let Some(mmap) = &self.mmap {
            writer.write_all(&mmap[..bytes_to_copy])?;
        }
        Ok(())
    }
```

- [ ] **Step 5: Validate filename range against physical row count before merging**

In `crates/tqsdk-data/src/history_series_cache.rs`, inside `merge_adjacent_files_unlocked`, after opening `mapped` and before `mapped.write_rows_to`, add:

```rust
                    let expected_rows = usize::try_from(range.1 - range.0).map_err(|_| {
                        DataError::InvalidResponse(
                            "history series cache range row count overflow".to_string(),
                        )
                    })?;
                    if mapped.row_count() != expected_rows {
                        return Err(DataError::InvalidResponse(
                            "history series cache range does not match row count".to_string(),
                        ));
                    }
```

The loop should become:

```rust
                for (range, rows_to_copy) in &group {
                    let path = self.data_file_path(symbol, duration_ns, range.0, range.1);
                    let mapped = MappedSeriesFile::open(path, layout)?;
                    let expected_rows = usize::try_from(range.1 - range.0).map_err(|_| {
                        DataError::InvalidResponse(
                            "history series cache range row count overflow".to_string(),
                        )
                    })?;
                    if mapped.row_count() != expected_rows {
                        return Err(DataError::InvalidResponse(
                            "history series cache range does not match row count".to_string(),
                        ));
                    }
                    mapped.write_rows_to(*rows_to_copy, &mut writer)?;
                }
```

- [ ] **Step 6: Run focused and full data cache tests**

Run:

```bash
cargo test -p tqsdk-data --test history_series_cache merge_adjacent_files_rejects_segment_shorter_than_filename_range -- --nocapture
cargo test -p tqsdk-data --test history_series_cache merge_adjacent_files_rejects_copy_count_larger_than_mapped_segment -- --nocapture
cargo test -p tqsdk-data --test history_series_cache
```

Expected: all pass.

- [ ] **Step 7: Commit history cache merge fix**

Run:

```bash
git add crates/tqsdk-data/src/history_series_cache/storage.rs crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/tests/history_series_cache.rs
git commit -m "fix(data): reject corrupt history cache merge segments"
```

---

### Task 3: Split Risk Report Checks Into Private Helpers

**Files:**
- Modify: `crates/tqsdk-task/src/risk.rs`
- Test: existing `crates/tqsdk-task/tests/risk_orders.rs`

- [ ] **Step 1: Capture current behavior with full risk tests**

Run:

```bash
cargo test -p tqsdk-task --test risk_orders
```

Expected before refactor: pass.

- [ ] **Step 2: Extract small private helper methods without changing decisions**

In `crates/tqsdk-task/src/risk.rs`, keep the public `RiskEngine` API unchanged. Refactor `check_report_from_views` into a short coordinator that computes the shared values and calls private helpers named:

```rust
fn check_max_order_volume(&self, intent: &TaskOrderIntent) -> Option<RiskRejection>;
fn check_order_rate_limit(&self, account_id: &str, exchange_id: &str) -> Option<RiskRejection>;
fn check_daily_open_count_limit(&self, intent: &TaskOrderIntent) -> Option<RiskRejection>;
fn check_daily_open_volume_limit(&self, intent: &TaskOrderIntent) -> Option<RiskRejection>;
fn check_available_funds(&self, projected_notional: f64, account: Option<&Account>) -> Option<RiskRejection>;
fn check_projected_position(&self, projection: &OrderProjection) -> Option<RiskRejection>;
fn check_tick_alignment(&self, intent: &TaskOrderIntent, spec: Option<&InstrumentSpec>) -> Option<RiskRejection>;
fn check_price_deviation(&self, intent: &TaskOrderIntent, quote: Option<&Quote>) -> Option<RiskRejection>;
```

Use the exact existing `RiskRejection` variants and values from the current `check_report_from_views` body. Do not change report fields, revision selection, projection calculations, or the order in which checks are evaluated. If a helper needs data already computed in the coordinator, pass that value instead of recomputing it from the runtime snapshot.

- [ ] **Step 3: Run risk tests after each extraction batch**

After extracting max-volume/rate/daily checks, run:

```bash
cargo test -p tqsdk-task --test risk_orders
```

After extracting funds/position/tick/price checks, run:

```bash
cargo test -p tqsdk-task --test risk_orders
```

Expected: pass both times.

- [ ] **Step 4: Check formatting and linting for task crate**

Run:

```bash
cargo fmt --all --check
cargo clippy -p tqsdk-task --all-targets -- -D warnings
```

Expected: pass.

- [ ] **Step 5: Commit risk refactor**

Run:

```bash
git add crates/tqsdk-task/src/risk.rs
git commit -m "refactor(task): split risk report checks"
```

---

### Task 4: Split Session Service Request and Parsing Helpers

**Files:**
- Modify: `crates/tqsdk-session/src/services.rs`
- Test: `cargo test -p tqsdk-session`

- [ ] **Step 1: Capture current session service behavior**

Run:

```bash
cargo test -p tqsdk-session
cargo test -p tqsdk-session --no-default-features
```

Expected before refactor: pass.

- [ ] **Step 2: Extract helper functions for `query_symbol_ranking`**

In `crates/tqsdk-session/src/services.rs`, keep the public method signature:

```rust
pub async fn query_symbol_ranking(
    &self,
    request: SymbolRankingRequest,
) -> Result<SymbolRankingResponse>
```

Move pure logic into private helpers in the same file:

```rust
fn build_symbol_ranking_query(request: &SymbolRankingRequest) -> Result<Value>;
fn parse_symbol_ranking_response(value: Value, request: &SymbolRankingRequest) -> Result<SymbolRankingResponse>;
```

The async method should only validate/build, submit, and parse:

```rust
let payload = build_symbol_ranking_query(&request)?;
let value = self.query_graphql_value(payload).await?;
parse_symbol_ranking_response(value, &request)
```

Preserve all existing error messages and response ordering. If the current method does more than this shape, pass the minimal additional context into the helper rather than changing the public response.

- [ ] **Step 3: Run session tests for ranking extraction**

Run:

```bash
cargo test -p tqsdk-session
```

Expected: pass.

- [ ] **Step 4: Extract helper functions for `query_edb_data`**

In `crates/tqsdk-session/src/services.rs`, keep the public method signature:

```rust
pub async fn query_edb_data(&self, request: EdbDataRequest) -> Result<EdbDataResponse>
```

Move pure logic into private helpers in the same file:

```rust
fn build_edb_data_query(request: &EdbDataRequest) -> Result<Value>;
fn parse_edb_data_response(value: Value, request: &EdbDataRequest) -> Result<EdbDataResponse>;
fn fill_edb_data_calendar_gaps(response: EdbDataResponse, request: &EdbDataRequest) -> Result<EdbDataResponse>;
```

The async method should become a short coordinator:

```rust
let payload = build_edb_data_query(&request)?;
let value = self.query_graphql_value(payload).await?;
let response = parse_edb_data_response(value, &request)?;
fill_edb_data_calendar_gaps(response, &request)
```

Preserve current date parsing, fill-forward behavior, missing-value behavior, and error text.

- [ ] **Step 5: Run session tests and no-default tests**

Run:

```bash
cargo test -p tqsdk-session
cargo test -p tqsdk-session --no-default-features
cargo clippy -p tqsdk-session --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 6: Commit services refactor**

Run:

```bash
git add crates/tqsdk-session/src/services.rs
git commit -m "refactor(session): split service query parsing"
```

---

### Task 5: Workspace Verification and Documentation Decision

**Files:**
- No required code files.
- Modify docs only if implementation changes public API, feature flags, crate boundaries, runtime contract, or validation commands.

- [ ] **Step 1: Run standard workspace verification**

Run:

```bash
git diff --check
cargo fmt --all --check
cargo check --workspace --examples
cargo test --workspace
cargo clippy --workspace --examples --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Run feature matrix**

Run:

```bash
cargo check --workspace --no-default-features
cargo check --workspace --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --workspace --all-features --examples
```

Expected: all pass.

- [ ] **Step 3: Run release-level checks if this branch is intended for release validation**

Run:

```bash
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo deny check
cargo package --workspace --no-verify
```

Expected: all pass. Existing `cargo deny check` may emit warnings for unmatched license allowances and duplicated `windows-sys`; that is acceptable only if the command exits zero.

- [ ] **Step 4: Decide documentation impact**

If the implementation only fixes private behavior and private refactors:

```text
No architecture docs changed: crate boundaries, public API, feature flags, runtime contract, and validation commands are unchanged.
```

If any public API, feature flag, crate boundary, or validation command changed, update the exact affected docs before committing:

```bash
docs/architecture/ai-workflow.md
docs/architecture/README.md
docs/architecture/crate-boundaries.md
docs/architecture/validation.md
README.md
crates/<affected-crate>/README.md
```

- [ ] **Step 5: Commit docs only if needed**

If docs changed, run:

```bash
git add README.md docs/architecture/ai-workflow.md docs/architecture/README.md docs/architecture/crate-boundaries.md docs/architecture/validation.md crates/*/README.md
git commit -m "docs: align architecture notes with review fixes"
```

If no docs changed, do not create an empty commit.

---

## Self-Review

- Spec coverage: Task 1 covers the P1 task-risk idempotency bug. Task 2 covers the P2 history cache merge corruption bug. Tasks 3 and 4 cover the P3 maintainability findings. Task 5 covers validation and documentation decision.
- Placeholder scan: No step uses open-ended deferred-work markers; code snippets and commands are explicit.
- Type consistency: The plan uses existing types and methods from the repository: `TaskHost`, `RiskEngine`, `OrderTicket::was_submitted`, `OrderTicket::command_id`, `HistorySeriesCache`, `DataError`, and Cargo package names.
