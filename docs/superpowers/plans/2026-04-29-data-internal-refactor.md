# Data Internal Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split high-churn `tqsdk-data` history and download internals into focused private modules while preserving all existing public data APIs.

**Architecture:** Keep `tqsdk-data` as the research/offline data layer. Do not move `query_his_cont_quotes`, `query_option_greeks`, chart reads, or download permission checks into `tqsdk-session`, `tqsdk-wait`, or `tqsdk-core`. Public exports from `crates/tqsdk-data/src/lib.rs` must remain source-compatible.

**Tech Stack:** Rust private submodules under `client/` and `download/`, existing `tqsdk-data` unit/integration tests, workspace examples.

---

## Files

- Modify: `crates/tqsdk-data/src/client.rs`
- Create: `crates/tqsdk-data/src/client/page.rs`
- Create: `crates/tqsdk-data/src/client/chart_reader.rs`
- Create: `crates/tqsdk-data/src/client/cont_quotes.rs`
- Create: `crates/tqsdk-data/src/client/permissions.rs`
- Modify: `crates/tqsdk-data/src/download.rs`
- Create: `crates/tqsdk-data/src/download/page.rs`
- Create: `crates/tqsdk-data/src/download/inner.rs`
- Modify: `crates/tqsdk-data/src/lib.rs` only if re-export paths cannot stay through `client` / `download`

## Task 1: Characterize Existing Data Behavior

- [x] Run `cargo test -p tqsdk-data query_his_cont_quotes_returns_last_n_trading_days_with_fill_forward`.
- [x] Run `cargo test -p tqsdk-data get_kline_data_page_returns_ready_rows_within_chart_bounds`.
- [x] Run `cargo test -p tqsdk-data get_tick_data_page_returns_ready_rows_within_chart_bounds`.
- [x] Run `cargo test -p tqsdk-data kline_data_download_skips_empty_leading_pages_and_reports_progress`.
- [x] Run `cargo test -p tqsdk-data tick_data_download_marks_last_emitted_page_complete`.
- [x] Run `cargo test -p tqsdk-data kline_data_download_requires_tq_dl_when_auth_context_is_known`.

Expected: all focused tests pass before moving code.

## Task 2: Split Page Request And Page Types

- [x] Add `mod page;` near the top of `crates/tqsdk-data/src/client.rs`.
- [x] Move these public types from `client.rs` into `client/page.rs`:
  - `KlineDataPageRequest`
  - `KlineDataPage`
  - `TickDataPageRequest`
  - `TickDataPage`
  - `KlineDataSeriesRequest`
  - `KlineDataSeries`
  - `TickDataSeriesRequest`
  - `TickDataSeries`
- [x] Move these private specs into `client/page.rs`:
  - `KlineDataPageSpec`
  - `TickDataPageSpec`
  - any `validate()` helpers that only serve page or series request validation
- [x] Re-export public page and series types from `client.rs` with:

```rust
pub use page::{
    KlineDataPage, KlineDataPageRequest, KlineDataSeries, KlineDataSeriesRequest, TickDataPage,
    TickDataPageRequest, TickDataSeries, TickDataSeriesRequest,
};
```

- [x] Keep `crates/tqsdk-data/src/lib.rs` exports unchanged unless the compiler requires a path-only fix.

## Task 3: Split Ready Chart Reading

- [x] Add `mod chart_reader;` to `client.rs`.
- [x] Move these helpers from `client.rs` into `client/chart_reader.rs`:
  - `wait_for_ready_chart`
  - `chart_is_ready`
  - `read_ready_kline_data_page`
  - `read_ready_tick_data_page`
  - `next_kline_page_chart_id`
  - `next_tick_page_chart_id`
  - `cancel_chart_best_effort`
- [x] Keep `DataClient::get_kline_data_page()` and `DataClient::get_tick_data_page()` in `client.rs`, but call the moved helpers through `chart_reader::`.
- [x] Preserve current timeout behavior and the accepted `more_data == true` ready chart behavior.

## Task 4: Split Continuous Quote And Trading-Day Logic

- [x] Add `mod cont_quotes;` to `client.rs`.
- [x] Move `HistoricalContQuotesRow` into `client/cont_quotes.rs` and re-export it from `client.rs`.
- [x] Move `DataClient::query_his_cont_quotes()`, `DataClient::trading_days()`, `DataClient::fetch_continuous_updates()`, and continuous-table parsing helpers into `client/cont_quotes.rs`.
- [x] Keep `DataClient::fetch_json()` in `client.rs` so the module can reuse the existing services/live feature split without duplicating HTTP code.
- [x] Preserve validation error strings for empty symbols, zero days, invalid continuous-contract code, missing continuous table entry, and trading calendar range errors.

## Task 5: Deduplicate History Download Permission Checks

- [x] Add `mod permissions;` to `client.rs`.
- [x] Move `HISTORY_DOWNLOAD_PERMISSION_MESSAGE` and common feature inspection into `client/permissions.rs`.
- [x] Implement a private helper in `client/permissions.rs`:

```rust
pub(super) fn has_tq_dl_feature(auth_context: &serde_json::Value) -> Option<bool> {
    let features = auth_context.get("features").and_then(serde_json::Value::as_array)?;
    Some(
        features
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|feature| feature == "tq_dl"),
    )
}
```

- [x] Keep both call sites source-compatible:
  - `DataClient::require_history_download_permission(&self) -> Result<()>`
  - `DataClient::require_history_download_permission_async(&self, session: &tqsdk_session::SessionClient) -> Result<()>`
- [x] Preserve the current async fallback: `SessionFacadeError::InvalidState(_)` still means permission check passes.

## Task 6: Split Download Page Types

- [x] Add `mod page;` to `download.rs`.
- [x] Move these public types from `download.rs` into `download/page.rs`:
  - `DataDownloadProgress`
  - `KlineDataDownloadPage`
  - `TickDataDownloadPage`
- [x] Re-export public download page types from `download.rs` with:

```rust
pub use page::{DataDownloadProgress, KlineDataDownloadPage, TickDataDownloadPage};
```

- [x] Keep `crates/tqsdk-data/src/lib.rs` exports unchanged.

## Task 7: Internalize Download Inner Machinery

- [x] Add `mod inner;` to `download.rs`.
- [x] Move these private items from `download.rs` into `download/inner.rs`:
  - `DataClientKlinePageSource`
  - `DataClientTickPageSource`
  - `KlineDataDownloadInner`
  - `TickDataDownloadInner`
  - source traits and fake test sources currently used only by download tests
- [x] Share duplicated completion/progress calculation through private helper functions in `download/inner.rs`; keep public wrapper names unchanged:
  - `KlineDataDownload`
  - `TickDataDownload`
  - `KlineDataDownloadPage`
  - `TickDataDownloadPage`
- [x] Keep `DataClient::kline_data_download()` and `DataClient::tick_data_download()` public behavior unchanged, including session-backed requirement and synchronous permission check.

## Task 8: Verify Data Compatibility

- [x] Run `cargo test -p tqsdk-data --test market_cache`.
- [x] Run `cargo test -p tqsdk-data`.
- [x] Run `cargo check -p tqsdk-data --examples`.
- [x] Run `cargo check --workspace --examples`.

Expected: data crate tests and examples pass with unchanged public usage.

## Task 9: Commit

- [x] Run `git add crates/tqsdk-data/src/client.rs crates/tqsdk-data/src/client crates/tqsdk-data/src/download.rs crates/tqsdk-data/src/download crates/tqsdk-data/src/lib.rs`.
- [x] Run `git commit -m "refactor(data): split history and download internals"`.

Output:

- Verified before commit:
  - `cargo test -p tqsdk-data`
  - `cargo check -p tqsdk-data --examples`
  - `cargo check --workspace --examples`
- Committed in worktree `.worktrees/audit-guardrails` as `8c11d14 refactor(data): split history and download internals`.
