# Core Scenario Contract Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add formal S25-S29 scenario contract examples and documentation updates for core SDK workflows whose public APIs already exist but lack contract coverage.

**Architecture:** This is contract and documentation coverage, not capability expansion. New examples must stay inside the approved crate boundaries: wait owns diff-backed live refs and serial windows, session owns one-shot metadata/service queries, data owns research/download/export/Greeks, and task owns TargetPos ownership. Any compile gap found while adding these examples may only be fixed in the crate that already owns the capability.

**Tech Stack:** Rust, Cargo workspace, Tokio examples, existing `tqsdk-*` crates, Markdown docs.

---

## Source Inputs

- Design spec: `docs/superpowers/specs/2026-05-01-core-scenario-contract-coverage-design.md`
- Scenario boundary review: `docs/reviews/public-api-scenario-review.md`
- User-layer boundary plan: `docs/scenarios/user-layer-iteration-plan.md`
- Architecture authority: `docs/architecture/ai-workflow.md`, `docs/architecture/README.md`, `docs/architecture/crate-boundaries.md`
- Existing contract style examples:
  - `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs`
  - `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`
  - `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs`
  - `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`

## File Structure

Create formal scenario examples:

- `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`
  - Contract for wait-owned trading status, kline serial, tick serial, `wait_update`, and `is_changing`.
- `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`
  - Contract for one-shot metadata and service query pack owned by session.
- `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`
  - Contract for data-layer historical continuous quotes, pull-based downloads, `collect_remaining`, and CSV export.
- `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`
  - Contract for data-layer option Greeks as an owned research query.
- `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`
  - Contract for TargetPosTask/TargetPosScheduler ownership and wait-driven progression.
- `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`
  - Contract for wait-owned notification, settlement, risk management, security refs, and `confirm_settlement`.

Modify crate README files:

- `crates/tqsdk-wait/README.md`
  - Add S25 and S26 example links in the example section.
- `crates/tqsdk-session/README.md`
  - Add S27 example link and mention direct query pack ownership.
- `crates/tqsdk-data/README.md`
  - Add S28 example links for download/export and Greeks.
- `crates/tqsdk-task/README.md`
  - Add S29 example link beside `target_pos.rs` and `target_pos_scheduler.rs`.

Modify scenario/review docs:

- `docs/reviews/public-api-scenario-review.md`
  - Add S25-S29 to batch status, scenario matrix, and main conclusions.
- `docs/scenarios/user-layer-iteration-plan.md`
  - Add S25-S29 to user-layer table and relevant priority sections.

Do not modify:

- `tqsdk-core` public exports.
- `docs/architecture/*`, unless implementation reveals an actual architecture change. This plan should not require one.
- `docs/scenarios/api_gaps/*`, unless a later cleanup task is explicitly requested.

## Task 1: S25 Wait Serial And Trading Status

**Files:**
- Create: `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`
- Modify: `crates/tqsdk-wait/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create the S25 contract example**

Create `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs` with this complete content:

```rust
//! Scenario: Wait 行情序列与交易状态
//!
//! Primary user layer:
//! - 单策略作者
//!
//! Intended crate path:
//! - `tqsdk-wait`
//!
//! Lower-level escape hatch:
//! - 需要自管 cursor / hot read 时使用 `tqsdk-session` + `RuntimeReader`
//!
//! Non-goal:
//! - 历史下载、DataFrame / polars、direct query metadata
//!
//! User goal:
//! - 在单一 `wait_update()` 推进点内读取交易状态、K线窗口和 tick 窗口
//! - 用 `is_changing()` / `is_changing_fields()` 判断本轮 commit 是否影响对象
//! - 不把实时序列误用成历史下载接口
//!
//! API contract:
//! - `TqApi::get_trading_status` 返回 diff-backed live ref
//! - `TqApi::get_kline_serial` / `get_tick_serial` 返回实时窗口 ref
//! - `wait_update` 是用户可见状态推进边界
//! - `is_changing` 与 `is_changing_fields` 解释最近一次用户可见 commit
//! - 需要历史范围下载时转向 `tqsdk-data`
//!
//! Forbidden:
//! - GraphQL / metadata direct query
//! - `DataClient` downloader
//! - `RuntimeCommand`
//! - `StatePath`
//! - `serde_json::Value`
//!
//! Regression signal:
//! - 用户必须手写 chart command 才能拿到 K线或 tick 窗口
//! - 交易状态被放到 session direct query
//! - `is_changing()` 不能解释 serial window 的最近一次 commit
//!
//! Review questions:
//! - wait facade 是否自然表达实时序列窗口？
//! - 交易状态是否保持为 live ref 而不是一次性 metadata？
//! - 历史下载和实时窗口边界是否清晰？

use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let seconds = std::env::var("TQ_KLINE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    let data_length = std::env::var("TQ_SERIAL_LENGTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32);

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .build()
        .await?;

    let trading_status = api.get_trading_status(&symbol).await?;
    let klines = api
        .get_kline_serial(&symbol, Duration::from_secs(seconds), data_length)
        .await?;
    let ticks = api.get_tick_serial(&symbol, data_length).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        if !api
            .wait_update(Some(tokio::time::Instant::now() + Duration::from_secs(5)))
            .await?
        {
            continue;
        }

        if api.is_changing(&trading_status)? {
            let status = trading_status.load(&api)?;
            println!(
                "trading_status symbol={} status={:?}",
                symbol, status.trade_status
            );
        }

        if api.is_changing(&klines)? || api.is_changing_fields(&klines, &["data", "last_id"])? {
            let window = klines.load(&api)?;
            if let Some(last) = window.last() {
                println!(
                    "kline symbol={} rows={} datetime={} close={}",
                    symbol,
                    window.len(),
                    last.datetime,
                    last.close
                );
            }
        }

        if api.is_changing(&ticks)? || api.is_changing_fields(&ticks, &["data", "last_id"])? {
            let window = ticks.load(&api)?;
            if let Some(last) = window.last() {
                println!(
                    "tick symbol={} rows={} datetime={} last_price={}",
                    symbol,
                    window.len(),
                    last.datetime,
                    last.last_price
                );
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Compile S25 example and capture any real API mismatch**

Run:

```bash
cargo check -p tqsdk-wait --example api_contract_s25_wait_serial_trading_status
```

Expected:

The command exits with status 0 and Cargo output contains `Finished`.

If this fails because `KlineWindow` or `TickWindow` does not expose `last()` / `len()`, inspect `crates/tqsdk-wait/src/views/*.rs` and update the example to use the existing public accessor from the same view type. Do not add new view methods unless the existing README claims the accessor exists and the missing method is an actual public contract gap.

- [ ] **Step 3: Update wait README for S25**

In `crates/tqsdk-wait/README.md`, add this paragraph after the quote snapshot paragraph:

```markdown
实时交易状态和序列窗口属于 wait facade 的持续状态消费能力，而不是 data-layer 历史下载。`TqApi::get_trading_status`、`get_kline_serial` 和 `get_tick_serial` 返回 diff-backed live refs，并通过同一个 `wait_update()` / `is_changing()` 截面解释推进。契约示例见 [examples/api_contract_s25_wait_serial_trading_status.rs](examples/api_contract_s25_wait_serial_trading_status.rs)。
```

- [ ] **Step 4: Update scenario/review docs for S25**

In `docs/reviews/public-api-scenario-review.md`, add this row after S24 once the full S25-S29 matrix rows are being inserted:

```markdown
| 25. Wait 行情序列与交易状态 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`; `TqApi::{get_trading_status,get_kline_serial,get_tick_serial,wait_update,is_changing,is_changing_fields}`; 实时序列窗口属于 wait，不属于 data download 或 session direct query |
```

In `docs/scenarios/user-layer-iteration-plan.md`, update the user-layer table row for `单策略作者` so `对应场景` includes `25`:

```markdown
| 单策略作者 | 低样板、`wait_update()`、稳定状态截面、交易状态易懂 | `tqsdk-wait` | 1, 3, 6, 7, 8, 9, 10, 25, 26 | 继承 Python 语义，不复制 Python 单体 |
```

Add this bullet to the P0 startup/reconnect or wait-stable-snapshot section:

```markdown
- `api_contract_s25_wait_serial_trading_status`（新增）：覆盖 wait 风格 trading status、K线 serial、tick serial 和 `is_changing` 契约，确认实时窗口不回流到 session/data。
```

- [ ] **Step 5: Validate S25 batch**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check -p tqsdk-wait --examples
```

Expected:

```text
```

for `scripts/check_api_contract_examples.sh`.

The `cargo check` command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 6: Commit S25**

Run:

```bash
git add crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs crates/tqsdk-wait/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: add wait serial scenario contract"
```

Expected:

```text
docs: add wait serial scenario contract
```

Verify with:

```bash
git log -1 --pretty=%s
```

## Task 2: S27 Session Metadata And Service Query Pack

**Files:**
- Create: `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`
- Modify: `crates/tqsdk-session/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create the S27 contract example**

Create `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs` with this complete content:

```rust
//! Scenario: Session metadata 与 service query pack
//!
//! Primary user layer:
//! - 低层 / 高频用户
//! - direct-query 用户
//!
//! Intended crate path:
//! - `tqsdk-session`
//!
//! Lower-level escape hatch:
//! - 需要 raw payload 时使用 `SessionRawQuery::query_graphql_value`
//!
//! Non-goal:
//! - wait/stream live refs、历史下载、Greeks、DataFrame / polars
//!
//! User goal:
//! - 用一次性 query 查询合约列表、主连、期权链、交易日历、结算价、排名和 EDB
//! - 保持 direct query 归属在 session
//! - 不为了 metadata 查询创建 wait 或 stream facade
//!
//! API contract:
//! - metadata query 使用 `SessionClient` 的 typed one-shot public API
//! - service query 返回 core typed DTO
//! - options helper 使用 session-owned query DTO
//! - direct query 不复制到 `tqsdk-wait` 或 `tqsdk-stream`
//!
//! Forbidden:
//! - `TqApi::get_quote` / live subscription
//! - `TqStream`
//! - `DataClient`
//! - provider 内部 session type
//! - `serde_json::Value` 作为用户必须解析的返回值
//!
//! Regression signal:
//! - metadata query 必须通过 wait/stream facade
//! - calendar / settlement / ranking / EDB 被下沉到 data crate
//! - options query 要求用户手写 GraphQL payload
//!
//! Review questions:
//! - session 是否自然表达完整 direct-query pack？
//! - metadata 与 research/data 边界是否清晰？
//! - service query 返回值是否足够 typed？

use chrono::{Duration as ChronoDuration, Utc};
use tqsdk_session::{
    AllLevelOptionQuery, AtmOptionQuery, EdbDataAlign, EdbDataFill, FinanceOptionLevelQuery,
    OptionQueryFilter, SessionClientBuilder, SymbolRankingType,
};

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let exchange = std::env::var("TQ_TEST_EXCHANGE").unwrap_or_else(|_| "SHFE".to_string());
    let product = std::env::var("TQ_TEST_PRODUCT").unwrap_or_else(|_| "au".to_string());
    let underlying =
        std::env::var("TQ_OPTION_UNDERLYING").unwrap_or_else(|_| "SHFE.au2606".to_string());
    let finance_underlying =
        std::env::var("TQ_FINANCE_OPTION_UNDERLYING").unwrap_or_else(|_| "SSE.510300".to_string());

    let session = SessionClientBuilder::new(user, pass)
        .enable_query()
        .build()?;

    let quotes = session
        .query_quotes(Some("FUTURE"), Some(&exchange), Some(&product), Some(false), None)
        .await?;
    let cont_quotes = session
        .query_cont_quotes(Some(&exchange), Some(&product), None)
        .await?;

    let option_filter = OptionQueryFilter::new();
    let options = session
        .query_options(&underlying, &option_filter)
        .await?;
    let atm_options = session
        .query_atm_options(&underlying, &AtmOptionQuery::new(480.0, [-1, 0, 1], "CALL"))
        .await?;
    let all_level_options = session
        .query_all_level_options(&underlying, &AllLevelOptionQuery::new(480.0, "CALL"))
        .await?;
    let finance_options = session
        .query_all_level_finance_options(
            &finance_underlying,
            &FinanceOptionLevelQuery::new(4.0, "CALL", [0, 1, 2]),
        )
        .await?;

    let today = Utc::now().date_naive();
    let start = today - ChronoDuration::days(7);
    let calendar = session.get_trading_calendar(start, today).await?;
    let settlements = session
        .query_symbol_settlement(&[symbol.as_str()], 3, None)
        .await?;
    let ranking = session
        .query_symbol_ranking(&symbol, SymbolRankingType::Volume, 3, None, None)
        .await?;
    let edb = session
        .query_edb_data(
            &[100001],
            start,
            today,
            Some(EdbDataAlign::Day),
            Some(EdbDataFill::Forward),
        )
        .await?;

    println!(
        "quotes={} cont={} options={} atm={} all_level={}/{}/{} finance={}/{}/{} calendar={} settlements={} ranking={} edb={}",
        quotes.len(),
        cont_quotes.len(),
        options.len(),
        atm_options.len(),
        all_level_options.in_money.len(),
        all_level_options.at_money.len(),
        all_level_options.out_of_money.len(),
        finance_options.in_money.len(),
        finance_options.at_money.len(),
        finance_options.out_of_money.len(),
        calendar.len(),
        settlements.len(),
        ranking.len(),
        edb.len()
    );

    Ok(())
}
```

- [ ] **Step 2: Compile S27 example and capture any real API mismatch**

Run:

```bash
cargo check -p tqsdk-session --example api_contract_s27_metadata_service_queries
```

Expected:

The command exits with status 0 and Cargo output contains `Finished`.

If `chrono::Duration` is not available to examples through the crate feature set, replace date arithmetic with `today.pred_opt()` loops from `chrono::NaiveDate`. Do not add a new dependency.

- [ ] **Step 3: Update session README for S27**

In `crates/tqsdk-session/README.md`, add this paragraph after the paragraph describing `SessionMetadataQuery` / `SessionServiceQuery`:

```markdown
完整 metadata / service direct-query pack 的契约示例见 [examples/api_contract_s27_metadata_service_queries.rs](examples/api_contract_s27_metadata_service_queries.rs)。该示例覆盖 `query_quotes`、主连查询、期权链、ATM / all-level options、交易日历、结算价、排名和 EDB，并确认这些一次性请求继续归属 `tqsdk-session`，不复制到 wait/stream。
```

- [ ] **Step 4: Update scenario/review docs for S27**

In `docs/reviews/public-api-scenario-review.md`, add this row after S26 once the full S25-S29 matrix rows are being inserted:

```markdown
| 27. Session metadata 与 service query pack | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`; `SessionClient::{query_quotes,query_cont_quotes,query_options,query_atm_options,query_all_level_options,query_all_level_finance_options,get_trading_calendar,query_symbol_settlement,query_symbol_ranking,query_edb_data}`; direct query 继续归属 session |
```

In `docs/scenarios/user-layer-iteration-plan.md`, update the user-layer table row for `低层 / 高频用户` so `对应场景` includes `27`:

```markdown
| 低层 / 高频用户 | 自带 Tokio runtime、自己推进 session、热路径读取行情 | `tqsdk-core` + `tqsdk-session` | 5, 23, 27 | 维持薄底座，不上移厚 facade |
```

Add this bullet to the direct-query / metadata section:

```markdown
- `api_contract_s27_metadata_service_queries`（新增）：覆盖 metadata/service query pack，确认合约列表、主连、期权、交易日历、结算价、排名和 EDB 仍是 session one-shot request/response。
```

- [ ] **Step 5: Validate S27 batch**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check -p tqsdk-session --examples
```

Expected:

```text
```

for `scripts/check_api_contract_examples.sh`.

The `cargo check` command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 6: Commit S27**

Run:

```bash
git add crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs crates/tqsdk-session/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: add session query pack scenario contract"
```

Expected:

```text
docs: add session query pack scenario contract
```

Verify with:

```bash
git log -1 --pretty=%s
```

## Task 3: S28 Data Download, Export, And Greeks

**Files:**
- Create: `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`
- Create: `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create the S28 download/export contract example**

Create `crates/tqsdk-data/examples/api_contract_s28_download_export.rs` with this complete content:

```rust
//! Scenario: Data 历史下载与 CSV 导出
//!
//! Primary user layer:
//! - 研究 / 数据用户
//!
//! Intended crate path:
//! - `tqsdk-data`
//!
//! Lower-level escape hatch:
//! - 需要 page 级控制时使用 `get_kline_data_page` / `get_tick_data_page`
//!
//! Non-goal:
//! - live `wait_update()`、stream fan-out、session metadata query
//!
//! User goal:
//! - 查询历史主连
//! - 按时间范围拉取 K线 / tick
//! - 观察下载进度
//! - 将历史数据导出到调用方提供的 async writer
//!
//! API contract:
//! - 历史下载与 CSV materialization 使用 `tqsdk-data`
//! - download 是 pull-based async substrate，不内置后台线程
//! - `collect_remaining()` 只 materialize 尚未消费的剩余页
//! - CSV export 写入调用方提供的 `AsyncWrite`
//!
//! Forbidden:
//! - `TqApi::wait_update`
//! - `TqStream`
//! - direct `RuntimeCommand::MarketChartCommand`
//! - SDK 内部路径管理或后台 downloader daemon
//! - DataFrame / polars 作为必需依赖
//!
//! Regression signal:
//! - 历史下载必须通过实时订阅循环
//! - CSV export 要求用户自己拼 chart page
//! - data crate 开始拥有 live session facade
//!
//! Review questions:
//! - data crate 是否自然表达历史下载和导出？
//! - pull-based download 的进度语义是否足够？
//! - session/wait/data 的边界是否清晰？

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::io::sink;
use tqsdk_data::{DataClient, KlineDataSeriesRequest, TickDataSeriesRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());
    let cont_symbol =
        std::env::var("TQ_CONT_SYMBOL").unwrap_or_else(|_| "KQ.m@SHFE.au".to_string());
    let end = Utc::now();
    let start = end - ChronoDuration::hours(4);
    let start_ns = start
        .timestamp_nanos_opt()
        .ok_or("invalid start timestamp")?;
    let end_ns = end.timestamp_nanos_opt().ok_or("invalid end timestamp")?;

    let cont_symbols = [cont_symbol.as_str()];
    let cont_rows = DataClient::new()
        .query_his_cont_quotes(&cont_symbols, 5, Some(end.date_naive()))
        .await?;

    let session = SessionClientBuilder::new(user, pass)
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);

    let mut download = client.kline_data_download(
        KlineDataSeriesRequest::new(
            symbol.clone(),
            Duration::from_secs(60),
            start_ns,
            end_ns,
        )
        .with_page_view_width(128)
        .with_timeout(Duration::from_secs(30)),
    )?;

    let first_page_rows = match download.next_page().await? {
        Some(page) => {
            let progress = page.progress();
            println!(
                "first_page rows={} emitted_pages={} progress={:.2}%",
                page.len(),
                progress.emitted_pages(),
                progress.completion_percent()
            );
            page.len()
        }
        None => 0,
    };
    let remaining_rows = download.collect_remaining().await?;
    let final_progress = download.progress();

    let mut kline_csv = sink();
    let kline_summary = client
        .export_kline_data_csv(
            KlineDataSeriesRequest::new(
                symbol.clone(),
                Duration::from_secs(60),
                start_ns,
                end_ns,
            )
            .with_page_view_width(128)
            .with_timeout(Duration::from_secs(30)),
            &mut kline_csv,
        )
        .await?;

    let mut tick_csv = sink();
    let tick_summary = client
        .export_tick_data_csv(
            TickDataSeriesRequest::new(symbol.as_str(), start_ns, end_ns)
                .with_page_view_width(128)
                .with_timeout(Duration::from_secs(30)),
            &mut tick_csv,
        )
        .await?;

    println!(
        "symbol={} cont_rows={} first_page_rows={} remaining_rows={} complete={} kline_csv_rows={} tick_csv_rows={}",
        symbol,
        cont_rows.len(),
        first_page_rows,
        remaining_rows.len(),
        final_progress.is_complete(),
        kline_summary.rows_written,
        tick_summary.rows_written
    );

    Ok(())
}
```

- [ ] **Step 2: Create the S28 option Greeks contract example**

Create `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs` with this complete content:

```rust
//! Scenario: Data 期权 Greeks 研究查询
//!
//! Primary user layer:
//! - 研究 / 数据用户
//!
//! Intended crate path:
//! - `tqsdk-data`
//!
//! Lower-level escape hatch:
//! - 需要原始实时 quote 时使用 wait/stream live market API，而不是复用 Greeks 内部 snapshot helper
//!
//! Non-goal:
//! - 通用 live quote snapshot public API、交易下单、风控、DataFrame / polars
//!
//! User goal:
//! - 一次性计算期权 Greeks
//! - 获得 owned typed rows
//! - 不手写 Black-Scholes 输入装配或临时订阅清理
//!
//! API contract:
//! - `query_option_greeks` 是 `tqsdk-data` 的研究接口
//! - request 显式携带 symbols、波动率、无风险利率和超时
//! - 返回 `OptionGreeksResult` / `OptionGreeksRow`
//! - 内部 live quote snapshot 不作为通用 public market snapshot surface 暴露
//!
//! Forbidden:
//! - `TqApi::get_quote` 作为 Greeks 查询前置样板
//! - `RuntimeCommand`
//! - provider 内部 quote snapshot helper
//! - 用户手写 `Arc<Mutex<_>>` 管理临时订阅
//!
//! Regression signal:
//! - Greeks 查询被移到 session metadata layer
//! - 用户必须先创建 wait facade 才能调用 Greeks
//! - 返回值退化成 raw JSON 或 tuple
//!
//! Review questions:
//! - Greeks 是否保持为 data/research 能力？
//! - request/response 是否足够 typed？
//! - 内部 snapshot 能力是否仍未被错误提升为通用 API？

use std::time::Duration;

use tqsdk_data::{DataClient, OptionGreeksRequest};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let symbols = std::env::var("TQ_OPTION_SYMBOLS")
        .unwrap_or_else(|_| "SHFE.au2606C720".to_string())
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();

    let session = SessionClientBuilder::new(user, pass)
        .enable_query()
        .futures_market()
        .build()?;
    let client = DataClient::from_session(session);

    let result = client
        .query_option_greeks(
            OptionGreeksRequest::new(symbols)
                .with_volatilities([0.18, 0.22, 0.26])
                .with_risk_free_rate(0.02)
                .with_timeout(Duration::from_secs(30)),
        )
        .await?;

    for row in result.iter() {
        println!(
            "symbol={} class={} underlying={} vol={} delta={} gamma={} theta={} vega={} rho={}",
            row.symbol,
            row.option_class,
            row.underlying_symbol,
            row.volatility,
            row.delta,
            row.gamma,
            row.theta,
            row.vega,
            row.rho
        );
    }

    Ok(())
}
```

- [ ] **Step 3: Compile S28 examples and capture real API mismatches**

Run:

```bash
cargo check -p tqsdk-data --example api_contract_s28_download_export
cargo check -p tqsdk-data --example api_contract_s28_option_greeks
```

Expected for each command:

The command exits with status 0 and Cargo output contains `Finished`.

If the download example fails because `query_his_cont_quotes` has changed, inspect `crates/tqsdk-data/src/client/cont_quotes.rs` and update the example to use the current public function signature. Do not move historical continuous quotes to session.

- [ ] **Step 4: Update data README for S28**

In `crates/tqsdk-data/README.md`, add these bullets under the example list:

```markdown
- [examples/api_contract_s28_download_export.rs](examples/api_contract_s28_download_export.rs)
- [examples/api_contract_s28_option_greeks.rs](examples/api_contract_s28_option_greeks.rs)
```

Add this paragraph after the current paragraph about session-backed option Greeks:

```markdown
S28 契约示例把下载 / 导出和 Greeks 拆成两个文件：download/export 覆盖历史主连、K线 / tick pull-based download、`collect_remaining()` 和调用方提供 writer 的 CSV export；Greeks 覆盖 `OptionGreeksRequest` / `OptionGreeksResult`。两者都属于 `tqsdk-data` 的 research/offline surface，不回流到 session/wait/stream。
```

- [ ] **Step 5: Update scenario/review docs for S28**

In `docs/reviews/public-api-scenario-review.md`, add this row after S27 once the full S25-S29 matrix rows are being inserted:

```markdown
| 28. Data 下载 / 导出 / Greeks | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`; `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`; `DataClient::{query_his_cont_quotes,kline_data_download,tick_data_download,export_kline_data_csv,export_tick_data_csv,query_option_greeks}`; research/download/Greeks 继续归属 data |
```

In `docs/scenarios/user-layer-iteration-plan.md`, update the user-layer table row for `研究 / 数据用户` so `对应场景` includes `28`:

```markdown
| 研究 / 数据用户 | 历史数据、批处理、缓存、CSV、离线分析 | `tqsdk-data` | 16, 17, 18, 28 | 独立数据层，不污染 session/wait |
```

Add this bullet to the P2 data/research section:

```markdown
- `api_contract_s28_download_export` 与 `api_contract_s28_option_greeks`（新增）：覆盖历史主连、下载进度、CSV materialization 和 Greeks research query，确认这些能力不进入 session/wait/stream。
```

- [ ] **Step 6: Validate S28 batch**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check -p tqsdk-data --examples
```

Expected:

```text
```

for `scripts/check_api_contract_examples.sh`.

The `cargo check` command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 7: Commit S28**

Run:

```bash
git add crates/tqsdk-data/examples/api_contract_s28_download_export.rs crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs crates/tqsdk-data/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: add data research scenario contracts"
```

Expected:

```text
docs: add data research scenario contracts
```

Verify with:

```bash
git log -1 --pretty=%s
```

## Task 4: S29 Target Position Ownership

**Files:**
- Create: `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`
- Modify: `crates/tqsdk-task/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create the S29 contract example**

Create `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs` with this complete content:

```rust
//! Scenario: TargetPosTask ownership
//!
//! Primary user layer:
//! - 执行工具用户
//! - 单策略作者
//!
//! Intended crate path:
//! - `tqsdk-task`
//!
//! Lower-level escape hatch:
//! - 需要直接下单时使用 `TaskHost::orders` 或 wait 层 `OrderTicket`
//!
//! Non-goal:
//! - 自动 hedge / flatten、跨账户 TargetPos 编排、durable audit/resume
//!
//! User goal:
//! - 为同账户同合约创建目标持仓任务
//! - 让 task ownership 阻止手动下单互相踩状态
//! - 通过同一个 `TaskHost::wait_update()` 推进 task 和 scheduler
//! - dry-run 时也能验证 ownership 契约而不真实下单
//!
//! API contract:
//! - `TaskHost::target_pos` 注册 `account_id + symbol` ownership
//! - 同一 ownership 下的手动下单会被 `check_manual_order_allowed` 拒绝
//! - `TargetPosTask::set_target_volume` 只设置目标，实际提交由 host 推进
//! - `TargetPosScheduler` 也由 `TaskHost::wait_update()` 驱动
//! - 不承诺自动跨账户调仓或生产级持久恢复
//!
//! Forbidden:
//! - 绕过 `TaskHost` 直接在任务运行时手动插单
//! - 在 `tqsdk-core` 中实现 TargetPos
//! - 跨账户 TargetPos orchestration
//! - 自动 hedge / flatten / 补单策略
//!
//! Regression signal:
//! - 同账户同合约可以同时存在多个 TargetPos owner
//! - 手动下单绕过 ownership guard
//! - TargetPosTask 需要用户自己创建独立 wait loop
//!
//! Review questions:
//! - TargetPos ownership 是否足够显式？
//! - scheduler 是否复用同一个 task/wait 推进点？
//! - dry-run 路径是否能验证核心 ownership contract？

use std::time::Duration;

use tqsdk_task::{TargetPosScheduleStep, TaskHost};
use tqsdk_wait::TqApiBuilder;

fn read_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("missing environment variable: {name}").into())
}

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let account_id = read_optional_env("TQ_TASK_ACCOUNT_ID").unwrap_or_else(|| "sim".to_string());
    let symbol = read_optional_env("TQ_TASK_SYMBOL").unwrap_or_else(|| "SHFE.au2602".to_string());
    let allow_orders = std::env::var_os("TQ_TASK_ALLOW_ORDERS").is_some();
    let target_volume = std::env::var("TQ_TARGET_VOLUME")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    let api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()
        .await?;
    let mut host = TaskHost::new(api);

    let task = host
        .target_pos(account_id.as_str(), symbol.as_str())
        .build()?;

    let manual_order_check = host.check_manual_order_allowed(account_id.as_str(), symbol.as_str());
    if manual_order_check.is_ok() {
        return Err("manual order should be rejected while TargetPosTask owns the symbol".into());
    }

    let conflicting_task = host
        .target_pos(account_id.as_str(), symbol.as_str())
        .build();
    if conflicting_task.is_ok() {
        return Err("duplicate TargetPosTask should be rejected".into());
    }

    let other_symbol = format!("{symbol}.dry_run_contract_boundary");
    let scheduler = host
        .target_pos_scheduler(account_id.as_str(), other_symbol.as_str())
        .steps(vec![TargetPosScheduleStep::pause(Duration::from_millis(1))])
        .build()?;

    if !allow_orders {
        let _updated = host
            .wait_update(Some(tokio::time::Instant::now() + Duration::from_millis(100)))
            .await?;
        println!(
            "dry_run account={} symbol={} target_owner_active={} scheduler_finished={}",
            account_id,
            symbol,
            !task.is_finished(),
            scheduler.is_finished()
        );
        return Ok(());
    }

    task.set_target_volume(target_volume)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut event_cursor = 0_usize;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err("timed out waiting for TargetPosTask".into());
        }

        let _updated = host
            .wait_update(Some(tokio::time::Instant::now() + Duration::from_secs(1)))
            .await?;

        let (next_event_cursor, events) = task.execution_events_since(event_cursor);
        event_cursor = next_event_cursor;
        for event in events {
            println!("target_pos_event={event:?}");
        }

        if let Some(error) = task.last_error() {
            return Err(format!("TargetPosTask failed: {error}").into());
        }

        if task.is_finished() {
            println!("target task finished report={:?}", task.execution_report());
            break;
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Compile S29 example and capture any real API mismatch**

Run:

```bash
cargo check -p tqsdk-task --example api_contract_s29_target_pos_ownership
```

Expected:

The command exits with status 0 and Cargo output contains `Finished`.

If `TargetPosTask::execution_events_since` changes its return type, inspect `crates/tqsdk-task/src/target_pos.rs` and keep the cursor-style incremental event read in the example. Do not weaken the ownership assertions.

- [ ] **Step 3: Update task README for S29**

In `crates/tqsdk-task/README.md`, add this bullet in the example list:

```markdown
- [examples/api_contract_s29_target_pos_ownership.rs](examples/api_contract_s29_target_pos_ownership.rs)
```

Add this paragraph after the existing target-pos example description:

```markdown
`api_contract_s29_target_pos_ownership.rs` 单独覆盖 TargetPosTask / TargetPosScheduler 的 ownership 契约：同账户同合约任务会阻止重复 owner 和手动下单，实际推进仍由 `TaskHost::wait_update()` 统一驱动。示例默认 dry-run，只验证 ownership；只有显式设置 `TQ_TASK_ALLOW_ORDERS=1` 和 `TQ_TARGET_VOLUME` 时才进入真实调仓。
```

- [ ] **Step 4: Update scenario/review docs for S29**

In `docs/reviews/public-api-scenario-review.md`, add this row after S28 once the full S25-S29 matrix rows are being inserted:

```markdown
| 29. TargetPosTask ownership | 自然 | 中 | 无 | 无 | 中 | 低 | 维护边界 | `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`; `TaskHost::{target_pos,target_pos_scheduler,check_manual_order_allowed,wait_update}`; `TargetPosTask`; `TargetPosScheduler`; 同账户同合约 ownership 属于 task，跨账户 TargetPos 编排和 durable audit 不进入核心 SDK |
```

In `docs/scenarios/user-layer-iteration-plan.md`, update the user-layer table row for `执行工具用户` so `对应场景` includes `29`:

```markdown
| 执行工具用户 | 目标持仓、订单 intent、撤补、两腿套利、风控、多账户 | `tqsdk-task` | 10, 11, 12, 13, 19, 29 | 建立执行层抽象，不下沉到 core |
```

Add this bullet to the P1 execution section:

```markdown
- `api_contract_s29_target_pos_ownership`（新增）：把 TargetPosTask / scheduler ownership 从 S11 策略示例中独立出来，确认同账户同合约 owner、手动下单 guard 和 `TaskHost::wait_update()` 统一推进。
```

- [ ] **Step 5: Validate S29 batch**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check -p tqsdk-task --examples
```

Expected:

```text
```

for `scripts/check_api_contract_examples.sh`.

The `cargo check` command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 6: Commit S29**

Run:

```bash
git add crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs crates/tqsdk-task/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: add target position scenario contract"
```

Expected:

```text
docs: add target position scenario contract
```

Verify with:

```bash
git log -1 --pretty=%s
```

## Task 5: S26 Wait Trade And System Live Refs

**Files:**
- Create: `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`
- Modify: `crates/tqsdk-wait/README.md`
- Modify: `docs/reviews/public-api-scenario-review.md`
- Modify: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Create the S26 contract example**

Create `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs` with this complete content:

```rust
//! Scenario: Wait trade 与 system live refs
//!
//! Primary user layer:
//! - 单策略作者
//! - 交易状态观察用户
//!
//! Intended crate path:
//! - `tqsdk-wait`
//!
//! Lower-level escape hatch:
//! - 需要多消费者事件流时使用 `tqsdk-stream`
//!
//! Non-goal:
//! - direct query metadata、生产级风控服务、证券交易策略封装
//!
//! User goal:
//! - 在 wait facade 中持有通知、结算、风险和证券交易对象 live refs
//! - 通过 `snapshot` 做可选读取，通过 `is_changing` 解释最近一次 commit
//! - 用 `confirm_settlement` 提交结算确认命令
//!
//! API contract:
//! - trade/system 对象 ref 属于 wait 的 diff-backed live state surface
//! - missing object 使用 `snapshot -> Option<T>` 表达
//! - `confirm_settlement` 是 wait 风格 trade command wrapper
//! - 证券 account/position/order/trade 使用独立 typed refs
//!
//! Forbidden:
//! - GraphQL / metadata direct query
//! - provider 内部 trade path
//! - 手动 `StatePath`
//! - 用字符串解析交易对象类型
//! - 本地第二棵交易状态树
//!
//! Regression signal:
//! - 风险、通知、结算或证券对象只能通过 raw state path 读取
//! - `confirm_settlement` 被移到 session direct-query API
//! - securities refs 与 futures refs 只能用 untyped JSON 区分
//!
//! Review questions:
//! - less-visible wait refs 是否可发现？
//! - optional snapshot 是否比 load panic/错误路径更适合文档示例？
//! - trade command 和 direct query 的边界是否清晰？

use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let account_id = read_optional_env("TQ_TRADE_ACCOUNT_ID").unwrap_or_else(|| "sim".to_string());
    let exchange_id = read_optional_env("TQ_TEST_EXCHANGE").unwrap_or_else(|| "SHFE".to_string());
    let symbol = read_optional_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.au2602".to_string());
    let trading_day = read_optional_env("TQ_TRADING_DAY").unwrap_or_else(|| "20260101".to_string());
    let notification_id =
        read_optional_env("TQ_NOTIFICATION_ID").unwrap_or_else(|| "latest".to_string());
    let order_id = read_optional_env("TQ_SECURITY_ORDER_ID").unwrap_or_else(|| "sample-order".to_string());
    let trade_id = read_optional_env("TQ_SECURITY_TRADE_ID").unwrap_or_else(|| "sample-trade".to_string());

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()
        .await?;

    let notification = api.get_notification(notification_id.as_str());
    let settlement = api.get_settlement_info(account_id.as_str(), trading_day.as_str());
    let risk_rule = api.get_risk_management_rule(account_id.as_str(), exchange_id.as_str());
    let risk_data = api.get_risk_management_data(account_id.as_str(), symbol.as_str());
    let security_account = api.get_security_account(account_id.as_str());
    let security_position = api.get_security_position(account_id.as_str(), symbol.as_str());
    let security_order = api.get_security_order(account_id.as_str(), order_id.as_str());
    let security_trade = api.get_security_trade(account_id.as_str(), trade_id.as_str());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if !api
            .wait_update(Some(tokio::time::Instant::now() + Duration::from_secs(1)))
            .await?
        {
            continue;
        }

        if api.is_changing(&notification)? {
            println!("notification={:?}", notification.snapshot(&api)?);
        }
        if api.is_changing(&settlement)? {
            println!("settlement_ready={}", settlement.is_ready(&api)?);
        }
        if api.is_changing(&risk_rule)? {
            println!("risk_rule_ready={}", risk_rule.is_ready(&api)?);
        }
        if api.is_changing(&risk_data)? {
            println!("risk_data_ready={}", risk_data.is_ready(&api)?);
        }
        if api.is_changing(&security_account)? {
            println!("security_account_ready={}", security_account.is_ready(&api)?);
        }
        if api.is_changing(&security_position)? {
            println!("security_position_ready={}", security_position.is_ready(&api)?);
        }
        if api.is_changing(&security_order)? {
            println!("security_order_ready={}", security_order.is_ready(&api)?);
        }
        if api.is_changing(&security_trade)? {
            println!("security_trade_ready={}", security_trade.is_ready(&api)?);
        }
    }

    if std::env::var_os("TQ_CONFIRM_SETTLEMENT").is_some() {
        api.confirm_settlement(account_id.as_str()).await?;
        println!("confirm_settlement submitted account={}", account_id);
    }

    Ok(())
}
```

- [ ] **Step 2: Compile S26 example and capture any real API mismatch**

Run:

```bash
cargo check -p tqsdk-wait --example api_contract_s26_trade_system_refs
```

Expected:

The command exits with status 0 and Cargo output contains `Finished`.

If the example becomes noisy during implementation, keep this file focused on notification/settlement/risk/confirm-settlement and create `crates/tqsdk-wait/examples/api_contract_s26_security_trade_refs.rs` for securities refs with the same required contract headers. If split, update README and review docs to list both files under S26.

- [ ] **Step 3: Update wait README for S26**

In `crates/tqsdk-wait/README.md`, add this paragraph after the S25 paragraph:

```markdown
较少见的 trade/system live refs 也属于 wait facade：`NotificationRef`、`SettlementInfoRef`、`RiskManagementRuleRef`、`RiskManagementDataRef` 以及证券 account/position/order/trade refs 都通过同一 runtime state tree 和 `is_changing()` 观察。`confirm_settlement` 是 wait 风格 trade command wrapper。契约示例见 [examples/api_contract_s26_trade_system_refs.rs](examples/api_contract_s26_trade_system_refs.rs)。
```

- [ ] **Step 4: Update scenario/review docs for S26**

In `docs/reviews/public-api-scenario-review.md`, add this row after S25 once the full S25-S29 matrix rows are being inserted:

```markdown
| 26. Wait trade 与 system live refs | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`; `TqApi::{get_notification,get_settlement_info,get_risk_management_rule,get_risk_management_data,get_security_account,get_security_position,get_security_order,get_security_trade,confirm_settlement}`; less-visible live refs 继续归属 wait |
```

In `docs/scenarios/user-layer-iteration-plan.md`, ensure the user-layer table row for `单策略作者` includes both `25` and `26`:

```markdown
| 单策略作者 | 低样板、`wait_update()`、稳定状态截面、交易状态易懂 | `tqsdk-wait` | 1, 3, 6, 7, 8, 9, 10, 25, 26 | 继承 Python 语义，不复制 Python 单体 |
```

Add this bullet to the wait facade section:

```markdown
- `api_contract_s26_trade_system_refs`（新增）：覆盖 notification、settlement、risk management、证券交易对象 ref 与 `confirm_settlement`，确认这些对象是 wait live refs，不是 session direct query。
```

- [ ] **Step 5: Validate S26 batch**

Run:

```bash
scripts/check_api_contract_examples.sh
cargo check -p tqsdk-wait --examples
```

Expected:

```text
```

for `scripts/check_api_contract_examples.sh`.

The `cargo check` command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 6: Commit S26**

Run:

```bash
git add crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs crates/tqsdk-wait/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: add wait trade ref scenario contract"
```

Expected:

```text
docs: add wait trade ref scenario contract
```

Verify with:

```bash
git log -1 --pretty=%s
```

## Task 6: Final Consistency Validation

**Files:**
- Review: `crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`
- Review: `crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`
- Review: `crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`
- Review: `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`
- Review: `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`
- Review: `crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`
- Review: `crates/tqsdk-wait/README.md`
- Review: `crates/tqsdk-session/README.md`
- Review: `crates/tqsdk-data/README.md`
- Review: `crates/tqsdk-task/README.md`
- Review: `docs/reviews/public-api-scenario-review.md`
- Review: `docs/scenarios/user-layer-iteration-plan.md`

- [ ] **Step 1: Check that every new contract example has required headers**

Run:

```bash
scripts/check_api_contract_examples.sh
```

Expected:

```text
```

- [ ] **Step 2: Check all workspace examples**

Run:

```bash
cargo check --workspace --examples
```

Expected:

The command exits with status 0 and Cargo output contains `Finished`.

- [ ] **Step 3: Verify scenario numbering and links**

Run:

```bash
rg -l "api_contract_s25_wait_serial_trading_status.rs|api_contract_s26_trade_system_refs.rs" crates/tqsdk-wait/README.md
rg -l "api_contract_s27_metadata_service_queries.rs" crates/tqsdk-session/README.md
rg -l "api_contract_s28_download_export.rs|api_contract_s28_option_greeks.rs" crates/tqsdk-data/README.md
rg -l "api_contract_s29_target_pos_ownership.rs" crates/tqsdk-task/README.md
rg -l "25. Wait 行情序列与交易状态|26. Wait trade 与 system live refs|27. Session metadata 与 service query pack|28. Data 下载 / 导出 / Greeks|29. TargetPosTask ownership" docs/reviews/public-api-scenario-review.md
```

Expected output:

```text
crates/tqsdk-wait/README.md
crates/tqsdk-session/README.md
crates/tqsdk-data/README.md
crates/tqsdk-task/README.md
docs/reviews/public-api-scenario-review.md
```

- [ ] **Step 4: Confirm no architecture authority document changed**

Run:

```bash
git diff --name-only HEAD~5 HEAD | rg '^docs/architecture/' || true
```

Expected:

```text
```

If this command prints architecture files, inspect the diff and include an explicit note in the final execution report explaining the actual architecture change. For this contract-coverage plan, the expected result is no architecture file change.

- [ ] **Step 5: Final commit if Task 6 made doc fixes**

If Task 6 required link or wording fixes, run:

```bash
git add crates/tqsdk-wait/README.md crates/tqsdk-session/README.md crates/tqsdk-data/README.md crates/tqsdk-task/README.md docs/reviews/public-api-scenario-review.md docs/scenarios/user-layer-iteration-plan.md
git commit -m "docs: align scenario contract references"
```

Expected if files changed:

```text
docs: align scenario contract references
```

Verify with:

```bash
git log -1 --pretty=%s
```

Expected if no files changed:

```text
nothing to commit, working tree clean
```

## Final Acceptance Criteria

- S25-S29 formal contract examples exist under the owning crates and compile.
- `scripts/check_api_contract_examples.sh` passes with no missing header output.
- `cargo check --workspace --examples` passes.
- `docs/reviews/public-api-scenario-review.md` lists S25-S29 as core scenario-contract coverage, not capability expansion.
- `docs/scenarios/user-layer-iteration-plan.md` maps S25-S29 to the correct user layers.
- README files for wait/session/data/task link the new contract examples.
- Direct query APIs remain in `tqsdk-session`.
- Historical download/export/Greeks remain in `tqsdk-data`.
- TargetPos ownership remains in `tqsdk-task`.
- No new non-core platform capability is promoted to a formal SDK contract.
