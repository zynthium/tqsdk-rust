# tqsdk-rust SDK 审查报告 — 待处理项

> 审查日期：2026-04-29
> 审查范围：全 workspace（6 crate，161 文件，2098 函数）
> 已修复项见 commit `6c44c6c` 及后续未提交变更

---

## 处理闭环状态（2026-04-29）

| 项目 | 状态 | 结论 / 证据 |
| --- | --- | --- |
| 1.1 unsafe 块缺少 SAFETY 注释 | `done` | endpoint config tests 与 noop waker helpers 已补 `SAFETY:` 注释（`3109ff3`），并由 `cargo clippy --workspace --examples --all-targets -- -D warnings` 验证。 |
| 1.2 `CLIENT_SECRET` 硬编码 | `done` + `won't do` | 已补注释说明是公开 OAuth2 client identifier（`3109ff3`）。不做 builder 注入，因当前没有凭据轮换需求且不是用户密钥。 |
| 1.3 `AuthContext` public token fields | `done` | `AuthContext` 字段已私有化并保留 `new` / accessor / builder-style methods（`418f7ee`），新增 compile-fail doctest 防止直接字段构造回归。 |
| 1.4 workspace crate license 字段缺失 | `blocked by architecture decision` | 已从低风险 guardrail 批次排除；需要项目 license 明确选择后才能修改 workspace metadata。 |
| 1.5 yawc WebSocket 审计 | `blocked by architecture decision` | 这是持续安全审计流程项，不在本批代码修复范围；后续应接入 `cargo audit` / dependency review。 |
| 2.1 总体覆盖率不足 | `focused batch done; global threshold out of scope` | 已执行 focused coverage expansion（`1d7f237`、`d011286`、`0fb5e74`、`1896bf2`），覆盖 core/session/wait/task 的剩余 helper 模块；本批仍不承诺全局 80% 数字目标或覆盖率工具接入。 |
| 2.2 缺少测试的关键模块 | `focused batch done` | P0 guardrails 已由 `a94c887` 等覆盖；P1/P2 helper 覆盖由 focused coverage expansion 补齐，`execution_group` 经核对已有 10 个集成测试覆盖计划列出的关键场景。 |
| 2.3 测试基础设施利用不足 | `focused batch done; cross-crate fake expansion out of scope` | 已继续复用 explicit testing fixtures 和私有单元测试覆盖 helper 行为；跨 crate fake 层扩展与全局覆盖率工具不是本批目标。 |
| 3.1 `tqsdk-data/src/client.rs` 大文件与重复 | `done` | 已拆分 `client/page.rs`、`chart_reader.rs`、`cont_quotes.rs`、`permissions.rs`，并保留 source-compatible public API（`8c11d14`）。 |
| 3.2 `tqsdk-core/src/session_runtime.rs` 大文件与重复 | `done` | command status derivation 已拆分（`7e43df8`），reconnect/timer/transport/detail helpers 已在下一批 runtime hardening 中拆入 `session_runtime/*` 子模块并由 session runtime 回归测试覆盖。 |
| 3.3 `tqsdk-task/src/target_pos.rs` 控制流复杂 | `done` | 已提取 target plan state / cancel / desired batch / target reached 辅助并简化 drop/cancel 流程（`49ff822`）。 |
| 3.4 `tqsdk-data/src/download.rs` 对称重复 | `done` | 已拆分 `download/page.rs`、`download/inner.rs` 并共享 progress helper（`8c11d14`）。 |
| 3.5 `tqsdk-session/src/client/io.rs` route driving 重复 | `done` | 已提取 route deadline driving 和 pending-route executor helper（`a7c42e8`）。 |
| 3.6 history download permission 重复 | `done` | 已提取 `client/permissions.rs` 和 `has_tq_dl_feature()`，保持同步/异步行为兼容（`8c11d14`）。 |

最终验证：

- `cargo check --workspace --examples`
- `cargo test --workspace`
- `cargo clippy --workspace --examples --all-targets -- -D warnings`
- `cargo build -p tqsdk-session --no-default-features`
- `cargo build -p tqsdk-wait --no-default-features`
- `cargo build -p tqsdk-stream --no-default-features`
- `cargo build -p tqsdk-task --no-default-features`
- `cargo build -p tqsdk-data --no-default-features`

Next runtime hardening batch verification:

- `cargo test -p tqsdk-core`
- `cargo test --workspace`
- `cargo clippy --workspace --examples --all-targets -- -D warnings`

---

## 一、安全

### 1.1 [MEDIUM] unsafe 块缺少 SAFETY 注释

测试代码中 10+ 处 `unsafe { std::env::set_var(...) }` 和 `noop_waker` 模式缺少
`// SAFETY:` 注释。虽然有 `ENV_MUTEX` 保护，但安全不变量未文档化。

涉及文件：

- `crates/tqsdk-core/tests/runtime_contract_endpoint_config.rs` 第 28、64、122、131、134 行
- `crates/tqsdk-core/tests/runtime_contract_command_ledger.rs`（noop_waker）
- `crates/tqsdk-core/tests/runtime_contract_session_cycle.rs`（noop_waker）
- 其他 8+ 个测试文件中的 noop_waker 模式

修复方式：在每个 `unsafe` 块前添加 `// SAFETY:` 注释，说明为什么操作是安全的。

### 1.2 [MEDIUM] CLIENT_SECRET 硬编码

`crates/tqsdk-session/src/tq_auth.rs:23`：

```rust
const CLIENT_SECRET: &str = "be30b9f4-6862-488a-99ad-21bde0400081";
```

这是天勤官方 Python SDK 中公开使用的 OAuth2 public client 凭证。建议至少添加注释
说明其公开性质，或通过编译时环境变量注入以便轮换时无需重新编译。

### 1.3 [LOW] AuthContext 的 access_token 字段是 pub

`crates/tqsdk-core/src/auth.rs:9`：

```rust
pub struct AuthContext {
    pub access_token: String,  // 直接公开字段
    ...
}
```

已有 `pub fn access_token(&self) -> &str` 访问器，但字段本身也是 `pub`，允许外部代码
直接构造或修改。建议将字段改为私有，只通过构造函数和访问器暴露。

### 1.4 [LOW] 缺少 workspace crate license 字段

6 个 workspace crate 的 `Cargo.toml` 中没有 `license` 字段，导致 `cargo deny check
licenses` 失败。需要决定项目 license 后在 `[workspace.package]` 中统一添加。

### 1.5 [LOW] yawc WebSocket 库审计

`Cargo.toml:30` 使用 `yawc = "0.3.3"`，这不是主流 WebSocket 库（相比
`tokio-tungstenite`），下载量和社区审计记录有限。WebSocket 传输层是安全关键路径
（承载认证 token 和交易指令）。建议定期运行 `cargo audit` 确认无已知 CVE。

---

## 二、测试覆盖

### 2.1 总体覆盖率不足

约 88 个测试覆盖 2098 个函数，远低于 80% 目标。

### 2.2 缺少测试的关键模块（按优先级排序）

| 优先级 | 模块 | 风险说明 |
|--------|------|----------|
| P0 | `tqsdk-core/src/order_lifecycle.rs` | 订单生命周期状态机，无专项测试 |
| P0 | `tqsdk-task/src/host.rs` | TaskHost 核心，无测试 |
| P0 | `tqsdk-task/src/strategy.rs` | 策略执行核心，无测试 |
| P0 | `tqsdk-wait/src/driver.rs` | WaitDriver 驱动层，无测试 |
| P1 | `tqsdk-core/src/aggregation.rs` | 聚合逻辑，无测试 |
| P1 | `tqsdk-core/src/state/changes.rs` | 状态变更追踪，无测试 |
| P1 | `tqsdk-core/src/state/domain.rs` | 域分区逻辑，无测试 |
| P1 | `tqsdk-core/src/state/path.rs` | 路径解析，无测试 |
| P1 | `tqsdk-core/src/state/read.rs` | 状态读取，无测试 |
| P1 | `tqsdk-task/src/execution_group.rs` | 执行组，无测试 |
| P1 | `tqsdk-task/src/deployment.rs` | 部署逻辑，无测试 |
| P2 | `tqsdk-wait/src/change.rs` | 变更检测，无测试 |
| P2 | `tqsdk-wait/src/views/kline_window.rs` | K线窗口视图，无测试 |
| P2 | `tqsdk-wait/src/views/tick_window.rs` | Tick窗口视图，无测试 |
| P2 | `tqsdk-session/src/metadata_helpers.rs` | 元数据辅助，无测试 |
| P2 | `tqsdk-session/src/services_helpers.rs` | 服务辅助，无测试 |
| P2 | `tqsdk-task/src/shared.rs` | 共享状态，无测试 |

### 2.3 测试基础设施利用不足

`tqsdk-task/src/testing.rs` 提供了完整的 `FakeMarket`/`FakeBroker`/`StrategyTestHarness`
基础设施，但目前只被 `tqsdk-task/tests/` 下的少数测试使用。其他 crate 没有等价的
fake 层。

建议：
- 为 `tqsdk-core` 添加 `TestRuntimeBuilder` 辅助
- 为 `tqsdk-wait` 添加 `TestTqApi` 辅助
- 扩展 `StrategyTestHarness` 的使用范围

---

## 三、代码质量 — 大文件拆分

### 3.1 [HIGH] `crates/tqsdk-data/src/client.rs`（2886 行）

**Kline/Tick 对称重复**（部分已修复，以下为剩余项）：

- `KlineDataPage`（第 181-284 行）与 `TickDataPage`（第 393-490 行）结构完全相同，
  只有泛型参数不同。可以用泛型 `DataPage<R>` 合并。
- `KlineDataPageRequest`（第 57-177 行）与 `TickDataPageRequest`（第 287-391 行）的
  builder 方法和 `validate()` 逻辑高度重复。
- `read_ready_kline_data_page`（第 1569-1630 行）与 `read_ready_tick_data_page`
  （第 1632-1683 行）结构相同。

**过长函数**：

- `query_his_cont_quotes`（第 1030-1102 行，72 行）— 混合参数验证、日期计算、HTTP
  请求、数据对齐。建议拆分为 `compute_lookback_range` + `build_cont_quotes_rows`。
- `trading_days`（第 1378-1440 行，62 行）— 混合 HTTP 获取、JSON 解析、假日集合构建、
  日期迭代。
- `fetch_continuous_updates`（第 1442-1492 行，50 行）— 嵌套循环解析 JSON，嵌套深度
  达到 4 层。

**拆分建议**：

| 新文件 | 内容 |
|--------|------|
| `page_types.rs` | `KlineDataPage`、`TickDataPage`（可用泛型合并） |
| `request_types.rs` | `KlineDataPageRequest`、`TickDataPageRequest` 及 Series 变体 |
| `chart_reader.rs` | `read_ready_kline_data_page`、`read_ready_tick_data_page`、`wait_for_ready_chart` |
| `cont_quotes.rs` | `query_his_cont_quotes`、`trading_days`、`fetch_continuous_updates` |
| `client.rs` | 仅保留 `DataClient` 结构体和核心方法（约 300 行） |

### 3.2 [HIGH] `crates/tqsdk-core/src/session_runtime.rs`（1481 行）

**`derive_trade_*_command_status` 重复**：

6 个函数（第 940-1191 行）结构高度相似：

- `derive_trade_login_command_status`
- `derive_trade_account_info_command_status`
- `derive_trade_pre_insert_order_command_status`
- `derive_trade_risk_management_rule_command_status`
- `derive_trade_settlement_query_command_status`
- `derive_trade_order_command_status`

每个函数都是：提取 `account_id` -> 检查 `commit_touches_path` -> 读取 snapshot ->
构建 `extra_detail` -> 返回 `CommandStatus::Completed`。可以用一个通用的
`derive_path_completion_status` 辅助函数加配置表替代，减少约 200 行。

**过长函数**：

- `recover_with_policy`（第 530-622 行，92 行）— 包含重试循环、退避计算、错误记录、
  阶段转换。建议拆分为 `attempt_recovery_once` + `record_reconnect_attempt`。

**`drive_timer_once` 中的重复 match**（第 319-392 行）：

`timer.label` 被匹配了两次，第一次只做验证，第二次才处理。可以合并为一次匹配。

**`command_detail_fields_from_dispatch`（第 1306-1372 行，66 行）**：

通过 JSON 字符串解析提取命令详情字段，依赖运行时 JSON 解析而非类型系统，较脆弱。

**拆分建议**：

| 新文件 | 内容 |
|--------|------|
| `session_runtime/command_status.rs` | 所有 `derive_*_command_status` 函数 |
| `session_runtime/reconnect.rs` | `recover_with_policy`、`recover_internal`、`reconnect_backoff_ms` |
| `session_runtime/transport.rs` | `handle_disconnect`、`handle_transport_signal`、`handle_transport_error` |
| `session_runtime.rs` | 保留核心编排逻辑 |

### 3.3 [HIGH] `crates/tqsdk-task/src/target_pos.rs`（1410 行）

**`process_wait_update`（第 338-419 行，81 行）**：

包含取消流程、目标检查、行情订阅、仓位计算、订单处理五个阶段，嵌套深度达到 4 层。
建议提取 `handle_cancel_flow`、`compute_desired_batch`、`check_target_reached` 三个
私有方法。

**`cancel_pending_orders_filtered`（第 660-698 行）**：

`should_cancel` 的双重检查模式可以用 `HashSet::insert` 的返回值直接简化：

```rust
// 当前
let should_cancel = self.with_state_mut(|state| {
    if state.cancel_requested_order_ids.contains(&order_id) {
        false
    } else {
        state.cancel_requested_order_ids.insert(order_id.clone());
        true
    }
});

// 可简化为
let should_cancel = self.with_state_mut(|state| {
    state.cancel_requested_order_ids.insert(order_id.clone())
});
```

**`Drop` 与 `finish` 重复**（第 764-790 行 vs 第 814-827 行）：

`Drop::drop()` 可以直接调用 `self.finish()`。

**`prune_terminal_orders` 重复调用**：

`has_live_orders`、`live_orders`、`unmaterialized_order_ids`、
`cancel_pending_orders_filtered` 都在开头调用 `prune_terminal_orders`，一次
`handle_live_orders` 调用链中可能被调用 3-4 次。建议在 `process_wait_update` 入口
调用一次，后续方法不再重复调用。

### 3.4 [HIGH] `crates/tqsdk-data/src/download.rs`（1180 行）

**`KlineDataDownloadInner` 与 `TickDataDownloadInner` 完全对称重复**：

两个 `Inner` 类型（第 242-363 行 vs 第 365-482 行）结构完全相同，`next_page` 方法
逻辑几乎一字不差。差异仅在于 `KlineDataDownloadInner` 有 `duration` 字段和使用
`KlineDataPage`/`TickDataPage` 不同类型。

同样，`KlineDataDownloadPage` 和 `TickDataDownloadPage`（第 91-192 行）也是完全对称
的重复。

可以用一个泛型 `DataDownloadInner<S, Row, Page, Spec>` 类型消除这些重复。但由于
`KlineDataDownloadPage` 和 `TickDataDownloadPage` 是 public API，直接合并为泛型会
破坏 API。建议用内部泛型 + public type alias 的方式：

```rust
// 内部泛型
struct DataDownloadPageInner<R> { rows: Vec<R>, progress: DataDownloadProgress }

// public alias 保持 API 兼容
pub type KlineDataDownloadPage = DataDownloadPageInner<Kline>;
pub type TickDataDownloadPage = DataDownloadPageInner<Tick>;
```

**`validate_kline_download_request` 与 `validate_tick_download_request` 重复**：

第 653-698 行的两个验证函数共享相同的 `symbol.is_empty()` 和 `end <= start` 检查，
只有 `duration_ns` 验证是 kline 独有的。

### 3.5 [MEDIUM] `crates/tqsdk-session/src/client/io.rs`

**`drive_route_label_once` 与 `drive_route_once_locked` 重复**：

两个函数（第 132-192 行 vs 第 255-305 行）几乎完全相同：都构建 `SessionRuntimeDeps`，
都调用 `self.runtime.drive_route_once`，都用相同的 `timeout` 模式处理 deadline。
可以提取为 `drive_route_with_deadline` 私有辅助函数。

**`drive_pending_route_label_once` 与 `drive_pending_once_locked` 重复**：

第 53-93 行和第 215-253 行都执行相同的 executor 选择逻辑。

### 3.6 [MEDIUM] `require_history_download_permission` 同步/异步重复

`crates/tqsdk-data/src/client.rs` 第 974-995 行和第 997-1028 行实现了相同的权限检查
逻辑，但一个是同步的，一个是异步的。`tq_dl` 特性检查逻辑可以合并。

---

## 四、已修复项（供参考）

以下问题已在本次审查中修复：

| 问题 | 修复 | 位置 |
|------|------|------|
| AuthContext Debug 泄漏 access_token | 手动实现 Debug | `tqsdk-core/src/auth.rs` |
| TradeLoginCommand Debug 泄漏 password | 手动实现 Debug | `tqsdk-core/src/commands.rs` |
| TradeCommand::Transfer Debug 泄漏 bank_password/future_password | 手动实现 Debug | `tqsdk-core/src/commands.rs` |
| DiffLoginRequest Debug 泄漏 password | 手动实现 Debug | `tqsdk-core/src/diff_protocol/outbound.rs` |
| DiffTransferRequest Debug 泄漏 bank_password/future_password | 手动实现 Debug | `tqsdk-core/src/diff_protocol/outbound.rs` |
| PasswordCredentials Debug 泄漏 password | 手动实现 Debug | `tqsdk-session/src/tq_auth.rs` |
| record_session_failure 中 commit 丢弃意图不明确 | `if let Some(_commit)` 改为 `let _ =` | `tqsdk-core/src/session_runtime.rs` |
| rustls-webpki RUSTSEC-2026-0104 漏洞 | 升级到 0.103.13 | `Cargo.lock` |
| dedup_sort_klines/ticks_by_id 重复 | 泛型 `dedup_sort_rows_by_id` | `tqsdk-data/src/client.rs` |
| extend_kline/tick_rows_in_window 重复 | 泛型 `extend_rows_in_window` | `tqsdk-data/src/client.rs` |
| normalize_history_view_width 重复定义 | 提升为 `pub(crate)` 并引用 | `tqsdk-data/src/download.rs` |
| 新增 cargo-deny 配置 | `deny.toml` | 项目根目录 |
