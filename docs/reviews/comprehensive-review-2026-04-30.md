# 全面审查报告 — 2026-04-30

审查范围：架构设计、Public API、性能、可读性与可维护性
构建状态：`cargo check` PASS, `cargo clippy -D warnings` PASS, `cargo test` PASS (44 tests), `cargo fmt --check` FAIL (9 files)

---

## 2026-05-01 接续执行状态

本节记录 Codex 接手后的复核和落地状态。原始审查表保留为问题来源；后续计划应优先读取本节、当前代码和 `docs/architecture/*`，不要把下方原始表格中的旧状态当作未复核事实。

### 已修复并提交到 checkpoint

- `order_is_terminal()` 已改用 `OrderLifecycle::is_terminal()`，避免字符串状态误判。
- `ChangeSet::from_mutations` 已用集合去重，避免 O(N²) 热路径退化。
- `CommitLog` 已改为读写锁，降低多 consumer 读取竞争。
- `market_cache.rs` 已拆分为模块目录。
- `TargetPosConfig` / `VolumeSplitPolicy` 已收口字段和验证构造路径。
- `run_driver` 已改用 `Notify` 唤醒，移除 1ms idle polling。
- `insert_order` 已引入 `OrderPrice` typed 边界，并保留 legacy 兼容桥。
- ID 类型、`OrderLifecycle`、`RiskRejection` 已补充 `Display`。

### 本批代码已落地，随当前批次验证和提交

- `tqsdk_core::internal` 已补充 `#[doc(hidden)]` sibling-crate bridge 稳定性声明，并用 runtime contract surface test 固化。
- wait/stream 的 kline/tick/quote 热路径已改用 `read_market_state()`；generic path stream、health/system event 仍允许使用 full snapshot。
- `Order` / `Trade` / `Account` / `Position` / `PreInsertOrder` 已实现可处理 schema 默认 `NaN` 的 `PartialEq`。
- `Order.volume_orign` / `SecurityOrder.volume_orign` public 字段已改为 `volume_origin`，serde 仍兼容协议字段 `"volume_orign"`。
- `StreamRetryPolicy::max_attempts` 已恢复链式 builder 语义，严格校验路径保留为 `try_max_attempts`。
- `StreamSinkHandle` 共享状态已封装为 `SharedStreamSinkState`，调用点不再传递裸 `Arc<Mutex<StreamSinkState>>`。
- `MarketCacheQueue::enqueue_event` 已改为复用持久 writer；queue 读取、清理和 rotating drain 会先 flush writer，rotating drain 后重开 append writer。
- `run_market_cache_supervisor` 续租错误复核结果：当前代码已计入 `periodic_errors` 并写入 report，不再是静默吞错。

### 2026-05-01 后续批次已落地

- `record_command_status` fallback 已从全量 `StateStore::snapshot()` 改为只借读 runtime 分区；`StateStore::read_partition(root)` / `read_runtime_state()` 作为内部读面支撑该路径。
- `normalize_order_lifecycle_mutations` 已减少订单状态 overlay 的 `serde_json::Value` clone；后续 typed state migration 前不再扩大该路径改造范围。
- `StreamSinkWalWriter` / `StreamCommitJournalWriter` 已抽出共享 `JsonlRecordWriter`，保留 WAL 和 commit journal 的语义 wrapper。
- `MarketCacheService` / `MarketCacheDaemon` / `MarketCacheSupervisor` 及其关键配置、报告、方法已补公开文档注释。
- 6 个 crate 的 crate-level docs 已补最小可编译 doctest；后续仍应继续补 public API 的细粒度示例。
- `RiskEngine` 已补浮点 tick 对齐和净持仓投影的 property-style 边界测试，不引入额外测试依赖。
- `TqApi::new_for_test` 已收为私有构造路径；测试侧改用普通 `TqApi::new(...)` 配合 `tqsdk_session::testing::ManualSession`，减少 hidden public surface。
- `TqStream::new_for_test_with_capacity` 已替换为正式 `TqStream::with_commit_channel_capacity`，容量配置归属 stream facade，不再通过 hidden test 构造器暴露。
- `DataClient::new_for_test_with_urls` 已收为 `#[cfg(test)]` 私有 helper；服务 URL 覆盖不再作为 hidden public API 暴露。
- `DynAuthProvider` 已从 `tqsdk_core` root public re-export 收回到 `tqsdk_core::internal` sibling bridge，`AuthContext` / `AuthProvider` 保持 root contract。
- `TaskHost` 隐藏 ownership 测试 hook 已收口：`check_manual_order_allowed_for_test()` 改为正式 `check_manual_order_allowed()` dry-run API，未使用的 owner register/unregister hidden hooks 已删除，测试改用真实 scheduler builder 覆盖冲突路径。
- `TargetPosTask::applied_target_volume_for_test()` 已删除，`applied_target_volume()` 成为正式公开观测 API 并补文档。
- `tqsdk-session` 已新增 `testing::ManualSession` 作为明确的 no-IO/manual 测试入口；session、wait、stream、task、data 的手动 session 构造调用已迁移，`SessionClient::new_for_test_with_handle()` hidden public 构造器已删除。`drain_dispatches()` 暂时保留，等待 wait/stream/task dispatch fixture 迁移。
- `tqsdk-stream` 已删除 `TqStream::handle_for_test()`；stream 测试 support 改用公开 `stream.session().handle()`。`tqsdk-wait` 自身测试也已迁离 `TqApi::handle_for_test()`，但该 shim 暂留给 task fixture 迁移。

### 仍保留为独立计划项

- `_for_test` feature-gating 不能直接机械改：`tqsdk-task::testing` 和多个 integration contract 仍依赖测试 runtime 注入。TaskHost ownership 与 TargetPos duplicate observer 已收口；剩余主要是 session/wait/stream manual test-driver 与 task fixture 的 runtime ingest/dispatch 控制，需要先设计 stable fake harness 注入面，再收缩 hidden runtime handle。
- `Order.direction` / `offset` / `price_type` 从 `String` 迁移到枚举是 source-breaking schema API 改造，需要单独 public API 迁移计划。
- 全局 `serde_json::Value` 状态树 typed migration 属于 runtime contract 长期演进，不应混入本批修复。
- `apply_and_publish_locked` 的 `CommitResult` clone 受当前 public `CommitResult` 返回值和 commit log 持有所有权约束；若要彻底消除，需要单独评估 `Arc<CommitResult>` 或 cursor API contract。
- `transport.rs`、`account_group.rs`、`sink.rs` 模块级拆分属于较大内部重构，应分 child plan 执行并先补 characterization tests。
- public 文档注释仍是质量补强任务，不影响本批已定位 bug/perf 修复。

---

## 一、架构设计 — WARNING

6 个 crate 分层清晰，依赖 DAG 无环，四项 runtime 不变量全部代码级强制执行。

| 严重度 | 发现 | 位置 |
|--------|------|------|
| MEDIUM | `_for_test` 方法用 `#[doc(hidden)]` 而非 feature flag，下游可调用 `handle_for_test()` 绕过 session 层 | 多个 crate |
| MEDIUM | `tqsdk-stream`/`tqsdk-wait` 多处用 `reader.read()` 全量快照读 kline/tick/quote，应改用 `read_market_state()` | `stream/typed.rs:62`, `stream/window.rs:197`, `wait/refs/kline.rs:16` |
| MEDIUM | `cargo fmt --check` 在 9 个文件上失败 | `tqsdk-core`, `tqsdk-data` |
| LOW | `tqsdk_core::internal` 模块缺少稳定性声明 | `tqsdk-core/src/lib.rs` |
| LOW | `tqsdk-data` 的 `stream` feature 与 `tqsdk-stream` 横向耦合（已正确 feature-gate） | `tqsdk-data/Cargo.toml` |
| LOW | `tqsdk-task::testing` 通过 `handle_for_test()` 紧耦合 `RuntimeHandle::ingest` | `tqsdk-task/src/testing.rs` |

---

## 二、Public API — BLOCK

| 严重度 | 发现 | 位置 |
|--------|------|------|
| **CRITICAL** | `order_is_terminal()` 用字符串 `"FINISHED"` 判断终态，忽略 `OrderLifecycle::is_terminal()`。不同经纪商 status 字符串不同，会导致订单误判、重复撤单或任务挂起 | `tqsdk-task/src/target_pos.rs:865` |
| HIGH | `insert_order` 的 `limit_price: Option<Value>` 接受魔法字符串，无类型安全（已于 2026-05-01 改为 `OrderPrice` typed 边界，并保留 `TaskHost::insert_order_guarded` 的 legacy 兼容桥接） | `tqsdk-wait/src/api.rs:352` |
| HIGH | `market_target(bool, bool)` 布尔陷阱，已有命名快捷方法覆盖所有组合 | `tqsdk-session/src/builder.rs:79` |
| HIGH | `TargetPosConfig` 所有字段 `pub`，绕过 builder 验证。`VolumeSplitPolicy::validate()` 是 `pub(crate)` | `tqsdk-task/src/config.rs:57` |
| HIGH | `RiskRejection` 无 `Display`，`TaskError` 回退到 `Debug` 格式 | `tqsdk-task/src/risk.rs:119` |
| HIGH | 20+ 个 `#[doc(hidden)]` public 项属于稳定 ABI | 多个 crate |
| MEDIUM | `Symbol`/`AccountId`/`OrderId` 等 ID 类型缺少 `Display` | `tqsdk-core/src/ids.rs` |
| MEDIUM | `volume_orign` 公开字段拼写错误 | `tqsdk-core/src/types/trading.rs:153` |
| MEDIUM | `Order`/`Trade`/`Account`/`Position` 缺少 `PartialEq` | `tqsdk-core/src/types/trading.rs` |
| MEDIUM | `Order.direction`/`offset`/`price_type` 是 `String`，已有对应枚举未使用 | `tqsdk-core/src/types/trading.rs` |
| MEDIUM | `StreamRetryPolicy::max_attempts` 返回 `Result<Self>` 破坏 builder 链式调用 | `tqsdk-stream/src/error.rs:137` |
| MEDIUM | ~80 个 public 项缺少 `///` 文档注释 | 全部 crate |

---

## 三、性能 — 需关注

| 严重度 | 发现 | 位置 |
|--------|------|------|
| HIGH | `RuntimeHandle` 单一 `Mutex<RuntimeCore>`，`record_command_status` 持锁期间获取 12 个 `RwLock` 读锁 | `runtime/handle.rs:188-199` |
| HIGH | `CommitLog` 用 `Mutex` 而非 `RwLock`，多 consumer 读与 publish 写竞争 | `runtime/commit_log.rs:21-82` |
| HIGH | `ChangeSet::from_mutations` 用 `Vec::contains` O(N²) 去重，每次 commit 执行 | `state/changes.rs:37-64` |
| HIGH | `StateStore::snapshot()` 克隆全部 12 分区，`normalize_order_lifecycle_mutations` 只需 `trade` 分区 | `state/store.rs:134-155` |
| HIGH | `StreamSinkHandle` 每次 commit 投递 7 次独立 Mutex lock/unlock | `sink.rs:937-987` |
| HIGH | `run_driver` 用 1ms sleep 轮询，空闲 1000 次/秒唤醒 | `stream/driver.rs:121-143` |
| HIGH | `MarketCacheQueue::enqueue_event` 每次事件 open/write/flush/close 文件 | `market_cache.rs:1153-1163` |
| MEDIUM | `apply_and_publish_locked` 克隆整个 `CommitResult` | `handle.rs:392` |
| MEDIUM | `normalize_order_lifecycle_mutations` 每个交易订单 mutation 两次 `Value` 克隆 | `handle.rs:593-598` |
| MEDIUM | `serde_json::Value` 状态树是读路径最大性能天花板 | 全局 |

---

## 四、可读性与可维护性 — BLOCK

| 严重度 | 发现 | 位置 |
|--------|------|------|
| HIGH | `market_cache.rs` 2430 行混合 7 个职责，必须拆分 | `tqsdk-data/src/market_cache.rs` |
| HIGH | `sink.rs` 10 个自由函数共享 `Arc<Mutex<StreamSinkState>>`，复合操作无原子性 | `tqsdk-stream/src/sink.rs:937-1005` |
| HIGH | `MarketCacheService`/`Daemon`/`Supervisor` 无文档注释 | `market_cache.rs` |
| MEDIUM | `transport.rs` 1135 行混合 4 个职责 | `tqsdk-core/src/transport.rs` |
| MEDIUM | `account_group.rs` 967 行，API 与 helper 混合 | `tqsdk-task/src/account_group.rs` |
| MEDIUM | `sink.rs` 建议拆分为 `sink/` 模块目录 | `tqsdk-stream/src/sink.rs` |
| MEDIUM | `WalWriter`/`JournalWriter` 结构重复，应泛化 | `sink.rs:1253-1353` |
| MEDIUM | `run_market_cache_supervisor` 静默吞掉锁续期错误 | `market_cache.rs:2383-2395` |
| MEDIUM | `scheduler.rs` 测试用源码字符串扫描做架构断言 | `tqsdk-task/tests/scheduler.rs:22-47` |
| LOW | 全部 crate 零 doc-test | 全局 |
| LOW | `RiskEngine` 浮点比较缺少 property-based 测试 | `tqsdk-task/src/risk.rs` |

---

## 五、优先修复计划

### P0 — 立即修复

1. `order_is_terminal` 改用 `order.lifecycle.is_terminal()`
2. `ChangeSet::from_mutations` 改用 `HashSet` 去重

### P1 — 短期修复

3. `StateStore` 增加 `read_partition(root)` 方法
4. `CommitLog` 改用 `RwLock`
5. `market_cache.rs` 拆分为模块目录
6. `TargetPosConfig`/`VolumeSplitPolicy` 字段 private + 验证构造器
7. `run_driver` 改用 `Notify` 信号替代 1ms sleep

### P2 — 中期改进

8. `sink.rs` 共享状态封装为 wrapper
9. `transport.rs` 拆分为模块目录
10. `insert_order` 引入 `OrderPrice` 枚举（已完成，2026-05-01）
11. ID 类型/`OrderLifecycle`/`RiskRejection` 实现 `Display`
12. `reader.read()` 改为分区读
13. `_for_test` 方法改用 feature flag
14. 补充核心 public 类型文档

### P3 — 长期优化

15. 状态树从 `serde_json::Value` 迁移到 typed struct
16. `Order`/`Trade` 字符串字段迁移到类型枚举
17. 引入 property-based 测试覆盖风控浮点边界
