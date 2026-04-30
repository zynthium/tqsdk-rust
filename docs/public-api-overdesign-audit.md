# Public API 过度设计与代码冗余审计

> 审计日期: 2026-04-29
> 审计范围: 全部 6 个 crate 的 `lib.rs` 导出、commands/auth/outbound 模块
> 基准 commit: 5f2878d

---

## 概览

| Crate | Public 符号数 | 评估 | 主要问题 |
|-------|-------------|------|---------|
| `tqsdk-core` | **153** | 过多 | 运行时内部类型泄漏到 public API |
| `tqsdk-task` | **100** | 过多 | Strategy/Report/Event 类型爆炸 |
| `tqsdk-stream` | **74** | 偏多 | Sink/WAL/Journal 内部类型暴露 |
| `tqsdk-data` | **67** | 偏多 | MarketCache 子系统独占 44 个符号 |
| `tqsdk-wait` | 29 | 合理 | — |
| `tqsdk-session` | 27 | 合理 | — |

**总计 450 个 public 符号**。对于一个量化交易 SDK，用户真正需要的入口类型不超过 80 个。

---

## 修复闭环状态（2026-04-29）

| 审查项 | 状态 | 结论 / 证据 |
| --- | --- | --- |
| P0 `tqsdk-core` public API 表面积过大 | `done` + `won't do` | 已完成安全收口：`AuthContext` 字段私有化（`418f7ee`）、aggregation root exports 与 `OutboundEnvelope` 收口（`1556e93`）、core public surface 文档同步（`33e1df5`）。`RuntimeInput`、`NormalizedMutation`、`SnapshotReadGuard`、`CommitLog`、`OutboundRequest`、`OutboundDispatch` 等是架构保护的 runtime/adapter contract，按 `docs/public-api-disposition-matrix.md` 保留，不按原审查建议下收。 |
| P1 `tqsdk-data` MarketCache 类型爆炸 | `blocked by architecture decision` | S18 examples、`api-data.md`、`tqsdk-data/README.md`、`public-api-scenario-review.md` 仍将 manifest/recovery/election/queue/lock/index/compaction/service/daemon/supervisor 类型作为 public scenario contract。Task 7 triage 判定本批不改 re-export，需 future S18 cache API redesign。 |
| P2 `tqsdk-stream` Sink/WAL 内部类型泄漏 | `blocked by architecture decision` | S21 slow-consumer example、`api-stream.md`、`tqsdk-stream/README.md` 和 stream tests 仍直接使用 `StreamSinkWal*` / `StreamCommitJournal*`。Task 7 triage 判定需 future S21 durability API redesign。 |
| P3 `tqsdk-task` Strategy/Report 类型膨胀 | `blocked by architecture decision` | S15/S20 docs/examples 仍使用 `StrategyShutdownSignal`、supervisor health、telemetry concepts；S12/S13 docs 暴露 `ExecutionGroupStatus` / `MultiAccountOrderStatus` 为 status return types。Task 7 triage 判定需 future task API compatibility plans。 |
| P4 `TradePreInsertOrderCommand` 字段重复 | `moved to breaking-change batch` | Task 6 Step 3 已评估：adapter 侧已复用内部 `DiffOrderRequest`，改 public struct literal 会破坏现有构造 ergonomics；本批不做，未来如需组合化必须先写独立兼容性计划。 |
| P5 `CLIENT_SECRET` 硬编码 | `done` + `won't do` | 已添加注释说明这是 ShinnyTech public OAuth2 client identifier（`3109ff3`）。不改 builder 注入，原因是它不是用户凭据，且当前计划未引入凭据轮换需求。 |
| P6 `serde(skip)` 缺少意图注释 | `done` | 已添加保留协议字段防覆盖注释和 guardrail（`3109ff3`）。 |

---

## 问题清单

### P0 — tqsdk-core public API 表面积过大

**严重程度**: 高
**文件**: `crates/tqsdk-core/src/lib.rs`

core 作为"协议完整运行时基底"，导出了 153 个符号。以下类别的类型属于内部实现细节，不应出现在 public API 中：

**运行时内部类型（建议降为 `pub(crate)` 或移入 `internal`）**:
- `CommitLog`, `CursorLagged`, `OutboundEnvelope`, `SnapshotReadGuard`, `CommitReadGuard`
- `AggregatedCommit`, `AggregatedCursor`, `AggregatedRuntimeReader`, `AggregatedSnapshotReadGuard`, `StateSourceId`
- `FieldMutation`, `NormalizedMutation`, `MutationSource`, `InputPayload`, `RuntimeInput`
- `IoEvent`, `TimerEvent`, `InternalEvent`, `ReplayEvent`, `AuthEvent`

**协议编码类型（用户不直接使用）**:
- `OutboundFrame`, `OutboundRequest`, `OutboundDispatch`, `CommandEnvelope`, `CausationMeta`
- `HttpMethod`, `HttpRequest`, `ReplayRequest`, `InternalRequest`, `QueryRequest`

**建议**:
1. 将上述类型移入已有的 `internal` 模块，或降为 `pub(crate)`
2. 只保留用户直接交互的类型：`RuntimeCommand` 变体、`CommandStatus`、domain 类型（`Account`, `Order`, `Quote` 等）、ID 类型、错误类型
3. 目标：将 core 的 public 符号从 153 降到 ~60

---

### P1 — tqsdk-data MarketCache 类型爆炸

**严重程度**: 高
**文件**: `crates/tqsdk-data/src/lib.rs`, `crates/tqsdk-data/src/market_cache/`

MarketCache 子系统导出了 **44 个** public 类型。按子功能分布：

| 子功能 | 类型数 | 类型列表 |
|--------|--------|---------|
| Writer/Election | 6 | `MarketCacheWriter`, `MarketCacheWriterElection`, `MarketCacheWriterElectionOutcome`, `MarketCacheWriterElectionReport`, `MarketCacheWriterElectionStatus`, `MarketCacheWriterLease` |
| Recovery | 6 | `MarketCacheRecoveryScan`, `MarketCacheRecoveryReport`, `MarketCacheRecoveryAction`, `MarketCacheRecoveryActionReport`, `MarketCacheRecoveryFileKind`, `MarketCacheRecoveryFileReport` |
| Compaction | 5 | `MarketCacheCompaction`, `MarketCacheCompactionOwnership`, `MarketCacheCompactionOwnershipReport`, `MarketCacheCompactionReport`, `MarketCacheAtomicCompactionReport` |
| Reader | 4 | `MarketCacheReader`, `MarketCacheReaderCheckpoint`, `MarketCacheReaderLag`, `MarketCacheReaderManifest` |
| Queue/Lock/Index | 6 | `MarketCacheQueue`, `MarketCacheQueueDrainError`, `MarketCacheQueueDrainReport`, `MarketCacheLock`, `MarketCacheLockOptions`, `MarketCacheIndex`, `MarketCacheIndexEntry`, `MarketCacheIndexKey` |
| Service/Daemon/Supervisor | 9 | `MarketCacheService`, `MarketCacheServiceConfig`, `MarketCacheServiceOpen`, `MarketCacheServiceOpenReport`, `MarketCacheServiceShutdownReport`, `MarketCacheDaemon`, `MarketCacheDaemonConfig`, `MarketCacheDaemonShutdownReport`, `MarketCacheSupervisor`, `MarketCacheSupervisorConfig`, `MarketCacheSupervisorShutdownReport` |
| Payload/Event/Replay | 4 | `MarketCachePayload`, `MarketCachePayloadKind`, `MarketCacheEvent`, `MarketCacheReplay` |

**建议**:
1. 用户入口只需要: `MarketCacheWriter`, `MarketCacheReader`, `MarketCacheReplay`, `MarketCacheService`, `MarketCacheDaemon`, `MarketCacheSupervisor` 及其 Config
2. 将 Election/Recovery/Compaction 的细节类型降为 `pub(crate)`，通过顶层类型的方法返回值暴露
3. 合并 Report 类型为嵌套结构，例如 `MarketCacheService::open()` 返回 `OpenReport` 而非独立的 `MarketCacheServiceOpenReport`
4. 目标：从 44 降到 ~15

---

### P2 — tqsdk-stream Sink/WAL 内部类型泄漏

**严重程度**: 中
**文件**: `crates/tqsdk-stream/src/lib.rs`, `crates/tqsdk-stream/src/sink.rs`

`sink` 模块导出了 18 个类型，其中 WAL 和 Journal 的内部实现不应暴露：

**应降为 `pub(crate)` 的类型**:
- `StreamSinkWalCompaction`, `StreamSinkWalCompactionReport`
- `StreamSinkWalFsyncPolicy`, `StreamSinkWalRecord`, `StreamSinkWalRecordKind`
- `StreamSinkWalRecovery`, `StreamSinkWalRecoveryReport`
- `StreamCommitJournal`, `StreamCommitJournalDomain`, `StreamCommitJournalRecord`
- `StreamCommitJournalReplayReport`, `StreamCommitJournalScope`

**应保留为 public 的类型**:
- `CommitSink`, `StreamSinkOptions`, `StreamSinkHandle`, `StreamSinkProfile`
- `StreamSinkStatus`, `StreamSinkStats`

**建议**: 将 WAL/Journal 细节类型降为 `pub(crate)`，目标从 18 降到 ~6

---

### P3 — tqsdk-task Strategy/Report 类型膨胀

**严重程度**: 中
**文件**: `crates/tqsdk-task/src/lib.rs`

`deployment` 模块导出 17 个 `Strategy*` 类型，`account_group` 导出 13 个 `MultiAccountOrder*` 类型，`execution_group` 导出 11 个 `Execution*` 类型。

**可合并的类型对**:
- `StrategySupervisorHealth` + `StrategySupervisorHealthStatus` → 合并为一个 enum
- `StrategyRunReport` + `StrategyRunStopReason` → `StopReason` 作为 `RunReport` 的字段
- `StrategyShutdownReport` + `StrategyShutdownSignal` → `Signal` 作为 `ShutdownReport` 的字段
- `StrategyTelemetryEvent` + `StrategyTelemetryEventKind` → `Kind` 作为 `Event` 的内联 enum
- `MultiAccountOrderState` + `MultiAccountOrderStatus` → 合并

**建议**:
1. 将 Report 中的 Reason/Signal/Kind 作为关联类型或嵌套 enum，而非独立顶层类型
2. 目标：从 100 降到 ~60

---

### P4 — commands.rs InsertOrder/PreInsertOrder 字段重复

**严重程度**: 低
**文件**: `crates/tqsdk-core/src/commands.rs:276-303`

`TradeInsertOrderCommand` 和 `TradePreInsertOrderCommand` 共享 10 个相同字段：

```
account_id, order_id, symbol, direction, offset,
volume, price_type, limit_price, time_condition, volume_condition
```

`PreInsertOrder` 仅多出 `hedge_flag` 和 `contingent_condition`。

`outbound.rs` 中已经用组合方式处理了这个问题（`DiffPreInsertOrderRequest` 包含 `DiffOrderRequest`），但 `commands.rs` 没有对齐。

**建议**: 让 `TradePreInsertOrderCommand` 包含 `TradeInsertOrderCommand` 加额外字段：

```rust
pub struct TradePreInsertOrderCommand {
    pub order: TradeInsertOrderCommand,
    pub hedge_flag: String,
    pub contingent_condition: String,
}
```

---

### P5 — 硬编码 OAuth 凭据

**严重程度**: 中（安全）
**文件**: `crates/tqsdk-session/src/tq_auth.rs:22-23`

```rust
const CLIENT_ID: &str = "shinny_tq";
const CLIENT_SECRET: &str = "be30b9f4-6862-488a-99ad-21bde0400081";
```

这是天勤平台的公开 OAuth client credentials，Python SDK 中也有同样的硬编码。虽然是公开值，但作为 Rust SDK 的最佳实践，建议通过 `TqAuthProvider` 的 builder 注入，硬编码值作为默认值。此外，常量上方应添加注释说明这是公开的 OAuth2 客户端标识而非用户凭据，避免未来维护者误判。

---

### P6 — outbound.rs SetRiskManagementRule 的 serde(skip) 缺少意图注释

**严重程度**: 低
**文件**: `crates/tqsdk-core/src/diff_protocol/outbound.rs:99-104`

```rust
#[serde(rename = "set_risk_management_rule")]
SetRiskManagementRule {
    user_id: String,
    #[serde(skip)]
    rule: Map<String, Value>,
},
```

`rule` 字段用 `#[serde(skip)]` 跳过序列化，然后在 `into_value()` 中手动合并。这是为了防止用户通过 `rule` 注入 `aid` 或 `user_id` 等保留协议字段（测试 `risk_rule_message_preserves_reserved_protocol_fields` 验证了这一点）。设计意图正确，但对维护者不够直观。

**建议**: 在 `#[serde(skip)]` 上方添加注释：

```rust
// rule is merged manually in into_value() to prevent user-supplied keys
// from overriding reserved protocol fields (aid, user_id).
#[serde(skip)]
rule: Map<String, Value>,
```

---

## 实施路线

### 第一阶段：收窄 core（影响最大）

1. 将 `tqsdk-core` 中的运行时内部类型移入 `internal` 模块
2. 将协议编码类型降为 `pub(crate)`
3. 更新依赖 crate 的 import 路径（使用 `tqsdk_core::internal::*`）
4. 预期：153 → ~60 个 public 符号

### 第二阶段：收窄 data 和 stream

1. 将 MarketCache 内部类型降为 `pub(crate)`
2. 将 Sink/WAL/Journal 内部类型降为 `pub(crate)`
3. 预期：data 67 → ~25，stream 74 → ~50

### 第三阶段：精简 task

1. 合并 Report/Status/Reason 类型对
2. 将 execution_group 和 account_group 的内部状态类型降为 `pub(crate)`
3. 预期：100 → ~60

### 第四阶段：小修

1. 统一 commands.rs 的 InsertOrder/PreInsertOrder 复用
2. 将 OAuth 凭据改为可配置默认值

**总目标**: 450 个 public 符号 → ~220 个（削减 ~50%）

---

## 不属于过度设计的部分

以下经审查认为设计合理，不需要修改：

- `auth.rs` 中的 `AuthProvider` + `DynAuthProvider` 双 trait 模式：这是 Rust 中 RPITIT 无法直接 `dyn` 的标准解法，`DynAuthProvider` 已标记 `#[doc(hidden)]`
- `commands.rs` 中的 `RuntimeCommand` enum 及其子 enum：变体数量与天勤 DIFF 协议的实际命令一一对应，没有多余抽象
- `outbound.rs` 的 `DiffProtocolMessage`：`pub(crate)` 可见性正确，内部结构与协议报文对齐
- `tqsdk-wait` 和 `tqsdk-session` 的 API 表面积：分别 29 和 27 个符号，精简合理
- `TradeCommand` 的手动 `Debug` impl：正确地 REDACT 了密码字段
