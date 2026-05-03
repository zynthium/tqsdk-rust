# tqsdk-rust 代码级架构审查报告

## 0. 文档定位

- **核对日期**：2026-05-03
- **审查对象**：当前 workspace 代码、架构文档、场景契约示例与 public API 审查记录
- **主要依据**：`README.md`、`ROADMAP.md`、`docs/README.md`、`docs/architecture/*`、`docs/scenarios/*`、`docs/reviews/*`、各 crate `README.md`、当前 `crates/*/examples/api_contract_sXX_*.rs`
- **验证状态**：本次整理只做文档审查与定向核对，未重新运行全仓 Rust 测试矩阵；提交前应至少运行 `git diff --check`

`report.md` 是审查输入和风险备忘，不是架构权威。若本报告与 `docs/architecture/*` 或当前代码冲突，应以架构文档和代码为准，再决定是否把建议转化为新的计划。

## 1. 执行摘要

当前仓库已经从早期骨架进入“核心 SDK foundation 基本闭环，后续以维护边界和发布质量为主”的阶段。

已稳定的分层仍是：

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
tqsdk-wait / tqsdk-stream / tqsdk-data
    ^
    |
tqsdk-task
```

当前审查结论：

- 未发现需要立即启动破坏性 core 重构的 P0 架构风险。
- 旧报告中的状态污染、订单状态回退、`ContractFuture` public alias、core root 暴露 TQ auth/http 实现细节等问题已经被当前代码和架构文档吸收。
- 当前 active API gap 只剩 S14 多 provider 行情聚合；该能力已明确暂缓，不属于近期核心 SDK 目标。
- 后续主要风险不再是“核心能力缺口”，而是文档漂移、public API 膨胀、把平台能力伪装成 SDK 缺口，以及验证矩阵没有持续执行。

当前建议：保持架构边界，继续维护已落地 foundation；不要按旧报告重启大规模 crate 拆分、状态树替换或 facade 合并。

## 2. 当前代码事实

### 2.1 Crate 职责

| Crate | 当前职责 | 审查判断 |
| --- | --- | --- |
| `tqsdk-core` | protocol-complete runtime substrate：命令、状态、commit/revision、cursor、adapter、schema types、底层 session/runtime contract | 边界健康，public surface 仍应保持克制 |
| `tqsdk-session` | shared session、one-shot request/response、direct query、metadata/schema/service query、auth/replay/session control-plane | 边界健康，direct query 归属明确 |
| `tqsdk-wait` | Python 风格 single-owner `wait_update()` facade、live refs、serial/window、wait 风格交易命令 | 边界健康，不应复制 direct query |
| `tqsdk-stream` | async-native multi-consumer commit stream、typed event stream、health/recovery、sink isolation、WAL/journal foundation | 边界健康，慢消费者和生产 primitive 已有最小 foundation |
| `tqsdk-task` | 执行工具层：TargetPos、scheduler、execution/account group、risk、strategy host/supervisor、testing、trading desk thin profile | 边界健康，需避免继续膨胀成策略平台/OMS |
| `tqsdk-data` | research/offline data：history page/series/download、CSV、Greeks、local cache/replay、history series cache | 边界健康，不应回灌 session/wait/stream |

### 2.2 Runtime contract

当前仍保持单一提交源：

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> SharedCommitResult
    -> RuntimeReader / UpdateCursor
```

关键事实：

- 兼容状态树继续存在，用于 DIFF 稀疏对象、generic path、system/query/schema/replay 等路径。
- market/trade 热路径已有 domain partitions 和 typed read surface。
- 同一低延迟决策可通过 `RuntimeReader::read_market_trade_state()` 读取同 revision market/trade 截面。
- `MutationSource` 根路径防线已经是 runtime apply 前的硬约束。
- command/order 状态转换必须走 runtime 状态机；不应使用字符串或 adapter 本地判断绕过。

### 2.3 场景契约状态

当前正式场景契约已经覆盖 S1-S13、S15-S31 的核心 SDK foundation。S14 多 provider 行情聚合仍保留在 `docs/scenarios/api_gaps/`，并被明确标记为暂缓。

这意味着：

- 当前路线不应继续按 Python SDK 方法清单补齐所有 facade。
- 当前 public API 审查重点应从“还能加什么”转为“哪些能力不该继续扩张”。
- 新增能力必须先说明服务哪个用户层、落在哪个 crate、是否维持单一 runtime commit/revision/command lifecycle。

## 3. 已闭环的历史风险

| 历史结论 | 当前状态 | 后续要求 |
| --- | --- | --- |
| P0：状态树为无类型全局树、无领域隔离 | 已过时。当前采用兼容状态树 + market/trade partitions + `MutationSource` 根路径防线 | 不要以“彻底删除兼容树”为目标；只按热点扩 typed view |
| P0：无显式订单状态机 | 已过时。当前已有 command 状态转换校验与 `OrderLifecycle` | 不要绕过 `record_command_status()` 或退回字符串状态判断 |
| P1：`ContractFuture` / boxed future 是 public trait 边界 | 已过时。当前使用 AFIT/RPITIT 风格 async trait 边界，架构禁止恢复 public alias | dyn erased boundary 需要显式、局部、可解释 |
| P1：core root 暴露 TQ auth/http 实现细节 | 已收口。TQ auth/http 具体实现归属 session feature-gated 能力 | 不要重新导出 `TqAuthProvider`、`ReqwestHttpExecutor` 等实现细节 |
| P1：无 feature/no-default 构建基线 | 已闭环。`docs/architecture/validation.md` 固化 feature/no-default matrix | 发布前应持续执行矩阵，不让 README、CI、报告各写一套命令 |
| P2：大量 API gap 仍无法自然表达 | 已大幅收敛。除 S14 外，核心场景已有正式 example 或明确降级 | 不要把已降级平台能力重新解释成 core SDK 缺口 |

## 4. 当前仍需关注的问题

### P1：文档与路线图漂移

- **风险**：根路线图、审查报告、场景计划和架构文档分别描述不同“下一步”，会误导后续 AI session 重启已完成工作。
- **处理原则**：
  - `docs/architecture/*` 是架构权威。
  - `ROADMAP.md` 只描述执行顺序和阶段状态。
  - `docs/scenarios/user-layer-iteration-plan.md` 与 `docs/reviews/public-api-scenario-review.md` 描述场景层状态。
  - `report.md` 只保留审查输入，不承担路线图权威。
- **建议**：每次场景闭环后，同步更新 roadmap/current status，避免“已落地项”继续排在下一步。

### P1：public API 继续膨胀为平台能力

- **风险**：S12/S13/S18/S20/S21/S24 等场景的高级编排已经明确降级；如果后续又把自动 hedge、生产 daemon、跨进程 cache service、HTTP metrics endpoint、完整仿真交易所等塞回 SDK，会破坏当前边界。
- **处理原则**：
  - SDK 提供 typed substrate、thin foundation 和 escape hatch。
  - 用户策略、运维系统、行情中台、OMS、全局风控服务应留在用户层或独立项目。
  - `tqsdk-task` 不应演化成完整策略平台；`tqsdk-data` 不应演化成跨进程 cache daemon。

### P1：release gate 没有持续执行

- **风险**：feature/no-default、examples、clippy、doc/package gate 已写入 `validation.md`，但如果只在单次审查中运行，会逐渐失效。
- **建议**：
  - 常规开发至少跑与改动相关的 crate tests 与 `cargo check --workspace --examples`。
  - 改 feature/dependency 时补跑 no-default/all-features matrix。
  - 发布前按 `docs/architecture/validation.md` 的内部生产发布门禁执行。
  - live smoke 保持 ignored/env-gated，不作为普通验证默认运行。

### P2：typed partition 覆盖面继续按热点推进

- **事实**：market/trade 已有 typed partition read surface；query/schema/replay/system 仍可主要走兼容树。
- **判断**：这不是 P0，也不是必须一次性重写的缺陷。
- **建议**：只在高频、高风险或用户 API 明确需要时新增 typed view；generic path 仍保留官方稀疏对象和兼容层价值。

### P2：`tqsdk_core::internal` bridge 继续维持最窄

- **事实**：internal bridge 是 sibling crates 组装期间的低层桥，不是用户稳定 API。
- **建议**：
  - 不新增面向用户的 `internal` 逃生口。
  - 若某个 internal 类型开始被广泛依赖，应先判断是 session 内部实现、core contract，还是需要新的架构决策。

## 5. 不建议执行的旧方案

以下方向不建议继续推进：

- 彻底替换兼容状态树。
- 让 adapter 直接产出强类型领域状态并绕开 `NormalizedMutation` / commit-first runtime contract。
- 立即拆出 `tqsdk-protocol`、`tqsdk-transport`、`tqsdk-tq`。
- 把 direct query 复制到 wait/stream。
- 把 downloader、DataFrame/polars、research helper 下沉到 session/core。
- 把 task runtime、strategy supervisor、cache storage 下沉到 core/session。
- 为去重重写 wait/stream builder 泛型体系。
- 恢复 `ContractFuture` public alias 或扩大 core root re-export。

## 6. 后续审查清单

后续每轮 public API 或架构相关改动，至少回答：

1. 这是否改变 crate 边界、public API、runtime 状态/提交模型、session/query 归属或 wait/stream/task/data facade 归属？
2. 如果改变，是否同轮更新 `docs/architecture/*`、相关 crate README 和根 README？
3. 是否新增第二棵状态树、第二套 revision、旁路 watcher 或 facade 私有 commit 语义？
4. 是否把一次性 request/response 错放进 wait/stream？
5. 是否把执行/研究/生产平台能力错放进 core/session？
6. 是否有正式 `api_contract_sXX_*.rs` 证明用户 API 自然表达？
7. 是否按风险运行了 `docs/architecture/validation.md` 中的对应验证？

## 7. 最终结论

当前主线不是“继续拆 core 或重写 runtime”，而是：

1. 保持 `tqsdk-core` 的 protocol-complete runtime substrate 边界。
2. 保持 `tqsdk-session` 对 one-shot request/response/direct-query 的归属。
3. 保持 wait/stream 只做 diff-backed continuous consumption。
4. 保持 task/data 分别承载执行工具与研究/offline data，不向底层倒灌。
5. 把已落地的 S1-S31 foundation 维护为薄、可验证、可组合的核心 SDK 能力。
6. 让 S14 和其他平台级诉求继续停在 desired sketch、用户层工具或独立项目。
7. 把文档同步、验证矩阵和 public API 克制当成近期最重要的工程纪律。
