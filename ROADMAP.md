# `tqsdk-rust` Roadmap

## 文档定位

这份文档是仓库级执行路线图。它描述当前阶段状态、后续优先级和明确不做的方向。

权威关系：

- crate 边界、runtime contract、public API 归属和 AI 工作流约束，以 `docs/architecture/*` 为准。
- 场景状态和 API gap，以 `docs/scenarios/*` 与 `docs/reviews/*` 为准。
- 本文件只决定“下一轮先做什么、暂缓什么”，不能覆盖架构文档。

如果本文件与 `docs/architecture/*` 或当前代码冲突，先按架构文档和代码修正本文件。

## 总原则

- `tqsdk-core` 继续保持 protocol-complete runtime substrate，只负责命令、状态、commit/revision、cursor、adapter、schema types 与底层 session/runtime contract。
- `tqsdk-session` 继续承载 shared session、one-shot request/response、direct query、metadata/schema/service query 与 control-plane helper。
- `tqsdk-wait` 和 `tqsdk-stream` 只负责 diff-backed continuous consumption。
- `tqsdk-task` 是执行工具层，不演化成完整策略平台、OMS 或生产 daemon。
- `tqsdk-data` 是 research/offline data 层，不演化成 session owner、live facade 或跨进程 cache service。
- 后续迭代按 Rust 使用者分层推进，不按 Python SDK 方法清单机械补 API。
- 每一轮改动都要求清晰 crate 边界、可验证验收标准、最小必要 public surface，以及同步更新测试/示例/文档。

## 当前基线

截至 2026-05-04，当前核心 SDK foundation 已经大体闭环：

- `tqsdk-core`
  - protocol-complete runtime contract
  - 单一 commit/revision/cursor 语义
  - compatible state tree + market/trade domain partitions
  - `MutationSource` 根路径防线
  - command/order 状态机与 typed order lifecycle
  - shared commit identity 与 reader-first hot path
- `tqsdk-session`
  - shared session shell
  - lazy establish / reconnect-resync / route driving substrate
  - direct query / schema / metadata / calendar / settlement / ranking / EDB
  - session-scoped order intent ledger
  - startup/recovery、command status 和 replay one-shot control-plane helper
- `tqsdk-wait`
  - single-owner `TqApi`
  - `wait_update()`、`is_changing()`、live quote/trade/system refs
  - kline/tick serial window
  - startup recovery、reconnect-safe order ticket、partial fill cancel flow
  - futures/security trade refs 与 wait 风格交易命令包装
- `tqsdk-stream`
  - multi-consumer commit stream facade
  - commit/path/scope/domain/object/field filters
  - typed market/trade stream、ready kline/tick window、trade session event stream
  - health snapshot、reconnect monitor、graceful shutdown
  - managed commit sink、finite retry、JSONL WAL、fsync policy、local compaction、recovery report、commit journal
- `tqsdk-task`
  - `TaskHost`、`TargetPosTask`、`TargetPosScheduler`
  - order builder、ownership / guarded order、execution report
  - `ExecutionGroup`、`AccountGroup`
  - `RiskEngine`、risk/projection report
  - `StrategyHost`、strategy environment/deployment/supervisor
  - public fake market / fake broker / deterministic clock test harness
  - `TradingDeskProfile` 低延迟柜台 thin profile
- `tqsdk-data`
  - `DataClient`
  - history page/series/download、CSV export
  - `query_his_cont_quotes`、`query_option_greeks`
  - local market cache record/replay、single-process live cache pipe
  - history series cache / mmap-compatible opt-in materialization
  - history series -> strategy replay adapter

场景契约状态：

- S1-S13、S15-S31 已有核心 SDK path 或 foundation，并以正式 examples / review 记录作为证据。
- S14 多 provider 行情聚合仍是唯一 active `docs/scenarios/api_gaps/` 项，当前暂缓。
- 已归档的 gap sketch 不应重新作为当前实现入口，除非新需求重新立项。

## 当前执行阶段

当前不再处于“补齐第一批 facade 能力”的阶段，而是进入：

1. **发布门禁维护**
2. **public API 边界维护**
3. **文档索引同步**
4. **文档同步和历史计划归档**

近期工作应优先服务这四件事，而不是继续扩大 public surface。

## P0：边界与发布门禁守护

### 目标

防止已闭环 foundation 在后续小改中退化，并保持当前 CI / release gate 对
public API 契约、feature flags 和 packaging 的覆盖。

### 应持续做

- 保持 `.github/workflows/ci.yml` 与 `docs/architecture/validation.md` 的发布门禁同步。
- 发布前按 `docs/architecture/validation.md` 的内部生产发布门禁复核本地或离线环境。
- public API、feature flags、crate dependency 变动后，运行对应 examples、no-default、all-features 和 clippy 检查。
- live smoke 继续保持 ignored/env-gated，不进入普通本地验证默认路径。
- 新增或修改场景契约时运行 `scripts/check_api_contract_examples.sh`，保持正式 examples 和 gap sketches 的场景头完整。
- 每次文档更新确认 `ROADMAP.md`、`docs/scenarios/user-layer-iteration-plan.md`、`docs/reviews/public-api-scenario-review.md` 没有互相漂移。

### 退出条件

- CI 持续覆盖 `fmt`、examples、workspace/all-features tests、clippy、no-default、docs、cargo-deny 和 package gate。
- README、ROADMAP、architecture、scenarios、reviews 对当前阶段描述一致。

## P1：维护核心 SDK foundation

### 目标

维护已落地能力的可用性、清晰度和最小 public surface。

### 重点

- `tqsdk-core`：保持 runtime contract 克制；只按高风险/高频需求扩 typed read surface。
- `tqsdk-session`：继续维护 metadata/service query、startup/recovery、order intent substrate，不吸收 live facade 配置。
- `tqsdk-wait`：维护单策略用户的稳定截面、live refs 和交易一致性，不复制 direct query。
- `tqsdk-stream`：维护 health/recovery/sink isolation foundation，不承诺跨进程 daemon queue 或 runtime state snapshot recovery。
- `tqsdk-task`：维护 execution/risk/strategy/test/trading-desk thin foundation，不扩成自动执行平台。
- `tqsdk-data`：维护 history/cache/replay/research foundation，不扩成跨进程 cache service。

### 可接受的新工作

- 缩短明显绕路的正式场景示例，但不改变架构边界。
- 给已有 foundation 补最小缺失测试、文档或 typed diagnostic。
- 修复 public API 退化、feature flag 退化或 no-default 构建退化。

### 明确不做

- 因为“方便”把 task/data/session/wait/stream 能力互相下沉或复制。
- 为了单个示例好看扩大 core root re-export。
- 为了自动化平台能力加入大面积 public API。

## P2：按需求评估的后续能力

这些能力可以未来评估，但不应抢在 P0/P1 稳定化之前推进。

### DataFrame / polars 适配层

- 只应进入 `tqsdk-data`。
- 应作为 opt-in adapter，而不是默认 data surface。
- 不得让 polars/DataFrame 依赖传播到 core/session/wait/stream/task。

### 路径管理型导出工具

- 可作为 `tqsdk-data` 的薄便利层评估。
- 应建立在现有 `AsyncWrite` CSV export 与 history download substrate 之上。
- 不应引入后台 downloader、GUI viewport 或跨进程服务语义。

### `tqsdk-callback`

- 只有当 handler-style 用户面明显独立于 stream，且维护成本合理时才考虑。
- 如果只是 stream 的薄包装，优先不拆 crate。
- 不承载 query、downloader 或 task runtime。

### `tqsdk-backtest`

- 只有当回测执行编排、指标统计、资金曲线、报告归档等用户层能力形成明确需求时再评估。
- replay step/reset、history series -> strategy replay、fake broker/test harness 目前已有落点，不足以单独证明需要新 crate。

## P3：暂缓能力

### 多 provider 行情聚合

当前唯一 active API gap 是 S14 多 provider 行情聚合。

暂缓原因：

- 官方 Python SDK 没有将多 provider 聚合作为核心 public API。
- 该能力更像行情中台或用户层基础设施。
- 它会引入 provider id、质量状态、冲突合并、健康策略等复杂语义，不能顺手下沉到 core/session。

未来只有在明确用户需求和架构计划同时存在时，才重新评估为 `tqsdk-stream` 之上的独立 facade 或独立项目。

## 不建议路线

下面方向当前明确避免：

- 回到单体 `TqApi` crate。
- 把 direct query 重新塞进 `tqsdk-wait` / `tqsdk-stream`。
- 把 downloader、DataFrame/polars 或 research helper 塞进 `tqsdk-session`。
- 把 task runtime、strategy supervisor 或 cache storage 塞进 `tqsdk-core`。
- 把 production daemon、HTTP health/metrics endpoint、GUI/web helper 做成 SDK 核心能力。
- 把跨进程 cache service、distributed queue、writer election、runtime snapshot recovery 做成 `tqsdk-data` 默认 public surface。
- 为了提前对齐所有 `tqsdk-python` 接口而牺牲 crate 边界和类型安全。
- 复制第二棵用户态状态树、第二套 revision 或 facade 私有 commit model。

## 每轮迭代的工作方式

1. 只选择一个明确子目标。
2. 先确认它属于哪个 crate，不跨层乱放。
3. 如果涉及 public API，先写或更新正式 `api_contract_sXX_*.rs` / review 记录。
4. 实现保持最小 surface，不用平台能力填补核心 SDK 边界之外的问题。
5. 按风险运行 `docs/architecture/validation.md` 中的验证命令。
6. 同步更新相关 README、architecture、scenarios、reviews 或 roadmap。
7. 完成并验证一个可提交单元后小步提交。

## 当前建议的下一步

下一轮实际开发优先级：

1. 保持 CI / release gate 绿色；若发布流程需要本地一键入口，再补最薄的 release-check 脚本。
2. 补齐 README / docs 索引漂移，尤其是正式场景 examples 列表与 S30/S31 当前状态。
3. 审查当前 public API export surface，确认已降级的平台能力没有重新进入 root exports 或 crate README。
4. 只在真实用户需求出现后，再评估 DataFrame/polars、callback 或 backtest 独立 crate。

在这些完成前，不建议新增大块 facade 或启动新 crate。
