# `tqsdk-rust` Roadmap

## 文档定位

这份文档是仓库级的 canonical roadmap，用于指导后续逐步迭代。

它回答的不是“最终想做多少功能”，而是：

- 未来应按什么顺序推进
- 每一阶段的目标是什么
- 哪些能力应该进入哪个 crate
- 哪些能力当前明确不该做
- 什么情况下才算一个阶段完成，可以进入下一阶段

如果架构文档和这里有冲突，以这里的执行顺序为准；如果 crate 边界不明确，以 `docs/architecture/crate-boundaries.md` 和 `docs/architecture/crate-blueprint.md` 为准。

## 总原则

- 优先保护 `tqsdk-core` 的稳定、高性能、纯 async substrate 边界。
- 共享 session 与 one-shot request/response 能力继续集中在 `tqsdk-session`。
- diff-backed continuous consumption 能力只进入 `tqsdk-wait` 和未来的 `tqsdk-stream`。
- `TargetPosTask`、downloader、DataFrame/polars、回测报告、GUI 都不应倒灌到底层。
- 每一阶段都要求：
  - 清晰 crate 边界
  - 可验证的验收标准
  - 最小必要 public surface
  - 测试与示例同步更新

## 当前基线

当前已完成并稳定的基础层：

- `tqsdk-core`
  - protocol-complete runtime contract
  - DIFF / trade / replay / auth / session / system / query / schema
- `tqsdk-session`
  - shared session shell
  - direct query / schema / metadata / calendar / settlement / ranking / EDB
- `tqsdk-wait`
  - Python 风格单 owner `wait_update()` facade
  - live quote / trading status / kline / tick / futures account-position-order-trade / security account-position-order-trade / pre-insert order / risk / settlement / notification
  - trade command 的 wait 风格薄包装

当前尚未进入但已经明确方向的上层：

- `tqsdk-stream`
- `tqsdk-task`
- `tqsdk-data`
- 可选的 `tqsdk-backtest`
- 可选的 `tqsdk-callback`

## 推荐执行顺序

建议按下面顺序推进，而不是并行膨胀：

1. 稳固当前 `core/session/wait`
2. 实现 `tqsdk-stream`
3. 实现 `tqsdk-task`
4. 实现 `tqsdk-data`
5. 视复杂度决定是否独立 `tqsdk-backtest`
6. 最后再决定 `tqsdk-callback` 是否独立存在

原因：

- `tqsdk-stream` 直接验证当前底座是否真的足以同时承载 Python 风格和 Rust 风格
- `tqsdk-task` 是最容易污染底层边界的一层，必须在 stream/wait 边界稳定后再做
- `tqsdk-data` 面向研究与离线分析，价值明确，但不应倒逼底层设计
- `tqsdk-backtest` 与 `tqsdk-callback` 的独立价值，要等前面几层稳定后再判断

## Phase 1：稳固当前基础层

### 目标

把当前已经落地的 `tqsdk-core`、`tqsdk-session`、`tqsdk-wait` 收敛成一个长期稳定、适合继续叠加 facade 的底座。

### 本阶段应完成

- 继续审计并收口 public API surface
- 把当前 core typed schema 对应的 diff-backed live 对象覆盖补齐到 `tqsdk-wait` 的合理边界
- 补足更多实际可运行示例，覆盖：
  - live 行情
  - trade 命令
  - session direct query
  - replay step/reset 的底层使用方式
- 补足文档和测试矩阵，使“哪些能力在哪层”不会再模糊
- 继续排查性能和实现上的明显低效点，但不做凭感觉的大重构

### 本阶段最适合补齐的能力

当前 Phase 1 中原本列出的 diff-backed live refs 已全部补齐。

接下来的重点不再是继续扩 public surface，而是：

- 继续审计 wait/session/core 的职责边界
- 补更多真实联机示例
- 评估 `tqsdk-stream` 的最小稳定 API

### 本阶段明确不做

- `tqsdk-stream`
- `TargetPosTask`
- downloader
- DataFrame / polars
- callback facade
- 回测报告系统

### 进入下一阶段的退出条件

- `core/session/wait` 的 crate 边界不再频繁变化
- `tqsdk-wait` 已覆盖当前 core 中最关键的 diff-backed live 对象
- 核心示例足以证明当前底座可用于真实联机与基础交易
- workspace 文档能清楚说明每一层职责

## Phase 2：`tqsdk-stream`

### 目标

在不修改 `tqsdk-core` commit/revision 语义的前提下，提供 Rust async-native 的 continuous-consumption facade。

### 这个 crate 应负责

- diff-backed live object 的 stream 消费
- 多消费者等待点
- 按对象 / 按协议域 / 按路径的 stream facade
- 背压策略与订阅生命周期管理
- 可靠事件流与状态流分层

### 推荐 API 方向

- 与 `tqsdk-wait` 共享同一底层 `SessionClient`
- 暴露尽量少的 canonical stream 入口
- 避免复制第二棵状态树
- 不把 one-shot query 搬进来

### 本阶段验收重点

- 是否能自然表达现有 `tqsdk-rs` 的多消费者场景
- 是否不破坏 `tqsdk-core` 的统一 commit 语义
- 是否与 `tqsdk-wait` 形成并列消费形状，而不是互相污染

### 本阶段明确不做

- `TargetPosTask`
- downloader
- DataFrame / polars
- GUI / callback integration

### 进入下一阶段的退出条件

- `tqsdk-stream` 的 canonical API 稳定
- 与 `tqsdk-wait` 的职责边界清楚
- 可靠事件流与状态流的分层方式被锁定
- 有 live 示例验证其基本可用性

## Phase 3：`tqsdk-task`

### 目标

把“持续读状态 + 持续发命令 + 维护内部任务状态”的高层执行工具独立出来，而不是继续塞进 wait/stream facade。

### 这个 crate 应负责

- `TargetPosTask`
- 调仓 scheduler
- 任务 ownership / symbol ownership
- 执行规划器
- quote hint / offset priority / volume split policy
- task execution report

### 设计要求

- 依赖 `tqsdk-wait` 或 `tqsdk-stream`，但不反向进入底层
- task 内部状态与用户态 live state 明确区分
- 不得倒逼 `tqsdk-core` 改写提交模型

### 本阶段验收重点

- 是否能承接 `tqsdk-python` 的 `TargetPosTask`
- 是否能吸收现有 `tqsdk-rs` 的 runtime/task registry 经验
- 是否没有污染底层 crate 的边界

### 本阶段明确不做

- DataFrame / 数据下载
- 报表系统
- GUI

### 进入下一阶段的退出条件

- `TargetPosTask` 基本语义稳定
- task ownership 与手动下单冲突策略被明确
- 关键 live/replay 场景下都能工作

## Phase 4：`tqsdk-data`

### 目标

独立承接离线/研究/批处理数据能力，而不是把研究接口堆进 `tqsdk-session` 或 `tqsdk-wait`。

### 这个 crate 应负责

- downloader
- 历史数据批量拉取
- `get_kline_data_series`
- `get_tick_data_series`
- `query_his_cont_quotes`
- `query_option_greeks`
- pandas/polars/DataFrame 兼容层
- 文件导出、缓存、落盘

### 设计要求

- 尽量复用 `tqsdk-session` 的 one-shot query 能力
- 如需 replay/history，建立在已有 replay contract 之上
- 研究型视图和高性能底层 API 分层明确

### 本阶段验收重点

- 是否能对齐 `tqsdk-python` 中研究/数据接口的主要能力
- 是否不会让 `session` / `wait` 重新变胖

### 本阶段明确不做

- 把 DataFrame/polars 倒灌到底层 crate
- 让 downloader 侵入 core runtime contract

### 进入下一阶段的退出条件

- 数据研究类接口已有明确独立落点
- 当前三层底座未被污染

## Phase 5：回放与回测用户层能力

### 目标

决定 replay/backtest 用户层能力是否需要独立为 `tqsdk-backtest`。

### 优先判断标准

如果未来只需要：

- replay step/reset
- live object 读取

那么继续让 replay contract 留在 `core/session` 即可。

如果未来还要明显扩展：

- 回测执行编排
- 回测报告
- 指标统计
- 资金曲线
- 结果归档

那么应独立为 `tqsdk-backtest`。

### 本阶段明确不做

- 为了“看起来完整”而过早拆 crate

### 退出条件

- 是否独立为 `tqsdk-backtest` 已有明确结论

## Phase 6：`tqsdk-callback` 或 callback integration

### 目标

只在真实需求明确存在时，再决定是否独立 callback facade。

### 适合进入

- callback / handler 风格 facade
- UI / 监控 / observer integration

### 不适合进入

- query
- downloader
- task runtime

### 判断标准

- 如果 callback 只是 stream 的薄包装，优先不要独立 crate
- 只有当 handler-style 用户面明显独立、维护成本合理时才拆

## 跨阶段的长期工作

这些事情不属于单独 crate，但应持续推进：

- 持续 public API 审计
- 性能回归检查
- 更多 live smoke 示例
- 更严格的错误语义与文档
- 发布元信息和文档站整理
- 测试矩阵继续补强

## 不建议的路线

下面这些方向当前应明确避免：

- 回到单体 `TqApi` crate
- 把 direct query 重新塞进 `tqsdk-wait`
- 把 downloader / DataFrame / polars 塞进 `tqsdk-session`
- 把 task runtime 塞进 `tqsdk-core`
- 为了提前对齐所有 `tqsdk-python` 接口而牺牲 crate 边界
- 复制第二棵用户态状态树

## 每轮迭代的工作方式

建议后续每一轮开发都按下面模式推进：

1. 只选择一个阶段中的一个明确子目标
2. 先确认它属于哪个 crate，不跨层乱放
3. 先补测试与文档，再补实现
4. 完成后立即更新相关 README / 架构文档
5. 小步提交，保持每一阶段都可回退

## 当前建议的下一步

如果按当前优先级继续推进，建议下一轮实际开发从下面二选一开始：

1. 继续 Phase 1，把 `tqsdk-wait` 对 core 中现有 diff-backed 对象的覆盖补齐
2. 开始 Phase 2，先写 `tqsdk-stream` 的最小架构文档与 API 草图，再进入实现

如果目标是先把底座做得更稳，优先选 1。
如果目标是尽快验证当前底座能否同时承载 Python 风格和 Rust 风格，优先选 2。
