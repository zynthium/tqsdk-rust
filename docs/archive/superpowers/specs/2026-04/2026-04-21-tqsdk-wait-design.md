# `tqsdk-wait` / `tqsdk-stream` / `tqsdk-session` 设计文档

> Archived on 2026-05-01.
> Current architecture authority lives in `docs/architecture/*`.

## 文档定位
本文档用于锁定 Rust 版 TQSDK 在 V2 facade 层的职责边界，避免后续实现时再次把 facade 语义反向压进 `tqsdk-core`。

状态说明：

- 这份文档形成于 `tqsdk-stream` 与 `tqsdk-task` 落地之前
- 当前 `tqsdk-wait`、`tqsdk-stream`、`tqsdk-session`、`tqsdk-task` 都已实现
- 文中涉及“未来 `tqsdk-stream`”的表述，代表的是实现前的设计判断

这份设计文档只回答三类问题：

- `tqsdk-core` 之上应该再分出哪些 crate，各自负责什么
- `tqsdk-wait` 首版到底做哪些能力，不做哪些能力
- 模式无关的 direct query / schema / metadata 接口应该放在哪里

它不是实现计划，也不是逐任务施工说明。

## 背景
当前仓库已经完成了 V1 core：

- `tqsdk-core`
  - 统一命令模型
  - 统一状态树
  - 统一 commit / revision / causality
  - `RuntimeReader + UpdateCursor` 读契约
  - market / trade / replay / query / schema / auth / session 的 protocol-complete substrate

此前已经完成两轮关键判断：

1. `tqsdk-python` 的核心优势不在“同步”，而在“单推进点 + 单稳定截面”的 `wait_update()` 语义
2. 现有 `tqsdk-rs` 并非纯 callback/stream SDK，而是“async state ref + per-subscription wait + event stream”的混合范式

因此，V2 facade 层不能简单复制任何一个现有项目的外观，而需要按职责重新切边界。

## 总体目标

### 目标 1
让 `tqsdk-wait` 和 `tqsdk-stream` 都只是 `tqsdk-core` 之上的便利包装，而不是新的底层。

### 目标 2
把“状态化 diff 消费接口”和“一次性 direct query 接口”彻底分开，不混入同一个 facade 语义里。

### 目标 3
让高级工具层，例如 downloader、`TargetPosTask`、策略辅助、报表、DataFrame/polars 集成，不属于 `wait`/`stream`，而属于后续独立 crate。

## 非目标

- 不回改 `tqsdk-core` 的 public contract
- 不在 facade 层维护第二棵状态树
- 不在 `tqsdk-wait` 首版中实现 downloader / `TargetPosTask` / callback / stream
- 不追求对 Python 所有表层行为逐项一比一兼容

## 关键设计判断

### 判断 1：按“状态化 diff 消费 vs 一次性 request/response”切边界
这是整个 V2 分层的第一原则。

#### 应归为状态化 diff 消费的接口
这些接口虽然在用户侧看上去像“getter”，但本质上依赖持续推进的状态树，必须留在 `tqsdk-wait` / `tqsdk-stream` 这类模式化 facade 内：

- `get_quote`
- `get_trading_status`
- `get_kline_serial`
- `get_tick_serial`
- `get_account`
- `get_position`
- `get_order`
- `get_trade`
- `insert_order`
- `cancel_order`
- `confirm_settlement`

这些接口都有共同特征：

- 返回的不是一次性结果，而是“当前状态树中的某个持续变化对象”
- 它们的正确使用方式依赖后续 commit 推进
- 它们的变化解释需要 `wait_update` 或 stream/callback 语义配合
