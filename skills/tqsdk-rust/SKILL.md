---
name: tqsdk-rust
description: Use when 需要构建、解释、调试或脚手架化 Rust TQSDK 代码，涉及 tqsdk-core、tqsdk-session、tqsdk-wait、tqsdk-stream、tqsdk-task、tqsdk-data，或行情、wait_update、async streams、metadata/direct queries、下单、TargetPosTask、策略/回放、历史 K 线/tick、CSV/cache/export、权限、Python TqSdk 等价工作流。
---

# TQSDK Rust

使用本 skill 时，把 TQSDK Rust 请求映射到正确的 crate、调用形态和最小代码，同时保持 workspace 的 crate 边界。

## 先路由请求

只读取当前问题需要的 reference。

1. 每个请求先读 [references/scenario-router.md](references/scenario-router.md)。按用户想持有或消费的对象分类，不要按用户第一个提到的 API 名分类。
2. 用户不确定该用哪个 crate、或问题涉及依赖写法和 crate 边界时，读 [references/crate-selection.md](references/crate-selection.md)。
3. 写示例代码或修复示例编译错误前，读 [references/code-patterns.md](references/code-patterns.md)。
4. 用户要求按角色给示例、完整覆盖场景、场景契约、public API 证据，或问“每类用户应该怎么做”时，读 [references/scenario-contracts.md](references/scenario-contracts.md)。
5. 策略循环、事件总线、研究、回放、测试、低延迟柜台工作流，读 [references/quant-workflows.md](references/quant-workflows.md)。
6. 凭证、权限、实盘交易、模拟、下单副作用、风控、live smoke test，读 [references/safety-and-operations.md](references/safety-and-operations.md)。
7. 只有用户要求新建独立 starter project 时，才使用 [scripts/new-tqsdk-rust-project.py](scripts/new-tqsdk-rust-project.py) 和 [assets/templates/wait-quote-loop](assets/templates/wait-quote-loop)。

## 核心规则

- 写代码前先选择能覆盖场景的最高层 crate。
- 官方 Python TqSdk 行为是语义参考，但 Rust 要映射到 crate 归属，不要重建 Python 单体 `TqApi`。
- one-shot query 放在 `tqsdk-session`，live ref 放在 `tqsdk-wait`，事件管线放在 `tqsdk-stream`，执行工具放在 `tqsdk-task`，离线/历史数据放在 `tqsdk-data`。
- 只有低层 runtime、自定义 facade、adapter、command 状态机、commit/cursor、hot-path `RuntimeReader` 才使用 `tqsdk-core`。
- 所有可见状态变化都必须经过 runtime commit 和 `RuntimeReader` / `UpdateCursor`；不要发明私有状态树、本地订单 overlay 或旁路通知。
- live/network 示例默认需要 Tokio、凭证、行情权限，以及明确的交易权限。
- 优先使用 `futures_market()`、`stock_market()`、`trade_target_tqkq()`、`enable_query()` 这类命名 builder，不要使用裸 bool route flag。
- 下单示例默认使用模拟/TqKq 风格；只有用户明确要求实盘接入并接受副作用时，才给 real-account 集成。
- 精确 API 形状重要时，先检查目标 crate README 和 `crates/*/examples/api_contract_sXX_*.rs`，再定稿代码。

## 常见错误

- 不要用 `tqsdk-wait` 回答 direct-query 问题；使用 `tqsdk-session` 或 `api.session()`。
- wait/stream app 里不要为了 metadata 再建第二个 client；复用 shared session。
- 不要把历史下载当作 live ref；使用 `tqsdk-data`。
- 普通用户示例不要从 `tqsdk-core` 起步，除非用户明确要 runtime internals。
- typed ticket、ref 或 status helper 已存在时，不要发明本地订单 overlay，也不要解析 status 字符串。
- 不要用字符串或 adapter-local 判断绕过 `record_command_status()` 和 runtime command lifecycle。
- 示例里不要隐藏凭证、权限或实盘订单副作用。
- 回答用法问题时，不要跨 crate 移动 direct query、downloader、task 或 research 语义。

## 回答风格

- 开头先说明使用哪个 crate 以及原因：live ref、event stream、one-shot query、task execution、offline rows 或 runtime substrate。
- 优先给和当前 example 匹配的短 Rust snippet，不要写大段伪代码。
- 覆盖用户角色或宽工作流时，引用 `scenario-contracts.md` 中对应的 `api_contract_sXX_*.rs` 示例。
- 点名用户下一步应调用的具体 API。
- 如果 Rust 答案刻意不同于 Python TqSdk，要说明原因是 Rust workspace 拆成了 `session`、`wait`、`stream`、`task`、`data`。
- 代码会下单、撤单、使用实盘账户或依赖付费行情权限时，先说明安全门槛。
- 请求不明确时，只问一个形状问题：“你需要一个带 ref 的单 live loop、多个事件消费者、one-shot query、task/order 抽象、历史 rows，还是 runtime commits？”

## 项目脚手架

从内置 asset template 创建最小 quote loop 项目：

```bash
python3 scripts/new-tqsdk-rust-project.py ./my-tqsdk-app \
  --sdk-source git \
  --sdk-value https://github.com/OWNER/tqsdk-rust \
  --symbol SHFE.au2602
```

本地开发使用 `--sdk-source path --sdk-value /path/to/tqsdk-rust`；crate 发布后可使用 `--sdk-source version --sdk-value <version>`。
