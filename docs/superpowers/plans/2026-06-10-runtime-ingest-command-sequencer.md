# Runtime Ingest / Command Sequencer Architecture Plan

> 本计划只用于下一轮架构门控设计与实现；当前 diff ingest 性能批次不得直接执行这里的代码改动。

## 背景

`crates/tqsdk-core/tests/runtime_ingest_tail_latency.rs` 在 2026-06-10 的 Batch 4
复测结果：

- no-load command submit p95: `81.085us`
- under large market ingest p95: `50.267216ms`

under-ingest p95 远高于 2x 门槛，说明大行情批次在 `RuntimeHandle::ingest()` 内持有
runtime mutex 时，会显著抬高并发 command submission 尾延迟。

## 目标

- 降低大行情 ingest 期间 command submission 的 p95/p99 等待时间。
- 保持现有 public runtime contract：`RuntimeHandle -> StateStore -> CommitResult ->
  RuntimeReader/UpdateCursor`。
- 保持所有可见状态变化仍进入同一棵状态树。

## 硬约束

- 保持一个全局 revision sequence，不得引入 facade 私有 revision。
- 保持一个 `CommitLog`，cursor 和 stream 仍消费同一提交序列。
- command ledger transition 继续通过现有状态机校验，不得用 adapter 本地字符串判断绕过
  `record_command_status()`。
- market/trade/query/session/replay mutations 的 commit 顺序必须可解释、可测试。
- 不得新增第二棵 state tree、旁路通知或 latest-only 隐式丢弃语义。

## 设计方向

1. 先拆分 measurement。
   - 增加 debug/ignored probe，分别测 decode、domain validation、state apply、commit publish、
     command ledger update 的耗时。
   - 将 command submit 的 no-load / under-ingest p95、p99 固化到可重复输出。

2. 缩短 command submit critical section。
   - 保留 command id 分配、ledger 记录、outbound dispatch 入队的原子顺序。
   - 避免 command submit 等待无关的大 market decode/apply 工作完成。
   - 如果需要后台 sequencer，sequencer 必须是唯一 revision 发布者。

3. 评估 market ingest 预处理边界。
   - 只有在不改变 adapter 观察语义时，才允许把纯 JSON decode 或 normalized mutation
     构造移出 runtime mutex。
   - `validate_mutation_domains`、order lifecycle normalization、state apply、commit assembly
     的重排必须有 contract tests 证明顺序不变。

4. 架构文档先行。
   - 实现前必须更新 `docs/architecture/README.md`。
   - 实现前必须更新 `docs/architecture/runtime-core/*.md` 中涉及 write path / commit contract
     的专题文档。
   - 实现前必须更新 `docs/architecture/validation.md`，列出新增 tail-latency 和 sequencing
     contract tests。

## 验收门槛

- `command_submit_latency_under_large_market_ingest_is_reported` 的 under-ingest p95 降至
  no-load p95 的 2x 以内，或文档解释为何受线程调度/测试模型限制不能作为硬断言。
- `cargo test -p tqsdk-core --test runtime_contract_commit_semantics`
- `cargo test -p tqsdk-core --test runtime_contract_runtime_core`
- `cargo test -p tqsdk-core --test runtime_ingest_tail_latency -- --ignored --nocapture`
- `cargo test --workspace`
- `cargo clippy --workspace --examples --all-targets -- -D warnings`

## 非目标

- 不改变 public `ChangeSet` shape。
- 不改变 stream exact-delivery / lagged 语义。
- 不把 task/data/direct-query 能力下沉到 core。
- 不在没有 architecture docs 和 sequencing tests 的情况下实现 actor rewrite。
