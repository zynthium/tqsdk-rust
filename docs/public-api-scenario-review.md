# 场景驱动 Public API 设计审查

本文档审查 `api_contract_sXX_*.rs` 所表达的终端用户场景。正式
`crates/*/examples/api_contract_sXX_*.rs` 文件是 public API 契约样本：能自然表达的场景使用当前
public API 写成可编译示例，并纳入 `cargo check --workspace --examples` 与 CI。

不能自然表达的场景只保留理想用户代码草案，放在
`docs/scenarios/api_gaps/`，并显式标记 API gap；这些 sketch 不参与 Cargo
example 自动发现，不用底层绕路代码伪装通过。

本轮新增和微调的 public surface 位于 `tqsdk-session` / `tqsdk-wait` /
`tqsdk-stream` / `tqsdk-task` facade：它们继续复用既有 session、runtime commit、
reader/cursor 与 domain partition 读面；未改变 crate 边界或 runtime contract。
对应架构文档与 crate README 已随 public API 调整同步更新。

本报告中的“当前 API 是否自然表达”应按 Rust 分层使用者判断，而不是按官方
Python SDK 的 public API 名称判断。Python SDK 提供成熟用户语义证据；Rust
版本通过 `core/session/wait/stream/task/data` 分层服务不同用户。后续 gap
修复顺序见 [`scenarios/user-layer-iteration-plan.md`](scenarios/user-layer-iteration-plan.md)。

## 批次状态

截至 2026-04-27，近期场景驱动批次已经完成并验证：

- S12 跨合约套利从“无法表达”推进到“勉强”：用户现在可以通过
  `TaskHost::execution_group(...)` 表达两腿 typed 下单、全腿 preflight、
  session-scoped retry idempotency 和 group outcome / exposure report。
- S12 仍不能标记为“自然”：自动 hedge / flatten、timed cancel / replace、
  group resume / audit log 仍是 API gap，保留在
  `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`。
- S13 多账户下单从“无法表达”推进到“勉强”：用户现在可以通过
  `TaskHost::account_group()` / `TaskHost::multi_account_order(...)` 表达 typed
  account group、比例拆单、全账户 preflight、session-scoped retry idempotency
  和 per-account outcome report。
  执行计划见
  [`superpowers/plans/2026-04-27-task-account-group-allocation.md`](superpowers/plans/2026-04-27-task-account-group-allocation.md)。
- S11 简单策略从“勉强”推进到“自然”：用户现在可以通过
  `StrategyHost` / `StrategyContext` 在同一稳定 task/wait 推进点内读取
  quote/account/position，并复用 `TaskHost::orders(...)`、`RiskEngine` 和
  `TargetPosTask` 表达入场与止盈止损平仓。
- S24 最小可测试策略从“无法表达”推进到“勉强”：`tqsdk-task::testing`
  提供 public `StrategyTestHarness`、`FakeMarket` 和 `FakeBroker`，测试代码不再
  需要 hidden `*_for_test` API、runtime handle、channel 或 provider protocol。
- S5 低层裸行情直通继续保持“自然”：低层用户可以用
  `SessionClient::subscribe_quotes(...)` 减少 raw `RuntimeCommand` 样板，同时仍通过
  `RuntimeReader::read_market_state()` 走热路径分区读面。
- S20 生产守护进程仍为“勉强”：`TqStream::health()` 现在返回
  `StreamHealthSnapshot::status()` / `should_restart()`，但 metrics endpoint、ctrl-c
  graceful shutdown 和完整 daemon supervisor 仍是 gap。
- S21 慢消费者隔离的 bounded fan-out / lag 诊断子集已经自然表达：`TqStreamBuilder`
  可配置 root fan-out capacity，`StreamFacadeError::diagnostic()` 暴露 typed
  lag 诊断；持久化 sink runtime / per-sink retry/storage policy 仍保留为 gap。
- S22 错误诊断与重试的 error diagnostic / retry hint 子集已经自然表达：core/session/stream
  均有 typed error kind 和 retry hint；业务拒单仍应通过订单/风控 public API
  判断，完整 retry orchestration 仍是 gap。
- S23 合约信息查询与标准化继续保持“自然”：`SessionClient::query_instrument_specs`
  返回 `InstrumentSpec`，用户不再把 live `Quote` 当作合约规格对象。

| 场景 | 当前 API 表达能力 | 样板代码量 | 内部细节泄漏 | 手动异步管理 | 状态一致性风险 | 热路径性能风险 | 建议处理方式 | 证据位置 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1. 零门槛行情订阅 | 自然 | 低 | 无 | 无 | 低 | 无 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs`; `tqsdk_wait::TqApi::{get_quote, wait_update, is_changing}` |
| 2. 多合约动态订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s02_dynamic_subscriptions.rs`; `tqsdk_stream::TqStream::quotes`; `QuoteSubscription::{add, remove, symbols}`; `runtime_contract_session_reconnect::session_runtime_recovery_requeues_market_subscription_intent` |
| 3. 行情快照读取 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs`; `tqsdk_wait::TqApi::quote_snapshot` |
| 4. Tick / Quote / K线混合订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s04_mixed_market_streams.rs`; `TqStream::market_events`; `MarketEventStream`; `MarketEvent` |
| 5. 高频裸行情直通 | 自然 | 中 | 少量 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`; `SessionClient::{subscribe_quotes, progress_once}`; `RuntimeReader::read_market_state` |
| 6. 普通限价下单 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs`; `TqApi::{login_trade_account, limit_order}`; `LimitOrderIntent::send_once`; `OrderTicket::wait_terminal` |
| 7. 撤单与部分成交 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs`; `OrderRef::{wait_partially_filled, cancel_remaining, wait_terminal}`; `Order.lifecycle`; `Order.volume_left` |
| 8. 账户 / 资金 / 持仓查询 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs`; `TqApi::{login_trade_account, get_account, get_position}` |
| 9. 启动后状态恢复 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs`; `tqsdk_session::StartupRecoverySpec`; `TqApi::startup_recovery`; `TqStream::recover_state` |
| 10. 断线重连中的订单一致性 | 自然 | 中 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs`; `tqsdk_session::OrderIntentRecord`; `TqApi::limit_order`; `OrderTicket`; `OrderTicketState`; session-scoped reconnect is covered, cross-process persistence remains out of scope |
| 11. 简单策略 | 自然 | 中 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`; `StrategyHost`; `StrategyContext`; `TaskHost::orders`; `RiskEngine`; `TargetPosTask` |
| 12. 跨合约套利 | 勉强 | 中 | 无 | 无 | 高 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s12_spread_arbitrage.rs`; `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`; `ExecutionGroupBuilder`; `ExecutionGroupOutcome`; automatic hedge remains gap |
| 13. 多账户下单 | 勉强 | 中 | 无 | 无 | 中 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s13_multi_account_ordering.rs`; `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`; `AccountGroup`; `MultiAccountOrderTicket`; advanced failure policy/resume/audit remains gap |
| 14. 多 provider 行情聚合 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 颠覆性重构 | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs`; no public provider aggregation facade |
| 15. 实盘 / 模拟 / 回放切换 | 勉强 | 高 | 少量 | 少量 | 中 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs`; builders have targets/replay URL, but no common strategy runtime |
| 16. 历史行情回放 | 不自然 | 高 | 少量 | 少量 | 中 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`; `DataClient` history series; `SessionClient::replay_step*` |
| 17. 研究场景 | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs`; `DataClient::get_kline_data_series` |
| 18. 本地行情缓存读写 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`; no live cache writer/reader contract |
| 19. 风控前置 | 勉强 | 中 | 无 | 无 | 中 | 低 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s19_pre_trade_risk.rs`; `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`; `RiskEngine`; `RiskRejection`; `TaskHost::orders`; guarded insert risk integration |
| 20. 生产守护进程 | 勉强 | 中 | 少量 | 少量 | 中 | 中 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`; `TqStream::health`; `StreamHealthSnapshot::{status, should_restart}`; metrics/graceful shutdown still gap |
| 21. 慢消费者隔离 | 勉强 | 中 | 无 | 少量 | 低 | 低 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`; `docs/scenarios/api_gaps/api_contract_s21_slow_consumer_isolation.rs`; bounded fan-out / typed lag diagnostic 子集自然；durable sink runtime 仍是 gap |
| 22. 错误诊断与重试 | 勉强 | 中 | 无 | 少量 | 低 | 低 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`; `docs/scenarios/api_gaps/api_contract_s22_error_diagnosis_retry.rs`; error kind / retry hint 子集自然；retry orchestration 仍是 gap |
| 23. 合约信息查询与标准化 | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`; `SessionClient::query_instrument_specs`; `InstrumentSpec`; `InstrumentClass` |
| 24. 最小可测试策略 | 勉强 | 中 | 无 | 无 | 低 | 低 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s24_testable_strategy.rs`; `docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs`; `StrategyTestHarness`; `FakeMarket`; `FakeBroker`; fake reconnect/deterministic clock remains gap |

## 主要结论

1. 当前最自然的终端用户场景是：零门槛 wait quote、低层裸行情直通、研究 K线批处理、合约 metadata 查询。
2. 交易相关场景的主要 API gap 不是 core command 能力缺失，而是用户级 execution/risk abstraction 不足。普通登录、限价单、部分成交撤单、session-scoped reconnect-safe order intent、最小前置风控、execution group foundation、account group foundation 和最小 strategy context 已具备薄 facade；跨进程持久恢复、自动对冲、组合级 what-if 风控和多账户高级执行策略仍需继续补齐。
3. `tqsdk-stream` 的底座方向正确，quote 订阅、动态 quote handle、混合 market event、health snapshot、health status、fan-out capacity 和 typed lag/error diagnostics 已有薄 facade；持久化 sink、完整 daemon supervisor/metrics 仍停留在底层组合能力之外。
4. 多 provider 聚合、完整 live/sim/replay environment、历史回放驱动策略、本地行情缓存、fake reconnect/deterministic clock 都是新 facade/tooling 层问题，不应下沉到 `tqsdk-core` 或 `tqsdk-session`。
