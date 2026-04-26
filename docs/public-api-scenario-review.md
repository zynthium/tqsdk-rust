# 场景驱动 Public API 设计审查

本文档审查 `api_contract_sXX_*.rs` 所表达的终端用户场景。正式
`crates/*/examples/api_contract_sXX_*.rs` 文件是 public API 契约样本：能自然表达的场景使用当前
public API 写成可编译示例，并纳入 `cargo check --workspace --examples` 与 CI。

不能自然表达的场景只保留理想用户代码草案，放在
`docs/scenarios/api_gaps/`，并显式标记 API gap；这些 sketch 不参与 Cargo
example 自动发现，不用底层绕路代码伪装通过。

本轮新增和微调的 public surface 只位于 `tqsdk-wait` / `tqsdk-stream`
facade：它们继续复用既有 session、runtime commit、reader/cursor 与
domain partition 读面；未改变 crate 边界或 runtime contract。对应架构文档与
crate README 已随 public API 调整同步更新。

本报告中的“当前 API 是否自然表达”应按 Rust 分层使用者判断，而不是按官方
Python SDK 的 public API 名称判断。Python SDK 提供成熟用户语义证据；Rust
版本通过 `core/session/wait/stream/task/data` 分层服务不同用户。后续 gap
修复顺序见 [`scenarios/user-layer-iteration-plan.md`](scenarios/user-layer-iteration-plan.md)。

| 场景 | 当前 API 表达能力 | 样板代码量 | 内部细节泄漏 | 手动异步管理 | 状态一致性风险 | 热路径性能风险 | 建议处理方式 | 证据位置 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1. 零门槛行情订阅 | 自然 | 低 | 无 | 无 | 低 | 无 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s01_zero_barrier_quote.rs`; `tqsdk_wait::TqApi::{get_quote, wait_update, is_changing}` |
| 2. 多合约动态订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s02_dynamic_subscriptions.rs`; `tqsdk_stream::TqStream::quotes`; `QuoteSubscription::{add, remove, symbols}`; `runtime_contract_session_reconnect::session_runtime_recovery_requeues_market_subscription_intent` |
| 3. 行情快照读取 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s03_quote_snapshot.rs`; `tqsdk_wait::TqApi::quote_snapshot` |
| 4. Tick / Quote / K线混合订阅 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s04_mixed_market_streams.rs`; `TqStream::market_events`; `MarketEventStream`; `MarketEvent` |
| 5. 高频裸行情直通 | 自然 | 中 | 少量 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-session/examples/api_contract_s05_bare_market_fast_path.rs`; `SessionClient::progress_once`; `RuntimeReader::read_market_state` |
| 6. 普通限价下单 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s06_limit_order.rs`; `TqApi::{login_trade_account, limit_order}`; `LimitOrderIntent::send_once`; `OrderTicket::wait_terminal` |
| 7. 撤单与部分成交 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s07_cancel_partial_fill.rs`; `OrderRef::{wait_partially_filled, cancel_remaining, wait_terminal}`; `Order.lifecycle`; `Order.volume_left` |
| 8. 账户 / 资金 / 持仓查询 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s08_account_position_updates.rs`; `TqApi::{login_trade_account, get_account, get_position}` |
| 9. 启动后状态恢复 | 自然 | 低 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s09_startup_state_recovery.rs`; `tqsdk_session::StartupRecoverySpec`; `TqApi::startup_recovery`; `TqStream::recover_state` |
| 10. 断线重连中的订单一致性 | 自然 | 中 | 无 | 无 | 低 | 低 | API 微调 | `crates/tqsdk-wait/examples/api_contract_s10_reconnect_order_consistency.rs`; `tqsdk_session::OrderIntentRecord`; `TqApi::limit_order`; `OrderTicket`; `OrderTicketState`; session-scoped reconnect is covered, cross-process persistence remains out of scope |
| 11. 简单策略 | 勉强 | 高 | 少量 | 少量 | 高 | 中 | 局部重构 | `crates/tqsdk-task/examples/api_contract_s11_simple_strategy.rs`; `TaskHost`; `TargetPosTask` |
| 12. 跨合约套利 | 无法表达 | 高 | 严重 | 严重 | P0 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s12_spread_arbitrage.rs`; no execution group / hedge policy API |
| 13. 多账户下单 | 无法表达 | 高 | 严重 | 严重 | 高 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s13_multi_account_ordering.rs`; `SessionClientBuilder::trade_target*`; no account group API |
| 14. 多 provider 行情聚合 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 颠覆性重构 | `docs/scenarios/api_gaps/api_contract_s14_multi_provider_market_aggregation.rs`; no public provider aggregation facade |
| 15. 实盘 / 模拟 / 回放切换 | 勉强 | 高 | 少量 | 少量 | 中 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s15_live_sim_replay_switch.rs`; builders have targets/replay URL, but no common strategy runtime |
| 16. 历史行情回放 | 不自然 | 高 | 少量 | 少量 | 中 | 中 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s16_history_replay_strategy.rs`; `DataClient` history series; `SessionClient::replay_step*` |
| 17. 研究场景 | 自然 | 低 | 无 | 无 | 无 | 低 | API 微调 | `crates/tqsdk-data/examples/api_contract_s17_research_kline_batch.rs`; `DataClient::get_kline_data_series` |
| 18. 本地行情缓存读写 | 无法表达 | 高 | 严重 | 严重 | 中 | 高 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s18_local_market_cache.rs`; no live cache writer/reader contract |
| 19. 风控前置 | 不自然 | 高 | 少量 | 无 | 高 | 低 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s19_pre_trade_risk.rs`; `TaskHost::insert_order_guarded` only guards task ownership |
| 20. 生产守护进程 | 勉强 | 中 | 少量 | 少量 | 中 | 中 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s20_production_daemon_health.rs`; `docs/scenarios/api_gaps/api_contract_s20_production_daemon.rs`; `TqStream::health`; `StreamHealthSnapshot`; health snapshot covered, metrics/graceful shutdown still gap |
| 21. 慢消费者隔离 | 勉强 | 中 | 少量 | 少量 | 低 | 中 | API 微调 | `crates/tqsdk-stream/examples/api_contract_s21_slow_consumer_isolation.rs`; `CommitStream`; `StreamFacadeError::Lagged` |
| 22. 错误诊断与重试 | 勉强 | 中 | 少量 | 少量 | 中 | 低 | 局部重构 | `crates/tqsdk-stream/examples/api_contract_s22_error_diagnosis_retry.rs`; `SessionFacadeError`; `StreamFacadeError`; `TradeSessionEvent` |
| 23. 合约信息查询与标准化 | 自然 | 低 | 无 | 无 | 无 | 无 | API 微调 | `crates/tqsdk-session/examples/api_contract_s23_contract_metadata.rs`; `SessionClient::query_symbol_info`; `Quote` metadata fields |
| 24. 最小可测试策略 | 无法表达 | 高 | 严重 | 严重 | 中 | 低 | 局部重构 | `docs/scenarios/api_gaps/api_contract_s24_testable_strategy.rs`; hidden `*_for_test` helpers; no public fake market/broker |

## 主要结论

1. 当前最自然的终端用户场景是：零门槛 wait quote、低层裸行情直通、研究 K线批处理、合约 metadata 查询。
2. 交易相关场景的主要 API gap 不是 core command 能力缺失，而是用户级 execution/risk abstraction 不足。普通登录、限价单、部分成交撤单和 session-scoped reconnect-safe order intent 已具备薄 facade；跨进程持久恢复、风控和组合执行仍需继续补齐。
3. `tqsdk-stream` 的底座方向正确，quote 订阅、动态 quote handle、混合 market event 和 health snapshot 已有薄 facade；慢消费者 sink、完整 daemon supervisor/metrics 仍停留在底层组合能力，距离终端用户契约仍有明显 gap。
4. 多 provider 聚合、统一策略 runtime、历史回放驱动策略、本地行情缓存、fake broker/test harness 都是新 facade/tooling 层问题，不应下沉到 `tqsdk-core` 或 `tqsdk-session`。
