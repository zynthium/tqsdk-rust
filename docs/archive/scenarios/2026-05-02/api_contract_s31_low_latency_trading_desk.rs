//! Scenario: 高频交易柜台低延迟 profile
//!
//! Primary user layer:
//! - 高频 / 基础设施用户
//! - 自研交易柜台开发者
//!
//! Intended crate path:
//! - hot path: `tqsdk-core` + `tqsdk-session`
//! - observability / fan-out: `tqsdk-stream`
//! - guarded execution primitives: `tqsdk-task`
//!
//! Lower-level escape hatch:
//! - `RuntimeReader::{cursor,read_market_state,read_trade_state}`
//! - `SessionClient::{subscribe_quotes,progress_once}`
//!
//! Non-goal:
//! - 策略平台
//! - 自动 hedge / flatten / 补单引擎
//! - 内置 OMS / GUI / HTTP endpoint
//! - 历史序列 memmap cache
//! - 多 provider 行情聚合
//!
//! User goal:
//! - 在一个可审计的低延迟循环里消费行情、读取账户/持仓、执行风控并提交订单
//! - hot path 使用 partition read surface，不做 full snapshot decode
//! - 决策链路能记录 market receive、commit、decision、order submit 和 ack 时间点
//! - 风控、订单 intent 和 command/order 状态使用 typed contract
//! - 慢日志、落盘、指标导出不阻塞行情/下单主循环
//!
//! API contract:
//! - 提供 trading desk profile 或 contract example，展示 core/session/task/stream
//!   如何组合成低延迟链路
//! - 行情 hot path 只读取同一 runtime state tree 和同一 commit/revision
//! - market/trade read 必须优先使用 partition read surface
//! - 订单提交必须通过 typed intent / command ledger / order state machine
//! - pre-trade risk gate 必须可在同一 revision-bound snapshot 内运行
//! - latency report 是 typed API，不要求用户散落 `Instant::now()` 和日志字符串
//! - 慢消费者隔离复用 `tqsdk-stream` managed sink / WAL / journal foundation
//! - 不要求用户手写 channel、unbounded queue 或 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 高频 hot path 进入 `tqsdk-data` / history cache / memmap cache
//! - 每个 tick 做 full snapshot clone 或 JSON path decode
//! - 字符串判断 command/order/trade status
//! - 慢日志或落盘 future await 在行情/下单主循环
//! - unbounded channel 隐藏背压
//! - 用户自己维护第二棵账户、持仓或订单状态树作为资金依据
//!
//! Regression signal:
//! - 高频用户只能通过 `TqApi::wait_update()` 或厚 facade 才能下单
//! - 低延迟用户必须手写 runtime command 或 provider 私有 order packet
//! - 风控检查与下单使用不同 revision，导致读到非一致截面
//! - 慢 sink 导致 market/trade hot loop lag 或丢失风险不可见
//! - 无法定位延迟来自 market route、commit、risk、order submit 还是 ack
//!
//! Review questions:
//! - 当前 core/session/task/stream primitives 是否足以拼出低延迟柜台主循环？
//! - 是否需要新增 thin trading desk profile，还是只补正式 contract example？
//! - typed latency report 应落在 `tqsdk-session`、`tqsdk-task` 还是独立 profiling helper？
//! - 如何保证该 profile 不演变成策略平台、OMS 或自动执行系统？
//!
//! Archived status:
//! S31 已提升为正式 compiled contract：
//! `crates/tqsdk-task/examples/api_contract_s31_low_latency_trading_desk.rs`。
//! 该 example 将现有 core/session/task/stream primitives 串成同一条低延迟
//! hot-path 链路；S30 history mmap cache 已闭环但明确不进入该 hot path。
//!
//! Closed API gap:
//! 正式 S31 example 覆盖 session 自驱动 quote hot path、同 revision
//! market/trade 分区读、risk gate、typed order intent、latency report 和
//! slow sink isolation，避免柜台用户自行把 data/cache、full snapshot、字符串状态或
//! 手写 channel 带进关键链路。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut desk = TradingDeskProfile::builder(session)
//!     .subscribe_quotes(["SHFE.au2602"])
//!     .risk_engine(risk_engine)
//!     .latency_probe(TradingLatencyProbe::enabled())
//!     .slow_sink_profile(StreamSinkProfile::reliable_jsonl(wal_path, journal_path))
//!     .build()
//!     .await?;
//!
//! while let Some(event) = desk.next_market_event().await? {
//!     let quote = desk.market_state().quote(event.symbol())?;
//!     let position = desk.trade_state().position(account_id, event.symbol())?;
//!     let decision = strategy.decide(event.revision(), quote, position)?;
//!
//!     if let Some(order) = decision.order_intent() {
//!         let report = desk.check_risk(order)?;
//!         if report.is_allowed() {
//!             let ticket = desk.submit_order(order).await?;
//!             desk.latency().record_order_ticket(&ticket);
//!         }
//!     }
//! }
//! ```

fn main() {}
