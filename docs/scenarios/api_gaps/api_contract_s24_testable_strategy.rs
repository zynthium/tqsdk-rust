//! Scenario: 最小可测试策略
//!
//! User goal:
//! - 用 fake market data / fake broker 做单元测试
//! - 不连接真实服务
//! - 断言策略发出的订单和状态变化
//!
//! API contract:
//! - public fake provider/test harness 可构造行情、成交、拒单、部分成交
//! - 策略测试不使用 hidden `*_for_test` API
//! - fake broker 与真实 broker 复用同一策略 context
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `#[doc(hidden)] new_for_test...`
//! - 直接写 runtime state tree
//! - provider 私有 protocol type
//! - 网络账号/真实行情依赖
//!
//! Regression signal:
//! - 单元测试只能用 live smoke 或 ignored test
//! - fake broker 无法覆盖 partial fill/reject
//! - 策略逻辑和真实运行入口不是同一个 context
//!
//! Review questions:
//! - 当前 API 是否自然表达可测试策略？
//! - 是否暴露测试专用内部细节？
//! - 应通过 API 微调还是新增 testing crate？
//!
//! Current API note:
//! `tqsdk-task::testing` 已提供最小 public `StrategyTestHarness`、
//! `FakeMarket`、`FakeBroker` 和 `StrategyTestClock`。用户可以在不连接真实服务、
//! 不调用 hidden `*_for_test` API 的情况下，用同一套
//! `StrategyHost` / `StrategyContext` 路径测试 quote 触发下单、全成、拒单、
//! 部分成交、确定性 fake broker 时间、step latency 和 broker disconnect/reconnect。
//!
//! Remaining API gap:
//! 当前 test harness 仍是 foundation：跨 step 部分成交推进、持久化恢复和更完整
//! broker 行为仍未冻结。
//!
//! 理想用户代码草案：
//! ```ignore
//! #[tokio::test]
//! async fn strategy_buys_when_breakout() -> Result<()> {
//!     let harness = StrategyTestHarness::new()
//!         .market(FakeMarket::new().quote("SHFE.au2602", 481.0))
//!         .broker(FakeBroker::new().fill_all().disconnect_for_steps(1).latency_steps(1))
//!         .clock(StrategyTestClock::new(1_800_000_000_000_000_000))
//!         .build()?;
//!
//!     let mut strategy = StrategyHost::builder(harness.into_task_host())
//!         .account("sim")
//!         .quote("SHFE.au2602")
//!         .build()
//!         .await?;
//!     let mut ctx = strategy.next_once().await?;
//!     ctx.orders("sim").buy_open("SHFE.au2602", 1).limit(481.0).send_once("entry-1").await?;
//!     assert_eq!(
//!         ctx.finish_test_step().await?.broker_connection_status(),
//!         FakeBrokerConnectionStatus::Disconnected
//!     );
//!     assert_eq!(ctx.finish_test_step().await?.pending_orders(), 1);
//!     let report = ctx.finish_test_step().await?;
//!     assert_eq!(report.orders().len(), 1);
//!     assert_eq!(report.position("sim", "SHFE.au2602")?.pos_long, 1);
//!     Ok(())
//! }
//! ```

fn main() {}
