//! Scenario: 最小可测试策略
//!
//! User goal:
//! - 用 fake market data / fake broker 做单元测试
//! - 不连接真实服务
//! - 断言策略发出的订单和状态变化
//!
//! API contract:
//! - public fake provider/test harness 可构造行情、成交、拒单、重连
//! - 策略测试不使用 hidden `*_for_test` API
//! - fake broker 与真实 broker 实现同一策略 contract
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
//! - fake broker 无法覆盖 partial fill/reject/reconnect
//! - 策略逻辑和真实运行入口不是同一个 trait
//!
//! Review questions:
//! - 当前 API 是否自然表达可测试策略？
//! - 是否暴露测试专用内部细节？
//! - 应通过 API 微调还是新增 testing crate？
//!
//! API gap:
//! 当前有若干 `#[doc(hidden)] ..._for_test` 内部入口，但没有稳定 public
//! fake market/fake broker/test harness。
//!
//! 理想用户代码草案：
//! ```ignore
//! #[tokio::test]
//! async fn strategy_buys_when_breakout() -> Result<()> {
//!     let harness = StrategyTestHarness::new()
//!         .market(FakeMarket::new().quote("SHFE.au2602", 481.0))
//!         .broker(FakeBroker::new().fill_all())
//!         .build();
//!
//!     let report = harness.run(MyStrategy::default()).await?;
//!     assert_eq!(report.orders().len(), 1);
//!     assert_eq!(report.position("SHFE.au2602").net(), 1);
//!     Ok(())
//! }
//! ```

fn main() {}
