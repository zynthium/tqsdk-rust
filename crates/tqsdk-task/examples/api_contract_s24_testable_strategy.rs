//! Scenario: 最小可测试策略
//!
//! User goal:
//! - 用 fake market data / fake broker 做单元测试
//! - 不连接真实服务
//! - 断言策略发出的订单和状态变化
//!
//! API contract:
//! - public fake provider/test harness 可构造行情、成交、拒单、部分成交、测试时钟和延迟成交
//! - 策略测试不使用 hidden `*_for_test` API
//! - fake broker 与真实 broker 复用同一 `StrategyHost` / `StrategyContext`
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
//! - fake broker 无法控制测试时间或延迟成交
//! - 策略逻辑和真实运行入口不是同一个 context
//!
//! Review questions:
//! - 当前 API 是否自然表达可测试策略？
//! - 是否暴露测试专用内部细节？
//! - fake broker 是否复用真实 typed 下单入口？

use std::time::Duration;

use tqsdk_task::StrategyHost;
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestClock, StrategyTestHarness};

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk_task::Result<()> {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(FakeBroker::new().fill_all().latency_steps(1))
        .clock(
            StrategyTestClock::new(1_800_000_000_000_000_000).step_by(Duration::from_millis(250)),
        )
        .build()?;

    let mut strategy = StrategyHost::builder(harness.into_task_host())
        .account("sim")
        .quote("SHFE.rb2601")
        .build()
        .await?;

    let mut ctx = strategy.next_once().await?;
    let quote = ctx.quote("SHFE.rb2601")?;
    let position = ctx.position("sim", "SHFE.rb2601")?;

    if quote.last_price > 3_600.0 && position.pos_long == 0 {
        ctx.orders("sim")
            .buy_open("SHFE.rb2601", 1)
            .limit(quote.last_price)
            .send_once("test-entry-1")
            .await?;
    }

    let first_step = ctx.finish_test_step().await?;
    assert_eq!(first_step.pending_orders(), 1);
    assert!(first_step.orders().is_empty());

    let report = ctx.finish_test_step().await?;
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(
        report.trades()[0].trade_date_time,
        1_800_000_000_250_000_000
    );
    assert_eq!(report.position("sim", "SHFE.rb2601")?.pos_long, 1);
    Ok(())
}
