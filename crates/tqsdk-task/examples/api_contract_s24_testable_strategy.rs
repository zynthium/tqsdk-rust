//! Scenario: 最小可测试策略
//!
//! User goal:
//! - 用 fake market data / fake broker 做单元测试
//! - 不连接真实服务
//! - 断言策略发出的订单和状态变化
//!
//! API contract:
//! - public fake provider/test harness 可构造行情、成交、拒单、跨 step 部分成交、测试时钟、延迟成交和 broker 断线恢复
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
//! - fake broker 无法覆盖 reject 或跨 step partial fill
//! - fake broker 无法控制测试时间、延迟成交或断线恢复
//! - 策略逻辑和真实运行入口不是同一个 context
//!
//! Review questions:
//! - 当前 API 是否自然表达可测试策略？
//! - 是否暴露测试专用内部细节？
//! - fake broker 是否复用真实 typed 下单入口？

use std::time::Duration;

use tqsdk_core::OrderLifecycle;
use tqsdk_task::StrategyHost;
use tqsdk_task::testing::{
    FakeBroker, FakeBrokerConnectionStatus, FakeMarket, StrategyTestClock, StrategyTestHarness,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> tqsdk_task::Result<()> {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote("SHFE.rb2601", 3_678.0)
                .account("sim", 80_000.0)
                .position("sim", "SHFE.rb2601", 0),
        )
        .broker(
            FakeBroker::new()
                .partial_fills([1, 1, 1])
                .disconnect_for_steps(1)
                .latency_steps(1),
        )
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
            .buy_open("SHFE.rb2601", 3)
            .limit(quote.last_price)
            .send_once("test-entry-1")
            .await?;
    }

    let first_step = ctx.finish_test_step().await?;
    assert_eq!(
        first_step.broker_connection_status(),
        FakeBrokerConnectionStatus::Disconnected
    );
    assert_eq!(first_step.pending_orders(), 1);
    assert!(first_step.orders().is_empty());

    let reconnected = ctx.finish_test_step().await?;
    assert_eq!(
        reconnected.broker_connection_status(),
        FakeBrokerConnectionStatus::Reconnected
    );
    assert_eq!(reconnected.pending_orders(), 1);
    assert!(reconnected.orders().is_empty());

    let first_fill = ctx.finish_test_step().await?;
    assert_eq!(
        first_fill.broker_connection_status(),
        FakeBrokerConnectionStatus::Connected
    );
    assert_eq!(first_fill.orders().len(), 1);
    assert_eq!(
        first_fill.orders()[0].lifecycle,
        OrderLifecycle::PartiallyFilled
    );
    assert_eq!(first_fill.orders()[0].volume_left, 2);
    assert_eq!(first_fill.trades().len(), 1);
    assert_eq!(first_fill.pending_orders(), 1);
    assert_eq!(first_fill.position("sim", "SHFE.rb2601")?.pos_long, 1);

    let second_fill = ctx.finish_test_step().await?;
    assert_eq!(second_fill.orders().len(), 1);
    assert_eq!(
        second_fill.orders()[0].lifecycle,
        OrderLifecycle::PartiallyFilled
    );
    assert_eq!(second_fill.orders()[0].volume_left, 1);
    assert_eq!(second_fill.trades().len(), 1);
    assert_eq!(second_fill.pending_orders(), 1);
    assert_eq!(second_fill.position("sim", "SHFE.rb2601")?.pos_long, 2);

    let report = ctx.finish_test_step().await?;
    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.orders()[0].lifecycle, OrderLifecycle::Filled);
    assert_eq!(report.orders()[0].volume_left, 0);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(
        report.trades()[0].trade_date_time,
        1_800_000_001_250_000_000
    );
    assert_eq!(report.position("sim", "SHFE.rb2601")?.pos_long, 3);
    Ok(())
}
