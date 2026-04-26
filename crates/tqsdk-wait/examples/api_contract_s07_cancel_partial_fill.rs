//! Scenario: 撤单与部分成交
//!
//! User goal:
//! - 观察订单部分成交
//! - 撤掉剩余未成交量
//! - 确认最终订单状态
//!
//! API contract:
//! - public API 暴露 typed order lifecycle 和剩余量
//! - 撤单可直接作用于订单 handle
//! - 最终状态等待不要求用户写重复状态机
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 字符串解析 `status`
//! - `RuntimeCommand::Trade`
//! - 业务代码自行推断 terminal state
//!
//! Regression signal:
//! - 用户必须在循环里组合 `is_dead` / `status` / `volume_left`
//! - 撤单只接受裸 order id，丢失订单归属上下文
//! - 部分成交和撤单终态没有 typed helper
//!
//! Review questions:
//! - 当前 API 是否自然表达部分成交撤单？
//! - 订单生命周期是否类型安全？
//! - 是否存在漏撤或误判终态的资金安全风险？

use tqsdk_core::{TradeDirection, TradeOffset};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()
        .await?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;

    let order = api
        .insert_limit_order(
            account_id.as_str(),
            "SHFE.au2602",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            3,
            480.0,
        )
        .await?;

    let partial = order.wait_partially_filled(&mut api).await?;
    println!(
        "partial order={} left={}",
        partial.order_id, partial.volume_left
    );

    order.cancel_remaining(&mut api).await?;

    let final_state = order.wait_terminal(&mut api).await?;
    println!(
        "final order={} lifecycle={}",
        final_state.order_id,
        final_state.lifecycle.as_str()
    );

    Ok(())
}
