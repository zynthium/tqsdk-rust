//! Scenario: 撤单与部分成交
//!
//! User goal:
//! - 观察订单部分成交
//! - 撤掉剩余未成交量
//! - 确认最终订单状态
//!
//! API contract:
//! - public API 暴露 typed order lifecycle 和剩余量
//! - 撤单可直接作用于 reconnect-safe order ticket / 订单 handle
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

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let symbol = std::env::var("TQ_ORDER_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let limit_price = std::env::var("TQ_ORDER_LIMIT_PRICE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(480.0);
    let client_order_id =
        std::env::var("TQ_CLIENT_ORDER_ID").unwrap_or_else(|_| "partial-cancel-001".into());
    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target(broker_id.clone(), account_id.clone())
        .build()
        .await?;
    api.login_trade_account(
        broker_id.as_str(),
        account_id.as_str(),
        account_password.as_str(),
        TradeAccountType::Future,
        Some(tokio::time::Instant::now() + Duration::from_secs(30)),
    )
    .await?;

    let ticket = api
        .limit_order(account_id.as_str(), symbol.as_str())
        .client_intent(client_order_id.as_str())
        .buy_open(3)
        .at(limit_price)
        .send_once()
        .await?;

    let partial = ticket
        .wait_partially_filled_until(
            &mut api,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    println!(
        "partial order={} left={}",
        partial.order_id, partial.volume_left
    );

    ticket.cancel_remaining(&mut api).await?;

    let final_state = ticket
        .wait_terminal_until(
            &mut api,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    println!(
        "final order={} lifecycle={}",
        final_state.order_id,
        final_state.lifecycle.as_str()
    );

    Ok(())
}
