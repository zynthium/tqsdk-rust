//! Scenario: 断线重连中的订单一致性
//!
//! User goal:
//! - 使用稳定 client intent id 下单
//! - 断线重连或 retry 后不重复提交同一笔订单
//! - 等待 typed terminal state，避免把未知状态误判为已成交或已撤单
//!
//! API contract:
//! - 用户能传入稳定 client order id / intent id
//! - 同一 `SessionClient` 内重复 `send_once()` 不会重复提交
//! - SDK 返回 typed `OrderTicketState`
//! - terminal wait 能区分 filled / rejected / failed / cancelled / unknown
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 业务代码用本地 bool 记录“是否已经下单”
//! - 字符串判断 runtime command status
//! - provider 内部 reconnect event
//! - 手写订单去重表
//!
//! Regression signal:
//! - 重连后需要用户自己扫订单表和本地 intent 表
//! - 相同策略信号可能提交第二笔订单
//! - 用户必须解析 command status 字符串或 order.status 字符串
//!
//! Review questions:
//! - 当前 API 是否能自然表达重连订单一致性？
//! - 是否存在 P0 级重复下单风险？
//! - 需要 API 微调、局部重构还是新增执行一致性层？

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_wait::{OrderTicketState, TqApiBuilder};

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
        std::env::var("TQ_CLIENT_ORDER_ID").unwrap_or_else(|_| "strategy-a-open-001".into());

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
        .buy_open(1)
        .at(limit_price)
        .send_once()
        .await?;

    let retry_ticket = api
        .limit_order(account_id.as_str(), symbol.as_str())
        .client_intent(client_order_id.as_str())
        .buy_open(1)
        .at(limit_price)
        .send_once()
        .await?;
    assert!(!retry_ticket.was_submitted());

    let state = ticket
        .wait_reconnect_safe_terminal_until(
            &mut api,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;

    match state {
        OrderTicketState::Filled { order, .. } => {
            println!("filled order={} left={}", order.order_id, order.volume_left);
        }
        OrderTicketState::Cancelled { order, .. } => match order {
            Some(order) => println!(
                "cancelled order={} left={}",
                order.order_id, order.volume_left
            ),
            None => println!("cancelled before order materialized"),
        },
        OrderTicketState::Rejected { command_id, order } => {
            println!("rejected command={command_id:?} order={order:?}");
        }
        OrderTicketState::Failed { command_id, order } => {
            println!("failed command={command_id:?} order={order:?}");
        }
        OrderTicketState::Unknown { command_id } => {
            println!("unknown terminal state command={command_id:?}");
        }
        OrderTicketState::CommandPending { .. } | OrderTicketState::Live { .. } => {}
    }

    Ok(())
}
