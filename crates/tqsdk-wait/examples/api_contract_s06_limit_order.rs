//! Scenario: 普通限价下单
//!
//! User goal:
//! - 登录交易账户
//! - 提交普通限价单
//! - 等待订单状态变化并打印成交结果
//!
//! API contract:
//! - 下单参数是 typed order request，而不是 `serde_json::Value`
//! - 用户能提供稳定 client intent id，SDK 返回 order ticket
//! - 登录、账户 ready、订单状态等待是用户级 API
//! - 订单状态用 typed lifecycle 表达
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value` 作为价格参数
//! - 手写 `TradeLoginCommand`
//! - `RuntimeCommand::Trade`
//! - 字符串判断订单状态
//!
//! Regression signal:
//! - 用户必须手动提交 login command
//! - 价格和 offset 需要靠 loosely typed JSON/string 表达
//! - retry 代码必须自己维护“是否已经提交过”的 bool
//! - 等待成交只能写状态轮询模板
//!
//! Review questions:
//! - 当前 API 是否自然表达普通限价单？
//! - 是否暴露交易协议细节？
//! - 是否存在资金安全或重复下单风险？

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
    let client_order_id =
        std::env::var("TQ_CLIENT_ORDER_ID").unwrap_or_else(|_| "example-limit-order-001".into());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target(broker_id.clone(), account_id.clone())
        .build()
        .await?;
    let account = api
        .login_trade_account(
            broker_id.as_str(),
            account_id.as_str(),
            account_password.as_str(),
            TradeAccountType::Future,
            Some(deadline),
        )
        .await?;
    let balance = account.load(&api)?.balance;
    println!("account={} balance={}", account_id, balance);

    let ticket = api
        .limit_order(account_id.as_str(), "SHFE.au2602")
        .client_intent(client_order_id)
        .buy_open(1)
        .at(480.0)
        .send_once()
        .await?;

    let finished = ticket
        .wait_terminal_until(
            &mut api,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    println!(
        "order={} lifecycle={}",
        finished.order_id,
        finished.lifecycle.as_str()
    );

    Ok(())
}
