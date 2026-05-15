//! Scenario: 账户 / 资金 / 持仓查询
//!
//! User goal:
//! - 读取账户资金快照
//! - 读取某合约持仓快照
//! - 接收后续资金 / 持仓增量变化
//!
//! API contract:
//! - 账户和持仓是 typed live refs
//! - 初始 ready 和后续 change checks 共享同一 `wait_update()` 截面
//! - 不要求用户读取底层 state path
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `serde_json::Value`
//! - `StatePath`
//! - provider 内部 session / protocol type
//! - 手写账户状态 cache
//!
//! Regression signal:
//! - 用户必须通过字符串路径读 `trade/{account_id}/...`
//! - 账户 ready 需要手写底层 command
//! - 增量和快照来自不同状态源
//!
//! Review questions:
//! - 当前 API 是否自然表达账户/持仓 live ref？
//! - 是否暴露内部路径？
//! - 是否存在状态一致性风险？

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
    let symbol = std::env::var("TQ_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());

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
            Some(tokio::time::Instant::now() + Duration::from_secs(30)),
        )
        .await?;
    let position = api.position(account_id.as_str(), symbol.as_str());

    println!("account balance={}", account.load()?.balance);

    loop {
        let Some(step) = api.step().await? else {
            continue;
        };

        if step.is_changing(&account) {
            println!("available={}", account.load()?.available);
        }

        if step.is_changing(&position) {
            let snapshot = position.load()?;
            println!("{} pos={}", symbol, snapshot.pos);
        }
    }
}
