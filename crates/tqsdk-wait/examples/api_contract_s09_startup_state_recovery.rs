//! Scenario: 启动后状态恢复
//!
//! User goal:
//! - 登录后恢复订阅
//! - 同步订单 / 成交 / 持仓 / 资金
//! - 在第一轮业务决策前得到一致初始截面
//!
//! API contract:
//! - SDK 提供明确的 startup recovery barrier
//! - market subscriptions 与 trade state sync 都有 typed ready signal
//! - 用户不需要知道 route/pending-route/replay 细节
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `SessionRuntime::recover`
//! - `RuntimeCommand`
//! - 手写多阶段 ready flag
//! - 业务代码自建状态恢复 cache
//!
//! Regression signal:
//! - 策略必须在多个 stream 中猜测“是否恢复完成”
//! - 启动后第一笔下单可能基于不完整持仓
//! - 订阅恢复和交易状态恢复没有同一个 barrier
//!
//! Review questions:
//! - 当前 API 是否自然表达启动恢复？
//! - 是否有状态一致性或资金安全风险？
//! - 缺口应由 facade 微调还是架构新增 recovery surface？

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

    let recovered = api
        .startup_recovery()
        .quotes(["SHFE.au2602", "SHFE.ag2606"])
        .trade_account(account_id.as_str())
        .deadline(deadline)
        .await?;
    assert!(recovered.is_ready());

    let quote = api.quote_ref("SHFE.au2602").load(&api)?;
    let account = account.load(&api)?;
    println!(
        "ready revision={} quote={} last_price={} account={} available={}",
        recovered.revision.get(),
        quote.instrument_id,
        quote.last_price,
        account.user_id,
        account.available
    );

    Ok(())
}
