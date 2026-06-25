//! Scenario: 多账户下单
//!
//! User goal:
//! - 同一策略按比例向多个账户下单
//! - 每个账户状态隔离
//! - 汇总执行结果
//!
//! API contract:
//! - 多账户是 typed account group，而不是业务代码里的字符串循环
//! - 比例拆单、最小手数和 deterministic client order id 由 task 层处理
//! - 每个账户订单、成交和错误隔离可追踪
//! - `max_unhedged` 在账户间分配裸露持续超时后返回 typed `NeedsAttention`
//! - multi-account report 绑定 runtime revision，作为审计/resume 的 public foundation
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 在业务代码里循环多个 `insert_order`
//! - 用共享 `HashMap` 拼账户执行状态
//! - 字符串判断订单状态或错误类型
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 一个账户拒单导致其他账户 outcome 无法解释
//! - 比例拆单、尾差和风控散落在用户代码
//! - 多账户状态相互污染
//!
//! Review questions:
//! - 当前 API 是否自然表达多账户执行？
//! - 是否有状态隔离和资金安全风险？
//! - 多账户能力是否留在 task 层，而不是下沉到 core/session/wait？
//!
//! Current limitation:
//! - `AccountFailurePolicy::ReportExposure` 只报告账户间裸露，不自动补单、调仓或平仓。

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_task::TaskHost;
use tqsdk_task::order_groups::{AccountFailurePolicy, MultiAccountOrderOutcome, Ratio};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_a = std::env::var("TQ_BROKER_ID_A")?;
    let account_a = std::env::var("TQ_ACCOUNT_ID_A")?;
    let password_a = std::env::var("TQ_ACCOUNT_PASSWORD_A")?;
    let broker_b = std::env::var("TQ_BROKER_ID_B")?;
    let account_b = std::env::var("TQ_ACCOUNT_ID_B")?;
    let password_b = std::env::var("TQ_ACCOUNT_PASSWORD_B")?;
    let symbol = std::env::var("TQ_MULTI_ACCOUNT_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target(broker_a.clone(), account_a.clone())
        .trade_target(broker_b.clone(), account_b.clone())
        .build()
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    api.login_trade_account(
        broker_a.as_str(),
        account_a.as_str(),
        password_a.as_str(),
        TradeAccountType::Future,
        Some(deadline),
    )
    .await?;
    api.login_trade_account(
        broker_b.as_str(),
        account_b.as_str(),
        password_b.as_str(),
        TradeAccountType::Future,
        Some(deadline),
    )
    .await?;
    wait_quote_ready(&mut api, symbol.as_str(), deadline).await?;

    let mut host = TaskHost::new(api);
    let accounts = host
        .account_group()
        .add(account_a.as_str(), Ratio::new(7, 10)?)
        .add(account_b.as_str(), Ratio::new(3, 10)?)
        .min_volume_per_account(1)
        .build()?;

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-au-001")
        .max_unhedged(Duration::from_secs(2))
        .on_account_failed(AccountFailurePolicy::ReportExposure)
        .buy_open(symbol.as_str(), 10)
        .limit(480.0)
        .send_once()
        .await?;

    let report = ticket.report(host.api())?;
    println!(
        "multi-account rev={} group={} accounts={} status={:?}",
        report.revision().get(),
        report.group_id(),
        report.accounts().len(),
        report.status()
    );

    match ticket.wait_finished(&mut host, Some(deadline)).await? {
        MultiAccountOrderOutcome::AllFilled { accounts } => {
            for account in accounts {
                println!(
                    "{} filled {}/{}",
                    account.account_id, account.filled_volume, account.requested_volume
                );
            }
        }
        MultiAccountOrderOutcome::NeedsAttention {
            filled_accounts,
            unfilled_accounts,
            ..
        } => {
            println!("filled={filled_accounts:?}, unfilled={unfilled_accounts:?}");
        }
        other => {
            println!("terminal multi-account outcome: {other:?}");
        }
    }

    Ok(())
}

async fn wait_quote_ready(
    api: &mut tqsdk_wait::TqApi,
    symbol: &str,
    deadline: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    let quote = api.quote(symbol).await?;
    while let Some(step) = api.step_until(Some(deadline)).await? {
        if step.is_changing(&quote) && quote.snapshot()?.is_some() {
            return Ok(());
        }
    }
    Err(format!("quote not ready: {symbol}").into())
}
