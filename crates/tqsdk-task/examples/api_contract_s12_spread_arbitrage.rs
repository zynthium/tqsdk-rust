//! Scenario: 跨合约套利
//!
//! User goal:
//! - 两腿使用同一个 typed execution group 下单
//! - 处理成交不同步
//! - 在单腿裸露时得到 typed exposure report
//!
//! API contract:
//! - 两腿 order intent 有同一个 client group id
//! - 下单前所有腿统一经过 ownership guard 和 risk gate
//! - 用户读取 group-level outcome，而不是手写 `Vec<OrderTicket>` 状态机
//! - `max_unhedged` 在观察到裸露持续超时后返回 typed `NeedsHedge`
//! - group report 绑定 runtime revision，作为审计/resume 的 public foundation
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 两腿分别用普通 order ref 手动拼事务语义
//! - 本地 bool/Vec 追踪腿状态作为资金安全依据
//! - 字符串判断订单状态
//! - `RuntimeCommand::Trade`
//!
//! Regression signal:
//! - 单腿成交后另一腿失败只能靠业务代码临时补救
//! - 无法表达最大净敞口或 group-level outcome
//! - group outcome 无法审计
//!
//! Review questions:
//! - 当前 API 是否能安全表达跨合约套利 foundation？
//! - 是否存在 P0 级单腿裸露风险？
//! - 自动对冲应继续留在 task 层，还是拆成独立 execution policy？
//!
//! Current limitation:
//! - `HedgePolicy::ReportExposure` 只报告 typed exposure，不自动提交对冲 / 平仓单。

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_task::{ExecutionGroupOutcome, HedgePolicy, RiskEngine, TaskHost};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let leg_a = std::env::var("TQ_SPREAD_LEG_A").unwrap_or_else(|_| "SHFE.au2602".into());
    let leg_b = std::env::var("TQ_SPREAD_LEG_B").unwrap_or_else(|_| "SHFE.ag2602".into());

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
    api.quote_snapshot(
        leg_a.as_str(),
        Some(tokio::time::Instant::now() + Duration::from_secs(30)),
    )
    .await?;
    api.quote_snapshot(
        leg_b.as_str(),
        Some(tokio::time::Instant::now() + Duration::from_secs(30)),
    )
    .await?;

    let risk = RiskEngine::new()
        .max_order_volume(20)
        .min_available(1_000.0)
        .max_price_deviation(50.0);
    let mut host = TaskHost::new(api).with_risk(risk);

    let group = host
        .execution_group(account_id.as_str())
        .client_group_id("spread-example-001")
        .max_unhedged(Duration::from_secs(2))
        .on_leg_failed(HedgePolicy::ReportExposure)
        .leg(leg_a.as_str())
        .buy_open(1)
        .limit(480.0)
        .leg(leg_b.as_str())
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await?;

    let report = group.report(host.api())?;
    println!(
        "execution group rev={} group={} account={} legs={} status={:?}",
        report.revision().get(),
        report.group_id(),
        report.account_id(),
        report.legs().len(),
        report.status()
    );

    let outcome = group
        .wait_finished(
            &mut host,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;

    match outcome {
        ExecutionGroupOutcome::AllFilled { legs } => {
            println!("spread filled legs={}", legs.len());
        }
        ExecutionGroupOutcome::NeedsHedge { exposure, legs } => {
            println!("spread needs manual hedge exposure={exposure:?} legs={legs:?}");
        }
        ExecutionGroupOutcome::Rejected { legs }
        | ExecutionGroupOutcome::Failed { legs }
        | ExecutionGroupOutcome::Cancelled { legs } => {
            println!("spread did not complete legs={legs:?}");
        }
    }

    Ok(())
}
