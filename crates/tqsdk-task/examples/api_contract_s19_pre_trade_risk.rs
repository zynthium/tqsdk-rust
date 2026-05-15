//! Scenario: 风控前置
//!
//! User goal:
//! - 下单前检查资金、持仓、价格、合约 tick size、单笔限额、开仓限额和订单频率
//! - 拒绝不安全订单
//! - 留下可审计的 typed 拒绝原因
//!
//! API contract:
//! - 风控规则是 typed public API
//! - 下单入口强制经过 `TaskHost` 的 risk gate 和 ownership guard
//! - 风控读取账户/持仓/quote 时使用同一稳定状态截面
//! - 风控检查可以返回带 revision 的 typed report 供审计
//! - 风控试算可以返回 revision-bound projection，供下单前解释 projected position/notional
//! - 合约 metadata 通过 `InstrumentSpec` 接入 task risk，而不是散落在策略代码
//! - 官方同类的基础开仓次数、开仓手数和订单频率规则由 `RiskEngine` 记录本进程内用量
//! - 订单价格和方向通过 typed builder 表达
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户在策略里散写 if 判断作为唯一风控
//! - `serde_json::Value` 表达订单价格
//! - 字符串判断合约/交易所规则
//! - 旁路下单绕过 guard
//!
//! Regression signal:
//! - 下单前资金/持仓/quote 不是同一 revision
//! - tick size / contract multiplier 需要用户自己查询和手写校验
//! - 规则拒绝原因不可审计
//! - guarded 和 unguarded order API 容易混用
//! - 用户必须手动同步订单去重、开仓限额和订单频率状态
//!
//! Review questions:
//! - 当前 API 是否自然表达前置风控？
//! - 风控是否暴露 provider/protocol 细节？
//! - 是否存在资金安全或重复下单风险？

use std::time::Duration;

use tqsdk_core::{TradeAccountType, TradeDirection, TradeOffset};
use tqsdk_task::{RiskEngine, TaskError, TaskHost, TaskOrderIntent};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let symbol = std::env::var("TQ_ORDER_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let exchange_id = symbol
        .split_once('.')
        .map(|(exchange_id, _)| exchange_id.to_owned())
        .unwrap_or_else(|| "SHFE".into());
    let limit_price = std::env::var("TQ_ORDER_LIMIT_PRICE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(480.0);
    let client_order_id =
        std::env::var("TQ_CLIENT_ORDER_ID").unwrap_or_else(|_| "risk-entry-001".into());

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
    api.quote(symbol.as_str()).await?;
    let instrument_specs = api
        .session()
        .query_instrument_specs(&[symbol.as_str()])
        .await?;

    let risk = RiskEngine::new()
        .max_order_volume(3)
        .daily_open_count_limit(10, [symbol.as_str()])
        .daily_open_volume_limit(30, [symbol.as_str()])
        .accumulated_open_volume_limit(50, [symbol.as_str()])
        .order_rate_limit_per_second(20, [exchange_id.as_str()])
        .min_available(1_000.0)
        .max_net_position(5)
        .max_price_deviation(20.0)
        .instrument_specs(instrument_specs);
    let mut host = TaskHost::new(api).with_risk(risk);
    let intent = TaskOrderIntent {
        account_id: account_id.clone(),
        symbol: symbol.clone(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 1,
        limit_price: Some(limit_price),
    };

    if let Some(risk) = host.risk() {
        let projection = risk.project_order(host.api(), &intent)?;
        println!(
            "risk projection rev={} account={} symbol={} current_net={:?} projected_net={:?} price_basis={:?} price_volume={:?} multiplier={:?} notional={:?}",
            projection.revision().get(),
            projection.account_id(),
            projection.symbol(),
            projection.current_net(),
            projection.projected_net(),
            projection.price_basis(),
            projection.estimated_price_volume(),
            projection.contract_multiplier(),
            projection.estimated_notional()
        );

        let report = risk.check_report(host.api(), &intent)?;
        println!(
            "risk revision={} decision={:?}",
            report.revision().get(),
            report.decision()
        );
        if let Some(rejection) = report.decision().rejection() {
            println!("risk rejected order before submit: {rejection:?}");
            return Ok(());
        }
    }

    match host
        .orders(account_id.as_str())
        .buy_open(symbol.as_str(), 1)
        .limit(limit_price)
        .send_once(client_order_id.as_str())
        .await
    {
        Ok(ticket) => {
            println!(
                "submitted client_order_id={} submitted={}",
                ticket.client_order_id(),
                ticket.was_submitted()
            );
        }
        Err(TaskError::RiskRejected(rejection)) => {
            println!("risk rejected order: {rejection:?}");
        }
        Err(error) => return Err(error.into()),
    }

    Ok(())
}
