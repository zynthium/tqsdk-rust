//! Scenario: 简单策略
//!
//! User goal:
//! - 行情触发下单
//! - 成交后更新持仓
//! - 触发止盈/止损后平仓
//!
//! API contract:
//! - 策略循环能在一个稳定状态截面内读取 quote/account/position
//! - 下单和平仓通过 typed execution API 表达
//! - 成交回报和持仓更新由 SDK 对齐，不要求用户维护第二份状态
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 手写本地 position cache 作为真实资金依据
//! - 字符串解析订单状态
//! - `RuntimeCommand::Trade`
//! - provider 内部 session / protocol type
//!
//! Regression signal:
//! - 策略必须在 quote stream 和 trade stream 之间自行同步状态
//! - 止盈止损可能基于过期持仓
//! - 用户必须手动防止重复下单
//!
//! Review questions:
//! - 当前 API 是否自然表达最小策略？
//! - 状态截面是否一致？
//! - 资金安全风险应由 task 层还是 strategy facade 处理？

use std::time::Duration;

use tqsdk_core::TradeAccountType;
use tqsdk_task::{RiskEngine, StrategyHost, TaskHost};
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = std::env::var("TQ_AUTH_USER")?;
    let pass = std::env::var("TQ_AUTH_PASS")?;
    let broker_id = std::env::var("TQ_BROKER_ID")?;
    let account_id = std::env::var("TQ_ACCOUNT_ID")?;
    let account_password = std::env::var("TQ_ACCOUNT_PASSWORD")?;
    let symbol = std::env::var("TQ_STRATEGY_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".into());
    let entry = std::env::var("TQ_STRATEGY_ENTRY")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(480.0);
    let take_profit = std::env::var("TQ_STRATEGY_TAKE_PROFIT")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(entry + 10.0);
    let stop_loss = std::env::var("TQ_STRATEGY_STOP_LOSS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(entry - 10.0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

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
        Some(deadline),
    )
    .await?;
    wait_quote_ready(&mut api, symbol.as_str(), deadline).await?;

    let risk = RiskEngine::new()
        .max_order_volume(1)
        .min_available(1_000.0)
        .max_net_position(1)
        .max_price_deviation(50.0);
    let host = TaskHost::new(api).with_risk(risk);
    let mut strategy = StrategyHost::builder(host)
        .account(account_id.as_str())
        .quote(symbol.as_str())
        .build()
        .await?;

    while let Some(mut ctx) = strategy.next(Some(deadline)).await? {
        let quote = ctx.quote(symbol.as_str())?;
        let position = ctx.position(account_id.as_str(), symbol.as_str())?;

        if quote.last_price > entry && position.pos_long == 0 {
            ctx.orders(account_id.as_str())
                .buy_open(symbol.as_str(), 1)
                .limit(quote.last_price)
                .send_once("simple-strategy-entry-1")
                .await?;
        }

        if position.pos_long > 0
            && (quote.last_price >= take_profit || quote.last_price <= stop_loss)
        {
            let task = ctx
                .target_pos(account_id.as_str(), symbol.as_str())
                .build()?;
            task.set_target_volume(0)?;
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            break;
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
