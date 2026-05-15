//! Scenario: Wait security trade live refs
//!
//! Primary user layer:
//! - 单策略作者
//! - 证券交易状态观察用户
//!
//! Intended crate path:
//! - `tqsdk-wait`
//!
//! Lower-level escape hatch:
//! - 需要多消费者事件流时使用 `tqsdk-stream`
//!
//! Non-goal:
//! - direct query metadata、证券交易策略封装、统一 futures/securities overlay
//!
//! User goal:
//! - 在 wait facade 中持有证券账户、持仓、委托和成交 live refs
//! - 通过 `is_ready` / `snapshot` 做可选读取
//! - 通过 `is_changing` 解释最近一次 commit
//!
//! API contract:
//! - 证券 account/position/order/trade 使用独立 typed refs
//! - missing object 使用 `snapshot -> Option<T>` 或 `is_ready` 表达
//! - 这些对象属于 wait 的 diff-backed live state surface
//!
//! Forbidden:
//! - GraphQL / metadata direct query
//! - provider 内部 trade path
//! - 手动 `StatePath`
//! - 用字符串解析证券交易对象类型
//! - 本地第二棵交易状态树
//!
//! Regression signal:
//! - securities refs 与 futures refs 只能用 untyped JSON 区分
//! - 证券交易对象只能通过 raw state path 读取
//! - security refs 被移动到 session direct-query API
//!
//! Review questions:
//! - securities refs 是否能独立于 futures order/account refs 被发现？
//! - optional snapshot 是否比 load panic/错误路径更适合文档示例？

use std::time::Duration;

use tqsdk_wait::TqApiBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let account_id = read_optional_env("TQ_TRADE_ACCOUNT_ID").unwrap_or_else(|| "sim".to_string());
    let symbol =
        read_optional_env("TQ_SECURITY_SYMBOL").unwrap_or_else(|| "SSE.600000".to_string());
    let order_id = read_env("TQ_SECURITY_ORDER_ID")?;
    let trade_id = read_env("TQ_SECURITY_TRADE_ID")?;

    let mut api = TqApiBuilder::new(user, pass)
        .stock_market()
        .trade_target_tqkq()
        .build()
        .await?;

    let account = api.security_account(account_id.as_str());
    let position = api.security_position(account_id.as_str(), symbol.as_str());
    let order = api.security_order(account_id.as_str(), order_id.as_str());
    let trade = api.security_trade(account_id.as_str(), trade_id.as_str());

    println!(
        "watching security refs account={} symbol={} order={} trade={}",
        account_id, symbol, order_id, trade_id
    );
    println!("default run is read-only; this example submits no trade commands");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Some(step) = api
            .step_until(Some(tokio::time::Instant::now() + Duration::from_secs(1)))
            .await?
        else {
            continue;
        };

        if step.is_changing(&account) {
            println!("security_account_ready={}", account.is_ready()?);
        }
        if step.is_changing(&position) {
            println!("security_position_snapshot={:?}", position.snapshot()?);
        }
        if step.is_changing(&order) {
            println!("security_order_ready={}", order.is_ready()?);
        }
        if step.is_changing(&trade) {
            println!("security_trade_snapshot={:?}", trade.snapshot()?);
        }
    }

    Ok(())
}
