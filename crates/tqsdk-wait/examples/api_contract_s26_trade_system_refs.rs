//! Scenario: Wait trade 与 system live refs
//!
//! Primary user layer:
//! - 单策略作者
//! - 交易状态观察用户
//!
//! Intended crate path:
//! - `tqsdk-wait`
//!
//! Lower-level escape hatch:
//! - 需要多消费者事件流时使用 `tqsdk-stream`
//!
//! Non-goal:
//! - direct query metadata、生产级风控服务、证券交易策略封装
//!
//! User goal:
//! - 在 wait facade 中持有通知、结算、风险和证券交易对象 live refs
//! - 通过 `snapshot` 做可选读取，通过 `is_changing` 解释最近一次 commit
//! - 用 `confirm_settlement` 提交结算确认命令
//!
//! API contract:
//! - trade/system 对象 ref 属于 wait 的 diff-backed live state surface
//! - missing object 使用 `snapshot -> Option<T>` 表达
//! - `confirm_settlement` 是 wait 风格 trade command wrapper
//! - 证券 account/position/order/trade 使用独立 typed refs
//!
//! Forbidden:
//! - GraphQL / metadata direct query
//! - provider 内部 trade path
//! - 手动 `StatePath`
//! - 用字符串解析交易对象类型
//! - 本地第二棵交易状态树
//!
//! Regression signal:
//! - 风险、通知、结算或证券对象只能通过 raw state path 读取
//! - `confirm_settlement` 被移到 session direct-query API
//! - securities refs 与 futures refs 只能用 untyped JSON 区分
//!
//! Review questions:
//! - less-visible wait refs 是否可发现？
//! - optional snapshot 是否比 load panic/错误路径更适合文档示例？
//! - trade command 和 direct query 的边界是否清晰？

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
    let exchange_id = read_optional_env("TQ_TEST_EXCHANGE").unwrap_or_else(|| "SHFE".to_string());
    let symbol = read_optional_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.au2602".to_string());
    let trading_day =
        read_optional_env("TQ_TRADING_DAY").unwrap_or_else(|| "20260101".to_string());
    let notification_id =
        read_optional_env("TQ_NOTIFICATION_ID").unwrap_or_else(|| "latest".to_string());
    let order_id =
        read_optional_env("TQ_SECURITY_ORDER_ID").unwrap_or_else(|| "sample-order".to_string());
    let trade_id =
        read_optional_env("TQ_SECURITY_TRADE_ID").unwrap_or_else(|| "sample-trade".to_string());

    let mut api = TqApiBuilder::new(user, pass)
        .futures_market()
        .trade_target_tqkq()
        .build()
        .await?;

    let notification = api.get_notification(notification_id.as_str());
    let settlement = api.get_settlement_info(account_id.as_str(), trading_day.as_str());
    let risk_rule = api.get_risk_management_rule(account_id.as_str(), exchange_id.as_str());
    let risk_data = api.get_risk_management_data(account_id.as_str(), symbol.as_str());
    let security_account = api.get_security_account(account_id.as_str());
    let security_position = api.get_security_position(account_id.as_str(), symbol.as_str());
    let security_order = api.get_security_order(account_id.as_str(), order_id.as_str());
    let security_trade = api.get_security_trade(account_id.as_str(), trade_id.as_str());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if !api
            .wait_update(Some(tokio::time::Instant::now() + Duration::from_secs(1)))
            .await?
        {
            continue;
        }

        if api.is_changing(&notification)? {
            println!("notification={:?}", notification.snapshot(&api)?);
        }
        if api.is_changing(&settlement)? {
            println!("settlement_ready={}", settlement.is_ready(&api)?);
        }
        if api.is_changing(&risk_rule)? {
            println!("risk_rule_ready={}", risk_rule.is_ready(&api)?);
        }
        if api.is_changing(&risk_data)? {
            println!("risk_data_snapshot={:?}", risk_data.snapshot(&api)?);
        }
        if api.is_changing(&security_account)? {
            println!("security_account_ready={}", security_account.is_ready(&api)?);
        }
        if api.is_changing(&security_position)? {
            println!("security_position_snapshot={:?}", security_position.snapshot(&api)?);
        }
        if api.is_changing(&security_order)? {
            println!("security_order_ready={}", security_order.is_ready(&api)?);
        }
        if api.is_changing(&security_trade)? {
            println!("security_trade_snapshot={:?}", security_trade.snapshot(&api)?);
        }
    }

    if std::env::var_os("TQ_CONFIRM_SETTLEMENT").is_some() {
        api.confirm_settlement(account_id.as_str()).await?;
        println!("confirm_settlement submitted account={}", account_id);
    }

    Ok(())
}
