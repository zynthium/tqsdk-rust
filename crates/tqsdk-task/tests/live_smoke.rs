use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeDirection, TradeLoginCommand,
    TradeOffset,
};
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and SIMNOW_USER_0/SIMNOW_PASS_0"]
async fn live_task_host_trade_account_ready_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let Some(account_id) = read_env("SIMNOW_USER_0") else {
        return;
    };
    let Some(trade_password) = read_env("SIMNOW_PASS_0") else {
        return;
    };

    let api = TqApiBuilder::new(auth_user, auth_pass)
        .trade_target("simnow", account_id.clone())
        .build()
        .await
        .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    host.api()
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(
            TradeLoginCommand {
                account_id: AccountId::new(account_id.clone()),
                broker_id: "simnow".to_string(),
                password: trade_password,
                account_type: TradeAccountType::Future,
                front_broker: None,
                front_url: None,
                client_app_id: None,
                client_system_info: None,
            },
        )))
        .await
        .expect("TradeLoginCommand should submit successfully");

    let account = host.api().get_account(account_id.as_str());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for trade account snapshot"
        );

        let _updated = host
            .wait_update(Some(now + Duration::from_secs(5)))
            .await
            .expect("TaskHost::wait_update should succeed");

        let Some(snapshot) = account
            .snapshot(host.api())
            .expect("account snapshot decode should succeed")
        else {
            continue;
        };

        assert_eq!(snapshot.user_id, account_id);
        assert_eq!(snapshot.currency, "CNY");
        return;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live order smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, SIMNOW_USER_0/SIMNOW_PASS_0, and explicit TQ_SMOKE_ALLOW_ORDER=1 with TQ_SMOKE_ORDER_SYMBOL/TQ_SMOKE_ORDER_LIMIT_PRICE"]
async fn live_insert_cancel_guarded_smoke() {
    if std::env::var_os("TQ_SMOKE_ALLOW_ORDER").is_none() {
        return;
    }

    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let Some(account_id) = read_env("SIMNOW_USER_0") else {
        return;
    };
    let Some(trade_password) = read_env("SIMNOW_PASS_0") else {
        return;
    };
    let symbol = require_env("TQ_SMOKE_ORDER_SYMBOL")
        .expect("TQ_SMOKE_ORDER_SYMBOL is required when TQ_SMOKE_ALLOW_ORDER=1");
    let limit_price = require_f64_env("TQ_SMOKE_ORDER_LIMIT_PRICE")
        .expect("TQ_SMOKE_ORDER_LIMIT_PRICE is required when TQ_SMOKE_ALLOW_ORDER=1");
    let volume = require_i64_env("TQ_SMOKE_ORDER_VOLUME").unwrap_or(1);

    let api = TqApiBuilder::new(auth_user, auth_pass)
        .trade_target("simnow", account_id.clone())
        .build()
        .await
        .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    login_trade_account(&host, account_id.as_str(), trade_password.as_str())
        .await
        .expect("trade login should submit");
    wait_for_trade_account_ready(&mut host, account_id.as_str(), Duration::from_secs(30))
        .await
        .expect("trade account should become ready");

    let order = host
        .insert_order_guarded(
            account_id.as_str(),
            symbol.as_str(),
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            volume,
            Some(json!(limit_price)),
        )
        .await
        .expect("guarded insert_order should succeed");

    let order_status = wait_for_order_snapshot(&mut host, &order, Duration::from_secs(30)).await;
    if order_status != "ALIVE" {
        return;
    }

    host.cancel_order_guarded(account_id.as_str(), order.order_id())
        .await
        .expect("guarded cancel_order should succeed");
    let order_status = wait_for_order_snapshot(&mut host, &order, Duration::from_secs(30)).await;
    assert_ne!(order_status, "ALIVE");
}

async fn login_trade_account(
    host: &TaskHost,
    account_id: &str,
    trade_password: &str,
) -> Result<(), String> {
    host.api()
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(
            TradeLoginCommand {
                account_id: AccountId::new(account_id.to_string()),
                broker_id: "simnow".to_string(),
                password: trade_password.to_string(),
                account_type: TradeAccountType::Future,
                front_broker: None,
                front_url: None,
                client_app_id: None,
                client_system_info: None,
            },
        )))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn wait_for_trade_account_ready(
    host: &mut TaskHost,
    account_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    let account = host.api().get_account(account_id);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".to_string());
        }

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .map_err(|error| error.to_string())?;

        let snapshot = account
            .snapshot(host.api())
            .map_err(|error| error.to_string())?;
        if snapshot.is_some() {
            return Ok(());
        }
    }
}

async fn wait_for_order_snapshot(
    host: &mut TaskHost,
    order: &tqsdk_wait::OrderRef,
    timeout: Duration,
) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let now = tokio::time::Instant::now();
        assert!(now < deadline, "timed out waiting for order snapshot");

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .expect("TaskHost::wait_update should succeed");

        let Some(snapshot) = order
            .snapshot(host.api())
            .expect("order snapshot decode should succeed")
        else {
            continue;
        };

        assert_eq!(snapshot.user_id, order.account_id());
        return snapshot.status;
    }
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn require_env(name: &str) -> Result<String, String> {
    read_env(name).ok_or_else(|| format!("missing environment variable: {name}"))
}

fn require_f64_env(name: &str) -> Result<f64, String> {
    let raw = require_env(name)?;
    raw.parse::<f64>()
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn require_i64_env(name: &str) -> Result<i64, String> {
    let raw = require_env(name)?;
    raw.parse::<i64>()
        .map_err(|error| format!("invalid {name}: {error}"))
}
