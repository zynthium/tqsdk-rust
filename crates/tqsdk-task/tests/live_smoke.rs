#![cfg(feature = "live")]

use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeDirection, TradeLoginCommand,
    TradeOffset,
};
use tqsdk_task::{
    TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep, TargetPosScheduler,
    TargetPosStepOutcomeReport, TargetPosTask, TaskHost,
};
use tqsdk_wait::TqApiBuilder;

type ExplicitTradeOverride = (String, String, String);

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and defaults to the official built-in TqKq account"]
async fn live_task_host_trade_account_ready_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let explicit_trade = explicit_trade_override().expect("explicit trade override should parse");
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");

    let builder = TqApiBuilder::new(auth_user, auth_pass);
    let api = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await
    .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    let trade_login = if let Some((broker_id, account_id, password)) = explicit_trade {
        TradeLoginCommand {
            account_id: AccountId::new(account_id),
            broker_id,
            password,
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    } else if let Some(number) = account_number {
        host.api()
            .session()
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        host.api()
            .session()
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();
    let account = host
        .api_mut()
        .login_trade_account(
            &trade_login.broker_id,
            trade_login.account_id.as_str(),
            &trade_login.password,
            trade_login.account_type,
            Some(tokio::time::Instant::now() + Duration::from_secs(30)),
        )
        .await
        .expect("trade login and settlement confirmation should succeed");
    let snapshot = account
        .snapshot()
        .expect("account snapshot decode should succeed")
        .expect("account should be ready after login helper returns");

    assert_eq!(snapshot.user_id, account_id);
    assert_eq!(snapshot.currency, "CNY");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live order smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, defaults to the official built-in TqKq account, and needs explicit TQ_SMOKE_ALLOW_ORDER=1 with TQ_SMOKE_ORDER_SYMBOL/TQ_SMOKE_ORDER_LIMIT_PRICE"]
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
    let explicit_trade = explicit_trade_override().expect("explicit trade override should parse");
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");
    let symbol = require_env("TQ_SMOKE_ORDER_SYMBOL")
        .expect("TQ_SMOKE_ORDER_SYMBOL is required when TQ_SMOKE_ALLOW_ORDER=1");
    let limit_price = require_f64_env("TQ_SMOKE_ORDER_LIMIT_PRICE")
        .expect("TQ_SMOKE_ORDER_LIMIT_PRICE is required when TQ_SMOKE_ALLOW_ORDER=1");
    let volume = require_i64_env("TQ_SMOKE_ORDER_VOLUME").unwrap_or(1);

    let builder = TqApiBuilder::new(auth_user, auth_pass);
    let api = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await
    .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    let trade_login = if let Some((broker_id, account_id, password)) = explicit_trade {
        TradeLoginCommand {
            account_id: AccountId::new(account_id),
            broker_id,
            password,
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    } else if let Some(number) = account_number {
        host.api()
            .session()
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        host.api()
            .session()
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();

    login_trade_account(&host, trade_login)
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and validates that TaskHost::wait_update advances scheduler pause steps without fresh diffs"]
async fn live_scheduler_pause_step_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };

    let api = TqApiBuilder::new(auth_user, auth_pass)
        .futures_market()
        .build()
        .await
        .expect("live wait api should build");
    let mut host = TaskHost::new(api);
    let scheduler = host
        .target_pos_scheduler("dry-run", "SHFE.ao2609")
        .steps(vec![TargetPosScheduleStep::pause(Duration::from_millis(
            50,
        ))])
        .build()
        .expect("pause-only scheduler should build");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !scheduler.is_finished() {
        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for pause-only scheduler to finish"
        );
        host.wait_update(Some(now + Duration::from_millis(50)))
            .await
            .expect("TaskHost::wait_update should succeed");
    }

    scheduler
        .wait_finished()
        .await
        .expect("pause-only scheduler should finish cleanly");
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 0,
            }],
            step_outcomes: vec![TargetPosStepOutcomeReport {
                step_index: 0,
                target_volume: 0,
                ..TargetPosStepOutcomeReport::default()
            }],
            ..TargetPosExecutionReport::default()
        }
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live target-pos smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, defaults to the official built-in TqKq account, and needs explicit TQ_SMOKE_ALLOW_ORDER=1"]
async fn live_target_pos_roundtrip_smoke() {
    if std::env::var_os("TQ_SMOKE_ALLOW_ORDER").is_none() {
        return;
    }

    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let explicit_trade = explicit_trade_override().expect("explicit trade override should parse");
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");
    let symbol = read_env("TQ_SMOKE_TASK_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let target_delta = require_i64_env("TQ_SMOKE_TARGET_DELTA").unwrap_or(1);
    let timeout_secs = require_u64_env("TQ_SMOKE_TIMEOUT_SECS").unwrap_or(60);

    let builder = TqApiBuilder::new(auth_user, auth_pass).futures_market();
    let api = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await
    .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    let trade_login = if let Some((broker_id, account_id, password)) = explicit_trade {
        TradeLoginCommand {
            account_id: AccountId::new(account_id),
            broker_id,
            password,
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    } else if let Some(number) = account_number {
        host.api()
            .session()
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        host.api()
            .session()
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();

    login_trade_account(&host, trade_login)
        .await
        .expect("trade login should submit");
    wait_for_trade_account_ready(&mut host, account_id.as_str(), Duration::from_secs(30))
        .await
        .expect("trade account should become ready");

    let baseline = current_net_position(&host, account_id.as_str(), symbol.as_str())
        .expect("position snapshot should decode");
    let first_target = baseline + target_delta;
    let task = host
        .target_pos(account_id.as_str(), symbol.as_str())
        .build()
        .expect("TargetPosTask should build");

    task.set_target_volume(first_target)
        .expect("first target volume should set");
    wait_for_task_target_reached(
        &mut host,
        &task,
        first_target,
        Duration::from_secs(timeout_secs),
    )
    .await
    .expect("target task should reach the first target");

    task.set_target_volume(baseline)
        .expect("baseline target volume should set");
    wait_for_task_target_reached(
        &mut host,
        &task,
        baseline,
        Duration::from_secs(timeout_secs),
    )
    .await
    .expect("target task should revert to the baseline target");

    task.cancel().await.expect("task cancel should succeed");
    wait_for_task_finished(&mut host, &task, Duration::from_secs(10))
        .await
        .expect("task should finish cleanly after cancel");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live scheduler smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, defaults to the official built-in TqKq account, and needs explicit TQ_SMOKE_ALLOW_ORDER=1"]
async fn live_target_pos_scheduler_roundtrip_smoke() {
    if std::env::var_os("TQ_SMOKE_ALLOW_ORDER").is_none() {
        return;
    }

    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let explicit_trade = explicit_trade_override().expect("explicit trade override should parse");
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");
    let symbol = read_env("TQ_SMOKE_TASK_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let target_delta = require_i64_env("TQ_SMOKE_TARGET_DELTA").unwrap_or(1);
    let timeout_secs = require_u64_env("TQ_SMOKE_TIMEOUT_SECS").unwrap_or(90);
    let step_hold_secs = require_u64_env("TQ_SMOKE_SCHEDULER_STEP_SECS").unwrap_or(15);

    let builder = TqApiBuilder::new(auth_user, auth_pass).futures_market();
    let api = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await
    .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    let trade_login = if let Some((broker_id, account_id, password)) = explicit_trade {
        TradeLoginCommand {
            account_id: AccountId::new(account_id),
            broker_id,
            password,
            client_mac_address: None,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        }
    } else if let Some(number) = account_number {
        host.api()
            .session()
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        host.api()
            .session()
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();

    login_trade_account(&host, trade_login)
        .await
        .expect("trade login should submit");
    wait_for_trade_account_ready(&mut host, account_id.as_str(), Duration::from_secs(30))
        .await
        .expect("trade account should become ready");

    let baseline = current_net_position(&host, account_id.as_str(), symbol.as_str())
        .expect("position snapshot should decode");
    let first_target = baseline + target_delta;
    let scheduler = host
        .target_pos_scheduler(account_id.as_str(), symbol.as_str())
        .steps(vec![
            TargetPosScheduleStep::target(
                Duration::from_secs(step_hold_secs.max(1)),
                first_target,
                tqsdk_task::PriceMode::Active,
            ),
            TargetPosScheduleStep::target(
                Duration::from_secs(timeout_secs.max(1)),
                baseline,
                tqsdk_task::PriceMode::Active,
            ),
        ])
        .build()
        .expect("TargetPosScheduler should build");

    wait_for_scheduler_finished(
        &mut host,
        &scheduler,
        Duration::from_secs(timeout_secs.max(step_hold_secs) + step_hold_secs + 30),
    )
    .await
    .expect("scheduler should finish cleanly");

    let report = scheduler.execution_report();
    assert_eq!(report.applied_steps.len(), 2);
    assert_eq!(report.step_outcomes.len(), 2);
    assert!(report.step_outcomes[0].target_reached);
    assert!(report.step_outcomes[1].target_reached);
    assert_eq!(
        current_net_position(&host, account_id.as_str(), symbol.as_str()),
        Ok(baseline)
    );
}

async fn login_trade_account(
    host: &TaskHost,
    trade_login: TradeLoginCommand,
) -> Result<(), String> {
    host.api()
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn wait_for_trade_account_ready(
    host: &mut TaskHost,
    account_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    let account = host.api().account(account_id);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".to_string());
        }

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .map_err(|error| error.to_string())?;

        let snapshot = account.snapshot().map_err(|error| error.to_string())?;
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
            .snapshot()
            .expect("order snapshot decode should succeed")
        else {
            continue;
        };

        assert_eq!(snapshot.user_id, order.account_id());
        return snapshot.status;
    }
}

async fn wait_for_task_target_reached(
    host: &mut TaskHost,
    task: &TargetPosTask,
    target_volume: i64,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(error) = task.last_error() {
            return Err(error.to_string());
        }
        if task.applied_target_volume() == Some(target_volume) {
            task.wait_target_reached()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(());
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(format!(
                "timed out waiting for target volume {target_volume}"
            ));
        }

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .map_err(|error| error.to_string())?;
    }
}

async fn wait_for_task_finished(
    host: &mut TaskHost,
    task: &TargetPosTask,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if task.is_finished() {
            task.wait_finished()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(());
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for task finish".to_string());
        }

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .map_err(|error| error.to_string())?;
    }
}

async fn wait_for_scheduler_finished(
    host: &mut TaskHost,
    scheduler: &TargetPosScheduler,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(error) = scheduler.last_error() {
            return Err(error.to_string());
        }
        if scheduler.is_finished() {
            scheduler
                .wait_finished()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(());
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for scheduler finish".to_string());
        }

        host.wait_update(Some(now + Duration::from_secs(5)))
            .await
            .map_err(|error| error.to_string())?;
    }
}

fn current_net_position(host: &TaskHost, account_id: &str, symbol: &str) -> Result<i64, String> {
    let snapshot = host
        .api()
        .position(account_id, symbol)
        .snapshot()
        .map_err(|error| error.to_string())?;
    Ok(snapshot.map_or(0, |position| position.pos))
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_u8_env(name: &str) -> Result<Option<u8>, String> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    raw.parse::<u8>()
        .map(Some)
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn explicit_trade_override() -> Result<Option<ExplicitTradeOverride>, String> {
    match (
        read_env("TQ_TRADE_BROKER_ID"),
        read_env("TQ_TRADE_ACCOUNT_ID"),
        read_env("TQ_TRADE_PASSWORD"),
    ) {
        (Some(broker_id), Some(account_id), Some(password)) => {
            Ok(Some((broker_id, account_id, password)))
        }
        (None, None, None) => Ok(None),
        _ => Err(
            "TQ_TRADE_BROKER_ID/TQ_TRADE_ACCOUNT_ID/TQ_TRADE_PASSWORD must be set together"
                .to_string(),
        ),
    }
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

fn require_u64_env(name: &str) -> Result<u64, String> {
    let raw = require_env(name)?;
    raw.parse::<u64>()
        .map_err(|error| format!("invalid {name}: {error}"))
}
