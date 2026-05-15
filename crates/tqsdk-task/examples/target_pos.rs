use std::error::Error;
use std::time::Duration;

use tqsdk_core::{AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand};
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

type ExplicitTradeOverride = (String, String, String);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = read_env("TQ_AUTH_USER")?;
    let auth_pass = read_env("TQ_AUTH_PASS")?;
    let explicit_trade = explicit_trade_override()?;
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO")?;
    let symbol = read_optional_env("TQ_TASK_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let timeout_secs = read_u64_env("TQ_TASK_TIMEOUT_SECS", 30)?;
    let allow_orders = std::env::var_os("TQ_TASK_ALLOW_ORDERS").is_some();

    let builder = TqApiBuilder::new(auth_user, auth_pass).futures_market();
    let api = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await?;
    let mut host = TaskHost::new(api);

    let trade_login = if let Some((broker_id, account_id, password)) = explicit_trade {
        TradeLoginCommand {
            account_id: AccountId::new(account_id),
            broker_id,
            password,
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
            .await?
    } else {
        host.api().session().tqkq_login_command().await?
    };
    let account_id = trade_login.account_id.as_str().to_string();

    host.api()
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await?;

    wait_for_trade_account_ready(&mut host, account_id.as_str(), timeout_secs).await?;

    if !allow_orders {
        println!(
            "trade account is ready for {}. dry-run only; set TQ_TASK_ALLOW_ORDERS=1 and TQ_TARGET_VOLUME to start TargetPosTask",
            account_id
        );
        return Ok(());
    }

    let target_volume = read_i64_env("TQ_TARGET_VOLUME")?;
    let task = host
        .target_pos(account_id.as_str(), symbol.as_str())
        .build()?;
    task.set_target_volume(target_volume)?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let mut emitted_events = 0_usize;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for TargetPosTask to finish".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(5))).await?;

        let report = task.execution_report();
        if report.events.len() > emitted_events {
            for event in &report.events[emitted_events..] {
                println!("task_event={event:?}");
            }
            emitted_events = report.events.len();
        }

        if let Some(error) = task.last_error() {
            return Err(format!("TargetPosTask failed: {error}").into());
        }
        if task.is_finished() {
            println!(
                "target task finished symbol={} target_volume={}",
                symbol, target_volume
            );
            break;
        }
    }

    Ok(())
}

async fn wait_for_trade_account_ready(
    host: &mut TaskHost,
    account_id: &str,
    timeout_secs: u64,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let account = host.api().account(account_id);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(5))).await?;
        if let Some(snapshot) = account.snapshot()? {
            println!(
                "trade account ready user_id={} currency={} available={}",
                snapshot.user_id, snapshot.currency, snapshot.available
            );
            return Ok(());
        }
    }
}

fn read_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("missing environment variable: {name}").into())
}

fn read_optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_u8_env(name: &str) -> Result<Option<u8>, Box<dyn Error>> {
    let Some(raw) = read_optional_env(name) else {
        return Ok(None);
    };
    Ok(Some(raw.parse()?))
}

fn explicit_trade_override() -> Result<Option<ExplicitTradeOverride>, Box<dyn Error>> {
    match (
        read_optional_env("TQ_TRADE_BROKER_ID"),
        read_optional_env("TQ_TRADE_ACCOUNT_ID"),
        read_optional_env("TQ_TRADE_PASSWORD"),
    ) {
        (Some(broker_id), Some(account_id), Some(password)) => {
            Ok(Some((broker_id, account_id, password)))
        }
        (None, None, None) => Ok(None),
        _ => Err(
            "TQ_TRADE_BROKER_ID/TQ_TRADE_ACCOUNT_ID/TQ_TRADE_PASSWORD must be set together".into(),
        ),
    }
}

fn read_i64_env(name: &str) -> Result<i64, Box<dyn Error>> {
    let raw = read_env(name)?;
    Ok(raw.parse()?)
}

fn read_u64_env(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}
