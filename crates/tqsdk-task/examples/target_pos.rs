use std::error::Error;
use std::time::Duration;

use tqsdk_core::{AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand};
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let auth_user = read_env("TQ_AUTH_USER")?;
    let auth_pass = read_env("TQ_AUTH_PASS")?;
    let account_id = read_env("SIMNOW_USER_0")?;
    let trade_password = read_env("SIMNOW_PASS_0")?;
    let symbol = read_optional_env("TQ_TASK_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());
    let timeout_secs = read_u64_env("TQ_TASK_TIMEOUT_SECS", 30)?;
    let allow_orders = std::env::var_os("TQ_TASK_ALLOW_ORDERS").is_some();

    let api = TqApiBuilder::new(auth_user, auth_pass)
        .futures_market()
        .trade_target("simnow", account_id.clone())
        .build()
        .await?;
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
    let account = host.api().get_account(account_id);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(5))).await?;
        if let Some(snapshot) = account.snapshot(host.api())? {
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
