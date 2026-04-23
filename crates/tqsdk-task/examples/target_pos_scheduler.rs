use std::error::Error;
use std::time::Duration;

use tqsdk_core::{AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand};
use tqsdk_task::{TargetPosScheduleStep, TaskHost};
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
    let pause_millis = read_u64_env("TQ_TASK_SCHEDULER_PAUSE_MS", 1000)?;
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

    let account_id = if let Some((_, account_id, password)) = explicit_trade {
        let trade_login = TradeLoginCommand {
            account_id: AccountId::new(account_id.clone()),
            broker_id: read_env("TQ_TRADE_BROKER_ID")?,
            password,
            account_type: TradeAccountType::Future,
            front_broker: None,
            front_url: None,
            client_app_id: None,
            client_system_info: None,
        };
        host.api()
            .session()
            .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
            .await?;
        wait_for_trade_account_ready(&mut host, account_id.as_str(), timeout_secs).await?;
        account_id
    } else {
        let trade_login = if let Some(number) = account_number {
            host.api()
                .session()
                .tqkq_login_command_numbered(number)
                .await?
        } else {
            host.api().session().tqkq_login_command().await?
        };
        let account_id = trade_login.account_id.as_str().to_string();
        if allow_orders {
            host.api()
                .session()
                .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
                .await?;
            wait_for_trade_account_ready(&mut host, account_id.as_str(), timeout_secs).await?;
        }
        account_id
    };

    let steps = if allow_orders {
        let target_volume = read_i64_env("TQ_TARGET_VOLUME")?;
        vec![
            TargetPosScheduleStep::pause(Duration::from_millis(pause_millis)),
            TargetPosScheduleStep::target(
                Duration::from_secs(timeout_secs.max(1)),
                target_volume,
                tqsdk_task::PriceMode::Active,
            ),
        ]
    } else {
        vec![TargetPosScheduleStep::pause(Duration::from_millis(
            pause_millis.max(1),
        ))]
    };

    let scheduler = host
        .target_pos_scheduler(account_id.as_str(), symbol.as_str())
        .steps(steps)
        .build()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let mut emitted_events = 0_usize;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for TargetPosScheduler to finish".into());
        }

        let _updated = host.wait_update(Some(now + Duration::from_secs(1))).await?;

        let events = scheduler.execution_events();
        if events.len() > emitted_events {
            for event in &events[emitted_events..] {
                println!("scheduler_event={event:?}");
            }
            emitted_events = events.len();
        }

        if let Some(error) = scheduler.last_error() {
            return Err(format!("TargetPosScheduler failed: {error}").into());
        }
        if scheduler.is_finished() {
            println!(
                "scheduler finished symbol={} report={:?}",
                symbol,
                scheduler.execution_report()
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

        let _updated = host.wait_update(Some(now + Duration::from_secs(1))).await?;
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

fn read_u8_env(name: &str) -> Result<Option<u8>, Box<dyn Error>> {
    let Some(raw) = read_optional_env(name) else {
        return Ok(None);
    };
    Ok(Some(raw.parse()?))
}

fn read_u64_env(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}

fn read_i64_env(name: &str) -> Result<i64, Box<dyn Error>> {
    let raw = read_env(name)?;
    Ok(raw.parse()?)
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
