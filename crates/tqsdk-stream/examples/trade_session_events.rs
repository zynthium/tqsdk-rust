use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand};
use tqsdk_stream::{TqStreamBuilder, TradeSessionEvent};

type ExplicitTradeOverride = (String, String, String);

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_u64_env(key: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match std::env::var(key) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}

fn read_u8_env(key: &str) -> Result<Option<u8>, Box<dyn Error>> {
    let Some(raw) = read_optional_env(key) else {
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let explicit_trade = explicit_trade_override()?;
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO")?;
    let timeout_secs = read_u64_env("TQ_STREAM_TIMEOUT_SECS", 30)?;
    let stream_once = std::env::var_os("TQ_STREAM_ONCE").is_some();

    let builder = TqStreamBuilder::new(user, pass);
    let stream = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await?;

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
        stream.session().tqkq_login_command_numbered(number).await?
    } else {
        stream.session().tqkq_login_command().await?
    };
    let account_id = trade_login.account_id.as_str().to_string();

    stream
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await?;

    let mut events = stream.trade_session_event_stream(account_id.as_str())?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade session event".into());
        }

        let update = match tokio::time::timeout(deadline - now, events.next()).await {
            Ok(Some(update)) => update?,
            Ok(None) => return Err("trade session event stream closed".into()),
            Err(_) => return Err("timed out waiting for trade session event".into()),
        };

        let revision = update
            .commit
            .as_ref()
            .map(|commit| commit.revision.get().to_string())
            .unwrap_or_else(|| "none".to_string());

        match update.event {
            TradeSessionEvent::TradeObject(event) => {
                println!("revision={revision} trade_object={event:?}");
            }
            TradeSessionEvent::Notification(notification) => {
                println!(
                    "revision={revision} notification level={} content={}",
                    notification.level, notification.content
                );
            }
            TradeSessionEvent::Reconnect(reconnect) => {
                println!(
                    "revision={revision} reconnect attempt={} exhausted={} detail={}",
                    reconnect.attempt, reconnect.exhausted, reconnect.detail
                );
            }
            TradeSessionEvent::SessionError(error) => {
                println!("revision={revision} session_error={error}");
            }
            _ => {}
        }

        if stream_once {
            break;
        }
    }

    Ok(())
}
