use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{RuntimeCommand, TradeCommand};
use tqsdk_stream::{TqStreamBuilder, TradeSessionEvent};

#[path = "support/live_trade_login.rs"]
mod live_trade_login;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_u64_env(key: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    match std::env::var(key) {
        Ok(raw) => Ok(raw.parse()?),
        Err(_) => Ok(default),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let trade_login = live_trade_login::resolve_live_trade_login(&user, &pass).await?;
    let timeout_secs = read_u64_env("TQ_STREAM_TIMEOUT_SECS", 30)?;
    let stream_once = std::env::var_os("TQ_STREAM_ONCE").is_some();

    let stream = TqStreamBuilder::new(user, pass)
        .trade_target(trade_login.broker_id(), trade_login.account_id())
        .build()
        .await?;

    stream
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(
            trade_login.login_command(),
        )))
        .await?;

    let mut events = stream.trade_session_event_stream(trade_login.account_id())?;
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
