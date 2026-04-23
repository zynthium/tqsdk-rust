use std::error::Error;
use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::{Account, RuntimeCommand, TradeCommand};
use tqsdk_session::SessionClientBuilder;

fn read_env(key: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(key).map_err(|_| format!("missing environment variable: {key}").into())
}

fn read_optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_u8_env(key: &str) -> Result<Option<u8>, Box<dyn Error>> {
    let Some(raw) = read_optional_env(key) else {
        return Ok(None);
    };
    Ok(Some(raw.parse()?))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let user = read_env("TQ_AUTH_USER")?;
    let pass = read_env("TQ_AUTH_PASS")?;
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO")?;

    let builder = SessionClientBuilder::new(user, pass);
    let session = if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()?;

    let trade_login = if let Some(number) = account_number {
        session.tqkq_login_command_numbered(number).await?
    } else {
        session.tqkq_login_command().await?
    };
    let account_id = trade_login.account_id.as_str().to_string();

    session
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await?;

    let account =
        wait_for_trade_account_ready(&session, account_id.as_str(), Duration::from_secs(30))
            .await?;

    println!(
        "trade account ready user_id={} currency={} available={}",
        account.user_id, account.currency, account.available
    );
    Ok(())
}

async fn wait_for_trade_account_ready(
    session: &tqsdk_session::SessionClient,
    account_id: &str,
    timeout: Duration,
) -> Result<Account, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(account) = session
            .reader()
            .read()
            .decode_path::<Account>(&["trade", account_id, "accounts", "CNY"])?
        {
            return Ok(account);
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".into());
        }

        let mut progress = session.flush_outbound().await?;
        progress |= session.drive_pending_once().await?;
        progress |= session
            .drive_route_once(Some(now + Duration::from_millis(250)))
            .await?;

        if !progress {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
