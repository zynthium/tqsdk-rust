use std::time::Duration;

use tokio::time::Instant;
use tqsdk_core::{Account, RuntimeCommand, TradeCommand};
use tqsdk_session::SessionClientBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and query access"]
async fn live_query_symbol_info_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .enable_query()
        .build()
        .expect("live session should build");
    let quotes = session
        .query_symbol_info(&[symbol.as_str()])
        .await
        .expect("query_symbol_info should succeed");
    let quote = quotes
        .into_iter()
        .next()
        .expect("query_symbol_info should return at least one row");

    assert!(!quote.instrument_id.is_empty());
    assert!(!quote.ins_class.is_empty());
    assert!(quote.price_tick.is_finite());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and uses the official built-in TqKq account"]
async fn live_tqkq_trade_login_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");

    let builder = SessionClientBuilder::new(auth_user, auth_pass);
    let session = if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .expect("live session should build");

    let trade_login = if let Some(number) = account_number {
        session
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        session
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();

    session
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await
        .expect("TradeLoginCommand should submit successfully");

    let account =
        wait_for_trade_account_ready(&session, account_id.as_str(), Duration::from_secs(30))
            .await
            .expect("trade account should become ready");

    assert_eq!(account.user_id, account_id);
    assert_eq!(account.currency, "CNY");
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

async fn wait_for_trade_account_ready(
    session: &tqsdk_session::SessionClient,
    account_id: &str,
    timeout: Duration,
) -> Result<Account, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(account) = session
            .reader()
            .read()
            .decode_path::<Account>(&["trade", account_id, "accounts", "CNY"])
            .map_err(|error| error.to_string())?
        {
            return Ok(account);
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for trade account snapshot".to_string());
        }

        let mut progress = session
            .flush_outbound()
            .await
            .map_err(|error| error.to_string())?;
        progress |= session
            .drive_pending_once()
            .await
            .map_err(|error| error.to_string())?;
        progress |= session
            .drive_route_once(Some(now + Duration::from_millis(250)))
            .await
            .map_err(|error| error.to_string())?;

        if !progress {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
