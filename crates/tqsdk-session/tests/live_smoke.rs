use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::Instant;
#[cfg(feature = "tq-auth")]
use tqsdk_core::{Account, TradeCommand};
use tqsdk_core::{MarketCommand, QueryCommand, QueryId, Quote, RuntimeCommand, Symbol};
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
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, stock market access, and validates raw query command waiting"]
async fn live_query_command_wait_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_QUERY_SYMBOL").unwrap_or_else(|| "SSE.000300".to_string());
    assert!(
        is_stock_symbol(symbol.as_str()),
        "TQ_QUERY_SYMBOL must be a stock symbol when query rides the official stock websocket"
    );

    let session = SessionClientBuilder::new(auth_user, auth_pass)
        .stock_market()
        .enable_query()
        .build()
        .expect("live session should build");
    let query_id = QueryId::new("live-symbol-info");
    let command_id = session
        .submit(RuntimeCommand::Query(QueryCommand::Fetch {
            query_id: query_id.clone(),
            query: r#"query($instrument_id:[String]){
  multi_symbol_info(instrument_id: $instrument_id) {
    ... on basic {
      instrument_id
      class
      price_tick
    }
  }
}"#
            .to_string(),
            variables: Some(json!({ "instrument_id": [symbol] })),
        }))
        .await
        .expect("raw query command should submit");

    session
        .wait_command_completed(command_id)
        .await
        .expect("raw query command should complete");

    let payload = session
        .query_result(query_id.as_str())
        .expect("query_result should decode")
        .expect("query command should produce a result payload");
    let instrument = first_symbol_info(payload).expect("query payload should contain one symbol");

    assert!(
        !instrument
            .get("instrument_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        !instrument
            .get("class")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        instrument
            .get("price_tick")
            .and_then(Value::as_f64)
            .is_some_and(f64::is_finite)
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market access"]
async fn live_quote_progress_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let session = build_market_session(auth_user, auth_pass, symbol.as_str());
    session
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await
        .expect("SubscribeQuotes should submit successfully");

    let quote = wait_for_quote_update(&session, symbol.as_str(), Duration::from_secs(30))
        .await
        .expect("quote should become ready");

    assert!(!quote.instrument_id.is_empty());
    assert!(!quote.datetime.is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "tq-auth")]
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

#[cfg(feature = "tq-auth")]
fn read_u8_env(name: &str) -> Result<Option<u8>, String> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    raw.parse::<u8>()
        .map(Some)
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn build_market_session(
    auth_user: String,
    auth_pass: String,
    symbol: &str,
) -> tqsdk_session::SessionClient {
    let builder = SessionClientBuilder::new(auth_user, auth_pass);
    if is_stock_symbol(symbol) {
        builder.stock_market()
    } else {
        builder.futures_market()
    }
    .build()
    .expect("live session should build")
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
}

fn first_symbol_info(payload: Value) -> Option<Value> {
    let payload = payload.get("result").unwrap_or(&payload);
    payload
        .get("multi_symbol_info")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .cloned()
}

async fn wait_for_quote_update(
    session: &tqsdk_session::SessionClient,
    symbol: &str,
    timeout: Duration,
) -> Result<Quote, String> {
    let reader = session.reader().clone();
    let mut cursor = reader.cursor();
    let deadline = Instant::now() + timeout;

    loop {
        while reader.next(&mut cursor).is_some() {
            if let Some(quote) = reader
                .read()
                .decode_path::<Quote>(&["quotes", symbol])
                .map_err(|error| error.to_string())?
                && !quote.datetime.is_empty()
            {
                return Ok(quote);
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err("timed out waiting for quote snapshot".to_string());
        }

        let progress = session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await
            .map_err(|error| error.to_string())?;

        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[cfg(feature = "tq-auth")]
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

        let progress = session
            .progress_once(Some(now + Duration::from_millis(250)))
            .await
            .map_err(|error| error.to_string())?;

        if !progress.is_progress() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
