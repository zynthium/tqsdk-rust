#![cfg(feature = "live")]

use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{
    AccountId, MarketCommand, RuntimeCommand, Symbol, TradeAccountType, TradeCommand,
    TradeLoginCommand,
};
use tqsdk_session::SessionClientBuilder;
use tqsdk_stream::{TqStreamBuilder, TradeSessionEvent};

type ExplicitTradeOverride = (String, String, String);

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and market access"]
async fn live_quote_stream_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.ao2609".to_string());

    let stream = build_stream_for_symbol(auth_user, auth_pass, symbol.as_str())
        .build()
        .await
        .expect("live stream facade should build");
    stream
        .session()
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await
        .expect("SubscribeQuotes should succeed");

    let mut quotes = stream
        .quote_stream(symbol.as_str())
        .expect("quote_stream should construct");
    let update = tokio::time::timeout(Duration::from_secs(30), quotes.next())
        .await
        .expect("quote stream should produce an item before timeout")
        .expect("quote stream should stay open")
        .expect("quote stream item should decode");

    assert!(!update.value.instrument_id.is_empty());
    assert!(!update.value.datetime.is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS, stock market access, and validates stream.session() direct-query reuse"]
async fn live_quote_stream_with_session_query_smoke() {
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

    let session_builder = SessionClientBuilder::new(auth_user, auth_pass)
        .stock_market()
        .enable_query();
    let stream = TqStreamBuilder::from_session_builder(session_builder)
        .build()
        .await
        .expect("live stream facade should build");

    let metadata = stream
        .session()
        .query_symbol_info(&[symbol.as_str()])
        .await
        .expect("query_symbol_info should succeed over the shared session");
    let instrument = metadata
        .first()
        .expect("query_symbol_info should return at least one row");
    assert!(!instrument.instrument_id.as_str().is_empty());

    stream
        .session()
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new(symbol.clone())],
        }))
        .await
        .expect("SubscribeQuotes should succeed");

    let mut quotes = stream
        .quote_stream(symbol.as_str())
        .expect("quote_stream should construct");
    let update = tokio::time::timeout(Duration::from_secs(30), quotes.next())
        .await
        .expect("quote stream should produce an item before timeout")
        .expect("quote stream should stay open")
        .expect("quote stream item should decode");

    assert!(!update.value.instrument_id.is_empty());
    assert!(!update.value.datetime.is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and defaults to the official built-in TqKq account"]
async fn live_trade_session_event_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let explicit_trade = explicit_trade_override().expect("explicit trade override should parse");
    let account_number = read_u8_env("TQ_TRADE_ACCOUNT_NO").expect("account number should parse");

    let builder = TqStreamBuilder::new(auth_user, auth_pass);
    let stream = if let Some((broker_id, account_id, _password)) = explicit_trade.as_ref() {
        builder.trade_target(broker_id.clone(), account_id.clone())
    } else if let Some(number) = account_number {
        builder.trade_target_tqkq_numbered(number)
    } else {
        builder.trade_target_tqkq()
    }
    .build()
    .await
    .expect("live stream facade should build");

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
        stream
            .session()
            .tqkq_login_command_numbered(number)
            .await
            .expect("numbered tqkq login should resolve")
    } else {
        stream
            .session()
            .tqkq_login_command()
            .await
            .expect("tqkq login should resolve")
    };
    let account_id = trade_login.account_id.as_str().to_string();
    let mut events = stream
        .trade_session_event_stream(account_id.as_str())
        .expect("trade_session_event_stream should construct");

    stream
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(trade_login)))
        .await
        .expect("TradeLoginCommand should submit successfully");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let now = tokio::time::Instant::now();
        assert!(now < deadline, "timed out waiting for trade session event");

        let update = tokio::time::timeout(deadline - now, events.next())
            .await
            .expect("trade session event stream should produce an item before timeout")
            .expect("trade session event stream should stay open")
            .expect("trade session event should decode");

        match update.event {
            TradeSessionEvent::TradeObject(_) => {
                assert!(update.commit.is_some());
                return;
            }
            TradeSessionEvent::Notification(notification) => {
                assert!(update.commit.is_some());
                assert_eq!(notification.user_id, account_id);
                return;
            }
            TradeSessionEvent::Reconnect(_) => continue,
            TradeSessionEvent::SessionError(error) => {
                panic!("unexpected trade session error: {error}");
            }
            _ => continue,
        }
    }
}

fn build_stream_for_symbol(auth_user: String, auth_pass: String, symbol: &str) -> TqStreamBuilder {
    let builder = TqStreamBuilder::new(auth_user, auth_pass);
    if is_stock_symbol(symbol) {
        builder.stock_market()
    } else {
        builder.futures_market()
    }
}

fn is_stock_symbol(symbol: &str) -> bool {
    symbol.starts_with("SSE.") || symbol.starts_with("SZSE.") || symbol.starts_with("BSE.")
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
