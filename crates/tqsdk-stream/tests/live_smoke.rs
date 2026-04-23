use std::time::Duration;

use futures::StreamExt;
use tqsdk_core::{MarketCommand, RuntimeCommand, Symbol, TradeCommand};
use tqsdk_stream::{TqStreamBuilder, TradeSessionEvent};

#[path = "../examples/support/live_trade_login.rs"]
mod live_trade_login;

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
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and defaults to the official built-in TqKq account"]
async fn live_trade_session_event_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let trade_login = live_trade_login::resolve_live_trade_login(&auth_user, &auth_pass)
        .await
        .expect("live trade login should resolve");

    let stream = TqStreamBuilder::new(auth_user, auth_pass)
        .trade_target(trade_login.broker_id(), trade_login.account_id())
        .build()
        .await
        .expect("live stream facade should build");
    let mut events = stream
        .trade_session_event_stream(trade_login.account_id())
        .expect("trade_session_event_stream should construct");

    stream
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(
            trade_login.login_command(),
        )))
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
                assert_eq!(notification.user_id, trade_login.account_id());
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
