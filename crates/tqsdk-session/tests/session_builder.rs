use tqsdk_core::MarketSessionTarget;
use tqsdk_session::{SessionClientBuilder, SessionFacadeError};

#[test]
fn builder_uses_official_schema_base_by_default() {
    let builder = SessionClientBuilder::new("user", "pass");

    assert_eq!(
        builder.endpoints().schema_url.as_deref(),
        Some("https://files.shinnytech.com")
    );
}

#[test]
fn builder_defaults_to_official_stock_live_market_target() {
    let builder = SessionClientBuilder::new("user", "pass");

    assert_eq!(
        builder.market_target_ref(),
        &MarketSessionTarget::stock_live()
    );
}

#[test]
fn builder_named_market_target_shortcuts_are_explicit() {
    let futures_live = SessionClientBuilder::new("user", "pass").futures_market();
    assert_eq!(
        futures_live.market_target_ref(),
        &MarketSessionTarget::futures_live()
    );

    let stock_backtest = SessionClientBuilder::new("user", "pass").stock_backtest_market();
    assert_eq!(
        stock_backtest.market_target_ref(),
        &MarketSessionTarget::stock_backtest()
    );

    let futures_backtest = SessionClientBuilder::new("user", "pass").futures_backtest_market();
    assert_eq!(
        futures_backtest.market_target_ref(),
        &MarketSessionTarget::futures_backtest()
    );
}

#[test]
fn builder_accepts_explicit_query_schema_and_replay_urls() {
    let builder = SessionClientBuilder::new("user", "pass")
        .query_url("https://query.example.com/graphql")
        .schema_url("https://schema.example.com/latest.json")
        .replay_url("wss://replay.example.com/feed");

    let endpoints = builder.endpoints();

    assert_eq!(
        endpoints.query_url.as_deref(),
        Some("https://query.example.com/graphql")
    );
    assert_eq!(
        endpoints.schema_url.as_deref(),
        Some("https://schema.example.com/latest.json")
    );
    assert_eq!(
        endpoints.replay_url.as_deref(),
        Some("wss://replay.example.com/feed")
    );
}

#[test]
fn builder_can_enable_query_without_explicit_query_url() {
    let builder = SessionClientBuilder::new("user", "pass").enable_query();

    assert_eq!(builder.endpoints().query_url, None);
}

#[test]
fn builder_accepts_auth_derived_tqkq_trade_targets() {
    let builder = SessionClientBuilder::new("user", "pass")
        .trade_target_tqkq()
        .trade_target_tqkq_numbered(7)
        .trade_target_tqkq_stock()
        .trade_target_tqkq_stock_numbered(8);

    assert_eq!(builder.trade_targets_ref().len(), 4);
    assert_eq!(builder.trade_targets_ref()[0].broker_id, "快期模拟");
    assert_eq!(
        builder.trade_targets_ref()[0].auth_derived,
        Some(tqsdk_core::AuthDerivedTradeTarget::TqKqFuture { number: None })
    );
    assert_eq!(
        builder.trade_targets_ref()[1].auth_derived,
        Some(tqsdk_core::AuthDerivedTradeTarget::TqKqFuture { number: Some(7) })
    );
    assert_eq!(builder.trade_targets_ref()[2].broker_id, "快期股票模拟");
    assert_eq!(
        builder.trade_targets_ref()[2].auth_derived,
        Some(tqsdk_core::AuthDerivedTradeTarget::TqKqStock { number: None })
    );
    assert_eq!(
        builder.trade_targets_ref()[3].auth_derived,
        Some(tqsdk_core::AuthDerivedTradeTarget::TqKqStock { number: Some(8) })
    );
}

#[test]
fn facade_error_converts_core_errors_and_formats_messages() {
    let error =
        SessionFacadeError::from(tqsdk_core::ContractError::validation("bad session state"));

    assert_eq!(error.to_string(), "validation error: bad session state");
    assert!(std::error::Error::source(&error).is_some());

    let invalid_state = SessionFacadeError::InvalidState("missing session config");
    assert_eq!(
        invalid_state.to_string(),
        "invalid session facade state: missing session config"
    );
    assert!(std::error::Error::source(&invalid_state).is_none());
}

#[cfg(feature = "live")]
#[test]
fn builder_rejects_invalid_tqkq_numbered_targets_before_live_session_build() {
    let zero = match SessionClientBuilder::new("user", "pass")
        .trade_target_tqkq_numbered(0)
        .build()
    {
        Ok(_) => panic!("number 0 should be rejected before live session construction"),
        Err(err) => err,
    };
    assert_eq!(
        zero.to_string(),
        "validation error: TqKq assistant account number must be within 1..=99, got 0"
    );

    let too_large = match SessionClientBuilder::new("user", "pass")
        .trade_target_tqkq_stock_numbered(100)
        .build()
    {
        Ok(_) => panic!("number 100 should be rejected before live session construction"),
        Err(err) => err,
    };
    assert_eq!(
        too_large.to_string(),
        "validation error: TqKq assistant account number must be within 1..=99, got 100"
    );
}
