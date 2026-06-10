use tqsdk_core::Quote;
use tqsdk_session::{InstrumentClass, InstrumentSpec, SymbolInfo};

fn quote() -> Quote {
    Quote {
        instrument_id: "SHFE.au2602".to_string(),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        ins_class: "FUTURE".to_string(),
        price_tick: 0.02,
        volume_multiple: 1000,
        expire_datetime: Some(1_770_000_000),
        ..Quote::default()
    }
}

#[test]
fn instrument_spec_normalizes_contract_metadata_from_quote() {
    let spec = InstrumentSpec::try_from(quote()).unwrap();

    assert_eq!(spec.symbol.as_str(), "SHFE.au2602");
    assert_eq!(spec.exchange_id, "SHFE");
    assert_eq!(spec.product_id, "au");
    assert_eq!(spec.class, InstrumentClass::Future);
    assert_eq!(spec.price_tick, 0.02);
    assert_eq!(spec.volume_multiple, 1000);
    assert_eq!(spec.expire_datetime_secs, Some(1_770_000_000));
    assert!(spec.is_derivative());
}

#[test]
fn instrument_spec_normalizes_contract_metadata_from_symbol_info() {
    let spec = InstrumentSpec::try_from(SymbolInfo {
        instrument_id: tqsdk_core::Symbol::new("SHFE.au2602"),
        instrument_name: "沪金2602".to_string(),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        ins_class: "FUTURE".to_string(),
        class: InstrumentClass::Future,
        price_tick: Some(0.02),
        volume_multiple: Some(1000),
        open_limit: Some(500),
        max_limit_order_volume: Some(100),
        max_market_order_volume: Some(50),
        min_limit_order_volume: Some(1),
        min_market_order_volume: Some(1),
        open_max_market_order_volume: Some(50),
        open_max_limit_order_volume: Some(100),
        open_min_market_order_volume: Some(1),
        open_min_limit_order_volume: Some(1),
        underlying_symbol: None,
        strike_price: None,
        expired: false,
        expire_datetime_secs: Some(1_770_000_000),
        expire_rest_days: Some(120),
        delivery_year: Some(2026),
        delivery_month: Some(2),
        last_exercise_datetime_secs: None,
        exercise_year: None,
        exercise_month: None,
        option_class: None,
        upper_limit: Some(800.0),
        lower_limit: Some(600.0),
        pre_settlement: Some(700.0),
        pre_open_interest: Some(10_000),
        pre_close: Some(699.0),
        trading_time: tqsdk_core::TradingTime::default(),
    })
    .unwrap();

    assert_eq!(spec.symbol.as_str(), "SHFE.au2602");
    assert_eq!(spec.expire_datetime_secs, Some(1_770_000_000));
}

#[test]
fn instrument_spec_rejects_missing_symbol() {
    let mut quote = quote();
    quote.instrument_id.clear();

    let err = InstrumentSpec::try_from(quote).unwrap_err();

    assert_eq!(
        err.diagnostic().retry_hint,
        tqsdk_core::RetryHint::DoNotRetry
    );
}
