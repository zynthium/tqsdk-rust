use tqsdk_core::Quote;
use tqsdk_session::{InstrumentClass, InstrumentSpec};

fn quote() -> Quote {
    Quote {
        instrument_id: "SHFE.au2602".to_string(),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        ins_class: "FUTURE".to_string(),
        price_tick: 0.02,
        volume_multiple: 1000,
        expire_datetime: Some(1_770_000_000_000_000_000),
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
    assert_eq!(
        spec.expire_datetime_ns,
        Some(1_770_000_000_000_000_000)
    );
    assert!(spec.is_derivative());
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
