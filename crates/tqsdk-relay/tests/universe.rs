use tqsdk_core::Quote;
use tqsdk_relay::{
    FuturesContract, FuturesProductCode, FuturesProductFilter, FuturesUniverseSelection,
    StaticFuturesUniverseResolver, futures_metadata_symbol_batches, resolve_futures_symbols,
    resolve_futures_symbols_with_selection,
};

#[tokio::test]
async fn resolver_selects_all_non_expired_futures_symbols() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        FuturesContract::new("SHFE.au2512", "SHFE", "au", true).unwrap(),
    ]);

    let symbols = resolve_futures_symbols(&FuturesProductFilter::All, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "SHFE.au2602"]);
}

#[tokio::test]
async fn resolver_filters_by_exchange_scoped_product_codes() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("INE.au2602", "INE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
    ]);
    let filter = FuturesProductFilter::Products(vec![
        FuturesProductCode::new(Some("SHFE"), "au").unwrap(),
        FuturesProductCode::new(None, "m").unwrap(),
    ]);

    let symbols = resolve_futures_symbols(&filter, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "SHFE.au2602"]);
}

#[tokio::test]
async fn resolver_selects_main_and_top_activity_contracts_per_product() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2608", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2612", "SHFE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2605", "DCE", "m", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        FuturesContract::new("DCE.m2611", "DCE", "m", false).unwrap(),
    ])
    .with_main_symbols(["SHFE.au2602", "DCE.m2605"])
    .with_quote_snapshots([
        quote("SHFE.au2602", "SHFE", "au", 90, 10),
        quote("SHFE.au2608", "SHFE", "au", 120, 8),
        quote("SHFE.au2612", "SHFE", "au", 80, 20),
        quote("DCE.m2605", "DCE", "m", 10, 1),
        quote("DCE.m2609", "DCE", "m", 200, 5),
        quote("DCE.m2611", "DCE", "m", 100, 50),
    ]);
    let selection = FuturesUniverseSelection {
        active_contracts_per_product: Some(2),
    };

    let symbols = resolve_futures_symbols_with_selection(
        &FuturesProductFilter::All,
        selection,
        &mut resolver,
    )
    .await
    .unwrap();

    assert_eq!(
        symbols,
        vec!["DCE.m2605", "DCE.m2609", "SHFE.au2602", "SHFE.au2608"]
    );
}

#[tokio::test]
async fn resolver_falls_back_to_activity_when_main_symbol_is_unknown() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2608", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2612", "SHFE", "au", false).unwrap(),
    ])
    .with_quote_snapshots([
        quote("SHFE.au2602", "SHFE", "au", 90, 10),
        quote("SHFE.au2608", "SHFE", "au", 120, 8),
        quote("SHFE.au2612", "SHFE", "au", 120, 20),
    ]);
    let selection = FuturesUniverseSelection {
        active_contracts_per_product: Some(2),
    };

    let symbols = resolve_futures_symbols_with_selection(
        &FuturesProductFilter::All,
        selection,
        &mut resolver,
    )
    .await
    .unwrap();

    assert_eq!(symbols, vec!["SHFE.au2608", "SHFE.au2612"]);
}

#[test]
fn futures_contract_extracts_product_code_from_symbol() {
    let contract = FuturesContract::from_symbol("CZCE.MA609", false).unwrap();

    assert_eq!(contract.exchange_id, "CZCE");
    assert_eq!(contract.product_id, "MA");
    assert_eq!(contract.symbol, "CZCE.MA609");
}

#[test]
fn futures_contract_uses_typed_quote_metadata_not_symbol_parser() {
    let quote = Quote {
        instrument_id: "CZCE.MA609".to_string(),
        exchange_id: "CZCE".to_string(),
        product_id: "typed-product".to_string(),
        expired: true,
        ..Quote::default()
    };

    let contract = FuturesContract::from_quote(&quote).unwrap();

    assert_eq!(contract.symbol, "CZCE.MA609");
    assert_eq!(contract.exchange_id, "CZCE");
    assert_eq!(contract.product_id, "typed-product");
    assert!(contract.expired);
}

fn quote(
    symbol: &str,
    exchange_id: &str,
    product_id: &str,
    open_interest: i64,
    volume: i64,
) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        exchange_id: exchange_id.to_string(),
        product_id: product_id.to_string(),
        open_interest,
        volume,
        ..Quote::default()
    }
}

#[test]
fn futures_metadata_symbol_batches_split_large_symbol_lists() {
    let symbols = vec![
        "SHFE.au2602".to_string(),
        "DCE.m2609".to_string(),
        "CZCE.MA609".to_string(),
        "GFEX.si2602".to_string(),
        "CFFEX.IF2606".to_string(),
    ];

    let batches = futures_metadata_symbol_batches(&symbols, 2).unwrap();

    assert_eq!(
        batches,
        vec![
            vec!["SHFE.au2602", "DCE.m2609"],
            vec!["CZCE.MA609", "GFEX.si2602"],
            vec!["CFFEX.IF2606"],
        ]
    );
}

#[test]
fn futures_metadata_symbol_batches_reject_zero_batch_size() {
    let symbols = vec!["SHFE.au2602".to_string()];

    let err = futures_metadata_symbol_batches(&symbols, 0).unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: futures metadata batch size must be greater than zero"
    );
}
