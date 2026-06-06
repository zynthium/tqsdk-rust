use tqsdk_relay::{
    FuturesContract, FuturesProductCode, FuturesProductFilter, StaticFuturesUniverseResolver,
    resolve_futures_symbols,
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

#[test]
fn futures_contract_extracts_product_code_from_symbol() {
    let contract = FuturesContract::from_symbol("CZCE.MA609", false).unwrap();

    assert_eq!(contract.exchange_id, "CZCE");
    assert_eq!(contract.product_id, "MA");
    assert_eq!(contract.symbol, "CZCE.MA609");
}
