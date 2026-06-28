use tqsdk_data::{
    FuturesContract, StaticFuturesUniverseResolver, UniverseExpression,
    resolve_futures_universe_symbols,
};

#[tokio::test]
async fn selector_matches_relay_expression_semantics() {
    let expression = UniverseExpression::parse("active:all;!CFFEX").unwrap();
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.rb2601", "SHFE", "rb", false).unwrap(),
        FuturesContract::new("CFFEX.IF2601", "CFFEX", "IF", false).unwrap(),
    ]);

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["SHFE.rb2601"]);
}

#[tokio::test]
async fn selector_preserves_continuous_contract_semantics() {
    let mut meal = FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap();
    meal.instrument_name = Some("豆粕2609".to_string());
    let mut resolver = StaticFuturesUniverseResolver::new([meal]).with_main_symbols(["DCE.m2609"]);
    let expression = UniverseExpression::parse("main:all;index:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@DCE.m"]);
}
