use tqsdk_data::{
    FuturesContract, FuturesUniverseResolver, StaticFuturesUniverseResolver, UniverseExpression,
    UniverseInput, UniverseSpec, resolve_futures_universe_symbols, resolve_futures_universe_v2,
    resolve_static_symbols_with_expression,
};

struct CountingFuturesUniverseResolver {
    contracts: Vec<FuturesContract>,
    active_futures_calls: usize,
}

impl FuturesUniverseResolver for CountingFuturesUniverseResolver {
    async fn active_futures(&mut self) -> tqsdk_data::Result<Vec<FuturesContract>> {
        self.active_futures_calls += 1;
        Ok(self.contracts.clone())
    }
}

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
async fn universe_v2_snapshot_uses_the_shared_materialized_provider_adapter() {
    let input = UniverseInput::from_spec(
        UniverseSpec::parse_v2(concat!(
            "snapshot(contract:all;main:DCE.m;continuous:DCE.m;index:DCE.m;",
            "!contract:SHFE.rb2601)"
        ))
        .unwrap(),
    )
    .expand()
    .unwrap();
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        FuturesContract::new("SHFE.rb2601", "SHFE", "rb", false).unwrap(),
        FuturesContract::new("KQD.CBOT.KE", "KQD", "CBOT.KE", false).unwrap(),
    ])
    .with_main_symbols(["DCE.m2609"]);

    let compiled = resolve_futures_universe_v2(&input, &mut resolver)
        .await
        .unwrap();
    let symbols = compiled
        .candidates()
        .iter()
        .map(|candidate| candidate.symbol())
        .collect::<Vec<_>>();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@DCE.m", "KQ.m@DCE.m"]);
    assert_eq!(compiled.physical_dependencies(), ["DCE.m2609"]);
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

#[tokio::test]
async fn selector_excludes_unsupported_kqd_external_contracts() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("KQD.CBOT.KE", "KQD", "CBOT.KE", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
    ])
    .with_main_symbols(["KQD.CBOT.KE", "DCE.m2609"]);
    let expression = UniverseExpression::parse("active:all;main:all;index:all;cont:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@DCE.m", "KQ.m@DCE.m"]);
}

#[tokio::test]
async fn combined_cont_and_index_selectors_load_active_futures_once() {
    let expression = UniverseExpression::parse("cont:all;index:all").unwrap();
    let mut resolver = CountingFuturesUniverseResolver {
        contracts: vec![FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap()],
        active_futures_calls: 0,
    };

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["KQ.i@DCE.m", "KQ.m@DCE.m"]);
    assert_eq!(resolver.active_futures_calls, 1);
}

#[test]
fn static_selector_excludes_unsupported_kqd_external_symbols() {
    let expression = UniverseExpression::parse("symbol:KQD.CBOT.KE,DCE.m2609").unwrap();

    let symbols = resolve_static_symbols_with_expression(&expression).unwrap();

    assert_eq!(symbols, vec!["DCE.m2609"]);
}
