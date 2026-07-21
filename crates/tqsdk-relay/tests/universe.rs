use tqsdk_core::{Quote, TradingTime};
use tqsdk_relay::universe::FuturesUniverseResolver;
use tqsdk_relay::{
    FuturesContract, StaticFuturesUniverseResolver, UniverseExpression,
    futures_metadata_symbol_batches, resolve_futures_universe_symbols,
};

#[tokio::test]
async fn resolver_selects_all_non_expired_universe_symbols() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        FuturesContract::new("SHFE.au2512", "SHFE", "au", true).unwrap(),
    ]);

    let expression = UniverseExpression::parse("active:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
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
    let expression = UniverseExpression::parse("active:SHFE.au,m").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
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
    let expression = UniverseExpression::parse("top:2:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(
        symbols,
        vec!["DCE.m2605", "DCE.m2609", "SHFE.au2602", "SHFE.au2608"]
    );
}

#[tokio::test]
async fn resolver_preserves_symbol_info_trading_time_in_selected_contracts() {
    let trading_time = trading_time(&[("09:00:00", "10:15:00")], &[("21:00:00", "23:00:00")]);
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new_with_trading_time(
            "SHFE.au2602",
            "SHFE",
            "au",
            false,
            trading_time.clone(),
        )
        .unwrap(),
        FuturesContract::new("SHFE.au2512", "SHFE", "au", true).unwrap(),
    ]);

    let expression = UniverseExpression::parse("active:all").unwrap();

    let contracts = tqsdk_relay::universe::resolve_futures_contracts_with_expression(
        &expression,
        &mut resolver,
    )
    .await
    .unwrap();

    assert_eq!(contracts.len(), 1);
    assert_eq!(contracts[0].symbol, "SHFE.au2602");
    assert_eq!(contracts[0].trading_time, trading_time);
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
    let expression = UniverseExpression::parse("top:2:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["SHFE.au2608", "SHFE.au2612"]);
}

#[tokio::test]
async fn resolver_selects_only_main_contracts_without_quote_snapshots_when_limit_is_one() {
    let mut resolver = MainOnlyResolver {
        contracts: vec![
            FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
            FuturesContract::new("SHFE.au2608", "SHFE", "au", false).unwrap(),
            FuturesContract::new("DCE.m2605", "DCE", "m", false).unwrap(),
            FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        ],
        main_symbols: vec!["SHFE.au2602".to_string(), "DCE.m2605".to_string()],
    };
    let expression = UniverseExpression::parse("main:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2605", "SHFE.au2602"]);
}

#[tokio::test]
async fn expression_resolves_main_and_index_then_excludes_product() {
    let mut au = FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap();
    au.instrument_name = Some("沪金2602".to_string());
    let mut meal = FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap();
    meal.instrument_name = Some("豆粕2609".to_string());
    let mut resolver = StaticFuturesUniverseResolver::new([au, meal])
        .with_main_symbols(["SHFE.au2602", "DCE.m2609"]);
    let expression = UniverseExpression::parse("main:all;index:all;!SHFE.au").unwrap();

    let contracts = tqsdk_relay::universe::resolve_futures_contracts_with_expression(
        &expression,
        &mut resolver,
    )
    .await
    .unwrap();
    let symbols = contracts
        .iter()
        .map(|contract| contract.symbol.as_str())
        .collect::<Vec<_>>();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@DCE.m"]);
    assert_eq!(contracts[1].instrument_name.as_deref(), Some("豆粕加权"));
}

#[tokio::test]
async fn expression_preserves_trading_time_for_continuous_contracts() {
    let trading_time = trading_time(&[("09:00:00", "10:15:00")], &[("21:00:00", "25:00:00")]);
    let mut au = FuturesContract::new_with_trading_time(
        "SHFE.ao2609",
        "SHFE",
        "ao",
        false,
        trading_time.clone(),
    )
    .unwrap();
    au.instrument_name = Some("氧化铝2609".to_string());
    let mut resolver = StaticFuturesUniverseResolver::new([au]);
    let expression = UniverseExpression::parse("index:all;cont:all").unwrap();

    let contracts = tqsdk_relay::universe::resolve_futures_contracts_with_expression(
        &expression,
        &mut resolver,
    )
    .await
    .unwrap();

    assert_eq!(contracts.len(), 2);
    assert!(
        contracts
            .iter()
            .all(|contract| contract.trading_time == trading_time)
    );
    assert_eq!(contracts[0].symbol, "KQ.i@SHFE.ao");
    assert_eq!(contracts[1].symbol, "KQ.m@SHFE.ao");
}

#[tokio::test]
async fn expression_does_not_generate_index_contracts_for_kqd() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("KQD.S2609", "KQD", "S", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
    ]);
    let expression = UniverseExpression::parse("index:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["KQ.i@DCE.m"]);
}

#[tokio::test]
async fn expression_excludes_unsupported_kqd_external_contracts() {
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
async fn expression_excludes_kqd_with_bare_exchange_token() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("KQD.S2609", "KQD", "S", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
    ]);
    let expression = UniverseExpression::parse("active:all;!KQD").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609"]);
}

#[tokio::test]
async fn expression_resolves_top_n_and_continuous_symbols() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2608", "SHFE", "au", false).unwrap(),
    ])
    .with_main_symbols(["SHFE.au2602"])
    .with_quote_snapshots([
        quote("SHFE.au2602", "SHFE", "au", 90, 10),
        quote("SHFE.au2608", "SHFE", "au", 120, 8),
    ]);
    let expression = UniverseExpression::parse("top:2:all;cont:all").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["KQ.m@SHFE.au", "SHFE.au2602", "SHFE.au2608"]);
}

#[tokio::test]
async fn expression_excludes_exact_symbol_and_exchange() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
        FuturesContract::new("CFFEX.IF2606", "CFFEX", "IF", false).unwrap(),
    ]);
    let expression = UniverseExpression::parse("active:all;index:all;!CFFEX;!KQ.i@DCE.m").unwrap();

    let symbols = resolve_futures_universe_symbols(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@SHFE.au", "SHFE.au2602"]);
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
    let trading_time = trading_time(&[("09:00:00", "10:15:00")], &[]);
    let quote = Quote {
        instrument_id: "CZCE.MA609".to_string(),
        instrument_name: "甲醇609".to_string(),
        exchange_id: "CZCE".to_string(),
        product_id: "typed-product".to_string(),
        expired: true,
        trading_time: trading_time.clone(),
        ..Quote::default()
    };

    let contract = FuturesContract::from_quote(&quote).unwrap();

    assert_eq!(contract.symbol, "CZCE.MA609");
    assert_eq!(contract.exchange_id, "CZCE");
    assert_eq!(contract.product_id, "typed-product");
    assert_eq!(contract.instrument_name.as_deref(), Some("甲醇609"));
    assert!(contract.expired);
    assert_eq!(contract.trading_time, trading_time);
}

struct MainOnlyResolver {
    contracts: Vec<FuturesContract>,
    main_symbols: Vec<String>,
}

impl FuturesUniverseResolver for MainOnlyResolver {
    async fn active_futures(&mut self) -> tqsdk_relay::RelayResult<Vec<FuturesContract>> {
        Ok(self.contracts.clone())
    }

    async fn main_futures(&mut self) -> tqsdk_relay::RelayResult<Vec<String>> {
        Ok(self.main_symbols.clone())
    }

    async fn quote_snapshots(
        &mut self,
        _symbols: &[String],
    ) -> tqsdk_relay::RelayResult<Vec<Quote>> {
        panic!("main-only selection must not subscribe quote snapshots")
    }
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

fn trading_time(day: &[(&str, &str)], night: &[(&str, &str)]) -> TradingTime {
    TradingTime {
        day: day
            .iter()
            .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
            .collect(),
        night: night
            .iter()
            .map(|(start, end)| vec![(*start).to_string(), (*end).to_string()])
            .collect(),
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
