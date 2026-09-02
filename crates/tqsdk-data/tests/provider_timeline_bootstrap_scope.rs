use std::collections::{BTreeMap, BTreeSet};

use tqsdk_data::{
    ExpandedUniverseInput, HistoricalAcquisitionContract, HistoricalCatalogAcquisition,
    HistoricalCatalogProof, HistoricalDailyObservation, PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS,
    UniverseInput, UniverseSpec, promote_provider_daily_history_observations,
    scope_provider_current_timeline_bootstrap,
};

fn contract(symbol: &str, exchange: &str, product: &str) -> HistoricalAcquisitionContract {
    HistoricalAcquisitionContract {
        symbol: symbol.to_string(),
        exchange_id: exchange.to_string(),
        product_id: product.to_string(),
        expired: false,
        expire_datetime_ns: None,
        authoritative_lifecycle: Vec::new(),
        first_available_data_ns: BTreeMap::new(),
    }
}

fn current(contracts: Vec<HistoricalAcquisitionContract>) -> HistoricalCatalogAcquisition {
    let roster = contracts
        .iter()
        .map(|contract| contract.symbol.clone())
        .collect::<Vec<_>>();
    HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "fixture:provider-current",
        "physical:all",
        PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS + 100,
        PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS + 200,
        true,
        roster.clone(),
        roster,
        contracts,
    )
    .unwrap()
}

fn fixture_current() -> HistoricalCatalogAcquisition {
    current(vec![
        contract("CZCE.RI307", "CZCE", "RI"),
        contract("CZCE.RI309", "CZCE", "RI"),
        contract("DCE.m2405", "DCE", "m"),
        contract("SHFE.au2406", "SHFE", "au"),
    ])
}

fn expanded(expression: &str) -> ExpandedUniverseInput {
    UniverseInput::from_spec(UniverseSpec::parse_v2(expression).unwrap())
        .expand()
        .unwrap()
}

fn symbols(acquisition: &HistoricalCatalogAcquisition) -> BTreeSet<String> {
    acquisition
        .contracts
        .iter()
        .map(|contract| contract.symbol.clone())
        .collect()
}

#[test]
fn contract_exclusions_do_not_enter_provider_daily_bootstrap_scope() {
    let input = expanded(
        "timeline(contract:all;except(contract:CFFEX.*,CZCE.ZC,CZCE.CY,CZCE.RI,CZCE.RS,CZCE.PM,CZCE.WH,CZCE.JR,CZCE.LR,DCE.rr,DCE.lg,DCE.fb,DCE.bb,SHFE.wr))",
    );

    let scoped = scope_provider_current_timeline_bootstrap(&fixture_current(), &input).unwrap();

    assert_eq!(
        symbols(&scoped),
        BTreeSet::from(["DCE.m2405".to_string(), "SHFE.au2406".to_string()])
    );
    assert!(
        scoped
            .source_identity
            .contains("timeline-bootstrap-closure.v1")
    );
}

#[test]
fn full_timeline_bootstrap_reuses_the_complete_discovery_acquisition() {
    let discovery = fixture_current();
    let input = expanded("timeline(contract:all)");

    let scoped = scope_provider_current_timeline_bootstrap(&discovery, &input).unwrap();

    assert_eq!(scoped, discovery);
}

#[test]
fn contract_exclusion_retains_required_continuous_underlyings() {
    let input = expanded("timeline(contract:all;continuous:CZCE.RI;except(contract:CZCE.RI))");

    let scoped = scope_provider_current_timeline_bootstrap(&fixture_current(), &input).unwrap();

    assert_eq!(symbols(&scoped), symbols(&fixture_current()));
}

#[test]
fn global_exclusion_removes_the_derived_product_bootstrap_closure() {
    let input = expanded("timeline(contract:all;continuous:CZCE.RI;except(all:CZCE.RI))");

    let scoped = scope_provider_current_timeline_bootstrap(&fixture_current(), &input).unwrap();

    assert_eq!(
        symbols(&scoped),
        BTreeSet::from(["DCE.m2405".to_string(), "SHFE.au2406".to_string()])
    );
}

#[test]
fn scoped_provider_history_refresh_reuses_its_pinned_closure() {
    let input = expanded("timeline(contract:all;except(contract:CZCE.RI))");
    let scoped = scope_provider_current_timeline_bootstrap(&fixture_current(), &input).unwrap();
    let observations = scoped
        .contracts
        .iter()
        .map(|contract| {
            (
                contract.symbol.clone(),
                HistoricalDailyObservation::new(
                    PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS,
                    PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS + 100,
                    Some(PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS + 1),
                )
                .unwrap(),
            )
        })
        .collect();
    let observed = promote_provider_daily_history_observations(scoped, observations).unwrap();
    let refreshed_full = current(vec![
        contract("CZCE.RI307", "CZCE", "RI"),
        contract("CZCE.RI309", "CZCE", "RI"),
        contract("CZCE.RI401", "CZCE", "RI"),
        contract("DCE.m2405", "DCE", "m"),
        contract("SHFE.au2406", "SHFE", "au"),
    ]);

    let refreshed = observed
        .project_provider_current_refresh(&refreshed_full)
        .unwrap();

    observed
        .validate_provider_daily_refresh_current(&refreshed)
        .unwrap();
    assert_eq!(symbols(&refreshed), symbols(&observed));
}
