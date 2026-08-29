use std::collections::BTreeMap;

use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DynamicUniverseScope,
    HistoricalAcquisitionContract, HistoricalCatalogAcquisition, HistoricalCatalogProof,
    HistoricalDataKind, HistoricalDependencyRole, HistoricalFillUniverseSpec,
    HistoricalSemanticCatalog, UniverseBudget, UniverseInstrumentId, UniverseMemberChange,
    compile_historical_universe_resolution,
};

fn fixture() -> (HistoricalCatalogAcquisition, HistoricalSemanticCatalog) {
    let contracts = [
        ("SHFE.au2404", "SHFE", "au", 100, 400),
        ("SHFE.au2406", "SHFE", "au", 200, 500),
        ("DCE.m2405", "DCE", "m", 150, 450),
    ];
    let acquisition = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::AuthoritativeLifecycle,
        "fixture:v1",
        "physical:all",
        500,
        600,
        true,
        contracts
            .iter()
            .map(|(symbol, ..)| (*symbol).to_string())
            .collect(),
        contracts
            .iter()
            .map(|(symbol, ..)| (*symbol).to_string())
            .collect(),
        contracts
            .iter()
            .map(
                |(symbol, exchange, product, start, end)| HistoricalAcquisitionContract {
                    symbol: (*symbol).to_string(),
                    exchange_id: (*exchange).to_string(),
                    product_id: (*product).to_string(),
                    expired: true,
                    expire_datetime_ns: Some(*end),
                    authoritative_lifecycle: vec![ActiveInterval::new(*start, *end).unwrap()],
                    first_available_data_ns: BTreeMap::from([
                        (HistoricalDataKind::Tick, start + 1),
                        (HistoricalDataKind::Minute, start + 2),
                        (HistoricalDataKind::Daily, start + 3),
                    ]),
                },
            )
            .collect(),
    )
    .unwrap();
    let catalog = CatalogSnapshot::new(
        "fixture:v1",
        "calendar:fixture:v1",
        true,
        DynamicUniverseScope::all(),
        contracts
            .iter()
            .map(|(symbol, exchange, product, start, end)| {
                CatalogContract::new(
                    *symbol,
                    *exchange,
                    *product,
                    vec![ActiveInterval::new(*start, *end).unwrap()],
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let semantic = HistoricalSemanticCatalog::new(
        &acquisition,
        "timeline(active:all;cont:all;index:all)",
        catalog,
    )
    .unwrap();
    (acquisition, semantic)
}

#[test]
fn cont_only_keeps_physical_sources_hidden_but_dependency_closed() {
    let (acquisition, semantic) = fixture();
    let resolution = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(cont:all)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap();
    assert!(resolution.plan.timeline.batches.iter().all(|batch| {
        batch.changes.iter().all(|change| match change {
            UniverseMemberChange::Add { instrument, .. }
            | UniverseMemberChange::Remove { instrument } => {
                matches!(instrument, UniverseInstrumentId::Continuous { .. })
            }
        })
    }));
    assert!(resolution.dependencies.iter().all(|dependency| {
        dependency
            .roles
            .contains(&HistoricalDependencyRole::ContinuousUnderlying)
            && !dependency.source_symbol.starts_with("KQ.m@")
    }));
    assert_eq!(
        resolution.targets_for_kind(HistoricalDataKind::Minute)[0].start_ns,
        152
    );
    assert_ne!(
        resolution.resolved_targets_sha256[&HistoricalDataKind::Tick],
        resolution.resolved_targets_sha256[&HistoricalDataKind::Minute]
    );
}

#[test]
fn index_only_has_logical_source_without_exposing_physical_members() {
    let (acquisition, semantic) = fixture();
    let resolution = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(index:SHFE.au)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap();
    assert_eq!(resolution.dependencies.len(), 1);
    assert_eq!(resolution.dependencies[0].source_symbol, "KQ.i@SHFE.au");
    assert_eq!(
        resolution.dependencies[0].roles,
        [HistoricalDependencyRole::IndexSeries].into()
    );
    assert!(resolution.plan.timeline.batches.iter().all(|batch| {
        batch.changes.iter().all(|change| match change {
            UniverseMemberChange::Add { instrument, .. }
            | UniverseMemberChange::Remove { instrument } => {
                matches!(instrument, UniverseInstrumentId::Index { .. })
            }
        })
    }));
}

#[test]
fn combined_selection_and_exclusions_preserve_visible_membership() {
    let (acquisition, semantic) = fixture();
    let resolution = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(active:all;cont:all;index:all;!exchange:DCE)")
            .unwrap(),
        100,
        500,
        UniverseBudget::new(40, 80).unwrap(),
    )
    .unwrap();
    assert!(
        resolution
            .dependencies
            .iter()
            .all(|dependency| !dependency.source_symbol.contains("DCE"))
    );
    assert!(resolution.plan.plan_version == 3);
    resolution.plan.verify().unwrap();
}
