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
    .unwrap()
    .with_derived_availability(
        "fixture-derived-availability:v1",
        BTreeMap::from([
            (
                "KQ.i@SHFE.au".to_string(),
                BTreeMap::from([
                    (HistoricalDataKind::Tick, 101),
                    (HistoricalDataKind::Minute, 102),
                    (HistoricalDataKind::Daily, 103),
                ]),
            ),
            (
                "KQ.i@DCE.m".to_string(),
                BTreeMap::from([
                    (HistoricalDataKind::Tick, 151),
                    (HistoricalDataKind::Minute, 152),
                    (HistoricalDataKind::Daily, 153),
                ]),
            ),
        ]),
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

#[test]
fn missing_kind_boundary_is_not_replaced_by_listing_time() {
    let (acquisition, semantic) = fixture();
    let mut contracts = acquisition.contracts.clone();
    contracts
        .iter_mut()
        .find(|contract| contract.symbol == "SHFE.au2404")
        .unwrap()
        .first_available_data_ns
        .remove(&HistoricalDataKind::Daily);
    let incomplete_boundaries = HistoricalCatalogAcquisition::new(
        acquisition.proof,
        acquisition.source_identity.clone(),
        acquisition.canonical_universe.clone(),
        acquisition.requested_as_of_ns,
        acquisition.observed_at_ns,
        acquisition.complete,
        acquisition.roster_before.clone(),
        acquisition.roster_after.clone(),
        contracts,
    )
    .unwrap();
    let semantic = HistoricalSemanticCatalog::new(
        &incomplete_boundaries,
        semantic.canonical_universe,
        semantic.catalog,
    )
    .unwrap();

    let error = compile_historical_universe_resolution(
        &incomplete_boundaries,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(cont:SHFE.au)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Daily availability boundary is unproven")
    );
}

#[test]
fn logical_index_series_requires_pinned_availability_evidence() {
    let (acquisition, semantic_with_evidence) = fixture();
    let semantic = HistoricalSemanticCatalog::new(
        &acquisition,
        semantic_with_evidence.canonical_universe,
        semantic_with_evidence.catalog,
    )
    .unwrap();
    let error = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(index:SHFE.au)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("KQ.i@SHFE.au"));
    assert!(error.to_string().contains("unproven"));
}

#[test]
fn v3_plan_rejects_tampered_pinned_targets() {
    let (acquisition, semantic) = fixture();
    let mut plan = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(active:all)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap()
    .plan;
    plan.v3_execution
        .as_mut()
        .unwrap()
        .targets
        .get_mut(&HistoricalDataKind::Minute)
        .unwrap()[0]
        .start_ns += 1;
    assert!(
        plan.verify()
            .unwrap_err()
            .to_string()
            .contains("target hash mismatch")
    );
}

#[test]
fn legacy_exact_physical_exclusion_still_removes_the_product_continuous_view() {
    let (acquisition, semantic) = fixture();
    let error = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(cont:SHFE.au;!symbol:SHFE.au2406)").unwrap(),
        100,
        500,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("historical universe selector resolves no visible members"),
        "unexpected error: {error}"
    );
}

#[test]
fn legacy_later_continuous_include_survives_an_earlier_product_exclusion() {
    let (acquisition, semantic) = fixture();
    let resolution = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(!product:SHFE.au;cont:SHFE.au)").unwrap(),
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
    assert_eq!(resolution.dependencies.len(), 2);
    assert!(resolution.dependencies.iter().all(|dependency| {
        dependency.source_symbol.starts_with("SHFE.au")
            && dependency
                .roles
                .contains(&HistoricalDependencyRole::ContinuousUnderlying)
    }));
    assert_eq!(
        resolution
            .plan
            .v3_identity
            .as_ref()
            .unwrap()
            .canonical_universe,
        "timeline(!product:SHFE.au;cont:SHFE.au)"
    );
}
