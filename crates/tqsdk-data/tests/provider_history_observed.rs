use std::collections::BTreeMap;

use tqsdk_data::{
    HistoricalAcquisitionContract, HistoricalCatalogAcquisition, HistoricalCatalogProof,
    HistoricalDailyObservation, HistoricalDailyObservationStatus, HistoricalDataKind,
    HistoricalFillUniverseSpec, HistoricalSemanticCatalog, HistoricalUniverseArtifactStore,
    PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS, UniverseBudget,
    compile_historical_universe_resolution, promote_provider_daily_history,
    promote_provider_daily_history_observations,
};

#[test]
fn daily_observation_compiles_user_request_floors_for_physical_and_derived_targets() {
    let origin = PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
    let contracts = vec![
        HistoricalAcquisitionContract {
            symbol: "SHFE.au2404".to_string(),
            exchange_id: "SHFE".to_string(),
            product_id: "au".to_string(),
            expired: true,
            expire_datetime_ns: Some(origin + 400),
            authoritative_lifecycle: Vec::new(),
            first_available_data_ns: BTreeMap::new(),
        },
        HistoricalAcquisitionContract {
            symbol: "SHFE.au2412".to_string(),
            exchange_id: "SHFE".to_string(),
            product_id: "au".to_string(),
            expired: false,
            expire_datetime_ns: None,
            authoritative_lifecycle: Vec::new(),
            first_available_data_ns: BTreeMap::new(),
        },
    ];
    let current = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "provider-current:v1",
        "physical:all",
        origin + 500,
        origin + 600,
        true,
        vec!["SHFE.au2404".to_string(), "SHFE.au2412".to_string()],
        vec!["SHFE.au2404".to_string(), "SHFE.au2412".to_string()],
        contracts,
    )
    .unwrap();
    let acquisition = promote_provider_daily_history(
        current,
        &BTreeMap::from([
            ("SHFE.au2404".to_string(), Some(origin + 100)),
            ("SHFE.au2412".to_string(), None),
        ]),
    )
    .unwrap();
    assert_eq!(acquisition.provider_daily_observations.len(), 2);
    assert_eq!(
        acquisition.provider_daily_observations["SHFE.au2412"].first_row_ns,
        None
    );
    assert!(
        !std::str::from_utf8(&serde_json::to_vec(&acquisition).unwrap())
            .unwrap()
            .contains("\"status\"")
    );
    let mut tampered = acquisition.clone();
    tampered.provider_daily_observations.remove("SHFE.au2412");
    assert!(
        tampered
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exactly cover acquired contracts")
    );
    let mut tampered = acquisition.clone();
    tampered
        .provider_daily_observations
        .get_mut("SHFE.au2404")
        .unwrap()
        .range_start_ns += 1;
    assert!(
        tampered
            .validate()
            .unwrap_err()
            .to_string()
            .contains("range does not match bootstrap contract")
    );
    let semantic =
        HistoricalSemanticCatalog::from_provider_history_observed(&acquisition, "calendar:test-v1")
            .unwrap();
    assert_eq!(semantic.catalog.contracts.len(), 1);

    let resolution = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(active:all;index:all)").unwrap(),
        origin + 200,
        origin + 400,
        UniverseBudget::new(20, 40).unwrap(),
    )
    .unwrap();
    for kind in [
        HistoricalDataKind::Tick,
        HistoricalDataKind::Minute,
        HistoricalDataKind::Daily,
    ] {
        let physical = resolution
            .targets_for_kind(kind)
            .iter()
            .find(|target| target.source_symbol == "SHFE.au2404")
            .unwrap();
        assert_eq!(physical.start_ns, origin + 200);
        let index = resolution
            .targets_for_kind(kind)
            .iter()
            .find(|target| target.source_symbol == "KQ.i@SHFE.au")
            .unwrap();
        assert_eq!(index.start_ns, origin + 200);
    }
    assert_eq!(
        resolution.plan.v3_identity.as_ref().unwrap().proof,
        HistoricalCatalogProof::ProviderHistoryObserved
    );

    let root = std::env::temp_dir().join(format!(
        "tqsdk-provider-history-observed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();
    store.publish_plan(&resolution.plan).unwrap();
    store.verify_plan_artifact_chain(&resolution.plan).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provider_history_promotion_requires_terminal_observation_for_every_roster_member() {
    let origin = PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
    let contract = HistoricalAcquisitionContract {
        symbol: "DCE.m2501".to_string(),
        exchange_id: "DCE".to_string(),
        product_id: "m".to_string(),
        expired: true,
        expire_datetime_ns: Some(origin + 500),
        authoritative_lifecycle: Vec::new(),
        first_available_data_ns: BTreeMap::new(),
    };
    let acquisition = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "provider-current:v1",
        "physical:all",
        origin + 600,
        origin + 700,
        true,
        vec![contract.symbol.clone()],
        vec![contract.symbol.clone()],
        vec![contract],
    )
    .unwrap();
    assert!(
        promote_provider_daily_history(acquisition, &BTreeMap::new())
            .unwrap_err()
            .to_string()
            .contains("exactly cover acquired roster")
    );
}

#[test]
fn provider_unavailable_candidate_is_audited_but_not_a_universe_member() {
    let origin = PROVIDER_DAILY_HISTORY_BOOTSTRAP_START_NS;
    let contract = HistoricalAcquisitionContract {
        symbol: "CZCE.CY011".to_string(),
        exchange_id: "CZCE".to_string(),
        product_id: "CY".to_string(),
        expired: true,
        expire_datetime_ns: None,
        authoritative_lifecycle: Vec::new(),
        first_available_data_ns: BTreeMap::new(),
    };
    let current = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "provider-current:v1",
        "physical:all",
        origin + 600,
        origin + 700,
        true,
        vec![contract.symbol.clone()],
        vec![contract.symbol.clone()],
        vec![contract],
    )
    .unwrap();
    let acquisition = promote_provider_daily_history_observations(
        current,
        BTreeMap::from([(
            "CZCE.CY011".to_string(),
            HistoricalDailyObservation::provider_unavailable(origin, origin + 600, 15_000_000_000)
                .unwrap(),
        )]),
    )
    .unwrap();

    assert_eq!(
        acquisition.provider_daily_observations["CZCE.CY011"].status,
        HistoricalDailyObservationStatus::ProviderUnavailable
    );
    assert_eq!(
        acquisition.provider_daily_observations["CZCE.CY011"].provider_unavailable_after_ns,
        Some(15_000_000_000)
    );
    let semantic =
        HistoricalSemanticCatalog::from_provider_history_observed(&acquisition, "calendar:test-v1")
            .unwrap();
    assert!(semantic.catalog.contracts.is_empty());

    let encoded = serde_json::to_vec(&acquisition).unwrap();
    assert!(
        std::str::from_utf8(&encoded)
            .unwrap()
            .contains("provider_unavailable")
    );
    let decoded: HistoricalCatalogAcquisition = serde_json::from_slice(&encoded).unwrap();
    decoded.validate().unwrap();
}
