use std::collections::BTreeMap;

use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, HistoricalAcquisitionContract,
    HistoricalCatalogAcquisition, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalSemanticCatalog, HistoricalUniverseArtifactStore,
};

fn contract(symbol: &str, start_ns: i64, end_ns: i64) -> HistoricalAcquisitionContract {
    HistoricalAcquisitionContract {
        symbol: symbol.to_string(),
        exchange_id: "SHFE".to_string(),
        product_id: "au".to_string(),
        expired: true,
        expire_datetime_ns: Some(end_ns),
        authoritative_lifecycle: vec![ActiveInterval::new(start_ns, end_ns).unwrap()],
        first_available_data_ns: BTreeMap::from([
            (HistoricalDataKind::Tick, start_ns + 10),
            (HistoricalDataKind::Minute, start_ns + 20),
            (HistoricalDataKind::Daily, start_ns + 30),
        ]),
    }
}

fn acquisition(proof: HistoricalCatalogProof) -> HistoricalCatalogAcquisition {
    HistoricalCatalogAcquisition::new(
        proof,
        "fixture:v1",
        "physical:all",
        500,
        600,
        true,
        vec!["SHFE.au2406".to_string(), "SHFE.au2404".to_string()],
        vec!["SHFE.au2404".to_string(), "SHFE.au2406".to_string()],
        vec![
            contract("SHFE.au2406", 200, 500),
            contract("SHFE.au2404", 100, 400),
        ],
    )
    .unwrap()
}

#[test]
fn acquisition_hash_is_canonical_and_kind_boundaries_remain_independent() {
    let acquisition = acquisition(HistoricalCatalogProof::AuthoritativeLifecycle);
    acquisition.validate().unwrap();
    assert_eq!(acquisition.roster_before, acquisition.roster_after);
    assert_eq!(acquisition.contracts[0].symbol, "SHFE.au2404");
    assert_ne!(
        acquisition.contracts[0].first_available_data_ns[&HistoricalDataKind::Tick],
        acquisition.contracts[0].first_available_data_ns[&HistoricalDataKind::Minute]
    );

    let rebuilt = HistoricalCatalogAcquisition::new(
        acquisition.proof,
        acquisition.source_identity.clone(),
        acquisition.canonical_universe.clone(),
        acquisition.requested_as_of_ns,
        acquisition.observed_at_ns,
        acquisition.complete,
        acquisition.roster_after.clone(),
        acquisition.roster_before.clone(),
        acquisition.contracts.iter().cloned().rev().collect(),
    )
    .unwrap();
    assert_eq!(rebuilt.acquisition_sha256, acquisition.acquisition_sha256);

    let mut tampered = acquisition.clone();
    tampered.contracts[0]
        .first_available_data_ns
        .insert(HistoricalDataKind::Minute, 999);
    assert!(
        tampered
            .validate()
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
}

#[test]
fn proof_and_stable_roster_are_fail_closed() {
    let drift = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "fixture:v1",
        "physical:all",
        500,
        600,
        true,
        vec!["SHFE.au2404".to_string()],
        vec!["SHFE.au2406".to_string()],
        vec![
            contract("SHFE.au2404", 100, 400),
            contract("SHFE.au2406", 200, 500),
        ],
    )
    .unwrap_err();
    assert!(drift.to_string().contains("stable before/after"));

    let mut observed_contract = contract("SHFE.au2404", 100, 400);
    observed_contract.authoritative_lifecycle.clear();
    let observed = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::ProviderCurrentObserved,
        "fixture:v1",
        "physical:all",
        500,
        600,
        true,
        vec!["SHFE.au2404".to_string()],
        vec!["SHFE.au2404".to_string()],
        vec![observed_contract.clone()],
    )
    .unwrap();
    let snapshot = CatalogSnapshot::new(
        "fixture-v1",
        "calendar:fixture-v1",
        true,
        tqsdk_data::DynamicUniverseScope::all(),
        vec![
            CatalogContract::new(
                "SHFE.au2404",
                "SHFE",
                "au",
                vec![ActiveInterval::new(100, 400).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert!(
        HistoricalSemanticCatalog::new(&observed, "timeline(active:all)", snapshot)
            .unwrap_err()
            .to_string()
            .contains("authoritative lifecycle")
    );

    let authoritative = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::AuthoritativeLifecycle,
        "fixture:v1",
        "physical:all",
        500,
        600,
        true,
        vec!["SHFE.au2404".to_string()],
        vec!["SHFE.au2404".to_string()],
        vec![observed_contract],
    );
    assert!(
        authoritative
            .unwrap_err()
            .to_string()
            .contains("every contract lifecycle")
    );
}

#[test]
fn content_addressed_store_round_trips_and_rejects_collision() {
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-artifact-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).unwrap();
    }
    let store = HistoricalUniverseArtifactStore::new(&root);
    let acquisition = acquisition(HistoricalCatalogProof::AuthoritativeLifecycle);

    let planned = store
        .acquisition_path(&acquisition.acquisition_sha256)
        .unwrap();
    assert!(
        !root.exists(),
        "path planning must not write during dry-run"
    );
    let published = store.publish_acquisition(&acquisition).unwrap();
    assert_eq!(published, planned);
    assert_eq!(
        store
            .load_acquisition(&acquisition.acquisition_sha256)
            .unwrap(),
        acquisition
    );
    store.publish_acquisition(&acquisition).unwrap();

    std::fs::write(&published, b"corrupt").unwrap();
    assert!(
        store
            .publish_acquisition(&acquisition)
            .unwrap_err()
            .to_string()
            .contains("collision")
    );
    std::fs::remove_dir_all(root).unwrap();
}
