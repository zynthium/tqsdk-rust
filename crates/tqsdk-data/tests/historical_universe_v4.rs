use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, HistoricalAcquisitionContract,
    HistoricalCatalogAcquisition, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalFillUniverseSpec, HistoricalSemanticCatalog, HistoricalUniverseArtifactStore,
    HistoricalUniversePlanArtifact, HistoricalUniversePlanV4, HistoricalUniversePlanV4Execution,
    UniverseBudget, UniverseSpec, compile_historical_universe_resolution,
};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("tqsdk-historical-v4-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).expect("create temporary test directory");
        Self(path)
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn acquisition_contract(symbol: &str, start_ns: i64, end_ns: i64) -> HistoricalAcquisitionContract {
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

fn fixture() -> (
    HistoricalCatalogAcquisition,
    HistoricalSemanticCatalog,
    tqsdk_data::HistoricalUniversePlan,
) {
    let acquisition = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::AuthoritativeLifecycle,
        "fixture:v4",
        "physical:all",
        500,
        600,
        true,
        vec!["SHFE.au2404".to_string(), "SHFE.au2406".to_string()],
        vec!["SHFE.au2404".to_string(), "SHFE.au2406".to_string()],
        vec![
            acquisition_contract("SHFE.au2404", 100, 400),
            acquisition_contract("SHFE.au2406", 200, 500),
        ],
    )
    .unwrap();
    let snapshot = CatalogSnapshot::new(
        "fixture-v4",
        "calendar:fixture-v4",
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
            CatalogContract::new(
                "SHFE.au2406",
                "SHFE",
                "au",
                vec![ActiveInterval::new(200, 500).unwrap()],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let semantic =
        HistoricalSemanticCatalog::new(&acquisition, "timeline(active:all)", snapshot).unwrap();
    let plan = compile_historical_universe_resolution(
        &acquisition,
        &semantic,
        &HistoricalFillUniverseSpec::parse("timeline(active:all)").unwrap(),
        100,
        500,
        UniverseBudget::new(8, 16).unwrap(),
    )
    .unwrap()
    .plan;
    (acquisition, semantic, plan)
}

fn v4_plan(
    acquisition: &HistoricalCatalogAcquisition,
    semantic: &HistoricalSemanticCatalog,
    rollback: &tqsdk_data::HistoricalUniversePlan,
) -> HistoricalUniversePlanV4 {
    let spec = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
    let execution = HistoricalUniversePlanV4Execution::from_v3(
        rollback.v3_execution.as_ref().expect("fixture is V3"),
    )
    .unwrap();
    let identity = tqsdk_data::HistoricalUniversePlanV4Identity::builder(&spec)
        .acquisition_sha256(&acquisition.acquisition_sha256)
        .semantic_catalog_sha256(&semantic.semantic_catalog_sha256)
        .calendar_identity(&semantic.catalog.calendar_identity)
        .proof(HistoricalCatalogProof::AuthoritativeLifecycle)
        .execution_sha256(execution.execution_sha256())
        .rollback_v3_plan_sha256(&rollback.plan_sha256)
        .build()
        .unwrap();
    HistoricalUniversePlanV4::new(
        rollback.timeline.clone(),
        rollback.budget,
        identity,
        execution,
    )
    .unwrap()
}

#[test]
fn v4_artifact_uses_flat_canonical_wire_and_validated_round_trip() {
    let (acquisition, semantic, rollback) = fixture();
    let plan = v4_plan(&acquisition, &semantic, &rollback);
    let artifact = HistoricalUniversePlanArtifact::V4(plan.clone());

    let bytes = serde_json::to_vec(&artifact).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["plan_version"], 4);
    assert!(value.get("V4").is_none());
    assert_eq!(value["plan_sha256"], plan.plan_sha256());
    assert_eq!(
        plan.identity().rollback_v3_plan_sha256(),
        rollback.plan_sha256
    );

    let decoded: HistoricalUniversePlanArtifact = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, artifact);
    decoded.verify().unwrap();
    assert_eq!(
        plan.plan_sha256(),
        "sha256:d2367dfd05fef186a795213ba8c1db767ee42beb89dd95b286a5dec534c44e41"
    );
}

#[test]
fn artifact_store_reads_legacy_and_v4_but_old_plan_reader_remains_legacy_only() {
    let directory = TempDirectory::new();
    let (acquisition, semantic, rollback) = fixture();
    let v4 = v4_plan(&acquisition, &semantic, &rollback);
    let store = HistoricalUniverseArtifactStore::new(&directory.0);

    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();
    store.publish_plan(&rollback).unwrap();
    let v4_path = store
        .publish_plan_artifact(&HistoricalUniversePlanArtifact::V4(v4.clone()))
        .unwrap();

    let legacy = store
        .load_plan_artifact(&rollback.plan_sha256)
        .expect("new reader accepts legacy");
    assert!(matches!(legacy, HistoricalUniversePlanArtifact::Legacy(_)));
    let artifact = store
        .load_plan_artifact(v4.plan_sha256())
        .expect("new reader accepts V4");
    assert_eq!(artifact, HistoricalUniversePlanArtifact::V4(v4.clone()));
    store
        .verify_plan_artifact_chain_artifact(&artifact)
        .unwrap();
    assert!(store.load_plan(v4.plan_sha256()).is_err());

    let canonical = fs::read(&v4_path).unwrap();
    let mut noncanonical = vec![b' '];
    noncanonical.extend(canonical);
    fs::write(v4_path, noncanonical).unwrap();
    assert!(
        store
            .load_plan_artifact(v4.plan_sha256())
            .unwrap_err()
            .to_string()
            .contains("canonical")
    );
}

#[test]
fn artifact_reader_rejects_unknown_versions_and_incomplete_v4() {
    assert!(
        serde_json::from_str::<HistoricalUniversePlanArtifact>(r#"{"plan_version":5}"#).is_err()
    );
    assert!(
        serde_json::from_str::<HistoricalUniversePlanArtifact>(
            r#"{"plan_version":4,"plan_sha256":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}"#
        )
        .is_err()
    );
}
