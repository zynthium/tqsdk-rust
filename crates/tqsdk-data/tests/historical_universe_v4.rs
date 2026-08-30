use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};
use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, HistoricalAcquisitionContract,
    HistoricalCatalogAcquisition, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalFillUniverseSpec, HistoricalPlanWritePolicy, HistoricalSemanticCatalog,
    HistoricalUniverseArtifactStore, HistoricalUniversePlanArtifact,
    HistoricalUniversePlanV3Identity, HistoricalUniversePlanV4, HistoricalUniversePlanV4Execution,
    UniverseBudget, UniverseSpec, compile_historical_universe_resolution,
    compile_historical_universe_resolution_v4,
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
) -> (HistoricalUniversePlanV4, tqsdk_data::HistoricalUniversePlan) {
    let spec = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
    let capabilities = (acquisition, semantic);
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(8, 16).unwrap(),
        None,
    )
    .unwrap();
    let write_set = resolution
        .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
        .unwrap();
    (write_set.v4().clone(), write_set.rollback_v3().clone())
}

#[test]
fn v4_artifact_uses_flat_canonical_wire_and_validated_round_trip() {
    let (acquisition, semantic, _) = fixture();
    let (plan, rollback) = v4_plan(&acquisition, &semantic);
    let artifact = HistoricalUniversePlanArtifact::V4(plan.clone());

    let bytes = serde_json::to_vec(&artifact).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "2d9d7e9690338b3ef872568d775a133920cb441d0d8c94d10f6ae91b873fcaa3",
        "V4 canonical JSON bytes are a persisted wire contract"
    );
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
        "sha256:7ff554a93c1f3c9e98b4e5c58364e9ca4a09aa53fde7042f4ed8f30dec4b1e8d"
    );
}

#[test]
fn artifact_store_reads_legacy_and_v4_but_old_plan_reader_remains_legacy_only() {
    let directory = TempDirectory::new();
    let (acquisition, semantic, _) = fixture();
    let (v4, rollback) = v4_plan(&acquisition, &semantic);
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
fn artifact_chain_rejects_a_self_valid_rollback_with_the_wrong_projection_identity() {
    let directory = TempDirectory::new();
    let (acquisition, semantic, _) = fixture();
    let (valid_v4, valid_rollback) = v4_plan(&acquisition, &semantic);
    let valid_identity = valid_rollback.v3_identity.as_ref().unwrap();
    let rollback_execution = valid_rollback.v3_execution.as_ref().unwrap().clone();
    let wrong_identity = HistoricalUniversePlanV3Identity::new(
        valid_identity.canonical_universe.clone(),
        "universe-v2-projection:wrong",
        valid_identity.acquisition_sha256.clone(),
        valid_identity.semantic_catalog_sha256.clone(),
        valid_identity.compiler_identity.clone(),
        valid_identity.proof,
    )
    .unwrap()
    .with_execution_sha256(rollback_execution.execution_sha256.clone())
    .unwrap();
    let wrong_rollback = valid_rollback
        .timeline
        .clone()
        .prepare_v3(
            valid_rollback.budget,
            wrong_identity,
            rollback_execution.clone(),
        )
        .unwrap();

    let spec = UniverseSpec::parse_v2("timeline(contract:all)").unwrap();
    let execution = HistoricalUniversePlanV4Execution::from_v3(&rollback_execution).unwrap();
    let identity = tqsdk_data::HistoricalUniversePlanV4Identity::builder(&spec)
        .acquisition_sha256(&acquisition.acquisition_sha256)
        .semantic_catalog_sha256(&semantic.semantic_catalog_sha256)
        .calendar_identity(&semantic.catalog.calendar_identity)
        .proof(HistoricalCatalogProof::AuthoritativeLifecycle)
        .execution_sha256(execution.execution_sha256())
        .rollback_v3_plan_sha256(&wrong_rollback.plan_sha256)
        .build()
        .unwrap();
    let wrong_v4 = HistoricalUniversePlanV4::new(
        valid_v4.timeline().clone(),
        valid_v4.budget(),
        identity,
        execution,
    )
    .unwrap();

    let store = HistoricalUniverseArtifactStore::new(&directory.0);
    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();
    store.publish_plan(&wrong_rollback).unwrap();
    let artifact = HistoricalUniversePlanArtifact::V4(wrong_v4);
    store.publish_plan_artifact(&artifact).unwrap();

    let error = store
        .verify_plan_artifact_chain_artifact(&artifact)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("rollback identity/execution chain mismatch"),
        "unexpected error: {error}"
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
