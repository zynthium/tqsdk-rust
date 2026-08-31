use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, HistoricalAcquisitionContract,
    HistoricalCatalogAcquisition, HistoricalCatalogProof, HistoricalDataKind,
    HistoricalDependencyRole, HistoricalPlanWritePolicy, HistoricalSemanticCatalog,
    HistoricalUniverseArtifactStore, HistoricalUniversePlanArtifact, HistoricalUniversePlanV5,
    HistoricalUniverseV4Error, TimelineCapabilities, UniverseBudget, UniverseMemberChange,
    UniverseSpec, compile_historical_universe_resolution_v4,
};

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

#[allow(deprecated)]
#[test]
fn deprecated_daily_lifecycle_calendar_alias_remains_source_compatible() {
    assert_eq!(
        tqsdk_data::PROVIDER_DAILY_LIFECYCLE_CALENDAR_IDENTITY,
        tqsdk_data::PROVIDER_DAILY_MEMBERSHIP_CALENDAR_IDENTITY
    );
}

fn fixture() -> (HistoricalCatalogAcquisition, HistoricalSemanticCatalog) {
    let acquisition = HistoricalCatalogAcquisition::new(
        HistoricalCatalogProof::AuthoritativeLifecycle,
        "fixture:v4-resolution",
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
        "fixture-v4-resolution",
        "calendar:fixture-v4-resolution",
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
    let semantic = HistoricalSemanticCatalog::new(
        &acquisition,
        "timeline(contract:all;continuous:all;index:all)",
        snapshot,
    )
    .unwrap()
    .with_derived_availability(
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        BTreeMap::from([(
            "KQ.i@SHFE.au".to_string(),
            BTreeMap::from([
                (HistoricalDataKind::Tick, 150),
                (HistoricalDataKind::Minute, 160),
                (HistoricalDataKind::Daily, 170),
            ]),
        )]),
    )
    .unwrap();
    (acquisition, semantic)
}

#[test]
fn v2_timeline_compiler_keeps_logical_provenance_and_kind_specific_boundaries() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2(concat!(
        "timeline(contract:all;continuous:SHFE.au;index:SHFE.au;",
        "!contract:SHFE.au2404)"
    ))
    .unwrap();

    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();

    let visible = resolution
        .timeline()
        .batches
        .iter()
        .flat_map(|batch| &batch.changes)
        .filter_map(|change| match change {
            UniverseMemberChange::Add { instrument, .. } => Some(instrument.symbol()),
            UniverseMemberChange::Remove { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        visible,
        BTreeSet::from([
            "KQ.i@SHFE.au".to_string(),
            "KQ.m@SHFE.au".to_string(),
            "SHFE.au2406".to_string(),
        ])
    );

    let au2404 = resolution
        .dependencies()
        .iter()
        .find(|dependency| dependency.source_symbol == "SHFE.au2404")
        .expect("logical views retain the excluded contract as a dependency");
    assert_eq!(
        au2404.roles,
        BTreeSet::from([HistoricalDependencyRole::ContinuousUnderlying])
    );
    let index = resolution
        .dependencies()
        .iter()
        .find(|dependency| dependency.source_symbol == "KQ.i@SHFE.au")
        .expect("index view keeps its directly downloadable series");
    assert_eq!(
        index.roles,
        BTreeSet::from([HistoricalDependencyRole::IndexSeries])
    );
    let au2406 = resolution
        .dependencies()
        .iter()
        .find(|dependency| dependency.source_symbol == "SHFE.au2406")
        .unwrap();
    assert!(
        au2406
            .roles
            .contains(&HistoricalDependencyRole::VisiblePhysical)
    );

    let tick = resolution.targets_for_kind(HistoricalDataKind::Tick);
    let minute = resolution.targets_for_kind(HistoricalDataKind::Minute);
    let daily = resolution.targets_for_kind(HistoricalDataKind::Daily);
    assert_eq!(
        tick.iter()
            .find(|target| target.source_symbol == "SHFE.au2404")
            .unwrap()
            .start_ns,
        110
    );
    assert_eq!(
        minute
            .iter()
            .find(|target| target.source_symbol == "SHFE.au2404")
            .unwrap()
            .start_ns,
        120
    );
    assert_eq!(
        daily
            .iter()
            .find(|target| target.source_symbol == "SHFE.au2404")
            .unwrap()
            .start_ns,
        130
    );
    assert_eq!(
        tick.iter()
            .find(|target| target.source_symbol == "KQ.i@SHFE.au")
            .unwrap()
            .start_ns,
        150
    );
    assert_eq!(
        minute
            .iter()
            .find(|target| target.source_symbol == "KQ.i@SHFE.au")
            .unwrap()
            .start_ns,
        160
    );
    assert_eq!(
        daily
            .iter()
            .find(|target| target.source_symbol == "KQ.i@SHFE.au")
            .unwrap()
            .start_ns,
        170
    );
    assert_ne!(
        resolution.resolved_targets_sha256()[&HistoricalDataKind::Tick],
        resolution.resolved_targets_sha256()[&HistoricalDataKind::Minute]
    );
}

#[test]
fn exact_contract_requires_provider_membership_intersection() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:SHFE.au2404)").unwrap();

    assert!(matches!(
        compile_historical_universe_resolution_v4(
            &capabilities,
            &spec,
            &[],
            450,
            500,
            UniverseBudget::new(8, 16).unwrap(),
            None,
        ),
        Err(HistoricalUniverseV4Error::NoCandidates)
    ));
}

struct CountingCapabilities {
    acquisition: HistoricalCatalogAcquisition,
    semantic: HistoricalSemanticCatalog,
    calls: Cell<usize>,
}

impl TimelineCapabilities for CountingCapabilities {
    fn acquisition(&self) -> tqsdk_data::Result<&HistoricalCatalogAcquisition> {
        self.calls.set(self.calls.get() + 1);
        Ok(&self.acquisition)
    }

    fn semantic_catalog(&self) -> tqsdk_data::Result<&HistoricalSemanticCatalog> {
        self.calls.set(self.calls.get() + 1);
        Ok(&self.semantic)
    }
}

#[test]
fn timeline_main_and_top_fail_before_any_catalog_capability_call() {
    let (acquisition, semantic) = fixture();
    let capabilities = CountingCapabilities {
        acquisition,
        semantic,
        calls: Cell::new(0),
    };
    for expression in ["timeline(main:all)", "timeline(top:3:SHFE.au)"] {
        let spec = UniverseSpec::parse_v2(expression).unwrap();
        assert!(matches!(
            compile_historical_universe_resolution_v4(
                &capabilities,
                &spec,
                &[],
                100,
                500,
                UniverseBudget::new(8, 16).unwrap(),
                None,
            ),
            Err(HistoricalUniverseV4Error::UnsupportedTimelineRanking { .. })
        ));
    }
    assert_eq!(capabilities.calls.get(), 0);
}

#[test]
fn write_set_projects_byte_equivalent_execution_and_pins_rollback() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:all;continuous:SHFE.au)").unwrap();
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();

    assert_eq!(
        HistoricalPlanWritePolicy::from_str("legacy-only").unwrap(),
        HistoricalPlanWritePolicy::LegacyOnly
    );
    assert!(matches!(
        resolution.prepare_write_set(HistoricalPlanWritePolicy::LegacyOnly),
        Err(HistoricalUniverseV4Error::WriterDisabled)
    ));
    let write_set = resolution
        .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
        .unwrap();
    assert_eq!(
        write_set.v4().identity().rollback_v3_plan_sha256(),
        write_set.rollback_v3().plan_sha256
    );
    assert_eq!(
        write_set.v4().execution().to_v3().unwrap(),
        *write_set.rollback_v3().v3_execution.as_ref().unwrap()
    );
    assert_ne!(
        write_set
            .rollback_v3()
            .v3_identity
            .as_ref()
            .unwrap()
            .execution_sha256
            .as_deref()
            .unwrap(),
        write_set.v4().identity().execution_sha256(),
        "V3 and V4 hash equivalent execution under version-specific domains"
    );
    assert_eq!(write_set.v4().timeline(), &write_set.rollback_v3().timeline);
}

#[test]
fn explicit_and_file_continuous_symbols_pin_the_materialized_mapping_identity() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let cases = [
        (
            UniverseSpec::parse_v2("timeline(symbol:KQ.m@SHFE.au)").unwrap(),
            Vec::new(),
            None,
        ),
        (
            UniverseSpec::parse_v2("timeline(contract:SHFE.au2406)").unwrap(),
            vec!["KQ.m@SHFE.au".to_string()],
            Some(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
            ),
        ),
    ];

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-universe-v4-materialized-continuous-{}-{nanos}",
        std::process::id()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();

    for (spec, expanded_symbols, input_sources_sha256) in cases {
        let resolution = compile_historical_universe_resolution_v4(
            &capabilities,
            &spec,
            &expanded_symbols,
            100,
            500,
            UniverseBudget::new(16, 32).unwrap(),
            None,
        )
        .unwrap()
        .with_input_sources_sha256(input_sources_sha256);
        let write_set = resolution
            .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
            .unwrap();

        assert_eq!(
            write_set.v4().identity().continuous_identity(),
            Some(tqsdk_data::HISTORICAL_UNIVERSE_CONTINUOUS_ID)
        );
        assert_eq!(
            write_set
                .rollback_v3()
                .v3_identity
                .as_ref()
                .unwrap()
                .continuous_identity
                .as_deref(),
            Some(tqsdk_data::HISTORICAL_UNIVERSE_CONTINUOUS_ID)
        );
        store.publish_plan_write_set(&write_set).unwrap();
        store
            .verify_plan_artifact_chain_artifact(&HistoricalUniversePlanArtifact::V4(
                write_set.v4().clone(),
            ))
            .unwrap();
    }

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn v4_to_v5_migration_verifies_the_full_chain_and_preserves_the_source() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:all;continuous:SHFE.au)").unwrap();
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();
    let write_set = resolution
        .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
        .unwrap();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-universe-v5-migration-{}-{nanos}",
        std::process::id()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();
    let published = store.publish_plan_write_set(&write_set).unwrap();
    let source_path = published.v4_path().to_path_buf();
    let source_bytes = fs::read(&source_path).unwrap();

    let preview = store
        .preview_v4_migration(published.v4_plan_sha256())
        .unwrap();
    assert_eq!(preview.source_plan_version(), 4);
    assert_eq!(preview.current_plan_version(), 5);
    assert!(!preview.current_path().exists());

    let migration = store.migrate_v4_plan(published.v4_plan_sha256()).unwrap();
    assert_eq!(migration, preview);
    assert_eq!(
        migration.current_plan_sha256(),
        "sha256:1bed0981628430f913cc176c0e14662c72398da8d9d48675f282c927821d9dc0"
    );
    assert_eq!(fs::read(&source_path).unwrap(), source_bytes);
    assert!(migration.current_path().is_file());
    assert!(published.rollback_v3_path().is_file());

    let current = store
        .load_current_plan(migration.current_plan_sha256())
        .unwrap();
    assert_eq!(current.timeline(), write_set.v4().timeline());
    assert_eq!(current.budget(), write_set.v4().budget());
    store.verify_current_plan_artifact_chain(&current).unwrap();
    assert!(matches!(
        store
            .load_plan_artifact(migration.current_plan_sha256())
            .unwrap(),
        HistoricalUniversePlanArtifact::V5(_)
    ));

    let canonical = fs::read(migration.current_path()).unwrap();
    assert_eq!(
        serde_json::to_vec(&current).unwrap(),
        current.canonical_json_bytes().unwrap()
    );
    assert_eq!(current.canonical_json_bytes().unwrap(), canonical);
    assert_eq!(
        format!("{:x}", Sha256::digest(&canonical)),
        "290e0772163372a460e2b369a04f5d49b69ee77d9fe7466bcd0245fdb8deeb5b",
        "V5 canonical artifact byte SHA-256 changed"
    );
    let uppercase_sha = String::from_utf8(canonical.clone())
        .unwrap()
        .replace("sha256:", "SHA256:");
    assert!(serde_json::from_str::<HistoricalUniversePlanV5>(&uppercase_sha).is_err());
    let unknown_nested_field = String::from_utf8(canonical.clone()).unwrap().replacen(
        "\"scope\":{",
        "\"scope\":{\"unexpected\":true,",
        1,
    );
    assert!(serde_json::from_str::<HistoricalUniversePlanV5>(&unknown_nested_field).is_err());

    let noncanonical_hash =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let noncanonical_path = store.plan_path(noncanonical_hash).unwrap();
    fs::create_dir_all(noncanonical_path.parent().unwrap()).unwrap();
    let mut noncanonical = b" \n".to_vec();
    noncanonical.extend_from_slice(&canonical);
    fs::write(&noncanonical_path, noncanonical).unwrap();
    let error = store.load_current_plan(noncanonical_hash).unwrap_err();
    assert!(error.to_string().contains("canonical V5 JSON"));

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn current_prepare_plan_publishes_a_v5_artifact_without_a_rollback_companion() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:all;continuous:SHFE.au)").unwrap();
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();
    let plan = resolution.prepare_plan().unwrap();
    assert_eq!(
        plan.plan_version(),
        tqsdk_data::HISTORICAL_UNIVERSE_PLAN_VERSION
    );
    assert_eq!(plan.plan_version(), 5);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-universe-v5-current-{}-{nanos}",
        std::process::id()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    store.publish_acquisition(&acquisition).unwrap();
    store.publish_semantic_catalog(&semantic).unwrap();
    let path = store.publish_current_plan(&plan).unwrap();
    assert!(path.is_file());
    assert_eq!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json"))
            .count(),
        1
    );
    let loaded = store.load_current_plan(plan.plan_sha256()).unwrap();
    assert_eq!(loaded, plan);
    store.verify_current_plan_artifact_chain(&loaded).unwrap();

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn dual_write_publishes_loadable_v4_and_v3_artifacts() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:all;continuous:SHFE.au)").unwrap();
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();
    let write_set = resolution
        .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
        .unwrap();

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-universe-v4-dual-write-{}-{nanos}",
        std::process::id()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    let published = store.publish_plan_write_set(&write_set).unwrap();

    assert!(published.v4_path().is_file());
    assert!(published.rollback_v3_path().is_file());
    assert_eq!(published.v4_plan_sha256(), write_set.v4().plan_sha256());
    assert_eq!(
        published.rollback_v3_plan_sha256(),
        write_set.rollback_v3().plan_sha256
    );
    assert!(matches!(
        store
            .load_plan_artifact(published.v4_plan_sha256())
            .unwrap(),
        HistoricalUniversePlanArtifact::V4(_)
    ));
    assert_eq!(
        store
            .load_plan(published.rollback_v3_plan_sha256())
            .unwrap(),
        write_set.rollback_v3().clone()
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dual_write_failure_preserves_the_collision_and_leaves_a_valid_rollback_orphan() {
    let (acquisition, semantic) = fixture();
    let capabilities = (&acquisition, &semantic);
    let spec = UniverseSpec::parse_v2("timeline(contract:all;continuous:SHFE.au)").unwrap();
    let resolution = compile_historical_universe_resolution_v4(
        &capabilities,
        &spec,
        &[],
        100,
        500,
        UniverseBudget::new(16, 32).unwrap(),
        None,
    )
    .unwrap();
    let write_set = resolution
        .prepare_write_set(HistoricalPlanWritePolicy::V4WithV3Rollback)
        .unwrap();

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "tqsdk-historical-universe-v4-partial-write-{}-{nanos}",
        std::process::id()
    ));
    let store = HistoricalUniverseArtifactStore::new(&root);
    let v4_path = store.plan_path(write_set.v4().plan_sha256()).unwrap();
    fs::create_dir_all(v4_path.parent().unwrap()).unwrap();
    fs::write(&v4_path, b"immutable collision").unwrap();

    let error = store.publish_plan_write_set(&write_set).unwrap_err();
    assert!(
        error.to_string().contains("hash collision"),
        "unexpected error: {error}"
    );
    assert_eq!(fs::read(&v4_path).unwrap(), b"immutable collision");
    assert_eq!(
        store
            .load_plan(&write_set.rollback_v3().plan_sha256)
            .unwrap(),
        write_set.rollback_v3().clone()
    );
    assert!(
        store
            .load_plan_artifact(write_set.v4().plan_sha256())
            .is_err()
    );

    fs::remove_dir_all(root).unwrap();
}
