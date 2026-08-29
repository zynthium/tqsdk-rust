use std::collections::BTreeSet;

use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DerivedView, DynamicUniverseScope,
    UniverseBudget, UniverseInstrumentId, UniverseMemberChange,
};

fn contract(symbol: &str, start_ns: i64, end_ns: i64) -> CatalogContract {
    CatalogContract::new(
        symbol,
        "SHFE",
        "au",
        vec![ActiveInterval::new(start_ns, end_ns).unwrap()],
    )
    .unwrap()
}

#[test]
fn complete_catalog_keeps_delisted_contracts_in_historical_timeline() {
    let scope = DynamicUniverseScope::all();
    let catalog = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        true,
        scope.clone(),
        vec![
            contract("SHFE.au2406", 10, 20),
            contract("SHFE.au2412", 20, 40),
        ],
    )
    .unwrap();

    let timeline = catalog
        .compile_timeline(0, 50, scope, [DerivedView::Continuous, DerivedView::Index])
        .unwrap();

    assert_eq!(timeline.batches.len(), 3);
    assert_eq!(timeline.batches[0].effective_ns, 10);
    assert!(
        timeline.batches[0]
            .changes
            .contains(&UniverseMemberChange::Add {
                instrument: UniverseInstrumentId::Physical {
                    symbol: "SHFE.au2406".to_string(),
                },
                provenance: "catalog:fixture-v1".to_string(),
            })
    );
    assert!(
        timeline.batches[0]
            .changes
            .contains(&UniverseMemberChange::Add {
                instrument: UniverseInstrumentId::Continuous {
                    exchange_id: "SHFE".to_string(),
                    product_id: "au".to_string(),
                },
                provenance: "catalog:fixture-v1:derived".to_string(),
            })
    );
    assert_eq!(timeline.batches[2].effective_ns, 40);
    assert!(
        timeline.batches[2]
            .changes
            .contains(&UniverseMemberChange::Remove {
                instrument: UniverseInstrumentId::Index {
                    exchange_id: "SHFE".to_string(),
                    product_id: "au".to_string(),
                },
            })
    );
}

#[test]
fn incomplete_catalog_and_scope_mismatch_fail_closed() {
    let scope = DynamicUniverseScope::all();
    let incomplete = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        false,
        scope.clone(),
        vec![contract("SHFE.au2406", 10, 20)],
    );
    assert!(
        incomplete
            .unwrap_err()
            .to_string()
            .contains("complete=true")
    );

    let catalog = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        true,
        scope,
        vec![contract("SHFE.au2406", 10, 20)],
    )
    .unwrap();
    let mismatched_scope = DynamicUniverseScope {
        exchanges: BTreeSet::from(["SHFE".to_string()]),
        ..DynamicUniverseScope::all()
    };
    assert!(
        catalog
            .compile_timeline(0, 30, mismatched_scope, [])
            .unwrap_err()
            .to_string()
            .contains("exactly match")
    );
}

#[test]
fn lifecycle_intervals_must_be_sorted_and_non_overlapping() {
    let error = CatalogContract::new(
        "SHFE.au2406",
        "SHFE",
        "au",
        vec![
            ActiveInterval::new(20, 30).unwrap(),
            ActiveInterval::new(10, 25).unwrap(),
        ],
    )
    .unwrap_err();
    assert!(error.to_string().contains("sorted and non-overlapping"));
}

#[test]
fn prepared_plan_is_pinned_and_requires_an_explicit_budget() {
    let scope = DynamicUniverseScope::all();
    let timeline = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        true,
        scope.clone(),
        vec![contract("SHFE.au2406", 10, 20)],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap();
    let plan = timeline
        .clone()
        .prepare(UniverseBudget::new(2, 2).unwrap())
        .unwrap();
    assert!(plan.plan_sha256.starts_with("sha256:"));
    assert_eq!(plan.timeline, timeline);
    plan.verify().unwrap();
    let mut tampered = plan.clone();
    tampered.timeline.end_ns += 1;
    assert!(
        tampered
            .verify()
            .unwrap_err()
            .to_string()
            .contains("hash mismatch")
    );
    assert!(
        timeline
            .prepare(UniverseBudget::new(1, 1).unwrap())
            .unwrap_err()
            .to_string()
            .contains("exceeding budget")
    );
}

#[test]
fn timeline_validation_rejects_inconsistent_membership_changes() {
    let scope = DynamicUniverseScope::all();
    let timeline = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        true,
        scope.clone(),
        vec![contract("SHFE.au2406", 10, 20)],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap();
    let mut inconsistent = timeline.clone();
    let duplicate = inconsistent.batches[0].changes[0].clone();
    inconsistent.batches[0].changes.push(duplicate);
    assert!(
        inconsistent
            .validate()
            .unwrap_err()
            .to_string()
            .contains("already-active")
    );
}
