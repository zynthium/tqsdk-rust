use serde::Serialize;
use sha2::{Digest, Sha256};
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

#[derive(Serialize)]
struct LegacyTimeline<'a> {
    catalog_id: &'a str,
    catalog_sha256: &'a str,
    calendar_identity: &'a str,
    start_ns: i64,
    end_ns: i64,
    scope: &'a DynamicUniverseScope,
    derived_views: &'a BTreeSet<DerivedView>,
    batches: &'a [tqsdk_data::UniverseTimelineBatch],
}

impl<'a> From<&'a tqsdk_data::HistoricalUniverseTimeline> for LegacyTimeline<'a> {
    fn from(timeline: &'a tqsdk_data::HistoricalUniverseTimeline) -> Self {
        Self {
            catalog_id: &timeline.catalog_id,
            catalog_sha256: &timeline.catalog_sha256,
            calendar_identity: &timeline.calendar_identity,
            start_ns: timeline.start_ns,
            end_ns: timeline.end_ns,
            scope: &timeline.scope,
            derived_views: &timeline.derived_views,
            batches: &timeline.batches,
        }
    }
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
    assert_eq!(plan.plan_version, 2);
    assert_eq!(
        plan.timeline.physical_listing_starts.get("SHFE.au2406"),
        Some(&10)
    );
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
fn v1_plan_remains_verifiable_without_listing_starts() {
    let scope = DynamicUniverseScope::all();
    let mut plan = CatalogSnapshot::new(
        "fixture-v1",
        "calendar-sha256:abc",
        true,
        scope.clone(),
        vec![contract("SHFE.au2406", 10, 20)],
    )
    .unwrap()
    .compile_timeline(0, 30, scope, [])
    .unwrap()
    .prepare(UniverseBudget::new(2, 2).unwrap())
    .unwrap();
    let legacy_bytes =
        serde_json::to_vec(&(1_u32, LegacyTimeline::from(&plan.timeline), plan.budget)).unwrap();
    plan.plan_version = 1;
    plan.timeline.physical_listing_starts.clear();
    plan.plan_sha256 = format!("sha256:{:x}", Sha256::digest(legacy_bytes));
    plan.verify().unwrap();
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
