use tqsdk_data::{
    HistoricalUniverseDispatch, SnapshotUniverseDispatch, UniverseEvaluationPolicy,
    UniverseLanguage, parse_historical_universe_compatible, parse_snapshot_universe_compatible,
};

#[test]
fn snapshot_dispatch_is_legacy_first_but_snapshot_wrapper_forces_v2() {
    let legacy = parse_snapshot_universe_compatible("cont:all").expect("legacy expression");
    assert!(matches!(legacy, SnapshotUniverseDispatch::Legacy { .. }));
    assert_eq!(legacy.report().language(), UniverseLanguage::LegacyV1);
    assert_eq!(
        legacy.report().evaluation_policy(),
        UniverseEvaluationPolicy::LegacySequentialV1
    );

    let v2 = parse_snapshot_universe_compatible("snapshot(continuous:all)")
        .expect("explicit V2 expression");
    assert!(matches!(v2, SnapshotUniverseDispatch::V2 { .. }));
    assert_eq!(v2.report().language(), UniverseLanguage::V2);
}

#[test]
fn snapshot_dispatch_rejects_timeline_before_trying_the_permissive_legacy_parser() {
    assert!(parse_snapshot_universe_compatible("timeline(cont:all)").is_err());
}

#[test]
fn historical_dispatch_preserves_valid_legacy_and_falls_back_to_v2() {
    let macro_spec =
        parse_historical_universe_compatible("physical:all").expect("legacy cache macro");
    assert!(matches!(
        macro_spec,
        HistoricalUniverseDispatch::Legacy { .. }
    ));

    let legacy_timeline = parse_historical_universe_compatible("timeline(cont:all)")
        .expect("legacy timeline expression");
    assert!(matches!(
        legacy_timeline,
        HistoricalUniverseDispatch::Legacy { .. }
    ));

    let v2_timeline = parse_historical_universe_compatible("timeline(contract:all)")
        .expect("V2 timeline expression");
    assert!(matches!(v2_timeline, HistoricalUniverseDispatch::V2 { .. }));
    assert_eq!(
        v2_timeline.report().evaluation_policy(),
        UniverseEvaluationPolicy::SetAlgebraV2
    );
}
