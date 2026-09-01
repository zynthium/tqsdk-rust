use tqsdk_data::{
    HistoricalUniverseDispatch, SnapshotUniverseDispatch, UniverseSpec, UniverseSpecError,
    parse_historical_universe_compatible, parse_snapshot_universe_compatible,
};

fn invalid_target(input: &str) -> (String, &'static str) {
    match UniverseSpec::parse_v2(input) {
        Err(UniverseSpecError::InvalidTarget { target, reason }) => (target, reason),
        other => panic!("expected invalid target for {input:?}, got {other:?}"),
    }
}

#[test]
fn except_syntax_normalizes_to_existing_exclusion_identity() {
    let legacy = UniverseSpec::parse_v2(
        "timeline(contract:all;!contract:CFFEX.*,CZCE.ZC,CZCE.CY;!CFFEX.*;!SHFE.wr)",
    )
    .expect("legacy exclusion syntax parses");
    let except = UniverseSpec::parse_v2(
        "timeline(contract:all;except(contract:CFFEX.*,CZCE.ZC,CZCE.CY);except(all:CFFEX.*,SHFE.wr))",
    )
    .expect("compact exclusion syntax parses");

    assert_eq!(except, legacy);
    assert_eq!(
        except.canonical_ast_json_bytes(),
        legacy.canonical_ast_json_bytes()
    );
    assert_eq!(except.canonical_ast_hash(), legacy.canonical_ast_hash());
    assert_eq!(
        except.canonical_text(),
        "timeline(contract:all;!contract:CFFEX.*,CZCE.CY,CZCE.ZC;!CFFEX.*;!SHFE.wr)"
    );
}

#[test]
fn except_syntax_reuses_existing_view_target_rules() {
    let legacy =
        UniverseSpec::parse_v2("contract:all;top:2:all;!top:2:CZCE.ZC;!symbol:KQ.m@SHFE.au")
            .expect("legacy exclusion syntax parses");
    let except = UniverseSpec::parse_v2(
        "contract:all;top:2:all;except(top:2:CZCE.ZC);except(symbol:KQ.m@SHFE.au)",
    )
    .expect("view-scoped compact exclusions parse");

    assert_eq!(except, legacy);
}

#[test]
fn except_syntax_requires_an_explicit_scope_and_valid_global_targets() {
    assert_eq!(
        invalid_target("contract:all;except(CFFEX.*)"),
        (
            "CFFEX.*".to_string(),
            "except requires all:<targets> or view:<targets>",
        )
    );
    assert_eq!(
        invalid_target("contract:all;except(all:all)"),
        ("all".to_string(), "all is not a structural global filter")
    );
    assert_eq!(
        invalid_target("contract:all;except(all:CFFEX.*"),
        (
            "except(all:CFFEX.*".to_string(),
            "except clause must end with `)`",
        )
    );
}

#[test]
fn except_syntax_reaches_snapshot_compatibility_dispatch() {
    let legacy = parse_snapshot_universe_compatible(
        "snapshot(contract:all;!contract:CFFEX.*,CZCE.ZC;!CFFEX.*)",
    )
    .expect("legacy V2 snapshot parses");
    let except = parse_snapshot_universe_compatible(
        "snapshot(contract:all;except(contract:CFFEX.*,CZCE.ZC);except(all:CFFEX.*))",
    )
    .expect("compact V2 snapshot parses");
    let SnapshotUniverseDispatch::V2 {
        spec: legacy_spec, ..
    } = legacy
    else {
        panic!("snapshot wrapper must use V2");
    };
    let SnapshotUniverseDispatch::V2 {
        spec: except_spec, ..
    } = except
    else {
        panic!("compact snapshot must use V2");
    };

    assert_eq!(except_spec, legacy_spec);
    assert_eq!(
        except_spec.canonical_ast_hash(),
        legacy_spec.canonical_ast_hash()
    );
}

#[test]
fn except_syntax_accepts_the_cache_fill_product_list() {
    let spec = UniverseSpec::parse_v2(
        "timeline(contract:all;except(contract:CFFEX.*,CZCE.ZC,CZCE.CY,CZCE.RI,CZCE.RS,CZCE.PM,CZCE.WH,CZCE.JR,DCE.rr,DCE.lg,DCE.fb,DCE.bb,SHFE.wr))",
    )
    .expect("cache fill product list parses");

    assert_eq!(spec.excludes().len(), 1);
    assert_eq!(spec.excludes()[0].targets().len(), 13);
}

#[test]
fn except_syntax_reaches_historical_compatibility_dispatch() {
    let dispatch =
        parse_historical_universe_compatible("timeline(contract:all;except(all:CFFEX.*,CZCE.ZC))")
            .expect("V2 historical expression parses");
    let HistoricalUniverseDispatch::V2 { spec, .. } = dispatch else {
        panic!("compact V2 exclusion must not take the legacy path");
    };

    assert_eq!(
        spec.canonical_text(),
        "timeline(contract:all;!CFFEX.*;!CZCE.ZC)"
    );
}
