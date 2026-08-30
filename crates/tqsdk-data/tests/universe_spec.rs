use tqsdk_data::{
    UNIVERSE_CANONICALIZER_ID, UNIVERSE_COMPILER_ID, UNIVERSE_LANGUAGE_VERSION, UniverseMode,
    UniverseSpec, UniverseSpecError, UniverseTarget, UniverseView,
};

#[test]
fn v2_defaults_to_snapshot_and_normalizes_aliases_and_structural_targets() {
    let spec =
        UniverseSpec::parse_v2("contract:shfe.au2506,SHFE.au;cont:cffex.*;symbol:KQ.m@SHFE.au")
            .expect("valid V2 universe");

    assert_eq!(spec.mode(), UniverseMode::Snapshot);
    assert_eq!(
        spec.canonical_text(),
        "contract:SHFE.au,SHFE.au2506;continuous:CFFEX.*;symbol:KQ.m@SHFE.au"
    );
    assert_eq!(spec.includes()[0].view(), UniverseView::Contract);
    assert_eq!(
        spec.includes()[0].targets(),
        &[
            UniverseTarget::Product {
                exchange: "SHFE".to_string(),
                product: "au".to_string(),
            },
            UniverseTarget::Contract {
                exchange: "SHFE".to_string(),
                contract: "au2506".to_string(),
            },
        ]
    );
}

#[test]
fn v2_normalization_is_order_independent_and_deduplicates_same_polarity() {
    let left = UniverseSpec::parse_v2(
        "timeline(index:all;contract:SHFE.au2506;contract:DCE.m,DCE.m;top:5:SHFE.au;main:DCE.m)",
    )
    .expect("valid V2 universe");
    let right = UniverseSpec::parse_v2(
        "timeline(main:DCE.m;top:5:SHFE.au;contract:DCE.m,SHFE.au2506;index:all)",
    )
    .expect("equivalent V2 universe");

    assert_eq!(left, right);
    assert_eq!(left.mode(), UniverseMode::Timeline);
    assert_eq!(
        left.canonical_text(),
        "timeline(contract:DCE.m,SHFE.au2506;main:DCE.m;top:5:SHFE.au;index:all)"
    );
    assert_eq!(
        left.canonical_ast_json_bytes(),
        right.canonical_ast_json_bytes()
    );
    assert_eq!(left.canonical_ast_hash(), right.canonical_ast_hash());
}

#[test]
fn v2_uses_fixed_canonical_ast_wire_shape() {
    let spec = UniverseSpec::parse_v2(
        "timeline(index:all;contract:shfe.au2506,SHFE.au;cont:cffex.*;!contract:SHFE.au2507;!dce.*)",
    )
    .expect("valid V2 universe");

    assert_eq!(UNIVERSE_LANGUAGE_VERSION, 2);
    assert_eq!(UNIVERSE_CANONICALIZER_ID, "tqsdk.universe.canonical.v2");
    assert_eq!(UNIVERSE_COMPILER_ID, "tqsdk.universe.compiler.v2");
    assert_eq!(
        std::str::from_utf8(spec.canonical_ast_json_bytes()).expect("wire is UTF-8 JSON"),
        concat!(
            r#"{"language_version":2,"mode":"timeline","includes":["#,
            r#"{"view":{"kind":"contract","limit":null},"targets":["#,
            r#"{"kind":"product","exchange":"SHFE","value":"au"},"#,
            r#"{"kind":"contract","exchange":"SHFE","value":"au2506"}]},"#,
            r#"{"view":{"kind":"continuous","limit":null},"targets":["#,
            r#"{"kind":"exchange","exchange":"CFFEX","value":null}]},"#,
            r#"{"view":{"kind":"index","limit":null},"targets":["#,
            r#"{"kind":"all","exchange":null,"value":null}]}],"#,
            r#""excludes":[{"view":{"kind":"contract","limit":null},"targets":["#,
            r#"{"kind":"contract","exchange":"SHFE","value":"au2507"}]}],"#,
            r#""global_filters":[{"kind":"exchange","exchange":"DCE","value":null}]}"#,
        )
    );
    assert_eq!(
        spec.canonical_ast_hash(),
        "sha256:d9d6ba50de68602876bc3443cb0dafbae63e9528ce06f17e4f667ff40efccdcf"
    );
}

#[test]
fn v2_rejects_ambiguous_or_legacy_only_syntax() {
    for value in [
        "SHFE.au",
        "active:all",
        "physical:all",
        "exchange:CFFEX",
        "product:SHFE.au",
        "file:symbols.txt",
        "~SHFE.au",
        "snapshot(timeline(contract:all))",
    ] {
        assert!(
            UniverseSpec::parse_v2(value).is_err(),
            "{value:?} must not enter the V2 language"
        );
    }
}

#[test]
fn v2_validates_view_target_capabilities() {
    assert!(matches!(
        UniverseSpec::parse_v2("main:SHFE.au2506"),
        Err(UniverseSpecError::UnsupportedTarget { .. })
    ));
    assert!(matches!(
        UniverseSpec::parse_v2("symbol:all"),
        Err(UniverseSpecError::InvalidTarget { .. })
    ));
    assert!(matches!(
        UniverseSpec::parse_v2("top:0:all"),
        Err(UniverseSpecError::InvalidTopLimit { .. })
    ));
}

#[test]
fn v2_rejects_mixed_all_and_exact_view_contradictions() {
    assert!(matches!(
        UniverseSpec::parse_v2("contract:all;contract:SHFE.au"),
        Err(UniverseSpecError::MixedAll { .. })
    ));
    assert!(matches!(
        UniverseSpec::parse_v2("contract:SHFE.au;!contract:shfe.au"),
        Err(UniverseSpecError::ContradictorySelector { .. })
    ));

    UniverseSpec::parse_v2("contract:all;!contract:CFFEX.*")
        .expect("a broad include with a narrow exclusion is valid");
    UniverseSpec::parse_v2("contract:CFFEX.*;!CFFEX.*")
        .expect("a global filter may remove a broad include");
}

#[test]
fn v2_accepts_bare_structural_targets_only_as_global_filters() {
    let spec = UniverseSpec::parse_v2("contract:all;!cffex.*;!SHFE.au;!DCE.m2509")
        .expect("valid global filters");

    assert_eq!(
        spec.canonical_text(),
        "contract:all;!CFFEX.*;!SHFE.au;!DCE.m2509"
    );
    assert_eq!(spec.global_filters().len(), 3);
}
