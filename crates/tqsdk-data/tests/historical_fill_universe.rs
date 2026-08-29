use tqsdk_data::{HISTORICAL_FILL_UNIVERSE_CANONICALIZATION, HistoricalFillUniverseSpec};

#[test]
fn parses_observed_physical_and_timeline_without_changing_current_grammar() {
    let observed = HistoricalFillUniverseSpec::parse(" physical:all ").unwrap();
    assert_eq!(observed.to_string(), "physical:all");
    assert_eq!(
        observed.canonicalization_identity(),
        HISTORICAL_FILL_UNIVERSE_CANONICALIZATION
    );

    let timeline =
        HistoricalFillUniverseSpec::parse("timeline(active:all;cont:all;index:all;!exchange:KQD)")
            .unwrap();
    assert_eq!(
        timeline.to_string(),
        "timeline(active:all;cont:all;index:all;!exchange:KQD)"
    );
    assert!(timeline.timeline_expression().is_some());
}

#[test]
fn timeline_rejects_nested_implicit_and_ranking_selectors() {
    for expression in [
        "timeline(timeline(active:all))",
        "timeline(all)",
        "timeline(main:all)",
        "timeline(top:2:all)",
        "timeline(file:contracts.txt)",
        "timeline(!exchange:KQD)",
    ] {
        assert!(
            HistoricalFillUniverseSpec::parse(expression).is_err(),
            "{expression} must be rejected"
        );
    }
}

#[test]
fn historical_only_tokens_remain_invalid_current_universe_expressions() {
    assert!(tqsdk_data::UniverseExpression::parse("physical:all").is_err());
    assert!(tqsdk_data::UniverseExpression::parse("timeline(active:all)").is_err());
}
