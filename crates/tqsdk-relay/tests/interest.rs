use tqsdk_relay::{ClientId, InterestRegistry, SetChartCommand};

fn chart(chart_id: &str) -> SetChartCommand {
    chart_with(chart_id, vec!["SHFE.au2602"], 60_000_000_000, 64)
}

fn chart_with(
    chart_id: &str,
    symbols: Vec<&str>,
    duration_ns: i64,
    view_width: usize,
) -> SetChartCommand {
    SetChartCommand {
        chart_id: chart_id.to_string(),
        symbols: symbols.into_iter().map(ToString::to_string).collect(),
        duration_ns,
        view_width,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    }
}

#[test]
fn same_downstream_chart_id_is_isolated_by_client() {
    let mut registry = InterestRegistry::default();
    let client_a = ClientId::new(1);
    let client_b = ClientId::new(2);

    let source_a = registry.set_chart(client_a, chart("chart-1"));
    let source_b = registry.set_chart(client_b, chart("chart-1"));

    assert_eq!(source_a, source_b);
    assert_eq!(
        registry.downstream_chart_id(client_a, &source_a),
        Some("chart-1")
    );
    assert_eq!(
        registry.downstream_chart_id(client_b, &source_b),
        Some("chart-1")
    );
    assert_eq!(registry.chart_interest_count(&source_a), 2);
}

#[test]
fn removing_one_client_keeps_shared_source_for_other_client() {
    let mut registry = InterestRegistry::default();
    let source = registry.set_chart(ClientId::new(1), chart("chart-1"));
    registry.set_chart(ClientId::new(2), chart("chart-1"));

    registry.remove_client(ClientId::new(1));

    assert_eq!(registry.chart_interest_count(&source), 1);
    assert!(
        registry
            .downstream_chart_id(ClientId::new(1), &source)
            .is_none()
    );
    assert_eq!(
        registry.downstream_chart_id(ClientId::new(2), &source),
        Some("chart-1")
    );
}

#[test]
fn replacing_client_chart_removes_old_source_mapping() {
    let mut registry = InterestRegistry::default();
    let client = ClientId::new(1);
    let old_source = registry.set_chart(client, chart("chart-1"));
    let new_source = registry.set_chart(client, chart_with("chart-1", vec!["DCE.m2609"], 120, 32));

    assert_ne!(old_source, new_source);
    assert_eq!(registry.chart_interest_count(&old_source), 0);
    assert_eq!(registry.chart_interest_count(&new_source), 1);
    assert!(registry.downstream_chart_id(client, &old_source).is_none());
    assert_eq!(
        registry.downstream_chart_id(client, &new_source),
        Some("chart-1")
    );
}

#[test]
fn source_key_symbols_are_sorted_and_deduplicated() {
    let mut registry = InterestRegistry::default();

    let source = registry.set_chart(
        ClientId::new(1),
        chart_with(
            "chart-1",
            vec!["SHFE.au2602", "DCE.m2609", "SHFE.au2602"],
            60_000_000_000,
            64,
        ),
    );

    assert_eq!(
        source.symbols,
        vec!["DCE.m2609".to_string(), "SHFE.au2602".to_string()]
    );
}

#[test]
fn quote_symbols_are_tracked_per_client() {
    let mut registry = InterestRegistry::default();
    registry.set_quotes(ClientId::new(1), vec!["SHFE.au2602".to_string()]);
    registry.set_quotes(
        ClientId::new(2),
        vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
    );

    assert_eq!(registry.quote_interest_count("SHFE.au2602"), 2);
    assert_eq!(registry.quote_interest_count("DCE.m2609"), 1);
}

#[test]
fn replacing_client_quotes_removes_old_quote_interests() {
    let mut registry = InterestRegistry::default();
    let client = ClientId::new(1);

    registry.set_quotes(client, vec!["SHFE.au2602".to_string()]);
    registry.set_quotes(client, vec!["DCE.m2609".to_string()]);

    assert_eq!(registry.quote_interest_count("SHFE.au2602"), 0);
    assert_eq!(registry.quote_interest_count("DCE.m2609"), 1);
}
