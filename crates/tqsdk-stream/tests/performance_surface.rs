fn function_block<'a>(source: &'a str, signature: &str, next_signature: &str) -> &'a str {
    source
        .split(signature)
        .nth(1)
        .and_then(|rest| rest.split(next_signature).next())
        .expect("source block should be present")
}

#[test]
fn row_projection_reads_rows_from_data_maps_without_per_row_path_construction() {
    let source = include_str!("../src/window.rs");
    let kline_block = function_block(
        source,
        "fn read_kline_rows_by_id(",
        "\n}\n\nfn read_tick_rows_in_range(",
    );
    let tick_block = function_block(source, "fn read_tick_rows_by_id(", "\n}\n\n#[cfg(test)]");

    for block in [kline_block, tick_block] {
        assert!(
            !block.contains("let id_key = id.to_string();"),
            "row projection should avoid per-row id string allocation inside the read loop"
        );
    }
}

#[test]
fn market_event_collect_events_avoids_tree_sets_and_owned_lookup_keys_per_commit() {
    let source = include_str!("../src/market_event.rs");
    let block = function_block(source, "fn collect_events(", "\n    }\n}\n\nimpl Stream");

    assert!(
        !block.contains("BTreeSet::new()"),
        "mixed market event collection should avoid tree sets in the per-commit path"
    );
    assert!(
        !block.contains("symbol.to_string()"),
        "mixed market event collection should not allocate owned String lookup keys per commit"
    );
}
