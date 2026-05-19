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

#[test]
fn commit_touch_set_avoids_tree_collections_on_per_commit_path() {
    let source = include_str!("../src/window.rs");
    let block = function_block(
        source,
        "pub(crate) struct CommitTouchSet",
        "\nstruct ProjectedValueStream",
    );

    for tree_collection in ["BTreeMap", "BTreeSet"] {
        assert!(
            !block.contains(tree_collection),
            "CommitTouchSet should avoid {tree_collection} in the per-commit path"
        );
    }
}

#[test]
fn path_backed_row_streams_do_not_allocate_root_commit_receiver_per_stream() {
    let source = include_str!("../src/api.rs");
    let kline_block = function_block(
        source,
        "pub async fn kline_stream(",
        "\n    pub async fn tick_stream(",
    );
    let tick_block = function_block(
        source,
        "pub async fn tick_stream(",
        "\n    pub fn account_stream(",
    );

    for block in [kline_block, tick_block] {
        assert!(
            !block.contains("self.commit_stream()?.filter_paths"),
            "path-backed row streams should use the shared path dispatcher instead of creating one root broadcast receiver per stream"
        );
    }
}
