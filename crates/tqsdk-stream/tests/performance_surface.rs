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

#[test]
fn path_commit_stream_precompiles_path_filters_by_root() {
    let source = include_str!("../src/filter.rs");
    let stream_block = function_block(source, "pub struct PathCommitStream {", "\n}");

    assert!(
        stream_block.contains("matcher: PathMatcher"),
        "PathCommitStream should store a precompiled path matcher"
    );
    assert!(
        !stream_block.contains("paths: Vec<StatePath>"),
        "PathCommitStream should not retain raw path filters for per-commit scans"
    );
    assert!(
        source.contains("paths_by_root"),
        "path matcher should narrow candidates by root path segment before prefix checks"
    );
}

#[test]
fn path_dispatcher_reuses_precompiled_path_matchers_per_subscriber() {
    let source = include_str!("../src/path_dispatcher.rs");
    let subscriber_block = function_block(
        source,
        "struct PathSubscriber",
        "\npub(crate) struct PathDispatcher",
    );
    let notify_matching_block = function_block(
        source,
        "fn notify_matching(&mut self",
        "\n    fn notify_all",
    );

    assert!(
        subscriber_block.contains("matcher: PathMatcher"),
        "path dispatcher subscribers should store precompiled path matchers"
    );
    assert!(
        !subscriber_block.contains("paths: Vec<StatePath>"),
        "path dispatcher should not retain raw paths for per-commit scans"
    );
    assert!(
        !notify_matching_block.contains("matches_path_filters"),
        "path dispatcher should not rebuild or rescan raw path filters while dispatching commits"
    );
}

#[test]
fn path_dispatcher_indexes_subscribers_for_commit_delivery() {
    let source = include_str!("../src/path_dispatcher.rs");
    let notify_matching_block = function_block(
        source,
        "fn notify_matching(&mut self",
        "\n    fn notify_all",
    );

    assert!(
        source.contains("subscribers_by_root"),
        "path dispatcher should index broad path subscribers by root segment"
    );
    assert!(
        source.contains("quote_subscribers_by_symbol"),
        "path dispatcher should index quote path subscribers by symbol"
    );
    assert!(
        !notify_matching_block.contains("notify("),
        "commit dispatch should use the path index instead of scanning every subscriber through notify()"
    );
    assert!(
        !notify_matching_block.contains("cleanup_dead()"),
        "commit dispatch should not scan every subscriber for cleanup before each indexed delivery"
    );
}
