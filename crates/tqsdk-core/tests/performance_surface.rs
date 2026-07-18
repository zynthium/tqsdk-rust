fn function_block<'a>(source: &'a str, signature: &str, next_signature: &str) -> &'a str {
    source
        .split(signature)
        .nth(1)
        .and_then(|rest| rest.split(next_signature).next())
        .expect("source block should be present")
}

#[test]
fn change_set_from_mutations_avoids_duplicate_owned_keys_for_deduplication() {
    let source = include_str!("../src/state/changes.rs");
    let block = function_block(source, "pub fn from_mutations(", "\n}\n\n#[derive");

    assert!(
        !block.contains("field_seen.insert(hit.clone())"),
        "field deduplication should not clone a ChangeHit just to test membership"
    );
    assert!(
        !block.contains("object_seen.insert(object.clone())"),
        "object deduplication should use borrowed keys and clone only when emitting output"
    );
}

#[test]
fn domain_event_path_decode_uses_borrowed_state_path_segments() {
    let source = include_str!("../src/domain_event.rs");
    let block = function_block(
        source,
        "fn decode_at_path<T>(",
        "\n}\n\nfn path_object_has_field",
    );

    assert!(
        !block.contains("collect::<Vec<_>>()"),
        "domain event decoding should not allocate a Vec of path segments per event"
    );
}

#[test]
fn generic_object_flattening_uses_push_pop_path_stack() {
    let source = include_str!("../src/adapter/common.rs");
    let block = function_block(
        source,
        "fn flatten_object(",
        "\n}\n\nfn inject_market_data_row_id",
    );

    assert!(
        !block.contains("let mut child_path = path.clone()"),
        "generic object flattening should not clone the full path for every child branch"
    );
    assert!(
        !block.contains("flatten_object(child_path"),
        "recursive flattening should reuse a push/pop path stack"
    );
}

#[test]
fn diff_ingest_bench_does_not_construct_quote_payloads_inside_ingest_timer() {
    let source = include_str!("../examples/diff_ingest_microbench.rs");
    let start = source
        .find("fn run_ingest_case")
        .expect("run_ingest_case must exist");
    let end = source[start..]
        .find("fn run_noop_case")
        .map(|offset| start + offset)
        .expect("run_noop_case must follow run_ingest_case");
    let body = &source[start..end];

    assert!(
        !body.contains("quote_rtn_data(symbols, sequence)"),
        "timed ingest benchmark must consume prebuilt inputs, not build quote payloads"
    );
    assert!(
        !body.contains("market_input(quote_rtn_data"),
        "timed ingest benchmark must not build RuntimeInput payloads inside the timer"
    );
}

#[test]
fn diff_ingest_text_bench_does_not_measure_adapter_ignored_text_payloads() {
    let source = include_str!("../examples/diff_ingest_microbench.rs");

    assert!(
        !source.contains("InputPayload::Text"),
        "wire-text benchmarks should parse prebuilt text into JSON inputs; adapters ignore InputPayload::Text"
    );
}

#[test]
fn applied_change_metadata_does_not_store_owned_field_names() {
    let source = include_str!("../src/state/changes.rs");
    assert!(
        !source.contains("pub(crate) fields: Vec<String>"),
        "AppliedChange should track changed field indexes or borrowed metadata, not clone field names before ChangeSet construction"
    );
}

#[test]
fn state_apply_records_changed_field_names_without_value_clone() {
    let source = include_str!("../src/state/store.rs");
    let apply_fields = function_block(source, "fn apply_fields(", "\n}\n\nfn preserves_null_field");
    assert!(
        !apply_fields.contains("changed_fields.push(field.clone())"),
        "state apply should not clone changed serde_json::Value data for commit metadata"
    );

    let commit_engine = include_str!("../src/runtime/commit_engine.rs");
    assert!(
        commit_engine.contains("ChangeSet::from_applied_changes(&applied, mutations)"),
        "commit metadata should be built from applied-change records, not cloned mutations"
    );
}

#[test]
fn state_store_apply_has_single_root_fast_path_before_btreeset_classification() {
    let source = include_str!("../src/state/store.rs");
    assert!(
        source.contains("apply_single_root"),
        "StateStore::apply_with should have a single-root fast path for common quote batches"
    );
}

#[test]
fn ensure_child_object_looks_up_existing_child_before_cloning_segment() {
    let source = include_str!("../src/state/store.rs");
    assert!(
        !source.contains(".entry(segment.clone())"),
        "existing state path children should be looked up by borrowed segment before allocating a key"
    );
}

#[test]
fn pure_market_mutations_skip_trade_order_lifecycle_scan_by_domain() {
    let source = include_str!("../src/runtime/handle.rs");
    assert!(
        source.contains("domains_are_pure_market"),
        "pure market batches should skip trade order lifecycle scanning before iterating every mutation"
    );
}

#[test]
fn quote_fast_path_uses_unstable_field_sorting() {
    let source = include_str!("../src/adapter/common.rs");
    let block = function_block(
        source,
        "fn decode_quote_object_fast_path(",
        "\n}\n\nfn decode_query_envelope",
    );

    assert!(
        block.contains("sort_unstable_by"),
        "quote fast path fields have unique names and should use unstable sorting"
    );
}

#[test]
fn quote_fast_path_validates_shapes_before_allocating_mutations() {
    let source = include_str!("../src/adapter/common.rs");
    let block = function_block(
        source,
        "fn decode_quote_object_fast_path(",
        "\n}\n\nfn decode_query_envelope",
    );
    let validation_offset = block
        .find("quotes.values().any")
        .expect("quote fast path should prevalidate quote shapes");
    let allocation_offset = block
        .find("Vec::with_capacity(quotes.len())")
        .expect("quote fast path should still preallocate output after validation");

    assert!(
        validation_offset < allocation_offset,
        "quote fast path should validate all shapes before allocating output mutations"
    );
}
