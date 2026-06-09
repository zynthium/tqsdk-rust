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
