fn function_block<'a>(source: &'a str, signature: &str, next_signature: &str) -> &'a str {
    source
        .split(signature)
        .nth(1)
        .and_then(|rest| rest.split(next_signature).next())
        .expect("source block should be present")
}

#[test]
fn live_quote_snapshot_helpers_read_market_partition() {
    let source = include_str!("../src/live_quote.rs");
    let missing_quote_symbols = function_block(
        source,
        "fn missing_quote_symbols(",
        "fn read_ready_quote_snapshots(",
    );
    let read_ready_quote_snapshots = function_block(
        source,
        "fn read_ready_quote_snapshots(",
        "fn contract_error_into_data(",
    );

    for block in [missing_quote_symbols, read_ready_quote_snapshots] {
        assert!(
            block.contains("read_market_state()"),
            "live quote helpers should read market partitions directly"
        );
        assert!(
            !block.contains("reader.read()"),
            "live quote helpers should not materialize full snapshots"
        );
    }
}

#[test]
fn chart_reader_helpers_read_market_partition() {
    let source = include_str!("../src/client/chart_reader.rs");
    let chart_is_ready = function_block(source, "fn chart_is_ready(", "fn chart_state_matches(");
    let read_ready_kline_data_page = function_block(
        source,
        "pub(super) fn read_ready_kline_data_page(",
        "pub(super) fn read_ready_tick_data_page(",
    );
    let read_ready_tick_data_page = function_block(
        source,
        "pub(super) fn read_ready_tick_data_page(",
        "fn contract_error_into_data(",
    );

    for block in [
        chart_is_ready,
        read_ready_kline_data_page,
        read_ready_tick_data_page,
    ] {
        assert!(
            block.contains("read_market_state()"),
            "chart reader helpers should read market partitions directly"
        );
        assert!(
            !block.contains("reader.read()"),
            "chart reader helpers should not materialize full snapshots"
        );
    }
}
