#[test]
fn upstream_recv_events_sends_peek_before_json_decode() {
    let source = include_str!("../src/upstream.rs");
    let start = source.find("async fn recv_events").expect("recv_events exists");
    let end = source[start..]
        .find("fn record_decode_report")
        .map(|offset| start + offset)
        .expect("record_decode_report follows recv_events");
    let body = &source[start..end];
    let peek = body
        .find("self.send_peek_message().await?")
        .expect("peek is sent");
    let parse = body
        .find("serde_json::from_str")
        .expect("text JSON parse exists");
    assert!(
        peek < parse,
        "peek_message must be sent before text JSON decode"
    );
}

#[test]
fn decode_upstream_market_report_handles_200_symbols() {
    let mut ticks = serde_json::Map::new();
    for idx in 0..200 {
        let symbol = format!("TEST.s{idx:03}");
        ticks.insert(
            symbol,
            serde_json::json!({
                "data": {
                    "1": {
                        "datetime": 1_780_000_000_000_000_000_i64,
                        "last_price": 10.0 + idx as f64,
                        "volume": idx as i64,
                        "open_interest": 1000 + idx as i64
                    }
                }
            }),
        );
    }
    let frame = serde_json::json!({
        "aid": "rtn_data",
        "data": [{ "ticks": ticks }]
    });

    let report = tqsdk_relay::decode_upstream_market_report(frame).unwrap();
    assert_eq!(report.ticks().len(), 200);
    assert_eq!(report.invalid_rows(), 0);
}
