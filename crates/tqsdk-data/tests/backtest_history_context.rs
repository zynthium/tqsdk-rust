use std::time::Duration;

use tqsdk_data::{BacktestHistoryContextRequest, BacktestHistoryKind};

#[test]
fn context_request_keeps_anchor_and_row_contract() {
    let request = BacktestHistoryContextRequest::new(
        42,
        "KQ.i@DCE.jm",
        BacktestHistoryKind::Kline {
            duration: Duration::from_secs(1),
        },
        1_000,
        899,
        900,
    );

    assert_eq!(request.anchor_ns, 1_000);
    assert_eq!(request.before_rows + 1 + request.after_rows, 1_800);
    assert!(request.anchor_row_id.is_none());
}
