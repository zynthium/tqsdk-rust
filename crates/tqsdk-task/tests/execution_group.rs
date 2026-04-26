use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{HedgePolicy, TaskError, TaskHost};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn execution_group_submits_two_typed_legs_under_one_group_id() {
    let mut host = seeded_host();

    let group = host
        .execution_group("sim")
        .client_group_id("spread-entry-001")
        .max_unhedged(Duration::from_secs(2))
        .on_leg_failed(HedgePolicy::ReportExposure)
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(group.group_id(), "spread-entry-001");
    assert_eq!(group.legs().len(), 2);
    assert_eq!(group.legs()[0].client_order_id(), "spread-entry-001:leg:0");
    assert_eq!(group.legs()[1].client_order_id(), "spread-entry-001:leg:1");
    assert!(group.legs()[0].ticket().was_submitted());
    assert!(group.legs()[1].ticket().was_submitted());

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);

    let leg0 = transport_payload(&dispatches[0].request);
    assert_eq!(leg0["aid"], "insert_order");
    assert_eq!(leg0["user_id"], "sim");
    assert_eq!(leg0["order_id"], "spread-entry-001:leg:0");
    assert_eq!(leg0["exchange_id"], "SHFE");
    assert_eq!(leg0["instrument_id"], "au2602");
    assert_eq!(leg0["direction"], "BUY");
    assert_eq!(leg0["offset"], "OPEN");
    assert_eq!(leg0["volume"], 1);
    assert_eq!(leg0["limit_price"], 480.0);

    let leg1 = transport_payload(&dispatches[1].request);
    assert_eq!(leg1["aid"], "insert_order");
    assert_eq!(leg1["user_id"], "sim");
    assert_eq!(leg1["order_id"], "spread-entry-001:leg:1");
    assert_eq!(leg1["exchange_id"], "SHFE");
    assert_eq!(leg1["instrument_id"], "ag2602");
    assert_eq!(leg1["direction"], "SELL");
    assert_eq!(leg1["offset"], "OPEN");
    assert_eq!(leg1["volume"], 15);
    assert_eq!(leg1["limit_price"], 6500.0);
}

#[tokio::test(flavor = "current_thread")]
async fn execution_group_rejects_missing_group_id_before_dispatch() {
    let mut host = seeded_host();

    let err = host
        .execution_group("sim")
        .leg("SHFE.au2602")
        .buy_open(1)
        .limit(480.0)
        .leg("SHFE.ag2602")
        .sell_open(15)
        .limit(6500.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::InvalidState("execution group id is required")
    );
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
