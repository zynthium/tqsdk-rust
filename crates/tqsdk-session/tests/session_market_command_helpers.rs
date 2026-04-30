use serde_json::{Value, json};
use tqsdk_core::{AdapterRegistry, OutboundFrame, OutboundRequest, ProtocolDomain, RuntimeHandle};
use tqsdk_session::testing::ManualSession;

fn runtime_handle_with_default_adapters() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

fn transport_bodies(session: &ManualSession) -> Vec<Value> {
    session
        .drain_dispatches()
        .unwrap()
        .into_iter()
        .map(|dispatch| {
            assert_eq!(dispatch.domain, ProtocolDomain::Market);
            match dispatch.request {
                OutboundRequest::Transport(OutboundFrame::Text(text)) => {
                    serde_json::from_str(&text).unwrap()
                }
                other => panic!("expected websocket market dispatch, got {other:?}"),
            }
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_quotes_submits_market_command_without_raw_runtime_command() {
    let session = ManualSession::from_runtime(runtime_handle_with_default_adapters());
    let client = session.client();

    let command_id = client
        .subscribe_quotes(["SHFE.au2602", "DCE.m2609"])
        .await
        .unwrap();

    assert!(command_id.get() > 0);
    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("subscribe_quote")))
        .expect("subscribe_quotes should emit a subscribe_quote frame");
    assert_eq!(body.get("aid"), Some(&json!("subscribe_quote")));
    assert_eq!(body.get("ins_list"), Some(&json!("DCE.m2609,SHFE.au2602")));
}

#[tokio::test(flavor = "current_thread")]
async fn unsubscribe_quotes_submits_market_command() {
    let session = ManualSession::from_runtime(runtime_handle_with_default_adapters());
    let client = session.client();

    client
        .subscribe_quotes(["SHFE.au2602", "DCE.m2609"])
        .await
        .unwrap();
    let _ = session.drain_dispatches().unwrap();

    client.unsubscribe_quotes(["SHFE.au2602"]).await.unwrap();

    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("subscribe_quote")))
        .expect("unsubscribe_quotes should emit a refreshed subscribe_quote frame");
    assert_eq!(body.get("aid"), Some(&json!("subscribe_quote")));
    assert_eq!(body.get("ins_list"), Some(&json!("DCE.m2609")));
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_quotes_rejects_empty_symbol_list() {
    let session = ManualSession::from_runtime(runtime_handle_with_default_adapters());
    let client = session.client();

    let err = client
        .subscribe_quotes(std::iter::empty::<&str>())
        .await
        .unwrap_err();

    assert_eq!(
        err.diagnostic().retry_hint,
        tqsdk_core::RetryHint::DoNotRetry
    );
}
