use serde_json::{Value, json};
use tqsdk_core::{
    AdapterRegistry, MarketChartCommand, OutboundFrame, OutboundRequest, ProtocolDomain,
    RuntimeHandle, Symbol,
};
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

#[tokio::test(flavor = "current_thread")]
async fn quote_leases_share_session_interest_until_last_owner_closes() {
    let session = ManualSession::from_runtime(runtime_handle_with_default_adapters());
    let client = session.client();

    let first = client
        .ensure_quotes(["SHFE.au2602", "DCE.m2609"])
        .await
        .unwrap();
    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("subscribe_quote")))
        .expect("first quote lease should subscribe both symbols");
    assert_eq!(body.get("ins_list"), Some(&json!("DCE.m2609,SHFE.au2602")));

    let second = client.ensure_quotes(["SHFE.au2602"]).await.unwrap();
    assert!(
        session.drain_dispatches().unwrap().is_empty(),
        "overlapping quote lease should not resubmit an unchanged subscription"
    );

    second.close().await.unwrap();
    assert!(
        session.drain_dispatches().unwrap().is_empty(),
        "closing an overlapping quote lease must not unsubscribe another owner"
    );

    first.close().await.unwrap();
    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("subscribe_quote")))
        .expect("last quote lease close should refresh subscription");
    assert_eq!(body.get("ins_list"), Some(&json!("")));
}

#[tokio::test(flavor = "current_thread")]
async fn chart_leases_share_session_interest_until_last_owner_closes() {
    let session = ManualSession::from_runtime(runtime_handle_with_default_adapters());
    let client = session.client();
    let chart = MarketChartCommand {
        chart_id: "session-kline-SHFE_au2602-60000000000-64".to_string(),
        symbols: vec![Symbol::new("SHFE.au2602")],
        duration_ns: 60_000_000_000,
        view_width: 64,
        left_kline_id: None,
        focus_datetime_ns: None,
        focus_position: None,
    };

    let first = client.ensure_chart(chart.clone()).await.unwrap();
    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("set_chart")))
        .expect("first chart lease should submit set_chart");
    assert_eq!(body.get("chart_id"), Some(&json!(chart.chart_id)));

    let second = client.ensure_chart(chart.clone()).await.unwrap();
    assert!(
        session.drain_dispatches().unwrap().is_empty(),
        "matching chart lease should not resubmit set_chart"
    );

    second.close().await.unwrap();
    assert!(
        session.drain_dispatches().unwrap().is_empty(),
        "closing an overlapping chart lease must not cancel another owner"
    );

    first.close().await.unwrap();
    let bodies = transport_bodies(&session);
    let body = bodies
        .iter()
        .find(|body| body.get("aid") == Some(&json!("set_chart")))
        .expect("last chart lease close should submit cancel set_chart");
    assert_eq!(body.get("chart_id"), Some(&json!(chart.chart_id)));
    assert_eq!(body.get("ins_list"), Some(&json!("")));
}
