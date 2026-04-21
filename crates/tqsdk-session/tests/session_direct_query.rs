use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, OutboundRequest, ProtocolDomain, Revision, RuntimeHandle, SessionPhase,
};
use tqsdk_session::{SessionClient, SessionFacadeConfig};

fn runtime_handle_with_default_adapters() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

#[test]
fn test_only_session_client_keeps_handle_and_reader_aligned() {
    let handle = RuntimeHandle::new();
    let shared_handle = handle.clone();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());

    shared_handle
        .record_session_phase(SessionPhase::Running, None, vec![])
        .unwrap();

    assert_eq!(client.reader().head_revision(), Some(Revision::new(1)));
    assert_eq!(
        client.reader().head_revision(),
        client.handle().reader().head_revision()
    );
}

#[test]
fn graphql_fetch_submits_query_command() {
    let handle = runtime_handle_with_default_adapters();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let first_query = "query A { x }";
    let second_query = "query B { y }";
    assert_eq!(first_query.len(), second_query.len());

    let first_command_id = runtime
        .block_on(client.query_graphql(first_query, Some(json!({ "x": 1 }))))
        .unwrap();
    let second_command_id = runtime
        .block_on(client.query_graphql(second_query, Some(json!({ "y": 2 }))))
        .unwrap();

    assert!(first_command_id.get() > 0);
    assert!(second_command_id.get() > first_command_id.get());

    let dispatches = client.drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);
    assert_eq!(dispatches[0].domain, ProtocolDomain::Query);
    assert_eq!(dispatches[1].domain, ProtocolDomain::Query);

    let first_body = match &dispatches[0].request {
        OutboundRequest::Query(request) => request.body(),
        other => panic!("expected query dispatch, got {other:?}"),
    };
    let second_body = match &dispatches[1].request {
        OutboundRequest::Query(request) => request.body(),
        other => panic!("expected query dispatch, got {other:?}"),
    };

    assert_eq!(first_body.get("query"), Some(&json!(first_query)));
    assert_eq!(first_body.get("variables"), Some(&json!({ "x": 1 })));
    assert_eq!(second_body.get("query"), Some(&json!(second_query)));
    assert_eq!(second_body.get("variables"), Some(&json!({ "y": 2 })));
    assert_ne!(first_body.get("query_id"), second_body.get("query_id"));
}
