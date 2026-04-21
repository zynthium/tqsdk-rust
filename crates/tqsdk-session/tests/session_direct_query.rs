use serde_json::json;
use tqsdk_core::{AdapterRegistry, RuntimeHandle};
use tqsdk_session::{SessionClient, SessionFacadeConfig};

fn runtime_handle_with_default_adapters() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

#[test]
fn test_only_session_client_keeps_handle_and_reader_aligned() {
    let handle = RuntimeHandle::new();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());

    assert_eq!(
        client.reader().head_revision(),
        client.handle().reader().head_revision()
    );
}

#[test]
fn graphql_fetch_submits_query_command() {
    let handle = runtime_handle_with_default_adapters();
    let shared_handle = handle.clone();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let command_id = runtime
        .block_on(client.query_graphql("query Ping { ping }", Some(json!({ "x": 1 }))))
        .unwrap();

    assert!(command_id.get() > 0);
    assert_eq!(shared_handle.drain_dispatches().unwrap().len(), 1);
}
