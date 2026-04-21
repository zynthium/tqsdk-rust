use serde_json::json;
use tqsdk_core::RuntimeHandle;
use tqsdk_session::{SessionClient, SessionFacadeConfig};

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
    let handle = RuntimeHandle::new();
    let client = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let command_id = runtime
        .block_on(client.query_graphql("query Ping { ping }", Some(json!({ "x": 1 }))))
        .unwrap();

    assert!(command_id.get() > 0);
}
