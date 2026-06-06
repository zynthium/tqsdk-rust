use std::sync::{Arc, Mutex};

use serde_json::json;
use tqsdk_relay::{RelayEngine, RelayServer};

#[tokio::test(flavor = "current_thread")]
async fn server_handles_json_market_command_without_starting_real_socket() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);

    let frames = server
        .handle_text(
            1,
            json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
        )
        .await
        .unwrap();

    assert!(frames.is_empty());
    assert_eq!(
        server
            .engine()
            .lock()
            .unwrap()
            .metrics_snapshot()
            .quote_subscriptions,
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn server_rejects_unsupported_non_market_command() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);

    let err = server
        .handle_text(1, json!({"aid": "insert_order"}).to_string())
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "unsupported relay market command: insert_order"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn server_rejects_invalid_json_frame() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);

    let err = server.handle_text(1, "{".to_string()).await.unwrap_err();

    assert!(
        err.to_string()
            .starts_with("invalid relay protocol: invalid JSON frame:")
    );
}
