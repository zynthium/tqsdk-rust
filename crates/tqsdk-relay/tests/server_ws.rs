use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tqsdk_relay::{RelayEngine, RelayServer};

#[tokio::test(flavor = "current_thread")]
async fn relay_accepts_websocket_market_command_and_updates_engine() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        server.serve_once(listener).await.unwrap();
    });

    let mut stream = connect_ws(addr).await;
    send_masked_text(
        &mut stream,
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
    )
    .await;
    stream.shutdown().await.unwrap();
    server_task.await.unwrap();

    assert_eq!(
        engine
            .lock()
            .unwrap()
            .metrics_snapshot()
            .quote_subscriptions,
        1
    );
}

async fn connect_ws(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /market HTTP/1.1\r\n\
Host: {addr}\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        response.push(byte[0]);
    }
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 101"));
    stream
}

async fn send_masked_text(stream: &mut TcpStream, text: String) {
    let bytes = text.as_bytes();
    assert!(
        bytes.len() <= 125,
        "test frame keeps the short websocket length path"
    );
    let mask = [1_u8, 2, 3, 4];
    let mut frame = Vec::with_capacity(bytes.len() + 6);
    frame.push(0x81);
    frame.push(0x80 | bytes.len() as u8);
    frame.extend_from_slice(&mask);
    frame.extend(
        bytes
            .iter()
            .enumerate()
            .map(|(index, byte)| *byte ^ mask[index % 4]),
    );
    stream.write_all(&frame).await.unwrap();
}
