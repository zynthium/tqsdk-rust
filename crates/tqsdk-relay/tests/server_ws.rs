use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tqsdk_relay::{
    FakeUpstreamTickSource, RelayConfig, RelayEngine, RelayServer, RelayTickRow, UpstreamTick,
    spawn_configured_upstream_pump,
};

#[path = "../../tqsdk-core/tests/support/websocket.rs"]
mod websocket_support;

fn recv_text_json(
    socket: &mut websocket_support::TestWebSocketConnection,
    expected: &str,
) -> serde_json::Value {
    let websocket_support::ClientFrame::Text(text) = socket.recv().unwrap() else {
        panic!("expected upstream {expected} text frame");
    };
    serde_json::from_str(&text).unwrap()
}

fn expect_set_chart(socket: &mut websocket_support::TestWebSocketConnection, ins_list: &str) {
    let set_chart = recv_text_json(socket, "set_chart");
    assert_eq!(set_chart["aid"], "set_chart");
    assert_eq!(set_chart["ins_list"], ins_list);
}

fn expect_peek_message(socket: &mut websocket_support::TestWebSocketConnection) {
    assert_eq!(
        recv_text_json(socket, "peek_message"),
        json!({"aid": "peek_message"})
    );
}

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

#[tokio::test(flavor = "current_thread")]
async fn relay_answers_downstream_ping_and_keeps_client_connected() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        server.serve_once(listener).await.unwrap();
    });

    let mut stream = connect_ws(addr).await;
    send_masked_ping(&mut stream, b"relay").await;
    let pong = read_unmasked_control(&mut stream).await;
    assert_eq!(pong, (0x0a, b"relay".to_vec()));

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

#[tokio::test(flavor = "current_thread")]
async fn relay_accept_loop_accepts_multiple_downstream_clients_until_shutdown() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_task = tokio::spawn(async move {
        server.serve_until(listener, shutdown_rx).await.unwrap();
    });

    let mut first_stream = connect_ws(addr).await;
    send_masked_text(
        &mut first_stream,
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
    )
    .await;

    let mut second_stream = connect_ws(addr).await;
    send_masked_text(
        &mut second_stream,
        json!({"aid": "subscribe_quote", "ins_list": "DCE.m2609"}).to_string(),
    )
    .await;
    wait_for_quote_subscriptions(&engine, 2).await;

    assert_eq!(
        engine
            .lock()
            .unwrap()
            .metrics_snapshot()
            .quote_subscriptions,
        2
    );

    first_stream.shutdown().await.unwrap();
    second_stream.shutdown().await.unwrap();
    wait_for_quote_subscriptions(&engine, 0).await;
    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap();

    assert_eq!(
        engine
            .lock()
            .unwrap()
            .metrics_snapshot()
            .quote_subscriptions,
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn relay_dispatches_ingested_tick_frames_to_connected_downstream_client() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let dispatcher = server.clone();
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
    wait_for_quote_subscriptions(&engine, 1).await;

    let frames = engine
        .lock()
        .unwrap()
        .ingest_tick("SHFE.au2602", tick(1, 1_000, 610.0))
        .unwrap();
    assert_eq!(dispatcher.dispatch_frames(frames).unwrap(), 1);

    let payload: serde_json::Value =
        serde_json::from_str(&read_unmasked_text(&mut stream).await).unwrap();
    assert_eq!(
        payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );

    stream.shutdown().await.unwrap();
    server_task.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn relay_pumps_upstream_tick_frames_to_connected_downstream_client() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let dispatcher = server.clone();
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
    wait_for_quote_subscriptions(&engine, 1).await;

    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push(UpstreamTick {
        symbol: "SHFE.au2602".to_string(),
        row: tick(1, 1_000, 610.0),
    });
    assert_eq!(
        dispatcher.pump_upstream_once(&mut upstream).await.unwrap(),
        1
    );

    let payload: serde_json::Value =
        serde_json::from_str(&read_unmasked_text(&mut stream).await).unwrap();
    assert_eq!(
        payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );
    assert_eq!(
        dispatcher.pump_upstream_once(&mut upstream).await.unwrap(),
        0
    );

    stream.shutdown().await.unwrap();
    server_task.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn relay_configured_websocket_upstream_fans_out_to_downstream_client() {
    use websocket_support::TestWebSocketServer;

    let (send_tick_tx, send_tick_rx) = std::sync::mpsc::channel();
    let upstream = TestWebSocketServer::spawn(move |mut socket| {
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);
        send_tick_rx.recv().unwrap();
        socket
            .send_text(
                json!({
                    "aid": "rtn_data",
                    "data": [
                        {
                            "ticks": {
                                "SHFE.au2602": {
                                    "data": {
                                        "17": {
                                            "datetime": 1_000,
                                            "last_price": 610.0,
                                            "volume": 170,
                                            "open_interest": 1007
                                        }
                                    }
                                }
                            }
                        }
                    ]
                })
                .to_string(),
            )
            .unwrap();
        socket.send_close().unwrap();
    })
    .unwrap();
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let config = RelayConfig {
        upstream_market_url: upstream.url("/market"),
        futures_universe_expression: Some(
            tqsdk_relay::UniverseExpression::parse("symbol:SHFE.au2602").unwrap(),
        ),
        ..RelayConfig::default()
    };
    let _upstream_shutdown = spawn_configured_upstream_pump(&config, server.clone())
        .await
        .unwrap()
        .unwrap();
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
    wait_for_quote_subscriptions(&engine, 1).await;
    send_tick_tx.send(()).unwrap();

    let payload: serde_json::Value =
        serde_json::from_str(&read_unmasked_text(&mut stream).await).unwrap();
    assert_eq!(
        payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );

    stream.shutdown().await.unwrap();
    server_task.await.unwrap();
    upstream.join();
}

#[tokio::test(flavor = "current_thread")]
async fn relay_pump_upstream_until_drains_source_to_downstream_client() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine.clone());
    let dispatcher = server.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_task = tokio::spawn(async move {
        server.serve_once(listener).await.unwrap();
    });

    let mut stream = connect_ws(addr).await;
    send_masked_text(
        &mut stream,
        json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602,DCE.m2609"}).to_string(),
    )
    .await;
    wait_for_quote_subscriptions(&engine, 2).await;

    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push(UpstreamTick {
        symbol: "SHFE.au2602".to_string(),
        row: tick(1, 1_000, 610.0),
    });
    upstream.push(UpstreamTick {
        symbol: "DCE.m2609".to_string(),
        row: tick(2, 2_000, 3300.0),
    });

    assert_eq!(
        dispatcher
            .pump_upstream_until(&mut upstream, shutdown_rx)
            .await
            .unwrap(),
        2
    );
    let first: serde_json::Value =
        serde_json::from_str(&read_unmasked_text(&mut stream).await).unwrap();
    let second: serde_json::Value =
        serde_json::from_str(&read_unmasked_text(&mut stream).await).unwrap();

    assert_eq!(
        first["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );
    assert_eq!(
        second["data"][0]["quotes"]["DCE.m2609"]["last_price"],
        3300.0
    );

    stream.shutdown().await.unwrap();
    server_task.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn relay_pump_upstream_until_stops_when_shutdown_arrives_before_next_tick() {
    let engine = Arc::new(Mutex::new(RelayEngine::new_memory_only(16, 16)));
    let server = RelayServer::new(engine);
    let (_tick_tx, tick_rx) = mpsc::unbounded_channel();
    let mut upstream = ChannelUpstreamTickSource { tick_rx };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown_tx.send(()).unwrap();
    });

    let sent = tokio::time::timeout(
        Duration::from_secs(1),
        server.pump_upstream_until(&mut upstream, shutdown_rx),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(sent, 0);
}

struct ChannelUpstreamTickSource {
    tick_rx: mpsc::UnboundedReceiver<UpstreamTick>,
}

impl tqsdk_relay::UpstreamTickSource for ChannelUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        self.tick_rx.recv().await
    }
}

fn tick(id: i64, datetime: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume: id * 10,
        open_interest: 1000 + id,
    }
}

async fn wait_for_quote_subscriptions(engine: &Arc<Mutex<RelayEngine>>, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if engine
            .lock()
            .unwrap()
            .metrics_snapshot()
            .quote_subscriptions
            == expected
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

async fn send_masked_ping(stream: &mut TcpStream, payload: &[u8]) {
    send_masked_control(stream, 0x9, payload).await;
}

async fn send_masked_control(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
    assert!(
        payload.len() <= 125,
        "control frames must use the short websocket length path"
    );
    let mask = [1_u8, 2, 3, 4];
    let mut frame = Vec::with_capacity(payload.len() + 6);
    frame.push(0x80 | opcode);
    frame.push(0x80 | payload.len() as u8);
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| *byte ^ mask[index % 4]),
    );
    stream.write_all(&frame).await.unwrap();
}

async fn read_unmasked_control(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0_u8; 2];
    stream.read_exact(&mut header).await.unwrap();
    let opcode = header[0] & 0x0f;
    assert!(matches!(opcode, 0x8..=0x0a));
    let len = usize::from(header[1] & 0x7f);
    assert_ne!(header[1] & 0x80, 0x80, "server frames must not be masked");
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    (opcode, payload)
}

async fn read_unmasked_text(stream: &mut TcpStream) -> String {
    let mut header = [0_u8; 2];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0] & 0x0f, 0x1);
    let mut len = u64::from(header[1] & 0x7f);
    if len == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).await.unwrap();
        len = u64::from(u16::from_be_bytes(extended));
    }
    assert_ne!(header[1] & 0x80, 0x80, "server frames must not be masked");
    let mut payload = vec![0_u8; usize::try_from(len).unwrap()];
    stream.read_exact(&mut payload).await.unwrap();
    String::from_utf8(payload).unwrap()
}
