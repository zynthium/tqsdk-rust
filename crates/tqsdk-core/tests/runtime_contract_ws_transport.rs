use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod support;

use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_core::{
    OutboundFrame,
    transport::{RawFrame, Transport, WebSocketTransport},
};

#[test]
fn websocket_transport_connects_and_round_trips_frames() {
    run_on_tokio(async {
        let server = TestWebSocketServer::spawn(|mut socket| {
            match socket.recv().unwrap() {
                ClientFrame::Text(text) => assert_eq!(text, "hello"),
                other => panic!("expected text frame, got {other:?}"),
            }

            socket.send_text("world").unwrap();

            match socket.recv().unwrap() {
                ClientFrame::Ping => {}
                other => panic!("expected ping frame, got {other:?}"),
            }

            socket.send_binary(vec![1_u8, 2, 3]).unwrap();

            match socket.recv().unwrap() {
                ClientFrame::Close => {}
                other => panic!("expected close frame, got {other:?}"),
            }
        })
        .unwrap();

        let mut transport = WebSocketTransport::new(server.url(""));
        transport.connect().await.unwrap();
        transport
            .send(OutboundFrame::Text("hello".to_string()))
            .await
            .unwrap();

        match transport.recv().await.unwrap() {
            RawFrame::Text(text) => assert_eq!(text, "world"),
            other => panic!("expected text frame, got {other:?}"),
        }

        transport.send(OutboundFrame::Ping).await.unwrap();

        match transport.recv().await.unwrap() {
            RawFrame::Binary(bytes) => assert_eq!(bytes, vec![1_u8, 2, 3]),
            other => panic!("expected binary frame, got {other:?}"),
        }

        transport.close().await.unwrap();
        server.join();
    });
}

#[test]
fn websocket_transport_sends_custom_handshake_headers() {
    run_on_tokio(async {
        let server = TestWebSocketServer::spawn(|mut socket| {
            assert_eq!(
                socket.request().header("authorization"),
                Some("Bearer test-token"),
            );
            assert_eq!(socket.request().header("x-tq-app"), Some("contract-test"));

            match socket.recv().unwrap() {
                ClientFrame::Close => {}
                other => panic!("expected close frame, got {other:?}"),
            }
        })
        .unwrap();

        let mut transport = WebSocketTransport::new(server.url(""))
            .with_header("Authorization", "Bearer test-token")
            .with_header("X-Tq-App", "contract-test");

        transport.connect().await.unwrap();
        transport.close().await.unwrap();
        server.join();
    });
}

#[test]
fn websocket_transport_offers_deflate_without_client_no_context_takeover_request() {
    run_on_tokio(async {
        let server = TestWebSocketServer::spawn(|mut socket| {
            let extensions = socket
                .request()
                .header("sec-websocket-extensions")
                .expect("websocket client should offer permessage-deflate");
            assert!(
                extensions.contains("permessage-deflate"),
                "expected permessage-deflate offer, got {extensions}",
            );
            assert!(
                !extensions.contains("client_no_context_takeover"),
                "client should not request client_no_context_takeover: {extensions}",
            );
            assert!(
                extensions.contains("server_no_context_takeover"),
                "server_no_context_takeover keeps current Tianqin websocket negotiation compatible: {extensions}",
            );

            match socket.recv().unwrap() {
                ClientFrame::Close => {}
                other => panic!("expected close frame, got {other:?}"),
            }
        })
        .unwrap();

        let mut transport = WebSocketTransport::new(server.url(""));
        transport.connect().await.unwrap();
        transport.close().await.unwrap();
        server.join();
    });
}

#[test]
fn websocket_transport_requires_tokio_runtime() {
    let mut transport = WebSocketTransport::new("ws://127.0.0.1:9");
    let err = block_on(transport.connect()).expect_err("transport should require tokio");
    assert_eq!(
        err.to_string(),
        "validation error: websocket transport requires an active Tokio runtime"
    );
}

#[test]
fn websocket_transport_connect_errors_use_transport_error_category() {
    run_on_tokio(async {
        let mut transport = WebSocketTransport::new("ws://127.0.0.1:9");
        let err = transport
            .connect()
            .await
            .expect_err("connecting to a closed port should fail");
        assert!(
            err.to_string()
                .starts_with("transport error: websocket connect failed:"),
            "{err}"
        );
    });
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = Pin::from(Box::new(future));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn noop_waker() -> Waker {
    // SAFETY: the static null-data waker owns no resources and is only used to
    // poll test futures that are expected to complete synchronously.
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

unsafe fn noop_clone(_: *const ()) -> RawWaker {
    noop_raw_waker()
}

unsafe fn noop(_: *const ()) {}

static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

fn run_on_tokio<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
