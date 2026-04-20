use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

mod support;

use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_runtime_contract::{OutboundFrame, RawFrame, Transport, WebSocketTransport};

#[test]
fn websocket_transport_connects_and_round_trips_frames() {
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
    block_on(transport.connect()).unwrap();
    block_on(transport.send(OutboundFrame::Text("hello".to_string()))).unwrap();

    match block_on(transport.recv()).unwrap() {
        RawFrame::Text(text) => assert_eq!(text, "world"),
        other => panic!("expected text frame, got {other:?}"),
    }

    block_on(transport.send(OutboundFrame::Ping)).unwrap();

    match block_on(transport.recv()).unwrap() {
        RawFrame::Binary(bytes) => assert_eq!(bytes, vec![1_u8, 2, 3]),
        other => panic!("expected binary frame, got {other:?}"),
    }

    block_on(transport.close()).unwrap();
    server.join();
}

#[test]
fn websocket_transport_sends_custom_handshake_headers() {
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

    block_on(transport.connect()).unwrap();
    block_on(transport.close()).unwrap();
    server.join();
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
