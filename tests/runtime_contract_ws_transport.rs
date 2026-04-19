use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tqsdk_runtime_contract::{OutboundFrame, RawFrame, Transport, WebSocketTransport};
use tungstenite::{accept, Message};

#[test]
fn websocket_transport_connects_and_round_trips_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket = accept(stream).unwrap();

        match socket.read().unwrap() {
            Message::Text(text) => assert_eq!(text, "hello"),
            other => panic!("expected text frame, got {other:?}"),
        }

        socket.send(Message::Text("world".into())).unwrap();

        match socket.read().unwrap() {
            Message::Ping(payload) => assert!(payload.is_empty()),
            other => panic!("expected ping frame, got {other:?}"),
        }

        socket.send(Message::Binary(vec![1_u8, 2, 3].into())).unwrap();

        match socket.read().unwrap() {
            Message::Close(_) => {}
            other => panic!("expected close frame, got {other:?}"),
        }
    });

    let mut transport = WebSocketTransport::new(format!("ws://{addr}"));
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
    server.join().unwrap();
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

static NOOP_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(noop_clone, noop, noop, noop);
