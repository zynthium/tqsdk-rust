use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, Revision, Runtime,
    RuntimeHandle, RuntimeInput, RuntimeReader, Symbol,
};

#[test]
fn runtime_reader_exposes_zero_copy_snapshot_reads_and_cursor_access() {
    let handle = runtime_with_default_adapters();
    let reader: RuntimeReader = handle.reader();
    let mut cursor = reader.cursor();

    {
        let snapshot = reader.read();
        assert_eq!(snapshot.revision(), Revision::new(0));
        assert_eq!(snapshot.get(["quotes", "SHFE.au2602"]), None);
    }

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": { "last_price": 512.0 }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();

    {
        let snapshot = reader.read();
        assert_eq!(snapshot.revision(), Revision::new(1));
        assert_eq!(
            snapshot.get(["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(512.0))
        );
    }

    let commit = reader.next(&mut cursor).unwrap();
    assert_eq!(commit.revision, Revision::new(1));
}

#[test]
fn runtime_handle_runtime_trait_surface_uses_reader_not_owned_snapshot() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();
    assert_eq!(reader.head_revision(), None);
    assert_eq!(reader.cursor().next_revision().get(), 1);

    let command_id = block_on(handle.submit(tqsdk_runtime_contract::RuntimeCommand::Market(
        tqsdk_runtime_contract::MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    assert_eq!(command_id.get(), 1);
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
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
    // SAFETY: test-only noop waker with static vtable and null data.
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
