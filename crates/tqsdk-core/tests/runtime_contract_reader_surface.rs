use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, ContractError, InputPayload, IoEvent, ProtocolDomain, Quote,
    Revision, Runtime, RuntimeHandle, RuntimeInput, RuntimeReader, Symbol,
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
                            "SHFE.au2602": {
                                "instrument_id": "SHFE.au2602",
                                "last_price": 512.0
                            }
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
        let borrowed_quote = snapshot
            .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
            .expect("borrowed-path quote decode should succeed")
            .expect("borrowed-path quote should exist");
        assert_eq!(borrowed_quote.instrument_id, "SHFE.au2602");
        assert_eq!(borrowed_quote.last_price, 512.0);
        let quote = snapshot
            .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
            .expect("quote decode should succeed")
            .expect("quote should exist");
        assert_eq!(quote.instrument_id, "SHFE.au2602");
        assert_eq!(quote.last_price, 512.0);
    }

    let commit = reader.next(&mut cursor).unwrap();
    assert_eq!(commit.revision, Revision::new(1));
}

#[test]
fn runtime_reader_next_view_returns_revision_consistent_zero_copy_guard() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();
    let mut cursor = reader.cursor();

    ingest_quote(&handle, 512.0);

    let guard = reader.next_view(&mut cursor).unwrap().unwrap();
    assert_eq!(guard.commit().revision, Revision::new(1));
    assert_eq!(guard.revision(), Revision::new(1));
    assert_eq!(
        guard.get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(512.0))
    );
    let quote = guard
        .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
        .expect("commit quote decode should succeed")
        .expect("commit quote should exist");
    assert_eq!(quote.last_price, 512.0);
}

#[test]
fn runtime_reader_next_view_reports_lagged_cursor_when_head_has_advanced() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();
    let mut cursor = reader.cursor();

    ingest_quote(&handle, 512.0);
    ingest_quote(&handle, 513.0);

    let lagged = match reader.next_view(&mut cursor) {
        Err(lagged) => lagged,
        Ok(result) => panic!("expected lagged cursor error, got {result:?}"),
    };
    assert_eq!(lagged.expected_revision(), Revision::new(1));
    assert_eq!(lagged.current_revision(), Revision::new(2));
    assert_eq!(lagged.oldest_available_revision(), Revision::new(1));
}

#[test]
fn runtime_ingest_preserves_root_market_scalars_in_snapshot_and_market_guard() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();
    let mut cursor = reader.cursor();
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "ins_list": "SHFE.au2602,DCE.m2609",
                        "mdhis_more_data": false
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("root market scalars should publish a commit");

    let snapshot = handle.latest_snapshot();
    assert_eq!(
        snapshot.get(["ins_list"]),
        Some(&json!("SHFE.au2602,DCE.m2609"))
    );
    assert_eq!(snapshot.get(["mdhis_more_data"]), Some(&json!(false)));
    let market = reader.read_market_state();
    assert_eq!(
        market.get_path(&["ins_list"]),
        Some(&json!("SHFE.au2602,DCE.m2609"))
    );
    assert_eq!(market.get_path(&["mdhis_more_data"]), Some(&json!(false)));

    let commit = reader.next(&mut cursor).unwrap();
    assert!(
        commit
            .changes
            .path_hits
            .contains(&tqsdk_core::StatePath::new(["mdhis_more_data"]))
    );
}

#[test]
fn runtime_ingest_materializes_empty_objects_and_keeps_empty_parent_after_delete() {
    let handle = runtime_with_default_adapters();

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.empty": {}
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("empty object diff should publish a structural commit");
    assert_eq!(
        handle.latest_snapshot().get(["quotes", "SHFE.empty"]),
        Some(&json!({}))
    );

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.delete": {
                                "last_price": 512.0
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed quote should publish a commit");
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.delete": {
                                "last_price": null
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("null field delete should publish a commit");

    assert_eq!(
        handle.latest_snapshot().get(["quotes", "SHFE.delete"]),
        Some(&json!({}))
    );
}

#[test]
fn runtime_handle_runtime_trait_surface_uses_reader_not_owned_snapshot() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();
    assert_eq!(reader.head_revision(), None);
    assert_eq!(reader.cursor().next_revision().get(), 1);

    let command_id = block_on(handle.submit(tqsdk_core::RuntimeCommand::Market(
        tqsdk_core::MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        },
    )))
    .unwrap();
    assert_eq!(command_id.get(), 1);
}

#[test]
fn runtime_reader_decode_distinguishes_missing_paths_from_invalid_payloads() {
    let handle = runtime_with_default_adapters();
    let reader = handle.reader();

    {
        let snapshot = reader.read();
        assert!(
            snapshot
                .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
                .expect("missing paths should not fail")
                .is_none()
        );
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
                            "SHFE.au2602": {
                                "instrument_id": "SHFE.au2602",
                                "trading_time": "invalid"
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();

    let snapshot = reader.read();
    let err = snapshot
        .decode::<Quote, _, _>(["quotes", "SHFE.au2602"])
        .expect_err("invalid payload should surface as validation error");
    assert!(matches!(err, ContractError::Validation(_)));
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

fn ingest_quote(handle: &RuntimeHandle, last_price: f64) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "instrument_id": "SHFE.au2602",
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
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
