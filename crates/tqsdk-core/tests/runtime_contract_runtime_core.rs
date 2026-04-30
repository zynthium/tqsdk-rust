use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{future::Future, sync::Arc};

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, ChangeHit, CommandId, CommitScope, IoEvent, ObjectKey, OutboundDispatch,
    OutboundFrame, OutboundRequest, ProtocolDomain, Revision, Runtime, RuntimeCommand,
    RuntimeHandle, RuntimeInput, StatePath, Symbol, UpdateCursor,
};

#[test]
fn runtime_handle_routes_commands_into_outbox_without_advancing_revision() {
    let handle = runtime_with_default_adapters();

    let command = RuntimeCommand::Market(tqsdk_core::MarketCommand::SubscribeQuotes {
        symbols: vec![Symbol::new("SHFE.au2602")],
    });

    let command_id = block_on(handle.submit(command)).unwrap();
    assert_eq!(command_id.get(), 1);
    assert_eq!(handle.latest_snapshot().revision().get(), 0);
    assert_eq!(handle.commit_log().head_revision(), None);
    assert_eq!(
        handle.drain_dispatches().unwrap(),
        vec![
            OutboundDispatch {
                command_id,
                domain: ProtocolDomain::Market,
                account_id: None,
                request: OutboundRequest::Transport(OutboundFrame::Text(
                    json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
                )),
            },
            OutboundDispatch {
                command_id,
                domain: ProtocolDomain::Market,
                account_id: None,
                request: OutboundRequest::Transport(OutboundFrame::Text(
                    json!({"aid": "peek_message"}).to_string(),
                )),
            },
        ]
    );
    assert!(handle.drain_dispatches().unwrap().is_empty());
}

#[test]
fn runtime_handle_ingests_inputs_into_committed_snapshot_and_cursored_log() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let submit_id = block_on(handle.submit(RuntimeCommand::System(
        tqsdk_core::SystemCommand::RefreshAuth,
    )))
    .unwrap();
    assert_eq!(
        handle.drain_dispatches().unwrap(),
        vec![OutboundDispatch {
            command_id: submit_id,
            domain: ProtocolDomain::System,
            account_id: None,
            request: OutboundRequest::internal_label("refresh-auth"),
        }]
    );

    let mut pre_commit_cursor = handle.cursor();
    let commit = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: tqsdk_core::InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": 618.5,
                                "ask_price1": 619.0
                            }
                        }
                    }]
                })),
            }),
            vec![submit_id],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();

    assert_eq!(commit.revision, Revision::new(1));
    assert_eq!(commit.caused_by, vec![submit_id]);
    assert_eq!(commit.scope, CommitScope::RealtimeUpdate);
    assert_eq!(
        commit.changes.path_hits,
        vec![StatePath::new(["quotes", "SHFE.au2602"])]
    );
    assert_eq!(
        commit.changes.object_hits,
        vec![ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2602"),
        }]
    );
    assert_eq!(
        commit.changes.field_hits,
        vec![
            ChangeHit::field(
                StatePath::new(["quotes", "SHFE.au2602"]),
                ObjectKey::Quote {
                    symbol: Symbol::new("SHFE.au2602"),
                },
                "ask_price1",
            ),
            ChangeHit::field(
                StatePath::new(["quotes", "SHFE.au2602"]),
                ObjectKey::Quote {
                    symbol: Symbol::new("SHFE.au2602"),
                },
                "last_price",
            ),
        ]
    );

    let snapshot = handle.latest_snapshot();
    assert_eq!(snapshot.revision(), Revision::new(1));
    assert_eq!(
        snapshot.get(["quotes", "SHFE.au2602", "ask_price1"]),
        Some(&json!(619.0))
    );
    assert_eq!(
        snapshot.get(["quotes", "SHFE.au2602", "last_price"]),
        Some(&json!(618.5))
    );
    assert_eq!(log.head_revision(), Some(Revision::new(1)));

    assert_eq!(log.next(&mut pre_commit_cursor), Some(commit.clone()));
    assert_eq!(log.next(&mut pre_commit_cursor), None);

    let mut replay_cursor: UpdateCursor = handle.cursor_from(Revision::new(1));
    assert_eq!(log.next(&mut replay_cursor), Some(commit.clone()));
    assert_eq!(log.next(&mut replay_cursor), None);

    let mut future_only_cursor = handle.cursor();
    assert_eq!(log.next(&mut future_only_cursor), None);

    let ignored = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: tqsdk_core::InputPayload::Text("noop".to_string()),
            }),
            vec![CommandId::new(99)],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    assert_eq!(ignored, None);
    assert_eq!(handle.latest_snapshot().revision(), Revision::new(1));
    assert_eq!(log.head_revision(), Some(Revision::new(1)));
}

#[test]
fn runtime_publishes_and_returns_the_same_shared_commit() {
    let handle = runtime_with_default_adapters();
    let returned = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: tqsdk_core::InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
                                "last_price": 618.5
                            }
                        }
                    }]
                })),
            }),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )
        .expect("ingest should succeed")
        .expect("quote update should publish a commit");

    let mut cursor = handle.cursor_from(returned.revision);
    let logged = handle
        .commit_log()
        .next(&mut cursor)
        .expect("published commit should be retained in the log");

    assert!(Arc::ptr_eq(&returned, &logged));
}

#[test]
fn commit_log_retention_drops_old_commits_when_no_cursor_needs_them() {
    let handle = runtime_with_retention(2);
    let log = handle.commit_log();

    ingest_quote(&handle, 601.0);
    ingest_quote(&handle, 602.0);
    ingest_quote(&handle, 603.0);

    let mut stale_cursor = handle.cursor_from(Revision::new(1));
    let mut current_cursor = handle.cursor_from(Revision::new(2));

    assert_eq!(log.next(&mut stale_cursor), None);
    assert_eq!(
        log.next(&mut current_cursor).unwrap().revision,
        Revision::new(2)
    );
    assert_eq!(
        log.next(&mut current_cursor).unwrap().revision,
        Revision::new(3)
    );
    assert_eq!(log.next(&mut current_cursor), None);
}

#[test]
fn commit_log_retention_preserves_old_commits_while_cursor_is_active() {
    let handle = runtime_with_retention(2);
    let log = handle.commit_log();
    let mut protected_cursor = handle.cursor_from(Revision::new(1));

    ingest_quote(&handle, 611.0);
    ingest_quote(&handle, 612.0);
    ingest_quote(&handle, 613.0);

    assert_eq!(
        log.next(&mut protected_cursor).unwrap().revision,
        Revision::new(1)
    );
    assert_eq!(
        log.next(&mut protected_cursor).unwrap().revision,
        Revision::new(2)
    );
    assert_eq!(
        log.next(&mut protected_cursor).unwrap().revision,
        Revision::new(3)
    );
    assert_eq!(log.next(&mut protected_cursor), None);
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

fn runtime_with_retention(max_commit_log_entries: usize) -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters_and_commit_log_retention(registry, max_commit_log_entries)
}

fn ingest_quote(handle: &RuntimeHandle, last_price: f64) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: tqsdk_core::InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.au2602": {
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
