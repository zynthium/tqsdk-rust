use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AdapterRegistry, ChangeHit, CommandId, CommitScope, IoEvent, ObjectKey, OutboundEnvelope, OutboundFrame,
    OutboundRequest, ProtocolDomain, Revision, Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput, StatePath,
    Symbol, UpdateCursor,
};

#[test]
fn runtime_handle_routes_commands_into_outbox_without_advancing_revision() {
    let handle = runtime_with_default_adapters();

    let command = RuntimeCommand::Market(tqsdk_runtime_contract::MarketCommand::SubscribeQuotes {
        symbols: vec![Symbol::new("SHFE.au2602")],
    });

    let command_id = block_on(handle.submit(command)).unwrap();
    assert_eq!(command_id.get(), 1);
    assert_eq!(handle.latest_snapshot().revision().get(), 0);
    assert_eq!(handle.commit_log().head_revision(), None);
    assert_eq!(
        handle.drain_outbound(),
        vec![
            OutboundEnvelope {
                command_id,
                request: OutboundRequest::Transport(OutboundFrame::Text(
                    json!({"aid": "subscribe_quote", "ins_list": "SHFE.au2602"}).to_string(),
                )),
            },
            OutboundEnvelope {
                command_id,
                request: OutboundRequest::Transport(OutboundFrame::Text(
                    json!({"aid": "peek_message"}).to_string(),
                )),
            },
        ]
    );
    assert!(handle.drain_outbound().is_empty());
}

#[test]
fn runtime_handle_ingests_inputs_into_committed_snapshot_and_cursored_log() {
    let handle = runtime_with_default_adapters();
    let log = handle.commit_log();
    let submit_id = block_on(handle.submit(RuntimeCommand::System(
        tqsdk_runtime_contract::SystemCommand::RefreshAuth,
    )))
    .unwrap();
    assert_eq!(
        handle.drain_outbound(),
        vec![OutboundEnvelope {
            command_id: submit_id,
            request: OutboundRequest::internal_label("refresh-auth"),
        }]
    );

    let mut pre_commit_cursor = handle.cursor();
    let commit = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market.shared".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: tqsdk_runtime_contract::InputPayload::Json(json!({
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
    assert_eq!(commit.changes.path_hits, vec![StatePath::new(["quotes", "SHFE.au2602"])]);
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
    assert_eq!(snapshot.get(["quotes", "SHFE.au2602", "ask_price1"]), Some(&json!(619.0)));
    assert_eq!(snapshot.get(["quotes", "SHFE.au2602", "last_price"]), Some(&json!(618.5)));
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
                payload: tqsdk_runtime_contract::InputPayload::Text("noop".to_string()),
            }),
            vec![CommandId::new(99)],
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    assert_eq!(ignored, None);
    assert_eq!(handle.latest_snapshot().revision(), Revision::new(1));
    assert_eq!(log.head_revision(), Some(Revision::new(1)));
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
