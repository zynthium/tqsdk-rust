use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    AccountId, AdapterRegistry, ChangeHit, CommandStatus, CommitScope, ObjectKey, OrderId,
    OutboundEnvelope, OutboundFrame, OutboundRequest, Runtime, RuntimeCommand, RuntimeHandle,
    StatePath, Symbol, TradeCommand, TradeDirection, TradeInsertOrderCommand, TradeOffset,
    TradePriceType, TradeTimeCondition, TradeVolumeCondition,
};

#[test]
fn rejected_trade_commands_enter_runtime_command_snapshot_and_commit_log() {
    let handle = runtime_with_default_adapters();

    let command_id = block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new("order-1"),
                symbol: Symbol::new("SHFE.au2602"),
                direction: TradeDirection::Buy,
                offset: Some(TradeOffset::Open),
                volume: 2,
                price_type: TradePriceType::Limit,
                limit_price: Some(json!(618.5)),
                time_condition: TradeTimeCondition::Gfd,
                volume_condition: TradeVolumeCondition::Any,
            },
        ))),
    )
    .unwrap();

    assert_eq!(
        handle.drain_outbound(),
        vec![OutboundEnvelope {
            command_id,
            request: OutboundRequest::Transport(OutboundFrame::Text(
                json!({
                    "aid": "insert_order",
                    "user_id": "simnow",
                    "order_id": "order-1",
                    "exchange_id": "SHFE",
                    "instrument_id": "au2602",
                    "direction": "BUY",
                    "offset": "OPEN",
                    "volume": 2,
                    "price_type": "LIMIT",
                    "limit_price": 618.5,
                    "time_condition": "GFD",
                    "volume_condition": "ANY",
                })
                .to_string(),
            )),
        }]
    );

    let commit = handle
        .record_command_status(
            command_id,
            CommandStatus::Rejected,
            Some(json!({"reason": "insufficient_margin"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .unwrap();

    let command_segment = command_id.get().to_string();
    let path = StatePath::new(vec![
        "runtime".to_string(),
        "commands".to_string(),
        command_segment.clone(),
    ]);
    let object = ObjectKey::Command { command_id };
    assert_eq!(commit.revision.get(), 1);
    assert_eq!(commit.caused_by, vec![command_id]);
    assert_eq!(commit.changes.path_hits, vec![path.clone()]);
    assert_eq!(commit.changes.object_hits, vec![object.clone()]);
    assert_eq!(
        commit.changes.field_hits,
        vec![
            ChangeHit::field(path.clone(), object.clone(), "detail"),
            ChangeHit::field(path.clone(), object.clone(), "domain"),
            ChangeHit::field(path.clone(), object.clone(), "status"),
        ]
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("rejected"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "domain"]),
        Some(&json!("trade"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "detail"]),
        Some(&json!({"reason": "insufficient_margin"}))
    );

    let repeated = handle
        .record_command_status(
            command_id,
            CommandStatus::Rejected,
            Some(json!({"reason": "insufficient_margin"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    assert_eq!(repeated, None);
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

static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
