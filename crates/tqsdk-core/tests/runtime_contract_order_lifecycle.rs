use serde_json::{Value, json};
use tqsdk_core::{
    AccountId, CommitScope, DomainEvent, FieldMutation, InputPayload, IoEvent, MutationSource,
    NormalizedMutation, ObjectKey, Order, OrderId, OrderLifecycle, OutboundRequest,
    ProtocolAdapter, ProtocolDomain, Runtime, RuntimeHandle, RuntimeInput, StatePath, TradeEvent,
    collect_domain_events,
};

mod support;

use support::runtime::TestRuntimeBuilder;

#[derive(Clone)]
struct OrderLifecycleAdapter;

impl ProtocolAdapter for OrderLifecycleAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Trade
    }

    fn accepts_command(&self, _cmd: &tqsdk_core::RuntimeCommand) -> bool {
        false
    }

    fn encode(
        &mut self,
        _cmd: &tqsdk_core::RuntimeCommand,
    ) -> tqsdk_core::Result<Vec<OutboundRequest>> {
        Ok(Vec::new())
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Io(IoEvent { route, .. }) if route == "order-lifecycle"
        )
    }

    fn decode(&mut self, input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        let RuntimeInput::Io(IoEvent {
            payload: InputPayload::Json(payload),
            ..
        }) = input
        else {
            return Ok(Vec::new());
        };

        Ok(vec![order_mutation(payload)])
    }
}

#[test]
fn trade_order_mutations_materialize_typed_lifecycle() {
    let handle = runtime_with_order_lifecycle_adapter();

    let commit = ingest_order(
        &handle,
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "order_id": "order-1",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 2,
            "volume_orign": 2
        }),
    )
    .unwrap()
    .expect("order update should publish a commit");

    let snapshot = handle.latest_snapshot();
    assert_eq!(
        snapshot.get(["trade", "simnow", "orders", "order-1", "lifecycle"]),
        Some(&json!("accepted"))
    );

    let order = snapshot
        .decode::<Order, _, _>(["trade", "simnow", "orders", "order-1"])
        .unwrap()
        .expect("order should decode");
    assert_eq!(order.lifecycle, OrderLifecycle::Accepted);

    let events = collect_domain_events(&commit, snapshot.read()).unwrap();
    let lifecycle = events
        .iter()
        .find_map(|event| match event {
            DomainEvent::Trade(TradeEvent::OrderUpdate { order, .. }) => Some(order.lifecycle),
            _ => None,
        })
        .expect("order event should be emitted");
    assert_eq!(lifecycle, OrderLifecycle::Accepted);
}

#[test]
fn runtime_rejects_order_lifecycle_terminal_regression() {
    let handle = runtime_with_order_lifecycle_adapter();

    ingest_order(
        &handle,
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "is_dead": true,
            "order_id": "order-1",
            "status": "FINISHED",
            "user_id": "simnow",
            "volume_left": 0,
            "volume_orign": 2
        }),
    )
    .unwrap()
    .expect("filled order update should publish a commit");

    let err = ingest_order(
        &handle,
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "is_dead": false,
            "order_id": "order-1",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 2,
            "volume_orign": 2
        }),
    )
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("invalid order lifecycle transition"),
        "unexpected error: {err}"
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["trade", "simnow", "orders", "order-1", "lifecycle"]),
        Some(&json!("filled"))
    );
}

#[test]
fn runtime_accepts_order_lifecycle_forward_progression() {
    let handle = runtime_with_order_lifecycle_adapter();

    for order in [
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "order_id": "order-forward",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 2,
            "volume_orign": 2
        }),
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "order_id": "order-forward",
            "status": "ALIVE",
            "user_id": "simnow",
            "volume_left": 1,
            "volume_orign": 2
        }),
        json!({
            "exchange_id": "SHFE",
            "instrument_id": "au2602",
            "is_dead": true,
            "order_id": "order-forward",
            "status": "FINISHED",
            "user_id": "simnow",
            "volume_left": 0,
            "volume_orign": 2
        }),
    ] {
        ingest_order(&handle, order)
            .unwrap()
            .expect("forward lifecycle update should publish a commit");
    }

    assert_eq!(
        lifecycle_value(&handle, "order-forward"),
        Some(json!("filled"))
    );
}

#[test]
fn runtime_keeps_duplicate_terminal_order_lifecycle_idempotent() {
    let handle = runtime_with_order_lifecycle_adapter();
    let terminal = json!({
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "is_dead": true,
        "order_id": "order-duplicate-terminal",
        "status": "FINISHED",
        "user_id": "simnow",
        "volume_left": 0,
        "volume_orign": 2
    });

    ingest_order(&handle, terminal.clone())
        .unwrap()
        .expect("first terminal lifecycle update should publish a commit");
    assert!(
        ingest_order(&handle, terminal).unwrap().is_none(),
        "duplicate terminal update should not publish a visible change"
    );
    assert_eq!(
        lifecycle_value(&handle, "order-duplicate-terminal"),
        Some(json!("filled"))
    );
}

#[test]
fn runtime_materializes_rejected_failed_and_cancelled_order_lifecycles() {
    let handle = runtime_with_order_lifecycle_adapter();

    for (order_id, status, expected) in [
        ("order-rejected", "REJECTED", "rejected"),
        ("order-failed", "ERROR", "failed"),
        ("order-cancelled", "CANCELLED", "cancelled"),
    ] {
        ingest_order(
            &handle,
            json!({
                "exchange_id": "SHFE",
                "instrument_id": "au2602",
                "is_dead": true,
                "order_id": order_id,
                "status": status,
                "user_id": "simnow",
                "volume_left": 2,
                "volume_orign": 2
            }),
        )
        .unwrap()
        .expect("terminal branch update should publish a commit");

        assert_eq!(lifecycle_value(&handle, order_id), Some(json!(expected)));
    }
}

fn runtime_with_order_lifecycle_adapter() -> RuntimeHandle {
    TestRuntimeBuilder::new()
        .with_adapter(OrderLifecycleAdapter)
        .build()
}

fn ingest_order(
    handle: &RuntimeHandle,
    order: Value,
) -> tqsdk_core::Result<Option<tqsdk_core::CommitResult>> {
    handle.ingest(
        RuntimeInput::Io(IoEvent {
            route: "order-lifecycle".to_string(),
            domains: vec![ProtocolDomain::Trade],
            payload: InputPayload::Json(order),
        }),
        Vec::new(),
        CommitScope::RealtimeUpdate,
    )
}

fn order_mutation(order: &Value) -> NormalizedMutation {
    let fields = order
        .as_object()
        .expect("test order payload must be an object")
        .iter()
        .map(|(field, value)| FieldMutation {
            field: field.clone(),
            value: value.clone(),
        })
        .collect();

    let account_id = order
        .get("user_id")
        .and_then(Value::as_str)
        .unwrap_or("simnow");
    let order_id = order
        .get("order_id")
        .and_then(Value::as_str)
        .unwrap_or("order-1");

    NormalizedMutation {
        path: StatePath::new(["trade", account_id, "orders", order_id]),
        object: Some(ObjectKey::Order {
            account_id: AccountId::new(account_id),
            order_id: OrderId::new(order_id),
        }),
        fields,
        source: MutationSource::TradeReply,
    }
}

fn lifecycle_value(handle: &RuntimeHandle, order_id: &str) -> Option<Value> {
    handle
        .latest_snapshot()
        .get(["trade", "simnow", "orders", order_id, "lifecycle"])
        .cloned()
}
