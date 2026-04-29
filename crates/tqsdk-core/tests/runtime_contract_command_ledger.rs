use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_core::{
    AccountId, AdapterRegistry, ChangeHit, CommandStatus, CommitScope, ContractError,
    FieldMutation, InputPayload, IoEvent, MutationSource, NormalizedMutation, ObjectKey, OrderId,
    OutboundDispatch, OutboundFrame, OutboundRequest, ProtocolAdapter, ProtocolDomain, Runtime,
    RuntimeCommand, RuntimeHandle, RuntimeInput, StatePath, Symbol, TradeCommand, TradeDirection,
    TradeInsertOrderCommand, TradeOffset, TradePriceType, TradeTimeCondition, TradeVolumeCondition,
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
        handle.drain_dispatches().unwrap(),
        vec![OutboundDispatch {
            command_id,
            domain: ProtocolDomain::Trade,
            account_id: Some(AccountId::new("simnow")),
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
        Some(&json!({
            "aid": "insert_order",
            "account_id": "simnow",
            "order_id": "order-1",
            "symbol": "SHFE.au2602",
            "reason": "insufficient_margin",
        }))
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

#[test]
fn command_statuses_accept_forward_lifecycle_path() {
    let handle = runtime_with_default_adapters();
    let command_id = submit_trade_command(&handle, "order-forward");

    for status in [
        CommandStatus::Sent,
        CommandStatus::Acked,
        CommandStatus::Completed,
    ] {
        handle
            .record_command_status(command_id, status, None, CommitScope::RealtimeUpdate)
            .unwrap()
            .expect("forward status update should publish a commit");
    }

    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
}

#[test]
fn command_statuses_reject_terminal_rollback() {
    let handle = runtime_with_default_adapters();
    let command_id = submit_trade_command(&handle, "order-terminal-rollback");

    handle
        .record_command_status(
            command_id,
            CommandStatus::Sent,
            Some(json!({"detail": "sent"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("sent status should publish a commit");

    handle
        .record_command_status(
            command_id,
            CommandStatus::Completed,
            Some(json!({"detail": "terminal"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("terminal status should publish a commit");

    let err = handle
        .record_command_status(
            command_id,
            CommandStatus::Sent,
            Some(json!({"detail": "rollback"})),
            CommitScope::RealtimeUpdate,
        )
        .expect_err("terminal status rollback should be rejected");

    assert!(
        matches!(err, ContractError::Validation(_)),
        "unexpected error: {err}"
    );
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "detail"]),
        Some(&json!({
            "aid": "insert_order",
            "account_id": "simnow",
            "order_id": "order-terminal-rollback",
            "symbol": "SHFE.au2602",
            "detail": "terminal",
        }))
    );
}

#[test]
fn command_statuses_reject_in_progress_regression() {
    let handle = runtime_with_default_adapters();
    let command_id = submit_trade_command(&handle, "order-in-progress-regression");

    for status in [CommandStatus::Sent, CommandStatus::PartiallyApplied] {
        handle
            .record_command_status(command_id, status, None, CommitScope::RealtimeUpdate)
            .unwrap()
            .expect("forward status update should publish a commit");
    }

    let err = handle
        .record_command_status(
            command_id,
            CommandStatus::Acked,
            None,
            CommitScope::RealtimeUpdate,
        )
        .expect_err("partially applied command should not regress to acked");

    assert!(
        matches!(err, ContractError::Validation(_)),
        "unexpected error: {err}"
    );
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("partially_applied"))
    );
}

#[test]
fn duplicate_terminal_status_is_idempotent_even_with_new_detail() {
    let handle = runtime_with_default_adapters();
    let command_id = submit_trade_command(&handle, "order-idempotent-terminal");

    handle
        .record_command_status(
            command_id,
            CommandStatus::Rejected,
            Some(json!({"reason": "first"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("terminal status should publish a commit");

    let repeated = handle
        .record_command_status(
            command_id,
            CommandStatus::Rejected,
            Some(json!({"reason": "second"})),
            CommitScope::RealtimeUpdate,
        )
        .unwrap();
    assert_eq!(repeated, None);

    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "detail"]),
        Some(&json!({
            "aid": "insert_order",
            "account_id": "simnow",
            "order_id": "order-idempotent-terminal",
            "symbol": "SHFE.au2602",
            "reason": "first",
        }))
    );
}

#[test]
fn mutation_source_domain_guard_rejects_market_write_to_trade_root() {
    let mut registry = AdapterRegistry::new();
    registry.register_adapter(MaliciousMarketAdapter);
    let handle = RuntimeHandle::with_adapters(registry);

    let err = handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "malicious-market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({})),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .expect_err("market mutation must not be allowed to write trade root");

    assert!(
        matches!(err, ContractError::Validation(_)),
        "unexpected error: {err}"
    );
    assert_eq!(handle.latest_snapshot().get(["trade", "simnow"]), None);
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    RuntimeHandle::with_adapters(registry)
}

fn submit_trade_command(handle: &RuntimeHandle, order_id: &str) -> tqsdk_core::CommandId {
    block_on(
        handle.submit(RuntimeCommand::Trade(TradeCommand::InsertOrder(
            TradeInsertOrderCommand {
                account_id: AccountId::new("simnow"),
                order_id: OrderId::new(order_id),
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
    .unwrap()
}

struct MaliciousMarketAdapter;

impl ProtocolAdapter for MaliciousMarketAdapter {
    fn domain(&self) -> ProtocolDomain {
        ProtocolDomain::Market
    }

    fn accepts_command(&self, _cmd: &RuntimeCommand) -> bool {
        false
    }

    fn encode(&mut self, _cmd: &RuntimeCommand) -> tqsdk_core::Result<Vec<OutboundRequest>> {
        Ok(vec![])
    }

    fn accepts_input(&self, input: &RuntimeInput) -> bool {
        matches!(
            input,
            RuntimeInput::Io(IoEvent { route, domains, .. })
                if route == "malicious-market" && domains.contains(&ProtocolDomain::Market)
        )
    }

    fn decode(&mut self, _input: &RuntimeInput) -> tqsdk_core::Result<Vec<NormalizedMutation>> {
        Ok(vec![NormalizedMutation {
            path: StatePath::new(["trade", "simnow"]),
            object: None,
            fields: vec![FieldMutation {
                field: "balance".to_string(),
                value: json!(1),
            }],
            source: MutationSource::MarketDiff,
        }])
    }
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
