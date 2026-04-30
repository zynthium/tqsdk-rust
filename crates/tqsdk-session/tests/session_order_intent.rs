use tqsdk_core::{CommandId, RuntimeHandle, TradeDirection, TradeOffset};
use tqsdk_session::{
    OrderIntentRecord, OrderIntentRegistration, OrderIntentSpec, testing::ManualSession,
};

fn order_intent(client_order_id: &str, volume: i64, limit_price: f64) -> OrderIntentRecord {
    OrderIntentRecord::new(OrderIntentSpec {
        account_id: "sim".to_string(),
        client_order_id: client_order_id.to_string(),
        order_id: client_order_id.to_string(),
        symbol: "SHFE.ao2602".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume,
        limit_price,
    })
}

#[test]
fn session_order_intent_ledger_is_shared_across_client_clones() {
    let manual = ManualSession::from_runtime(RuntimeHandle::new());
    let client = manual.client();
    let clone = client.clone();
    let record = order_intent("strategy-a-open-001", 1, 618.0);

    assert_eq!(
        client.remember_order_intent(record.clone()).unwrap(),
        OrderIntentRegistration::Registered(record.clone())
    );
    client
        .update_order_intent_command("sim", "strategy-a-open-001", CommandId::new(7))
        .unwrap();

    let existing = match clone.remember_order_intent(record).unwrap() {
        OrderIntentRegistration::Existing(record) => record,
        OrderIntentRegistration::Registered(_) => panic!("expected existing order intent"),
    };
    assert_eq!(existing.command_id(), Some(CommandId::new(7)));
    assert_eq!(
        clone
            .order_intent("sim", "strategy-a-open-001")
            .unwrap()
            .unwrap()
            .order_id(),
        "strategy-a-open-001"
    );
}

#[test]
fn session_order_intent_ledger_rejects_mismatched_retry() {
    let manual = ManualSession::from_runtime(RuntimeHandle::new());
    let client = manual.client();
    client
        .remember_order_intent(order_intent("strategy-a-open-001", 1, 618.0))
        .unwrap();

    let error = client
        .remember_order_intent(order_intent("strategy-a-open-001", 2, 619.0))
        .unwrap_err();

    assert_eq!(
        error,
        tqsdk_session::SessionFacadeError::InvalidState(
            "client order intent already registered with different order fields"
        )
    );
}
