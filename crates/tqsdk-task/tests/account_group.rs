use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput,
};
use tqsdk_session::SessionClient;
use tqsdk_task::{
    AccountFailurePolicy, AccountGroup, MultiAccountOrderOutcome, Ratio, RiskEngine, RiskRejection,
    TaskError, TaskHost,
};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle);
    TaskHost::new(TqApi::new(session))
}

fn transport_payload(request: &OutboundRequest) -> serde_json::Value {
    match request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            serde_json::from_str(text).expect("transport frame should contain valid json payload")
        }
        OutboundRequest::Transport(OutboundFrame::Binary(bytes)) => serde_json::from_slice(bytes)
            .expect("transport frame should contain valid json payload"),
        other => panic!("expected transport request, got {other:?}"),
    }
}

fn seed_account_position_quote(
    host: &TaskHost,
    account_id: &str,
    symbol: &str,
    available: f64,
    net_position: i64,
    last_price: f64,
) {
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: {
                                "datetime": "2026-04-27 09:30:00.000000",
                                "last_price": last_price
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .map(|_| ());

    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "accounts": {
                                    "CNY": {
                                        "user_id": account_id,
                                        "available": available
                                    }
                                },
                                "positions": {
                                    symbol: {
                                        "user_id": account_id,
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "volume_long": net_position.max(0),
                                        "volume_short": (-net_position).max(0),
                                        "pos_long": net_position.max(0),
                                        "pos_short": (-net_position).max(0),
                                        "pos": net_position
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed account/position commit should produce a commit");
}

struct OrderStatusSeed<'a> {
    account_id: &'a str,
    symbol: &'a str,
    order_id: &'a str,
    direction: &'a str,
    offset: &'a str,
    volume_orign: i64,
    volume_left: i64,
    status: &'a str,
}

fn seed_order_status_commit(host: &TaskHost, seed: OrderStatusSeed<'_>) {
    let (exchange_id, instrument_id) = seed
        .symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            seed.account_id: {
                                "orders": {
                                    seed.order_id: {
                                        "seqno": 1,
                                        "user_id": seed.account_id,
                                        "order_id": seed.order_id,
                                        "exchange_order_id": format!("exchange-{}", seed.order_id),
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": seed.direction,
                                        "offset": seed.offset,
                                        "volume_orign": seed.volume_orign,
                                        "volume_left": seed.volume_left,
                                        "limit_price": 1.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "last_msg": "",
                                        "status": seed.status,
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed order status commit should produce a commit");
}

#[test]
fn account_group_allocates_ratio_volume_with_largest_remainder() {
    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(2, 3).unwrap())
        .add("sim-b", Ratio::new(1, 3).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    let plan = group.allocate(5).unwrap();

    let allocations: Vec<_> = plan
        .allocations()
        .iter()
        .map(|allocation| (allocation.account_id(), allocation.volume()))
        .collect();
    assert_eq!(allocations, vec![("sim-a", 3), ("sim-b", 2)]);
}

#[test]
fn account_group_rejects_empty_and_duplicate_accounts() {
    let empty = AccountGroup::builder().build().unwrap_err();
    assert_eq!(
        empty,
        TaskError::InvalidState("account group cannot be empty")
    );

    let duplicate = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .build()
        .unwrap_err();
    assert_eq!(
        duplicate,
        TaskError::InvalidState("duplicate account id in account group")
    );
}

#[test]
fn account_group_rejects_invalid_ratio_and_impossible_minimum() {
    assert_eq!(
        Ratio::new(0, 10).unwrap_err(),
        TaskError::InvalidState("account allocation ratio numerator must be positive")
    );
    assert_eq!(
        Ratio::new(1, 0).unwrap_err(),
        TaskError::InvalidState("account allocation ratio denominator must be positive")
    );

    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    assert_eq!(
        group.allocate(1).unwrap_err(),
        TaskError::InvalidState("total volume cannot satisfy account minimum volume")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_submits_allocated_orders_with_deterministic_ids() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(7, 10).unwrap())
        .add("sim-b", Ratio::new(3, 10).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-au-001")
        .max_unhedged(std::time::Duration::from_secs(2))
        .on_account_failed(AccountFailurePolicy::ReportExposure)
        .buy_open("SHFE.au2602", 10)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();

    assert_eq!(ticket.group_id(), "alloc-au-001");
    assert_eq!(ticket.orders().len(), 2);
    assert_eq!(ticket.orders()[0].account_id(), "sim-a");
    assert_eq!(ticket.orders()[0].client_order_id(), "alloc-au-001:acct:0");
    assert_eq!(ticket.orders()[0].intent().volume, 7);
    assert_eq!(ticket.orders()[1].account_id(), "sim-b");
    assert_eq!(ticket.orders()[1].client_order_id(), "alloc-au-001:acct:1");
    assert_eq!(ticket.orders()[1].intent().volume, 3);

    let dispatches = host.api().handle_for_test().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 2);

    let first = transport_payload(&dispatches[0].request);
    assert_eq!(first["aid"], "insert_order");
    assert_eq!(first["user_id"], "sim-a");
    assert_eq!(first["order_id"], "alloc-au-001:acct:0");
    assert_eq!(first["volume"], 7);

    let second = transport_payload(&dispatches[1].request);
    assert_eq!(second["aid"], "insert_order");
    assert_eq!(second["user_id"], "sim-b");
    assert_eq!(second["order_id"], "alloc-au-001:acct:1");
    assert_eq!(second["volume"], 3);
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_preflights_all_accounts_before_dispatch() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_price_deviation(10.0));
    seed_account_position_quote(&host, "sim-a", "SHFE.au2602", 2_000.0, 0, 480.0);
    seed_account_position_quote(&host, "sim-b", "SHFE.au2602", 2_000.0, 0, 480.0);

    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let err = host
        .multi_account_order(accounts)
        .client_group_id("alloc-risk-001")
        .buy_open("SHFE.au2602", 2)
        .limit(500.5)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::PriceDeviationExceeded {
            symbol: "SHFE.au2602".to_string(),
            limit_price: 500.5,
            reference_price: 480.0,
            max_abs_deviation: 10.0,
        })
    );
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_retry_reuses_existing_account_intents() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    host.multi_account_order(accounts.clone())
        .client_group_id("alloc-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();
    assert_eq!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .len(),
        2
    );

    let retry = host
        .multi_account_order(accounts)
        .client_group_id("alloc-retry-001")
        .sell_open("SHFE.au2602", 4)
        .limit(481.0)
        .send_once()
        .await
        .unwrap();

    assert!(
        retry
            .orders()
            .iter()
            .all(|order| !order.ticket().was_submitted())
    );
    assert!(
        host.api()
            .handle_for_test()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_reports_all_accounts_filled() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-filled-001")
        .buy_open("SHFE.au2602", 4)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(
        &host,
        OrderStatusSeed {
            account_id: "sim-a",
            symbol: "SHFE.au2602",
            order_id: "alloc-filled-001:acct:0",
            direction: "BUY",
            offset: "OPEN",
            volume_orign: 2,
            volume_left: 0,
            status: "FINISHED",
        },
    );
    seed_order_status_commit(
        &host,
        OrderStatusSeed {
            account_id: "sim-b",
            symbol: "SHFE.au2602",
            order_id: "alloc-filled-001:acct:1",
            direction: "BUY",
            offset: "OPEN",
            volume_orign: 2,
            volume_left: 0,
            status: "FINISHED",
        },
    );

    let outcome = ticket.outcome(host.api()).unwrap().unwrap();
    match outcome {
        MultiAccountOrderOutcome::AllFilled { accounts } => {
            assert_eq!(accounts.len(), 2);
            assert!(
                accounts
                    .iter()
                    .all(|account| account.filled_volume == account.requested_volume)
            );
        }
        other => panic!("expected all-filled outcome, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn multi_account_order_reports_mixed_account_outcome() {
    let mut host = seeded_host();
    let accounts = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .build()
        .unwrap();

    let ticket = host
        .multi_account_order(accounts)
        .client_group_id("alloc-mixed-001")
        .buy_open("SHFE.au2602", 4)
        .limit(480.0)
        .send_once()
        .await
        .unwrap();
    host.api().handle_for_test().drain_dispatches().unwrap();

    seed_order_status_commit(
        &host,
        OrderStatusSeed {
            account_id: "sim-a",
            symbol: "SHFE.au2602",
            order_id: "alloc-mixed-001:acct:0",
            direction: "BUY",
            offset: "OPEN",
            volume_orign: 2,
            volume_left: 0,
            status: "FINISHED",
        },
    );
    seed_order_status_commit(
        &host,
        OrderStatusSeed {
            account_id: "sim-b",
            symbol: "SHFE.au2602",
            order_id: "alloc-mixed-001:acct:1",
            direction: "BUY",
            offset: "OPEN",
            volume_orign: 2,
            volume_left: 2,
            status: "FINISHED",
        },
    );

    let outcome = ticket.outcome(host.api()).unwrap().unwrap();
    match outcome {
        MultiAccountOrderOutcome::NeedsAttention {
            filled_accounts,
            unfilled_accounts,
            accounts,
        } => {
            assert_eq!(filled_accounts, vec!["sim-a".to_string()]);
            assert_eq!(unfilled_accounts, vec!["sim-b".to_string()]);
            assert_eq!(accounts.len(), 2);
        }
        other => panic!("expected needs-attention outcome, got {other:?}"),
    }
}
