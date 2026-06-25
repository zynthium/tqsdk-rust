use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;
use tqsdk_core::{
    AdapterRegistry, CommandStatus, CommitScope, InputPayload, IoEvent, OutboundFrame,
    OutboundRequest, ProtocolDomain, Revision, RuntimeHandle, RuntimeInput, TradeDirection,
    TradeOffset,
};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::trading_desk::{TradingDeskOrderState, TradingDeskProfile, TradingLatencyProbe};
use tqsdk_task::{RiskEngine, TaskError, TaskOrderIntent};

fn manual_session() -> (tqsdk_session::SessionClient, RuntimeHandle) {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle.clone()).into_client();
    (session, handle)
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
    handle: &RuntimeHandle,
    available: f64,
    net_position: i64,
    last_price: f64,
) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.rb2601": {
                                "datetime": "2026-05-02 09:30:00.000000",
                                "instrument_id": "SHFE.rb2601",
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
        .expect("seed quote commit should produce a commit");

    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            "sim": {
                                "accounts": {
                                    "CNY": {
                                        "user_id": "sim",
                                        "available": available
                                    }
                                },
                                "positions": {
                                    "SHFE.rb2601": {
                                        "user_id": "sim",
                                        "exchange_id": "SHFE",
                                        "instrument_id": "rb2601",
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
        .expect("seed trade commit should produce a commit");
}

fn seed_live_order(handle: &RuntimeHandle, order_id: &str) {
    handle
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            "sim": {
                                "orders": {
                                    order_id: {
                                        "seqno": 1,
                                        "user_id": "sim",
                                        "order_id": order_id,
                                        "exchange_order_id": "exchange-order-1",
                                        "exchange_id": "SHFE",
                                        "instrument_id": "rb2601",
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "volume_orign": 1,
                                        "volume_left": 1,
                                        "limit_price": 3_678.0,
                                        "price_type": "LIMIT",
                                        "volume_condition": "ANY",
                                        "time_condition": "GFD",
                                        "insert_date_time": 1_713_660_000_000_000_000_i64,
                                        "status": "ALIVE",
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
        .expect("seed live order commit should produce a commit");
}

fn desk_intent(volume: i64) -> TaskOrderIntent {
    TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume,
        limit_price: Some(3_678.0),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn trading_desk_prechecks_submits_and_recovers_existing_ticket() {
    let (session, handle) = manual_session();
    let mut desk = TradingDeskProfile::builder(session.clone())
        .subscribe_quotes(["SHFE.rb2601"])
        .risk_engine(
            RiskEngine::new()
                .max_order_volume(3)
                .max_price_deviation(20.0)
                .max_net_position(5),
        )
        .latency_probe(TradingLatencyProbe::enabled())
        .build()
        .await
        .unwrap();

    assert!(
        handle
            .drain_dispatches()
            .unwrap()
            .iter()
            .any(|dispatch| transport_payload(&dispatch.request)["aid"] == "subscribe_quote")
    );

    seed_account_position_quote(&handle, 100_000.0, 1, 3_660.0);
    let event = desk
        .next_market_event(Some(Instant::now() + Duration::from_millis(1)))
        .await
        .unwrap()
        .expect("seeded market commit should produce a desk event");
    assert_eq!(event.symbols()[0].as_str(), "SHFE.rb2601");
    assert!(event.latency_cycle().is_some());

    let state = desk.read_market_trade_state();
    let prechecked = desk
        .precheck_order(&state, desk_intent(1), "desk-order-1")
        .unwrap();
    assert_eq!(prechecked.risk_report().revision(), state.revision());
    assert_eq!(prechecked.projection().projected_net(), Some(2));
    drop(state);

    assert!(
        session
            .order_intent("sim", "desk-order-1")
            .unwrap()
            .is_some()
    );

    let ticket = desk.submit_prechecked_order(prechecked).await.unwrap();
    assert!(ticket.was_submitted());
    assert_eq!(ticket.client_order_id(), "desk-order-1");
    let order_dispatches = handle.drain_dispatches().unwrap();
    assert_eq!(order_dispatches.len(), 1);
    let payload = transport_payload(&order_dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["exchange_id"], "SHFE");
    assert_eq!(payload["instrument_id"], "rb2601");

    let command_id = ticket
        .command_id()
        .expect("submitted ticket has command id");
    handle
        .record_command_status(
            command_id,
            CommandStatus::Sent,
            None,
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("sent command status should publish");
    let status = ticket.status(&desk).unwrap();
    assert!(matches!(
        status.state(),
        TradingDeskOrderState::CommandPending {
            status: CommandStatus::Sent
        }
    ));

    seed_live_order(&handle, "desk-order-1");
    let status = ticket.status(&desk).unwrap();
    assert!(matches!(status.state(), TradingDeskOrderState::Live));

    let state = desk.read_market_trade_state();
    let duplicate_prechecked = desk
        .precheck_order(&state, desk_intent(1), "desk-order-1")
        .unwrap();
    drop(state);
    let duplicate = desk
        .submit_prechecked_order(duplicate_prechecked)
        .await
        .unwrap();
    assert!(!duplicate.was_submitted());
    assert_eq!(duplicate.command_id(), Some(command_id));
    assert!(handle.drain_dispatches().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn trading_desk_precheck_rejection_does_not_submit_or_register_intent() {
    let (session, handle) = manual_session();
    let desk = TradingDeskProfile::builder(session.clone())
        .risk_engine(RiskEngine::new().max_order_volume(1))
        .build()
        .await
        .unwrap();
    seed_account_position_quote(&handle, 100_000.0, 0, 3_660.0);

    let state = desk.read_market_trade_state();
    let err = desk
        .precheck_order(&state, desk_intent(2), "too-large")
        .unwrap_err();

    assert!(matches!(err, TaskError::RiskRejected(_)));
    assert!(session.order_intent("sim", "too-large").unwrap().is_none());
    assert!(handle.drain_dispatches().unwrap().is_empty());
}

#[test]
fn trading_latency_cycle_reports_only_after_all_markers_are_present() {
    let probe = TradingLatencyProbe::enabled();
    let mut cycle = probe
        .start_cycle(Revision::new(7))
        .expect("enabled probe should create cycles");

    assert!(cycle.report().is_none());
    cycle.mark_decision();
    cycle.mark_risk();
    cycle.mark_submit();
    assert!(cycle.report().is_none());
    cycle.mark_ack();

    let report = cycle.report().expect("all markers should produce a report");
    assert_eq!(report.revision(), Revision::new(7));
    assert!(report.total() >= Duration::ZERO);
}
