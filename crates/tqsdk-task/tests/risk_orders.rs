use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, OutboundFrame, OutboundRequest,
    ProtocolDomain, RuntimeHandle, RuntimeInput, TradeDirection, TradeOffset,
};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::{RiskEngine, RiskRejection, TaskError, TaskHost, TaskKind, TaskOrderIntent};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle).into_client();
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
    available: f64,
    net_position: i64,
    last_price: f64,
) {
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            "SHFE.rb2601": {
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
        .expect("seed quote commit should produce a commit");

    host.api()
        .session()
        .handle()
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
        .expect("seed account/position commit should produce a commit");
}

fn seed_order_commit(host: &TaskHost, account_id: &str, symbol: &str, order_id: &str) {
    let (exchange_id, instrument_id) = symbol
        .split_once('.')
        .expect("test symbol should contain an exchange prefix");
    host.api()
        .session()
        .handle()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "trade".to_string(),
                domains: vec![ProtocolDomain::Trade],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "trade": {
                            account_id: {
                                "orders": {
                                    order_id: {
                                        "seqno": 1,
                                        "user_id": account_id,
                                        "order_id": order_id,
                                        "exchange_order_id": "exchange-order-1",
                                        "exchange_id": exchange_id,
                                        "instrument_id": instrument_id,
                                        "direction": "BUY",
                                        "offset": "OPEN",
                                        "volume_orign": 1,
                                        "volume_left": 1,
                                        "limit_price": 3678.0,
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
        .expect("seed order commit should produce a commit");
}

#[tokio::test(flavor = "current_thread")]
async fn task_order_builder_submits_typed_client_intent_without_json_price() {
    let mut host = seeded_host();

    let ticket = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3678.0)
        .send_once("strategy-entry-001")
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    assert_eq!(ticket.client_order_id(), "strategy-entry-001");

    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
    let payload = transport_payload(&dispatches[0].request);
    assert_eq!(payload["aid"], "insert_order");
    assert_eq!(payload["user_id"], "sim");
    assert_eq!(payload["exchange_id"], "SHFE");
    assert_eq!(payload["instrument_id"], "rb2601");
    assert_eq!(payload["volume"], 2);
    assert_eq!(payload["limit_price"], 3678.0);
}

#[tokio::test(flavor = "current_thread")]
async fn task_order_builder_uses_existing_task_ownership_guard() {
    let mut host = seeded_host();
    let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3678.0)
        .send_once("blocked-entry")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_allows_order_on_current_snapshot() {
    let mut host = seeded_host().with_risk(
        RiskEngine::new()
            .max_order_volume(3)
            .min_available(1_000.0)
            .max_net_position(5)
            .max_price_deviation(20.0),
    );
    seed_account_position_quote(&host, 2_000.0, 4, 3_660.0);

    let ticket = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("risk-accepted")
        .await
        .unwrap();

    assert!(ticket.was_submitted());
    let dispatches = host.api().session().handle().drain_dispatches().unwrap();
    assert_eq!(dispatches.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_rejects_price_outside_quote_band() {
    let mut host = seeded_host().with_risk(
        RiskEngine::new()
            .max_order_volume(3)
            .max_price_deviation(10.0),
    );
    seed_account_position_quote(&host, 2_000.0, 0, 3_660.0);

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_678.0)
        .send_once("risk-rejected")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::PriceDeviationExceeded {
            symbol: "SHFE.rb2601".to_string(),
            limit_price: 3_678.0,
            reference_price: 3_660.0,
            max_abs_deviation: 10.0,
        })
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn risk_engine_rejects_projected_net_position_limit() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_net_position(5));
    seed_account_position_quote(&host, 2_000.0, 5, 3_660.0);

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-position-rejected")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::NetPositionLimitExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            current_net: 5,
            projected_net: 6,
            max_abs_net: 5,
        })
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn risk_engine_report_exposes_revision_bound_decision() {
    let host = seeded_host();
    seed_account_position_quote(&host, 2_000.0, 0, 3_660.0);

    let accepted_intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 1,
        limit_price: Some(3_678.0),
    };
    let accepted_report = RiskEngine::new()
        .max_price_deviation(20.0)
        .check_report(host.api(), &accepted_intent)
        .unwrap();

    assert!(accepted_report.revision().get() > 0);
    assert!(accepted_report.decision().is_accepted());

    let rejected_report = RiskEngine::new()
        .max_price_deviation(10.0)
        .check_report(host.api(), &accepted_intent)
        .unwrap();

    assert_eq!(
        rejected_report.decision().rejection(),
        Some(&RiskRejection::PriceDeviationExceeded {
            symbol: "SHFE.rb2601".to_string(),
            limit_price: 3_678.0,
            reference_price: 3_660.0,
            max_abs_deviation: 10.0,
        })
    );
    assert_eq!(rejected_report.revision(), accepted_report.revision());
}

#[test]
fn risk_engine_tracks_daily_open_count_by_account_and_symbol() {
    let host = seeded_host();
    let mut risk = RiskEngine::new().daily_open_count_limit(1, ["SHFE.rb2601"]);
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 1,
        limit_price: Some(3_660.0),
    };

    assert!(
        risk.check_report(host.api(), &intent)
            .unwrap()
            .decision()
            .is_accepted()
    );
    risk.record_accepted_order(&intent).unwrap();

    let report = risk.check_report(host.api(), &intent).unwrap();
    assert_eq!(
        report.decision().rejection(),
        Some(&RiskRejection::DailyOpenCountLimitExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            current: 1,
            requested: 1,
            max: 1,
        })
    );

    let close_intent = TaskOrderIntent {
        offset: Some(TradeOffset::Close),
        ..intent
    };
    assert!(
        risk.check_report(host.api(), &close_intent)
            .unwrap()
            .decision()
            .is_accepted()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task_host_records_daily_open_volume_after_submitted_order() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().daily_open_volume_limit(2, ["SHFE.rb2601"]));

    let first = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 2)
        .limit(3_660.0)
        .send_once("risk-daily-volume-1")
        .await
        .unwrap();
    assert!(first.was_submitted());
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-daily-volume-2")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::DailyOpenVolumeLimitExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            current: 2,
            requested: 1,
            max: 2,
        })
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn execution_group_checks_daily_open_count_limit_before_partial_submit() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().daily_open_count_limit(1, ["SHFE.rb2601"]));

    let err = host
        .execution_group("sim")
        .client_group_id("risk-group-preflight")
        .leg("SHFE.rb2601")
        .buy_open(1)
        .limit(3_660.0)
        .leg("SHFE.rb2601")
        .buy_open(1)
        .limit(3_660.0)
        .send_once()
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::DailyOpenCountLimitExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            current: 1,
            requested: 1,
            max: 1,
        })
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task_host_rejects_order_rate_limit_for_same_exchange() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().order_rate_limit_per_second(1, ["SHFE"]));

    let first = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-rate-1")
        .await
        .unwrap();
    assert!(first.was_submitted());
    assert_eq!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .len(),
        1
    );

    let err = host
        .orders("sim")
        .buy_open("SHFE.rb2602", 1)
        .limit(3_660.0)
        .send_once("risk-rate-2")
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::OrderRateLimitExceeded {
            account_id: "sim".to_string(),
            exchange_id: "SHFE".to_string(),
            current: 1,
            requested: 1,
            max: 1,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_order_guarded_uses_order_rate_limit() {
    let mut host =
        seeded_host().with_risk(RiskEngine::new().order_rate_limit_per_second(1, ["SHFE"]));
    seed_order_commit(&host, "sim", "SHFE.rb2601", "order-1");

    let first = host
        .orders("sim")
        .buy_open("SHFE.rb2601", 1)
        .limit(3_660.0)
        .send_once("risk-rate-before-cancel")
        .await
        .unwrap();
    assert!(first.was_submitted());
    host.api().session().handle().drain_dispatches().unwrap();

    let err = host
        .cancel_order_guarded("sim", "order-1")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::OrderRateLimitExceeded {
            account_id: "sim".to_string(),
            exchange_id: "SHFE".to_string(),
            current: 1,
            requested: 1,
            max: 1,
        })
    );
}

#[test]
fn risk_engine_tracks_accumulated_open_volume_across_symbol_group() {
    let host = seeded_host();
    let mut risk =
        RiskEngine::new().accumulated_open_volume_limit(3, ["SHFE.rb2601", "SHFE.rb2602"]);
    let first = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 2,
        limit_price: Some(3_660.0),
    };
    let second = TaskOrderIntent {
        symbol: "SHFE.rb2602".to_string(),
        volume: 2,
        ..first.clone()
    };

    risk.record_accepted_order(&first).unwrap();

    let report = risk.check_report(host.api(), &second).unwrap();
    assert_eq!(
        report.decision().rejection(),
        Some(&RiskRejection::AccumulatedOpenVolumeLimitExceeded {
            account_id: "sim".to_string(),
            symbols: vec!["SHFE.rb2601".to_string(), "SHFE.rb2602".to_string()],
            current: 2,
            requested: 2,
            max: 3,
        })
    );
}

#[test]
fn risk_engine_project_order_exposes_revision_bound_position_projection() {
    let host = seeded_host();
    seed_account_position_quote(&host, 100_000.0, 1, 3_660.0);
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 3,
        limit_price: Some(3_678.0),
    };

    let report = RiskEngine::new()
        .project_order(host.api(), &intent)
        .expect("projection should read one runtime snapshot");

    assert_eq!(report.account_id(), "sim");
    assert_eq!(report.symbol(), "SHFE.rb2601");
    assert_eq!(report.current_net(), Some(1));
    assert_eq!(report.projected_net(), Some(4));
    assert_eq!(report.price_basis(), Some(3_678.0));
    assert_eq!(report.estimated_price_volume(), Some(11_034.0));
    assert_eq!(
        report.revision(),
        host.api().session().reader().read().revision()
    );
}

#[test]
fn risk_engine_rejects_limit_price_not_aligned_to_instrument_tick() {
    let host = seeded_host();
    seed_account_position_quote(&host, 100_000.0, 0, 3_660.0);
    let spec = tqsdk_session::InstrumentSpec {
        symbol: tqsdk_core::Symbol::new("SHFE.rb2601"),
        exchange_id: "SHFE".to_string(),
        product_id: "rb".to_string(),
        class: tqsdk_session::InstrumentClass::Future,
        price_tick: 0.2,
        volume_multiple: 10,
        expire_datetime_ns: None,
        underlying_symbol: None,
    };
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 1,
        limit_price: Some(3_678.1),
    };

    let report = RiskEngine::new()
        .instrument_specs([spec])
        .check_report(host.api(), &intent)
        .unwrap();

    assert!(matches!(
        report.decision().rejection(),
        Some(RiskRejection::PriceNotOnTick {
            symbol,
            limit_price,
            price_tick
        }) if symbol == "SHFE.rb2601" && *limit_price == 3_678.1 && *price_tick == 0.2
    ));
}

#[test]
fn risk_projection_uses_instrument_volume_multiple_for_notional() {
    let host = seeded_host();
    seed_account_position_quote(&host, 100_000.0, 2, 3_660.0);
    let spec = tqsdk_session::InstrumentSpec {
        symbol: tqsdk_core::Symbol::new("SHFE.rb2601"),
        exchange_id: "SHFE".to_string(),
        product_id: "rb".to_string(),
        class: tqsdk_session::InstrumentClass::Future,
        price_tick: 1.0,
        volume_multiple: 10,
        expire_datetime_ns: None,
        underlying_symbol: None,
    };
    let intent = TaskOrderIntent {
        account_id: "sim".to_string(),
        symbol: "SHFE.rb2601".to_string(),
        direction: TradeDirection::Buy,
        offset: Some(TradeOffset::Open),
        volume: 3,
        limit_price: Some(3_678.0),
    };

    let projection = RiskEngine::new()
        .instrument_specs([spec])
        .project_order(host.api(), &intent)
        .unwrap();

    assert_eq!(projection.contract_multiplier(), Some(10));
    assert_eq!(projection.estimated_notional(), Some(3_678.0 * 3.0 * 10.0));
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_guarded_insert_uses_configured_risk_engine() {
    let mut host = seeded_host().with_risk(RiskEngine::new().max_order_volume(1));
    seed_account_position_quote(&host, 2_000.0, 0, 3_660.0);

    let err = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            2,
            Some(json!(3_660.0)),
        )
        .await
        .unwrap_err();

    assert_eq!(
        err,
        TaskError::RiskRejected(RiskRejection::MaxOrderVolumeExceeded {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            requested: 2,
            max: 1,
        })
    );
    assert!(
        host.api()
            .session()
            .handle()
            .drain_dispatches()
            .unwrap()
            .is_empty()
    );
}
