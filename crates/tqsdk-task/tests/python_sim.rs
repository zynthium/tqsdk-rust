use tqsdk_core::{Quote, TradeDirection, TradeOffset};
use tqsdk_task::{TqSim, TqSimOrderRequest};

#[test]
fn tqsim_fills_crossing_limit_order_at_order_price_without_partial_fill() {
    let mut sim = TqSim::new()
        .with_margin("SHFE.rb2501", 1_000.0)
        .with_commission("SHFE.rb2501", 2.5);
    sim.update_quote(
        "SHFE.rb2501",
        Quote {
            datetime: "2026-05-15 09:30:00.000000".to_string(),
            last_price: 100.0,
            ask_price1: 100.0,
            ask_volume1: 10,
            bid_price1: 99.0,
            bid_volume1: 8,
            ..Quote::default()
        },
    );

    let report = sim
        .insert_order(TqSimOrderRequest::limit(
            "order-1",
            "SHFE.rb2501",
            TradeDirection::Buy,
            TradeOffset::Open,
            2,
            101.0,
        ))
        .expect("crossing order should be accepted");

    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.orders()[0].volume_origin, 2);
    assert_eq!(report.orders()[0].volume_left, 0);
    assert_eq!(report.trades()[0].price, 101.0);
    assert_eq!(report.trades()[0].volume, 2);

    let account = sim.account();
    assert_eq!(account.user_id, "TQSIM");
    assert_eq!(account.balance, 9_999_995.0);
    assert_eq!(account.margin, 2_000.0);
    assert_eq!(account.commission, 5.0);
    assert_eq!(account.available, 9_997_995.0);

    let position = sim.position("SHFE.rb2501");
    assert_eq!(position.pos_long, 2);
    assert_eq!(position.pos, 2);
    assert_eq!(position.margin, 2_000.0);
}

#[test]
fn tqsim_keeps_non_crossing_limit_order_alive_until_quote_crosses() {
    let mut sim = TqSim::new();
    sim.update_quote(
        "SHFE.rb2501",
        Quote {
            ask_price1: 100.0,
            ask_volume1: 10,
            bid_price1: 99.0,
            bid_volume1: 8,
            ..Quote::default()
        },
    );

    let report = sim
        .insert_order(TqSimOrderRequest::limit(
            "order-2",
            "SHFE.rb2501",
            TradeDirection::Buy,
            TradeOffset::Open,
            1,
            99.0,
        ))
        .unwrap();
    assert_eq!(report.orders()[0].status, "ALIVE");
    assert!(report.trades().is_empty());

    let report = sim.update_quote(
        "SHFE.rb2501",
        Quote {
            ask_price1: 98.0,
            ask_volume1: 10,
            bid_price1: 97.0,
            bid_volume1: 8,
            ..Quote::default()
        },
    );
    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.trades()[0].price, 99.0);
    assert_eq!(sim.position("SHFE.rb2501").pos_long, 1);
}

#[test]
fn tqsim_cancels_market_order_without_counterparty_quote() {
    let mut sim = TqSim::new();

    let report = sim
        .insert_order(TqSimOrderRequest::any(
            "order-3",
            "SHFE.rb2501",
            TradeDirection::Buy,
            TradeOffset::Open,
            1,
        ))
        .unwrap();

    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.orders()[0].lifecycle.as_str(), "cancelled");
    assert_eq!(report.orders()[0].volume_left, 1);
    assert!(report.trades().is_empty());
}

#[test]
fn tqsim_rejects_open_order_when_available_funds_are_insufficient() {
    let mut sim = TqSim::with_account("TQSIM", 100.0).with_margin("SHFE.rb2501", 1_000.0);
    sim.update_quote(
        "SHFE.rb2501",
        Quote {
            ask_price1: 100.0,
            ask_volume1: 10,
            bid_price1: 99.0,
            bid_volume1: 8,
            ..Quote::default()
        },
    );

    let report = sim
        .insert_order(TqSimOrderRequest::limit(
            "order-4",
            "SHFE.rb2501",
            TradeDirection::Buy,
            TradeOffset::Open,
            1,
            101.0,
        ))
        .unwrap();

    assert_eq!(report.orders()[0].status, "FINISHED");
    assert_eq!(report.orders()[0].lifecycle.as_str(), "rejected");
    assert_eq!(report.orders()[0].last_msg, "可用资金不足");
    assert!(report.trades().is_empty());
    assert_eq!(sim.position("SHFE.rb2501").pos, 0);
}
