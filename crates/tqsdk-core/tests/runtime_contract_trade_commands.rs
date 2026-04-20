use serde_json::json;
use tqsdk_core::{
    AccountId, ProtocolDomain, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand,
    TradeOffset, TradePreInsertOrderCommand, TradePriceType, TradeTimeCondition,
    TradeVolumeCondition,
};

#[test]
fn trade_command_surface_covers_session_and_control_flows() {
    let login = TradeLoginCommand {
        account_id: AccountId::new("simnow"),
        broker_id: "9999".to_string(),
        password: "secret".to_string(),
        account_type: TradeAccountType::Future,
        front_broker: Some("9999".to_string()),
        front_url: Some("tcp://127.0.0.1:12345".to_string()),
        client_app_id: Some("SHINNY_TQ_1.0".to_string()),
        client_system_info: Some("SYSINFO".to_string()),
    };

    let command = RuntimeCommand::Trade(TradeCommand::Login(login.clone()));
    assert_eq!(command.domain(), ProtocolDomain::Trade);

    match command {
        RuntimeCommand::Trade(TradeCommand::Login(payload)) => {
            assert_eq!(payload.account_id.as_str(), "simnow");
            assert_eq!(payload.broker_id, "9999");
            assert_eq!(payload.account_type.as_str(), "future");
            assert_eq!(payload.front_url.as_deref(), Some("tcp://127.0.0.1:12345"));
        }
        other => panic!("expected trade login command, got {other:?}"),
    }

    assert!(matches!(
        TradeCommand::ConfirmSettlement {
            account_id: AccountId::new("simnow"),
        },
        TradeCommand::ConfirmSettlement { .. }
    ));

    assert!(matches!(
        TradeCommand::QuerySettlementInfo {
            account_id: AccountId::new("simnow"),
            trading_day: 20260419,
        },
        TradeCommand::QuerySettlementInfo { .. }
    ));

    assert!(matches!(
        TradeCommand::PreInsertOrder(TradePreInsertOrderCommand {
            account_id: AccountId::new("simnow"),
            order_id: tqsdk_core::OrderId::new("pre-1"),
            symbol: tqsdk_core::Symbol::new("SHFE.au2602"),
            direction: tqsdk_core::TradeDirection::Buy,
            offset: Some(TradeOffset::Open),
            volume: 1,
            price_type: TradePriceType::Limit,
            limit_price: Some(json!(0.0)),
            time_condition: TradeTimeCondition::Gfd,
            volume_condition: TradeVolumeCondition::Any,
            hedge_flag: "SPECULATION".to_string(),
            contingent_condition: "IMMEDIATELY".to_string(),
        }),
        TradeCommand::PreInsertOrder(..)
    ));

    assert!(matches!(
        TradeCommand::Transfer {
            account_id: AccountId::new("simnow"),
            bank_id: "b001".to_string(),
            bank_password: "bank-pass".to_string(),
            future_account: "future-acc".to_string(),
            future_password: "future-pass".to_string(),
            currency: "CNY".to_string(),
            amount: json!(1500.0),
        },
        TradeCommand::Transfer { .. }
    ));

    assert!(matches!(
        TradeCommand::SetRiskManagementRule {
            account_id: AccountId::new("simnow"),
            rule: json!({"self_trade": false}),
        },
        TradeCommand::SetRiskManagementRule { .. }
    ));
}
