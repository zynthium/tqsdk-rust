use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{ContractError, Result};

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct SensitiveString(String);

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "aid")]
pub(crate) enum DiffProtocolMessage {
    #[serde(rename = "subscribe_quote")]
    SubscribeQuote { ins_list: String },
    #[serde(rename = "subscribe_trading_status")]
    SubscribeTradingStatus { ins_list: String },
    #[serde(rename = "set_chart")]
    SetChart {
        chart_id: String,
        ins_list: String,
        duration: i64,
        view_width: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        left_kline_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        focus_datetime: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        focus_position: Option<usize>,
    },
    #[serde(rename = "peek_message")]
    PeekMessage,
    #[serde(rename = "req_login")]
    ReqLogin {
        bid: String,
        user_name: String,
        password: SensitiveString,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_mac_address: Option<SensitiveString>,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_app_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_system_info: Option<SensitiveString>,
        #[serde(skip_serializing_if = "Option::is_none")]
        broker_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        front: Option<String>,
    },
    #[serde(rename = "confirm_settlement")]
    ConfirmSettlement,
    #[serde(rename = "qry_account_info")]
    QueryAccountInfo { user_id: String },
    #[serde(rename = "qry_account_register")]
    QueryAccountRegister { user_id: String },
    #[serde(rename = "qry_settlement_info")]
    QuerySettlementInfo {
        user_name: String,
        trading_day: String,
    },
    #[serde(rename = "insert_order")]
    InsertOrder {
        user_id: String,
        order_id: String,
        exchange_id: String,
        instrument_id: String,
        direction: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<String>,
        volume: i64,
        price_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit_price: Option<Value>,
        time_condition: String,
        volume_condition: String,
    },
    #[serde(rename = "pre_insert_order")]
    PreInsertOrder {
        user_id: String,
        order_id: String,
        exchange_id: String,
        instrument_id: String,
        direction: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<String>,
        volume: i64,
        price_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit_price: Option<Value>,
        time_condition: String,
        volume_condition: String,
        hedge_flag: String,
        contingent_condition: String,
    },
    #[serde(rename = "cancel_order")]
    CancelOrder { user_id: String, order_id: String },
    #[serde(rename = "req_transfer")]
    ReqTransfer {
        user_id: String,
        bank_id: String,
        bank_password: String,
        future_account: String,
        future_password: String,
        currency: String,
        amount: Value,
    },
    #[serde(rename = "set_risk_management_rule")]
    SetRiskManagementRule {
        user_id: String,
        // Merged manually in `into_value()` so caller-provided rule fields
        // cannot override reserved protocol fields such as `aid` and `user_id`.
        #[serde(skip)]
        rule: Map<String, Value>,
    },
}

impl DiffProtocolMessage {
    pub(crate) fn subscribe_quote(ins_list: impl Into<String>) -> Self {
        Self::SubscribeQuote {
            ins_list: ins_list.into(),
        }
    }

    pub(crate) fn subscribe_trading_status(ins_list: impl Into<String>) -> Self {
        Self::SubscribeTradingStatus {
            ins_list: ins_list.into(),
        }
    }

    pub(crate) fn set_chart(request: DiffSetChartRequest) -> Self {
        Self::SetChart {
            chart_id: request.chart_id,
            ins_list: request.ins_list,
            duration: request.duration,
            view_width: request.view_width,
            left_kline_id: request.left_kline_id,
            focus_datetime: request.focus_datetime,
            focus_position: request.focus_position,
        }
    }

    pub(crate) fn peek_message() -> Self {
        Self::PeekMessage
    }

    pub(crate) fn req_login(request: DiffLoginRequest) -> Self {
        Self::ReqLogin {
            bid: request.bid,
            user_name: request.user_name,
            password: SensitiveString(request.password),
            client_mac_address: request.client_mac_address.map(SensitiveString),
            client_app_id: request.client_app_id,
            client_system_info: request.client_system_info.map(SensitiveString),
            broker_id: request.broker_id,
            front: request.front,
        }
    }

    pub(crate) fn confirm_settlement() -> Self {
        Self::ConfirmSettlement
    }

    pub(crate) fn query_account_info(user_id: impl Into<String>) -> Self {
        Self::QueryAccountInfo {
            user_id: user_id.into(),
        }
    }

    pub(crate) fn query_account_register(user_id: impl Into<String>) -> Self {
        Self::QueryAccountRegister {
            user_id: user_id.into(),
        }
    }

    pub(crate) fn query_settlement_info(
        user_name: impl Into<String>,
        trading_day: impl Into<String>,
    ) -> Self {
        Self::QuerySettlementInfo {
            user_name: user_name.into(),
            trading_day: trading_day.into(),
        }
    }

    pub(crate) fn insert_order(request: DiffOrderRequest) -> Self {
        Self::InsertOrder {
            user_id: request.user_id,
            order_id: request.order_id,
            exchange_id: request.exchange_id,
            instrument_id: request.instrument_id,
            direction: request.direction,
            offset: request.offset,
            volume: request.volume,
            price_type: request.price_type,
            limit_price: request.limit_price,
            time_condition: request.time_condition,
            volume_condition: request.volume_condition,
        }
    }

    pub(crate) fn pre_insert_order(request: DiffPreInsertOrderRequest) -> Self {
        Self::PreInsertOrder {
            user_id: request.order.user_id,
            order_id: request.order.order_id,
            exchange_id: request.order.exchange_id,
            instrument_id: request.order.instrument_id,
            direction: request.order.direction,
            offset: request.order.offset,
            volume: request.order.volume,
            price_type: request.order.price_type,
            limit_price: request.order.limit_price,
            time_condition: request.order.time_condition,
            volume_condition: request.order.volume_condition,
            hedge_flag: request.hedge_flag,
            contingent_condition: request.contingent_condition,
        }
    }

    pub(crate) fn cancel_order(user_id: impl Into<String>, order_id: impl Into<String>) -> Self {
        Self::CancelOrder {
            user_id: user_id.into(),
            order_id: order_id.into(),
        }
    }

    pub(crate) fn req_transfer(request: DiffTransferRequest) -> Self {
        Self::ReqTransfer {
            user_id: request.user_id,
            bank_id: request.bank_id,
            bank_password: request.bank_password,
            future_account: request.future_account,
            future_password: request.future_password,
            currency: request.currency,
            amount: request.amount,
        }
    }

    pub(crate) fn set_risk_management_rule(
        user_id: impl Into<String>,
        rule: Map<String, Value>,
    ) -> Self {
        Self::SetRiskManagementRule {
            user_id: user_id.into(),
            rule,
        }
    }

    pub(crate) fn into_value(self) -> Result<Value> {
        if let Self::SetRiskManagementRule { user_id, mut rule } = self {
            rule.insert("aid".to_string(), json!("set_risk_management_rule"));
            rule.insert("user_id".to_string(), json!(user_id));
            return Ok(Value::Object(rule));
        }

        serde_json::to_value(self).map_err(|error| {
            ContractError::Adapter(format!("failed to encode DIFF protocol message: {error}"))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiffSetChartRequest {
    chart_id: String,
    ins_list: String,
    duration: i64,
    view_width: usize,
    left_kline_id: Option<i64>,
    focus_datetime: Option<i64>,
    focus_position: Option<usize>,
}

impl DiffSetChartRequest {
    pub(crate) fn new(
        chart_id: impl Into<String>,
        ins_list: impl Into<String>,
        duration: i64,
        view_width: usize,
    ) -> Self {
        Self {
            chart_id: chart_id.into(),
            ins_list: ins_list.into(),
            duration,
            view_width,
            left_kline_id: None,
            focus_datetime: None,
            focus_position: None,
        }
    }

    pub(crate) fn with_left_kline_id(mut self, left_kline_id: i64) -> Self {
        self.left_kline_id = Some(left_kline_id);
        self
    }

    pub(crate) fn with_focus(mut self, focus_datetime: i64, focus_position: usize) -> Self {
        self.focus_datetime = Some(focus_datetime);
        self.focus_position = Some(focus_position);
        self
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DiffLoginRequest {
    pub(crate) bid: String,
    pub(crate) user_name: String,
    pub(crate) password: String,
    pub(crate) client_mac_address: Option<String>,
    pub(crate) client_app_id: Option<String>,
    pub(crate) client_system_info: Option<String>,
    pub(crate) broker_id: Option<String>,
    pub(crate) front: Option<String>,
}

impl std::fmt::Debug for DiffLoginRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffLoginRequest")
            .field("bid", &self.bid)
            .field("user_name", &self.user_name)
            .field("password", &"[REDACTED]")
            .field(
                "client_mac_address",
                &self.client_mac_address.as_ref().map(|_| "[REDACTED]"),
            )
            .field("client_app_id", &self.client_app_id)
            .field(
                "client_system_info",
                &self.client_system_info.as_ref().map(|_| "[REDACTED]"),
            )
            .field("broker_id", &self.broker_id)
            .field("front", &self.front)
            .finish()
    }
}

impl DiffLoginRequest {
    pub(crate) fn new(
        bid: impl Into<String>,
        user_name: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            bid: bid.into(),
            user_name: user_name.into(),
            password: password.into(),
            client_mac_address: None,
            client_app_id: None,
            client_system_info: None,
            broker_id: None,
            front: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DiffOrderRequest {
    pub(crate) user_id: String,
    pub(crate) order_id: String,
    pub(crate) exchange_id: String,
    pub(crate) instrument_id: String,
    pub(crate) direction: String,
    pub(crate) offset: Option<String>,
    pub(crate) volume: i64,
    pub(crate) price_type: String,
    pub(crate) limit_price: Option<Value>,
    pub(crate) time_condition: String,
    pub(crate) volume_condition: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DiffPreInsertOrderRequest {
    pub(crate) order: DiffOrderRequest,
    pub(crate) hedge_flag: String,
    pub(crate) contingent_condition: String,
}

#[derive(Clone, PartialEq)]
pub(crate) struct DiffTransferRequest {
    pub(crate) user_id: String,
    pub(crate) bank_id: String,
    pub(crate) bank_password: String,
    pub(crate) future_account: String,
    pub(crate) future_password: String,
    pub(crate) currency: String,
    pub(crate) amount: Value,
}

impl std::fmt::Debug for DiffTransferRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffTransferRequest")
            .field("user_id", &self.user_id)
            .field("bank_id", &self.bank_id)
            .field("bank_password", &"[REDACTED]")
            .field("future_account", &self.future_account)
            .field("future_password", &"[REDACTED]")
            .field("currency", &self.currency)
            .field("amount", &self.amount)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        DiffLoginRequest, DiffOrderRequest, DiffProtocolMessage, DiffSetChartRequest,
        DiffTransferRequest,
    };

    #[test]
    fn market_messages_encode_official_diff_aids() {
        assert_eq!(
            DiffProtocolMessage::subscribe_quote("SHFE.au2602,DCE.m2609")
                .into_value()
                .unwrap(),
            json!({
                "aid": "subscribe_quote",
                "ins_list": "SHFE.au2602,DCE.m2609",
            })
        );
        assert_eq!(
            DiffProtocolMessage::subscribe_trading_status("SHFE.au2602")
                .into_value()
                .unwrap(),
            json!({
                "aid": "subscribe_trading_status",
                "ins_list": "SHFE.au2602",
            })
        );
    }

    #[test]
    fn chart_message_skips_unset_cursor_fields() {
        let message = DiffProtocolMessage::set_chart(
            DiffSetChartRequest::new("chart-1", "SHFE.au2602", 60_000_000_000, 128)
                .with_left_kline_id(42),
        );

        assert_eq!(
            message.into_value().unwrap(),
            json!({
                "aid": "set_chart",
                "chart_id": "chart-1",
                "ins_list": "SHFE.au2602",
                "duration": 60_000_000_000_i64,
                "view_width": 128,
                "left_kline_id": 42,
            })
        );
    }

    #[test]
    fn trade_messages_encode_official_diff_aids() {
        let mut login = DiffLoginRequest::new("9999", "simnow", "secret");
        login.front = Some("tcp://127.0.0.1:12345".to_string());
        assert_eq!(
            DiffProtocolMessage::req_login(login).into_value().unwrap(),
            json!({
                "aid": "req_login",
                "bid": "9999",
                "user_name": "simnow",
                "password": "secret",
                "front": "tcp://127.0.0.1:12345",
            })
        );

        assert_eq!(
            DiffProtocolMessage::cancel_order("simnow", "order-1")
                .into_value()
                .unwrap(),
            json!({
                "aid": "cancel_order",
                "user_id": "simnow",
                "order_id": "order-1",
            })
        );
        assert_eq!(
            DiffProtocolMessage::req_transfer(DiffTransferRequest {
                user_id: "simnow".to_string(),
                bank_id: "b001".to_string(),
                bank_password: "bank-pass".to_string(),
                future_account: "future-acc".to_string(),
                future_password: "future-pass".to_string(),
                currency: "CNY".to_string(),
                amount: json!(1500.0),
            })
            .into_value()
            .unwrap(),
            json!({
                "aid": "req_transfer",
                "user_id": "simnow",
                "bank_id": "b001",
                "bank_password": "bank-pass",
                "future_account": "future-acc",
                "future_password": "future-pass",
                "currency": "CNY",
                "amount": 1500.0,
            })
        );
    }

    #[test]
    fn login_protocol_debug_redacts_secrets_but_serialization_preserves_them() {
        let mut login =
            DiffLoginRequest::new("debug-broker", "debug-account", "debug-password-secret");
        login.client_mac_address = Some("01-23-45-67-89-AB".to_string());
        login.client_system_info = Some("debug-system-info-secret".to_string());

        let request_debug = format!("{login:?}");
        assert!(!request_debug.contains("debug-password-secret"));
        assert!(!request_debug.contains("01-23-45-67-89-AB"));
        assert!(!request_debug.contains("debug-system-info-secret"));

        let message = DiffProtocolMessage::req_login(login);
        let message_debug = format!("{message:?}");
        assert!(!message_debug.contains("debug-password-secret"));
        assert!(!message_debug.contains("01-23-45-67-89-AB"));
        assert!(!message_debug.contains("debug-system-info-secret"));
        assert_eq!(
            message.into_value().unwrap(),
            json!({
                "aid": "req_login",
                "bid": "debug-broker",
                "user_name": "debug-account",
                "password": "debug-password-secret",
                "client_mac_address": "01-23-45-67-89-AB",
                "client_system_info": "debug-system-info-secret",
            })
        );
    }

    #[test]
    fn trade_order_message_skips_absent_optional_fields() {
        let message = DiffProtocolMessage::insert_order(DiffOrderRequest {
            user_id: "simnow".to_string(),
            order_id: "order-1".to_string(),
            exchange_id: "SHFE".to_string(),
            instrument_id: "au2602".to_string(),
            direction: "BUY".to_string(),
            offset: None,
            volume: 2,
            price_type: "ANY".to_string(),
            limit_price: None,
            time_condition: "IOC".to_string(),
            volume_condition: "ANY".to_string(),
        });

        assert_eq!(
            message.into_value().unwrap(),
            json!({
                "aid": "insert_order",
                "user_id": "simnow",
                "order_id": "order-1",
                "exchange_id": "SHFE",
                "instrument_id": "au2602",
                "direction": "BUY",
                "volume": 2,
                "price_type": "ANY",
                "time_condition": "IOC",
                "volume_condition": "ANY",
            })
        );
    }

    #[test]
    fn risk_rule_message_preserves_reserved_protocol_fields() {
        let rule = json!({
            "aid": "malicious",
            "user_id": "wrong",
            "exchange_id": "SSE",
            "enable": true,
        })
        .as_object()
        .unwrap()
        .clone();

        assert_eq!(
            DiffProtocolMessage::set_risk_management_rule("simnow", rule)
                .into_value()
                .unwrap(),
            json!({
                "aid": "set_risk_management_rule",
                "user_id": "simnow",
                "exchange_id": "SSE",
                "enable": true,
            })
        );
    }
}
