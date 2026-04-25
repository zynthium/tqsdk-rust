use serde::Serialize;
use serde_json::Value;

use crate::{ContractError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

    pub(crate) fn into_value(self) -> Result<Value> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{DiffProtocolMessage, DiffSetChartRequest};

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
}
