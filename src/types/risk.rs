use serde::{Deserialize, Serialize};

use super::helpers::{default_nan, deserialize_f64_or_nan};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfTradeRule {
    pub count_limit: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FrequentCancellationRule {
    pub insert_order_count_limit: i64,
    pub cancel_order_count_limit: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub cancel_order_percent_limit: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TradePositionRatioRule {
    pub trade_units_limit: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub trade_position_ratio_limit: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskManagementRule {
    pub user_id: String,
    pub exchange_id: String,
    pub enable: bool,
    pub self_trade: SelfTradeRule,
    pub frequent_cancellation: FrequentCancellationRule,
    pub trade_position_ratio: TradePositionRatioRule,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfTrade {
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub highest_buy_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lowest_sell_price: f64,
    pub self_trade_count: i64,
    pub rejected_count: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FrequentCancellation {
    pub insert_order_count: i64,
    pub cancel_order_count: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub cancel_order_percent: f64,
    pub rejected_count: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TradePositionRatio {
    pub trade_units: i64,
    pub net_position_units: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub trade_position_ratio: f64,
    pub rejected_count: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskManagementData {
    pub user_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub self_trade: SelfTrade,
    pub frequent_cancellation: FrequentCancellation,
    pub trade_position_ratio: TradePositionRatio,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}
