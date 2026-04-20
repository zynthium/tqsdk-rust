use serde::{Deserialize, Serialize};

use super::helpers::{default_currency, default_nan, deserialize_f64_or_nan};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Account {
    pub user_id: String,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub static_balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub available: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ctp_balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ctp_available: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub close_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_margin: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub margin: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_commission: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub commission: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_premium: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub premium: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub deposit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub withdraw: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub risk_ratio: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Position {
    pub user_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub pos_long_his: i64,
    pub pos_long_today: i64,
    pub pos_short_his: i64,
    pub pos_short_today: i64,
    pub volume_long_today: i64,
    pub volume_long_his: i64,
    pub volume_long: i64,
    pub volume_long_frozen_today: i64,
    pub volume_long_frozen_his: i64,
    pub volume_long_frozen: i64,
    pub volume_short_today: i64,
    pub volume_short_his: i64,
    pub volume_short: i64,
    pub volume_short_frozen_today: i64,
    pub volume_short_frozen_his: i64,
    pub volume_short_frozen: i64,
    pub volume_long_yd: i64,
    pub volume_short_yd: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open_price_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open_price_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open_cost_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open_cost_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_price_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_price_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_cost_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_cost_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub last_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_profit_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_profit_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub position_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub margin_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub margin_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub margin: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value_long: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value_short: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value: f64,
    pub pos: i64,
    pub pos_long: i64,
    pub pos_short: i64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PreInsertOrder {
    pub user_id: String,
    pub order_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub direction: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_margin: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Order {
    pub seqno: i64,
    pub user_id: String,
    pub order_id: String,
    pub exchange_order_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub direction: String,
    pub offset: String,
    pub volume_orign: i64,
    pub volume_left: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub limit_price: f64,
    pub price_type: String,
    pub volume_condition: String,
    pub time_condition: String,
    pub insert_date_time: i64,
    pub last_msg: String,
    pub status: String,
    pub is_dead: Option<bool>,
    pub is_online: Option<bool>,
    pub is_error: Option<bool>,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub trade_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_margin: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_premium: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Trade {
    pub seqno: i64,
    pub user_id: String,
    pub order_id: String,
    pub trade_id: String,
    pub exchange_trade_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub direction: String,
    pub offset: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub price: f64,
    pub volume: i64,
    pub trade_date_time: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub commission: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SettlementInfo {
    pub content: String,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Notification {
    pub code: String,
    pub level: String,
    pub r#type: String,
    pub content: String,
    pub bid: String,
    pub user_id: String,
}
