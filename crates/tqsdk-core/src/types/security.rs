use serde::{Deserialize, Serialize};

use crate::order_lifecycle::OrderLifecycle;

use super::helpers::{default_currency, default_nan, deserialize_f64_or_nan};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityAccount {
    pub user_id: String,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub asset: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub asset_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub available: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub available_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub cost: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub drawable: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub deposit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub withdraw: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_frozen_balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_frozen_fee: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_balance_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_fee_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_balance_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_fee_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub hold_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub real_profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_rate_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub dividend_balance_today: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityPosition {
    pub user_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub create_date: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub cost: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub cost_his: f64,
    pub volume: i64,
    pub volume_his: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub last_price: f64,
    pub buy_volume_today: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_balance_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_fee_today: f64,
    pub sell_volume_today: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_balance_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_fee_today: f64,
    pub buy_volume_his: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_balance_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub buy_fee_his: f64,
    pub sell_volume_his: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_balance_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub sell_fee_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub shared_volume_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub devidend_balance_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub market_value_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub float_profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub real_profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub real_profit_his: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_rate_today: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub hold_profit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub real_profit_total: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_total: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub profit_rate_total: f64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityOrder {
    pub user_id: String,
    pub order_id: String,
    pub exchange_order_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub direction: String,
    pub volume_orign: i64,
    pub volume_left: i64,
    pub price_type: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub limit_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub frozen_fee: f64,
    pub insert_date_time: i64,
    pub status: String,
    pub lifecycle: OrderLifecycle,
    pub last_msg: String,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityTrade {
    pub user_id: String,
    pub trade_id: String,
    pub exchange_id: String,
    pub instrument_id: String,
    pub order_id: String,
    pub exchange_order_id: String,
    pub direction: String,
    pub volume: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub balance: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub fee: f64,
    pub trade_date_time: i64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}
