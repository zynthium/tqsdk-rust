use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::helpers::{
    default_nan, default_neg_one, default_true, deserialize_f64_or_nan, deserialize_i64_or_zero,
    deserialize_option_i64_or_none, deserialize_vec_or_default,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CategoryInfo {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TradingTime {
    /// Day-session windows as `[start, end]` time strings.
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub day: Vec<Vec<String>>,
    /// Night-session windows as `[start, end]` time strings. Official metadata may encode
    /// next-day close times with hours above 24, for example `25:00:00` or `26:30:00`.
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub night: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Quote {
    pub datetime: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price1: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume1: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price1: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume1: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price2: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume2: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price2: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume2: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price3: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume3: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price3: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume3: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price4: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume4: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price4: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume4: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price5: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume5: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price5: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume5: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub last_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub highest: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lowest: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub close: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub average: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub volume: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub amount: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_interest: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub settlement: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub upper_limit: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lower_limit: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub pre_open_interest: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_settlement: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_close: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub price_tick: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub price_decs: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub volume_multiple: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_limit: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub max_limit_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub max_market_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub min_limit_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub min_market_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_max_market_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_max_limit_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_min_market_order_volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_min_limit_order_volume: i64,
    pub underlying_symbol: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub strike_price: f64,
    #[serde(default, alias = "class")]
    pub ins_class: String,
    pub instrument_id: String,
    pub instrument_name: String,
    pub exchange_id: String,
    pub expired: bool,
    #[serde(default)]
    pub trading_time: TradingTime,
    #[serde(default, deserialize_with = "deserialize_option_i64_or_none")]
    pub expire_datetime: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub delivery_year: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub delivery_month: i64,
    #[serde(default, deserialize_with = "deserialize_option_i64_or_none")]
    pub last_exercise_datetime: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub exercise_year: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub exercise_month: i64,
    pub option_class: String,
    pub exercise_type: String,
    pub product_id: String,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub iopv: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub public_float_share_quantity: i64,
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub stock_dividend_ratio: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub cash_dividend_ratio: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_option_i64_or_none")]
    pub expire_rest_days: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub categories: Vec<CategoryInfo>,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub position_limit: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub change: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub change_percent: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub pre_volume: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub margin: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub commission: f64,
    pub product_short_name: String,
    pub underlying_product: String,
    pub py: String,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    #[serde(deserialize_with = "deserialize_option_i64_or_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Kline {
    pub id: i64,
    pub datetime: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub high: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub low: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub close: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub volume: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_oi: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub close_oi: i64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Tick {
    pub id: i64,
    pub datetime: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub last_price: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub average: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub highest: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lowest: f64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price1: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume1: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price1: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume1: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price2: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume2: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price2: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume2: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price3: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume3: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price3: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume3: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price4: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume4: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price4: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume4: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price5: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume5: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price5: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume5: i64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub volume: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub amount: f64,
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_interest: i64,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Chart {
    #[serde(default = "default_neg_one")]
    pub left_id: i64,
    #[serde(default = "default_neg_one")]
    pub right_id: i64,
    #[serde(default = "default_true")]
    pub more_data: bool,
    pub ready: bool,
    pub state: HashMap<String, Value>,
    #[serde(default, rename = "_epoch", skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartInfo {
    pub chart_id: String,
    #[serde(default = "default_neg_one")]
    pub left_id: i64,
    #[serde(default = "default_neg_one")]
    pub right_id: i64,
    #[serde(default = "default_true")]
    pub more_data: bool,
    pub ready: bool,
    pub view_width: usize,
}
