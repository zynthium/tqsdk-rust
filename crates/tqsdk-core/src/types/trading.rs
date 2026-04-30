use serde::{Deserialize, Serialize};

use crate::order_lifecycle::OrderLifecycle;

use super::helpers::{default_currency, default_nan, deserialize_f64_or_nan};

fn schema_f64_eq(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}

macro_rules! eq_non_float_fields {
    ($left:expr, $right:expr, [$($field:ident),* $(,)?]) => {
        true $(&& $left.$field == $right.$field)*
    };
}

macro_rules! eq_float_fields {
    ($left:expr, $right:expr, [$($field:ident),* $(,)?]) => {
        true $(&& schema_f64_eq($left.$field, $right.$field))*
    };
}

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

impl PartialEq for Account {
    fn eq(&self, other: &Self) -> bool {
        eq_non_float_fields!(self, other, [user_id, currency, epoch])
            && eq_float_fields!(
                self,
                other,
                [
                    pre_balance,
                    static_balance,
                    balance,
                    available,
                    ctp_balance,
                    ctp_available,
                    float_profit,
                    position_profit,
                    close_profit,
                    frozen_margin,
                    margin,
                    frozen_commission,
                    commission,
                    frozen_premium,
                    premium,
                    deposit,
                    withdraw,
                    risk_ratio,
                    market_value,
                ]
            )
    }
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

impl PartialEq for Position {
    fn eq(&self, other: &Self) -> bool {
        eq_non_float_fields!(
            self,
            other,
            [
                user_id,
                exchange_id,
                instrument_id,
                pos_long_his,
                pos_long_today,
                pos_short_his,
                pos_short_today,
                volume_long_today,
                volume_long_his,
                volume_long,
                volume_long_frozen_today,
                volume_long_frozen_his,
                volume_long_frozen,
                volume_short_today,
                volume_short_his,
                volume_short,
                volume_short_frozen_today,
                volume_short_frozen_his,
                volume_short_frozen,
                volume_long_yd,
                volume_short_yd,
                pos,
                pos_long,
                pos_short,
                epoch,
            ]
        ) && eq_float_fields!(
            self,
            other,
            [
                open_price_long,
                open_price_short,
                open_cost_long,
                open_cost_short,
                position_price_long,
                position_price_short,
                position_cost_long,
                position_cost_short,
                last_price,
                float_profit_long,
                float_profit_short,
                float_profit,
                position_profit_long,
                position_profit_short,
                position_profit,
                margin_long,
                margin_short,
                margin,
                market_value_long,
                market_value_short,
                market_value,
            ]
        )
    }
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

impl PartialEq for PreInsertOrder {
    fn eq(&self, other: &Self) -> bool {
        eq_non_float_fields!(
            self,
            other,
            [
                user_id,
                order_id,
                exchange_id,
                instrument_id,
                direction,
                epoch,
            ]
        ) && schema_f64_eq(self.pre_margin, other.pre_margin)
    }
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
    #[serde(default, rename = "volume_orign")]
    pub volume_origin: i64,
    pub volume_left: i64,
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub limit_price: f64,
    pub price_type: String,
    pub volume_condition: String,
    pub time_condition: String,
    pub insert_date_time: i64,
    pub last_msg: String,
    pub status: String,
    pub lifecycle: OrderLifecycle,
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

impl PartialEq for Order {
    fn eq(&self, other: &Self) -> bool {
        eq_non_float_fields!(
            self,
            other,
            [
                seqno,
                user_id,
                order_id,
                exchange_order_id,
                exchange_id,
                instrument_id,
                direction,
                offset,
                volume_origin,
                volume_left,
                price_type,
                volume_condition,
                time_condition,
                insert_date_time,
                last_msg,
                status,
                lifecycle,
                is_dead,
                is_online,
                is_error,
                epoch,
            ]
        ) && eq_float_fields!(
            self,
            other,
            [limit_price, trade_price, frozen_margin, frozen_premium]
        )
    }
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

impl PartialEq for Trade {
    fn eq(&self, other: &Self) -> bool {
        eq_non_float_fields!(
            self,
            other,
            [
                seqno,
                user_id,
                order_id,
                trade_id,
                exchange_trade_id,
                exchange_id,
                instrument_id,
                direction,
                offset,
                volume,
                trade_date_time,
                epoch,
            ]
        ) && eq_float_fields!(self, other, [price, commission])
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
