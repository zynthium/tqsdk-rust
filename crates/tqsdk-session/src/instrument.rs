#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Map, Value};
use tqsdk_core::{Quote, Symbol, TradingTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentClass {
    Future,
    Continuous,
    Index,
    Option,
    Stock,
    Fund,
    Bond,
    Unknown,
}

impl InstrumentClass {
    #[must_use]
    pub fn from_wire(value: &str) -> Self {
        match value {
            "FUTURE" => Self::Future,
            "CONT" => Self::Continuous,
            "INDEX" => Self::Index,
            "OPTION" => Self::Option,
            "STOCK" => Self::Stock,
            "FUND" => Self::Fund,
            "BOND" => Self::Bond,
            _ => Self::Unknown,
        }
    }
}

/// Typed metadata returned by the official `multi_symbol_info` query.
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolInfo {
    pub instrument_id: Symbol,
    pub instrument_name: String,
    pub exchange_id: String,
    pub product_id: String,
    pub ins_class: String,
    pub class: InstrumentClass,
    pub price_tick: Option<f64>,
    pub volume_multiple: Option<i64>,
    pub open_limit: Option<i64>,
    pub max_limit_order_volume: Option<i64>,
    pub max_market_order_volume: Option<i64>,
    pub min_limit_order_volume: Option<i64>,
    pub min_market_order_volume: Option<i64>,
    pub open_max_market_order_volume: Option<i64>,
    pub open_max_limit_order_volume: Option<i64>,
    pub open_min_market_order_volume: Option<i64>,
    pub open_min_limit_order_volume: Option<i64>,
    pub underlying_symbol: Option<Symbol>,
    pub strike_price: Option<f64>,
    pub expired: bool,
    pub expire_datetime_secs: Option<i64>,
    pub expire_rest_days: Option<i64>,
    pub delivery_year: Option<i64>,
    pub delivery_month: Option<i64>,
    pub last_exercise_datetime_secs: Option<i64>,
    pub exercise_year: Option<i64>,
    pub exercise_month: Option<i64>,
    pub option_class: Option<String>,
    pub upper_limit: Option<f64>,
    pub lower_limit: Option<f64>,
    pub pre_settlement: Option<f64>,
    pub pre_open_interest: Option<i64>,
    pub pre_close: Option<f64>,
    pub trading_time: TradingTime,
}

impl SymbolInfo {
    pub(crate) fn from_metadata_map(
        requested_symbol: &str,
        map: &Map<String, Value>,
    ) -> Result<Self, crate::SessionFacadeError> {
        let instrument_id =
            string_value(map, "instrument_id").unwrap_or_else(|| requested_symbol.to_string());
        let ins_class = string_value(map, "ins_class").unwrap_or_default();
        let trading_time = match map.get("trading_time") {
            Some(value) => serde_json::from_value::<TradingTime>(value.clone()).map_err(|_| {
                crate::SessionFacadeError::InvalidState(
                    "instrument metadata trading_time has invalid shape",
                )
            })?,
            None => TradingTime::default(),
        };

        Ok(Self {
            instrument_id: Symbol::new(instrument_id),
            instrument_name: string_value(map, "instrument_name").unwrap_or_default(),
            exchange_id: string_value(map, "exchange_id").unwrap_or_default(),
            product_id: string_value(map, "product_id").unwrap_or_default(),
            class: InstrumentClass::from_wire(ins_class.as_str()),
            ins_class,
            price_tick: finite_f64_value(map, "price_tick"),
            volume_multiple: i64_value(map, "volume_multiple"),
            open_limit: i64_value(map, "open_limit"),
            max_limit_order_volume: i64_value(map, "max_limit_order_volume"),
            max_market_order_volume: i64_value(map, "max_market_order_volume"),
            min_limit_order_volume: i64_value(map, "min_limit_order_volume"),
            min_market_order_volume: i64_value(map, "min_market_order_volume"),
            open_max_market_order_volume: i64_value(map, "open_max_market_order_volume"),
            open_max_limit_order_volume: i64_value(map, "open_max_limit_order_volume"),
            open_min_market_order_volume: i64_value(map, "open_min_market_order_volume"),
            open_min_limit_order_volume: i64_value(map, "open_min_limit_order_volume"),
            underlying_symbol: string_value(map, "underlying_symbol").map(Symbol::new),
            strike_price: finite_f64_value(map, "strike_price"),
            expired: map.get("expired").and_then(Value::as_bool).unwrap_or(false),
            expire_datetime_secs: i64_value(map, "expire_datetime"),
            expire_rest_days: i64_value(map, "expire_rest_days"),
            delivery_year: i64_value(map, "delivery_year"),
            delivery_month: i64_value(map, "delivery_month"),
            last_exercise_datetime_secs: i64_value(map, "last_exercise_datetime"),
            exercise_year: i64_value(map, "exercise_year"),
            exercise_month: i64_value(map, "exercise_month"),
            option_class: string_value(map, "option_class"),
            upper_limit: finite_f64_value(map, "upper_limit"),
            lower_limit: finite_f64_value(map, "lower_limit"),
            pre_settlement: finite_f64_value(map, "pre_settlement"),
            pre_open_interest: i64_value(map, "pre_open_interest"),
            pre_close: finite_f64_value(map, "pre_close"),
            trading_time,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentSpec {
    pub symbol: Symbol,
    pub exchange_id: String,
    pub product_id: String,
    pub class: InstrumentClass,
    pub price_tick: f64,
    pub volume_multiple: i64,
    pub expire_datetime_secs: Option<i64>,
    pub underlying_symbol: Option<Symbol>,
}

impl InstrumentSpec {
    #[must_use]
    pub fn is_derivative(&self) -> bool {
        matches!(
            self.class,
            InstrumentClass::Future
                | InstrumentClass::Continuous
                | InstrumentClass::Index
                | InstrumentClass::Option
        )
    }
}

impl TryFrom<Quote> for InstrumentSpec {
    type Error = crate::SessionFacadeError;

    fn try_from(quote: Quote) -> Result<Self, Self::Error> {
        if quote.instrument_id.is_empty() {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata is missing instrument_id",
            ));
        }
        if !quote.price_tick.is_finite() || quote.price_tick <= 0.0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata price_tick must be positive",
            ));
        }
        if quote.volume_multiple <= 0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata volume_multiple must be positive",
            ));
        }

        Ok(Self {
            symbol: Symbol::new(quote.instrument_id),
            exchange_id: quote.exchange_id,
            product_id: quote.product_id,
            class: InstrumentClass::from_wire(quote.ins_class.as_str()),
            price_tick: quote.price_tick,
            volume_multiple: quote.volume_multiple,
            expire_datetime_secs: quote.expire_datetime,
            underlying_symbol: (!quote.underlying_symbol.is_empty())
                .then(|| Symbol::new(quote.underlying_symbol)),
        })
    }
}

impl TryFrom<SymbolInfo> for InstrumentSpec {
    type Error = crate::SessionFacadeError;

    fn try_from(info: SymbolInfo) -> Result<Self, Self::Error> {
        let price_tick = info
            .price_tick
            .ok_or(crate::SessionFacadeError::InvalidState(
                "instrument metadata is missing price_tick",
            ))?;
        if !price_tick.is_finite() || price_tick <= 0.0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata price_tick must be positive",
            ));
        }

        let volume_multiple =
            info.volume_multiple
                .ok_or(crate::SessionFacadeError::InvalidState(
                    "instrument metadata is missing volume_multiple",
                ))?;
        if volume_multiple <= 0 {
            return Err(crate::SessionFacadeError::InvalidState(
                "instrument metadata volume_multiple must be positive",
            ));
        }

        Ok(Self {
            symbol: info.instrument_id,
            exchange_id: info.exchange_id,
            product_id: info.product_id,
            class: info.class,
            price_tick,
            volume_multiple,
            expire_datetime_secs: info.expire_datetime_secs,
            underlying_symbol: info.underlying_symbol,
        })
    }
}

fn string_value(map: &Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn i64_value(map: &Map<String, Value>, key: &str) -> Option<i64> {
    map.get(key).and_then(Value::as_i64)
}

fn finite_f64_value(map: &Map<String, Value>, key: &str) -> Option<f64> {
    map.get(key)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}
