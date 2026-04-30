#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Number, Value};
use tqsdk_core::{TradePriceType, TradeTimeCondition};

use crate::{Result, WaitFacadeError};

/// Typed order-price intent for wait-facade trade submission.
///
/// This keeps price semantics explicit at the facade boundary instead of
/// overloading `serde_json::Value` with magic strings.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderPrice(OrderPriceKind);

#[derive(Debug, Clone, PartialEq)]
enum OrderPriceKind {
    Any,
    Best,
    FiveLevel,
    Limit(Number),
}

impl OrderPrice {
    #[must_use]
    pub fn any() -> Self {
        Self(OrderPriceKind::Any)
    }

    #[must_use]
    pub fn best() -> Self {
        Self(OrderPriceKind::Best)
    }

    #[must_use]
    pub fn five_level() -> Self {
        Self(OrderPriceKind::FiveLevel)
    }

    pub fn limit(limit_price: f64) -> Result<Self> {
        let number = Number::from_f64(limit_price)
            .ok_or(WaitFacadeError::InvalidState("limit price must be finite"))?;
        Ok(Self(OrderPriceKind::Limit(number)))
    }

    pub(crate) fn into_command_parts(self) -> (TradePriceType, Option<Value>, TradeTimeCondition) {
        match self.0 {
            OrderPriceKind::Any => (TradePriceType::Any, None, TradeTimeCondition::Ioc),
            OrderPriceKind::Best => (TradePriceType::Best, None, TradeTimeCondition::Ioc),
            OrderPriceKind::FiveLevel => (TradePriceType::FiveLevel, None, TradeTimeCondition::Ioc),
            OrderPriceKind::Limit(limit_price) => (
                TradePriceType::Limit,
                Some(Value::Number(limit_price)),
                TradeTimeCondition::Gfd,
            ),
        }
    }
}
