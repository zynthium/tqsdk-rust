#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{Quote, Symbol};

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

#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentSpec {
    pub symbol: Symbol,
    pub exchange_id: String,
    pub product_id: String,
    pub class: InstrumentClass,
    pub price_tick: f64,
    pub volume_multiple: i64,
    pub expire_datetime_ns: Option<i64>,
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

        let class = match quote.ins_class.as_str() {
            "FUTURE" => InstrumentClass::Future,
            "CONT" => InstrumentClass::Continuous,
            "INDEX" => InstrumentClass::Index,
            "OPTION" => InstrumentClass::Option,
            "STOCK" => InstrumentClass::Stock,
            "FUND" => InstrumentClass::Fund,
            "BOND" => InstrumentClass::Bond,
            _ => InstrumentClass::Unknown,
        };

        Ok(Self {
            symbol: Symbol::new(quote.instrument_id),
            exchange_id: quote.exchange_id,
            product_id: quote.product_id,
            class,
            price_tick: quote.price_tick,
            volume_multiple: quote.volume_multiple,
            expire_datetime_ns: quote.expire_datetime,
            underlying_symbol: (!quote.underlying_symbol.is_empty())
                .then(|| Symbol::new(quote.underlying_symbol)),
        })
    }
}
