#![cfg_attr(not(test), forbid(unsafe_code))]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMarketKind {
    Futures,
    Stock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TqBacktest {
    start_datetime_ns: i64,
    end_datetime_ns: i64,
    market_kind: BacktestMarketKind,
}

impl TqBacktest {
    pub fn new(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::futures(start_datetime_ns, end_datetime_ns)
    }

    pub fn futures(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(
            start_datetime_ns,
            end_datetime_ns,
            BacktestMarketKind::Futures,
        )
    }

    pub fn stock(start_datetime_ns: i64, end_datetime_ns: i64) -> crate::error::Result<Self> {
        Self::with_market_kind(
            start_datetime_ns,
            end_datetime_ns,
            BacktestMarketKind::Stock,
        )
    }

    fn with_market_kind(
        start_datetime_ns: i64,
        end_datetime_ns: i64,
        market_kind: BacktestMarketKind,
    ) -> crate::error::Result<Self> {
        if start_datetime_ns >= end_datetime_ns {
            return Err(crate::error::WaitFacadeError::InvalidState(
                "backtest start_datetime_ns must be less than end_datetime_ns",
            ));
        }
        Ok(Self {
            start_datetime_ns,
            end_datetime_ns,
            market_kind,
        })
    }

    #[must_use]
    pub fn start_datetime_ns(&self) -> i64 {
        self.start_datetime_ns
    }

    #[must_use]
    pub fn end_datetime_ns(&self) -> i64 {
        self.end_datetime_ns
    }

    #[must_use]
    pub fn market_kind(&self) -> BacktestMarketKind {
        self.market_kind
    }
}
