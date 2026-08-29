//! Stable history-row schema shared by cache and transport adapters.

use crate::error::{DataError, Result};

/// Row family served by a backtest-history query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistorySchemaSeries {
    /// Tick rows.
    Tick,
    /// Kline rows.
    Kline,
}

/// Canonical scalar representation for a history field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryValueKind {
    /// Nanoseconds since the Unix epoch.
    Timestamp,
    /// Signed integer represented without precision loss.
    Integer,
    /// Market price.
    Price,
    /// Decimal quantity such as turnover.
    Decimal,
}

/// Canonical history-row field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BacktestHistoryField {
    Time,
    Id,
    Open,
    High,
    Low,
    Close,
    Volume,
    OpenOi,
    CloseOi,
    LastPrice,
    Average,
    Highest,
    Lowest,
    AskPrice1,
    AskVolume1,
    BidPrice1,
    BidVolume1,
    AskPrice2,
    AskVolume2,
    BidPrice2,
    BidVolume2,
    AskPrice3,
    AskVolume3,
    BidPrice3,
    BidVolume3,
    AskPrice4,
    AskVolume4,
    BidPrice4,
    BidVolume4,
    AskPrice5,
    AskVolume5,
    BidPrice5,
    BidVolume5,
    Amount,
    OpenInterest,
}

impl BacktestHistoryField {
    /// Stable short field name used by external projections.
    #[must_use]
    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::Time => "t",
            Self::Id => "id",
            Self::Open => "o",
            Self::High => "h",
            Self::Low => "l",
            Self::Close => "c",
            Self::Volume => "v",
            Self::OpenOi => "oi0",
            Self::CloseOi | Self::OpenInterest => "oi",
            Self::LastPrice => "lp",
            Self::Average => "avg",
            Self::Highest => "hi",
            Self::Lowest => "lo",
            Self::AskPrice1 => "ap1",
            Self::AskVolume1 => "av1",
            Self::BidPrice1 => "bp1",
            Self::BidVolume1 => "bv1",
            Self::AskPrice2 => "ap2",
            Self::AskVolume2 => "av2",
            Self::BidPrice2 => "bp2",
            Self::BidVolume2 => "bv2",
            Self::AskPrice3 => "ap3",
            Self::AskVolume3 => "av3",
            Self::BidPrice3 => "bp3",
            Self::BidVolume3 => "bv3",
            Self::AskPrice4 => "ap4",
            Self::AskVolume4 => "av4",
            Self::BidPrice4 => "bp4",
            Self::BidVolume4 => "bv4",
            Self::AskPrice5 => "ap5",
            Self::AskVolume5 => "av5",
            Self::BidPrice5 => "bp5",
            Self::BidVolume5 => "bv5",
            Self::Amount => "amt",
        }
    }

    /// Accepted aliases for this field.
    #[must_use]
    pub const fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Time => &["t", "time", "timestamp", "datetime"],
            Self::Id => &["id"],
            Self::Open => &["o", "open"],
            Self::High => &["h", "high"],
            Self::Low => &["l", "low"],
            Self::Close => &["c", "close"],
            Self::Volume => &["v", "volume"],
            Self::OpenOi => &["oi0", "open_oi"],
            Self::CloseOi => &["oi", "close_oi"],
            Self::LastPrice => &["lp", "last_price"],
            Self::Average => &["avg", "average"],
            Self::Highest => &["hi", "highest"],
            Self::Lowest => &["lo", "lowest"],
            Self::AskPrice1 => &["ap1", "ask_price1"],
            Self::AskVolume1 => &["av1", "ask_volume1"],
            Self::BidPrice1 => &["bp1", "bid_price1"],
            Self::BidVolume1 => &["bv1", "bid_volume1"],
            Self::AskPrice2 => &["ap2", "ask_price2"],
            Self::AskVolume2 => &["av2", "ask_volume2"],
            Self::BidPrice2 => &["bp2", "bid_price2"],
            Self::BidVolume2 => &["bv2", "bid_volume2"],
            Self::AskPrice3 => &["ap3", "ask_price3"],
            Self::AskVolume3 => &["av3", "ask_volume3"],
            Self::BidPrice3 => &["bp3", "bid_price3"],
            Self::BidVolume3 => &["bv3", "bid_volume3"],
            Self::AskPrice4 => &["ap4", "ask_price4"],
            Self::AskVolume4 => &["av4", "ask_volume4"],
            Self::BidPrice4 => &["bp4", "bid_price4"],
            Self::BidVolume4 => &["bv4", "bid_volume4"],
            Self::AskPrice5 => &["ap5", "ask_price5"],
            Self::AskVolume5 => &["av5", "ask_volume5"],
            Self::BidPrice5 => &["bp5", "bid_price5"],
            Self::BidVolume5 => &["bv5", "bid_volume5"],
            Self::Amount => &["amt", "amount"],
            Self::OpenInterest => &["oi", "open_interest"],
        }
    }

    /// Scalar representation of this field.
    #[must_use]
    pub const fn value_kind(self) -> BacktestHistoryValueKind {
        match self {
            Self::Time => BacktestHistoryValueKind::Timestamp,
            Self::Id
            | Self::Volume
            | Self::OpenOi
            | Self::CloseOi
            | Self::AskVolume1
            | Self::BidVolume1
            | Self::AskVolume2
            | Self::BidVolume2
            | Self::AskVolume3
            | Self::BidVolume3
            | Self::AskVolume4
            | Self::BidVolume4
            | Self::AskVolume5
            | Self::BidVolume5
            | Self::OpenInterest => BacktestHistoryValueKind::Integer,
            Self::Amount => BacktestHistoryValueKind::Decimal,
            _ => BacktestHistoryValueKind::Price,
        }
    }
}

const KLINE_FIELDS: [BacktestHistoryField; 9] = [
    BacktestHistoryField::Time,
    BacktestHistoryField::Id,
    BacktestHistoryField::Open,
    BacktestHistoryField::High,
    BacktestHistoryField::Low,
    BacktestHistoryField::Close,
    BacktestHistoryField::Volume,
    BacktestHistoryField::OpenOi,
    BacktestHistoryField::CloseOi,
];

const TICK_FIELDS: [BacktestHistoryField; 29] = [
    BacktestHistoryField::Time,
    BacktestHistoryField::Id,
    BacktestHistoryField::LastPrice,
    BacktestHistoryField::Average,
    BacktestHistoryField::Highest,
    BacktestHistoryField::Lowest,
    BacktestHistoryField::AskPrice1,
    BacktestHistoryField::AskVolume1,
    BacktestHistoryField::BidPrice1,
    BacktestHistoryField::BidVolume1,
    BacktestHistoryField::AskPrice2,
    BacktestHistoryField::AskVolume2,
    BacktestHistoryField::BidPrice2,
    BacktestHistoryField::BidVolume2,
    BacktestHistoryField::AskPrice3,
    BacktestHistoryField::AskVolume3,
    BacktestHistoryField::BidPrice3,
    BacktestHistoryField::BidVolume3,
    BacktestHistoryField::AskPrice4,
    BacktestHistoryField::AskVolume4,
    BacktestHistoryField::BidPrice4,
    BacktestHistoryField::BidVolume4,
    BacktestHistoryField::AskPrice5,
    BacktestHistoryField::AskVolume5,
    BacktestHistoryField::BidPrice5,
    BacktestHistoryField::BidVolume5,
    BacktestHistoryField::Volume,
    BacktestHistoryField::Amount,
    BacktestHistoryField::OpenInterest,
];

const KLINE_DEFAULT_FIELDS: [BacktestHistoryField; 7] = [
    BacktestHistoryField::Time,
    BacktestHistoryField::Open,
    BacktestHistoryField::High,
    BacktestHistoryField::Low,
    BacktestHistoryField::Close,
    BacktestHistoryField::Volume,
    BacktestHistoryField::CloseOi,
];

const TICK_DEFAULT_FIELDS: [BacktestHistoryField; 8] = [
    BacktestHistoryField::Time,
    BacktestHistoryField::LastPrice,
    BacktestHistoryField::AskPrice1,
    BacktestHistoryField::AskVolume1,
    BacktestHistoryField::BidPrice1,
    BacktestHistoryField::BidVolume1,
    BacktestHistoryField::Volume,
    BacktestHistoryField::OpenInterest,
];

/// Returns fields in their stable, canonical schema order.
#[must_use]
pub const fn backtest_history_schema_fields(
    series: BacktestHistorySchemaSeries,
) -> &'static [BacktestHistoryField] {
    match series {
        BacktestHistorySchemaSeries::Tick => &TICK_FIELDS,
        BacktestHistorySchemaSeries::Kline => &KLINE_FIELDS,
    }
}

/// Returns the stable default projection for a row family.
#[must_use]
pub const fn backtest_history_default_fields(
    series: BacktestHistorySchemaSeries,
) -> &'static [BacktestHistoryField] {
    match series {
        BacktestHistorySchemaSeries::Tick => &TICK_DEFAULT_FIELDS,
        BacktestHistorySchemaSeries::Kline => &KLINE_DEFAULT_FIELDS,
    }
}

/// Resolves aliases to canonical fields and rejects duplicates.
pub fn backtest_history_resolve_fields<I, S>(
    series: BacktestHistorySchemaSeries,
    aliases: I,
) -> Result<Vec<BacktestHistoryField>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut resolved = Vec::new();
    for alias in aliases {
        let alias = alias.as_ref().trim().to_ascii_lowercase();
        let field = backtest_history_schema_fields(series)
            .iter()
            .copied()
            .find(|field| field.aliases().contains(&alias.as_str()))
            .ok_or_else(|| {
                DataError::Validation(format!(
                    "unknown {} history field {alias}",
                    match series {
                        BacktestHistorySchemaSeries::Tick => "Tick",
                        BacktestHistorySchemaSeries::Kline => "Kline",
                    }
                ))
            })?;
        if resolved.contains(&field) {
            return Err(DataError::Validation(format!(
                "duplicate {} history field {}",
                match series {
                    BacktestHistorySchemaSeries::Tick => "Tick",
                    BacktestHistorySchemaSeries::Kline => "Kline",
                },
                field.canonical_name()
            )));
        }
        resolved.push(field);
    }
    Ok(resolved)
}
