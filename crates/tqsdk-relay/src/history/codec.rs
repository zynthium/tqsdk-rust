//! Relay-private JSON projection for CacheOnly history rows.
//!
//! Field alias resolution belongs to `tqsdk-data`. This module only accepts
//! already-resolved fields and turns typed rows into the HTTP positional-row
//! representation.

use std::fmt;

use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use serde_json::Value;
use tqsdk_core::{Kline, Tick};
use tqsdk_data::{
    BacktestHistoryField, BacktestHistoryRows, BacktestHistorySchemaSeries,
    backtest_history_schema_fields,
};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 60 * 60 * NANOS_PER_SECOND;

/// A validated relay-local projection for one typed history row family.
#[derive(Debug, Clone)]
pub(super) struct HistoryRowCodec {
    series: BacktestHistorySchemaSeries,
    columns: Vec<HistoryColumn>,
}

impl HistoryRowCodec {
    /// Builds a positional JSON projection from fields already resolved by
    /// `tqsdk-data`.
    pub(super) fn new(
        series: BacktestHistorySchemaSeries,
        columns: Vec<HistoryColumn>,
    ) -> Result<Self, HistoryRowCodecError> {
        if columns.is_empty() {
            return Err(HistoryRowCodecError::EmptyProjection);
        }
        let legal_fields = backtest_history_schema_fields(series);
        if let Some(field) = columns.iter().find_map(|column| match column {
            HistoryColumn::Field(field) if !legal_fields.contains(field) => Some(*field),
            HistoryColumn::Field(_) | HistoryColumn::RawNanoseconds => None,
        }) {
            return Err(HistoryRowCodecError::UnsupportedField { series, field });
        }
        Ok(Self { series, columns })
    }

    /// Canonical column names in the exact positional order used for rows.
    #[must_use]
    pub(super) fn column_names(&self) -> Vec<&'static str> {
        self.columns
            .iter()
            .map(|column| column.canonical_name())
            .collect()
    }

    /// Projects a single incremental history chunk.
    ///
    /// `estimated_json_bytes` is the exact UTF-8 length of the returned rows
    /// encoded as a JSON array. The enclosing response's fixed fields are not
    /// included, so the HTTP coordinator can account for them exactly once.
    pub(super) fn encode_chunk(
        &self,
        chunk: &BacktestHistoryRows,
    ) -> Result<EncodedHistoryRows, HistoryRowCodecError> {
        let rows = match (self.series, chunk) {
            (BacktestHistorySchemaSeries::Tick, BacktestHistoryRows::Ticks(rows)) => rows
                .iter()
                .map(|row| self.encode_tick(row))
                .collect::<Result<Vec<_>, _>>()?,
            (
                BacktestHistorySchemaSeries::Kline,
                BacktestHistoryRows::Klines { duration_ns, rows },
            ) => rows
                .iter()
                .map(|row| self.encode_kline(row, *duration_ns))
                .collect::<Result<Vec<_>, _>>()?,
            (expected, BacktestHistoryRows::Ticks(_)) => {
                return Err(HistoryRowCodecError::ChunkSeriesMismatch {
                    expected,
                    actual: BacktestHistorySchemaSeries::Tick,
                });
            }
            (expected, BacktestHistoryRows::Klines { .. }) => {
                return Err(HistoryRowCodecError::ChunkSeriesMismatch {
                    expected,
                    actual: BacktestHistorySchemaSeries::Kline,
                });
            }
        };
        let row_count = rows.len();
        let estimated_json_bytes = serde_json::to_vec(&rows)
            .map_err(HistoryRowCodecError::Json)?
            .len();
        Ok(EncodedHistoryRows {
            rows,
            row_count,
            estimated_json_bytes,
        })
    }

    fn encode_tick(&self, row: &Tick) -> Result<Value, HistoryRowCodecError> {
        let cells = self
            .columns
            .iter()
            .map(|column| match column {
                HistoryColumn::Field(field) => tick_cell(row, *field),
                HistoryColumn::RawNanoseconds => Ok(integer_cell(row.datetime)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Value::Array(cells))
    }

    fn encode_kline(&self, row: &Kline, duration_ns: i64) -> Result<Value, HistoryRowCodecError> {
        if duration_ns <= 0 {
            return Err(HistoryRowCodecError::InvalidKlineDuration(duration_ns));
        }
        let cells = self
            .columns
            .iter()
            .map(|column| match column {
                HistoryColumn::Field(field) => kline_cell(row, *field, duration_ns),
                HistoryColumn::RawNanoseconds => Ok(integer_cell(row.datetime)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Value::Array(cells))
    }
}

/// One positional history JSON column selected by the HTTP route.
///
/// Field aliases are resolved upstream; `RawNanoseconds` corresponds to the
/// relay-only `tns` pseudo-column and keeps the caller's selected order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HistoryColumn {
    Field(BacktestHistoryField),
    RawNanoseconds,
}

impl HistoryColumn {
    #[must_use]
    pub(super) const fn canonical_name(self) -> &'static str {
        match self {
            Self::Field(field) => field.canonical_name(),
            Self::RawNanoseconds => "tns",
        }
    }
}

/// Encoded positional rows plus accounting for the response coordinator.
#[derive(Debug, Clone)]
pub(super) struct EncodedHistoryRows {
    pub(super) rows: Vec<Value>,
    pub(super) row_count: usize,
    pub(super) estimated_json_bytes: usize,
}

/// Projection failures the HTTP layer can map to a stable response error.
#[derive(Debug)]
pub(super) enum HistoryRowCodecError {
    EmptyProjection,
    UnsupportedField {
        series: BacktestHistorySchemaSeries,
        field: BacktestHistoryField,
    },
    ChunkSeriesMismatch {
        expected: BacktestHistorySchemaSeries,
        actual: BacktestHistorySchemaSeries,
    },
    InvalidKlineDuration(i64),
    InvalidTimestamp(i64),
    Json(serde_json::Error),
}

impl fmt::Display for HistoryRowCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyProjection => formatter.write_str("history row projection is empty"),
            Self::UnsupportedField { series, field } => write!(
                formatter,
                "{} history field {} is not supported by this row family",
                series_name(*series),
                field.canonical_name()
            ),
            Self::ChunkSeriesMismatch { expected, actual } => write!(
                formatter,
                "{} history projection cannot encode {} history rows",
                series_name(*expected),
                series_name(*actual)
            ),
            Self::InvalidKlineDuration(duration_ns) => {
                write!(formatter, "Kline duration {duration_ns} must be positive")
            }
            Self::InvalidTimestamp(value) => {
                write!(formatter, "timestamp {value} is outside the RFC3339 range")
            }
            Self::Json(error) => write!(formatter, "history row JSON encoding failed: {error}"),
        }
    }
}

impl std::error::Error for HistoryRowCodecError {}

fn tick_cell(row: &Tick, field: BacktestHistoryField) -> Result<Value, HistoryRowCodecError> {
    match field {
        BacktestHistoryField::Time => tick_time_cell(row.datetime),
        BacktestHistoryField::Id => Ok(integer_cell(row.id)),
        BacktestHistoryField::LastPrice => Ok(float_cell(row.last_price)),
        BacktestHistoryField::Average => Ok(float_cell(row.average)),
        BacktestHistoryField::Highest => Ok(float_cell(row.highest)),
        BacktestHistoryField::Lowest => Ok(float_cell(row.lowest)),
        BacktestHistoryField::AskPrice1 => Ok(float_cell(row.ask_price1)),
        BacktestHistoryField::AskVolume1 => Ok(integer_cell(row.ask_volume1)),
        BacktestHistoryField::BidPrice1 => Ok(float_cell(row.bid_price1)),
        BacktestHistoryField::BidVolume1 => Ok(integer_cell(row.bid_volume1)),
        BacktestHistoryField::AskPrice2 => Ok(float_cell(row.ask_price2)),
        BacktestHistoryField::AskVolume2 => Ok(integer_cell(row.ask_volume2)),
        BacktestHistoryField::BidPrice2 => Ok(float_cell(row.bid_price2)),
        BacktestHistoryField::BidVolume2 => Ok(integer_cell(row.bid_volume2)),
        BacktestHistoryField::AskPrice3 => Ok(float_cell(row.ask_price3)),
        BacktestHistoryField::AskVolume3 => Ok(integer_cell(row.ask_volume3)),
        BacktestHistoryField::BidPrice3 => Ok(float_cell(row.bid_price3)),
        BacktestHistoryField::BidVolume3 => Ok(integer_cell(row.bid_volume3)),
        BacktestHistoryField::AskPrice4 => Ok(float_cell(row.ask_price4)),
        BacktestHistoryField::BidVolume4 => Ok(integer_cell(row.bid_volume4)),
        BacktestHistoryField::BidPrice4 => Ok(float_cell(row.bid_price4)),
        BacktestHistoryField::AskVolume4 => Ok(integer_cell(row.ask_volume4)),
        BacktestHistoryField::BidPrice5 => Ok(float_cell(row.bid_price5)),
        BacktestHistoryField::BidVolume5 => Ok(integer_cell(row.bid_volume5)),
        BacktestHistoryField::AskPrice5 => Ok(float_cell(row.ask_price5)),
        BacktestHistoryField::AskVolume5 => Ok(integer_cell(row.ask_volume5)),
        BacktestHistoryField::Volume => Ok(integer_cell(row.volume)),
        BacktestHistoryField::Amount => Ok(float_cell(row.amount)),
        BacktestHistoryField::OpenInterest => Ok(integer_cell(row.open_interest)),
        _ => Err(HistoryRowCodecError::UnsupportedField {
            series: BacktestHistorySchemaSeries::Tick,
            field,
        }),
    }
}

fn kline_cell(
    row: &Kline,
    field: BacktestHistoryField,
    duration_ns: i64,
) -> Result<Value, HistoryRowCodecError> {
    match field {
        BacktestHistoryField::Time => kline_time_cell(row.datetime, duration_ns),
        BacktestHistoryField::Id => Ok(integer_cell(row.id)),
        BacktestHistoryField::Open => Ok(float_cell(row.open)),
        BacktestHistoryField::High => Ok(float_cell(row.high)),
        BacktestHistoryField::Low => Ok(float_cell(row.low)),
        BacktestHistoryField::Close => Ok(float_cell(row.close)),
        BacktestHistoryField::Volume => Ok(integer_cell(row.volume)),
        BacktestHistoryField::OpenOi => Ok(integer_cell(row.open_oi)),
        BacktestHistoryField::CloseOi => Ok(integer_cell(row.close_oi)),
        _ => Err(HistoryRowCodecError::UnsupportedField {
            series: BacktestHistorySchemaSeries::Kline,
            field,
        }),
    }
}

fn integer_cell(value: i64) -> Value {
    Value::String(value.to_string())
}

fn float_cell(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

fn tick_time_cell(value: i64) -> Result<Value, HistoryRowCodecError> {
    Ok(Value::String(
        shanghai_time(value)?.to_rfc3339_opts(SecondsFormat::Millis, false),
    ))
}

fn kline_time_cell(value: i64, duration_ns: i64) -> Result<Value, HistoryRowCodecError> {
    let time = shanghai_time(value)?;
    if duration_ns >= NANOS_PER_DAY {
        Ok(Value::String(time.format("%Y-%m-%d").to_string()))
    } else {
        Ok(Value::String(
            time.to_rfc3339_opts(SecondsFormat::Secs, false),
        ))
    }
}

fn shanghai_time(value: i64) -> Result<DateTime<FixedOffset>, HistoryRowCodecError> {
    let seconds = value.div_euclid(NANOS_PER_SECOND);
    let nanos = value.rem_euclid(NANOS_PER_SECOND) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|time| {
            time.with_timezone(
                &FixedOffset::east_opt(8 * 60 * 60)
                    .expect("Asia/Shanghai UTC+08:00 offset must be valid"),
            )
        })
        .ok_or(HistoryRowCodecError::InvalidTimestamp(value))
}

const fn series_name(series: BacktestHistorySchemaSeries) -> &'static str {
    match series {
        BacktestHistorySchemaSeries::Tick => "Tick",
        BacktestHistorySchemaSeries::Kline => "Kline",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tqsdk_core::{Kline, Tick};
    use tqsdk_data::{
        BacktestHistoryField as Field, BacktestHistoryRows, BacktestHistorySchemaSeries,
        backtest_history_schema_fields,
    };

    use super::{HistoryColumn, HistoryRowCodec};

    #[test]
    fn tick_projection_uses_canonical_columns_typed_cells_and_tns() {
        let mut tick = Tick {
            id: 9_007_199_254_740_993,
            datetime: 1_754_012_345_678_901_234,
            ..Tick::default()
        };
        tick.last_price = 3201.5;
        tick.ask_price1 = f64::NAN;
        tick.ask_volume1 = 12;
        tick.bid_price1 = f64::INFINITY;
        tick.bid_volume1 = -3;
        tick.volume = 99;
        tick.open_interest = 7;
        let codec = HistoryRowCodec::new(
            BacktestHistorySchemaSeries::Tick,
            vec![
                HistoryColumn::Field(Field::Id),
                HistoryColumn::RawNanoseconds,
                HistoryColumn::Field(Field::Time),
                HistoryColumn::Field(Field::LastPrice),
                HistoryColumn::Field(Field::AskPrice1),
                HistoryColumn::Field(Field::AskVolume1),
                HistoryColumn::Field(Field::BidPrice1),
                HistoryColumn::Field(Field::BidVolume1),
                HistoryColumn::Field(Field::Volume),
                HistoryColumn::Field(Field::OpenInterest),
            ],
        )
        .unwrap();

        let encoded = codec
            .encode_chunk(&BacktestHistoryRows::Ticks(vec![tick]))
            .unwrap();

        assert_eq!(
            codec.column_names(),
            [
                "id", "tns", "t", "lp", "ap1", "av1", "bp1", "bv1", "v", "oi"
            ]
        );
        assert_eq!(encoded.row_count, 1);
        assert_eq!(
            encoded.rows,
            vec![json!([
                "9007199254740993",
                "1754012345678901234",
                "2025-08-01T09:39:05.678+08:00",
                3201.5,
                null,
                "12",
                null,
                "-3",
                "99",
                "7"
            ])]
        );
        assert_eq!(
            encoded.estimated_json_bytes,
            serde_json::to_vec(&encoded.rows).unwrap().len()
        );
    }

    #[test]
    fn kline_projection_formats_intraday_and_daily_times_and_all_schema_fields() {
        let row = Kline {
            id: 42,
            datetime: 1_754_012_345_678_901_234,
            open: 1.0,
            high: f64::NAN,
            low: 0.5,
            close: f64::NEG_INFINITY,
            volume: 123,
            open_oi: 456,
            close_oi: 789,
            ..Kline::default()
        };
        let fields = vec![
            HistoryColumn::Field(Field::Time),
            HistoryColumn::Field(Field::Id),
            HistoryColumn::Field(Field::Open),
            HistoryColumn::Field(Field::High),
            HistoryColumn::Field(Field::Low),
            HistoryColumn::Field(Field::Close),
            HistoryColumn::Field(Field::Volume),
            HistoryColumn::Field(Field::OpenOi),
            HistoryColumn::Field(Field::CloseOi),
        ];
        let intraday = HistoryRowCodec::new(BacktestHistorySchemaSeries::Kline, fields.clone())
            .unwrap()
            .encode_chunk(&BacktestHistoryRows::Klines {
                duration_ns: 60 * 1_000_000_000,
                rows: vec![row.clone()],
            })
            .unwrap();
        let daily = HistoryRowCodec::new(BacktestHistorySchemaSeries::Kline, fields)
            .unwrap()
            .encode_chunk(&BacktestHistoryRows::Klines {
                duration_ns: 24 * 60 * 60 * 1_000_000_000,
                rows: vec![row],
            })
            .unwrap();

        assert_eq!(
            intraday.rows,
            vec![json!([
                "2025-08-01T09:39:05+08:00",
                "42",
                1.0,
                null,
                0.5,
                null,
                "123",
                "456",
                "789"
            ])]
        );
        assert_eq!(
            daily.rows,
            vec![json!([
                "2025-08-01",
                "42",
                1.0,
                null,
                0.5,
                null,
                "123",
                "456",
                "789"
            ])]
        );
    }

    #[test]
    fn tick_projection_supports_every_typed_schema_field() {
        let mut tick = Tick {
            id: 1,
            datetime: 0,
            ask_volume4: 404,
            bid_volume4: 405,
            ask_volume5: 504,
            bid_volume5: 505,
            volume: 606,
            open_interest: 707,
            ..Tick::default()
        };
        tick.ask_price4 = 4.4;
        tick.bid_price4 = 4.5;
        tick.ask_price5 = 5.4;
        tick.bid_price5 = 5.5;
        tick.amount = 1.25;
        let columns = backtest_history_schema_fields(BacktestHistorySchemaSeries::Tick)
            .iter()
            .copied()
            .map(HistoryColumn::Field)
            .collect();
        let codec = HistoryRowCodec::new(BacktestHistorySchemaSeries::Tick, columns).unwrap();

        let encoded = codec
            .encode_chunk(&BacktestHistoryRows::Ticks(vec![tick]))
            .unwrap();

        assert_eq!(encoded.rows[0].as_array().unwrap().len(), 29);
        assert_eq!(encoded.rows[0][0], json!("1970-01-01T08:00:00.000+08:00"));
        assert_eq!(encoded.rows[0][1], json!("1"));
        assert_eq!(encoded.rows[0][18], json!(4.4));
        assert_eq!(encoded.rows[0][19], json!("404"));
        assert_eq!(encoded.rows[0][20], json!(4.5));
        assert_eq!(encoded.rows[0][21], json!("405"));
        assert_eq!(encoded.rows[0][22], json!(5.4));
        assert_eq!(encoded.rows[0][23], json!("504"));
        assert_eq!(encoded.rows[0][24], json!(5.5));
        assert_eq!(encoded.rows[0][25], json!("505"));
        assert_eq!(encoded.rows[0][26], json!("606"));
        assert_eq!(encoded.rows[0][27], json!(1.25));
        assert_eq!(encoded.rows[0][28], json!("707"));
    }

    #[test]
    fn rejects_cross_family_fields_and_chunks() {
        let error = HistoryRowCodec::new(
            BacktestHistorySchemaSeries::Tick,
            vec![HistoryColumn::Field(Field::Open)],
        )
        .unwrap_err();
        assert!(error.to_string().contains("field o"));

        let codec = HistoryRowCodec::new(
            BacktestHistorySchemaSeries::Tick,
            vec![HistoryColumn::Field(Field::Time)],
        )
        .unwrap();
        let error = codec
            .encode_chunk(&BacktestHistoryRows::Klines {
                duration_ns: 60 * 1_000_000_000,
                rows: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("Tick history projection"));
    }

    #[test]
    fn rejects_invalid_kline_duration() {
        let codec = HistoryRowCodec::new(
            BacktestHistorySchemaSeries::Kline,
            vec![HistoryColumn::Field(Field::Time)],
        )
        .unwrap();
        let error = codec
            .encode_chunk(&BacktestHistoryRows::Klines {
                duration_ns: 0,
                rows: vec![Kline::default()],
            })
            .unwrap_err();
        assert!(error.to_string().contains("must be positive"));
    }

    #[test]
    fn preserves_pre_resolved_field_order_without_re_resolving_aliases() {
        let codec = HistoryRowCodec::new(
            BacktestHistorySchemaSeries::Tick,
            vec![
                HistoryColumn::Field(Field::Volume),
                HistoryColumn::Field(Field::LastPrice),
                HistoryColumn::Field(Field::Time),
            ],
        )
        .unwrap();
        assert_eq!(codec.column_names(), ["v", "lp", "t"]);

        let mut tick = Tick {
            datetime: 0,
            volume: 1,
            ..Tick::default()
        };
        tick.last_price = 2.0;
        let rows = codec
            .encode_chunk(&BacktestHistoryRows::Ticks(vec![tick]))
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            vec![Value::Array(vec![
                json!("1"),
                json!(2.0),
                json!("1970-01-01T08:00:00.000+08:00")
            ])]
        );
    }
}
