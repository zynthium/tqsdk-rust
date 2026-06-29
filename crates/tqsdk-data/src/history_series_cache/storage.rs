use std::io::Write;

use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::{KLINE_DATA_COLS, TICK_1_LEVEL_DATA_COLS, TICK_5_LEVEL_DATA_COLS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SeriesLayout {
    Kline { duration_ns: i64 },
    Tick { five_level: bool },
}

impl SeriesLayout {
    pub(super) fn row_size(self) -> usize {
        let cols = match self {
            Self::Kline { .. } => KLINE_DATA_COLS,
            Self::Tick { five_level: true } => TICK_5_LEVEL_DATA_COLS,
            Self::Tick { five_level: false } => TICK_1_LEVEL_DATA_COLS,
        };
        (2 + cols) * 8
    }
}

pub(super) fn write_kline_row(writer: &mut impl Write, row: &Kline) -> Result<()> {
    write_i64(writer, row.id)?;
    write_i64(writer, row.datetime)?;
    write_f64(writer, row.open)?;
    write_f64(writer, row.high)?;
    write_f64(writer, row.low)?;
    write_f64(writer, row.close)?;
    write_f64(writer, row.volume as f64)?;
    write_f64(writer, row.open_oi as f64)?;
    write_f64(writer, row.close_oi as f64)
}

pub(super) fn write_tick_row(writer: &mut impl Write, row: &Tick, five_level: bool) -> Result<()> {
    write_i64(writer, row.id)?;
    write_i64(writer, row.datetime)?;
    write_f64(writer, row.last_price)?;
    write_f64(writer, row.highest)?;
    write_f64(writer, row.lowest)?;
    write_f64(writer, row.average)?;
    write_f64(writer, row.volume as f64)?;
    write_f64(writer, row.amount)?;
    write_f64(writer, row.open_interest as f64)?;
    write_tick_level(
        writer,
        row.bid_price1,
        row.bid_volume1,
        row.ask_price1,
        row.ask_volume1,
    )?;
    if five_level {
        write_tick_level(
            writer,
            row.bid_price2,
            row.bid_volume2,
            row.ask_price2,
            row.ask_volume2,
        )?;
        write_tick_level(
            writer,
            row.bid_price3,
            row.bid_volume3,
            row.ask_price3,
            row.ask_volume3,
        )?;
        write_tick_level(
            writer,
            row.bid_price4,
            row.bid_volume4,
            row.ask_price4,
            row.ask_volume4,
        )?;
        write_tick_level(
            writer,
            row.bid_price5,
            row.bid_volume5,
            row.ask_price5,
            row.ask_volume5,
        )?;
    }
    Ok(())
}

fn write_tick_level(
    writer: &mut impl Write,
    bid_price: f64,
    bid_volume: i64,
    ask_price: f64,
    ask_volume: i64,
) -> Result<()> {
    write_f64(writer, bid_price)?;
    write_f64(writer, bid_volume as f64)?;
    write_f64(writer, ask_price)?;
    write_f64(writer, ask_volume as f64)
}

fn write_i64(writer: &mut impl Write, value: i64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}

fn write_f64(writer: &mut impl Write, value: f64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}
