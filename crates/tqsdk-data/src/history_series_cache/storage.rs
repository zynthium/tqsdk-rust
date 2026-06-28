#![allow(unsafe_code)]

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use memmap2::Mmap;
use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::{KLINE_DATA_COLS, TICK_1_LEVEL_DATA_COLS, TICK_5_LEVEL_DATA_COLS};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SeriesLayout {
    Kline { duration_ns: i64 },
    Tick { five_level: bool },
}

pub(super) struct MappedSeriesFile {
    mmap: Option<Mmap>,
    pub(super) row_count: usize,
    layout: SeriesLayout,
}

impl MappedSeriesFile {
    pub(super) fn open(path: PathBuf, layout: SeriesLayout) -> Result<Self> {
        let file = File::open(&path)?;
        let len = file.metadata()?.len() as usize;
        let row_size = layout.row_size();
        if len == 0 {
            return Ok(Self {
                mmap: None,
                row_count: 0,
                layout,
            });
        }
        if len % row_size != 0 {
            return Err(DataError::InvalidResponse(format!(
                "history series cache file length does not match row width: {}",
                path.display()
            )));
        }
        let mmap = map_file(&file)?;
        Ok(Self {
            mmap: Some(mmap),
            row_count: len / row_size,
            layout,
        })
    }

    pub(super) fn datetime_at(&self, index: usize) -> Result<i64> {
        self.read_i64(index, 8)
    }

    pub(super) fn last_index_where<F>(&self, predicate: F) -> Result<Option<usize>>
    where
        F: Fn(i64) -> bool,
    {
        let mut lo = 0usize;
        let mut hi = self.row_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if predicate(self.datetime_at(mid)?) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 { Ok(None) } else { Ok(Some(lo - 1)) }
    }

    pub(super) fn read_row(&self, index: usize) -> Result<DecodedRow> {
        let mut offset = index.checked_mul(self.layout.row_size()).ok_or_else(|| {
            DataError::InvalidResponse("history series cache offset overflow".to_string())
        })?;
        let mmap = self.mmap.as_ref().ok_or_else(|| {
            DataError::InvalidResponse("history series cache row index out of bounds".to_string())
        })?;
        let id = read_i64_from(mmap, offset)?;
        offset += 8;
        let datetime = read_i64_from(mmap, offset)?;
        offset += 8;
        match self.layout {
            SeriesLayout::Kline { .. } => Ok(DecodedRow::Kline(Kline {
                id,
                datetime,
                open: read_f64_advance(mmap, &mut offset)?,
                high: read_f64_advance(mmap, &mut offset)?,
                low: read_f64_advance(mmap, &mut offset)?,
                close: read_f64_advance(mmap, &mut offset)?,
                volume: read_f64_advance(mmap, &mut offset)? as i64,
                open_oi: read_f64_advance(mmap, &mut offset)? as i64,
                close_oi: read_f64_advance(mmap, &mut offset)? as i64,
                ..Kline::default()
            })),
            SeriesLayout::Tick { five_level } => {
                let mut row = Tick {
                    id,
                    datetime,
                    last_price: read_f64_advance(mmap, &mut offset)?,
                    highest: read_f64_advance(mmap, &mut offset)?,
                    lowest: read_f64_advance(mmap, &mut offset)?,
                    average: read_f64_advance(mmap, &mut offset)?,
                    volume: read_f64_advance(mmap, &mut offset)? as i64,
                    amount: read_f64_advance(mmap, &mut offset)?,
                    open_interest: read_f64_advance(mmap, &mut offset)? as i64,
                    ..Tick::default()
                };
                read_tick_level(
                    mmap,
                    &mut offset,
                    &mut row.bid_price1,
                    &mut row.bid_volume1,
                    &mut row.ask_price1,
                    &mut row.ask_volume1,
                )?;
                if five_level {
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price2,
                        &mut row.bid_volume2,
                        &mut row.ask_price2,
                        &mut row.ask_volume2,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price3,
                        &mut row.bid_volume3,
                        &mut row.ask_price3,
                        &mut row.ask_volume3,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price4,
                        &mut row.bid_volume4,
                        &mut row.ask_price4,
                        &mut row.ask_volume4,
                    )?;
                    read_tick_level(
                        mmap,
                        &mut offset,
                        &mut row.bid_price5,
                        &mut row.bid_volume5,
                        &mut row.ask_price5,
                        &mut row.ask_volume5,
                    )?;
                }
                Ok(DecodedRow::Tick(row))
            }
        }
    }

    pub(super) fn write_rows_to(&self, rows_to_copy: i64, writer: &mut impl Write) -> Result<()> {
        let rows_to_copy = usize::try_from(rows_to_copy.max(0)).map_err(|_| {
            DataError::InvalidResponse("history series merge row count overflow".to_string())
        })?;
        if rows_to_copy > self.row_count {
            return Err(DataError::InvalidResponse(
                "history series merge requested more rows than segment contains".to_string(),
            ));
        }
        let bytes_to_copy = rows_to_copy
            .checked_mul(self.layout.row_size())
            .ok_or_else(|| {
                DataError::InvalidResponse("history series merge byte count overflow".to_string())
            })?;
        if let Some(mmap) = &self.mmap {
            writer.write_all(&mmap[..bytes_to_copy])?;
        }
        Ok(())
    }

    #[must_use]
    pub(super) fn row_count(&self) -> usize {
        self.row_count
    }

    fn read_i64(&self, index: usize, field_offset: usize) -> Result<i64> {
        let offset = index
            .checked_mul(self.layout.row_size())
            .and_then(|base| base.checked_add(field_offset))
            .ok_or_else(|| {
                DataError::InvalidResponse("history series cache offset overflow".to_string())
            })?;
        let mmap = self.mmap.as_ref().ok_or_else(|| {
            DataError::InvalidResponse("history series cache row index out of bounds".to_string())
        })?;
        read_i64_from(mmap, offset)
    }
}

pub(super) enum DecodedRow {
    Kline(Kline),
    Tick(Tick),
}

pub(super) enum WindowRows {
    Kline(Vec<Kline>),
    Tick(Vec<Tick>),
}

impl WindowRows {
    pub(super) fn push(&mut self, row: DecodedRow) {
        match (self, row) {
            (Self::Kline(rows), DecodedRow::Kline(row)) => rows.push(row),
            (Self::Tick(rows), DecodedRow::Tick(row)) => rows.push(row),
            _ => {}
        }
    }

    pub(super) fn into_klines(self) -> Vec<Kline> {
        match self {
            Self::Kline(rows) => rows,
            Self::Tick(_) => Vec::new(),
        }
    }

    pub(super) fn into_ticks(self) -> Vec<Tick> {
        match self {
            Self::Tick(rows) => rows,
            Self::Kline(_) => Vec::new(),
        }
    }
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

    pub(super) fn duration_ns(self) -> i64 {
        match self {
            Self::Kline { duration_ns } => duration_ns,
            Self::Tick { .. } => 0,
        }
    }
}

pub(super) fn layout_for(symbol: &str, duration_ns: i64) -> SeriesLayout {
    if duration_ns == 0 {
        SeriesLayout::Tick {
            five_level: tick_uses_five_levels(symbol),
        }
    } else {
        SeriesLayout::Kline { duration_ns }
    }
}

pub(super) fn tick_uses_five_levels(symbol: &str) -> bool {
    matches!(symbol.split('.').next(), Some("SHFE" | "SSE" | "SZSE"))
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

pub(super) fn write_tick_level(
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

pub(super) fn write_i64(writer: &mut impl Write, value: i64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}

pub(super) fn write_f64(writer: &mut impl Write, value: f64) -> Result<()> {
    writer
        .write_all(&value.to_ne_bytes())
        .map_err(DataError::from)
}

fn map_file(file: &File) -> Result<Mmap> {
    // SAFETY: the mapping is read-only and all typed access below copies bytes
    // into fixed-size arrays before decoding. The public API returns owned rows,
    // so mmap lifetimes never escape this module.
    unsafe { Mmap::map(file) }.map_err(DataError::from)
}

fn read_tick_level(
    mmap: &[u8],
    offset: &mut usize,
    bid_price: &mut f64,
    bid_volume: &mut i64,
    ask_price: &mut f64,
    ask_volume: &mut i64,
) -> Result<()> {
    *bid_price = read_f64_advance(mmap, offset)?;
    *bid_volume = read_f64_advance(mmap, offset)? as i64;
    *ask_price = read_f64_advance(mmap, offset)?;
    *ask_volume = read_f64_advance(mmap, offset)? as i64;
    Ok(())
}

fn read_f64_advance(bytes: &[u8], offset: &mut usize) -> Result<f64> {
    let value = read_f64_from(bytes, *offset)?;
    *offset += 8;
    Ok(value)
}

fn read_i64_from(bytes: &[u8], offset: usize) -> Result<i64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series cache offset overflow".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series cache row width mismatch".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    Ok(i64::from_ne_bytes(array))
}

fn read_f64_from(bytes: &[u8], offset: usize) -> Result<f64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        DataError::InvalidResponse("history series cache offset overflow".to_string())
    })?;
    let slice = bytes.get(offset..end).ok_or_else(|| {
        DataError::InvalidResponse("history series cache row width mismatch".to_string())
    })?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    Ok(f64::from_ne_bytes(array))
}
