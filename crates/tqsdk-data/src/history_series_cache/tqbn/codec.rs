#[cfg(feature = "tqbn-zstd")]
use std::io::Read;
use std::mem::size_of;

use crate::error::{DataError, Result};
use crate::history_series_cache::tqbn::fixed::{
    decode_amount, decode_price, encode_amount, encode_price,
};
use crate::history_series_cache::tqbn::format::{
    NONE_EPOCH, TQBN_BLOCK_MAGIC, TQBN_MAGIC, TQBN_SCHEMA_VERSION, TQBN_VERSION,
    TqbnCoverageRecordV1, TqbnKlineRecordV1, TqbnRType, TqbnRecordHeader, TqbnTick1RecordV1,
    TqbnTick5RecordV1,
};
use tqsdk_core::{Kline, Tick};

const SINGLE_SERIES_INSTRUMENT_ID: u32 = 1;
const FILE_PREFIX_HEADER_LEN: usize = TQBN_MAGIC.len() + 1 + 4 + 4 + 8;
const MAX_FILE_PREFIX_METADATA_LEN: usize = 64 * 1024;
const BLOCK_HEADER_LEN: usize = TQBN_BLOCK_MAGIC.len() + 1 + 3 + 8 + 8;
pub(super) const TQBN_BLOCK_FLAG_ZSTD: u8 = 0x01;
#[cfg(feature = "tqbn-zstd")]
const TQBN_ZSTD_LEVEL: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnFilePrefix {
    pub(super) bytes: Vec<u8>,
    pub(super) version: u8,
    pub(super) schema_version: u32,
    pub(super) metadata_checksum: u64,
    pub(super) metadata: Vec<u8>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TqbnBlockType {
    Metadata = 1,
    Records = 2,
    Index = 3,
}

impl TqbnBlockType {
    #[cfg(test)]
    fn decode(value: u8) -> Result<Self> {
        match value {
            value if value == Self::Metadata as u8 => Ok(Self::Metadata),
            value if value == Self::Records as u8 => Ok(Self::Records),
            value if value == Self::Index as u8 => Ok(Self::Index),
            _ => Err(DataError::InvalidResponse(format!(
                "TQBN block type {value} is unknown"
            ))),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnBlock {
    pub(super) block_type: TqbnBlockType,
    pub(super) records: Vec<u8>,
}

pub(super) fn encode_file_prefix(metadata: &[u8]) -> TqbnFilePrefix {
    let metadata_len = u32::try_from(metadata.len()).expect("TQBN metadata length must fit in u32");
    let metadata_checksum = checksum64_fnv1a(metadata);
    let mut bytes = Vec::with_capacity(FILE_PREFIX_HEADER_LEN + metadata.len());
    bytes.extend_from_slice(&TQBN_MAGIC);
    bytes.push(TQBN_VERSION);
    bytes.extend_from_slice(&TQBN_SCHEMA_VERSION.to_le_bytes());
    bytes.extend_from_slice(&metadata_len.to_le_bytes());
    bytes.extend_from_slice(&metadata_checksum.to_le_bytes());
    bytes.extend_from_slice(metadata);

    TqbnFilePrefix {
        bytes,
        version: TQBN_VERSION,
        schema_version: TQBN_SCHEMA_VERSION,
        metadata_checksum,
        metadata: metadata.to_vec(),
    }
}

pub(super) fn decode_file_prefix(bytes: &[u8]) -> Result<(TqbnFilePrefix, usize)> {
    if bytes.len() < TQBN_MAGIC.len() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file prefix magic is truncated: got {} bytes",
            bytes.len()
        )));
    }
    if bytes[..TQBN_MAGIC.len()] != TQBN_MAGIC {
        return Err(DataError::InvalidResponse(
            "TQBN file prefix magic mismatch".to_string(),
        ));
    }
    if bytes.len() < FILE_PREFIX_HEADER_LEN {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file prefix is truncated: requires {FILE_PREFIX_HEADER_LEN} bytes, got {}",
            bytes.len()
        )));
    }

    let version = bytes[TQBN_MAGIC.len()];
    if version != TQBN_VERSION {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file version {version} is unsupported"
        )));
    }

    let mut offset = TQBN_MAGIC.len() + 1;
    let schema_version = read_u32_at(bytes, &mut offset, "TQBN file schema version")?;
    let metadata_len = read_u32_at(bytes, &mut offset, "TQBN file metadata length")? as usize;
    if metadata_len > MAX_FILE_PREFIX_METADATA_LEN {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file metadata length {metadata_len} exceeds max {MAX_FILE_PREFIX_METADATA_LEN}"
        )));
    }
    let metadata_checksum = read_u64_at(bytes, &mut offset, "TQBN file metadata checksum")?;
    let end = offset.checked_add(metadata_len).ok_or_else(|| {
        DataError::InvalidResponse("TQBN file metadata length overflow".to_string())
    })?;
    if end > bytes.len() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file metadata is truncated: requires {metadata_len} bytes, got {}",
            bytes.len().saturating_sub(offset)
        )));
    }

    let metadata = &bytes[offset..end];
    let actual_checksum = checksum64_fnv1a(metadata);
    if actual_checksum != metadata_checksum {
        return Err(DataError::InvalidResponse(format!(
            "TQBN file metadata checksum mismatch: expected {metadata_checksum}, got {actual_checksum}"
        )));
    }

    Ok((
        TqbnFilePrefix {
            bytes: bytes[..end].to_vec(),
            version,
            schema_version,
            metadata_checksum,
            metadata: metadata.to_vec(),
        },
        end,
    ))
}

pub(super) fn encode_block(block_type: TqbnBlockType, records: &[u8]) -> Vec<u8> {
    encode_block_with_flags(block_type, 0, records)
}

pub(super) fn encode_records_block(records: &[u8]) -> Result<Vec<u8>> {
    #[cfg(feature = "tqbn-zstd")]
    {
        if !records.is_empty() {
            let compressed = zstd::bulk::compress(records, TQBN_ZSTD_LEVEL).map_err(|error| {
                DataError::InvalidResponse(format!("TQBN zstd compression failed: {error}"))
            })?;
            if compressed.len() < records.len() {
                return Ok(encode_block_with_flags(
                    TqbnBlockType::Records,
                    TQBN_BLOCK_FLAG_ZSTD,
                    &compressed,
                ));
            }
        }
    }
    Ok(encode_block(TqbnBlockType::Records, records))
}

fn encode_block_with_flags(block_type: TqbnBlockType, flags: u8, records: &[u8]) -> Vec<u8> {
    let records_len = u64::try_from(records.len()).expect("TQBN block length must fit in u64");
    let records_checksum = checksum64_fnv1a(records);
    let mut bytes = Vec::with_capacity(BLOCK_HEADER_LEN + records.len());
    bytes.extend_from_slice(&TQBN_BLOCK_MAGIC);
    bytes.push(block_type as u8);
    bytes.push(flags);
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend_from_slice(&records_len.to_le_bytes());
    bytes.extend_from_slice(&records_checksum.to_le_bytes());
    bytes.extend_from_slice(records);
    bytes
}

#[cfg(test)]
pub(super) fn decode_blocks(bytes: &[u8]) -> Result<Vec<TqbnBlock>> {
    let mut offset = 0;
    let mut blocks = Vec::new();
    while offset < bytes.len() {
        if bytes.len() - offset < TQBN_BLOCK_MAGIC.len() {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block magic is truncated at offset {offset}"
            )));
        }
        if bytes[offset..offset + TQBN_BLOCK_MAGIC.len()] != TQBN_BLOCK_MAGIC {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block magic mismatch at offset {offset}"
            )));
        }
        if bytes.len() - offset < BLOCK_HEADER_LEN {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block header is truncated at offset {offset}: requires {BLOCK_HEADER_LEN} bytes, got {}",
                bytes.len() - offset
            )));
        }

        let block_type = TqbnBlockType::decode(bytes[offset + TQBN_BLOCK_MAGIC.len()])?;
        let flags = bytes[offset + TQBN_BLOCK_MAGIC.len() + 1];
        let mut cursor = offset + TQBN_BLOCK_MAGIC.len() + 1 + 3;
        let records_len_u64 = read_u64_at(bytes, &mut cursor, "TQBN block records length")?;
        let records_len = usize::try_from(records_len_u64).map_err(|_| {
            DataError::InvalidResponse(format!(
                "TQBN block records length {records_len_u64} does not fit in usize"
            ))
        })?;
        let records_checksum = read_u64_at(bytes, &mut cursor, "TQBN block checksum")?;
        let end = cursor.checked_add(records_len).ok_or_else(|| {
            DataError::InvalidResponse("TQBN block records length overflow".to_string())
        })?;
        if end > bytes.len() {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block payload is truncated at offset {offset}: requires {records_len} bytes, got {}",
                bytes.len().saturating_sub(cursor)
            )));
        }

        let payload = &bytes[cursor..end];
        let actual_checksum = checksum64_fnv1a(payload);
        if actual_checksum != records_checksum {
            return Err(DataError::InvalidResponse(format!(
                "TQBN block checksum mismatch at offset {offset}: expected {records_checksum}, got {actual_checksum}"
            )));
        }
        let records =
            decode_block_payload(block_type as u8, flags, payload.to_vec(), usize::MAX / 2)?;

        blocks.push(TqbnBlock {
            block_type,
            records,
        });
        offset = end;
    }
    Ok(blocks)
}

pub(super) fn decode_block_payload(
    block_type: u8,
    flags: u8,
    payload: Vec<u8>,
    max_decoded_payload_bytes: usize,
) -> Result<Vec<u8>> {
    if flags & !TQBN_BLOCK_FLAG_ZSTD != 0 {
        return Err(DataError::InvalidResponse(format!(
            "TQBN block flags {flags:#04x} contain unsupported bits"
        )));
    }
    if flags & TQBN_BLOCK_FLAG_ZSTD == 0 {
        return Ok(payload);
    }
    if block_type != TqbnBlockType::Records as u8 {
        return Err(DataError::InvalidResponse(
            "TQBN zstd compression is only supported for records blocks".to_string(),
        ));
    }

    #[cfg(feature = "tqbn-zstd")]
    {
        decode_zstd_payload(&payload, max_decoded_payload_bytes)
    }
    #[cfg(not(feature = "tqbn-zstd"))]
    {
        let _ = max_decoded_payload_bytes;
        Err(DataError::InvalidResponse(
            "TQBN zstd-compressed block requires the tqbn-zstd feature".to_string(),
        ))
    }
}

#[cfg(feature = "tqbn-zstd")]
fn decode_zstd_payload(payload: &[u8], max_decoded_payload_bytes: usize) -> Result<Vec<u8>> {
    let limit = u64::try_from(max_decoded_payload_bytes.checked_add(1).ok_or_else(|| {
        DataError::InvalidResponse("TQBN zstd decode limit overflow".to_string())
    })?)
    .map_err(|_| DataError::InvalidResponse("TQBN zstd decode limit overflow".to_string()))?;
    let decoder = zstd::stream::read::Decoder::new(payload).map_err(|error| {
        DataError::InvalidResponse(format!("TQBN zstd decoder initialization failed: {error}"))
    })?;
    let mut reader = decoder.take(limit);
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).map_err(|error| {
        DataError::InvalidResponse(format!("TQBN zstd decompression failed: {error}"))
    })?;
    if decoded.len() > max_decoded_payload_bytes {
        return Err(DataError::InvalidResponse(format!(
            "TQBN zstd decoded payload length {} exceeds max {max_decoded_payload_bytes}",
            decoded.len()
        )));
    }
    Ok(decoded)
}

#[derive(Debug)]
pub(super) enum DecodedTqbnRecord<'a> {
    Kline {
        bytes: &'a [u8],
        record_size: usize,
    },
    Tick1 {
        bytes: &'a [u8],
        record_size: usize,
    },
    Tick5 {
        bytes: &'a [u8],
        record_size: usize,
    },
    Coverage {
        bytes: &'a [u8],
        record_size: usize,
    },
    Unknown {
        #[cfg_attr(
            not(test),
            expect(
                dead_code,
                reason = "normal reader skips unknown TQBN records by size; tests assert rtype"
            )
        )]
        rtype: u8,
        record_size: usize,
    },
}

pub(super) fn decode_one_record(bytes: &[u8]) -> Result<DecodedTqbnRecord<'_>> {
    let header = decode_record_header(bytes)?;
    let record_size = header.record_size();

    if record_size < size_of::<TqbnRecordHeader>() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN record length {record_size} is shorter than TQBN header {}",
            size_of::<TqbnRecordHeader>()
        )));
    }

    if record_size > bytes.len() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN record length {record_size} exceeds available bytes {}",
            bytes.len()
        )));
    }

    match header.rtype {
        value if value == TqbnRType::Kline as u8 => {
            decode_known_record::<TqbnKlineRecordV1>(bytes, record_size, "kline")
                .map(|bytes| DecodedTqbnRecord::Kline { bytes, record_size })
        }
        value if value == TqbnRType::Tick1 as u8 => {
            decode_known_record::<TqbnTick1RecordV1>(bytes, record_size, "tick1")
                .map(|bytes| DecodedTqbnRecord::Tick1 { bytes, record_size })
        }
        value if value == TqbnRType::Tick5 as u8 => {
            decode_known_record::<TqbnTick5RecordV1>(bytes, record_size, "tick5")
                .map(|bytes| DecodedTqbnRecord::Tick5 { bytes, record_size })
        }
        value if value == TqbnRType::Coverage as u8 => {
            decode_known_record::<TqbnCoverageRecordV1>(bytes, record_size, "coverage")
                .map(|bytes| DecodedTqbnRecord::Coverage { bytes, record_size })
        }
        rtype => Ok(DecodedTqbnRecord::Unknown { rtype, record_size }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EncodedTickRecord {
    Tick1(TqbnTick1RecordV1),
    Tick5(TqbnTick5RecordV1),
}

pub(super) fn encode_kline_record(row: &Kline) -> Result<TqbnKlineRecordV1> {
    let ts_event = encode_ts_event("kline", row.datetime)?;
    Ok(TqbnKlineRecordV1 {
        hd: TqbnRecordHeader::new::<TqbnKlineRecordV1>(
            TqbnRType::Kline,
            SINGLE_SERIES_INSTRUMENT_ID,
            ts_event,
        ),
        row_id: row.id,
        open: encode_price(row.open)?,
        high: encode_price(row.high)?,
        low: encode_price(row.low)?,
        close: encode_price(row.close)?,
        volume: row.volume,
        open_oi: row.open_oi,
        close_oi: row.close_oi,
        epoch: encode_epoch(row.epoch, "kline")?,
    })
}

pub(super) fn decode_kline_record(record: &TqbnKlineRecordV1) -> Result<Kline> {
    Ok(Kline {
        id: record.row_id,
        datetime: decode_datetime("kline", record.hd.ts_event)?,
        open: decode_price(record.open),
        high: decode_price(record.high),
        low: decode_price(record.low),
        close: decode_price(record.close),
        volume: record.volume,
        open_oi: record.open_oi,
        close_oi: record.close_oi,
        epoch: decode_epoch(record.epoch),
    })
}

pub(super) fn encode_tick_record(row: &Tick, five_level: bool) -> Result<EncodedTickRecord> {
    if five_level {
        encode_tick5_record(row).map(EncodedTickRecord::Tick5)
    } else {
        encode_tick1_record(row).map(EncodedTickRecord::Tick1)
    }
}

pub(super) fn decode_tick1_record(record: &TqbnTick1RecordV1) -> Result<Tick> {
    Ok(Tick {
        id: record.row_id,
        datetime: decode_datetime("tick1", record.hd.ts_event)?,
        last_price: decode_price(record.last_price),
        average: decode_price(record.average),
        highest: decode_price(record.highest),
        lowest: decode_price(record.lowest),
        ask_price1: decode_price(record.ask_price1),
        ask_volume1: record.ask_volume1,
        bid_price1: decode_price(record.bid_price1),
        bid_volume1: record.bid_volume1,
        volume: record.volume,
        amount: decode_amount(record.amount),
        open_interest: record.open_interest,
        epoch: decode_epoch(record.epoch),
        ..Default::default()
    })
}

pub(super) fn decode_tick5_record(record: &TqbnTick5RecordV1) -> Result<Tick> {
    Ok(Tick {
        id: record.row_id,
        datetime: decode_datetime("tick5", record.hd.ts_event)?,
        last_price: decode_price(record.last_price),
        average: decode_price(record.average),
        highest: decode_price(record.highest),
        lowest: decode_price(record.lowest),
        ask_price1: decode_price(record.ask_price1),
        ask_volume1: record.ask_volume1,
        bid_price1: decode_price(record.bid_price1),
        bid_volume1: record.bid_volume1,
        ask_price2: decode_price(record.ask_price2),
        ask_volume2: record.ask_volume2,
        bid_price2: decode_price(record.bid_price2),
        bid_volume2: record.bid_volume2,
        ask_price3: decode_price(record.ask_price3),
        ask_volume3: record.ask_volume3,
        bid_price3: decode_price(record.bid_price3),
        bid_volume3: record.bid_volume3,
        ask_price4: decode_price(record.ask_price4),
        ask_volume4: record.ask_volume4,
        bid_price4: decode_price(record.bid_price4),
        bid_volume4: record.bid_volume4,
        ask_price5: decode_price(record.ask_price5),
        ask_volume5: record.ask_volume5,
        bid_price5: decode_price(record.bid_price5),
        bid_volume5: record.bid_volume5,
        volume: record.volume,
        amount: decode_amount(record.amount),
        open_interest: record.open_interest,
        epoch: decode_epoch(record.epoch),
    })
}

fn decode_record_header(bytes: &[u8]) -> Result<TqbnRecordHeader> {
    if bytes.len() < size_of::<TqbnRecordHeader>() {
        return Err(DataError::InvalidResponse(format!(
            "TQBN header requires {} bytes, got {}",
            size_of::<TqbnRecordHeader>(),
            bytes.len()
        )));
    }

    Ok(TqbnRecordHeader {
        length_words: bytes[0],
        rtype: bytes[1],
        flags: u16::from_le_bytes([bytes[2], bytes[3]]),
        instrument_id: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        ts_event: u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]),
    })
}

fn read_u32_at(bytes: &[u8], offset: &mut usize, field_name: &'static str) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| DataError::InvalidResponse(format!("{field_name} offset overflow")))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| DataError::InvalidResponse(format!("{field_name} is truncated")))?;
    let mut array = [0_u8; 4];
    array.copy_from_slice(slice);
    *offset = end;
    Ok(u32::from_le_bytes(array))
}

fn read_u64_at(bytes: &[u8], offset: &mut usize, field_name: &'static str) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| DataError::InvalidResponse(format!("{field_name} offset overflow")))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| DataError::InvalidResponse(format!("{field_name} is truncated")))?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(slice);
    *offset = end;
    Ok(u64::from_le_bytes(array))
}

pub(super) fn checksum64_fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn decode_known_record<'a, R>(
    bytes: &'a [u8],
    record_size: usize,
    record_name: &'static str,
) -> Result<&'a [u8]> {
    let v1_size = size_of::<R>();
    if record_size < v1_size {
        return Err(DataError::InvalidResponse(format!(
            "TQBN {record_name} record length {record_size} is shorter than v1 layout {v1_size}"
        )));
    }

    Ok(&bytes[..v1_size])
}

fn encode_tick1_record(row: &Tick) -> Result<TqbnTick1RecordV1> {
    let ts_event = encode_ts_event("tick1", row.datetime)?;
    Ok(TqbnTick1RecordV1 {
        hd: TqbnRecordHeader::new::<TqbnTick1RecordV1>(
            TqbnRType::Tick1,
            SINGLE_SERIES_INSTRUMENT_ID,
            ts_event,
        ),
        row_id: row.id,
        last_price: encode_price(row.last_price)?,
        average: encode_price(row.average)?,
        highest: encode_price(row.highest)?,
        lowest: encode_price(row.lowest)?,
        ask_price1: encode_price(row.ask_price1)?,
        ask_volume1: row.ask_volume1,
        bid_price1: encode_price(row.bid_price1)?,
        bid_volume1: row.bid_volume1,
        volume: row.volume,
        amount: encode_amount(row.amount)?,
        open_interest: row.open_interest,
        epoch: encode_epoch(row.epoch, "tick1")?,
    })
}

fn encode_tick5_record(row: &Tick) -> Result<TqbnTick5RecordV1> {
    let ts_event = encode_ts_event("tick5", row.datetime)?;
    Ok(TqbnTick5RecordV1 {
        hd: TqbnRecordHeader::new::<TqbnTick5RecordV1>(
            TqbnRType::Tick5,
            SINGLE_SERIES_INSTRUMENT_ID,
            ts_event,
        ),
        row_id: row.id,
        last_price: encode_price(row.last_price)?,
        average: encode_price(row.average)?,
        highest: encode_price(row.highest)?,
        lowest: encode_price(row.lowest)?,
        ask_price1: encode_price(row.ask_price1)?,
        ask_volume1: row.ask_volume1,
        bid_price1: encode_price(row.bid_price1)?,
        bid_volume1: row.bid_volume1,
        ask_price2: encode_price(row.ask_price2)?,
        ask_volume2: row.ask_volume2,
        bid_price2: encode_price(row.bid_price2)?,
        bid_volume2: row.bid_volume2,
        ask_price3: encode_price(row.ask_price3)?,
        ask_volume3: row.ask_volume3,
        bid_price3: encode_price(row.bid_price3)?,
        bid_volume3: row.bid_volume3,
        ask_price4: encode_price(row.ask_price4)?,
        ask_volume4: row.ask_volume4,
        bid_price4: encode_price(row.bid_price4)?,
        bid_volume4: row.bid_volume4,
        ask_price5: encode_price(row.ask_price5)?,
        ask_volume5: row.ask_volume5,
        bid_price5: encode_price(row.bid_price5)?,
        bid_volume5: row.bid_volume5,
        volume: row.volume,
        amount: encode_amount(row.amount)?,
        open_interest: row.open_interest,
        epoch: encode_epoch(row.epoch, "tick5")?,
    })
}

fn encode_ts_event(record_name: &'static str, datetime: i64) -> Result<u64> {
    u64::try_from(datetime).map_err(|_| {
        DataError::InvalidResponse(format!(
            "TQBN {record_name} row datetime must be non-negative, got {datetime}"
        ))
    })
}

fn decode_datetime(record_name: &'static str, ts_event: u64) -> Result<i64> {
    i64::try_from(ts_event).map_err(|_| {
        DataError::InvalidResponse(format!(
            "TQBN {record_name} record ts_event {ts_event} exceeds i64::MAX"
        ))
    })
}

fn encode_epoch(epoch: Option<i64>, record_name: &'static str) -> Result<i64> {
    match epoch {
        Some(value) if value == NONE_EPOCH => Err(DataError::InvalidResponse(format!(
            "TQBN {record_name} row epoch uses reserved epoch sentinel {NONE_EPOCH}"
        ))),
        Some(value) => Ok(value),
        None => Ok(NONE_EPOCH),
    }
}

fn decode_epoch(epoch: i64) -> Option<i64> {
    if epoch == NONE_EPOCH {
        None
    } else {
        Some(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_series_cache::tqbn::format::{TqbnRType, TqbnRecordHeader};

    #[test]
    fn record_header_reports_size_from_length_words() {
        let header = TqbnRecordHeader::new::<TqbnCoverageRecordV1>(TqbnRType::Coverage, 1, 123);
        assert_eq!(
            header.record_size(),
            std::mem::size_of::<TqbnCoverageRecordV1>()
        );
    }

    #[test]
    fn file_prefix_round_trips_metadata() {
        let metadata = br#"{"symbol":"SHFE.rb2601","kind":"tick"}"#;
        let encoded = encode_file_prefix(metadata);

        let (decoded, consumed) = decode_file_prefix(&encoded.bytes).unwrap();

        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.version, super::super::format::TQBN_VERSION);
        assert_eq!(
            decoded.schema_version,
            super::super::format::TQBN_SCHEMA_VERSION
        );
        assert_eq!(consumed, encoded.bytes.len());
    }

    #[test]
    fn file_prefix_rejects_bad_magic() {
        let encoded = encode_file_prefix(b"metadata");
        let mut bytes = encoded.bytes;
        bytes[0..4].copy_from_slice(b"BAD!");

        let error = decode_file_prefix(&bytes).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN file prefix magic"))
        );
    }

    #[test]
    fn file_prefix_rejects_oversized_metadata_length() {
        let metadata_len = (MAX_FILE_PREFIX_METADATA_LEN + 1) as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&super::super::format::TQBN_MAGIC);
        bytes.push(super::super::format::TQBN_VERSION);
        bytes.extend_from_slice(&super::super::format::TQBN_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&metadata_len.to_le_bytes());
        bytes.extend_from_slice(&checksum64_fnv1a(&[]).to_le_bytes());

        let error = decode_file_prefix(&bytes).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("metadata length") && message.contains("exceeds max"))
        );
    }

    #[test]
    fn block_round_trips_records() {
        let records = b"record-bytes";
        let encoded = encode_block(TqbnBlockType::Records, records);

        let decoded = decode_blocks(&encoded).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].block_type, TqbnBlockType::Records);
        assert_eq!(decoded[0].records, records);
    }

    #[test]
    fn records_block_encoder_round_trips_payload() {
        let records = b"record-bytes";
        let encoded = encode_records_block(records).unwrap();

        let decoded = decode_blocks(&encoded).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].block_type, TqbnBlockType::Records);
        assert_eq!(decoded[0].records, records);
    }

    #[cfg(feature = "tqbn-zstd")]
    #[test]
    fn records_block_encoder_compresses_repetitive_payload() {
        let records = vec![7_u8; 16 * 1024];
        let encoded = encode_records_block(&records).unwrap();

        assert_eq!(encoded[5] & TQBN_BLOCK_FLAG_ZSTD, TQBN_BLOCK_FLAG_ZSTD);
        assert!(encoded.len() < records.len());

        let decoded = decode_blocks(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].block_type, TqbnBlockType::Records);
        assert_eq!(decoded[0].records, records.as_slice());
    }

    #[test]
    fn blocks_reject_checksum_mismatch() {
        let mut encoded = encode_block(TqbnBlockType::Metadata, b"metadata");
        *encoded.last_mut().unwrap() ^= 0xff;

        let error = decode_blocks(&encoded).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN block checksum mismatch"))
        );
    }

    #[test]
    fn blocks_reject_truncated_payload() {
        let mut encoded = encode_block(TqbnBlockType::Index, b"index");
        encoded.pop();

        let error = decode_blocks(&encoded).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN block payload is truncated"))
        );
    }

    #[test]
    #[should_panic(expected = "TQBN record layout size must be a multiple of 4 bytes")]
    fn record_header_rejects_non_word_sized_layout() {
        let _ = TqbnRecordHeader::new::<[u8; 5]>(TqbnRType::Coverage, 1, 123);
    }

    #[test]
    #[should_panic(expected = "TQBN record layout length must fit in u8 words")]
    fn record_header_rejects_layout_too_large_for_length_words() {
        let _ = TqbnRecordHeader::new::<[u8; 1024]>(TqbnRType::Coverage, 1, 123);
    }

    #[test]
    fn decoder_skips_unknown_record_by_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[4, 255, 0, 0]);
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&123_u64.to_le_bytes());

        let decoded = decode_one_record(&bytes).unwrap();
        assert!(matches!(
            decoded,
            DecodedTqbnRecord::Unknown {
                rtype: 255,
                record_size: 16
            }
        ));
    }

    #[test]
    fn known_record_returns_v1_prefix_when_record_is_longer() {
        assert_known_prefix::<TqbnKlineRecordV1>(TqbnRType::Kline, |decoded| {
            let DecodedTqbnRecord::Kline { bytes, record_size } = decoded else {
                panic!("expected kline record");
            };
            assert_eq!(bytes.len(), std::mem::size_of::<TqbnKlineRecordV1>());
            assert_eq!(
                record_size,
                std::mem::size_of::<TqbnKlineRecordV1>() + TqbnRecordHeader::LENGTH_MULTIPLIER
            );
        });
        assert_known_prefix::<TqbnTick1RecordV1>(TqbnRType::Tick1, |decoded| {
            let DecodedTqbnRecord::Tick1 { bytes, record_size } = decoded else {
                panic!("expected tick1 record");
            };
            assert_eq!(bytes.len(), std::mem::size_of::<TqbnTick1RecordV1>());
            assert_eq!(
                record_size,
                std::mem::size_of::<TqbnTick1RecordV1>() + TqbnRecordHeader::LENGTH_MULTIPLIER
            );
        });
        assert_known_prefix::<TqbnTick5RecordV1>(TqbnRType::Tick5, |decoded| {
            let DecodedTqbnRecord::Tick5 { bytes, record_size } = decoded else {
                panic!("expected tick5 record");
            };
            assert_eq!(bytes.len(), std::mem::size_of::<TqbnTick5RecordV1>());
            assert_eq!(
                record_size,
                std::mem::size_of::<TqbnTick5RecordV1>() + TqbnRecordHeader::LENGTH_MULTIPLIER
            );
        });
        assert_known_prefix::<TqbnCoverageRecordV1>(TqbnRType::Coverage, |decoded| {
            let DecodedTqbnRecord::Coverage { bytes, record_size } = decoded else {
                panic!("expected coverage record");
            };
            assert_eq!(bytes.len(), std::mem::size_of::<TqbnCoverageRecordV1>());
            assert_eq!(
                record_size,
                std::mem::size_of::<TqbnCoverageRecordV1>() + TqbnRecordHeader::LENGTH_MULTIPLIER
            );
        });
    }

    #[test]
    fn decoder_rejects_declared_record_length_shorter_than_header() {
        let bytes = record_bytes_with_length_words(TqbnRType::Coverage, 0, 16);

        let error = decode_one_record(&bytes).unwrap_err();
        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN record length 0 is shorter than TQBN header"))
        );
    }

    #[test]
    fn decoder_rejects_declared_record_length_beyond_input() {
        let bytes = record_bytes_with_length_words(TqbnRType::Coverage, 5, 16);

        let error = decode_one_record(&bytes).unwrap_err();
        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN record length 20 exceeds available bytes 16"))
        );
    }

    #[test]
    fn known_record_rejects_shorter_than_v1_layout() {
        let v1_size = std::mem::size_of::<TqbnCoverageRecordV1>();
        let record_size = v1_size - TqbnRecordHeader::LENGTH_MULTIPLIER;
        let bytes = record_bytes(TqbnRType::Coverage, record_size);

        let error = decode_one_record(&bytes).unwrap_err();
        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN coverage record length"))
        );
    }

    #[test]
    fn kline_record_round_trips_user_row() {
        let row = tqsdk_core::Kline {
            id: 7,
            datetime: 1_000,
            open: 10.1,
            high: 11.2,
            low: 9.9,
            close: 10.8,
            volume: 100,
            open_oi: 200,
            close_oi: 210,
            epoch: Some(42),
        };
        let rec = encode_kline_record(&row).unwrap();
        let decoded = decode_kline_record(&rec).unwrap();
        assert_eq!(decoded.id, row.id);
        assert_eq!(decoded.datetime, row.datetime);
        assert_eq!(decoded.open, row.open);
        assert_eq!(decoded.close, row.close);
        assert_eq!(decoded.epoch, Some(42));
    }

    #[test]
    fn tick5_record_round_trips_depth_five_row() {
        let row = tick_with_five_levels();
        let rec = encode_tick_record(&row, true).unwrap();
        let decoded = match rec {
            EncodedTickRecord::Tick5(record) => decode_tick5_record(&record).unwrap(),
            EncodedTickRecord::Tick1(_) => panic!("expected tick5 record"),
        };
        assert_eq!(decoded.id, row.id);
        assert_eq!(decoded.datetime, row.datetime);
        assert_eq!(decoded.ask_price5, row.ask_price5);
        assert_eq!(decoded.bid_volume5, row.bid_volume5);
    }

    #[test]
    fn kline_record_round_trips_none_epoch() {
        let row = tqsdk_core::Kline {
            id: 8,
            datetime: 1_001,
            epoch: None,
            ..Default::default()
        };

        let rec = encode_kline_record(&row).unwrap();
        let decoded = decode_kline_record(&rec).unwrap();

        assert_eq!(rec.epoch, super::super::format::NONE_EPOCH);
        assert_eq!(decoded.epoch, None);
    }

    #[test]
    fn kline_record_rejects_reserved_epoch_sentinel() {
        let row = tqsdk_core::Kline {
            id: 8,
            datetime: 1_001,
            epoch: Some(super::super::format::NONE_EPOCH),
            ..Default::default()
        };

        let error = encode_kline_record(&row).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN kline row epoch uses reserved epoch sentinel"))
        );
    }

    #[test]
    fn tick_records_reject_reserved_epoch_sentinel() {
        let row = tqsdk_core::Tick {
            epoch: Some(super::super::format::NONE_EPOCH),
            ..tick_with_five_levels()
        };

        for five_level in [false, true] {
            let error = encode_tick_record(&row, five_level).unwrap_err();

            assert!(
                matches!(error, DataError::InvalidResponse(message) if message.contains("reserved epoch sentinel"))
            );
        }
    }

    #[test]
    fn tick1_record_leaves_depth_two_through_five_at_defaults() {
        let row = tick_with_five_levels();

        let rec = encode_tick_record(&row, false).unwrap();
        let decoded = match rec {
            EncodedTickRecord::Tick1(record) => decode_tick1_record(&record).unwrap(),
            EncodedTickRecord::Tick5(_) => panic!("expected tick1 record"),
        };

        assert_eq!(decoded.ask_price1, row.ask_price1);
        assert_eq!(decoded.bid_volume1, row.bid_volume1);
        let default_tick = tqsdk_core::Tick::default();
        assert_eq!(decoded.ask_price2, default_tick.ask_price2);
        assert_eq!(decoded.ask_volume2, default_tick.ask_volume2);
        assert_eq!(decoded.bid_price5, default_tick.bid_price5);
        assert_eq!(decoded.bid_volume5, default_tick.bid_volume5);
    }

    #[test]
    fn encode_kline_record_rejects_negative_datetime() {
        let row = tqsdk_core::Kline {
            datetime: -1,
            ..Default::default()
        };

        let error = encode_kline_record(&row).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN kline row datetime must be non-negative"))
        );
    }

    #[test]
    fn decode_datetime_rejects_ts_event_above_i64_max() {
        let ts_event = i64::MAX as u64 + 1;

        let error = decode_datetime("kline", ts_event).unwrap_err();

        assert!(
            matches!(error, DataError::InvalidResponse(message) if message.contains("TQBN kline record ts_event"))
        );
    }

    fn tick_with_five_levels() -> tqsdk_core::Tick {
        tqsdk_core::Tick {
            id: 9,
            datetime: 2_000,
            last_price: 618.5,
            average: 617.5,
            highest: 620.0,
            lowest: 610.0,
            ask_price1: 619.5,
            ask_volume1: 10,
            bid_price1: 617.5,
            bid_volume1: 11,
            ask_price2: 620.5,
            ask_volume2: 20,
            bid_price2: 616.5,
            bid_volume2: 21,
            ask_price3: 621.5,
            ask_volume3: 30,
            bid_price3: 615.5,
            bid_volume3: 31,
            ask_price4: 622.5,
            ask_volume4: 40,
            bid_price4: 614.5,
            bid_volume4: 41,
            ask_price5: 623.5,
            ask_volume5: 50,
            bid_price5: 613.5,
            bid_volume5: 51,
            volume: 1000,
            amount: 1_234_567.8,
            open_interest: 888,
            epoch: Some(77),
        }
    }

    fn assert_known_prefix<R>(
        rtype: TqbnRType,
        assert_decoded: impl FnOnce(DecodedTqbnRecord<'_>),
    ) {
        let record_size = std::mem::size_of::<R>() + TqbnRecordHeader::LENGTH_MULTIPLIER;
        let bytes = record_bytes(rtype, record_size);

        assert_decoded(decode_one_record(&bytes).unwrap());
    }

    fn record_bytes(rtype: TqbnRType, record_size: usize) -> Vec<u8> {
        assert_eq!(record_size % TqbnRecordHeader::LENGTH_MULTIPLIER, 0);
        record_bytes_with_length_words(
            rtype,
            (record_size / TqbnRecordHeader::LENGTH_MULTIPLIER) as u8,
            record_size,
        )
    }

    fn record_bytes_with_length_words(
        rtype: TqbnRType,
        length_words: u8,
        byte_len: usize,
    ) -> Vec<u8> {
        let header = TqbnRecordHeader {
            length_words,
            rtype: rtype as u8,
            flags: 0,
            instrument_id: 1,
            ts_event: 123,
        };
        let mut bytes = Vec::with_capacity(byte_len);
        bytes.push(header.length_words);
        bytes.push(header.rtype);
        bytes.extend_from_slice(&header.flags.to_le_bytes());
        bytes.extend_from_slice(&header.instrument_id.to_le_bytes());
        bytes.extend_from_slice(&header.ts_event.to_le_bytes());
        bytes.resize(byte_len, 0);
        bytes
    }
}
