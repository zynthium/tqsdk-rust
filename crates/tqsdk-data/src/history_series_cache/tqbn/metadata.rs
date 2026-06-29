use crate::error::{DataError, Result};

use super::format::{FIXED_AMOUNT_SCALE, FIXED_PRICE_SCALE};

const MIN_MAPPING_BYTES: usize = 4 + 2 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TqbnSchema {
    Kline = 1,
    Tick = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnInstrumentMapping {
    pub instrument_id: u32,
    pub symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnMetadata {
    pub dataset: String,
    pub schema: TqbnSchema,
    pub symbol: String,
    pub duration_ns: i64,
    pub price_scale: i64,
    pub amount_scale: i64,
    pub level_depth: u8,
    pub instruments: Vec<TqbnInstrumentMapping>,
}

impl TqbnMetadata {
    pub(super) fn single_series_tick(symbol: String, level_depth: u8) -> Self {
        Self {
            dataset: "tqsdk-history".to_string(),
            schema: TqbnSchema::Tick,
            symbol: symbol.clone(),
            duration_ns: 0,
            price_scale: FIXED_PRICE_SCALE,
            amount_scale: FIXED_AMOUNT_SCALE,
            level_depth,
            instruments: vec![single_mapping(symbol)],
        }
    }

    pub(super) fn single_series_kline(symbol: String, duration_ns: i64) -> Self {
        Self {
            dataset: "tqsdk-history".to_string(),
            schema: TqbnSchema::Kline,
            symbol: symbol.clone(),
            duration_ns,
            price_scale: FIXED_PRICE_SCALE,
            amount_scale: FIXED_AMOUNT_SCALE,
            level_depth: 0,
            instruments: vec![single_mapping(symbol)],
        }
    }
}

pub(super) fn encode_metadata(metadata: &TqbnMetadata) -> Result<Vec<u8>> {
    let instrument_count = u32::try_from(metadata.instruments.len())
        .map_err(|_| metadata_error("too many instrument mappings"))?;

    let mut bytes = Vec::new();
    write_string(&mut bytes, &metadata.dataset)?;
    bytes.push(metadata.schema as u8);
    write_string(&mut bytes, &metadata.symbol)?;
    bytes.extend_from_slice(&metadata.duration_ns.to_le_bytes());
    bytes.extend_from_slice(&metadata.price_scale.to_le_bytes());
    bytes.extend_from_slice(&metadata.amount_scale.to_le_bytes());
    bytes.push(metadata.level_depth);
    bytes.extend_from_slice(&instrument_count.to_le_bytes());

    for instrument in &metadata.instruments {
        bytes.extend_from_slice(&instrument.instrument_id.to_le_bytes());
        write_string(&mut bytes, &instrument.symbol)?;
        bytes.extend_from_slice(&instrument.start_ns.to_le_bytes());
        bytes.extend_from_slice(&instrument.end_ns.to_le_bytes());
    }

    Ok(bytes)
}

pub(super) fn decode_metadata(bytes: &[u8]) -> Result<TqbnMetadata> {
    let mut decoder = MetadataDecoder::new(bytes);

    let dataset = decoder.read_string("dataset")?;
    let schema = match decoder.read_u8("schema")? {
        1 => TqbnSchema::Kline,
        2 => TqbnSchema::Tick,
        value => {
            return Err(metadata_error(format!("unknown schema id {value}")));
        }
    };
    let symbol = decoder.read_string("symbol")?;
    let duration_ns = decoder.read_i64("duration_ns")?;
    let price_scale = decoder.read_i64("price_scale")?;
    let amount_scale = decoder.read_i64("amount_scale")?;
    let level_depth = decoder.read_u8("level_depth")?;
    let instrument_count = decoder.read_u32("instrument_count")?;
    let instrument_count = instrument_count as usize;

    if instrument_count > decoder.remaining_len() / MIN_MAPPING_BYTES {
        return Err(metadata_error(format!(
            "instrument count {instrument_count} exceeds remaining metadata bytes"
        )));
    }

    let mut instruments = Vec::with_capacity(instrument_count);

    for _ in 0..instrument_count {
        instruments.push(TqbnInstrumentMapping {
            instrument_id: decoder.read_u32("instrument_id")?,
            symbol: decoder.read_string("instrument symbol")?,
            start_ns: decoder.read_i64("start_ns")?,
            end_ns: decoder.read_i64("end_ns")?,
        });
    }

    if !decoder.is_done() {
        return Err(metadata_error("trailing bytes after metadata"));
    }

    Ok(TqbnMetadata {
        dataset,
        schema,
        symbol,
        duration_ns,
        price_scale,
        amount_scale,
        level_depth,
        instruments,
    })
}

fn single_mapping(symbol: String) -> TqbnInstrumentMapping {
    TqbnInstrumentMapping {
        instrument_id: 1,
        symbol,
        start_ns: i64::MIN,
        end_ns: i64::MAX,
    }
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    let len =
        u16::try_from(value.len()).map_err(|_| metadata_error("string exceeds u16 length"))?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

struct MetadataDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MetadataDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self, field: &str) -> Result<u8> {
        let bytes = self.read_exact(field, 1)?;
        Ok(bytes[0])
    }

    fn read_u16(&mut self, field: &str) -> Result<u16> {
        let bytes = self.read_exact(field, 2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self, field: &str) -> Result<u32> {
        let bytes = self.read_exact(field, 4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64> {
        let bytes = self.read_exact(field, 8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self, field: &str) -> Result<String> {
        let len = self.read_u16(field)? as usize;
        let bytes = self.read_exact(field, len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| metadata_error(format!("{field} is not UTF-8")))
    }

    fn read_exact(&mut self, field: &str, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| metadata_error(format!("offset overflow while reading {field}")))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| metadata_error(format!("truncated while reading {field}")))?;
        self.offset = end;
        Ok(bytes)
    }

    fn is_done(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

fn metadata_error(message: impl Into<String>) -> DataError {
    DataError::InvalidResponse(format!("history-cache TQBN metadata: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trips_tick_symbol_mapping() {
        let metadata = TqbnMetadata::single_series_tick("SHFE.rb2601".to_string(), 5);
        let encoded = encode_metadata(&metadata).unwrap();
        let decoded = decode_metadata(&encoded).unwrap();

        assert_eq!(decoded.dataset, "tqsdk-history");
        assert_eq!(decoded.symbol, "SHFE.rb2601");
        assert_eq!(decoded.duration_ns, 0);
        assert_eq!(decoded.level_depth, 5);
        assert_eq!(decoded.instruments.len(), 1);
        assert_eq!(decoded.instruments[0].instrument_id, 1);
        assert_eq!(decoded.instruments[0].symbol, "SHFE.rb2601");
    }

    #[test]
    fn metadata_round_trips_kline_duration() {
        let metadata = TqbnMetadata::single_series_kline("SHFE.au2608".to_string(), 60_000_000_000);
        let decoded = decode_metadata(&encode_metadata(&metadata).unwrap()).unwrap();

        assert_eq!(decoded.schema, TqbnSchema::Kline);
        assert_eq!(decoded.duration_ns, 60_000_000_000);
        assert_eq!(decoded.level_depth, 0);
    }

    #[test]
    fn metadata_rejects_count_larger_than_remaining_mapping_bytes() {
        let metadata = TqbnMetadata {
            instruments: Vec::new(),
            ..TqbnMetadata::single_series_tick("SHFE.rb2601".to_string(), 5)
        };
        let mut encoded = encode_metadata(&metadata).unwrap();
        let count_offset = encoded.len() - std::mem::size_of::<u32>();
        encoded[count_offset..].copy_from_slice(&u32::MAX.to_le_bytes());

        let err = decode_metadata(&encoded).unwrap_err();

        assert!(matches!(
            err,
            DataError::InvalidResponse(message)
                if message.starts_with("history-cache TQBN metadata: instrument count")
        ));
    }
}
