//! Lossless Tick blocks for the common container. Words retain their original
//! bits; XOR + canonical unsigned LEB128 encodes changes, never price rounding.
//! Every block is independently decodable and bounded before row allocation.

use super::*;

const MAGIC: &[u8; 4] = b"TX01";
const WORDS: usize = 31;
const HEADER_BYTES: usize = 8;
const FIRST_BYTES: usize = HEADER_BYTES + WORDS * 8;
pub(crate) const MAX_ROWS: usize = 8_192;

pub(super) fn valid_length(rows: u64, bytes: u64) -> bool {
    (1..=MAX_ROWS as u64).contains(&rows)
        && bytes >= FIRST_BYTES as u64 + (rows - 1) * 4
        && bytes <= FIRST_BYTES as u64 + (rows - 1) * (4 + WORDS as u64 * 10)
}

// The Tick store adapter owns format dispatch. Existing TQBN files are never
// silently converted by this codec.
pub(crate) fn encode(rows: &[Tick]) -> Result<EncodedBlock> {
    if rows.is_empty()
        || rows.len() > MAX_ROWS
        || rows
            .windows(2)
            .any(|pair| pair[0].datetime > pair[1].datetime)
    {
        return Err(invalid("invalid Tick block row count or time order"));
    }
    let mut payload = encode_payload(rows)?;
    let decoded_len = payload.len() as u64;
    let compression = compress(&mut payload)?;
    Ok(EncodedBlock {
        block: Block {
            offset: 0,
            len: payload.len() as u64,
            decoded_len,
            checksum: checksum(&payload),
            codec: Codec::TickXorV1,
            tick_order_strict: Some(
                rows.windows(2)
                    .all(|pair| pair[0].datetime < pair[1].datetime && pair[0].id < pair[1].id),
            ),
            id_bounds: Some((
                rows.iter().map(|r| r.id).min().unwrap(),
                rows.iter().map(|r| r.id).max().unwrap(),
            )),
            compression,
            rows: rows.len() as u64,
            first_ns: rows[0].datetime,
            last_ns: rows[rows.len() - 1].datetime,
        },
        payload,
    })
}

pub(crate) fn read<M>(
    file: &mut File,
    index: &Index<M>,
    id: usize,
    allocation_limit: Option<usize>,
) -> Result<Vec<Tick>> {
    let block = index
        .blocks
        .get(id)
        .ok_or_else(|| invalid("unknown block"))?;
    if block.codec != Codec::TickXorV1 || !valid_length(block.rows, block.decoded_len) {
        return Err(invalid("invalid Tick block identity or size"));
    }
    check_allocation(block.read_allocation_bytes()?, allocation_limit)?;
    let payload = read_block(file, index, id)?;
    let mut cursor = Cursor::new(&payload, block.rows as usize)?;
    let mut rows: Vec<Tick> = Vec::new();
    rows.try_reserve_exact(block.rows as usize)
        .map_err(|_| invalid("cannot allocate decoded Tick rows"))?;
    while let Some(row) = cursor.next()? {
        if rows.last().is_some_and(|last| last.datetime > row.datetime) {
            return Err(invalid("Tick block times are not ordered"));
        }
        rows.push(row);
    }
    if Some(
        rows.windows(2)
            .all(|pair| pair[0].datetime < pair[1].datetime && pair[0].id < pair[1].id),
    ) != block.tick_order_strict
        || rows.first().map(|row| row.datetime) != Some(block.first_ns)
        || rows.last().map(|row| row.datetime) != Some(block.last_ns)
        || rows
            .iter()
            .map(|row| row.id)
            .min()
            .zip(rows.iter().map(|row| row.id).max())
            != block.id_bounds
    {
        return Err(invalid("Tick block index disagrees with decoded rows"));
    }
    Ok(rows)
}

fn encode_payload(rows: &[Tick]) -> Result<Vec<u8>> {
    if rows.is_empty() || rows.len() > MAX_ROWS {
        return Err(invalid("invalid Tick row count"));
    }
    // Count exact bytes first. No growing Vec capacity or unbounded temporary
    // row matrix; both passes use two fixed-size word arrays on the stack.
    let mut bytes = FIRST_BYTES;
    let mut previous = words(&rows[0]);
    for row in &rows[1..] {
        let current = words(row);
        bytes += 4;
        for (&left, &right) in previous.iter().zip(&current) {
            let xor = left ^ right;
            if xor != 0 {
                bytes += varint_len(xor);
            }
        }
        previous = current;
    }
    if bytes > MAX_BLOCK_BYTES {
        return Err(invalid("Tick payload exceeds block limit"));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes)
        .map_err(|_| invalid("cannot allocate Tick block"))?;
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    previous = words(&rows[0]);
    for value in previous {
        output.extend_from_slice(&value.to_le_bytes());
    }
    for row in &rows[1..] {
        let current = words(row);
        let mut mask = 0_u32;
        for (position, (&left, &right)) in previous.iter().zip(&current).enumerate() {
            if left != right {
                mask |= 1 << position;
            }
        }
        output.extend_from_slice(&mask.to_le_bytes());
        for (&left, &right) in previous.iter().zip(&current) {
            let xor = left ^ right;
            if xor != 0 {
                write_varint(&mut output, xor);
            }
        }
        previous = current;
    }
    debug_assert_eq!(output.len(), bytes);
    Ok(output)
}

fn varint_len(value: u64) -> usize {
    ((64 - value.leading_zeros()) as usize).div_ceil(7).max(1)
}

fn write_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    remaining: usize,
    first: bool,
    previous: [u64; WORDS],
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8], expected_rows: usize) -> Result<Self> {
        if !valid_length(expected_rows as u64, bytes.len() as u64)
            || bytes.get(..4) != Some(MAGIC.as_slice())
        {
            return Err(invalid("invalid Tick payload magic or length"));
        }
        let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        if count != expected_rows {
            return Err(invalid("Tick payload row count mismatch"));
        }
        let previous = std::array::from_fn(|i| {
            u64::from_le_bytes(bytes[8 + i * 8..16 + i * 8].try_into().unwrap())
        });
        Ok(Self {
            bytes,
            offset: FIRST_BYTES,
            remaining: count,
            first: true,
            previous,
        })
    }

    fn next(&mut self) -> Result<Option<Tick>> {
        if self.remaining == 0 {
            if self.offset != self.bytes.len() {
                return Err(invalid("Tick payload trailing bytes"));
            }
            return Ok(None);
        }
        if !self.first {
            let mask_bytes = self
                .bytes
                .get(self.offset..self.offset + 4)
                .ok_or_else(|| invalid("truncated Tick change mask"))?;
            let mask = u32::from_le_bytes(mask_bytes.try_into().unwrap());
            self.offset += 4;
            if mask >> WORDS != 0 {
                return Err(invalid("unknown Tick change-mask bit"));
            }
            for position in 0..WORDS {
                if mask & (1 << position) != 0 {
                    let xor = self.read_varint()?;
                    if xor == 0 {
                        return Err(invalid("redundant Tick change-mask bit"));
                    }
                    self.previous[position] ^= xor;
                }
            }
        }
        self.first = false;
        self.remaining -= 1;
        let row = from_words(&self.previous)?;
        // Reject surplus bytes before yielding the final row, not on a later
        // next() that a bounded caller might never make.
        if self.remaining == 0 && self.offset != self.bytes.len() {
            return Err(invalid("Tick payload trailing bytes"));
        }
        Ok(Some(row))
    }

    fn read_varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for position in 0..10 {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or_else(|| invalid("truncated Tick varint"))?;
            self.offset += 1;
            if position == 9 && byte > 1 {
                return Err(invalid("Tick varint overflow"));
            }
            value |= u64::from(byte & 0x7f) << (position * 7);
            if byte & 0x80 == 0 {
                if position > 0 && byte == 0 {
                    return Err(invalid("non-canonical Tick varint"));
                }
                return Ok(value);
            }
        }
        Err(invalid("Tick varint overflow"))
    }
}

fn words(row: &Tick) -> [u64; WORDS] {
    [
        row.id as u64,
        row.datetime as u64,
        row.last_price.to_bits(),
        row.average.to_bits(),
        row.highest.to_bits(),
        row.lowest.to_bits(),
        row.ask_price1.to_bits(),
        row.ask_volume1 as u64,
        row.bid_price1.to_bits(),
        row.bid_volume1 as u64,
        row.ask_price2.to_bits(),
        row.ask_volume2 as u64,
        row.bid_price2.to_bits(),
        row.bid_volume2 as u64,
        row.ask_price3.to_bits(),
        row.ask_volume3 as u64,
        row.bid_price3.to_bits(),
        row.bid_volume3 as u64,
        row.ask_price4.to_bits(),
        row.ask_volume4 as u64,
        row.bid_price4.to_bits(),
        row.bid_volume4 as u64,
        row.ask_price5.to_bits(),
        row.ask_volume5 as u64,
        row.bid_price5.to_bits(),
        row.bid_volume5 as u64,
        row.volume as u64,
        row.amount.to_bits(),
        row.open_interest as u64,
        u64::from(row.epoch.is_some()),
        row.epoch.unwrap_or(0) as u64,
    ]
}

fn from_words(w: &[u64; WORDS]) -> Result<Tick> {
    let epoch = match (w[29], w[30]) {
        (0, 0) => None,
        (1, value) => Some(value as i64),
        _ => return Err(invalid("invalid Tick epoch presence/value")),
    };
    Ok(Tick {
        id: w[0] as i64,
        datetime: w[1] as i64,
        last_price: f64::from_bits(w[2]),
        average: f64::from_bits(w[3]),
        highest: f64::from_bits(w[4]),
        lowest: f64::from_bits(w[5]),
        ask_price1: f64::from_bits(w[6]),
        ask_volume1: w[7] as i64,
        bid_price1: f64::from_bits(w[8]),
        bid_volume1: w[9] as i64,
        ask_price2: f64::from_bits(w[10]),
        ask_volume2: w[11] as i64,
        bid_price2: f64::from_bits(w[12]),
        bid_volume2: w[13] as i64,
        ask_price3: f64::from_bits(w[14]),
        ask_volume3: w[15] as i64,
        bid_price3: f64::from_bits(w[16]),
        bid_volume3: w[17] as i64,
        ask_price4: f64::from_bits(w[18]),
        ask_volume4: w[19] as i64,
        bid_price4: f64::from_bits(w[20]),
        bid_volume4: w[21] as i64,
        ask_price5: f64::from_bits(w[22]),
        ask_volume5: w[23] as i64,
        bid_price5: f64::from_bits(w[24]),
        bid_volume5: w[25] as i64,
        volume: w[26] as i64,
        amount: f64::from_bits(w[27]),
        open_interest: w[28] as i64,
        epoch,
    })
}

#[cfg(test)]
mod tests;
