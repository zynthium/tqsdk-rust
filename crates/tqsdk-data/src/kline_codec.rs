//! Shared lossless Kline field codec. Epoch placement belongs to the enclosing
//! format; the nine market fields have identical meaning at every duration.
//! This seam lets the existing daily/minute envelopes retain their exact bytes
//! while the versioned common container is introduced separately.

use tqsdk_core::Kline;

pub(crate) const FIELD_BYTES: usize = 72;

/// Semantic migration oracle independent of either envelope's wire encoding.
pub(crate) fn rows_equal(left: &[Kline], right: &[Kline]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.id == right.id
                && left.datetime == right.datetime
                && left.open.to_bits() == right.open.to_bits()
                && left.high.to_bits() == right.high.to_bits()
                && left.low.to_bits() == right.low.to_bits()
                && left.close.to_bits() == right.close.to_bits()
                && left.volume == right.volume
                && left.open_oi == right.open_oi
                && left.close_oi == right.close_oi
                && left.epoch == right.epoch
        })
}

pub(crate) fn encode_fields(output: &mut Vec<u8>, row: &Kline) {
    for word in [
        row.id as u64,
        row.datetime as u64,
        row.open.to_bits(),
        row.high.to_bits(),
        row.low.to_bits(),
        row.close.to_bits(),
        row.volume as u64,
        row.open_oi as u64,
        row.close_oi as u64,
    ] {
        output.extend_from_slice(&word.to_le_bytes());
    }
}

pub(crate) fn decode_fields(bytes: &[u8; FIELD_BYTES], epoch: Option<i64>) -> Kline {
    let word = |index: usize| {
        u64::from_le_bytes(
            bytes[index * 8..index * 8 + 8]
                .try_into()
                .expect("fixed Kline field"),
        )
    };
    Kline {
        id: word(0) as i64,
        datetime: word(1) as i64,
        open: f64::from_bits(word(2)),
        high: f64::from_bits(word(3)),
        low: f64::from_bits(word(4)),
        close: f64::from_bits(word(5)),
        volume: word(6) as i64,
        open_oi: word(7) as i64,
        close_oi: word(8) as i64,
        epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_wire_bytes_preserve_integer_extremes_and_float_payloads() {
        let words = [
            i64::MIN as u64,
            i64::MAX as u64,
            0x8000_0000_0000_0000,
            0x7ff8_0000_0000_0123,
            0xfff0_0000_0000_0000,
            0x0000_0000_0000_0001,
            u64::MAX,
            0,
            i64::MAX as u64,
        ];
        let expected: Vec<_> = words.into_iter().flat_map(u64::to_le_bytes).collect();
        let row = Kline {
            id: i64::MIN,
            datetime: i64::MAX,
            open: -0.0,
            high: f64::from_bits(0x7ff8_0000_0000_0123),
            low: f64::NEG_INFINITY,
            close: f64::from_bits(1),
            volume: -1,
            open_oi: 0,
            close_oi: i64::MAX,
            epoch: Some(17),
        };
        let mut encoded = Vec::new();
        encode_fields(&mut encoded, &row);
        assert_eq!(encoded, expected);
        for epoch in [None, Some(17)] {
            let decoded = decode_fields(expected.as_slice().try_into().unwrap(), epoch);
            assert_eq!(decoded.epoch, epoch);
            assert_eq!(decoded.id, row.id);
            assert_eq!(decoded.datetime, row.datetime);
            assert_eq!(decoded.volume, row.volume);
            assert_eq!(decoded.open_oi, row.open_oi);
            assert_eq!(decoded.close_oi, row.close_oi);
            assert_eq!(decoded.open.to_bits(), row.open.to_bits());
            assert_eq!(decoded.high.to_bits(), row.high.to_bits());
            assert_eq!(decoded.low.to_bits(), row.low.to_bits());
            assert_eq!(decoded.close.to_bits(), row.close.to_bits());
        }
    }
}
