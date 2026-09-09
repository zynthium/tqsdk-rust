//! Independent legacy fixture encoder: never extracts bytes from a new writer.
use tqsdk_core::Kline;
use tqsdk_data::MinuteKlineCacheSnapshot;

pub fn raw_month(
    version: u16,
    symbol: &str,
    month: &str,
    snapshot: &MinuteKlineCacheSnapshot,
    range: (i64, i64),
    rows: &[Kline],
) -> Vec<u8> {
    let mut metadata = snapshot.version.to_le_bytes().to_vec();
    for value in [
        symbol,
        month,
        &snapshot.calendar_hash,
        &snapshot.session_hash,
    ] {
        metadata.extend_from_slice(&(value.len() as u16).to_le_bytes());
        metadata.extend_from_slice(value.as_bytes());
    }
    let mut payload = metadata.clone();
    payload.extend_from_slice(&range.0.to_le_bytes());
    payload.extend_from_slice(&range.1.to_le_bytes());
    for row in rows {
        payload.extend_from_slice(&row.id.to_le_bytes());
        payload.extend_from_slice(&row.datetime.to_le_bytes());
        for value in [row.open, row.high, row.low, row.close] {
            payload.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        for value in [
            row.volume,
            row.open_oi,
            row.close_oi,
            row.epoch.unwrap_or(i64::MIN),
        ] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
    }
    let checksum = payload.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    let mut output = b"TQMK".to_vec();
    output.extend_from_slice(&version.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    output.extend_from_slice(&1_u64.to_le_bytes());
    output.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    output.extend_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(&payload);
    output
}
