use crate::error::{DataError, Result};

use super::format::{FIXED_AMOUNT_SCALE, FIXED_PRICE_SCALE, UNDEF_AMOUNT, UNDEF_PRICE};

pub(super) fn encode_price(value: f64) -> Result<i64> {
    encode_scaled_decimal_or_sentinel(value, FIXED_PRICE_SCALE, UNDEF_PRICE)
}

pub(super) fn decode_price(value: i64) -> f64 {
    if value == UNDEF_PRICE {
        f64::NAN
    } else {
        value as f64 / FIXED_PRICE_SCALE as f64
    }
}

pub(super) fn encode_amount(value: f64) -> Result<i64> {
    encode_scaled_decimal_or_sentinel(value, FIXED_AMOUNT_SCALE, UNDEF_AMOUNT)
}

pub(super) fn decode_amount(value: i64) -> f64 {
    if value == UNDEF_AMOUNT {
        f64::NAN
    } else {
        decode_scaled_decimal(value, FIXED_AMOUNT_SCALE)
    }
}

fn encode_scaled_decimal_or_sentinel(value: f64, scale: i64, sentinel: i64) -> Result<i64> {
    if !value.is_finite() {
        return Ok(sentinel);
    }
    encode_scaled_decimal(value, scale)
}

fn encode_scaled_decimal(value: f64, scale: i64) -> Result<i64> {
    let scaled = (value * scale as f64).round();
    if scaled < i64::MIN as f64 || scaled >= i64::MAX as f64 {
        return Err(DataError::InvalidResponse(
            "history cache fixed-point value overflow".to_string(),
        ));
    }
    Ok(scaled as i64)
}

fn decode_scaled_decimal(value: i64, scale: i64) -> f64 {
    value as f64 / scale as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_price_round_trips_decimal_prices() {
        let encoded = encode_price(618.5).unwrap();
        assert_eq!(encoded, 618_500_000_000);
        assert_eq!(decode_price(encoded), 618.5);
    }

    #[test]
    fn fixed_price_maps_nan_to_sentinel() {
        assert_eq!(encode_price(f64::NAN).unwrap(), UNDEF_PRICE);
        assert!(decode_price(UNDEF_PRICE).is_nan());
    }

    #[test]
    fn fixed_amount_uses_metadata_amount_scale() {
        let encoded = encode_amount(1_234.567_8).unwrap();
        assert_eq!(encoded, 1_234_567_800);
        assert_eq!(decode_amount(encoded), 1_234.567_8);
    }

    #[test]
    fn fixed_point_rejects_sentinel_collision() {
        let err = encode_scaled_decimal(i64::MAX as f64, 1).unwrap_err();
        assert!(matches!(
            err,
            DataError::InvalidResponse(message)
                if message == "history cache fixed-point value overflow"
        ));
    }
}
